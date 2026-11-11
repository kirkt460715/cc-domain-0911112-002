import json
from decimal import Decimal
from hangar_ingest.types import canonical, decimal_string, digest, fail, identity, integer, number, object_fields
from hangar_ingest.runs import journal, partitions
from hangar_ingest.exports import releases

SCHEMA = '''
CREATE TABLE IF NOT EXISTS delivery_contracts(
 contract_id TEXT PRIMARY KEY,revision INTEGER NOT NULL,aircraft TEXT NOT NULL,
 metrics TEXT NOT NULL,min_samples INTEGER NOT NULL,max_quarantine_ratio TEXT NOT NULL,
 require_closed_orders INTEGER NOT NULL,required_evidence TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS acceptance_reports(
 report_id TEXT PRIMARY KEY,contract_id TEXT NOT NULL,contract_revision INTEGER NOT NULL,
 release_id TEXT NOT NULL,release_digest TEXT NOT NULL,payload TEXT NOT NULL,
 accepted INTEGER NOT NULL,created_at TEXT NOT NULL);
'''


class DeliveryContracts:
    def __init__(self, db):
        self.db = db
        if db.depth:
            fail('transaction_scope', 'Initialize delivery contracts outside transaction')
        db.db.executescript(SCHEMA)

    def configure(self, contract_id, revision, aircraft, metrics, *, min_samples=1, max_quarantine_ratio='0', require_closed_orders=True, required_evidence=None):
        identity(contract_id, 'contract_id')
        identity(aircraft, 'aircraft')
        integer(revision, 'revision')
        integer(min_samples, 'min_samples', 0, 1000000)
        if not isinstance(metrics, list) or not metrics or len(metrics) != len(set(metrics)):
            fail('contract_metrics', 'Metrics must be a unique nonempty list')
        normalized = sorted(identity(metric, 'metric') for metric in metrics)
        ratio = number(max_quarantine_ratio, 'max_quarantine_ratio')
        if not 0 <= ratio <= 1:
            fail('contract_ratio', 'Quarantine ratio must be between zero and one')
        if type(require_closed_orders) is not bool:
            fail('contract_boolean', 'Closed order requirement must be boolean')
        evidence = [] if required_evidence is None else required_evidence
        if not isinstance(evidence, list) or len(evidence) != len(set(evidence)):
            fail('contract_evidence', 'Evidence kinds must be unique')
        evidence = sorted(identity(item, 'evidence') for item in evidence)
        with self.db.transaction():
            previous = self.db.one('SELECT revision FROM delivery_contracts WHERE contract_id=?', (contract_id,))
            expected = previous['revision'] if previous else 0
            if revision != expected:
                fail('stale_revision', 'Delivery contract changed')
            self.db.execute(
                'INSERT INTO delivery_contracts(contract_id,revision,aircraft,metrics,min_samples,max_quarantine_ratio,require_closed_orders,required_evidence,created_at) '
                'VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(contract_id) DO UPDATE SET revision=excluded.revision,aircraft=excluded.aircraft,'
                'metrics=excluded.metrics,min_samples=excluded.min_samples,max_quarantine_ratio=excluded.max_quarantine_ratio,'
                'require_closed_orders=excluded.require_closed_orders,required_evidence=excluded.required_evidence',
                (contract_id, revision + 1, aircraft, canonical(normalized), min_samples, decimal_string(ratio), int(require_closed_orders), canonical(evidence), self.db.clock()),
            )
            journal.record(self.db, 'delivery_contract.configured', {'contract_id': contract_id, 'revision': revision + 1})
            return self.get(contract_id)

    def get(self, contract_id):
        row = self.db.one('SELECT * FROM delivery_contracts WHERE contract_id=?', (contract_id,))
        if row is None:
            fail('unknown_contract', 'Delivery contract does not exist')
        row['metrics'] = json.loads(row['metrics'])
        row['required_evidence'] = json.loads(row['required_evidence'])
        return row

    def assess(self, contract_id, release_id, evidence=None):
        contract = self.get(contract_id)
        package = releases.payloads(self.db, release_id)
        evidence = {} if evidence is None else evidence
        if not isinstance(evidence, dict):
            fail('evidence_object', 'Delivery evidence must be an object')
        failures = []
        missing = sorted(set(contract['required_evidence']) - set(evidence))
        if missing:
            failures.append({'code': 'missing_evidence', 'kinds': missing})
        metrics = {metric: 0 for metric in contract['metrics']}
        batches_used = set()
        observations = set()
        for entry in package['partitions']:
            payload = entry['payload']
            if payload['aircraft'] != contract['aircraft']:
                failures.append({'code': 'aircraft_mismatch', 'partition_key': entry['partition_key']})
            for row in payload['observations']:
                if row['observation_id'] in observations:
                    continue
                observations.add(row['observation_id'])
                if row['metric'] in metrics:
                    metrics[row['metric']] += 1
                origin = self.db.one('SELECT batch_id FROM observations WHERE observation_id=?', (row['observation_id'],))
                if origin:
                    batches_used.add(origin['batch_id'])
            if contract['require_closed_orders']:
                for window in payload['windows']:
                    if window['state'] != 'closed':
                        failures.append({'code': 'open_work_order', 'order_id': window['order_id']})
        for metric, count in metrics.items():
            if count < contract['min_samples']:
                failures.append({'code': 'insufficient_samples', 'metric': metric, 'count': count, 'required': contract['min_samples']})
        total = 0
        rejected = 0
        for batch_id in sorted(batches_used):
            batch = self.db.one('SELECT total,rejected FROM batches WHERE batch_id=?', (batch_id,))
            total += batch['total']
            rejected += batch['rejected']
        ratio = Decimal(rejected) / total if total else Decimal(0)
        if ratio > Decimal(contract['max_quarantine_ratio']):
            failures.append({'code': 'quarantine_ratio', 'actual': decimal_string(ratio), 'maximum': contract['max_quarantine_ratio']})
        report = {
            'contract_id': contract_id,
            'contract_revision': contract['revision'],
            'release_id': release_id,
            'release_digest': package['digest'],
            'accepted': not failures,
            'failures': failures,
            'metric_samples': metrics,
            'quarantine_ratio': decimal_string(ratio),
            'evidence_digests': {key: digest(value) for key, value in sorted(evidence.items())},
        }
        return report

    def certify(self, contract_id, release_id, evidence=None):
        with self.db.transaction():
            report = self.assess(contract_id, release_id, evidence)
            report_id = digest(report)
            self.db.execute(
                'INSERT OR IGNORE INTO acceptance_reports(report_id,contract_id,contract_revision,release_id,release_digest,payload,accepted,created_at) VALUES(?,?,?,?,?,?,?,?)',
                (report_id, contract_id, report['contract_revision'], release_id, report['release_digest'], canonical(report), int(report['accepted']), self.db.clock()),
            )
            journal.record(self.db, 'delivery.assessed', {'report_id': report_id, 'accepted': report['accepted']})
            return {'report_id': report_id, **report}

    def report(self, report_id):
        row = self.db.one('SELECT * FROM acceptance_reports WHERE report_id=?', (report_id,))
        if row is None:
            fail('unknown_report', 'Acceptance report does not exist')
        payload = json.loads(row['payload'])
        if digest(payload) != report_id:
            fail('report_corrupt', 'Acceptance report hash mismatch')
        return {'report_id': report_id, **payload}

    def listing(self):
        return [self.get(row['contract_id']) for row in self.db.all('SELECT contract_id FROM delivery_contracts ORDER BY contract_id')]
