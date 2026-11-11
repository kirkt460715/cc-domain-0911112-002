import json
from hangar_ingest.types import canonical, digest, fail, identity, instant, integer, object_fields
from hangar_ingest.runs import journal

SCHEMA = '''
CREATE TABLE IF NOT EXISTS source_manifests(
 manifest_id TEXT PRIMARY KEY,source TEXT NOT NULL,start TEXT NOT NULL,end TEXT NOT NULL,
 revision INTEGER NOT NULL,payload TEXT NOT NULL,digest TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS source_reconciliations(
 reconciliation_id TEXT PRIMARY KEY,manifest_id TEXT NOT NULL REFERENCES source_manifests(manifest_id),
 manifest_revision INTEGER NOT NULL,payload TEXT NOT NULL,matched INTEGER NOT NULL,created_at TEXT NOT NULL);
'''


class SourceReconciliation:
    def __init__(self, db):
        self.db = db
        if db.depth:
            fail('transaction_scope', 'Initialize source reconciliation outside transaction')
        db.db.executescript(SCHEMA)

    def register(self, manifest_id, source, start, end, events, *, revision=1):
        identity(manifest_id, 'manifest_id')
        identity(source, 'source')
        start = instant(start)
        end = instant(end)
        integer(revision, 'revision', 1)
        if start >= end:
            fail('manifest_interval', 'Manifest interval must be nonempty')
        if not isinstance(events, list) or len(events) > 100000:
            fail('manifest_events', 'Manifest event list exceeds supported size')
        normalized = []
        seen = set()
        for event in events:
            object_fields(event, {'event_id', 'observed_at', 'raw_hash'})
            event_id = identity(event['event_id'], 'event_id')
            observed_at = instant(event['observed_at'])
            if not start <= observed_at < end:
                fail('manifest_event_time', 'Manifest event lies outside declared interval')
            if event_id in seen:
                fail('manifest_duplicate', 'Manifest event identifiers must be unique')
            seen.add(event_id)
            checksum = event['raw_hash']
            if not isinstance(checksum, str) or len(checksum) != 64 or any(char not in '0123456789abcdef' for char in checksum):
                fail('manifest_hash', 'Raw hash must be lowercase SHA256')
            normalized.append({'event_id': event_id, 'observed_at': observed_at, 'raw_hash': checksum})
        normalized.sort(key=lambda event: event['event_id'])
        payload = {'manifest_id': manifest_id, 'source': source, 'start': start, 'end': end, 'revision': revision, 'events': normalized}
        checksum = digest(payload)
        with self.db.transaction():
            old = self.db.one('SELECT * FROM source_manifests WHERE manifest_id=?', (manifest_id,))
            if old:
                if old['digest'] == checksum:
                    return self.get(manifest_id)
                if revision != old['revision'] + 1:
                    fail('stale_revision', 'Manifest revisions must be consecutive')
                if old['source'] != source or old['start'] != start or old['end'] != end:
                    fail('manifest_identity', 'Manifest source and interval are immutable')
            elif revision != 1:
                fail('manifest_revision', 'Manifest starts at revision one')
            self.db.execute(
                'INSERT INTO source_manifests(manifest_id,source,start,end,revision,payload,digest,created_at) VALUES(?,?,?,?,?,?,?,?) '
                'ON CONFLICT(manifest_id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload,digest=excluded.digest',
                (manifest_id, source, start, end, revision, canonical(payload), checksum, self.db.clock()),
            )
            journal.record(self.db, 'source_manifest.registered', {'manifest_id': manifest_id, 'revision': revision, 'digest': checksum})
            return self.get(manifest_id)

    def get(self, manifest_id):
        row = self.db.one('SELECT * FROM source_manifests WHERE manifest_id=?', (manifest_id,))
        if row is None:
            fail('unknown_manifest', 'Source manifest does not exist')
        row['payload'] = json.loads(row['payload'])
        if digest(row['payload']) != row['digest']:
            fail('manifest_corrupt', 'Source manifest checksum mismatch')
        return row

    def reconcile(self, manifest_id):
        with self.db.transaction():
            manifest = self.get(manifest_id)
            expected = {row['event_id']: row for row in manifest['payload']['events']}
            actual = self.db.all(
                'SELECT external_event,raw_time,raw_hash,observation_id FROM observations WHERE source=? AND raw_time>=? AND raw_time<? ORDER BY external_event',
                (manifest['source'], manifest['start'], manifest['end']),
            )
            observed = {row['external_event']: row for row in actual}
            missing = sorted(set(expected) - set(observed))
            unexpected = sorted(set(observed) - set(expected))
            mismatches = []
            for event_id in sorted(set(expected) & set(observed)):
                left = expected[event_id]
                right = observed[event_id]
                if left['raw_hash'] != right['raw_hash'] or left['observed_at'] != right['raw_time']:
                    mismatches.append({'event_id': event_id, 'expected': left, 'actual': right})
            report = {
                'manifest_id': manifest_id,
                'manifest_revision': manifest['revision'],
                'manifest_digest': manifest['digest'],
                'expected_count': len(expected),
                'observed_count': len(observed),
                'missing': missing,
                'unexpected': unexpected,
                'mismatches': mismatches,
                'matched': not missing and not unexpected and not mismatches,
            }
            reconciliation_id = digest(report)
            self.db.execute(
                'INSERT OR IGNORE INTO source_reconciliations(reconciliation_id,manifest_id,manifest_revision,payload,matched,created_at) VALUES(?,?,?,?,?,?)',
                (reconciliation_id, manifest_id, manifest['revision'], canonical(report), int(report['matched']), self.db.clock()),
            )
            return {'reconciliation_id': reconciliation_id, **report}

    def report(self, reconciliation_id):
        row = self.db.one('SELECT payload FROM source_reconciliations WHERE reconciliation_id=?', (reconciliation_id,))
        if row is None:
            fail('unknown_reconciliation', 'Reconciliation report does not exist')
        payload = json.loads(row['payload'])
        if digest(payload) != reconciliation_id:
            fail('reconciliation_corrupt', 'Reconciliation digest mismatch')
        return {'reconciliation_id': reconciliation_id, **payload}

    def reports(self, manifest_id):
        self.get(manifest_id)
        rows = self.db.all('SELECT reconciliation_id FROM source_reconciliations WHERE manifest_id=? ORDER BY created_at,reconciliation_id', (manifest_id,))
        return [self.report(row['reconciliation_id']) for row in rows]

    def seal(self, manifest_id, watermark_revision):
        from hangar_ingest.exports.watermarks import advance
        with self.db.transaction():
            manifest = self.get(manifest_id)
            result = self.reconcile(manifest_id)
            if not result['matched']:
                fail('source_unreconciled', 'Cannot seal an unreconciled source interval', report=result)
            pending = self.db.scalar("SELECT COUNT(*) FROM batches WHERE source=? AND state IN ('staged','processing','paused')", (manifest['source'],))
            if pending:
                fail('source_pending', 'Source still has pending batches')
            watermark = advance(self.db, manifest['source'], manifest['end'], watermark_revision)
            journal.record(self.db, 'source_manifest.sealed', {'manifest_id': manifest_id, 'report': result['reconciliation_id']})
            return {'report': result, 'watermark': watermark}
