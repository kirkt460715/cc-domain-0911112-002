import json
from decimal import Decimal
from hangar_ingest.types import canonical, digest, fail, identity, instant, integer
from hangar_ingest.runs import journal, partitions
from hangar_ingest.quality import devices, rules


def dirty_observation(db, device_id, event_time, reason):
    config = devices.device(db, device_id)
    affected = {partitions.dirty(db, config['aircraft'], event_time[:10], reason)}
    orders = db.all(
        "SELECT opened_at,closed_at FROM work_orders WHERE aircraft=? AND component=? AND state!='cancelled' "
        'AND opened_at<=? AND closed_at>?',
        (config['aircraft'], config['component'], event_time, event_time),
    )
    for order in orders:
        affected.update(partitions.dirty_interval(db, config['aircraft'], order['opened_at'], order['closed_at'], reason))
    return sorted(affected)


def assess_device(db, device_id):
    config = devices.device(db, device_id)
    rule = rules.get(db, device_id)
    rows = db.all('SELECT * FROM observations WHERE device_id=? ORDER BY event_time,observation_id', (device_id,))
    outcomes = []
    for row in rows:
        issues = rules.bounds(row['value'], rule)
        issues.extend(rules.neighbor_checks(db, device_id, row['event_time'], row['value'], rule, exclude=row['observation_id']))
        assessment = rules.summarize(issues)
        outcomes.append({'observation_id': row['observation_id'], 'event_time': row['event_time'], **assessment})
    return {
        'device_id': device_id,
        'rule_revision': rule['revision'],
        'observations': outcomes,
        'violations': sum(not row['accepted'] for row in outcomes),
        'warnings': sum(len(row['warnings']) for row in outcomes),
    }


def refresh_warnings(db, device_id, expected_rule_revision):
    integer(expected_rule_revision, 'expected_rule_revision')
    with db.transaction():
        assessment = assess_device(db, device_id)
        if assessment['rule_revision'] != expected_rule_revision:
            fail('stale_revision', 'Quality rules changed')
        if assessment['violations']:
            fail('quality_history', 'Existing observations violate revised rules; explicit source correction is required', assessment=assessment)
        changed = []
        for outcome in assessment['observations']:
            row = db.one('SELECT quality FROM observations WHERE observation_id=?', (outcome['observation_id'],))
            new_quality = canonical(outcome['warnings'])
            if row['quality'] == new_quality:
                continue
            db.execute('UPDATE observations SET quality=? WHERE observation_id=?', (new_quality, outcome['observation_id']))
            dirty_observation(db, device_id, outcome['event_time'], 'quality_recomputed')
            changed.append(outcome['observation_id'])
        journal.record(db, 'quality.refreshed', {'device_id': device_id, 'rule_revision': expected_rule_revision, 'changed': changed})
        return {'changed': changed, 'assessment': assessment}


def rebuild_plan(db, aircraft=None):
    if aircraft is not None:
        identity(aircraft, 'aircraft')
    result = []
    for partition in partitions.listing(db, 'dirty'):
        current_aircraft, day = partitions.split(partition['partition_key'])
        if aircraft and current_aircraft != aircraft:
            continue
        start, end = partitions.day_bounds(day)
        observations = db.scalar(
            'SELECT COUNT(*) FROM observations o JOIN devices d ON d.device_id=o.device_id '
            'WHERE d.aircraft=? AND o.event_time>=? AND o.event_time<?', (current_aircraft, start, end),
        )
        orders = db.scalar(
            "SELECT COUNT(*) FROM work_orders WHERE aircraft=? AND state!='cancelled' AND opened_at<? AND closed_at>?",
            (current_aircraft, end, start),
        )
        result.append({
            'partition_key': partition['partition_key'],
            'current_revision': partition['revision'],
            'reason': partition['dirty_reason'],
            'observations': observations,
            'work_order_windows': orders,
            'estimated_weight': observations + orders * 10,
        })
    result.sort(key=lambda row: (row['estimated_weight'], row['partition_key']))
    return result


def rebuild_selected(db, plan):
    if not isinstance(plan, list) or len(plan) > 1000:
        fail('rebuild_plan', 'Rebuild plan must be a bounded list')
    with db.transaction():
        selected = set()
        for row in plan:
            if row['partition_key'] in selected:
                fail('rebuild_duplicate', 'Partition appears twice in rebuild plan')
            selected.add(row['partition_key'])
            current = partitions.get(db, row['partition_key'])
            if current['revision'] != row['current_revision'] or current['state'] != 'dirty':
                fail('rebuild_stale', 'Rebuild plan is stale', partition_key=row['partition_key'])
        return [partitions.materialize(db, row['partition_key'], expected_revision=row['current_revision']) for row in plan]


def explain_partition(db, partition_key):
    row = partitions.get(db, partition_key)
    aircraft, day = partitions.split(partition_key)
    start, end = partitions.day_bounds(day)
    origins = db.all(
        'SELECT o.batch_id,o.device_id,COUNT(*) AS observations,MIN(o.event_time) AS first,MAX(o.event_time) AS last '
        'FROM observations o JOIN devices d ON d.device_id=o.device_id WHERE d.aircraft=? AND o.event_time>=? AND o.event_time<? '
        'GROUP BY o.batch_id,o.device_id ORDER BY o.batch_id,o.device_id', (aircraft, start, end),
    )
    versions = db.all('SELECT revision,digest,created_at FROM partition_versions WHERE partition_key=? ORDER BY revision', (partition_key,))
    consumers = []
    for release in db.all('SELECT release_id,state,manifest FROM releases ORDER BY release_id'):
        manifest = json.loads(release['manifest'])
        for entry in manifest['partitions']:
            if entry['partition_key'] == partition_key:
                consumers.append({'release_id': release['release_id'], 'state': release['state'], 'revision': entry['revision']})
    return {'partition': row, 'origins': origins, 'versions': versions, 'releases': consumers}


def calibration_impact(db, calibration_id):
    calibration = db.one('SELECT * FROM calibrations WHERE calibration_id=?', (calibration_id,))
    if calibration is None:
        fail('unknown_calibration', 'Calibration does not exist')
    observations = db.all('SELECT observation_id,event_time,calibration_revision FROM observations WHERE calibration_id=? ORDER BY event_time', (calibration_id,))
    device = devices.device(db, calibration['device_id'])
    keys = sorted({partitions.key(device['aircraft'], row['event_time'][:10]) for row in observations})
    references = []
    for release in db.all("SELECT release_id,manifest FROM releases WHERE state='committed' ORDER BY release_id"):
        for entry in json.loads(release['manifest'])['partitions']:
            payload = partitions.version(db, entry['partition_key'], entry['revision'])['payload']
            affected = [row['observation_id'] for row in payload['observations'] if row['calibration_id'] == calibration_id]
            if affected:
                references.append({'release_id': release['release_id'], 'partition_key': entry['partition_key'], 'observation_ids': affected})
    return {'calibration': calibration, 'observations': observations, 'current_partitions': keys, 'historical_release_references': references}
