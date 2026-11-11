import json
from hangar_ingest.types import digest, fail, identity, integer
from hangar_ingest.runs import batches, journal, partitions
from hangar_ingest.runs.processor import verify_checkpoint
from hangar_ingest.quality import recalibration, review


def overview(db):
    batch_counts = db.all('SELECT state,COUNT(*) AS count FROM batches GROUP BY state ORDER BY state')
    partition_counts = db.all('SELECT state,COUNT(*) AS count FROM partitions GROUP BY state ORDER BY state')
    return {
        'devices': db.scalar('SELECT COUNT(*) FROM devices'),
        'observations': db.scalar('SELECT COUNT(*) FROM observations'),
        'work_orders': db.scalar('SELECT COUNT(*) FROM work_orders'),
        'batches': {row['state']: row['count'] for row in batch_counts},
        'partitions': {row['state']: row['count'] for row in partition_counts},
        'quarantine': review.reason_counts(db),
        'stale_calibrations': len(recalibration.stale_observations(db)),
        'releases': db.scalar("SELECT COUNT(*) FROM releases WHERE state='committed'"),
    }


def check(db):
    errors = []
    with db.transaction():
        database = db.integrity()
        chain = journal.verify(db)
        if not database['ok']:
            errors.append({'code': 'database_integrity', 'detail': database})
        if not chain['ok']:
            errors.append({'code': 'journal_integrity', 'detail': chain})
        for batch in batches.listing(db):
            result = verify_checkpoint(db, batch['batch_id'])
            if not result['ok']:
                errors.append({'code': 'checkpoint_integrity', 'detail': result})
        for row in db.all('SELECT * FROM partition_versions ORDER BY partition_key,revision'):
            try:
                value = json.loads(row['payload'])
            except ValueError:
                errors.append({'code': 'partition_json', 'partition_key': row['partition_key'], 'revision': row['revision']})
                continue
            if digest(value) != row['digest']:
                errors.append({'code': 'partition_digest', 'partition_key': row['partition_key'], 'revision': row['revision']})
        for row in db.all('SELECT * FROM observations ORDER BY observation_id'):
            raw = db.one('SELECT state FROM raw_rows WHERE batch_id=? AND row_number=?', (row['batch_id'], row['row_number']))
            if raw is None or raw['state'] not in {'accepted', 'replayed'}:
                errors.append({'code': 'observation_source', 'observation_id': row['observation_id']})
            device = db.one('SELECT aircraft FROM devices WHERE device_id=?', (row['device_id'],))
            expected_key = partitions.key(device['aircraft'], row['event_time'][:10])
            if db.one('SELECT partition_key FROM partitions WHERE partition_key=?', (expected_key,)) is None:
                errors.append({'code': 'observation_partition', 'observation_id': row['observation_id']})
        return {'ok': not errors, 'errors': errors, 'database': database, 'journal': chain}


def observation_trace(db, observation_id):
    row = db.one('SELECT * FROM observations WHERE observation_id=?', (observation_id,))
    if row is None:
        fail('unknown_observation', 'Observation does not exist')
    row['quality'] = json.loads(row['quality'])
    raw = db.one('SELECT * FROM raw_rows WHERE batch_id=? AND row_number=?', (row['batch_id'], row['row_number']))
    raw['payload'] = json.loads(raw['payload'])
    raw['error'] = json.loads(raw['error']) if raw['error'] else None
    calibration = db.one('SELECT * FROM calibrations WHERE calibration_id=?', (row['calibration_id'],)) if row['calibration_id'] else None
    return {
        'observation': row,
        'raw': raw,
        'batch': batches.get(db, row['batch_id']),
        'current_calibration': calibration,
        'calibration_stale': bool(calibration and calibration['revision'] != row['calibration_revision']),
    }


def batch_report(db, batch_id):
    batch = batches.get(db, batch_id)
    states = db.all('SELECT state,COUNT(*) AS count FROM raw_rows WHERE batch_id=? GROUP BY state ORDER BY state', (batch_id,))
    devices = db.all('SELECT device_id,COUNT(*) AS count,MIN(event_time) AS first,MAX(event_time) AS last FROM observations WHERE batch_id=? GROUP BY device_id ORDER BY device_id', (batch_id,))
    return {
        'batch': batch,
        'row_states': {row['state']: row['count'] for row in states},
        'devices': devices,
        'checkpoint': verify_checkpoint(db, batch_id),
        'quarantine': review.listing(db, batch_id=batch_id, limit=10000),
    }


def lineage(db, release_id):
    from hangar_ingest.exports.releases import payloads
    package = payloads(db, release_id)
    observation_ids = {row['observation_id'] for partition in package['partitions'] for row in partition['payload']['observations']}
    batches_used = set()
    calibrations_used = set()
    for partition in package['partitions']:
        for row in partition['payload']['observations']:
            if row['calibration_id']:
                calibrations_used.add((row['calibration_id'], row['calibration_revision']))
    for observation_id in sorted(observation_ids):
        row = db.one('SELECT batch_id FROM observations WHERE observation_id=?', (observation_id,))
        if row:
            batches_used.add(row['batch_id'])
    return {
        'release_id': release_id,
        'digest': package['digest'],
        'observations': len(observation_ids),
        'batches': [batches.get(db, batch_id) for batch_id in sorted(batches_used)],
        'calibrations': [{'calibration_id': key, 'revision': rev} for key, rev in sorted(calibrations_used)],
    }
