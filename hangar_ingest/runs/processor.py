import json

from hangar_ingest.types import PipelineError, canonical, digest, fail, integer
from hangar_ingest.quality.normalize import observation, duplicate_decision
from hangar_ingest.runs import batches, journal, partitions


def quarantine(db, batch_id, row_number, payload, error):
    quarantine_id = digest({'batch_id': batch_id, 'row_number': row_number})
    now = db.clock()
    db.execute(
        'INSERT INTO quarantine(quarantine_id,batch_id,row_number,reasons,payload,status,revision,created_at,updated_at) '
        "VALUES(?,?,?,?,?,'open',1,?,?) ON CONFLICT(batch_id,row_number) DO UPDATE SET "
        "reasons=excluded.reasons,payload=excluded.payload,status='open',revision=quarantine.revision+1,updated_at=excluded.updated_at",
        (quarantine_id, batch_id, row_number, canonical(error), canonical(payload), now, now),
    )
    return quarantine_id


def persist(db, batch_id, row_number, normalized):
    fields = [
        'observation_id', 'source', 'external_event', 'device_id', 'raw_time',
        'event_time', 'value', 'unit', 'raw_value', 'raw_unit',
        'calibration_id', 'calibration_revision', 'raw_hash',
    ]
    values = [normalized[field] for field in fields]
    db.execute(
        f"INSERT INTO observations({','.join(fields)},batch_id,row_number,quality,created_at) "
        f"VALUES({','.join('?' for _ in range(len(fields)+4))})",
        [*values, batch_id, row_number, canonical(normalized['quality']), db.clock()],
    )
    from hangar_ingest.runs.maintenance import dirty_observation
    dirty_observation(db, normalized['device_id'], normalized['event_time'], 'new_observation')
    journal.record(db, 'observation.accepted', {
        'observation_id': normalized['observation_id'],
        'batch_id': batch_id,
        'row_number': row_number,
        'calibration_id': normalized['calibration_id'],
        'calibration_revision': normalized['calibration_revision'],
        'rule_revision': normalized['rule_revision'],
    })


def process_row(db, batch, row):
    payload = row['payload']
    if row['state'] == 'invalid':
        error = row['error']
        quarantine(db, batch['batch_id'], row['row_number'], payload, error)
        return {'state': 'quarantined', 'accepted': False, 'error': error}
    try:
        duplicate = duplicate_decision(db, batch['source'], payload)
        if duplicate:
            return {'state': duplicate['status'], 'accepted': True, 'observation_id': duplicate['observation_id']}
        normalized = observation(db, batch['source'], payload)
        persist(db, batch['batch_id'], row['row_number'], normalized)
    except PipelineError as exc:
        error = exc.as_dict()
        quarantine(db, batch['batch_id'], row['row_number'], payload, error)
        return {'state': 'quarantined', 'accepted': False, 'error': error}
    return {'state': 'accepted', 'accepted': True, 'observation_id': normalized['observation_id']}


def process(db, batch_id, *, limit=100, expected_revision=None, claim=None):
    integer(limit, 'limit', 1, 10000)
    with db.transaction():
        batch = batches.get(db, batch_id)
        if expected_revision is not None and batch['revision'] != expected_revision:
            fail('stale_revision', 'Batch checkpoint changed')
        if claim is not None:
            from hangar_ingest.runs.claims import assert_owned
            assert_owned(db, batch_id, claim)
        if batch['state'] in {'complete', 'partial'}:
            return {'batch': batch, 'processed': [], 'finished': True}
        if batch['state'] not in {'staged', 'processing'}:
            fail('batch_state', 'Batch is not processable', state=batch['state'])
        selected = batches.rows(db, batch_id, after=batch['cursor'], limit=limit)
        accepted = batch['accepted']
        rejected = batch['rejected']
        cursor = batch['cursor']
        outcomes = []
        for row in selected:
            outcome = process_row(db, batch, row)
            accepted += int(outcome['accepted'])
            rejected += int(not outcome['accepted'])
            cursor = row['row_number']
            db.execute(
                'UPDATE raw_rows SET state=?,error=? WHERE batch_id=? AND row_number=?',
                (outcome['state'], canonical(outcome['error']) if 'error' in outcome else None, batch_id, cursor),
            )
            outcomes.append({'row_number': cursor, **outcome})
        remaining = db.scalar('SELECT COUNT(*) FROM raw_rows WHERE batch_id=? AND row_number>?', (batch_id, cursor))
        state = ('partial' if rejected else 'complete') if remaining == 0 else 'processing'
        db.execute(
            'UPDATE batches SET state=?,revision=revision+1,cursor=?,accepted=?,rejected=?,updated_at=? WHERE batch_id=?',
            (state, cursor, accepted, rejected, db.clock(), batch_id),
        )
        journal.record(db, 'batch.checkpoint', {'batch_id': batch_id, 'cursor': cursor, 'state': state, 'accepted': accepted, 'rejected': rejected})
        return {'batch': batches.get(db, batch_id), 'processed': outcomes, 'finished': remaining == 0}


def drain(db, batch_id, *, chunk=100):
    result = process(db, batch_id, limit=chunk)
    chunks = 1
    while not result['finished']:
        result = process(db, batch_id, limit=chunk, expected_revision=result['batch']['revision'])
        chunks += 1
    return {'batch': result['batch'], 'chunks': chunks}


def verify_checkpoint(db, batch_id):
    batch = batches.get(db, batch_id)
    rows = db.all('SELECT row_number,state FROM raw_rows WHERE batch_id=? ORDER BY row_number', (batch_id,))
    processed = [row for row in rows if row['row_number'] <= batch['cursor']]
    errors = []
    if len(rows) != batch['total']:
        errors.append('total_count')
    accepted = sum(row['state'] in {'accepted', 'duplicate', 'ignored_conflict', 'replayed'} for row in processed)
    rejected = sum(row['state'] == 'quarantined' for row in processed)
    if accepted != batch['accepted']:
        errors.append('accepted_count')
    if rejected != batch['rejected']:
        errors.append('rejected_count')
    if accepted + rejected != len(processed):
        errors.append('unprocessed_before_checkpoint')
    return {'ok': not errors, 'errors': errors, 'batch_id': batch_id, 'cursor': batch['cursor']}
