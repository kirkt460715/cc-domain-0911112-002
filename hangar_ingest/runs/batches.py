import hashlib
import json

from hangar_ingest.types import canonical, digest, fail, identity, integer, text
from hangar_ingest.decoders.sensors import records
from hangar_ingest.runs import journal


def stage(db, batch_id, source, data, format='ndjson'):
    identity(batch_id, 'batch_id')
    identity(source, 'source')
    if not isinstance(data, bytes) or len(data) > 64 * 1024 * 1024:
        fail('input_size', 'Batch input must be bounded bytes')
    checksum = hashlib.sha256(data).hexdigest()
    with db.transaction():
        existing = db.one('SELECT * FROM batches WHERE batch_id=?', (batch_id,))
        if existing:
            if (existing['source'], existing['content_hash'], existing['format']) == (source, checksum, format):
                return {**existing, 'duplicate': True}
            fail('batch_conflict', 'Batch identifier already has different content')
        duplicate = db.one('SELECT * FROM batches WHERE source=? AND content_hash=? AND format=?', (source, checksum, format))
        if duplicate:
            return {**duplicate, 'duplicate': True}
        now = db.clock()
        db.execute(
            "INSERT INTO batches(batch_id,source,content_hash,format,state,revision,cursor,total,accepted,rejected,created_at,updated_at) "
            "VALUES(?,?,?,?,'staged',1,0,0,0,0,?,?)", (batch_id, source, checksum, format, now, now),
        )
        count = 0
        for row_number, payload, error in records(data, format):
            count += 1
            db.execute(
                'INSERT INTO raw_rows(batch_id,row_number,payload,raw_hash,state,error) VALUES(?,?,?,?,?,?)',
                (batch_id, row_number, canonical(payload), digest(payload), 'invalid' if error else 'pending', canonical(error) if error else None),
            )
        db.execute('UPDATE batches SET total=? WHERE batch_id=?', (count, batch_id))
        journal.record(db, 'batch.staged', {'batch_id': batch_id, 'source': source, 'records': count, 'sha256': checksum})
        return get(db, batch_id)


def get(db, batch_id):
    identity(batch_id, 'batch_id')
    row = db.one('SELECT * FROM batches WHERE batch_id=?', (batch_id,))
    if row is None:
        fail('unknown_batch', 'Batch does not exist', batch_id=batch_id)
    return row


def rows(db, batch_id, *, after=0, limit=100):
    get(db, batch_id)
    integer(after, 'after')
    integer(limit, 'limit', 1, 10000)
    result = db.all('SELECT * FROM raw_rows WHERE batch_id=? AND row_number>? ORDER BY row_number LIMIT ?', (batch_id, after, limit))
    for row in result:
        row['payload'] = json.loads(row['payload'])
        row['error'] = json.loads(row['error']) if row['error'] else None
    return result


def cancel(db, batch_id, revision, reason):
    integer(revision, 'revision', 1)
    reason = text(reason, 'reason')
    with db.transaction():
        batch = get(db, batch_id)
        if batch['revision'] != revision:
            fail('stale_revision', 'Batch revision changed')
        if batch['state'] not in {'staged', 'processing', 'paused'}:
            fail('batch_state', 'Batch cannot be cancelled in current state')
        db.execute("UPDATE batches SET state='cancelled',revision=revision+1,updated_at=? WHERE batch_id=?", (db.clock(), batch_id))
        journal.record(db, 'batch.cancelled', {'batch_id': batch_id, 'reason': reason})
        return get(db, batch_id)


def pause(db, batch_id, revision):
    with db.transaction():
        batch = get(db, batch_id)
        if batch['revision'] != revision:
            fail('stale_revision', 'Batch revision changed')
        if batch['state'] not in {'staged', 'processing'}:
            fail('batch_state', 'Only active batch can be paused')
        db.execute("UPDATE batches SET state='paused',revision=revision+1,updated_at=? WHERE batch_id=?", (db.clock(), batch_id))
        journal.record(db, 'batch.paused', {'batch_id': batch_id})
        return get(db, batch_id)


def resume(db, batch_id, revision):
    with db.transaction():
        batch = get(db, batch_id)
        if batch['revision'] != revision:
            fail('stale_revision', 'Batch revision changed')
        if batch['state'] != 'paused':
            fail('batch_state', 'Only paused batch can resume')
        db.execute("UPDATE batches SET state='processing',revision=revision+1,updated_at=? WHERE batch_id=?", (db.clock(), batch_id))
        journal.record(db, 'batch.resumed', {'batch_id': batch_id})
        return get(db, batch_id)


def listing(db, state=None):
    allowed = {'staged', 'processing', 'paused', 'complete', 'partial', 'cancelled'}
    if state is not None and state not in allowed:
        fail('batch_state', 'Unknown batch state')
    if state:
        return db.all('SELECT * FROM batches WHERE state=? ORDER BY created_at,batch_id', (state,))
    return db.all('SELECT * FROM batches ORDER BY created_at,batch_id')
