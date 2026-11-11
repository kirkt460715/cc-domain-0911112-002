from hangar_ingest.types import fail, from_micros, identity, integer, micros
from hangar_ingest.runs.batches import get
from hangar_ingest.runs import journal


def acquire(db, batch_id, owner, ttl=30):
    identity(owner, 'owner')
    integer(ttl, 'ttl', 1, 3600)
    with db.transaction():
        get(db, batch_id)
        previous = db.one('SELECT * FROM processing_claims WHERE batch_id=?', (batch_id,))
        now = db.clock()
        if previous and previous['expires_at'] > now:
            fail('claim_busy', 'Batch is already claimed', owner=previous['owner'])
        generation = previous['generation'] + 1 if previous else 1
        expiration = from_micros(micros(now) + ttl * 1000000)
        db.execute(
            'INSERT INTO processing_claims(batch_id,owner,generation,expires_at) VALUES(?,?,?,?) '
            'ON CONFLICT(batch_id) DO UPDATE SET owner=excluded.owner,generation=excluded.generation,expires_at=excluded.expires_at',
            (batch_id, owner, generation, expiration),
        )
        journal.record(db, 'batch.claimed', {'batch_id': batch_id, 'owner': owner, 'generation': generation})
        return {'batch_id': batch_id, 'owner': owner, 'generation': generation, 'expires_at': expiration}


def assert_owned(db, batch_id, claim):
    if not isinstance(claim, dict):
        fail('claim_invalid', 'Claim must be an object')
    row = db.one('SELECT * FROM processing_claims WHERE batch_id=?', (batch_id,))
    if row is None:
        fail('claim_missing', 'Batch has no claim')
    if claim.get('batch_id') != batch_id or claim.get('owner') != row['owner'] or claim.get('generation') != row['generation']:
        fail('claim_stale', 'Claim belongs to an earlier generation')
    if row['expires_at'] <= db.clock():
        fail('claim_expired', 'Claim has expired')
    return row


def renew(db, batch_id, claim, ttl=30):
    integer(ttl, 'ttl', 1, 3600)
    with db.transaction():
        row = assert_owned(db, batch_id, claim)
        expiration = from_micros(micros(db.clock()) + ttl * 1000000)
        db.execute('UPDATE processing_claims SET expires_at=? WHERE batch_id=?', (expiration, batch_id))
        return {**row, 'expires_at': expiration}


def release(db, batch_id, claim):
    with db.transaction():
        row = assert_owned(db, batch_id, claim)
        db.execute('UPDATE processing_claims SET expires_at=? WHERE batch_id=?', (db.clock(), batch_id))
        journal.record(db, 'batch.claim_released', {'batch_id': batch_id, 'generation': row['generation']})
        return {'released': True}
