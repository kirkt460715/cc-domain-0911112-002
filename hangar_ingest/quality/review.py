import json
from hangar_ingest.types import canonical, fail, identity, integer, text
from hangar_ingest.decoders.sensors import normalize
from hangar_ingest.quality.normalize import observation, duplicate_decision
from hangar_ingest.runs import batches, journal
from hangar_ingest.runs.processor import persist


def get(db, quarantine_id):
    row = db.one('SELECT * FROM quarantine WHERE quarantine_id=?', (quarantine_id,))
    if row is None:
        fail('unknown_quarantine', 'Quarantine record does not exist')
    row['reasons'] = json.loads(row['reasons'])
    row['payload'] = json.loads(row['payload'])
    row['decision'] = json.loads(row['decision']) if row['decision'] else None
    return row


def listing(db, *, status='open', batch_id=None, limit=100):
    integer(limit, 'limit', 1, 10000)
    if status not in {'open', 'replayed', 'dismissed'}:
        fail('quarantine_state', 'Unknown quarantine status')
    params = [status]
    clause = 'status=?'
    if batch_id is not None:
        identity(batch_id, 'batch_id')
        clause += ' AND batch_id=?'
        params.append(batch_id)
    params.append(limit)
    rows = db.all(f'SELECT quarantine_id FROM quarantine WHERE {clause} ORDER BY batch_id,row_number LIMIT ?', params)
    return [get(db, row['quarantine_id']) for row in rows]


def replay(db, quarantine_id, revision, corrected, reason):
    integer(revision, 'revision', 1)
    reason = text(reason, 'reason', 2000)
    payload = normalize(corrected)
    with db.transaction():
        previous = get(db, quarantine_id)
        if previous['revision'] != revision:
            fail('stale_revision', 'Quarantine review changed')
        if previous['status'] != 'open':
            fail('quarantine_state', 'Only open quarantine can be replayed')
        batch = batches.get(db, previous['batch_id'])
        duplicate = duplicate_decision(db, batch['source'], payload)
        if duplicate and duplicate['status'] == 'ignored_conflict':
            fail('replay_conflict', 'Corrected record still conflicts with retained observation')
        if duplicate is None:
            normalized = observation(db, batch['source'], payload)
            persist(db, previous['batch_id'], previous['row_number'], normalized)
            observation_id = normalized['observation_id']
        else:
            observation_id = duplicate['observation_id']
        decision = {'reason': reason, 'corrected': payload, 'observation_id': observation_id}
        db.execute(
            "UPDATE quarantine SET status='replayed',revision=revision+1,decision=?,updated_at=? WHERE quarantine_id=?",
            (canonical(decision), db.clock(), quarantine_id),
        )
        db.execute(
            "UPDATE raw_rows SET state='replayed',error=NULL WHERE batch_id=? AND row_number=?",
            (previous['batch_id'], previous['row_number']),
        )
        rejected = batch['rejected'] - 1
        state = batch['state']
        if state == 'partial' and rejected == 0:
            state = 'complete'
        db.execute(
            'UPDATE batches SET accepted=accepted+1,rejected=?,state=?,revision=revision+1,updated_at=? WHERE batch_id=?',
            (rejected, state, db.clock(), previous['batch_id']),
        )
        journal.record(db, 'quarantine.replayed', {'quarantine_id': quarantine_id, **decision})
        return get(db, quarantine_id)


def dismiss(db, quarantine_id, revision, reason):
    integer(revision, 'revision', 1)
    reason = text(reason, 'reason', 2000)
    with db.transaction():
        previous = get(db, quarantine_id)
        if previous['revision'] != revision:
            fail('stale_revision', 'Quarantine review changed')
        if previous['status'] != 'open':
            fail('quarantine_state', 'Only open quarantine can be dismissed')
        db.execute(
            "UPDATE quarantine SET status='dismissed',revision=revision+1,decision=?,updated_at=? WHERE quarantine_id=?",
            (canonical({'reason': reason}), db.clock(), quarantine_id),
        )
        journal.record(db, 'quarantine.dismissed', {'quarantine_id': quarantine_id, 'reason': reason})
        return get(db, quarantine_id)


def reason_counts(db):
    counts = {}
    for row in db.all("SELECT reasons FROM quarantine WHERE status='open'"):
        reason = json.loads(row['reasons'])
        code = reason.get('code', 'unknown')
        counts[code] = counts.get(code, 0) + 1
    return dict(sorted(counts.items()))
