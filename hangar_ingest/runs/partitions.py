import datetime as dt
import json

from hangar_ingest.types import canonical, digest, fail, identity, instant, integer
from hangar_ingest.runs import journal


def key(aircraft, day):
    identity(aircraft, 'aircraft')
    try:
        parsed = dt.date.fromisoformat(day)
    except (TypeError, ValueError) as exc:
        raise ValueError('Partition day must be ISO date') from exc
    return aircraft + '/' + parsed.isoformat()


def split(partition_key):
    try:
        aircraft, day = partition_key.split('/')
        if key(aircraft, day) != partition_key:
            fail('partition_key', 'Noncanonical partition key')
    except (ValueError, TypeError) as exc:
        from hangar_ingest.types import PipelineError
        raise PipelineError('partition_key', 'Invalid partition key') from exc
    return aircraft, day


def day_bounds(day):
    parsed = dt.date.fromisoformat(day)
    start = parsed.isoformat() + 'T00:00:00.000000Z'
    end = (parsed + dt.timedelta(days=1)).isoformat() + 'T00:00:00.000000Z'
    return start, end


def dirty(db, aircraft, day, reason):
    partition_key = key(aircraft, day)
    db.execute(
        "INSERT INTO partitions(partition_key,revision,state,dirty_reason,updated_at) VALUES(?,0,'dirty',?,?) "
        "ON CONFLICT(partition_key) DO UPDATE SET state='dirty',dirty_reason=excluded.dirty_reason,updated_at=excluded.updated_at",
        (partition_key, reason, db.clock()),
    )
    return partition_key


def dirty_interval(db, aircraft, start, end, reason):
    start = instant(start)
    end = instant(end)
    if start >= end:
        fail('partition_interval', 'Dirty interval must be increasing')
    first = dt.date.fromisoformat(start[:10])
    last_point = dt.datetime.fromisoformat(end.replace('Z', '+00:00')) - dt.timedelta(microseconds=1)
    last = last_point.date()
    if (last - first).days > 366:
        fail('partition_span', 'Interval spans more than one year')
    result = []
    day = first
    while day <= last:
        result.append(dirty(db, aircraft, day.isoformat(), reason))
        day += dt.timedelta(days=1)
    return result


def get(db, partition_key):
    split(partition_key)
    row = db.one('SELECT * FROM partitions WHERE partition_key=?', (partition_key,))
    if row is None:
        fail('unknown_partition', 'Partition does not exist')
    return row


def listing(db, state=None):
    if state is None:
        return db.all('SELECT * FROM partitions ORDER BY partition_key')
    if state not in {'dirty', 'ready'}:
        fail('partition_state', 'Unknown partition state')
    return db.all('SELECT * FROM partitions WHERE state=? ORDER BY partition_key', (state,))


def materialize(db, partition_key, *, expected_revision=None):
    from hangar_ingest.runs.windows import build_day
    with db.transaction():
        previous = get(db, partition_key)
        if expected_revision is not None and previous['revision'] != expected_revision:
            fail('stale_revision', 'Partition revision changed')
        if previous['state'] == 'ready':
            return version(db, partition_key, previous['revision'])
        aircraft, day = split(partition_key)
        payload = build_day(db, aircraft, day)
        checksum = digest(payload)
        next_revision = previous['revision'] + 1
        db.execute(
            'INSERT INTO partition_versions(partition_key,revision,payload,digest,created_at) VALUES(?,?,?,?,?)',
            (partition_key, next_revision, canonical(payload), checksum, db.clock()),
        )
        db.execute(
            "UPDATE partitions SET revision=?,state='ready',dirty_reason=NULL,updated_at=? WHERE partition_key=?",
            (next_revision, db.clock(), partition_key),
        )
        journal.record(db, 'partition.materialized', {'partition_key': partition_key, 'revision': next_revision, 'digest': checksum})
        return version(db, partition_key, next_revision)


def version(db, partition_key, revision=None):
    row = get(db, partition_key)
    revision = row['revision'] if revision is None else integer(revision, 'revision', 1)
    result = db.one('SELECT * FROM partition_versions WHERE partition_key=? AND revision=?', (partition_key, revision))
    if result is None:
        fail('unknown_partition_version', 'Partition version does not exist')
    result['payload'] = json.loads(result['payload'])
    if digest(result['payload']) != result['digest']:
        fail('partition_corrupt', 'Partition digest does not match content')
    return result


def rebuild_dirty(db, limit=100):
    integer(limit, 'limit', 1, 10000)
    selected = listing(db, 'dirty')[:limit]
    return [materialize(db, row['partition_key']) for row in selected]


def difference(db, partition_key, before, after):
    left = version(db, partition_key, before)
    right = version(db, partition_key, after)
    old = {row['observation_id']: row for row in left['payload']['observations']}
    new = {row['observation_id']: row for row in right['payload']['observations']}
    changes = []
    for observation_id in sorted(set(old) | set(new)):
        if old.get(observation_id) != new.get(observation_id):
            changes.append({'observation_id': observation_id, 'before': old.get(observation_id), 'after': new.get(observation_id)})
    return {'partition_key': partition_key, 'before': before, 'after': after, 'changes': changes, 'windows_changed': left['payload']['windows'] != right['payload']['windows']}
