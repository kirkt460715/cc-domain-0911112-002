import json

from hangar_ingest.types import canonical, digest, identity, integer


def record(db, action, details):
    identity(action, 'action')
    previous = db.scalar('SELECT entry_hash FROM audit ORDER BY position DESC LIMIT 1') or '0' * 64
    at = db.clock()
    body = canonical(details)
    checksum = digest([previous, action, body, at])
    cursor = db.execute(
        'INSERT INTO audit(action,details,at,previous_hash,entry_hash) VALUES(?,?,?,?,?)',
        (action, body, at, previous, checksum),
    )
    return cursor.lastrowid


def verify(db):
    previous = '0' * 64
    count = 0
    for row in db.execute('SELECT * FROM audit ORDER BY position'):
        expected = digest([previous, row['action'], row['details'], row['at']])
        if row['previous_hash'] != previous or row['entry_hash'] != expected:
            return {'ok': False, 'broken_at': row['position'], 'count': count}
        previous = expected
        count += 1
    return {'ok': True, 'count': count, 'head': previous}


def page(db, after=0, limit=100):
    integer(after, 'after')
    integer(limit, 'limit', 1, 1000)
    rows = db.all('SELECT * FROM audit WHERE position>? ORDER BY position LIMIT ?', (after, limit))
    for row in rows:
        row['details'] = json.loads(row['details'])
    return rows
