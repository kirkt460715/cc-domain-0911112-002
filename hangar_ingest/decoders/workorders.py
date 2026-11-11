import json

from hangar_ingest.types import canonical, digest, fail, identity, instant, integer, object_fields, text
from hangar_ingest.runs import journal

FIELDS = {'order_id', 'revision', 'aircraft', 'component', 'opened_at', 'closed_at', 'issue', 'resolution', 'state'}


def normalize(value):
    object_fields(value, FIELDS)
    start = instant(value['opened_at'], 'opened_at')
    end = instant(value['closed_at'], 'closed_at')
    if start >= end:
        fail('order_interval', 'Work order must have a nonempty interval')
    if value['state'] not in {'open', 'closed', 'cancelled'}:
        fail('order_state', 'Unknown work order state')
    return {
        'order_id': identity(value['order_id'], 'order_id'),
        'revision': integer(value['revision'], 'revision', 1),
        'aircraft': identity(value['aircraft'], 'aircraft'),
        'component': identity(value['component'], 'component'),
        'opened_at': start,
        'closed_at': end,
        'issue': text(value['issue'], 'issue', 2000),
        'resolution': text(value['resolution'], 'resolution', 2000),
        'state': value['state'],
    }


def upsert(db, value):
    row = normalize(value)
    checksum = digest(row)
    with db.transaction():
        previous = db.one('SELECT * FROM work_orders WHERE order_id=?', (row['order_id'],))
        if previous:
            if previous['revision'] == row['revision'] and previous['payload_hash'] == checksum:
                return {'status': 'duplicate', 'order_id': row['order_id'], 'revision': row['revision']}
            if row['revision'] != previous['revision'] + 1:
                fail('order_revision', 'Work order revision must advance exactly once')
            if row['aircraft'] != previous['aircraft'] or row['component'] != previous['component']:
                fail('order_identity', 'Aircraft and component are immutable')
            if previous['state'] == 'cancelled':
                fail('order_terminal', 'Cancelled work order is terminal')
        elif row['revision'] != 1:
            fail('order_revision', 'First work order revision must be one')
        columns = sorted(FIELDS)
        values = [row[column] for column in columns]
        updates = ','.join(f'{column}=excluded.{column}' for column in columns if column != 'order_id')
        db.execute(
            f"INSERT INTO work_orders({','.join(columns)},payload_hash,updated_at) VALUES({','.join('?' for _ in range(len(columns)+2))}) "
            f'ON CONFLICT(order_id) DO UPDATE SET {updates},payload_hash=excluded.payload_hash,updated_at=excluded.updated_at',
            [*values, checksum, db.clock()],
        )
        db.execute('INSERT INTO order_versions(order_id,revision,payload) VALUES(?,?,?)', (row['order_id'], row['revision'], canonical(row)))
        from hangar_ingest.runs.partitions import dirty_interval
        dirty_interval(db, row['aircraft'], row['opened_at'], row['closed_at'], 'work_order_changed')
        if previous:
            dirty_interval(db, previous['aircraft'], previous['opened_at'], previous['closed_at'], 'work_order_previous_interval')
        journal.record(db, 'order.updated', {'order_id': row['order_id'], 'revision': row['revision'], 'checksum': checksum})
        return {'status': 'updated' if previous else 'created', 'order_id': row['order_id'], 'revision': row['revision']}


def get(db, order_id, revision=None):
    identity(order_id, 'order_id')
    if revision is not None:
        integer(revision, 'revision', 1)
        row = db.one('SELECT payload FROM order_versions WHERE order_id=? AND revision=?', (order_id, revision))
        if row is None:
            fail('unknown_order', 'Work order revision does not exist')
        return json.loads(row['payload'])
    row = db.one('SELECT * FROM work_orders WHERE order_id=?', (order_id,))
    if row is None:
        fail('unknown_order', 'Work order does not exist')
    return row


def import_orders(db, values):
    if not isinstance(values, list) or len(values) > 10000:
        fail('orders_size', 'Work order import requires a bounded list')
    with db.transaction():
        return [upsert(db, row) for row in values]


def overlap(db, aircraft, component, start, end):
    start = instant(start)
    end = instant(end)
    if start >= end:
        fail('window_interval', 'Window must be nonempty')
    return db.all(
        "SELECT * FROM work_orders WHERE aircraft=? AND component=? AND state!='cancelled' "
        'AND opened_at<? AND closed_at>? ORDER BY opened_at,closed_at,order_id',
        (aircraft, component, end, start),
    )


def history(db, order_id):
    get(db, order_id)
    return [json.loads(row['payload']) for row in db.all('SELECT payload FROM order_versions WHERE order_id=? ORDER BY revision', (order_id,))]
