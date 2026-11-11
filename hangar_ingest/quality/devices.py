from hangar_ingest.types import canonical, decimal_string, fail, identity, instant, integer, number, text
from hangar_ingest.quality import units
from hangar_ingest.runs import journal


def device(db, device_id):
    identity(device_id, 'device_id')
    row = db.one('SELECT * FROM devices WHERE device_id=?', (device_id,))
    if row is None:
        fail('unknown_device', 'Device is not registered', device_id=device_id)
    return row


def register(db, device_id, aircraft, component, metric, canonical_unit):
    for key, value in {'device_id': device_id, 'aircraft': aircraft, 'component': component, 'metric': metric}.items():
        identity(value, key)
    units.describe(canonical_unit)
    with db.transaction():
        existing = db.one('SELECT * FROM devices WHERE device_id=?', (device_id,))
        if existing:
            expected = (aircraft, component, metric, canonical_unit)
            actual = tuple(existing[key] for key in ('aircraft', 'component', 'metric', 'canonical_unit'))
            if actual != expected:
                fail('device_conflict', 'Device identifier has different immutable dimensions')
            return existing
        db.execute(
            'INSERT INTO devices(device_id,aircraft,component,metric,canonical_unit,revision,active,created_at) VALUES(?,?,?,?,?,1,1,?)',
            (device_id, aircraft, component, metric, canonical_unit, db.clock()),
        )
        journal.record(db, 'device.registered', {'device_id': device_id, 'aircraft': aircraft, 'component': component})
        return device(db, device_id)


def set_active(db, device_id, revision, active):
    integer(revision, 'revision', 1)
    if type(active) is not bool:
        fail('invalid_active', 'Active must be boolean')
    with db.transaction():
        row = device(db, device_id)
        if row['revision'] != revision:
            fail('stale_revision', 'Device revision changed')
        if bool(row['active']) == active:
            return row
        db.execute('UPDATE devices SET active=?,revision=revision+1 WHERE device_id=?', (int(active), device_id))
        journal.record(db, 'device.active', {'device_id': device_id, 'active': active})
        return device(db, device_id)


def add_alias(db, source, external_id, device_id, valid_from, valid_until):
    identity(source, 'source')
    identity(external_id, 'external_id')
    start = instant(valid_from)
    end = instant(valid_until)
    if start >= end:
        fail('alias_interval', 'Alias interval must be nonempty')
    with db.transaction():
        device(db, device_id)
        overlap = db.one(
            'SELECT * FROM aliases WHERE source=? AND external_id=? AND valid_from<? AND valid_until>?',
            (source, external_id, end, start),
        )
        if overlap:
            if (overlap['device_id'], overlap['valid_from'], overlap['valid_until']) == (device_id, start, end):
                return overlap
            fail('alias_overlap', 'Alias intervals may not overlap')
        db.execute(
            'INSERT INTO aliases(source,external_id,device_id,valid_from,valid_until) VALUES(?,?,?,?,?)',
            (source, external_id, device_id, start, end),
        )
        journal.record(db, 'alias.created', {'source': source, 'external_id': external_id, 'device_id': device_id, 'from': start, 'until': end})
        return {'source': source, 'external_id': external_id, 'device_id': device_id, 'valid_from': start, 'valid_until': end}


def resolve_alias(db, source, external_id, observed_at):
    observed_at = instant(observed_at)
    rows = db.all(
        'SELECT device_id FROM aliases WHERE source=? AND external_id=? AND valid_from<=? AND valid_until>?',
        (source, external_id, observed_at, observed_at),
    )
    if len(rows) != 1:
        fail('alias_unresolved', 'Device alias must resolve exactly once', source=source, external_id=external_id)
    resolved = device(db, rows[0]['device_id'])
    if not resolved['active']:
        fail('device_inactive', 'Device is inactive', device_id=resolved['device_id'])
    return resolved


def add_calibration(db, calibration_id, device_id, valid_from, valid_until, scale='1', offset='0', clock_offset_us=0):
    identity(calibration_id, 'calibration_id')
    start = instant(valid_from)
    end = instant(valid_until)
    if start >= end:
        fail('calibration_interval', 'Calibration interval must be nonempty')
    multiplier = number(scale, 'scale')
    if multiplier <= 0:
        fail('calibration_scale', 'Calibration scale must be positive')
    offset = number(offset, 'offset')
    integer(clock_offset_us, 'clock_offset_us', -86400000000, 86400000000)
    with db.transaction():
        device(db, device_id)
        existing = db.one('SELECT * FROM calibrations WHERE calibration_id=?', (calibration_id,))
        values = (device_id, start, end, decimal_string(multiplier), decimal_string(offset), clock_offset_us)
        if existing:
            actual = tuple(existing[key] for key in ('device_id', 'valid_from', 'valid_until', 'scale', 'offset', 'clock_offset_us'))
            if actual == values:
                return existing
            fail('calibration_conflict', 'Calibration identifier already used')
        overlap = db.one('SELECT calibration_id FROM calibrations WHERE device_id=? AND valid_from<? AND valid_until>?', (device_id, end, start))
        if overlap:
            fail('calibration_overlap', 'Calibration validity intervals overlap')
        db.execute(
            'INSERT INTO calibrations(calibration_id,device_id,valid_from,valid_until,scale,offset,clock_offset_us,revision,created_at) '
            'VALUES(?,?,?,?,?,?,?,1,?)', (calibration_id, *values, db.clock()),
        )
        journal.record(db, 'calibration.created', {'calibration_id': calibration_id, 'device_id': device_id})
        return db.one('SELECT * FROM calibrations WHERE calibration_id=?', (calibration_id,))


def calibration_at(db, device_id, raw_time):
    raw_time = instant(raw_time)
    return db.one(
        'SELECT * FROM calibrations WHERE device_id=? AND valid_from<=? AND valid_until>?',
        (device_id, raw_time, raw_time),
    )


def correct_calibration(db, calibration_id, revision, scale, offset, clock_offset_us):
    identity(calibration_id, 'calibration_id')
    integer(revision, 'revision', 1)
    scale = number(scale, 'scale')
    offset = number(offset, 'offset')
    if scale <= 0:
        fail('calibration_scale', 'Calibration scale must be positive')
    integer(clock_offset_us, 'clock_offset_us', -86400000000, 86400000000)
    with db.transaction():
        row = db.one('SELECT * FROM calibrations WHERE calibration_id=?', (calibration_id,))
        if row is None:
            fail('unknown_calibration', 'Calibration does not exist')
        if row['revision'] != revision:
            fail('stale_revision', 'Calibration revision changed')
        db.execute(
            'UPDATE calibrations SET scale=?,offset=?,clock_offset_us=?,revision=revision+1 WHERE calibration_id=?',
            (decimal_string(scale), decimal_string(offset), clock_offset_us, calibration_id),
        )
        journal.record(db, 'calibration.corrected', {'calibration_id': calibration_id, 'revision': revision + 1})
        return db.one('SELECT * FROM calibrations WHERE calibration_id=?', (calibration_id,))


def list_devices(db, aircraft=None):
    if aircraft is None:
        return db.all('SELECT * FROM devices ORDER BY aircraft,component,device_id')
    identity(aircraft, 'aircraft')
    return db.all('SELECT * FROM devices WHERE aircraft=? ORDER BY component,device_id', (aircraft,))
