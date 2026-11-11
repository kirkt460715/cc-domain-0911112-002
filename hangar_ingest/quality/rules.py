from decimal import Decimal

from hangar_ingest.types import decimal_string, fail, integer, number
from hangar_ingest.quality.devices import device
from hangar_ingest.runs import journal


def configure(db, device_id, revision=0, *, minimum=None, maximum=None, max_delta=None, max_gap_seconds=300, duplicate_policy='reject', required_calibration=True):
    device(db, device_id)
    integer(revision, 'revision')
    integer(max_gap_seconds, 'max_gap_seconds', 1, 86400)
    if duplicate_policy not in {'reject', 'keep_first'}:
        fail('duplicate_policy', 'Unsupported duplicate policy')
    if type(required_calibration) is not bool:
        fail('invalid_boolean', 'required_calibration must be boolean')
    low = number(minimum, 'minimum') if minimum is not None else None
    high = number(maximum, 'maximum') if maximum is not None else None
    delta = number(max_delta, 'max_delta') if max_delta is not None else None
    if low is not None and high is not None and low > high:
        fail('rule_bounds', 'Minimum exceeds maximum')
    if delta is not None and delta <= 0:
        fail('rule_delta', 'Maximum delta must be positive')
    with db.transaction():
        prior = db.one('SELECT * FROM quality_rules WHERE device_id=?', (device_id,))
        expected = prior['revision'] if prior else 0
        if revision != expected:
            fail('stale_revision', 'Quality configuration changed')
        values = (
            device_id, revision + 1,
            decimal_string(low) if low is not None else None,
            decimal_string(high) if high is not None else None,
            decimal_string(delta) if delta is not None else None,
            max_gap_seconds, duplicate_policy, int(required_calibration),
        )
        db.execute(
            'INSERT INTO quality_rules(device_id,revision,minimum,maximum,max_delta,max_gap_seconds,duplicate_policy,required_calibration) '
            'VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(device_id) DO UPDATE SET revision=excluded.revision,'
            'minimum=excluded.minimum,maximum=excluded.maximum,max_delta=excluded.max_delta,'
            'max_gap_seconds=excluded.max_gap_seconds,duplicate_policy=excluded.duplicate_policy,required_calibration=excluded.required_calibration', values,
        )
        journal.record(db, 'quality.configured', {'device_id': device_id, 'revision': revision + 1})
        return get(db, device_id)


def get(db, device_id):
    row = db.one('SELECT * FROM quality_rules WHERE device_id=?', (device_id,))
    if row is None:
        return {
            'device_id': device_id, 'revision': 0,
            'minimum': None, 'maximum': None, 'max_delta': None,
            'max_gap_seconds': 300, 'duplicate_policy': 'reject',
            'required_calibration': 1,
        }
    return row


def bounds(value, rule):
    value = number(value, 'value')
    reasons = []
    if rule['minimum'] is not None and value < Decimal(rule['minimum']):
        reasons.append({'code': 'below_minimum', 'value': decimal_string(value), 'minimum': rule['minimum']})
    if rule['maximum'] is not None and value > Decimal(rule['maximum']):
        reasons.append({'code': 'above_maximum', 'value': decimal_string(value), 'maximum': rule['maximum']})
    return reasons


def neighbor_checks(db, device_id, event_time, value, rule, *, exclude=None):
    from hangar_ingest.types import micros
    value = number(value, 'value')
    clauses = 'device_id=?'
    args = [device_id]
    if exclude:
        clauses += ' AND observation_id!=?'
        args.append(exclude)
    previous = db.one(
        f'SELECT observation_id,event_time,value FROM observations WHERE {clauses} AND event_time<? ORDER BY event_time DESC,observation_id DESC LIMIT 1',
        [*args, event_time],
    )
    following = db.one(
        f'SELECT observation_id,event_time,value FROM observations WHERE {clauses} AND event_time>? ORDER BY event_time,observation_id LIMIT 1',
        [*args, event_time],
    )
    reasons = []
    for label, neighbor in [('previous', previous), ('following', following)]:
        if neighbor is None:
            continue
        gap = abs(micros(event_time) - micros(neighbor['event_time']))
        if gap > rule['max_gap_seconds'] * 1000000:
            reasons.append({'code': 'sampling_gap', 'neighbor': label, 'seconds': decimal_string(Decimal(gap) / 1000000)})
        if rule['max_delta'] is not None:
            delta = abs(value - Decimal(neighbor['value']))
            if delta > Decimal(rule['max_delta']):
                reasons.append({'code': 'neighbor_delta', 'neighbor': label, 'delta': decimal_string(delta), 'limit': rule['max_delta']})
    return reasons


def summarize(reasons):
    fatal = {'below_minimum', 'above_maximum', 'neighbor_delta'}
    return {
        'accepted': not any(reason['code'] in fatal for reason in reasons),
        'warnings': [reason for reason in reasons if reason['code'] not in fatal],
        'errors': [reason for reason in reasons if reason['code'] in fatal],
    }
