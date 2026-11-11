from decimal import Decimal
from hangar_ingest.types import decimal_string, fail, from_micros, integer, micros
from hangar_ingest.runs.windows import select, statistics
from hangar_ingest.quality import devices, rules


def merge_intervals(intervals):
    normalized = sorted(intervals)
    result = []
    for start, end in normalized:
        if start >= end:
            continue
        if result and start <= result[-1][1]:
            result[-1][1] = max(result[-1][1], end)
        else:
            result.append([start, end])
    return result


def complement(intervals, start, end):
    cursor = start
    result = []
    for lower, upper in merge_intervals(intervals):
        lower = max(lower, start)
        upper = min(upper, end)
        if upper <= start or lower >= end:
            continue
        if lower > cursor:
            result.append([cursor, lower])
        cursor = max(cursor, upper)
    if cursor < end:
        result.append([cursor, end])
    return result


def measure(db, device_id, start, end, *, tolerance_seconds=None):
    device = devices.device(db, device_id)
    rule = rules.get(db, device_id)
    tolerance = rule['max_gap_seconds'] if tolerance_seconds is None else tolerance_seconds
    integer(tolerance, 'tolerance_seconds', 1, 86400)
    lower = micros(start)
    upper = micros(end)
    if lower >= upper:
        fail('coverage_interval', 'Coverage interval must be increasing')
    rows = select(db, device_id, start, end)
    intervals = []
    half = tolerance * 1000000 // 2
    for row in rows:
        point = micros(row['event_time'])
        intervals.append([max(lower, point - half), min(upper, point + half)])
    covered = merge_intervals(intervals)
    covered_us = sum(end - start for start, end in covered)
    missing = complement(covered, lower, upper)
    return {
        'device_id': device_id,
        'start': from_micros(lower),
        'end': from_micros(upper),
        'samples': len(rows),
        'covered_us': covered_us,
        'total_us': upper - lower,
        'coverage_ratio': decimal_string(Decimal(covered_us) / Decimal(upper - lower)),
        'missing': [{'start': from_micros(a), 'end': from_micros(b), 'duration_us': b - a} for a, b in missing],
        'tolerance_seconds': tolerance,
    }


def calibration_gaps(db, device_id, start, end):
    devices.device(db, device_id)
    lower = micros(start)
    upper = micros(end)
    if lower >= upper:
        fail('coverage_interval', 'Calibration interval must be increasing')
    rows = db.all('SELECT valid_from,valid_until FROM calibrations WHERE device_id=? ORDER BY valid_from', (device_id,))
    intervals = [[micros(row['valid_from']), micros(row['valid_until'])] for row in rows]
    gaps = complement(intervals, lower, upper)
    return {
        'device_id': device_id,
        'complete': not gaps,
        'gaps': [{'start': from_micros(a), 'end': from_micros(b)} for a, b in gaps],
    }


def order_readiness(db, order_id, *, minimum_ratio='0.8'):
    from hangar_ingest.decoders.workorders import get
    from hangar_ingest.types import number
    threshold = number(minimum_ratio, 'minimum_ratio')
    if not 0 <= threshold <= 1:
        fail('coverage_ratio', 'Coverage ratio must be between zero and one')
    order = get(db, order_id)
    matching = db.all('SELECT device_id FROM devices WHERE aircraft=? AND component=? ORDER BY device_id', (order['aircraft'], order['component']))
    checks = []
    for row in matching:
        coverage = measure(db, row['device_id'], order['opened_at'], order['closed_at'])
        calibration = calibration_gaps(db, row['device_id'], order['opened_at'], order['closed_at'])
        ready = Decimal(coverage['coverage_ratio']) >= threshold and calibration['complete']
        checks.append({'device_id': row['device_id'], 'ready': ready, 'coverage': coverage, 'calibration': calibration})
    return {
        'order_id': order_id,
        'ready': bool(checks) and all(row['ready'] for row in checks) and order['state'] == 'closed',
        'minimum_ratio': decimal_string(threshold),
        'state': order['state'],
        'devices': checks,
    }


def duplicate_timestamps(db, device_id):
    devices.device(db, device_id)
    groups = db.all(
        'SELECT event_time,COUNT(*) AS count FROM observations WHERE device_id=? GROUP BY event_time HAVING COUNT(*)>1 ORDER BY event_time',
        (device_id,),
    )
    for group in groups:
        group['observations'] = db.all(
            'SELECT observation_id,source,external_event,value FROM observations WHERE device_id=? AND event_time=? ORDER BY observation_id',
            (device_id, group['event_time']),
        )
        group['conflicting_values'] = len({row['value'] for row in group['observations']}) > 1
    return groups
