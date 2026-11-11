import json
from decimal import Decimal, localcontext

from hangar_ingest.types import decimal_string, fail, instant, integer, micros
from hangar_ingest.runs.partitions import day_bounds


def statistics(rows):
    if not rows:
        return {'count': 0, 'minimum': None, 'maximum': None, 'mean': None, 'median': None, 'first': None, 'last': None, 'duration_us': 0, 'max_gap_us': 0}
    values = sorted(Decimal(row['value']) for row in rows)
    count = len(values)
    with localcontext() as ctx:
        ctx.prec = 28
        mean = sum(values, Decimal(0)) / count
        middle = count // 2
        median = values[middle] if count % 2 else (values[middle - 1] + values[middle]) / 2
    times = sorted(micros(row['event_time']) for row in rows)
    gaps = [right - left for left, right in zip(times, times[1:])]
    return {
        'count': count,
        'minimum': decimal_string(values[0]),
        'maximum': decimal_string(values[-1]),
        'mean': decimal_string(mean),
        'median': decimal_string(median),
        'first': min(row['event_time'] for row in rows),
        'last': max(row['event_time'] for row in rows),
        'duration_us': times[-1] - times[0],
        'max_gap_us': max(gaps, default=0),
    }


def select(db, device_id, start, end):
    start = instant(start)
    end = instant(end)
    if start >= end:
        fail('window_interval', 'Observation window must be nonempty')
    rows = db.all(
        'SELECT * FROM observations WHERE device_id=? AND event_time>=? AND event_time<? ORDER BY event_time,observation_id',
        (device_id, start, end),
    )
    for row in rows:
        row['quality'] = json.loads(row['quality'])
    return rows


def order_window(db, order, *, padding_seconds=0):
    from hangar_ingest.types import from_micros
    integer(padding_seconds, 'padding_seconds', 0, 86400)
    start = from_micros(micros(order['opened_at']) - padding_seconds * 1000000)
    end = from_micros(micros(order['closed_at']) + padding_seconds * 1000000)
    devices = db.all('SELECT * FROM devices WHERE aircraft=? AND component=? ORDER BY device_id', (order['aircraft'], order['component']))
    measurements = []
    for device in devices:
        rows = select(db, device['device_id'], start, end)
        measurements.append({
            'device_id': device['device_id'],
            'metric': device['metric'],
            'unit': device['canonical_unit'],
            'statistics': statistics(rows),
            'observation_ids': [row['observation_id'] for row in rows],
        })
    return {
        'order_id': order['order_id'],
        'revision': order['revision'],
        'aircraft': order['aircraft'],
        'component': order['component'],
        'start': start,
        'end': end,
        'state': order['state'],
        'measurements': measurements,
    }


def match_orders(db, observation):
    device = db.one('SELECT aircraft,component FROM devices WHERE device_id=?', (observation['device_id'],))
    candidates = db.all(
        "SELECT * FROM work_orders WHERE aircraft=? AND component=? AND state!='cancelled' "
        'AND opened_at<=? AND closed_at>? ORDER BY opened_at,order_id',
        (device['aircraft'], device['component'], observation['event_time'], observation['event_time']),
    )
    ranked = sorted(candidates, key=lambda row: (micros(row['closed_at']) - micros(row['opened_at']), row['order_id']))
    return {
        'primary': ranked[0]['order_id'] if ranked else None,
        'all': [row['order_id'] for row in ranked],
        'ambiguous': len(ranked) > 1,
        'rule': 'smallest_enclosing_window_then_identifier',
    }


def build_day(db, aircraft, day):
    start, end = day_bounds(day)
    rows = db.all(
        'SELECT o.*,d.aircraft,d.component,d.metric FROM observations o JOIN devices d ON o.device_id=d.device_id '
        'WHERE d.aircraft=? AND o.event_time>=? AND o.event_time<? ORDER BY o.event_time,o.device_id,o.observation_id',
        (aircraft, start, end),
    )
    observations = []
    for row in rows:
        observations.append({
            'observation_id': row['observation_id'],
            'device_id': row['device_id'],
            'component': row['component'],
            'metric': row['metric'],
            'event_time': row['event_time'],
            'value': row['value'],
            'unit': row['unit'],
            'quality': json.loads(row['quality']),
            'calibration_id': row['calibration_id'],
            'calibration_revision': row['calibration_revision'],
            'orders': match_orders(db, row),
        })
    orders = db.all(
        "SELECT * FROM work_orders WHERE aircraft=? AND state!='cancelled' AND opened_at<? AND closed_at>? ORDER BY order_id",
        (aircraft, end, start),
    )
    windows = [order_window(db, order) for order in orders]
    groups = {}
    for row in observations:
        groups.setdefault(row['device_id'], []).append(row)
    return {
        'version': 1,
        'aircraft': aircraft,
        'day': day,
        'observations': observations,
        'windows': windows,
        'statistics': {device: statistics(values) for device, values in sorted(groups.items())},
    }


def resample(db, device_id, start, end, interval_seconds):
    integer(interval_seconds, 'interval_seconds', 1, 86400)
    start_us = micros(start)
    end_us = micros(end)
    if start_us >= end_us:
        fail('window_interval', 'Resampling interval must be nonempty')
    bucket_size = interval_seconds * 1000000
    if (end_us - start_us) // bucket_size > 100000:
        fail('bucket_limit', 'Resampling would create too many buckets')
    from hangar_ingest.types import from_micros
    buckets = {}
    for row in select(db, device_id, start, end):
        index = (micros(row['event_time']) - start_us) // bucket_size
        buckets.setdefault(index, []).append(row)
    result = []
    for index, lower in enumerate(range(start_us, end_us, bucket_size)):
        result.append({
            'start': from_micros(lower),
            'end': from_micros(min(lower + bucket_size, end_us)),
            'statistics': statistics(buckets.get(index, [])),
        })
    return result
