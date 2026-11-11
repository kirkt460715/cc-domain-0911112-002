from bisect import bisect_left
from decimal import Decimal, localcontext
from hangar_ingest.types import decimal_string, fail, integer, micros
from hangar_ingest.quality.devices import device
from hangar_ingest.runs.windows import select


def align(db, left_device, right_device, start, end, *, tolerance_seconds=5, reuse_right=False):
    left_config = device(db, left_device)
    right_config = device(db, right_device)
    if left_device == right_device:
        fail('alignment_devices', 'Alignment requires distinct devices')
    if left_config['aircraft'] != right_config['aircraft']:
        fail('alignment_aircraft', 'Devices must belong to the same aircraft')
    integer(tolerance_seconds, 'tolerance_seconds', 0, 3600)
    if type(reuse_right) is not bool:
        fail('alignment_reuse', 'Reuse policy must be boolean')
    left = select(db, left_device, start, end)
    right = select(db, right_device, start, end)
    times = [micros(row['event_time']) for row in right]
    tolerance = tolerance_seconds * 1000000
    used = set()
    pairs = []
    unmatched = []
    for observation in left:
        point = micros(observation['event_time'])
        lower = bisect_left(times, point - tolerance)
        candidates = []
        for index in range(lower, len(right)):
            if times[index] > point + tolerance:
                break
            if not reuse_right and index in used:
                continue
            candidates.append((abs(times[index] - point), times[index], right[index]['observation_id'], index))
        if not candidates:
            unmatched.append(observation['observation_id'])
            continue
        _, _, _, selected = min(candidates)
        used.add(selected)
        matched = right[selected]
        pairs.append({
            'left_id': observation['observation_id'],
            'right_id': matched['observation_id'],
            'left_time': observation['event_time'],
            'right_time': matched['event_time'],
            'offset_us': times[selected] - point,
            'left_value': observation['value'],
            'right_value': matched['value'],
        })
    return {
        'left_device': left_device,
        'right_device': right_device,
        'pairs': pairs,
        'unmatched_left': unmatched,
        'unmatched_right': [row['observation_id'] for index, row in enumerate(right) if index not in used],
        'policy': 'left_event_order_nearest_right_then_earlier_timestamp_then_identifier',
        'reuse_right': reuse_right,
        'tolerance_seconds': tolerance_seconds,
    }


def correlation(alignment):
    pairs = alignment['pairs']
    if len(pairs) < 2:
        return {'defined': False, 'reason': 'insufficient_pairs', 'pairs': len(pairs)}
    left = [Decimal(row['left_value']) for row in pairs]
    right = [Decimal(row['right_value']) for row in pairs]
    count = len(left)
    with localcontext() as context:
        context.prec = 28
        mean_left = sum(left) / count
        mean_right = sum(right) / count
        left_variance = sum((value - mean_left) ** 2 for value in left)
        right_variance = sum((value - mean_right) ** 2 for value in right)
        if not left_variance or not right_variance:
            return {'defined': False, 'reason': 'constant_series', 'pairs': count}
        covariance = sum((a - mean_left) * (b - mean_right) for a, b in zip(left, right))
        coefficient = covariance / (left_variance * right_variance).sqrt()
    return {'defined': True, 'coefficient': decimal_string(coefficient), 'pairs': count}


def lag_scan(db, left_device, right_device, start, end, offsets_seconds, *, tolerance_seconds=1):
    from hangar_ingest.types import from_micros
    if not isinstance(offsets_seconds, list) or not offsets_seconds or len(offsets_seconds) > 100:
        fail('lag_offsets', 'Lag scan requires one to one hundred candidate offsets')
    if len(offsets_seconds) != len(set(offsets_seconds)):
        fail('lag_offsets', 'Lag offsets must be unique')
    left = select(db, left_device, start, end)
    right = select(db, right_device, start, end)
    integer(tolerance_seconds, 'tolerance_seconds', 0, 3600)
    if device(db, left_device)['aircraft'] != device(db, right_device)['aircraft']:
        fail('alignment_aircraft', 'Devices must belong to the same aircraft')
    times = [micros(row['event_time']) for row in right]
    results = []
    for offset in sorted(offsets_seconds):
        integer(offset, 'offset', -3600, 3600)
        used = set()
        pairs = []
        for row in left:
            expected = micros(row['event_time']) + offset * 1000000
            lower = bisect_left(times, expected - tolerance_seconds * 1000000)
            candidates = []
            for index in range(lower, len(times)):
                if times[index] > expected + tolerance_seconds * 1000000:
                    break
                if index not in used:
                    candidates.append((abs(times[index] - expected), index))
            if candidates:
                _, index = min(candidates)
                used.add(index)
                pairs.append({'left_value': row['value'], 'right_value': right[index]['value']})
        score = correlation({'pairs': pairs})
        results.append({'offset_seconds': offset, **score})
    eligible = [row for row in results if row['defined']]
    best = max(eligible, key=lambda row: (abs(Decimal(row['coefficient'])), row['pairs'], -abs(row['offset_seconds']), -row['offset_seconds'])) if eligible else None
    return {'results': results, 'best': best, 'interpretation': 'descriptive_alignment_not_causation'}
