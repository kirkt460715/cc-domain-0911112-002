import json
from decimal import Decimal, localcontext
from hangar_ingest.types import decimal_string, fail, identity, integer, number
from hangar_ingest.runs.windows import select, statistics
from hangar_ingest.quality import devices


def percentile(values, quantile):
    values = sorted(number(value, 'value') for value in values)
    q = number(quantile, 'quantile')
    if not 0 <= q <= 1:
        fail('quantile_range', 'Quantile must be between zero and one')
    if not values:
        return None
    with localcontext() as context:
        context.prec = 28
        position = q * (len(values) - 1)
        lower = int(position)
        upper = min(lower + 1, len(values) - 1)
        fraction = position - lower
        result = values[lower] + fraction * (values[upper] - values[lower])
    return decimal_string(result)


def histogram(values, boundaries):
    if not isinstance(boundaries, list) or not boundaries or len(boundaries) > 1000:
        fail('histogram_boundaries', 'Histogram needs a bounded nonempty boundary list')
    cuts = [number(value, 'boundary') for value in boundaries]
    if cuts != sorted(set(cuts)):
        fail('histogram_order', 'Histogram boundaries must strictly increase')
    counts = [0] * (len(cuts) + 1)
    for raw in values:
        value = number(raw, 'value')
        index = 0
        while index < len(cuts) and value >= cuts[index]:
            index += 1
        counts[index] += 1
    result = []
    for index, count in enumerate(counts):
        result.append({
            'lower': decimal_string(cuts[index - 1]) if index else None,
            'upper': decimal_string(cuts[index]) if index < len(cuts) else None,
            'count': count,
            'interval': 'left_closed_right_open',
        })
    return result


def profile(db, device_id, start, end):
    config = devices.device(db, device_id)
    rows = select(db, device_id, start, end)
    values = [Decimal(row['value']) for row in rows]
    total = len(values)
    base = statistics(rows)
    if values:
        with localcontext() as context:
            context.prec = 28
            mean = sum(values) / total
            variance = sum((value - mean) ** 2 for value in values) / total
            standard_deviation = variance.sqrt()
        distinct = len(set(values))
        median = Decimal(percentile(values, '0.5'))
        deviations = [abs(value - median) for value in values]
        mad = percentile(deviations, '0.5')
    else:
        variance = None
        standard_deviation = None
        distinct = 0
        mad = None
    quality = {}
    sources = {}
    for row in rows:
        sources[row['source']] = sources.get(row['source'], 0) + 1
        for issue in row['quality']:
            quality[issue['code']] = quality.get(issue['code'], 0) + 1
    return {
        'device_id': device_id,
        'metric': config['metric'],
        'unit': config['canonical_unit'],
        'statistics': base,
        'variance_population': decimal_string(variance) if variance is not None else None,
        'standard_deviation': decimal_string(standard_deviation) if standard_deviation is not None else None,
        'distinct_values': distinct,
        'p05': percentile(values, '0.05'),
        'p95': percentile(values, '0.95'),
        'median_absolute_deviation': mad,
        'sources': dict(sorted(sources.items())),
        'warnings': dict(sorted(quality.items())),
    }


def compare_periods(db, device_id, baseline_start, baseline_end, current_start, current_end, threshold='0.2'):
    tolerance = number(threshold, 'threshold')
    if tolerance < 0:
        fail('drift_threshold', 'Drift threshold cannot be negative')
    baseline = profile(db, device_id, baseline_start, baseline_end)
    current = profile(db, device_id, current_start, current_end)
    old_mean = baseline['statistics']['mean']
    new_mean = current['statistics']['mean']
    if old_mean is None or new_mean is None:
        return {'comparable': False, 'baseline': baseline, 'current': current, 'reason': 'empty_period'}
    delta = Decimal(new_mean) - Decimal(old_mean)
    scale = abs(Decimal(old_mean))
    relative = abs(delta) / scale if scale else None
    drift = abs(delta) > tolerance if scale == 0 else relative > tolerance
    return {
        'comparable': True,
        'baseline': baseline,
        'current': current,
        'mean_delta': decimal_string(delta),
        'relative_delta': decimal_string(relative) if relative is not None else None,
        'drift': drift,
        'zero_baseline_rule': 'absolute_threshold',
    }


def outliers(db, device_id, start, end, multiplier='3'):
    factor = number(multiplier, 'multiplier')
    if factor <= 0:
        fail('outlier_multiplier', 'Outlier multiplier must be positive')
    rows = select(db, device_id, start, end)
    values = [Decimal(row['value']) for row in rows]
    if len(values) < 4:
        return {'method': 'iqr', 'assessed': False, 'reason': 'fewer_than_four_samples', 'outliers': []}
    lower_quartile = Decimal(percentile(values, '0.25'))
    upper_quartile = Decimal(percentile(values, '0.75'))
    spread = upper_quartile - lower_quartile
    lower = lower_quartile - factor * spread
    upper = upper_quartile + factor * spread
    flagged = []
    for row in rows:
        value = Decimal(row['value'])
        if value < lower or value > upper:
            flagged.append({'observation_id': row['observation_id'], 'event_time': row['event_time'], 'value': row['value']})
    return {'method': 'iqr', 'assessed': True, 'lower': decimal_string(lower), 'upper': decimal_string(upper), 'outliers': flagged}
