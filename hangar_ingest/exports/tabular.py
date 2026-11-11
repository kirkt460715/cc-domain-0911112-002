import csv
import io
import json
from decimal import Decimal
from hangar_ingest.types import canonical, fail, integer
from hangar_ingest.exports.releases import payloads, verify_package

OBSERVATION_COLUMNS = (
    'partition_key', 'partition_revision', 'observation_id', 'device_id',
    'component', 'metric', 'event_time', 'value', 'unit', 'calibration_id',
    'calibration_revision', 'primary_order', 'ambiguous_order', 'warning_count',
)


def safe_cell(value):
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    value = str(value)
    if value.startswith(('=', '+', '-', '@', '\t', '\r')):
        try:
            numeric = Decimal(value)
            if numeric.is_finite():
                return value
        except Exception:
            pass
        return "'" + value
    return value


def flatten(package):
    verify_package(package)
    rows = []
    for partition in package['partitions']:
        for observation in partition['payload']['observations']:
            rows.append({
                'partition_key': partition['partition_key'],
                'partition_revision': partition['revision'],
                'observation_id': observation['observation_id'],
                'device_id': observation['device_id'],
                'component': observation['component'],
                'metric': observation['metric'],
                'event_time': observation['event_time'],
                'value': observation['value'],
                'unit': observation['unit'],
                'calibration_id': observation['calibration_id'],
                'calibration_revision': observation['calibration_revision'],
                'primary_order': observation['orders']['primary'],
                'ambiguous_order': observation['orders']['ambiguous'],
                'warning_count': len(observation['quality']),
            })
    return rows


def csv_bytes(db, release_id):
    package = payloads(db, release_id)
    output = io.StringIO(newline='')
    writer = csv.DictWriter(output, fieldnames=OBSERVATION_COLUMNS, lineterminator='\n')
    writer.writeheader()
    for row in flatten(package):
        writer.writerow({key: safe_cell(value) for key, value in row.items()})
    return output.getvalue().encode('utf-8')


def ndjson_bytes(db, release_id):
    package = payloads(db, release_id)
    return ''.join(canonical(row) + '\n' for row in flatten(package)).encode('utf-8')


def window_rows(db, release_id):
    package = payloads(db, release_id)
    result = []
    seen = set()
    for partition in package['partitions']:
        for window in partition['payload']['windows']:
            for measurement in window['measurements']:
                key = (window['order_id'], window['revision'], measurement['device_id'])
                if key in seen:
                    continue
                seen.add(key)
                result.append({
                    'order_id': window['order_id'],
                    'order_revision': window['revision'],
                    'aircraft': window['aircraft'],
                    'component': window['component'],
                    'start': window['start'],
                    'end': window['end'],
                    'device_id': measurement['device_id'],
                    'metric': measurement['metric'],
                    'unit': measurement['unit'],
                    **measurement['statistics'],
                })
    return sorted(result, key=lambda row: (row['order_id'], row['device_id']))


def compare_releases(db, before, after):
    old = {(row['partition_key'], row['observation_id']): row for row in flatten(payloads(db, before))}
    new = {(row['partition_key'], row['observation_id']): row for row in flatten(payloads(db, after))}
    changes = []
    for key in sorted(set(old) | set(new)):
        left = old.get(key)
        right = new.get(key)
        left_semantic = {k: v for k, v in left.items() if k != 'partition_revision'} if left else None
        right_semantic = {k: v for k, v in right.items() if k != 'partition_revision'} if right else None
        if left_semantic == right_semantic:
            continue
        changes.append({
            'partition_key': key[0],
            'observation_id': key[1],
            'kind': 'added' if left is None else 'removed' if right is None else 'changed',
            'before': left,
            'after': right,
        })
    return {'before': before, 'after': after, 'changes': changes, 'count': len(changes)}
