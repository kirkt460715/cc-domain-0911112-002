import csv
import io
import json

from hangar_ingest.types import PipelineError, canonical, decimal_string, fail, identity, instant, number, object_fields

FIELDS = {'event_id', 'device', 'observed_at', 'value', 'unit'}


def normalize(row):
    object_fields(row, FIELDS)
    return {
        'event_id': identity(row['event_id'], 'event_id'),
        'device': identity(row['device'], 'device'),
        'observed_at': instant(row['observed_at']),
        'value': decimal_string(number(row['value'], 'value')),
        'unit': identity(row['unit'], 'unit') if '/' not in str(row['unit']) else str(row['unit']),
    }


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            fail('duplicate_key', 'JSON contains duplicate keys', key=key)
        result[key] = value
    return result


def decode_json(value):
    try:
        return json.loads(value, object_pairs_hook=unique_keys)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise PipelineError('malformed_json', 'Cannot decode JSON record') from exc


def records(data, format):
    if not isinstance(data, bytes) or len(data) > 64 * 1024 * 1024:
        fail('input_size', 'Sensor file must be at most 64 MiB')
    if format == 'ndjson':
        for line_number, line in enumerate(data.splitlines(), 1):
            if not line.strip():
                continue
            raw = line.decode('utf-8', errors='replace')
            try:
                if len(line) > 1048576:
                    fail('record_size', 'Sensor record exceeds one MiB')
                value = normalize(decode_json(line))
            except PipelineError as exc:
                yield line_number, {'raw': raw}, exc.as_dict()
            else:
                yield line_number, value, None
        return
    if format != 'csv':
        fail('unknown_format', 'Sensor format must be csv or ndjson')
    try:
        text = data.decode('utf-8')
    except UnicodeError as exc:
        raise PipelineError('invalid_utf8', 'CSV must be UTF-8') from exc
    reader = csv.DictReader(io.StringIO(text, newline=''), strict=True)
    if reader.fieldnames is None or len(reader.fieldnames) != len(FIELDS) or set(reader.fieldnames) != FIELDS:
        fail('csv_header', 'CSV header does not match sensor schema')
    try:
        for row in reader:
            try:
                if None in row or any(value is None for value in row.values()):
                    fail('csv_columns', 'CSV row field count mismatch')
                value = normalize(row)
            except PipelineError as exc:
                yield reader.line_num, row, exc.as_dict()
            else:
                yield reader.line_num, value, None
    except csv.Error as exc:
        raise PipelineError('csv_malformed', 'Malformed quoted CSV') from exc


def encode(rows, format='ndjson'):
    normalized = [normalize(row) for row in rows]
    if format == 'ndjson':
        return ''.join(canonical(row) + '\n' for row in normalized).encode()
    if format != 'csv':
        fail('unknown_format', 'Sensor format must be csv or ndjson')
    output = io.StringIO(newline='')
    writer = csv.DictWriter(output, fieldnames=['event_id', 'device', 'observed_at', 'value', 'unit'], lineterminator='\n')
    writer.writeheader()
    writer.writerows(normalized)
    return output.getvalue().encode()
