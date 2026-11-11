import datetime as dt
import hashlib
import json
import re
from decimal import Decimal, InvalidOperation


class PipelineError(Exception):
    def __init__(self, code, message, **context):
        super().__init__(message)
        self.code = code
        self.context = context

    def as_dict(self):
        return {
            'code': self.code,
            'message': str(self),
            'context': self.context,
        }


def fail(code, message, **context):
    raise PipelineError(code, message, **context)


def canonical(value):
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise PipelineError('invalid_json', 'Value is not canonical JSON') from exc


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def identity(value, field):
    if not isinstance(value, str):
        fail('invalid_id', 'Identifier must be a string', field=field)
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}', value):
        fail('invalid_id', 'Identifier contains invalid characters', field=field)
    return value


def integer(value, field, minimum=0, maximum=2**53 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        fail('invalid_integer', 'Integer outside accepted range', field=field)
    return value


def number(value, field):
    if isinstance(value, bool) or not isinstance(value, (int, str, float, Decimal)):
        fail('invalid_number', 'Expected a finite decimal', field=field)
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise PipelineError('invalid_number', 'Malformed decimal', field=field) from exc
    if not parsed.is_finite() or abs(parsed) > Decimal('1e18'):
        fail('invalid_number', 'Decimal outside supported range', field=field)
    return parsed


def decimal_string(value):
    value = number(value, 'value')
    if value == 0:
        return '0'
    return format(value.normalize(), 'f')


def text(value, field, maximum=512):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        fail('invalid_text', 'Expected bounded nonempty text', field=field)
    if any(ord(c) < 32 for c in value):
        fail('invalid_text', 'Control characters are not permitted', field=field)
    return value.strip()


def instant(value, field='timestamp'):
    if not isinstance(value, str):
        fail('invalid_time', 'Expected ISO timestamp', field=field)
    try:
        point = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise PipelineError('invalid_time', 'Malformed timestamp', field=field) from exc
    if point.tzinfo is None:
        fail('invalid_time', 'Timestamp must include timezone', field=field)
    point = point.astimezone(dt.timezone.utc)
    if not 2000 <= point.year <= 2100:
        fail('invalid_time', 'Timestamp outside supported interval', field=field)
    return point.isoformat(timespec='microseconds').replace('+00:00', 'Z')


def micros(value):
    point = dt.datetime.fromisoformat(instant(value).replace('Z', '+00:00'))
    delta = point - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    return delta.days * 86400000000 + delta.seconds * 1000000 + delta.microseconds


def from_micros(value):
    point = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(microseconds=value)
    return point.isoformat(timespec='microseconds').replace('+00:00', 'Z')


def object_fields(value, required, optional=()):
    if not isinstance(value, dict):
        fail('invalid_object', 'Expected an object')
    unknown = set(value) - set(required) - set(optional)
    missing = set(required) - set(value)
    if unknown or missing:
        fail('invalid_fields', 'Object does not match schema', unknown=sorted(unknown), missing=sorted(missing))
    return value


def utcnow():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
