from decimal import Decimal, localcontext

from hangar_ingest.types import decimal_string, fail, number

UNITS = {
    'C': ('temperature', Decimal('1'), Decimal('0')),
    'F': ('temperature', Decimal(5) / Decimal(9), -Decimal(32) * Decimal(5) / Decimal(9)),
    'K': ('temperature', Decimal('1'), Decimal('-273.15')),
    'Pa': ('pressure', Decimal('1'), Decimal('0')),
    'kPa': ('pressure', Decimal('1000'), Decimal('0')),
    'bar': ('pressure', Decimal('100000'), Decimal('0')),
    'psi': ('pressure', Decimal('6894.757293168'), Decimal('0')),
    'm/s': ('speed', Decimal('1'), Decimal('0')),
    'km/h': ('speed', Decimal(1) / Decimal('3.6'), Decimal('0')),
    'knot': ('speed', Decimal('0.5144444444444444444444444444'), Decimal('0')),
    'm/s2': ('acceleration', Decimal('1'), Decimal('0')),
    'g': ('acceleration', Decimal('9.80665'), Decimal('0')),
    'rpm': ('rotation', Decimal('1'), Decimal('0')),
    'Hz': ('rotation', Decimal('60'), Decimal('0')),
    'V': ('voltage', Decimal('1'), Decimal('0')),
    'mV': ('voltage', Decimal('.001'), Decimal('0')),
    'A': ('current', Decimal('1'), Decimal('0')),
    'mA': ('current', Decimal('.001'), Decimal('0')),
    'percent': ('ratio', Decimal('.01'), Decimal('0')),
    'ratio': ('ratio', Decimal('1'), Decimal('0')),
}


def describe(unit):
    if unit not in UNITS:
        fail('unknown_unit', 'Unsupported measurement unit', unit=unit)
    dimension, scale, offset = UNITS[unit]
    return {'unit': unit, 'dimension': dimension, 'scale': decimal_string(scale), 'offset': decimal_string(offset)}


def compatible(source, target):
    return describe(source)['dimension'] == describe(target)['dimension']


def convert(value, source, target):
    value = number(value, 'measurement')
    if not compatible(source, target):
        fail('unit_dimension', 'Measurement dimensions do not match', source=source, target=target)
    if source == target:
        return value
    _, source_scale, source_offset = UNITS[source]
    _, target_scale, target_offset = UNITS[target]
    with localcontext() as ctx:
        ctx.prec = 28
        base = value * source_scale + source_offset
        return (base - target_offset) / target_scale


def calibrate(value, source, target, scale, offset):
    converted = convert(value, source, target)
    with localcontext() as ctx:
        ctx.prec = 28
        return converted * number(scale, 'scale') + number(offset, 'offset')


def supported(dimension=None):
    rows = [describe(unit) for unit in sorted(UNITS)]
    return [row for row in rows if dimension is None or row['dimension'] == dimension]
