from hangar_ingest.types import decimal_string, digest, fail, from_micros, micros
from hangar_ingest.quality import devices, rules, units


def observation(db, source, payload):
    device = devices.resolve_alias(db, source, payload['device'], payload['observed_at'])
    rule = rules.get(db, device['device_id'])
    calibration = devices.calibration_at(db, device['device_id'], payload['observed_at'])
    if calibration is None and rule['required_calibration']:
        fail('calibration_missing', 'Observation has no applicable calibration', device_id=device['device_id'])
    scale = calibration['scale'] if calibration else '1'
    offset = calibration['offset'] if calibration else '0'
    clock_offset = calibration['clock_offset_us'] if calibration else 0
    corrected = units.calibrate(payload['value'], payload['unit'], device['canonical_unit'], scale, offset)
    corrected_time = from_micros(micros(payload['observed_at']) + clock_offset)
    issues = rules.bounds(corrected, rule)
    issues.extend(rules.neighbor_checks(db, device['device_id'], corrected_time, corrected, rule))
    assessment = rules.summarize(issues)
    if not assessment['accepted']:
        fail('quality_rejected', 'Observation violates device quality rules', errors=assessment['errors'], warnings=assessment['warnings'])
    return {
        'observation_id': digest({'source': source, 'event_id': payload['event_id']}),
        'source': source,
        'external_event': payload['event_id'],
        'device_id': device['device_id'],
        'aircraft': device['aircraft'],
        'component': device['component'],
        'metric': device['metric'],
        'raw_time': payload['observed_at'],
        'event_time': corrected_time,
        'value': decimal_string(corrected),
        'unit': device['canonical_unit'],
        'raw_value': payload['value'],
        'raw_unit': payload['unit'],
        'calibration_id': calibration['calibration_id'] if calibration else None,
        'calibration_revision': calibration['revision'] if calibration else None,
        'quality': assessment['warnings'],
        'raw_hash': digest(payload),
        'rule_revision': rule['revision'],
    }


def duplicate_decision(db, source, payload):
    previous = db.one('SELECT * FROM observations WHERE source=? AND external_event=?', (source, payload['event_id']))
    if previous is None:
        return None
    if previous['raw_hash'] == digest(payload):
        return {'status': 'duplicate', 'observation_id': previous['observation_id']}
    rule = rules.get(db, previous['device_id'])
    if rule['duplicate_policy'] == 'keep_first':
        return {'status': 'ignored_conflict', 'observation_id': previous['observation_id']}
    fail('event_conflict', 'Source event ID has different content', event_id=payload['event_id'])
