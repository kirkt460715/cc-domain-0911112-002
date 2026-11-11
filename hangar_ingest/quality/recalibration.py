from hangar_ingest.types import decimal_string, fail, from_micros, micros
from hangar_ingest.quality import devices, rules, units
from hangar_ingest.runs import journal, partitions


def plan(db, calibration_id):
    calibration = db.one('SELECT * FROM calibrations WHERE calibration_id=?', (calibration_id,))
    if calibration is None:
        fail('unknown_calibration', 'Calibration does not exist')
    device = devices.device(db, calibration['device_id'])
    rule = rules.get(db, device['device_id'])
    rows = db.all('SELECT * FROM observations WHERE calibration_id=? ORDER BY raw_time,observation_id', (calibration_id,))
    changes = []
    rejected = []
    for row in rows:
        corrected = units.calibrate(row['raw_value'], row['raw_unit'], device['canonical_unit'], calibration['scale'], calibration['offset'])
        event_time = from_micros(micros(row['raw_time']) + calibration['clock_offset_us'])
        errors = rules.bounds(corrected, rule)
        if errors:
            rejected.append({'observation_id': row['observation_id'], 'errors': errors})
        changed = (row['value'], row['event_time'], row['calibration_revision']) != (decimal_string(corrected), event_time, calibration['revision'])
        if changed:
            changes.append({
                'observation_id': row['observation_id'],
                'old_value': row['value'],
                'new_value': decimal_string(corrected),
                'old_time': row['event_time'],
                'new_time': event_time,
                'old_revision': row['calibration_revision'],
                'new_revision': calibration['revision'],
            })
    return {
        'calibration_id': calibration_id,
        'calibration_revision': calibration['revision'],
        'aircraft': device['aircraft'],
        'changes': changes,
        'rejected': rejected,
        'applicable': not rejected,
    }


def apply(db, calibration_id, expected_revision):
    with db.transaction():
        proposed = plan(db, calibration_id)
        if proposed['calibration_revision'] != expected_revision:
            fail('stale_revision', 'Calibration revision changed')
        if not proposed['applicable']:
            fail('recalibration_quality', 'Correction would violate quality bounds', rejected=proposed['rejected'])
        for change in proposed['changes']:
            db.execute(
                'UPDATE observations SET value=?,event_time=?,calibration_revision=? WHERE observation_id=?',
                (change['new_value'], change['new_time'], change['new_revision'], change['observation_id']),
            )
            from hangar_ingest.runs.maintenance import dirty_observation
            device_id = db.scalar('SELECT device_id FROM observations WHERE observation_id=?', (change['observation_id'],))
            dirty_observation(db, device_id, change['old_time'], 'recalibration_old_day')
            dirty_observation(db, device_id, change['new_time'], 'recalibration_new_day')
        journal.record(db, 'calibration.reprocessed', {
            'calibration_id': calibration_id,
            'revision': expected_revision,
            'changes': proposed['changes'],
        })
        return proposed


def stale_observations(db):
    return db.all(
        'SELECT o.observation_id,o.device_id,o.calibration_id,o.calibration_revision,c.revision AS current_revision '
        'FROM observations o JOIN calibrations c ON c.calibration_id=o.calibration_id '
        'WHERE o.calibration_revision!=c.revision ORDER BY o.device_id,o.event_time,o.observation_id',
    )
