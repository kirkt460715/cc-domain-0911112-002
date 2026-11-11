from hangar_ingest.types import fail, identity, instant
from hangar_ingest.quality import devices, coverage, rules
from hangar_ingest.runs import inspection


def source_configuration(db, source, start, end):
    identity(source, 'source')
    start = instant(start)
    end = instant(end)
    if start >= end:
        fail('preflight_interval', 'Preflight interval must be nonempty')
    aliases = db.all(
        'SELECT * FROM aliases WHERE source=? AND valid_from<? AND valid_until>? ORDER BY external_id,valid_from',
        (source, end, start),
    )
    errors = []
    warnings = []
    checked = []
    if not aliases:
        errors.append({'code': 'no_aliases', 'source': source})
    seen = set()
    for alias in aliases:
        config = devices.device(db, alias['device_id'])
        if not config['active']:
            errors.append({'code': 'inactive_device', 'device_id': config['device_id']})
        if config['device_id'] in seen:
            continue
        seen.add(config['device_id'])
        rule = rules.get(db, config['device_id'])
        lower = max(start, alias['valid_from'])
        upper = min(end, alias['valid_until'])
        calibration = coverage.calibration_gaps(db, config['device_id'], lower, upper)
        if not calibration['complete']:
            detail = {'code': 'calibration_gaps', 'device_id': config['device_id'], 'gaps': calibration['gaps']}
            if rule['required_calibration']:
                errors.append(detail)
            else:
                warnings.append(detail)
        if rule['revision'] == 0:
            warnings.append({'code': 'default_quality_rules', 'device_id': config['device_id']})
        if rule['minimum'] is None and rule['maximum'] is None:
            warnings.append({'code': 'unbounded_measurements', 'device_id': config['device_id']})
        checked.append({'device': config, 'rule': rule, 'calibration': calibration})
    return {'source': source, 'start': start, 'end': end, 'ready': not errors, 'errors': errors, 'warnings': warnings, 'devices': checked}


def device_inventory(db):
    result = []
    for config in devices.list_devices(db):
        aliases = db.all('SELECT source,external_id,valid_from,valid_until FROM aliases WHERE device_id=? ORDER BY source,external_id,valid_from', (config['device_id'],))
        calibrations = db.all('SELECT calibration_id,valid_from,valid_until,revision FROM calibrations WHERE device_id=? ORDER BY valid_from', (config['device_id'],))
        observations = db.one('SELECT COUNT(*) AS count,MIN(event_time) AS first,MAX(event_time) AS last FROM observations WHERE device_id=?', (config['device_id'],))
        result.append({
            'device': config,
            'aliases': aliases,
            'calibrations': calibrations,
            'observations': observations,
            'quality_rules': rules.get(db, config['device_id']),
        })
    return result


def order_conflicts(db, aircraft):
    identity(aircraft, 'aircraft')
    orders = db.all("SELECT * FROM work_orders WHERE aircraft=? AND state!='cancelled' ORDER BY component,opened_at,order_id", (aircraft,))
    conflicts = []
    for index, left in enumerate(orders):
        for right in orders[index + 1:]:
            if left['component'] != right['component']:
                continue
            start = max(left['opened_at'], right['opened_at'])
            end = min(left['closed_at'], right['closed_at'])
            if start >= end:
                continue
            conflicts.append({
                'left_order': left['order_id'],
                'right_order': right['order_id'],
                'component': left['component'],
                'overlap_start': start,
                'overlap_end': end,
                'resolution': 'deterministic_smallest_enclosing_window',
            })
    return conflicts


def delivery_readiness(db, partition_keys):
    from hangar_ingest.exports.watermarks import readiness
    from hangar_ingest.runs.partitions import get
    from hangar_ingest.quality.recalibration import stale_observations
    if not isinstance(partition_keys, list) or not partition_keys:
        fail('preflight_partitions', 'Delivery preflight needs partitions')
    checks = []
    errors = []
    for partition_key in sorted(set(partition_keys)):
        partition = get(db, partition_key)
        seal = readiness(db, partition_key)
        if partition['state'] != 'ready':
            errors.append({'code': 'dirty_partition', 'partition_key': partition_key})
        if not seal['ready']:
            errors.append({'code': 'unsealed_partition', 'partition_key': partition_key})
        checks.append({'partition': partition, 'seal': seal})
    stale = stale_observations(db)
    if stale:
        errors.append({'code': 'stale_calibrations', 'observations': len(stale)})
    integrity = inspection.check(db)
    if not integrity['ok']:
        errors.append({'code': 'integrity_failure', 'details': integrity['errors']})
    return {'ready': not errors, 'errors': errors, 'partitions': checks, 'integrity': integrity}


def orphan_configuration(db):
    devices_without_alias = db.all(
        'SELECT device_id FROM devices WHERE NOT EXISTS(SELECT 1 FROM aliases WHERE aliases.device_id=devices.device_id) ORDER BY device_id',
    )
    devices_without_calibration = db.all(
        'SELECT device_id FROM devices WHERE NOT EXISTS(SELECT 1 FROM calibrations WHERE calibrations.device_id=devices.device_id) ORDER BY device_id',
    )
    orders_without_device = db.all(
        'SELECT order_id,aircraft,component FROM work_orders WHERE NOT EXISTS('
        'SELECT 1 FROM devices WHERE devices.aircraft=work_orders.aircraft AND devices.component=work_orders.component) ORDER BY order_id',
    )
    return {
        'devices_without_alias': [row['device_id'] for row in devices_without_alias],
        'devices_without_calibration': [row['device_id'] for row in devices_without_calibration],
        'orders_without_device': orders_without_device,
    }
