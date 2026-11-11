import copy
import io
import json
import unittest
from decimal import Decimal
from hangar_ingest.types import PipelineError, canonical, decimal_string, digest, identity, instant, integer, micros, from_micros, number
from hangar_ingest.decoders import sensors, workorders
from hangar_ingest.quality import devices, rules, units
from tests.support import PipelineCase, sample, order, START, END


class ScalarTests(unittest.TestCase):
    def test_decimal_values_preserve_exactness(self):
        self.assertEqual(number('0.1', 'v') + number('0.2', 'v'), Decimal('0.3'))
        self.assertEqual(decimal_string('1.2300'), '1.23')
        self.assertEqual(decimal_string('-0.00'), '0')
        self.assertEqual(decimal_string('1e3'), '1000')
        with self.assertRaises(PipelineError):
            number(True, 'v')
        with self.assertRaises(PipelineError):
            number('NaN', 'v')
        with self.assertRaises(PipelineError):
            number('1e19', 'v')

    def test_time_roundtrip_with_timezone_and_microseconds(self):
        raw = '2026-11-11T08:00:00.123456+08:00'
        normalized = instant(raw)
        self.assertEqual(normalized, '2026-11-11T00:00:00.123456Z')
        self.assertEqual(from_micros(micros(raw)), normalized)
        self.assertEqual(micros('2026-11-11T00:00:01Z') - micros(START), 1000000)
        for bad in ('2026-11-11', 'not-time', '1999-01-01T00:00:00Z'):
            with self.subTest(bad=bad):
                with self.assertRaises(PipelineError):
                    instant(bad)

    def test_identifiers_and_integer_types(self):
        self.assertEqual(identity('plane-1:dev_2', 'id'), 'plane-1:dev_2')
        self.assertEqual(integer(0, 'cursor'), 0)
        with self.assertRaises(PipelineError):
            integer(True, 'cursor')
        with self.assertRaises(PipelineError):
            integer(-1, 'cursor')
        with self.assertRaises(PipelineError):
            identity('../outside', 'id')
        with self.assertRaises(PipelineError):
            identity('', 'id')
        self.assertEqual(digest({'a': 1, 'b': 2}), digest({'b': 2, 'a': 1}))

    def test_unit_dimensions_and_affine_conversion(self):
        self.assertEqual(units.convert('100', 'C', 'K'), Decimal('373.15'))
        self.assertAlmostEqual(units.convert('32', 'F', 'C'), Decimal('0'), places=20)
        self.assertEqual(units.convert('1', 'bar', 'Pa'), Decimal('100000'))
        self.assertEqual(units.convert('1000', 'mV', 'V'), Decimal('1'))
        self.assertEqual(units.convert('50', 'percent', 'ratio'), Decimal('.5'))
        self.assertEqual(units.calibrate('1000', 'mV', 'V', '2', '1'), Decimal('3'))
        with self.assertRaises(PipelineError):
            units.convert('1', 'C', 'Pa')
        with self.assertRaises(PipelineError):
            units.describe('unknown')
        self.assertEqual({x['dimension'] for x in units.supported('temperature')}, {'temperature'})

    def test_csv_and_ndjson_roundtrip(self):
        raw = [sample(1, '20.100'), sample(2, '-1.5')]
        for format in ('csv', 'ndjson'):
            with self.subTest(format=format):
                encoded = sensors.encode(raw, format)
                rows = list(sensors.records(encoded, format))
                self.assertEqual(len(rows), 2)
                self.assertIsNone(rows[0][2])
                self.assertEqual(rows[0][1]['value'], '20.1')
                self.assertEqual(rows[1][1]['value'], '-1.5')
                self.assertEqual(rows[0][1]['observed_at'], instant(raw[0]['observed_at']))

    def test_json_bad_lines_are_retained_as_errors(self):
        content = canonical(sample()).encode() + b'\n{"a":1,"a":2}\n\xff\n\n'
        rows = list(sensors.records(content, 'ndjson'))
        self.assertEqual(len(rows), 3)
        self.assertIsNone(rows[0][2])
        self.assertEqual(rows[1][2]['code'], 'duplicate_key')
        self.assertEqual(rows[2][2]['code'], 'malformed_json')
        self.assertEqual(rows[1][0], 2)
        self.assertIn('raw', rows[2][1])

    def test_csv_header_and_missing_columns(self):
        with self.assertRaises(PipelineError):
            list(sensors.records(b'wrong,header\n1,2\n', 'csv'))
        raw = b'event_id,device,observed_at,value,unit\nevent1,sensor1,2026-11-11T00:00:00Z,1\n'
        row = list(sensors.records(raw, 'csv'))[0]
        self.assertEqual(row[2]['code'], 'csv_columns')
        self.assertEqual(row[0], 2)
        with self.assertRaises(PipelineError):
            list(sensors.records(b'\xff', 'csv'))
        with self.assertRaises(PipelineError):
            list(sensors.records(b'', 'xml'))


class ConfigurationTests(PipelineCase):
    def test_device_identity_is_immutable(self):
        duplicate = devices.register(self.db, 'dev1', 'plane1', 'engine1', 'temperature', 'C')
        self.assertEqual(duplicate['revision'], 1)
        with self.assertRaises(PipelineError):
            devices.register(self.db, 'dev1', 'plane2', 'engine1', 'temperature', 'C')
        self.assertEqual(len(devices.list_devices(self.db)), 1)
        disabled = devices.set_active(self.db, 'dev1', 1, False)
        self.assertEqual(disabled['revision'], 2)
        with self.assertRaises(PipelineError):
            devices.resolve_alias(self.db, 'feed1', 'sensor1', START)
        with self.assertRaises(PipelineError):
            devices.set_active(self.db, 'dev1', 1, True)
        self.assertFalse(bool(devices.device(self.db, 'dev1')['active']))

    def test_alias_intervals_are_half_open(self):
        devices.register(self.db, 'dev2', 'plane1', 'engine2', 'temperature', 'C')
        devices.add_alias(self.db, 'feed2', 'sensor', 'dev1', START, END)
        devices.add_alias(self.db, 'feed2', 'sensor', 'dev2', END, '2026-11-13T00:00:00Z')
        self.assertEqual(devices.resolve_alias(self.db, 'feed2', 'sensor', START)['device_id'], 'dev1')
        self.assertEqual(devices.resolve_alias(self.db, 'feed2', 'sensor', END)['device_id'], 'dev2')
        with self.assertRaises(PipelineError):
            devices.add_alias(self.db, 'feed2', 'sensor', 'dev1', START, '2026-11-13T00:00:00Z')
        with self.assertRaises(PipelineError):
            devices.resolve_alias(self.db, 'feed2', 'missing', START)
        self.assertEqual(len(devices.list_devices(self.db, 'plane1')), 2)

    def test_calibration_overlap_and_compare_and_swap(self):
        with self.assertRaises(PipelineError):
            devices.add_calibration(self.db, 'bad', 'dev1', START, END)
        with self.assertRaises(PipelineError):
            devices.correct_calibration(self.db, 'cal1', 2, '2', '0', 0)
        updated = devices.correct_calibration(self.db, 'cal1', 1, '2', '-1', 1000000)
        self.assertEqual(updated['revision'], 2)
        self.assertEqual(updated['scale'], '2')
        self.assertEqual(updated['offset'], '-1')
        self.assertEqual(devices.calibration_at(self.db, 'dev1', START)['clock_offset_us'], 1000000)
        with self.assertRaises(PipelineError):
            devices.correct_calibration(self.db, 'cal1', 2, '0', '0', 0)
        self.assertEqual(devices.calibration_at(self.db, 'dev1', START)['revision'], 2)

    def test_quality_rules_validate_ranges_and_versions(self):
        with self.assertRaises(PipelineError):
            rules.configure(self.db, 'dev1', revision=1, minimum='10', maximum='0')
        with self.assertRaises(PipelineError):
            rules.configure(self.db, 'dev1', revision=0)
        updated = rules.configure(self.db, 'dev1', revision=1, minimum='0', maximum='100', max_delta='10', duplicate_policy='keep_first')
        self.assertEqual(updated['revision'], 2)
        self.assertEqual(rules.bounds('0', updated), [])
        self.assertEqual(rules.bounds('100', updated), [])
        self.assertEqual(rules.bounds('-1', updated)[0]['code'], 'below_minimum')
        self.assertEqual(rules.bounds('101', updated)[0]['code'], 'above_maximum')
        self.assertEqual(updated['duplicate_policy'], 'keep_first')

    def test_work_order_versions_and_terminal_state(self):
        first = workorders.upsert(self.db, order())
        self.assertEqual(first['status'], 'created')
        self.assertEqual(workorders.upsert(self.db, order())['status'], 'duplicate')
        with self.assertRaises(PipelineError):
            workorders.upsert(self.db, order(revision=3))
        workorders.upsert(self.db, order(revision=2, state='cancelled'))
        with self.assertRaises(PipelineError):
            workorders.upsert(self.db, order(revision=3, state='closed'))
        self.assertEqual(workorders.get(self.db, 'order1', 1)['state'], 'closed')
        self.assertEqual(workorders.get(self.db, 'order1')['state'], 'cancelled')
        self.assertEqual(len(workorders.history(self.db, 'order1')), 2)

    def test_work_order_import_rolls_back_on_later_invalid_order(self):
        before = self.db.scalar('SELECT COUNT(*) FROM audit')
        invalid = order('bad')
        invalid['closed_at'] = invalid['opened_at']
        with self.assertRaises(PipelineError):
            workorders.import_orders(self.db, [order(), invalid])
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM work_orders'), 0)
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM partitions'), 0)
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM audit'), before)
        self.assertTrue(self.db.integrity()['ok'])
