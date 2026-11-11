import io
import json
from pathlib import Path
from hangar_ingest.types import PipelineError, canonical, digest
from hangar_ingest.api import Application
from hangar_ingest.cli import main, write_new
from hangar_ingest.runs import batches, inspection, journal, partitions
from hangar_ingest.quality import preflight, devices, coverage, recalibration
from hangar_ingest.decoders import sensors, workorders
from hangar_ingest.exports import releases, watermarks
from hangar_ingest.runs.database import Database
from tests.support import PipelineCase, sample, order, START, END, PARTITION


class ApplicationTests(PipelineCase):
    def test_application_discovers_operations_and_validates_inputs(self):
        app = Application(self.db)
        commands = app.commands()
        self.assertIn('batch.stage', commands)
        self.assertIn('alignment.pairs', commands)
        self.assertIn('transfer.finish', commands)
        self.assertIn('preflight.source', commands)
        self.assertEqual(commands, sorted(set(commands)))
        with self.assertRaises(PipelineError) as unknown:
            app.call('missing')
        self.assertEqual(unknown.exception.code, 'unknown_operation')
        with self.assertRaises(PipelineError):
            app.call('device.get', {'device_id': 'dev1', 'typo': True})
        with self.assertRaises(PipelineError):
            app.call('device.get', {})
        self.assertEqual(app.call('device.get', {'device_id': 'dev1'})['aircraft'], 'plane1')

    def test_application_atomic_batch_rolls_back_first_command(self):
        app = Application(self.db)
        before = journal.verify(self.db)
        requests = [
            {'operation': 'device.register', 'parameters': {'device_id': 'new', 'aircraft': 'plane1', 'component': 'engine1', 'metric': 'pressure', 'canonical_unit': 'Pa'}},
            {'operation': 'device.get', 'parameters': {'device_id': 'absent'}},
        ]
        with self.assertRaises(PipelineError):
            app.batch(requests)
        self.assertEqual(len(devices.list_devices(self.db)), 1)
        self.assertEqual(journal.verify(self.db), before)
        self.assertTrue(self.db.integrity()['ok'])
        self.assertEqual(app.call('inspect.overview')['devices'], 1)

    def test_application_stage_process_and_inspect(self):
        app = Application(self.db)
        content = sensors.encode([sample()]).decode()
        staged = app.call('batch.stage', {'batch_id': 'batch1', 'source': 'feed1', 'content': content})
        self.assertEqual(staged['state'], 'staged')
        result = app.call('batch.drain', {'batch_id': 'batch1'})
        self.assertEqual(result['batch']['state'], 'complete')
        report = app.call('inspect.batch', {'batch_id': 'batch1'})
        self.assertTrue(report['checkpoint']['ok'])
        self.assertEqual(report['row_states'], {'accepted': 1})
        observation_id = self.db.scalar('SELECT observation_id FROM observations')
        trace = app.call('inspect.observation', {'observation_id': observation_id})
        self.assertEqual(trace['raw']['payload']['event_id'], 'event1')
        self.assertFalse(trace['calibration_stale'])
        self.assertEqual(trace['batch']['source'], 'feed1')

    def test_cli_returns_json_errors_and_success_codes(self):
        output = io.StringIO()
        error = io.StringIO()
        code = main(['--db', ':memory:', 'init'], stdout=output, stderr=error)
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(output.getvalue())['result']['initialized'])
        self.assertEqual(error.getvalue(), '')
        output = io.StringIO()
        error = io.StringIO()
        code = main(['--db', ':memory:', 'call', 'device.get', '--json', '{"device_id":"missing"}'], stdout=output, stderr=error)
        self.assertEqual(code, 2)
        self.assertEqual(output.getvalue(), '')
        self.assertEqual(json.loads(error.getvalue())['error']['code'], 'unknown_device')
        self.assertFalse(json.loads(error.getvalue())['ok'])

    def test_cli_ingest_and_export_real_file(self):
        path = self.path.parent / 'feed.csv'
        path.write_bytes(sensors.encode([sample()], 'csv'))
        output = io.StringIO()
        error = io.StringIO()
        code = main(['--db', str(self.path), 'ingest', str(path), '--batch', 'batch1', '--source', 'feed1', '--format', 'csv'], stdout=output, stderr=error)
        self.assertEqual(code, 0)
        result = json.loads(output.getvalue())['result']
        self.assertEqual(result['batch']['accepted'], 1)
        self.assertEqual(error.getvalue(), '')
        self.release()
        destination = self.path.parent / 'export.ndjson'
        output = io.StringIO()
        code = main(['--db', str(self.path), 'export', 'release1', str(destination), '--format', 'ndjson'], stdout=output, stderr=error)
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(destination.read_text())['value'], '20')
        self.assertGreater(json.loads(output.getvalue())['result']['bytes'], 0)

    def test_new_file_writer_refuses_overwrite_and_symlink(self):
        path = self.path.parent / 'new.bin'
        written = write_new(path, b'abc')
        self.assertEqual(written['bytes'], 3)
        self.assertEqual(path.read_bytes(), b'abc')
        with self.assertRaises(PipelineError):
            write_new(path, b'def')
        self.assertEqual(path.read_bytes(), b'abc')
        link = self.path.parent / 'link.bin'
        link.symlink_to(path)
        with self.assertRaises(PipelineError):
            write_new(link, b'xyz')
        self.assertEqual(path.read_bytes(), b'abc')

    def test_backup_restores_observations_and_journal(self):
        self.ingest([sample(1), sample(2)])
        destination = self.path.parent / 'backup.db'
        receipt = self.db.backup(destination)
        self.assertGreater(receipt['bytes'], 0)
        with Database(destination) as other:
            self.assertEqual(other.scalar('SELECT COUNT(*) FROM observations'), 2)
            self.assertEqual(journal.verify(other), journal.verify(self.db))
            self.assertTrue(inspection.check(other)['ok'])
        with self.assertRaises(PipelineError):
            self.db.backup(destination)
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 2)

    def test_integrity_check_detects_counter_and_partition_damage(self):
        self.ingest([sample()])
        partitions.materialize(self.db, PARTITION)
        self.assertTrue(inspection.check(self.db)['ok'])
        self.db.execute('UPDATE batches SET accepted=9 WHERE batch_id=?', ('batch1',))
        damaged = inspection.check(self.db)
        self.assertFalse(damaged['ok'])
        self.assertIn('checkpoint_integrity', {row['code'] for row in damaged['errors']})
        self.db.execute('UPDATE batches SET accepted=1 WHERE batch_id=?', ('batch1',))
        self.db.execute("UPDATE partition_versions SET digest='bad'")
        self.assertIn('partition_digest', {row['code'] for row in inspection.check(self.db)['errors']})
        with self.assertRaises(PipelineError):
            partitions.version(self.db, PARTITION)

    def test_journal_damage_is_reported(self):
        self.ingest([sample()])
        self.assertTrue(journal.verify(self.db)['ok'])
        entries = journal.page(self.db, limit=2)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]['action'], 'device.registered')
        self.db.execute("UPDATE audit SET details='{}' WHERE position=1")
        self.assertFalse(journal.verify(self.db)['ok'])
        checked = inspection.check(self.db)
        self.assertFalse(checked['ok'])
        self.assertIn('journal_integrity', {row['code'] for row in checked['errors']})

    def test_preflight_identifies_missing_aliases_calibrations_and_rules(self):
        ready = preflight.source_configuration(self.db, 'feed1', START, END)
        self.assertTrue(ready['ready'])
        self.assertEqual(ready['errors'], [])
        unknown = preflight.source_configuration(self.db, 'missing', START, END)
        self.assertFalse(unknown['ready'])
        self.assertEqual(unknown['errors'][0]['code'], 'no_aliases')
        devices.register(self.db, 'unconfigured', 'plane2', 'engine1', 'pressure', 'Pa')
        orphans = preflight.orphan_configuration(self.db)
        self.assertIn('unconfigured', orphans['devices_without_alias'])
        self.assertIn('unconfigured', orphans['devices_without_calibration'])
        self.assertEqual(len(preflight.device_inventory(self.db)), 2)

    def test_delivery_preflight_reports_stale_calibration(self):
        self.ingest([sample()])
        self.release()
        self.assertTrue(preflight.delivery_readiness(self.db, [PARTITION])['ready'])
        devices.correct_calibration(self.db, 'cal1', 1, '1', '1', 0)
        report = preflight.delivery_readiness(self.db, [PARTITION])
        self.assertFalse(report['ready'])
        self.assertIn('stale_calibrations', {row['code'] for row in report['errors']})
        self.assertEqual(len(recalibration.stale_observations(self.db)), 1)
        recalibration.apply(self.db, 'cal1', 2)
        self.assertEqual(recalibration.stale_observations(self.db), [])
        self.assertFalse(preflight.delivery_readiness(self.db, [PARTITION])['ready'])
        partitions.rebuild_dirty(self.db)
        self.assertTrue(preflight.delivery_readiness(self.db, [PARTITION])['ready'])

    def test_release_lineage_retains_used_calibration_revision(self):
        self.ingest([sample()])
        self.release()
        devices.correct_calibration(self.db, 'cal1', 1, '1', '1', 0)
        recalibration.apply(self.db, 'cal1', 2)
        lineage = inspection.lineage(self.db, 'release1')
        self.assertEqual(lineage['observations'], 1)
        self.assertEqual(lineage['batches'][0]['batch_id'], 'batch1')
        self.assertEqual(lineage['calibrations'], [{'calibration_id': 'cal1', 'revision': 1}])
        self.assertEqual(releases.payloads(self.db, 'release1')['partitions'][0]['payload']['observations'][0]['value'], '20')
        self.assertEqual(self.db.scalar('SELECT value FROM observations'), '21')

    def test_order_readiness_requires_closed_state_and_coverage(self):
        workorders.upsert(self.db, order(opened_at='2026-11-11T10:00:00Z', closed_at='2026-11-11T10:00:02Z', state='open'))
        self.ingest([sample()])
        report = coverage.order_readiness(self.db, 'order1', minimum_ratio='1')
        self.assertFalse(report['ready'])
        self.assertEqual(report['devices'][0]['coverage']['coverage_ratio'], '1')
        workorders.upsert(self.db, order(revision=2, opened_at='2026-11-11T10:00:00Z', closed_at='2026-11-11T10:00:02Z', state='closed'))
        self.assertTrue(coverage.order_readiness(self.db, 'order1', minimum_ratio='1')['ready'])
        with self.assertRaises(PipelineError):
            coverage.order_readiness(self.db, 'order1', minimum_ratio='1.1')
        self.assertTrue(inspection.check(self.db)['ok'])

    def test_same_timestamp_duplicate_values_are_visible(self):
        self.ingest([sample(1, '20', time=START), sample(2, '30', time=START)])
        groups = coverage.duplicate_timestamps(self.db, 'dev1')
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]['count'], 2)
        self.assertTrue(groups[0]['conflicting_values'])
        self.assertEqual({row['value'] for row in groups[0]['observations']}, {'20', '30'})
        partitions.materialize(self.db, PARTITION)
        stats = partitions.version(self.db, PARTITION)['payload']['statistics']['dev1']
        self.assertEqual(stats['mean'], '25')
        self.assertEqual(stats['duration_us'], 0)
        self.assertEqual(stats['count'], 2)
