"""与合成供应商的端到端采集测试，覆盖真实 HTTP 传输与 CLI。"""
import json
import subprocess
import sys
import threading
import unittest
from pathlib import Path

from synthetic_exchange.server import make_server
from hangar_ingest.api import Application
from hangar_ingest.runs.database import Database
from hangar_ingest.quality import devices, rules
from hangar_ingest.collections.client import SupplierClient
from hangar_ingest.collections.runner import CollectionRunner
from hangar_ingest.runs import inspection

SCENARIO = Path(__file__).resolve().parents[2] / 'synthetic_exchange' / 'scenario.json'
DEADLINE = '2099-12-31T00:00:00Z'
WINDOW_START = '2026-11-11T00:00:00Z'
WINDOW_END = '2026-11-12T00:00:00Z'


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.server = make_server(json.loads(SCENARIO.read_text()))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f'http://127.0.0.1:{self.server.server_port}'
        # 使用真实时钟：runner 在无 sleeper 时按真实时间等待供应商 Retry-After。
        self.db = Database(':memory:')
        devices.register(self.db, 'dev1', 'plane1', 'engine1', 'temperature', 'C')
        devices.add_alias(self.db, 'feed1', 'sensor1', 'dev1', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z')
        devices.add_calibration(self.db, 'cal1', 'dev1', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z')
        rules.configure(self.db, 'dev1', minimum='-100', maximum='200')

    def tearDown(self):
        self.db.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)

    def test_full_runner_flow_against_synthetic_supplier(self):
        client = SupplierClient(self.base_url, headers={'Idempotency-Key': 'local-window'}, timeout=5)
        runner = CollectionRunner(self.db, client, backoff=1)
        runner.start('col-e2e', 'feed1', WINDOW_START, WINDOW_END, 1,
                     max_attempts=20, deadline=DEADLINE, idempotency_key='local-window')
        final = runner.run('col-e2e')
        self.assertEqual(final['state'], 'completed')
        status = runner.status('col-e2e')
        self.assertEqual(status['shards']['verified'], 2)
        # event3 的 500 超过规则上限 200，记录级隔离但不阻断窗口
        self.assertEqual(status['summary']['records_total'], 3)
        self.assertEqual(status['summary']['records_accepted'], 2)
        self.assertEqual(status['summary']['records_quarantined'], 1)
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 2)
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM quarantine'), 1)
        self.assertEqual(self.db.one('SELECT watermark FROM source_watermarks WHERE source=?', ('feed1',))['watermark'],
                         '2026-11-12T00:00:00.000000Z')
        self.assertTrue(inspection.check(self.db)['ok'])

    def test_application_operations_and_status_visibility(self):
        app = Application(self.db)
        created = app.call('collection.start', {
            'collection_id': 'col-app', 'source': 'feed1', 'start': WINDOW_START, 'end': WINDOW_END,
            'config_revision': 1, 'max_attempts': 20, 'deadline': DEADLINE,
            'idempotency_key': 'local-window', 'supplier_url': self.base_url,
            'supplier_headers': {'Idempotency-Key': 'local-window'},
        })
        self.assertEqual(created['state'], 'requesting')
        final = app.call('collection.run', {'collection_id': 'col-app', 'supplier_url': self.base_url,
                                            'supplier_headers': {'Idempotency-Key': 'local-window'}})
        self.assertEqual(final['state'], 'completed')
        status = app.call('collection.status', {'collection_id': 'col-app'})
        self.assertEqual(status['stage'], 'completed')
        self.assertEqual(status['shards']['total'], 2)
        self.assertIsNotNone(status['summary'])
        # 同身份重复申请复用同一任务
        again = app.call('collection.start', {
            'collection_id': 'col-app', 'source': 'feed1', 'start': WINDOW_START, 'end': WINDOW_END,
            'config_revision': 1, 'max_attempts': 20, 'deadline': DEADLINE, 'supplier_url': self.base_url,
        })
        self.assertTrue(again['duplicate'])
        self.assertEqual(len(app.call('collection.list', {})), 1)


class CliEndToEndTests(unittest.TestCase):
    def test_cli_collect_command(self):
        server = make_server(json.loads(SCENARIO.read_text()))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            import tempfile
            directory = tempfile.TemporaryDirectory()
            db_path = str(Path(directory.name) / 'cli.db')
            # 先准备设备/别名/规则
            setup = Path(directory.name) / 'setup.json'
            setup.write_text(json.dumps([
                {'operation': 'device.register', 'parameters': {'device_id': 'dev1', 'aircraft': 'plane1', 'component': 'engine1', 'metric': 'temperature', 'canonical_unit': 'C'}},
                {'operation': 'alias.add', 'parameters': {'source': 'feed1', 'external_id': 'sensor1', 'device_id': 'dev1', 'valid_from': '2026-01-01T00:00:00Z', 'valid_until': '2027-01-01T00:00:00Z'}},
                {'operation': 'calibration.add', 'parameters': {'calibration_id': 'cal1', 'device_id': 'dev1', 'valid_from': '2026-01-01T00:00:00Z', 'valid_until': '2027-01-01T00:00:00Z'}},
                {'operation': 'quality.configure', 'parameters': {'device_id': 'dev1', 'minimum': '-100', 'maximum': '200'}},
            ]))
            setup_run = subprocess.run(
                [sys.executable, '-m', 'hangar_ingest', '--db', db_path, 'batch', str(setup)],
                cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(setup_run.returncode, 0, setup_run.stderr)
            result = subprocess.run(
                [sys.executable, '-m', 'hangar_ingest', '--db', db_path, 'collect', 'col-cli',
                 '--source', 'feed1', '--start', WINDOW_START, '--end', WINDOW_END,
                 '--config-revision', '1', '--supplier-url', f'http://127.0.0.1:{server.server_port}',
                 '--idempotency-key', 'local-window',
                 '--max-attempts', '20', '--deadline', DEADLINE],
                cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True, timeout=90,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload['result']['state'], 'completed')
            self.assertEqual(payload['result']['watermark'], '2026-11-12T00:00:00.000000Z')
            directory.cleanup()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(3)


if __name__ == '__main__':
    unittest.main(verbosity=2)
