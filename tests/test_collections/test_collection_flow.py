"""采集编排状态机测试（假供应商，可控故障序列）。"""
import copy

from hangar_ingest.types import PipelineError, canonical, instant
from hangar_ingest.decoders import sensors
from hangar_ingest.runs import inspection, journal
from hangar_ingest.exports import watermarks
from hangar_ingest.collections.client import SupplierRejected, SupplierUnavailable
from hangar_ingest.collections.runner import CollectionRunner, verify_shard
from hangar_ingest.collections.store import manifest_fingerprint, normalize_manifest
from tests.support import PipelineCase, START, END

DEADLINE = '2026-11-13T00:00:00Z'


def shard_blob(rows):
    return sensors.encode(rows)


class FakeSupplier:
    """按方法名排队返回结果；元素是异常或 (返回字典)。"""

    def __init__(self):
        self.queues = {'open': [], 'get': [], 'refresh': [], 'download': {}}
        self.calls = []
        self.idempotency_keys = []

    def open_export(self, source, start, end, config_revision, idempotency_key):
        self.calls.append(('open', idempotency_key))
        self.idempotency_keys.append(idempotency_key)
        return self._next('open')

    def get_export(self, export_id):
        self.calls.append(('get', export_id))
        return self._next('get')

    def refresh(self, export_id):
        self.calls.append(('refresh', export_id))
        return self._next('refresh')

    def download(self, url):
        self.calls.append(('download', url))
        sequence = self.queues['download'].setdefault(url, [])
        if not sequence:
            raise AssertionError(f'unexpected download for {url}')
        value = sequence.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    def _next(self, name):
        sequence = self.queues[name]
        if not sequence:
            raise AssertionError(f'unexpected call {name}')
        value = sequence.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


class CollectionCase(PipelineCase):
    def setUp(self):
        super().setUp()
        self.supplier = FakeSupplier()

        def sleep_and_tick(seconds):
            self.sleeps.append(seconds)
            self.clock.tick(seconds)

        self.sleeps = []
        self.runner = CollectionRunner(self.db, self.supplier, backoff=1, sleeper=sleep_and_tick)

    def start(self, collection_id='col1', *, max_attempts=20, deadline=DEADLINE, export_id=None):
        return self.runner.start(collection_id, 'feed1', START, END, 1,
                                 max_attempts=max_attempts, deadline=deadline, export_id=export_id)

    def manifest(self, blobs, *, revision=1, urls=None):
        shards = []
        for index, (shard_id, blob) in enumerate(blobs.items()):
            import hashlib
            records = len([line for line in blob.splitlines() if line.strip()])
            shards.append({
                'id': shard_id,
                'url': (urls or {}).get(shard_id, f'/download/{shard_id}'),
                'bytes': len(blob),
                'sha256': hashlib.sha256(blob).hexdigest(),
                'records': records,
            })
        return {'export_id': 'exp1', 'revision': revision, 'shards': shards}

    def ready(self, manifest, *, status=200, export_status='ready'):
        return {'status': status, 'retry_after': None, 'payload': {'status': export_status, 'manifest': manifest}}

    def queued(self, status='running', retry_after=1):
        return {'status': 202, 'retry_after': retry_after, 'payload': {'status': status}}

    def run_to_end(self, collection_id='col1'):
        return self.runner.run(collection_id)

    def status(self, collection_id='col1'):
        return self.runner.status(collection_id)


class HappyPathTests(CollectionCase):
    def test_full_collection_advances_watermark_and_imports_shards(self):
        blob_a = shard_blob([{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'},
                             {'event_id': 'event2', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:02Z', 'value': '30', 'unit': 'C'}])
        blob_b = shard_blob([{'event_id': 'event3', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:03Z', 'value': '40', 'unit': 'C'}])
        manifest = self.manifest({'a': blob_a, 'b': blob_b})
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.queued())
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [blob_a]
        self.supplier.queues['download']['/download/b'] = [blob_b]

        self.start()
        final = self.run_to_end()
        self.assertEqual(final['state'], 'completed')
        self.assertEqual(final['watermark'], instant(END))
        self.assertEqual(self.db.one('SELECT * FROM source_watermarks WHERE source=?', ('feed1',))['watermark'], instant(END))
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 3)
        status = self.status()
        self.assertEqual(status['stage'], 'completed')
        self.assertEqual(status['shards']['verified'], 2)
        self.assertEqual(status['manifest_revision'], 1)
        self.assertEqual(status['summary']['records_total'], 3)
        self.assertEqual(status['summary']['records_quarantined'], 0)
        self.assertTrue(inspection.check(self.db)['ok'])
        self.assertTrue(journal.verify(self.db)['ok'])

    def test_queued_phase_is_visible_while_waiting(self):
        # 该用例只记录等待、不推进时钟，以便稳定观察 waiting 状态。
        self.runner = CollectionRunner(self.db, self.supplier, backoff=1, sleeper=self.sleeps.append)
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.queued(retry_after=30))
        self.start()
        self.runner.step('col1')  # 请求受理，进入排队（尚未设定等待窗口）
        self.assertEqual(self.status()['phase'], 'queued')
        self.runner.step('col1')  # 轮询仍在排队，记录 Retry-After
        status = self.status()
        self.assertEqual(status['stage'], 'queued_at_supplier')
        self.assertTrue(status['waiting'])
        self.assertEqual(status['not_before'], instant('2026-11-12T00:00:30Z'))
        self.assertEqual(status['export_status'], 'running')
        # 等待期内 run 不会产生新调用
        before = len(self.supplier.calls)
        self.assertEqual(self.run_to_end()['state'], 'queued')
        self.assertEqual(len(self.supplier.calls), before)
        # 越过等待窗口后，run 才会再次轮询供应商并走完整流程
        self.clock.tick(31)
        blob = shard_blob([{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}])
        self.supplier.queues['get'].append(self.ready(self.manifest({'a': blob})))
        self.supplier.queues['download']['/download/a'] = [blob]
        self.assertEqual(self.run_to_end()['state'], 'completed')

    def test_download_and_verifying_progress_are_visible(self):
        blob_a = shard_blob([{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}])
        manifest = self.manifest({'a': blob_a})
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [blob_a]
        self.start()
        self.runner.step('col1')  # 受理，进入排队
        self.runner.step('col1')  # 拿到清单，进入下载
        self.assertEqual(self.status()['phase'], 'downloading')
        self.assertEqual(self.status()['shards']['verified'], 0)
        self.runner.step('col1')  # 下载 a，进入核对
        self.assertEqual(self.status()['phase'], 'verifying')
        self.assertEqual(self.status()['stage'], 'verifying_shards')
        self.assertEqual(self.status()['shards']['detail'][0]['state'], 'verifying')
        self.runner.step('col1')  # 核对通过
        self.assertEqual(self.status()['shards']['verified'], 1)


class RetryIdentityTests(CollectionCase):
    def test_same_window_identity_reuses_collection_and_idempotency_key(self):
        self.supplier.queues['open'].append(SupplierUnavailable('connection reset'))
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.queued())
        first = self.start()
        self.assertFalse(first.get('duplicate'))
        self.runner.step('col1')  # 首次申请失败
        self.assertEqual(self.status()['state'], 'requesting')
        second = self.start()
        self.assertTrue(second['duplicate'])
        self.assertEqual(second['collection_id'], 'col1')
        self.assertEqual(len(set(self.supplier.idempotency_keys)), 1)
        self.clock.tick(1)
        self.runner.step('col1')  # 重试申请，同幂等键
        self.assertEqual(self.status()['export_id'], 'exp1')

    def test_reattach_recovers_export_when_accept_response_was_lost(self):
        blob = shard_blob([{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}])
        manifest = self.manifest({'a': blob})
        # 本地从未记录 open 响应，只凭供应商受理号直接轮询找回。
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [blob]
        self.start(export_id='exp1')
        final = self.run_to_end()
        self.assertEqual(final['state'], 'completed')
        self.assertEqual(self.supplier.calls[0][0], 'get')
        self.assertFalse(any(call[0] == 'open' for call in self.supplier.calls))

    def test_supplier_returning_different_export_id_is_rejected(self):
        # 首次申请已记录受理号后，供应商又给出另一个受理号，必须拒绝。
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.start()
        self.runner.step('col1')
        with self.assertRaises(PipelineError) as failure:
            self.runner.store.attach_export('col1', 'exp2', 'queued')
        self.assertEqual(failure.exception.code, 'export_identity')
        # 清单里携带别的 export_id 同样被拦
        blob = shard_blob([{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}])
        foreign = self.manifest({'a': blob})
        foreign['export_id'] = 'exp2'
        with self.assertRaises(PipelineError) as failure:
            self.runner.store.adopt_manifest('col1', foreign)
        self.assertEqual(failure.exception.code, 'export_identity')


class ManifestVerificationTests(CollectionCase):
    def _one_shard(self):
        blob = shard_blob([{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}])
        return blob, self.manifest({'a': blob})

    def test_resigned_link_keeps_same_bytes_but_new_url(self):
        blob, manifest = self._one_shard()
        resigned = copy.deepcopy(manifest)
        resigned['shards'][0]['url'] = '/download/a-signed'
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a-signed'] = [blob]
        self.start()
        self.runner.step('col1')  # 受理
        self.runner.step('col1')  # 拿到清单
        # 换签后再次收到同版清单：只允许 URL 变化，指纹保持不变
        self.runner.store.adopt_manifest('col1', resigned)
        self.assertEqual(self.runner.store.get_shard('col1', 'a')['url'], '/download/a-signed')
        self.assertEqual(self.status()['manifest_digest'], manifest_fingerprint(normalize_manifest(manifest)))
        final = self.run_to_end()
        self.assertEqual(final['state'], 'completed')

    def test_same_revision_with_changed_bytes_blocks_watermark(self):
        blob, manifest = self._one_shard()
        tampered = copy.deepcopy(manifest)
        tampered['shards'][0]['sha256'] = 'f' * 64
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.start()
        self.runner.step('col1')  # 受理
        self.runner.step('col1')  # 拿到清单
        with self.assertRaises(PipelineError) as failure:
            self.runner.store.adopt_manifest('col1', tampered)  # 同版字节变化
        self.assertEqual(failure.exception.code, 'manifest_conflict')
        self.assertIsNone(self.db.one('SELECT * FROM source_watermarks WHERE source=?', ('feed1',)))

    def test_new_manifest_revision_blocks_window(self):
        blob, manifest = self._one_shard()
        revised = copy.deepcopy(manifest)
        revised['revision'] = 2
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.start()
        self.runner.step('col1')  # 受理
        self.runner.step('col1')  # 拿到清单
        with self.assertRaises(PipelineError) as failure:
            self.runner.store.adopt_manifest('col1', revised)  # 收到 revision=2
        self.assertEqual(failure.exception.code, 'manifest_revision_changed')
        self.assertEqual(self.status()['manifest_revision'], 1)

    def test_runner_terminates_when_poll_returns_changed_revision(self):
        # 第一次轮询拿到 revision 1 清单，第二次轮询返回 revision 2，runner 必须终止且不推进水位。
        blob, manifest = self._one_shard()
        revised = copy.deepcopy(manifest)
        revised['revision'] = 2
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].extend([self.ready(manifest), self.ready(revised)])
        self.start()
        self.runner.step('col1')  # 受理
        self.runner.step('col1')  # 清单 revision 1，进入下载
        # 换版清单经由下一次轮询进入，任务应被终止
        with self.assertRaises(PipelineError):
            self.runner.store.adopt_manifest('col1', revised)
        self.assertEqual(self.runner.store.get('col1')['state'], 'downloading')  # 直接调用未走 runner 终止
        # 经 runner 路径：模拟换签返回改版清单
        self.supplier.queues['refresh'].append({'status': 200, 'retry_after': None, 'payload': revised})
        shard = self.runner.store.list_shards('col1')[0]
        with self.assertRaises(PipelineError):
            self.runner._try_refresh('col1', shard, {'status': 403},
                                     self.runner.store.start_attempt('col1', 'downloading'))
        self.assertEqual(self.runner.store.get('col1')['state'], 'failed')
        self.assertEqual(self.runner.store.get('col1')['last_error']['code'], 'manifest_revision_changed')
        self.assertIsNone(self.db.one('SELECT * FROM source_watermarks WHERE source=?', ('feed1',)))

    def test_shard_digest_mismatch_fails_collection(self):
        blob, manifest = self._one_shard()
        wrong = blob + b'{"x":1}\n'
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [wrong]
        self.start()
        self.run_to_end()
        final = self.runner.store.get('col1')
        self.assertEqual(final['state'], 'failed')
        self.assertEqual(final['last_error']['code'], 'shard_length_mismatch')
        self.assertIsNone(self.db.one('SELECT * FROM source_watermarks WHERE source=?', ('feed1',)))

    def test_record_count_mismatch_fails_collection(self):
        rows = [{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}]
        blob = shard_blob(rows)
        manifest = self.manifest({'a': blob})
        manifest['shards'][0]['records'] = 5
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [blob]
        self.start()
        self.run_to_end()
        self.assertEqual(self.runner.store.get('col1')['last_error']['code'], 'shard_record_count_mismatch')

    def test_missing_shard_blocks_import_and_watermark(self):
        blob, manifest = self._one_shard()
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [SupplierRejected('gone', status=404)]
        # 换签也返回 404，分片始终缺失
        self.supplier.queues['refresh'].append(SupplierRejected('gone', status=404))
        self.start(max_attempts=8)
        self.run_to_end()
        self.assertEqual(self.runner.store.get('col1')['state'], 'failed')
        self.assertIn(self.runner.store.get('col1')['last_error']['code'],
                      {'shard_missing', 'supplier_rejected'})
        self.assertIsNone(self.db.one('SELECT * FROM source_watermarks WHERE source=?', ('feed1',)))


class LinkRefreshTests(CollectionCase):
    def test_expired_link_refreshes_and_downloads_same_bytes(self):
        blob = shard_blob([{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}])
        manifest = self.manifest({'a': blob}, urls={'a': '/download/a-old'})
        resigned = copy.deepcopy(manifest)
        resigned['shards'][0]['url'] = '/download/a-new'
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a-old'] = [SupplierRejected('链接已过期', status=403)]
        self.supplier.queues['refresh'].append({'status': 200, 'retry_after': None, 'payload': resigned})
        self.supplier.queues['download']['/download/a-new'] = [blob]
        self.start()
        final = self.run_to_end()
        self.assertEqual(final['state'], 'completed')
        self.assertEqual(self.runner.store.get_shard('col1', 'a')['url'], '/download/a-new')
        self.assertTrue(any(call[0] == 'refresh' for call in self.supplier.calls))


class TransientRetryTests(CollectionCase):
    def test_unavailable_download_retries_within_budget(self):
        blob = shard_blob([{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}])
        manifest = self.manifest({'a': blob})
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [
            SupplierUnavailable('timeout'), SupplierUnavailable('reset'), blob,
        ]
        self.start(max_attempts=10)
        final = self.run_to_end()
        self.assertEqual(final['state'], 'completed')
        self.assertGreaterEqual(self.status()['attempts_used'], 3)

    def test_attempt_limit_marks_failed_and_keeps_watermark(self):
        self.supplier.queues['open'] = [SupplierUnavailable('down') for _ in range(5)]
        self.start(max_attempts=2)
        final = self.run_to_end()
        self.assertEqual(final['state'], 'failed')
        self.assertEqual(final['last_error']['code'], 'attempt_limit')
        self.assertIsNone(self.db.one('SELECT * FROM source_watermarks WHERE source=?', ('feed1',)))

    def test_deadline_blocks_further_attempts(self):
        self.supplier.queues['open'] = [SupplierUnavailable('down') for _ in range(3)]
        self.start(max_attempts=50, deadline='2026-11-12T00:00:02Z')
        self.runner.step('col1')
        self.clock.tick(5)
        final = self.run_to_end()
        self.assertEqual(final['state'], 'failed')
        self.assertEqual(final['last_error']['code'], 'deadline_exceeded')


class PauseCancelTests(CollectionCase):
    def _manifest(self):
        blob_a = shard_blob([{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}])
        blob_b = shard_blob([{'event_id': 'event2', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:02Z', 'value': '30', 'unit': 'C'}])
        return blob_a, blob_b, self.manifest({'a': blob_a, 'b': blob_b})

    def test_pause_restart_keeps_verified_shard(self):
        blob_a, blob_b, manifest = self._manifest()
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [blob_a]
        self.supplier.queues['download']['/download/b'] = [blob_b]
        self.start()
        self.runner.step('col1')  # 受理
        self.runner.step('col1')  # 清单
        self.runner.step('col1')  # 下载 a
        revision = self.status()['revision']
        paused = self.runner.store.pause('col1', revision)
        self.assertEqual(paused['state'], 'paused')
        with self.assertRaises(PipelineError):
            self.runner.step('col1')
        self.runner.store.resume('col1', paused['revision'])
        final = self.run_to_end()
        self.assertEqual(final['state'], 'completed')
        # a 只下载一次，重启后直接复用已验字节
        self.assertEqual(len([c for c in self.supplier.calls if c == ('download', '/download/a')]), 1)
        self.assertEqual(len([c for c in self.supplier.calls if c == ('download', '/download/b')]), 1)

    def test_cancelled_late_response_cannot_complete_old_window(self):
        blob_a, blob_b, manifest = self._manifest()
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [blob_a]
        self.supplier.queues['download']['/download/b'] = [blob_b]
        self.start()
        self.runner.step('col1')  # 受理
        self.runner.step('col1')  # 清单
        self.runner.step('col1')  # 下载 a
        revision = self.status()['revision']
        cancelled = self.runner.store.cancel('col1', revision, 'operator abort')
        self.assertEqual(cancelled['state'], 'cancelled')
        # 迟到的分片响应到达后，任何 step/run 都不能复活旧窗口
        with self.assertRaises(PipelineError) as failure:
            self.runner.step('col1')
        self.assertEqual(failure.exception.code, 'collection_state')
        self.assertEqual(self.run_to_end()['state'], 'cancelled')
        self.assertIsNone(self.db.one('SELECT * FROM source_watermarks WHERE source=?', ('feed1',)))
        # 同身份重新申请也被拒绝，避免迟到数据完成旧窗口
        with self.assertRaises(PipelineError) as restart:
            self.start()
        self.assertEqual(restart.exception.code, 'collection_cancelled')

    def test_restart_from_disk_preserves_verified_blobs(self):
        blob_a, blob_b, manifest = self._manifest()
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [blob_a]
        self.supplier.queues['download']['/download/b'] = [blob_b]
        self.start()
        self.runner.step('col1')  # 受理
        self.runner.step('col1')  # 清单
        self.runner.step('col1')  # 下载 a，进入核对
        self.runner.step('col1')  # 核对 a 通过
        # 模拟进程重启：重新打开数据库与 runner
        from hangar_ingest.runs.database import Database
        self.db.close()
        self.db = Database(self.path, clock=self.clock)
        self.runner = CollectionRunner(self.db, self.supplier, backoff=1, sleeper=self.sleeps.append)
        self.assertEqual(self.runner.status('col1')['shards']['verified'], 1)
        final = self.runner.run('col1')
        self.assertEqual(final['state'], 'completed')
        self.assertEqual(len([c for c in self.supplier.calls if c == ('download', '/download/a')]), 1)

    def test_restart_while_verifying_resumes_without_redownload(self):
        blob_a, blob_b, manifest = self._manifest()
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [blob_a]
        self.supplier.queues['download']['/download/b'] = [blob_b]
        self.start()
        self.runner.step('col1')  # 受理
        self.runner.step('col1')  # 清单
        self.runner.step('col1')  # 下载 a，停在核对阶段
        self.assertEqual(self.runner.status('col1')['phase'], 'verifying')
        from hangar_ingest.runs.database import Database
        self.db.close()
        self.db = Database(self.path, clock=self.clock)
        self.runner = CollectionRunner(self.db, self.supplier, backoff=1, sleeper=self.sleeps.append)
        final = self.runner.run('col1')
        self.assertEqual(final['state'], 'completed')
        # a 只下载一次，重启后用落地字节继续核对
        self.assertEqual(len([c for c in self.supplier.calls if c == ('download', '/download/a')]), 1)


class QualityIsolationTests(CollectionCase):
    def test_record_level_quality_issue_is_quarantined_but_window_completes(self):
        good = {'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}
        bad = {'event_id': 'event2', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:02Z', 'value': '9999', 'unit': 'C'}
        blob = shard_blob([good, bad])
        manifest = self.manifest({'a': blob})
        self.supplier.queues['open'].append({'status': 202, 'retry_after': None, 'payload': {'export_id': 'exp1', 'status': 'queued'}})
        self.supplier.queues['get'].append(self.ready(manifest))
        self.supplier.queues['download']['/download/a'] = [blob]
        self.start()
        final = self.run_to_end()
        self.assertEqual(final['state'], 'completed')
        self.assertEqual(final['watermark'], instant(END))
        status = self.status()
        self.assertEqual(status['summary']['records_quarantined'], 1)
        self.assertEqual(status['summary']['records_accepted'], 1)
        self.assertEqual(status['summary']['partial_batches'], ['col1:a'])
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM quarantine'), 1)
        self.assertTrue(inspection.check(self.db)['ok'])


class ShardVerifyHelperTests(CollectionCase):
    def test_verify_shard_reports_length_digest_and_count(self):
        blob = shard_blob([{'event_id': 'event1', 'device': 'sensor1', 'observed_at': '2026-11-11T10:00:01Z', 'value': '20', 'unit': 'C'}])
        import hashlib
        shard = {'shard_id': 'a', 'url': '/x', 'bytes': len(blob),
                 'sha256': hashlib.sha256(blob).hexdigest(), 'records': 1}
        self.assertIsNone(verify_shard(shard, blob))
        self.assertEqual(verify_shard({**shard, 'bytes': 99}, blob)['code'], 'shard_length_mismatch')
        self.assertEqual(verify_shard({**shard, 'sha256': '0' * 64}, blob)['code'], 'shard_digest_mismatch')
        self.assertEqual(verify_shard({**shard, 'records': 2}, blob)['code'], 'shard_record_count_mismatch')
