"""把申请、排队、分片拉取、核对与导入连成一次可恢复的采集。

状态机由 :class:`~hangar_ingest.collections.store.CollectionStore` 持久化，
``step`` 每调用一次推进一个动作；``run`` 只是带退避等待的驱动循环，因此进程
暂停或重启后重新 ``run`` 即可从上次断点继续，已验分片的字节存在本地不重下。

推进水位（seal）是显式动作：缺片、摘要不符、记录数不符或清单改版都会把任务
置为 ``failed`` 并停在原水位；记录级质量问题走既有隔离流程，只计入说明。
"""
import hashlib
import time

from hangar_ingest.decoders.sensors import decode_json
from hangar_ingest.runs import batches, journal, processor
from hangar_ingest.exports import watermarks
from hangar_ingest.types import PipelineError, fail, integer, micros
from hangar_ingest.collections.client import SupplierError, SupplierRejected, SupplierUnavailable
from hangar_ingest.collections.store import CollectionStore

DEFAULT_BACKOFF = 2
MAX_BACKOFF = 60
QUEUED_STATUSES = {'queued', 'running', 'pending', 'accepted'}
READY_STATUSES = {'ready', 'complete', 'completed', 'succeeded'}


class CollectionRunner:
    def __init__(self, db, client, store=None, *, backoff=DEFAULT_BACKOFF, sleeper=None):
        self.db = db
        self.client = client
        self.store = store or CollectionStore(db)
        integer(backoff, 'backoff', 1, MAX_BACKOFF)
        self.backoff = backoff
        self.sleeper = sleeper

    # ------------------------------------------------------------------ 入口
    def start(self, collection_id, source, start, end, config_revision, *, max_attempts, deadline,
              idempotency_key=None, export_id=None):
        return self.store.create(
            collection_id, source, start, end, config_revision,
            max_attempts=max_attempts, deadline=deadline,
            idempotency_key=idempotency_key, export_id=export_id,
        )

    def run(self, collection_id, *, max_steps=None):
        """驱动任务直到终止或暂停。

        遇到供应商排队/退避窗口时，注入了 ``sleeper`` 用它等待（测试里可同时
        推进虚拟时钟）；否则用真实时间休眠，直到 ``not_before`` 到达。
        """
        if max_steps is not None:
            integer(max_steps, 'max_steps', 1)
        steps = 0
        while True:
            row = self.store.get(collection_id)
            if row['state'] in {'completed', 'failed', 'cancelled', 'paused'}:
                return self.store.get(collection_id)
            if row['not_before'] and micros(row['not_before']) > micros(self.db.clock()):
                wait_seconds = (micros(row['not_before']) - micros(self.db.clock())) / 1_000_000
                if self.sleeper is not None:
                    return self.store.get(collection_id)
                time.sleep(max(0.0, wait_seconds))
                continue
            steps += 1
            if max_steps and steps > max_steps:
                return self.store.get(collection_id)
            self.step(collection_id)

    # ------------------------------------------------------------------ 单步
    def step(self, collection_id):
        row = self.store.get(collection_id)
        if row['state'] in {'completed', 'failed'}:
            return row
        if row['state'] == 'cancelled':
            fail('collection_state', 'Collection was cancelled; late responses cannot complete the window',
                 state='cancelled', cancel_reason=row['cancel_reason'])
        if row['state'] == 'paused':
            fail('collection_paused', 'Collection is paused; resume before stepping')
        if not self.store.check_budget(collection_id):
            return self.store.get(collection_id)
        dispatch = {
            'requesting': self._step_request,
            'queued': self._step_queue,
            'downloading': self._step_download,
            'verifying': self._step_verify,
            'importing': self._step_import,
        }
        return dispatch[row['phase']](collection_id)

    # ------------------------------------------------------------- 申请/受理
    def _step_request(self, collection_id):
        row = self.store.get(collection_id)
        attempt_id = self.store.start_attempt(collection_id, 'requesting')
        try:
            if not row['export_id']:
                self.store.consume_attempt(collection_id)
                result = self.client.open_export(
                    row['source'], row['start'], row['end'], row['config_revision'], row['idempotency_key'],
                )
                payload = result['payload']
                export_id = _require_text(payload, 'export_id')
                status = str(payload.get('status', 'accepted'))
                self.store.attach_export(collection_id, export_id, status)
                if result['status'] == 202 or status in QUEUED_STATUSES:
                    self.store.set_phase(collection_id, 'queued', {'export_status': status},
                                         export_status=status)
                    self.store.end_attempt(attempt_id, 'ok', {'export_id': export_id, 'status': status})
                elif 'manifest' in payload and status in READY_STATUSES:
                    self._adopt_or_fail(collection_id, attempt_id, payload['manifest'])
                else:
                    raise SupplierRejected(f'Unexpected export status {status}', status=result['status'])
            else:
                # 供应商已受理但本地响应丢失：直接找回原任务。
                self._poll_export(collection_id, attempt_id)
        except SupplierUnavailable as exc:
            self._retry(collection_id, attempt_id, exc.as_dict(), exc.retry_after)
        except SupplierRejected as exc:
            self._permanent(collection_id, attempt_id, exc.as_dict())
        return self.store.get(collection_id)

    def _step_queue(self, collection_id):
        attempt_id = self.store.start_attempt(collection_id, 'queued')
        try:
            self._poll_export(collection_id, attempt_id)
        except SupplierUnavailable as exc:
            self._retry(collection_id, attempt_id, exc.as_dict(), exc.retry_after)
        except SupplierRejected as exc:
            self._permanent(collection_id, attempt_id, exc.as_dict())
        return self.store.get(collection_id)

    def _adopt_or_fail(self, collection_id, attempt_id, manifest_payload):
        """清单接纳失败（改版/同版字节变化）必须终止任务并停在原水位。"""
        try:
            manifest = self.store.adopt_manifest(collection_id, manifest_payload)
        except PipelineError as exc:
            self._permanent(collection_id, attempt_id, exc.as_dict())
            return None
        self.store.end_attempt(attempt_id, 'ok', {'manifest_revision': manifest['manifest']['revision']})
        return manifest

    def _poll_export(self, collection_id, attempt_id):
        row = self.store.get(collection_id)
        self.store.consume_attempt(collection_id)
        result = self.client.get_export(row['export_id'])
        payload = result['payload']
        status = str(payload.get('status', ''))
        if result['status'] == 202 or status in QUEUED_STATUSES:
            retry_after = result['retry_after']
            self.store.attach_export(collection_id, row['export_id'], status or 'queued')
            self.store.remember_wait(collection_id, retry_after or self.backoff,
                                     {'code': 'export_pending', 'export_status': status,
                                      'retry_after': retry_after})
            self.store.set_phase(collection_id, 'queued', {'export_status': status or 'queued'},
                                 preserve_wait=True)
            self.store.end_attempt(attempt_id, 'waiting', {'retry_after': retry_after})
            if self.sleeper and retry_after:
                self.sleeper(retry_after)
            return
        if 'manifest' not in payload or (status and status not in READY_STATUSES):
            raise SupplierRejected(f'Unexpected export status {status or result["status"]}',
                                   status=result['status'])
        self._adopt_or_fail(collection_id, attempt_id, payload['manifest'])

    # ------------------------------------------------------------- 下载/核对
    def _step_download(self, collection_id):
        attempt_id = self.store.start_attempt(collection_id, 'downloading')
        shards = self.store.list_shards(collection_id)
        pending = [shard for shard in shards if shard['state'] != 'verified']
        if not pending:
            self.store.mark_importing(collection_id)
            self.store.end_attempt(attempt_id, 'ok', {'all_shards_verified': True})
            return self.store.get(collection_id)
        shard = pending[0]
        # 重启后可能已下载但尚未核对，直接进入核对阶段，不重复拉取。
        if shard['state'] == 'verifying':
            self.store.set_phase(collection_id, 'verifying', {'shard_id': shard['shard_id']})
            self.store.end_attempt(attempt_id, 'ok', {'resume_verify': shard['shard_id']})
            return self.store.get(collection_id)
        try:
            self.store.consume_attempt(collection_id)
            self.store.mark_shard(collection_id, shard['shard_id'], 'downloading')
            content = self.client.download(shard['url'])
            # 先落地未验字节，使核对可以在重启后继续，而不必重新下载。
            self.store.store_blob(collection_id, shard['shard_id'], content)
            self.store.mark_shard(collection_id, shard['shard_id'], 'verifying', increment_attempt=True)
            self.store.set_phase(collection_id, 'verifying', {'shard_id': shard['shard_id']})
            self.store.end_attempt(attempt_id, 'ok', {'downloaded': shard['shard_id'], 'bytes': len(content)})
        except SupplierRejected as exc:
            detail = exc.as_dict()
            refreshed = self._try_refresh(collection_id, shard, detail, attempt_id)
            if not refreshed:
                self.store.mark_shard(collection_id, shard['shard_id'], 'pending', error=detail, increment_attempt=True)
                self._permanent(collection_id, attempt_id, detail)
            else:
                self.store.mark_shard(collection_id, shard['shard_id'], 'pending', increment_attempt=True)
        except SupplierUnavailable as exc:
            detail = exc.as_dict()
            self.store.mark_shard(collection_id, shard['shard_id'], 'pending', error=detail, increment_attempt=True)
            self._retry(collection_id, attempt_id, detail, exc.retry_after)
        except PipelineError:
            # 换签清单触发的阻断已在 _try_refresh 内终止任务。
            pass
        return self.store.get(collection_id)

    def _step_verify(self, collection_id):
        attempt_id = self.store.start_attempt(collection_id, 'verifying')
        shards = self.store.list_shards(collection_id)
        target = next((shard for shard in shards if shard['state'] == 'verifying'), None)
        if target is None:
            # 没有待核对分片，回到下载阶段继续后续分片。
            self.store.set_phase(collection_id, 'downloading', {'verified': sum(1 for s in shards if s['state'] == 'verified'),
                                                                 'total': len(shards)})
            self.store.end_attempt(attempt_id, 'ok', {'nothing_to_verify': True})
            return self.store.get(collection_id)
        try:
            content = self.store.get_blob(collection_id, target['shard_id'])
        except PipelineError as exc:
            # 落地字节丢失（例如外部清理），回退为重新下载。
            self.store.mark_shard(collection_id, target['shard_id'], 'pending', error=exc.as_dict())
            self.store.set_phase(collection_id, 'downloading', {'redownload': target['shard_id']})
            self.store.end_attempt(attempt_id, 'retry', exc.as_dict())
            return self.store.get(collection_id)
        error = verify_shard(target, content)
        if error is not None:
            # 摘要/长度/记录数不符是阻断性的，重试也不能让同版字节变化。
            self.store.mark_shard(collection_id, target['shard_id'], 'failed', error=error)
            self._permanent(collection_id, attempt_id, error)
            return self.store.get(collection_id)
        self.store.mark_shard(collection_id, target['shard_id'], 'verified', increment_attempt=True)
        journal.record(self.db, 'collection.shard_verified', {
            'collection_id': collection_id, 'shard_id': target['shard_id'],
            'bytes': target['bytes'], 'sha256': target['sha256'],
        })
        remaining = [s for s in self.store.list_shards(collection_id) if s['state'] != 'verified']
        self.store.set_phase(collection_id, 'downloading',
                             {'verified': len(shards) - len(remaining), 'total': len(shards),
                              'next': remaining[0]['shard_id'] if remaining else None})
        self.store.end_attempt(attempt_id, 'ok', {'shard_id': target['shard_id']})
        return self.store.get(collection_id)

    def _try_refresh(self, collection_id, shard, detail, attempt_id):
        """链接失效（401/403/404/410）时尝试换签；换签不改变同版字节。"""
        if detail.get('status') not in (401, 403, 404, 410):
            return False
        row = self.store.get(collection_id)
        try:
            self.store.consume_attempt(collection_id)
            result = self.client.refresh(row['export_id'])
            self.store.adopt_manifest(collection_id, result['payload'])
        except PipelineError as exc:
            # 换签返回了改版或同版字节变化的清单：阻断，水位不得推进。
            self.store.end_attempt(attempt_id, 'failed', exc.as_dict())
            self.store.terminate(collection_id, 'failed', exc.as_dict())
            raise
        except SupplierError as exc:
            self.store.end_attempt(attempt_id, 'retry', {'refresh_failed': exc.as_dict()})
            return False
        new_url = self.store.get_shard(collection_id, shard['shard_id'])['url']
        journal.record(self.db, 'collection.link_refreshed', {
            'collection_id': collection_id, 'shard_id': shard['shard_id'], 'url': new_url,
        })
        self.store.remember_wait(collection_id, 0, {'code': 'link_refreshed', 'shard_id': shard['shard_id']})
        self.store.end_attempt(attempt_id, 'ok', {'refreshed': True})
        return True

    # ------------------------------------------------------------- 导入/水位
    def _step_import(self, collection_id):
        attempt_id = self.store.start_attempt(collection_id, 'importing')
        row = self.store.get(collection_id)
        try:
            self.store.consume_attempt(collection_id)
            shards = self.store.list_shards(collection_id)
            missing = [shard['shard_id'] for shard in shards if shard['state'] != 'verified']
            if missing:
                self._permanent(collection_id, attempt_id,
                                {'code': 'shard_missing', 'message': 'Unverified shards block import', 'shards': missing})
                return self.store.get(collection_id)
            imported = []
            for shard in shards:
                content = self.store.get_blob(collection_id, shard['shard_id'])
                batch_id = f'{collection_id}:{shard["shard_id"]}'
                staged = batches.stage(self.db, batch_id, row['source'], content)
                result = processor.drain(self.db, batch_id)
                self.store.mark_shard(collection_id, shard['shard_id'], 'verified',
                                      batch_id=staged['batch_id'])
                imported.append({
                    'shard_id': shard['shard_id'],
                    'batch_id': staged['batch_id'],
                    'duplicate': bool(staged.get('duplicate')),
                    'total': result['batch']['total'],
                    'accepted': result['batch']['accepted'],
                    'rejected': result['batch']['rejected'],
                    'state': result['batch']['state'],
                })
            summary = build_summary(row, shards, imported)
            watermark = watermarks.advance(self.db, row['source'], row['end'])
            self.store.complete(collection_id, summary, watermark['watermark'],
                                [item['batch_id'] for item in imported])
            self.store.end_attempt(attempt_id, 'ok', summary)
        except PipelineError as exc:
            self._permanent(collection_id, attempt_id, exc.as_dict())
        except Exception as exc:  # 传输外的意外故障留给重试预算兜底
            self._retry(collection_id, attempt_id,
                        {'code': 'import_failed', 'message': str(exc)}, None)
        return self.store.get(collection_id)

    # ------------------------------------------------------------------ 失败/退避
    def _retry(self, collection_id, attempt_id, detail, retry_after):
        wait = retry_after if retry_after is not None else self.backoff
        # 没有注入 sleeper 的驱动（典型的单进程轮询）不挂未来等待，
        # 由调用方按自身节奏再次 step；注入了 sleeper 才记录退避窗口并睡等。
        if self.sleeper is None:
            self.store.remember_wait(collection_id, 0, detail)
        else:
            self.store.remember_wait(collection_id, wait, detail)
            self.sleeper(wait)
        self.store.end_attempt(attempt_id, 'retry', detail)
        self.store.check_budget(collection_id)

    def _permanent(self, collection_id, attempt_id, detail):
        self.store.end_attempt(attempt_id, 'failed', detail)
        self.store.terminate(collection_id, 'failed', detail)

    # ------------------------------------------------------------------ 只读视图
    def status(self, collection_id):
        row = self.store.get(collection_id)
        shards = self.store.list_shards(collection_id)
        now = micros(self.db.clock())
        waiting = bool(row['not_before'] and micros(row['not_before']) > now)
        phase_help = {
            'requesting': 'waiting_for_acceptance',
            'queued': 'queued_at_supplier',
            'downloading': 'downloading_shards',
            'verifying': 'verifying_shards',
            'importing': 'importing_batches',
            'paused': 'paused',
            'completed': 'completed',
            'failed': 'blocked',
            'cancelled': 'cancelled',
        }
        return {
            'collection_id': row['collection_id'],
            'state': row['state'],
            'phase': row['phase'],
            'stage': phase_help.get(row['phase'], row['phase']),
            'waiting': waiting,
            'not_before': row['not_before'],
            'deadline': row['deadline'],
            'attempts_used': row['attempts_used'],
            'attempts_remaining': max(0, row['max_attempts'] - row['attempts_used']),
            'max_attempts': row['max_attempts'],
            'export_id': row['export_id'],
            'export_status': row['export_status'],
            'manifest_revision': row['manifest_revision'],
            'manifest_digest': row['manifest_digest'],
            'shards': {
                'total': len(shards),
                'verified': sum(1 for s in shards if s['state'] == 'verified'),
                'pending': sum(1 for s in shards if s['state'] == 'pending'),
                'failed': sum(1 for s in shards if s['state'] == 'failed'),
                'detail': [{'shard_id': s['shard_id'], 'state': s['state'], 'attempts': s['attempts'],
                            'error': s['error'], 'batch_id': s['batch_id']} for s in shards],
            },
            'last_error': row['last_error'],
            'summary': row['summary'],
            'watermark': row['watermark'],
            'revision': row['revision'],
        }


def verify_shard(shard, content):
    """逐片核对长度与 SHA256；记录数在字节核对通过后解析计数。"""
    if len(content) != shard['bytes']:
        return {'code': 'shard_length_mismatch', 'message': 'Shard byte length differs from manifest',
                'shard_id': shard['shard_id'], 'expected': shard['bytes'], 'actual': len(content)}
    actual_hash = hashlib.sha256(content).hexdigest()
    if actual_hash != shard['sha256']:
        return {'code': 'shard_digest_mismatch', 'message': 'Shard sha256 differs from manifest',
                'shard_id': shard['shard_id'], 'expected': shard['sha256'], 'actual': actual_hash}
    count = 0
    for line in content.splitlines():
        if not line.strip():
            continue
        try:
            decode_json(line)
        except Exception as exc:
            return {'code': 'shard_record_malformed', 'message': 'Shard contains an undecodable record',
                    'shard_id': shard['shard_id'], 'detail': str(exc)}
        count += 1
    if count != shard['records']:
        return {'code': 'shard_record_count_mismatch', 'message': 'Shard record count differs from manifest',
                'shard_id': shard['shard_id'], 'expected': shard['records'], 'actual': count}
    return None


def build_summary(row, shards, imported):
    totals = {key: sum(item[key] for item in imported) for key in ('total', 'accepted', 'rejected')}
    return {
        'source': row['source'],
        'window': {'start': row['start'], 'end': row['end']},
        'config_revision': row['config_revision'],
        'manifest_revision': row['manifest_revision'],
        'manifest_digest': row['manifest_digest'],
        'shards_total': len(shards),
        'shards_verified': len(shards),
        'records_total': totals['total'],
        'records_accepted': totals['accepted'],
        'records_quarantined': totals['rejected'],
        'partial_batches': [item['batch_id'] for item in imported if item['state'] == 'partial'],
        'batches': imported,
    }


def _require_text(payload, field):
    if not isinstance(payload, dict) or not isinstance(payload.get(field), str) or not payload[field].strip():
        raise SupplierRejected(f'Supplier response missing {field}', status=202)
    return payload[field]
