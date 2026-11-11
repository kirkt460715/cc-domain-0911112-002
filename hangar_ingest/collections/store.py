"""采集任务的持久化状态机。

任务身份由 ``(source, UTC 窗口, 配置版本)`` 决定，同一身份的重复申请复用同一
采集任务和同一个供应商幂等键，供应商已受理但响应丢失时凭 export_id 找回原任务。
所有状态变更都在事务内完成并写入审计链，进程重启后可从磁盘状态继续。
"""
import json

from hangar_ingest.types import canonical, digest, fail, identity, instant, integer, micros, object_fields, text
from hangar_ingest.runs import journal

SCHEMA = '''
CREATE TABLE IF NOT EXISTS collections(
 collection_id TEXT PRIMARY KEY,source TEXT NOT NULL,start TEXT NOT NULL,end TEXT NOT NULL,
 config_revision INTEGER NOT NULL,idempotency_key TEXT NOT NULL,
 state TEXT NOT NULL,revision INTEGER NOT NULL,phase TEXT NOT NULL,phase_detail TEXT,
 export_id TEXT,export_status TEXT,
 manifest_revision INTEGER,manifest_digest TEXT,manifest_payload TEXT,
 max_attempts INTEGER NOT NULL,attempts_used INTEGER NOT NULL,deadline TEXT NOT NULL,
 not_before TEXT,last_error TEXT,last_retry_after INTEGER,
 batch_id TEXT,watermark TEXT,summary TEXT,cancel_reason TEXT,
 created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
 UNIQUE(source,start,end,config_revision));
CREATE TABLE IF NOT EXISTS collection_shards(
 collection_id TEXT NOT NULL REFERENCES collections(collection_id),
 shard_id TEXT NOT NULL,seq INTEGER NOT NULL,url TEXT NOT NULL,
 bytes INTEGER NOT NULL,sha256 TEXT NOT NULL,records INTEGER NOT NULL,
 state TEXT NOT NULL,attempts INTEGER NOT NULL,error TEXT,batch_id TEXT,
 PRIMARY KEY(collection_id,shard_id));
CREATE TABLE IF NOT EXISTS collection_shard_blobs(
 collection_id TEXT NOT NULL,shard_id TEXT NOT NULL,content BLOB NOT NULL,
 PRIMARY KEY(collection_id,shard_id));
CREATE TABLE IF NOT EXISTS collection_attempts(
 attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,collection_id TEXT NOT NULL REFERENCES collections(collection_id),
 phase TEXT NOT NULL,outcome TEXT NOT NULL,detail TEXT NOT NULL,started_at TEXT NOT NULL,ended_at TEXT);
CREATE INDEX IF NOT EXISTS collection_attempts_idx ON collection_attempts(collection_id,attempt_id);
'''

ACTIVE_STATES = {'requesting', 'queued', 'downloading', 'verifying', 'importing'}
TERMINAL_STATES = {'completed', 'failed', 'cancelled'}
ALL_STATES = ACTIVE_STATES | TERMINAL_STATES | {'paused'}
SHARD_STATES = {'pending', 'downloading', 'verifying', 'verified', 'failed'}


class CollectionStore:
    def __init__(self, db):
        self.db = db
        if db.depth:
            fail('transaction_scope', 'Initialize collection store outside transaction')
        db.db.executescript(SCHEMA)

    # ------------------------------------------------------------------ 任务身份
    def create(self, collection_id, source, start, end, config_revision, *, max_attempts, deadline,
               idempotency_key=None, export_id=None):
        identity(collection_id, 'collection_id')
        identity(source, 'source')
        start = instant(start, 'start')
        end = instant(end, 'end')
        if start >= end:
            fail('collection_interval', 'Collection window must be nonempty')
        integer(config_revision, 'config_revision', 1)
        integer(max_attempts, 'max_attempts', 1, 1000)
        deadline = instant(deadline, 'deadline')
        if idempotency_key is None:
            idempotency_key = 'collection:' + digest({'source': source, 'start': start, 'end': end, 'config_revision': config_revision})[:32]
        identity(idempotency_key, 'idempotency_key')
        with self.db.transaction():
            existing = self.db.one(
                'SELECT * FROM collections WHERE source=? AND start=? AND end=? AND config_revision=?',
                (source, start, end, config_revision),
            )
            if existing:
                if existing['state'] == 'cancelled':
                    fail('collection_cancelled', 'Cancelled collection cannot be restarted', collection_id=existing['collection_id'])
                return {**existing, 'duplicate': True}
            now = self.db.clock()
            self.db.execute(
                'INSERT INTO collections(collection_id,source,start,end,config_revision,idempotency_key,'
                "state,revision,phase,export_id,export_status,manifest_revision,manifest_digest,manifest_payload,"
                'max_attempts,attempts_used,deadline,not_before,last_error,last_retry_after,batch_id,watermark,'
                'summary,cancel_reason,created_at,updated_at) '
                "VALUES(?,?,?,?,?,?,'requesting',1,'requesting',?,?,NULL,NULL,NULL,?,0,?,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?,?)",
                (collection_id, source, start, end, config_revision, idempotency_key,
                 export_id, None, max_attempts, deadline, now, now),
            )
            journal.record(self.db, 'collection.created', {
                'collection_id': collection_id, 'source': source, 'start': start, 'end': end,
                'config_revision': config_revision, 'max_attempts': max_attempts, 'deadline': deadline,
            })
            return self.get(collection_id)

    def get(self, collection_id):
        identity(collection_id, 'collection_id')
        row = self.db.one('SELECT * FROM collections WHERE collection_id=?', (collection_id,))
        if row is None:
            fail('unknown_collection', 'Collection does not exist', collection_id=collection_id)
        for field in ('phase_detail', 'last_error', 'manifest_payload', 'summary'):
            row[field] = json.loads(row[field]) if row[field] else None
        return row

    def list(self, state=None):
        if state is not None and state not in ALL_STATES:
            fail('collection_state', 'Unknown collection state', state=state)
        if state:
            rows = self.db.all('SELECT collection_id FROM collections WHERE state=? ORDER BY created_at,collection_id', (state,))
        else:
            rows = self.db.all('SELECT collection_id FROM collections ORDER BY created_at,collection_id')
        return [self.get(row['collection_id']) for row in rows]

    def due(self, *, now=None):
        """可推进的任务：未暂停/未终止，且等待时间已到。"""
        now = now or self.db.clock()
        rows = self.db.all(
            "SELECT collection_id FROM collections WHERE state IN ('requesting','queued','downloading','verifying','importing') "
            'AND (not_before IS NULL OR not_before<=?) ORDER BY created_at,collection_id',
            (now,),
        )
        return [self.get(row['collection_id']) for row in rows]

    # ------------------------------------------------------------------ 状态切换
    def _update(self, collection_id, **fields):
        assignments = ', '.join(f'{key}=?' for key in fields)
        values = [*fields.values(), self.db.clock(), collection_id]
        self.db.execute(
            f'UPDATE collections SET {assignments},revision=revision+1,updated_at=? WHERE collection_id=?', values,
        )

    def set_phase(self, collection_id, phase, detail=None, *, not_before=None, last_error=None,
                  last_retry_after=None, export_status=None, preserve_wait=False):
        if phase not in ACTIVE_STATES:
            fail('collection_state', 'Phase must be an active state', phase=phase)
        with self.db.transaction():
            row = self._require_active(collection_id)
            updates = {'state': phase, 'phase': phase, 'phase_detail': canonical(detail) if detail is not None else None,
                       'not_before': row['not_before'] if preserve_wait else not_before,
                       'last_retry_after': last_retry_after,
                       'last_error': canonical(last_error) if last_error is not None else None}
            if export_status is not None:
                updates['export_status'] = export_status
            self._update(collection_id, **updates)
            journal.record(self.db, 'collection.phase', {'collection_id': collection_id, 'phase': phase, 'detail': detail})
            return self.get(collection_id)

    def attach_export(self, collection_id, export_id, export_status):
        identity(export_id, 'export_id')
        text(export_status, 'export_status', 64)
        with self.db.transaction():
            row = self._require_active(collection_id)
            if row['export_id'] and row['export_id'] != export_id:
                fail('export_identity', 'Supplier returned a different export id',
                     expected=row['export_id'], actual=export_id)
            self._update(collection_id, export_id=export_id, export_status=export_status)
            journal.record(self.db, 'collection.export_attached', {'collection_id': collection_id, 'export_id': export_id})
            return self.get(collection_id)

    def remember_wait(self, collection_id, retry_after, detail):
        with self.db.transaction():
            self._require_active(collection_id)
            not_before = None
            if retry_after:
                integer(retry_after, 'retry_after', 0, 86400)
                not_before = _instant_plus(self.db.clock(), retry_after)
            self._update(collection_id, not_before=not_before, last_retry_after=retry_after,
                         last_error=canonical(detail), phase_detail=canonical(detail))
            return self.get(collection_id)

    def budget_left(self, collection_id):
        row = self.get(collection_id)
        return row['max_attempts'] - row['attempts_used']

    def check_budget(self, collection_id):
        row = self.get(collection_id)
        if row['attempts_used'] >= row['max_attempts']:
            self.terminate(collection_id, 'failed', {
                'code': 'attempt_limit', 'message': 'Collection exhausted configured attempts',
                'attempts_used': row['attempts_used'], 'max_attempts': row['max_attempts'],
            })
            return False
        if micros(self.db.clock()) >= micros(row['deadline']):
            self.terminate(collection_id, 'failed', {
                'code': 'deadline_exceeded', 'message': 'Collection passed its deadline',
                'deadline': row['deadline'],
            })
            return False
        return True

    def consume_attempt(self, collection_id):
        with self.db.transaction():
            self._require_active(collection_id)
            self.db.execute('UPDATE collections SET attempts_used=attempts_used+1,updated_at=? WHERE collection_id=?',
                            (self.db.clock(), collection_id))
            return self.get(collection_id)

    def start_attempt(self, collection_id, phase):
        now = self.db.clock()
        cursor = self.db.execute(
            "INSERT INTO collection_attempts(collection_id,phase,outcome,detail,started_at) VALUES(?,?, 'running', ?,?)",
            (collection_id, phase, phase, now),
        )
        return cursor.lastrowid

    def end_attempt(self, attempt_id, outcome, detail):
        if outcome not in {'ok', 'retry', 'failed', 'blocked', 'waiting'}:
            fail('attempt_outcome', 'Unknown attempt outcome', outcome=outcome)
        self.db.execute(
            'UPDATE collection_attempts SET outcome=?,detail=?,ended_at=? WHERE attempt_id=?',
            (outcome, canonical(detail), self.db.clock(), attempt_id),
        )

    def attempts(self, collection_id):
        self.get(collection_id)
        rows = self.db.all('SELECT * FROM collection_attempts WHERE collection_id=? ORDER BY attempt_id', (collection_id,))
        for row in rows:
            row['detail'] = json.loads(row['detail'])
        return rows

    def terminate(self, collection_id, state, error_or_reason, *, summary=None, watermark=None, batch_id=None):
        if state not in TERMINAL_STATES:
            fail('collection_state', 'Terminal state required', state=state)
        with self.db.transaction():
            row = self.get(collection_id)
            if row['state'] in TERMINAL_STATES:
                fail('collection_state', 'Collection already terminated', state=row['state'])
            updates = {'state': state, 'phase': state, 'not_before': None,
                       'summary': canonical(summary) if summary is not None else row['summary']}
            if state == 'cancelled':
                updates['cancel_reason'] = text(error_or_reason, 'reason', 2000)
                updates['last_error'] = None
            else:
                updates['last_error'] = canonical(error_or_reason)
            if watermark is not None:
                updates['watermark'] = watermark
            if batch_id is not None:
                updates['batch_id'] = batch_id
            self._update(collection_id, **updates)
            journal.record(self.db, f'collection.{state}', {'collection_id': collection_id, 'detail': error_or_reason})
            return self.get(collection_id)

    def _require_active(self, collection_id):
        row = self.get(collection_id)
        if row['state'] not in ACTIVE_STATES:
            fail('collection_state', 'Collection is not active', state=row['state'])
        return row

    # ------------------------------------------------------------------ 暂停/取消
    def pause(self, collection_id, revision):
        integer(revision, 'revision', 1)
        with self.db.transaction():
            row = self.get(collection_id)
            if row['revision'] != revision:
                fail('stale_revision', 'Collection revision changed')
            if row['state'] not in ACTIVE_STATES:
                fail('collection_state', 'Only active collection can be paused', state=row['state'])
            # phase 保留可恢复的活动阶段，仅 state 标记暂停。
            self._update(collection_id, state='paused', not_before=None)
            journal.record(self.db, 'collection.paused', {'collection_id': collection_id, 'phase': row['phase']})
            return self.get(collection_id)

    def resume(self, collection_id, revision):
        integer(revision, 'revision', 1)
        with self.db.transaction():
            row = self.get(collection_id)
            if row['revision'] != revision:
                fail('stale_revision', 'Collection revision changed')
            if row['state'] != 'paused':
                fail('collection_state', 'Only paused collection can be resumed', state=row['state'])
            phase = row['phase'] if row['phase'] in ACTIVE_STATES else 'requesting'
            self._update(collection_id, state=phase, phase=phase)
            journal.record(self.db, 'collection.resumed', {'collection_id': collection_id, 'phase': phase})
            return self.get(collection_id)

    def cancel(self, collection_id, revision, reason):
        integer(revision, 'revision', 1)
        reason = text(reason, 'reason', 2000)
        with self.db.transaction():
            row = self.get(collection_id)
            if row['revision'] != revision:
                fail('stale_revision', 'Collection revision changed')
            if row['state'] in TERMINAL_STATES:
                fail('collection_state', 'Collection already terminated', state=row['state'])
            self._update(collection_id, state='cancelled', phase='cancelled', not_before=None, cancel_reason=reason)
            journal.record(self.db, 'collection.cancelled', {'collection_id': collection_id, 'reason': reason})
            return self.get(collection_id)

    # ------------------------------------------------------------------ 清单核对
    def adopt_manifest(self, collection_id, payload):
        """核对并接纳供应商清单。

        同 revision 的分片字节、摘要、记录数必须完全一致，仅链接允许换签；
        revision 变化（无论新旧）一律阻断，水位不得推进。
        """
        manifest = normalize_manifest(payload)
        fingerprint = manifest_fingerprint(manifest)
        with self.db.transaction():
            row = self._require_active(collection_id)
            if manifest.get('export_id') and row['export_id'] and manifest['export_id'] != row['export_id']:
                fail('export_identity', 'Manifest belongs to a different export id',
                     expected=row['export_id'], actual=manifest['export_id'])
            if row['manifest_revision'] is not None:
                if manifest['revision'] != row['manifest_revision']:
                    fail('manifest_revision_changed', 'Supplier manifest revision changed; window cannot advance',
                         expected=row['manifest_revision'], actual=manifest['revision'])
                if fingerprint != row['manifest_digest']:
                    fail('manifest_conflict', 'Same-revision manifest declares different shard bytes or digests')
                # 同版同字节：链接换签可接受，更新 URL 后直接返回。
                for shard in manifest['shards']:
                    self.db.execute('UPDATE collection_shards SET url=? WHERE collection_id=? AND shard_id=?',
                                    (shard['url'], collection_id, shard['id']))
                self._update(collection_id, manifest_payload=canonical(manifest),
                             phase_detail=canonical({'manifest_revision': manifest['revision'], 'resigned': True}))
                return {'collection': self.get(collection_id), 'manifest': manifest, 'resigned': True}
            self._update(collection_id, state='downloading', phase='downloading',
                         export_status='ready', manifest_revision=manifest['revision'],
                         manifest_digest=fingerprint, manifest_payload=canonical(manifest),
                         phase_detail=canonical({'manifest_revision': manifest['revision'], 'shards': len(manifest['shards'])}))
            for seq, shard in enumerate(manifest['shards']):
                self.db.execute(
                    "INSERT INTO collection_shards(collection_id,shard_id,seq,url,bytes,sha256,records,state,attempts) "
                    "VALUES(?,?,?,?,?,?,?, 'pending',0)",
                    (collection_id, shard['id'], seq, shard['url'], shard['bytes'], shard['sha256'], shard['records']),
                )
            journal.record(self.db, 'collection.manifest_adopted', {
                'collection_id': collection_id, 'revision': manifest['revision'],
                'digest': fingerprint, 'shards': len(manifest['shards']),
            })
            return {'collection': self.get(collection_id), 'manifest': manifest, 'resigned': False}

    def list_shards(self, collection_id):
        self.get(collection_id)
        rows = self.db.all('SELECT * FROM collection_shards WHERE collection_id=? ORDER BY seq', (collection_id,))
        for row in rows:
            row['error'] = json.loads(row['error']) if row['error'] else None
        return rows

    def get_shard(self, collection_id, shard_id):
        row = self.db.one('SELECT * FROM collection_shards WHERE collection_id=? AND shard_id=?',
                          (collection_id, shard_id))
        if row is None:
            fail('unknown_shard', 'Collection shard does not exist', shard_id=shard_id)
        row['error'] = json.loads(row['error']) if row['error'] else None
        return row

    def mark_shard(self, collection_id, shard_id, state, *, error=None, increment_attempt=False, batch_id=None):
        if state not in SHARD_STATES:
            fail('shard_state', 'Unknown shard state', state=state)
        with self.db.transaction():
            self._require_active(collection_id)
            self.get_shard(collection_id, shard_id)
            self.db.execute(
                'UPDATE collection_shards SET state=?,error=?,attempts=attempts+?,batch_id=COALESCE(?,batch_id) '
                'WHERE collection_id=? AND shard_id=?',
                (state, canonical(error) if error else None, 1 if increment_attempt else 0, batch_id, collection_id, shard_id),
            )
            return self.get_shard(collection_id, shard_id)

    def store_blob(self, collection_id, shard_id, content):
        if not isinstance(content, bytes):
            fail('shard_content', 'Verified shard content must be bytes')
        with self.db.transaction():
            self._require_active(collection_id)
            self.db.execute(
                'INSERT INTO collection_shard_blobs(collection_id,shard_id,content) VALUES(?,?,?) '
                'ON CONFLICT(collection_id,shard_id) DO UPDATE SET content=excluded.content',
                (collection_id, shard_id, content),
            )

    def get_blob(self, collection_id, shard_id):
        row = self.db.one('SELECT content FROM collection_shard_blobs WHERE collection_id=? AND shard_id=?',
                          (collection_id, shard_id))
        if row is None:
            fail('shard_blob_missing', 'Verified shard content is not stored (restart required re-download)')
        return row['content']

    def mark_importing(self, collection_id):
        return self.set_phase(collection_id, 'importing', {'step': 'batch_import'})

    def complete(self, collection_id, summary, watermark, batch_ids):
        with self.db.transaction():
            self._require_active(collection_id)
            self._update(collection_id, state='completed', phase='completed', not_before=None,
                         last_error=None, summary=canonical(summary), watermark=watermark,
                         batch_id=canonical(batch_ids))
            journal.record(self.db, 'collection.completed', {
                'collection_id': collection_id, 'watermark': watermark, 'batches': batch_ids,
            })
            return self.get(collection_id)


def normalize_manifest(payload):
    object_fields(payload, {'revision', 'shards'}, {'export_id'})
    revision = integer(payload['revision'], 'revision', 1)
    if not isinstance(payload['shards'], list) or len(payload['shards']) > 10000:
        fail('manifest_shards', 'Manifest shard list exceeds supported size')
    seen = set()
    shards = []
    for entry in payload['shards']:
        object_fields(entry, {'id', 'url', 'bytes', 'sha256', 'records'})
        shard_id = identity(entry['id'], 'shard_id')
        if shard_id in seen:
            fail('manifest_shard_duplicate', 'Manifest shard identifiers must be unique', shard_id=shard_id)
        seen.add(shard_id)
        length = integer(entry['bytes'], 'bytes', 0)
        count = integer(entry['records'], 'records', 0)
        checksum = entry['sha256']
        if not isinstance(checksum, str) or len(checksum) != 64 or any(c not in '0123456789abcdef' for c in checksum):
            fail('manifest_hash', 'Shard sha256 must be lowercase hex')
        shards.append({'id': shard_id, 'url': text(entry['url'], 'url', 2048),
                       'bytes': length, 'sha256': checksum, 'records': count})
    shards.sort(key=lambda item: item['id'])
    result = {'revision': revision, 'shards': shards}
    if 'export_id' in payload:
        result['export_id'] = identity(payload['export_id'], 'export_id')
    return result


def manifest_fingerprint(manifest):
    """同版清单的字节级身份：只覆盖 revision 与每片长度/摘要/记录数，不含 URL。"""
    return digest({
        'revision': manifest['revision'],
        'shards': [{'id': s['id'], 'bytes': s['bytes'], 'sha256': s['sha256'], 'records': s['records']}
                   for s in manifest['shards']],
    })


def _instant_plus(clock_value, seconds):
    point = micros(clock_value) + seconds * 1_000_000
    from hangar_ingest.types import from_micros
    return from_micros(point)
