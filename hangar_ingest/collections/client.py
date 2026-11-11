"""供应商异步导出协议客户端。

协议约定（与 ``synthetic_exchange`` 对齐）：

* ``POST /exports`` 携带 ``Idempotency-Key``，供应商受理后返回 202 与 export_id；
  重复申请必须使用同一幂等键，供应商会返回同一个 export_id。
* ``GET /exports/{id}`` 在排队或执行中返回 202（可带 ``Retry-After``），
  完成后返回 200 与清单（revision + 每片 id/url/bytes/sha256/records）。
* ``POST /exports/{id}/refresh`` 在下载链接过期后换签，返回同版清单，
  换签只允许改变链接，不允许改变分片字节与摘要。
* 分片用 GET 拉取，按字节长度、SHA256、记录数核对。

客户端只负责传输与协议形态，重试预算、状态机和核对逻辑在 runner 中。
传输类故障（连接、超时、5xx）抛 :class:`SupplierUnavailable`；
供应商明确拒绝（4xx）抛 :class:`SupplierRejected`，由上层决定是否可恢复。
"""
import json
import urllib.error
import urllib.request

from hangar_ingest.types import fail, identity, integer, instant, text, utcnow

MAX_DOWNLOAD_BYTES = 64 * 1024 * 1024


class SupplierError(Exception):
    def __init__(self, code, message, *, retryable, status=None, retry_after=None):
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status = status
        self.retry_after = retry_after

    def as_dict(self):
        return {
            'code': self.code,
            'message': str(self),
            'retryable': self.retryable,
            'status': self.status,
            'retry_after': self.retry_after,
        }


class SupplierUnavailable(SupplierError):
    def __init__(self, message, *, retry_after=None):
        super().__init__('supplier_unavailable', message, retryable=True, retry_after=retry_after)


class SupplierRejected(SupplierError):
    def __init__(self, message, *, status, retry_after=None):
        super().__init__('supplier_rejected', message, retryable=False, status=status, retry_after=retry_after)


def parse_retry_after(value, cap):
    if value is None:
        return None
    try:
        seconds = int(float(value))
    except (TypeError, ValueError):
        return None
    return max(0, min(seconds, cap))


class SupplierClient:
    def __init__(self, base_url, *, headers=None, timeout=10, max_bytes=MAX_DOWNLOAD_BYTES, opener=None, clock=utcnow):
        self.base_url = text(base_url, 'base_url', 2048).rstrip('/')
        self.headers = dict(headers or {})
        integer(timeout, 'timeout', 1, 300)
        integer(max_bytes, 'max_bytes', 1, MAX_DOWNLOAD_BYTES)
        self.timeout = timeout
        self.max_bytes = max_bytes
        self.opener = opener or urllib.request.build_opener()
        self.clock = clock

    def _request(self, method, url, body=None, extra_headers=None):
        headers = dict(self.headers)
        if extra_headers:
            headers.update(extra_headers)
        data = None
        if body is not None:
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            headers.setdefault('Content-Type', 'application/json')
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            response = self.opener.open(request, timeout=self.timeout)
        except urllib.error.HTTPError as exc:
            retry_after = parse_retry_after(exc.headers.get('Retry-After'), 3600)
            if exc.code in (408, 425, 429) or 500 <= exc.code < 600:
                raise SupplierUnavailable(f'Supplier responded {exc.code}', retry_after=retry_after) from exc
            raise SupplierRejected(f'Supplier rejected request with {exc.code}', status=exc.code, retry_after=retry_after) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SupplierUnavailable(f'Supplier connection failed: {exc}') from exc
        return response

    @staticmethod
    def _read(response, limit):
        chunks = []
        total = 0
        while True:
            chunk = response.read(65536)
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                fail('download_size', 'Shard exceeds configured byte limit', limit=limit)
            chunks.append(chunk)
        return b''.join(chunks)

    def _json(self, response):
        raw = self._read(response, self.max_bytes)
        try:
            return json.loads(raw.decode('utf-8'))
        except (ValueError, UnicodeError) as exc:
            raise SupplierRejected('Supplier returned malformed JSON', status=response.status) from exc

    def open_export(self, source, start, end, config_revision, idempotency_key):
        identity(source, 'source')
        instant(start, 'start')
        instant(end, 'end')
        integer(config_revision, 'config_revision', 1)
        identity(idempotency_key, 'idempotency_key')
        body = {'source': source, 'start': start, 'end': end, 'config_revision': config_revision}
        response = self._request('POST', self.base_url + '/exports', body, {'Idempotency-Key': idempotency_key})
        retry_after = parse_retry_after(response.headers.get('Retry-After'), 3600)
        return {'status': response.status, 'retry_after': retry_after, 'payload': self._json(response)}

    def get_export(self, export_id):
        identity(export_id, 'export_id')
        response = self._request('GET', f'{self.base_url}/exports/{export_id}')
        retry_after = parse_retry_after(response.headers.get('Retry-After'), 3600)
        return {'status': response.status, 'retry_after': retry_after, 'payload': self._json(response)}

    def refresh(self, export_id):
        identity(export_id, 'export_id')
        response = self._request('POST', f'{self.base_url}/exports/{export_id}/refresh', b'')
        return {'status': response.status, 'retry_after': parse_retry_after(response.headers.get('Retry-After'), 3600),
                'payload': self._json(response)}

    def download(self, url):
        text(url, 'url', 2048)
        target = url if url.startswith('http://') or url.startswith('https://') else self.base_url + (url if url.startswith('/') else '/' + url)
        response = self._request('GET', target)
        declared = response.headers.get('Content-Length')
        if declared is not None:
            try:
                if int(declared) > self.max_bytes:
                    fail('download_size', 'Shard content length exceeds configured byte limit', declared=int(declared))
            except ValueError:
                pass
        return self._read(response, self.max_bytes)
