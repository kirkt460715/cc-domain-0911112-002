import base64
import hashlib
import json
from hangar_ingest.types import canonical, digest, fail, identity, integer, object_fields
from hangar_ingest.exports import releases
from hangar_ingest.runs import journal

SCHEMA = '''
CREATE TABLE IF NOT EXISTS transfers(
 transfer_id TEXT PRIMARY KEY,manifest TEXT NOT NULL,state TEXT NOT NULL,
 total_chunks INTEGER NOT NULL,total_bytes INTEGER NOT NULL,content_hash TEXT NOT NULL,
 created_at TEXT NOT NULL,completed_at TEXT);
CREATE TABLE IF NOT EXISTS transfer_chunks(
 transfer_id TEXT NOT NULL REFERENCES transfers(transfer_id),chunk_index INTEGER NOT NULL,
 content BLOB NOT NULL,sha256 TEXT NOT NULL,PRIMARY KEY(transfer_id,chunk_index));
CREATE TABLE IF NOT EXISTS received_packages(
 transfer_id TEXT PRIMARY KEY REFERENCES transfers(transfer_id),release_id TEXT NOT NULL,
 release_digest TEXT NOT NULL,payload TEXT NOT NULL);
'''


def make_chunks(db, release_id, *, chunk_bytes=65536):
    integer(chunk_bytes, 'chunk_bytes', 1024, 1048576)
    package = releases.payloads(db, release_id)
    raw = canonical(package).encode()
    pieces = [raw[index:index + chunk_bytes] for index in range(0, len(raw), chunk_bytes)]
    descriptors = [{'index': index, 'bytes': len(value), 'sha256': hashlib.sha256(value).hexdigest()} for index, value in enumerate(pieces)]
    manifest = {
        'format': 'hangar-transfer',
        'version': 1,
        'release_id': release_id,
        'release_digest': package['digest'],
        'total_bytes': len(raw),
        'sha256': hashlib.sha256(raw).hexdigest(),
        'chunks': descriptors,
    }
    return {'transfer_id': digest(manifest), 'manifest': manifest, 'chunks': [base64.b64encode(value).decode() for value in pieces]}


class Receiver:
    def __init__(self, db):
        self.db = db
        if db.depth:
            fail('transaction_scope', 'Initialize transfer receiver outside transaction')
        db.db.executescript(SCHEMA)

    def begin(self, transfer_id, manifest):
        fields = {'format', 'version', 'release_id', 'release_digest', 'total_bytes', 'sha256', 'chunks'}
        object_fields(manifest, fields)
        if manifest['format'] != 'hangar-transfer' or type(manifest['version']) is not int or manifest['version'] != 1:
            fail('transfer_format', 'Unsupported transfer format')
        if digest(manifest) != transfer_id:
            fail('transfer_identity', 'Transfer identifier does not match manifest')
        identity(manifest['release_id'], 'release_id')
        integer(manifest['total_bytes'], 'total_bytes', 1, 128 * 1024 * 1024)
        chunks = manifest['chunks']
        if not isinstance(chunks, list) or not 1 <= len(chunks) <= 131072:
            fail('transfer_chunks', 'Invalid transfer chunk list')
        total = 0
        for index, chunk in enumerate(chunks):
            object_fields(chunk, {'index', 'bytes', 'sha256'})
            if chunk['index'] != index:
                fail('transfer_order', 'Chunk descriptors must be contiguous')
            total += integer(chunk['bytes'], 'chunk_bytes', 1, 1048576)
            if not isinstance(chunk['sha256'], str) or len(chunk['sha256']) != 64:
                fail('transfer_digest', 'Chunk digest must be SHA256')
        if total != manifest['total_bytes']:
            fail('transfer_size', 'Chunk sizes do not match total size')
        with self.db.transaction():
            existing = self.db.one('SELECT manifest FROM transfers WHERE transfer_id=?', (transfer_id,))
            if existing:
                if existing['manifest'] != canonical(manifest):
                    fail('transfer_conflict', 'Transfer manifest differs')
                return self.status(transfer_id)
            self.db.execute(
                "INSERT INTO transfers(transfer_id,manifest,state,total_chunks,total_bytes,content_hash,created_at) VALUES(?,?,'receiving',?,?,?,?)",
                (transfer_id, canonical(manifest), len(chunks), total, manifest['sha256'], self.db.clock()),
            )
            journal.record(self.db, 'transfer.begun', {'transfer_id': transfer_id, 'chunks': len(chunks)})
            return self.status(transfer_id)

    def status(self, transfer_id):
        row = self.db.one('SELECT * FROM transfers WHERE transfer_id=?', (transfer_id,))
        if row is None:
            fail('unknown_transfer', 'Transfer does not exist')
        present = {item['chunk_index'] for item in self.db.all('SELECT chunk_index FROM transfer_chunks WHERE transfer_id=?', (transfer_id,))}
        row['manifest'] = json.loads(row['manifest'])
        row['received_chunks'] = len(present)
        row['missing'] = [index for index in range(row['total_chunks']) if index not in present]
        return row

    def accept(self, transfer_id, chunk_index, content):
        integer(chunk_index, 'chunk_index')
        if not isinstance(content, str) or len(content) > 1400000:
            fail('chunk_encoding', 'Chunk must be bounded base64 text')
        try:
            raw = base64.b64decode(content, validate=True)
        except ValueError as exc:
            from hangar_ingest.types import PipelineError
            raise PipelineError('chunk_encoding', 'Invalid base64 content') from exc
        with self.db.transaction():
            status = self.status(transfer_id)
            if chunk_index >= status['total_chunks']:
                fail('chunk_index', 'Chunk index outside manifest')
            descriptor = status['manifest']['chunks'][chunk_index]
            checksum = hashlib.sha256(raw).hexdigest()
            if len(raw) != descriptor['bytes'] or checksum != descriptor['sha256']:
                fail('chunk_integrity', 'Chunk content differs from manifest')
            previous = self.db.one('SELECT sha256 FROM transfer_chunks WHERE transfer_id=? AND chunk_index=?', (transfer_id, chunk_index))
            if previous:
                return {'status': 'duplicate', 'chunk_index': chunk_index}
            if status['state'] != 'receiving':
                fail('transfer_state', 'Transfer is not receiving')
            self.db.execute('INSERT INTO transfer_chunks(transfer_id,chunk_index,content,sha256) VALUES(?,?,?,?)', (transfer_id, chunk_index, raw, checksum))
            return {'status': 'accepted', 'chunk_index': chunk_index}

    def finish(self, transfer_id):
        with self.db.transaction():
            status = self.status(transfer_id)
            if status['state'] == 'complete':
                return self.package(transfer_id)
            if status['missing']:
                fail('transfer_incomplete', 'Transfer has missing chunks', missing=status['missing'])
            chunks = self.db.all('SELECT content FROM transfer_chunks WHERE transfer_id=? ORDER BY chunk_index', (transfer_id,))
            raw = b''.join(row['content'] for row in chunks)
            if len(raw) != status['total_bytes'] or hashlib.sha256(raw).hexdigest() != status['content_hash']:
                fail('transfer_integrity', 'Assembled content does not match manifest')
            try:
                package = json.loads(raw)
            except (ValueError, UnicodeError) as exc:
                from hangar_ingest.types import PipelineError
                raise PipelineError('transfer_json', 'Package content is not valid JSON') from exc
            releases.verify_package(package)
            if package['digest'] != status['manifest']['release_digest'] or package['manifest']['release_id'] != status['manifest']['release_id']:
                fail('transfer_release', 'Assembled release identity differs')
            self.db.execute('INSERT INTO received_packages(transfer_id,release_id,release_digest,payload) VALUES(?,?,?,?)', (transfer_id, package['manifest']['release_id'], package['digest'], canonical(package)))
            self.db.execute("UPDATE transfers SET state='complete',completed_at=? WHERE transfer_id=?", (self.db.clock(), transfer_id))
            journal.record(self.db, 'transfer.completed', {'transfer_id': transfer_id, 'release_digest': package['digest']})
            return package

    def package(self, transfer_id):
        row = self.db.one('SELECT payload FROM received_packages WHERE transfer_id=?', (transfer_id,))
        if row is None:
            fail('transfer_incomplete', 'Transfer has not completed')
        value = json.loads(row['payload'])
        releases.verify_package(value)
        return value
