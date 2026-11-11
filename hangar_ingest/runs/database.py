import contextlib
import sqlite3
from pathlib import Path

from hangar_ingest.types import fail, utcnow

SCHEMA = '''
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS devices(
 device_id TEXT PRIMARY KEY,aircraft TEXT NOT NULL,component TEXT NOT NULL,
 metric TEXT NOT NULL,canonical_unit TEXT NOT NULL,revision INTEGER NOT NULL,
 active INTEGER NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS aliases(
 source TEXT NOT NULL,external_id TEXT NOT NULL,device_id TEXT NOT NULL REFERENCES devices(device_id),
 valid_from TEXT NOT NULL,valid_until TEXT NOT NULL,PRIMARY KEY(source,external_id,valid_from));
CREATE TABLE IF NOT EXISTS calibrations(
 calibration_id TEXT PRIMARY KEY,device_id TEXT NOT NULL REFERENCES devices(device_id),
 valid_from TEXT NOT NULL,valid_until TEXT NOT NULL,scale TEXT NOT NULL,offset TEXT NOT NULL,
 clock_offset_us INTEGER NOT NULL,revision INTEGER NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS quality_rules(
 device_id TEXT PRIMARY KEY REFERENCES devices(device_id),revision INTEGER NOT NULL,
 minimum TEXT,maximum TEXT,max_delta TEXT,max_gap_seconds INTEGER NOT NULL,
 duplicate_policy TEXT NOT NULL,required_calibration INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS work_orders(
 order_id TEXT PRIMARY KEY,revision INTEGER NOT NULL,aircraft TEXT NOT NULL,component TEXT NOT NULL,
 opened_at TEXT NOT NULL,closed_at TEXT NOT NULL,issue TEXT NOT NULL,resolution TEXT NOT NULL,
 state TEXT NOT NULL,payload_hash TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS order_versions(
 order_id TEXT NOT NULL,revision INTEGER NOT NULL,payload TEXT NOT NULL,
 PRIMARY KEY(order_id,revision));
CREATE TABLE IF NOT EXISTS batches(
 batch_id TEXT PRIMARY KEY,source TEXT NOT NULL,content_hash TEXT NOT NULL,
 format TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,
 cursor INTEGER NOT NULL,total INTEGER NOT NULL,accepted INTEGER NOT NULL,rejected INTEGER NOT NULL,
 created_at TEXT NOT NULL,updated_at TEXT NOT NULL,UNIQUE(source,content_hash,format));
CREATE TABLE IF NOT EXISTS raw_rows(
 batch_id TEXT NOT NULL REFERENCES batches(batch_id),row_number INTEGER NOT NULL,
 payload TEXT NOT NULL,raw_hash TEXT NOT NULL,state TEXT NOT NULL,error TEXT,
 PRIMARY KEY(batch_id,row_number));
CREATE TABLE IF NOT EXISTS observations(
 observation_id TEXT PRIMARY KEY,source TEXT NOT NULL,external_event TEXT NOT NULL,
 device_id TEXT NOT NULL REFERENCES devices(device_id),raw_time TEXT NOT NULL,event_time TEXT NOT NULL,
 value TEXT NOT NULL,unit TEXT NOT NULL,raw_value TEXT NOT NULL,raw_unit TEXT NOT NULL,
 calibration_id TEXT,calibration_revision INTEGER,raw_hash TEXT NOT NULL,
 batch_id TEXT NOT NULL,row_number INTEGER NOT NULL,quality TEXT NOT NULL,
 created_at TEXT NOT NULL,UNIQUE(source,external_event));
CREATE INDEX IF NOT EXISTS observations_window ON observations(device_id,event_time,observation_id);
CREATE TABLE IF NOT EXISTS quarantine(
 quarantine_id TEXT PRIMARY KEY,batch_id TEXT NOT NULL,row_number INTEGER NOT NULL,
 reasons TEXT NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL,
 revision INTEGER NOT NULL,decision TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
 UNIQUE(batch_id,row_number));
CREATE TABLE IF NOT EXISTS partitions(
 partition_key TEXT PRIMARY KEY,revision INTEGER NOT NULL,state TEXT NOT NULL,
 dirty_reason TEXT,watermark TEXT,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS partition_versions(
 partition_key TEXT NOT NULL,revision INTEGER NOT NULL,payload TEXT NOT NULL,digest TEXT NOT NULL,
 created_at TEXT NOT NULL,PRIMARY KEY(partition_key,revision));
CREATE TABLE IF NOT EXISTS releases(
 release_id TEXT PRIMARY KEY,state TEXT NOT NULL,manifest TEXT NOT NULL,digest TEXT NOT NULL,
 created_at TEXT NOT NULL,committed_at TEXT);
CREATE TABLE IF NOT EXISTS audit(
 position INTEGER PRIMARY KEY AUTOINCREMENT,action TEXT NOT NULL,details TEXT NOT NULL,
 at TEXT NOT NULL,previous_hash TEXT NOT NULL,entry_hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS source_watermarks(
 source TEXT PRIMARY KEY,watermark TEXT NOT NULL,revision INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS processing_claims(
 batch_id TEXT PRIMARY KEY,owner TEXT NOT NULL,generation INTEGER NOT NULL,expires_at TEXT NOT NULL);
'''


class Database:
    def __init__(self, path=':memory:', *, clock=utcnow):
        self.path = str(path)
        self.clock = clock
        self.db = sqlite3.connect(self.path, isolation_level=None, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.depth = 0
        version = self.db.execute('PRAGMA user_version').fetchone()[0]
        if version not in (0, 1):
            self.db.close()
            fail('schema_version', 'Unsupported database version', version=version)
        self.db.executescript(SCHEMA)
        self.db.execute('PRAGMA user_version=1')

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close()

    def close(self):
        self.db.close()

    @contextlib.contextmanager
    def transaction(self):
        current = self.depth
        if current:
            self.db.execute(f'SAVEPOINT nested_{current}')
        else:
            self.db.execute('BEGIN IMMEDIATE')
        self.depth += 1
        try:
            yield self
        except BaseException:
            if current:
                self.db.execute(f'ROLLBACK TO nested_{current}')
                self.db.execute(f'RELEASE nested_{current}')
            else:
                self.db.rollback()
            raise
        else:
            if current:
                self.db.execute(f'RELEASE nested_{current}')
            else:
                self.db.commit()
        finally:
            self.depth -= 1

    def execute(self, sql, values=()):
        return self.db.execute(sql, values)

    def one(self, sql, values=()):
        row = self.execute(sql, values).fetchone()
        return dict(row) if row is not None else None

    def all(self, sql, values=()):
        return [dict(row) for row in self.execute(sql, values).fetchall()]

    def scalar(self, sql, values=()):
        row = self.execute(sql, values).fetchone()
        return row[0] if row else None

    def integrity(self):
        rows = [row[0] for row in self.execute('PRAGMA integrity_check')]
        foreign = self.all('PRAGMA foreign_key_check')
        return {'ok': rows == ['ok'] and not foreign, 'checks': rows, 'foreign_keys': foreign}

    def backup(self, destination):
        target = Path(destination)
        if target.exists() or self.depth:
            fail('backup_conflict', 'Backup requires a new destination outside a transaction')
        with sqlite3.connect(str(target)) as other:
            self.db.backup(other)
        return {'path': str(target), 'bytes': target.stat().st_size}
