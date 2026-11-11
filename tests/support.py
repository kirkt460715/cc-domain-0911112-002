import datetime as dt
import tempfile
import unittest
from pathlib import Path
from hangar_ingest.runs.database import Database
from hangar_ingest.quality import devices, rules
from hangar_ingest.decoders import sensors, workorders
from hangar_ingest.runs import batches, processor, partitions
from hangar_ingest.exports import releases, watermarks

START = '2026-11-11T00:00:00Z'
END = '2026-11-12T00:00:00Z'
PARTITION = 'plane1/2026-11-11'


class Clock:
    def __init__(self):
        self.point = dt.datetime(2026, 11, 12, tzinfo=dt.timezone.utc)

    def __call__(self):
        return self.point.isoformat(timespec='microseconds').replace('+00:00', 'Z')

    def tick(self, seconds):
        self.point += dt.timedelta(seconds=seconds)


def sample(index=1, value='20', time=None, device='sensor1', unit='C'):
    return {'event_id': f'event{index}', 'device': device, 'observed_at': time or f'2026-11-11T10:00:{index:02d}Z', 'value': value, 'unit': unit}


def order(order_id='order1', revision=1, **changes):
    row = {'order_id': order_id, 'revision': revision, 'aircraft': 'plane1', 'component': 'engine1', 'opened_at': '2026-11-11T09:00:00Z', 'closed_at': '2026-11-11T11:00:00Z', 'issue': 'Synthetic temperature excursion', 'resolution': 'Synthetic inspection', 'state': 'closed'}
    row.update(changes)
    return row


class PipelineCase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / 'pipeline.db'
        self.clock = Clock()
        self.db = Database(self.path, clock=self.clock)
        devices.register(self.db, 'dev1', 'plane1', 'engine1', 'temperature', 'C')
        devices.add_alias(self.db, 'feed1', 'sensor1', 'dev1', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z')
        devices.add_calibration(self.db, 'cal1', 'dev1', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z')
        rules.configure(self.db, 'dev1', minimum='-100', maximum='200')

    def tearDown(self):
        self.db.close()
        self.directory.cleanup()

    def ingest(self, rows, batch_id='batch1', source='feed1', format='ndjson'):
        batches.stage(self.db, batch_id, source, sensors.encode(rows, format), format)
        return processor.drain(self.db, batch_id, chunk=2)

    def release(self, release_id='release1'):
        partitions.rebuild_dirty(self.db)
        old = self.db.one('SELECT revision FROM source_watermarks WHERE source=?', ('feed1',))
        watermarks.advance(self.db, 'feed1', END, old['revision'] if old else 0)
        releases.prepare(self.db, release_id, [PARTITION])
        releases.commit(self.db, release_id)
        return releases.payloads(self.db, release_id)
