from hangar_ingest.types import PipelineError
from hangar_ingest.runs.database import Database
from hangar_ingest.runs import batches, processor, claims, partitions, inspection
from hangar_ingest.decoders import sensors
from hangar_ingest.quality import devices
from hangar_ingest.exports import releases, watermarks
from tests.support import PipelineCase, sample, END, PARTITION


class ConcurrentStateTests(PipelineCase):
    def test_two_connections_compare_and_swap_checkpoint(self):
        batches.stage(self.db, 'batch1', 'feed1', sensors.encode([sample(1), sample(2)]))
        with Database(self.path, clock=self.clock) as second:
            read_a = batches.get(self.db, 'batch1')
            read_b = batches.get(second, 'batch1')
            self.assertEqual(read_a['revision'], read_b['revision'])
            first = processor.process(self.db, 'batch1', limit=1, expected_revision=read_a['revision'])
            with self.assertRaises(PipelineError) as stale:
                processor.process(second, 'batch1', limit=1, expected_revision=read_b['revision'])
            self.assertEqual(stale.exception.code, 'stale_revision')
            self.assertEqual(second.scalar('SELECT COUNT(*) FROM observations'), 1)
            next_result = processor.process(second, 'batch1', limit=1, expected_revision=first['batch']['revision'])
            self.assertTrue(next_result['finished'])
            self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 2)
            self.assertTrue(inspection.check(second)['ok'])

    def test_two_connections_fence_reclaimed_batch(self):
        batches.stage(self.db, 'batch1', 'feed1', sensors.encode([sample()]))
        first = claims.acquire(self.db, 'batch1', 'old', ttl=1)
        self.clock.tick(1)
        with Database(self.path, clock=self.clock) as second:
            replacement = claims.acquire(second, 'batch1', 'new', ttl=10)
            self.assertGreater(replacement['generation'], first['generation'])
            with self.assertRaises(PipelineError):
                processor.process(self.db, 'batch1', claim=first)
            self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 0)
            self.assertEqual(processor.process(second, 'batch1', claim=replacement)['batch']['accepted'], 1)
            self.assertEqual(batches.get(self.db, 'batch1')['state'], 'complete')

    def test_stale_calibration_blocks_new_release_not_old_snapshot(self):
        self.ingest([sample()])
        previous = self.release('old')
        devices.correct_calibration(self.db, 'cal1', 1, '1', '1', 0)
        with self.assertRaises(PipelineError) as error:
            releases.prepare(self.db, 'new', [PARTITION])
        self.assertEqual(error.exception.code, 'release_calibration_stale')
        self.assertEqual(releases.payloads(self.db, 'old'), previous)
        self.assertEqual(len(releases.list_releases(self.db)), 1)
        self.assertTrue(releases.verify_package(previous)['ok'])

    def test_calibration_change_after_prepare_blocks_commit(self):
        self.ingest([sample()])
        partitions.rebuild_dirty(self.db)
        watermarks.advance(self.db, 'feed1', END)
        releases.prepare(self.db, 'prepared', [PARTITION])
        devices.correct_calibration(self.db, 'cal1', 1, '1', '1', 0)
        with self.assertRaises(PipelineError) as error:
            releases.commit(self.db, 'prepared')
        self.assertEqual(error.exception.code, 'release_calibration_stale')
        self.assertEqual(releases.get(self.db, 'prepared')['state'], 'prepared')
        self.assertIsNone(releases.get(self.db, 'prepared')['committed_at'])

    def test_empty_batch_finishes_without_fake_observations(self):
        staged = batches.stage(self.db, 'empty', 'feed1', b'\n\n')
        self.assertEqual(staged['total'], 0)
        finished = processor.drain(self.db, 'empty')
        self.assertEqual(finished['batch']['state'], 'complete')
        self.assertEqual(finished['batch']['accepted'], 0)
        self.assertEqual(finished['batch']['rejected'], 0)
        self.assertEqual(finished['batch']['cursor'], 0)
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 0)
        self.assertEqual(partitions.listing(self.db), [])
        self.assertTrue(processor.verify_checkpoint(self.db, 'empty')['ok'])
        self.assertTrue(inspection.check(self.db)['ok'])
