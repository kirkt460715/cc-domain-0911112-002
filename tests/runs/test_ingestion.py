import copy
from hangar_ingest.types import PipelineError, canonical
from hangar_ingest.decoders import sensors
from hangar_ingest.quality import devices, rules, review
from hangar_ingest.runs import batches, processor, claims, inspection, journal
from hangar_ingest.runs.orchestrator import Pipeline
from tests.support import PipelineCase, sample, START, END


class IngestionTests(PipelineCase):
    def test_chunk_resume_keeps_exact_counts(self):
        data = sensors.encode([sample(1), sample(2), sample(3)])
        staged = batches.stage(self.db, 'batch1', 'feed1', data)
        self.assertEqual(staged['total'], 3)
        first = processor.process(self.db, 'batch1', limit=1)
        self.assertFalse(first['finished'])
        self.assertEqual(first['batch']['cursor'], 1)
        self.assertEqual(first['batch']['accepted'], 1)
        second = processor.process(self.db, 'batch1', limit=2, expected_revision=first['batch']['revision'])
        self.assertTrue(second['finished'])
        self.assertEqual(second['batch']['accepted'], 3)
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 3)
        self.assertTrue(processor.verify_checkpoint(self.db, 'batch1')['ok'])
        self.assertTrue(inspection.check(self.db)['ok'])

    def test_checkpoint_staleness_does_not_process_twice(self):
        staged = batches.stage(self.db, 'batch1', 'feed1', sensors.encode([sample(1), sample(2)]))
        processor.process(self.db, 'batch1', limit=1)
        with self.assertRaises(PipelineError) as failure:
            processor.process(self.db, 'batch1', expected_revision=staged['revision'])
        self.assertEqual(failure.exception.code, 'stale_revision')
        self.assertEqual(batches.get(self.db, 'batch1')['accepted'], 1)
        result = processor.drain(self.db, 'batch1')
        self.assertEqual(result['batch']['accepted'], 2)
        self.assertTrue(processor.verify_checkpoint(self.db, 'batch1')['ok'])

    def test_same_bytes_reuse_existing_batch_identity(self):
        data = sensors.encode([sample()])
        first = batches.stage(self.db, 'first', 'feed1', data)
        second = batches.stage(self.db, 'second', 'feed1', data)
        self.assertEqual(second['batch_id'], first['batch_id'])
        self.assertTrue(second['duplicate'])
        with self.assertRaises(PipelineError):
            batches.stage(self.db, 'first', 'feed1', sensors.encode([sample(2)]))
        self.assertEqual(len(batches.listing(self.db)), 1)
        self.assertEqual(batches.get(self.db, 'first')['state'], 'staged')

    def test_duplicate_events_do_not_duplicate_observations(self):
        self.ingest([sample()], 'batch1')
        result = self.ingest([sample(), sample(2)], 'batch2')
        self.assertEqual(result['batch']['accepted'], 2)
        self.assertEqual(result['batch']['rejected'], 0)
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 2)
        self.assertEqual(batches.rows(self.db, 'batch2')[0]['state'], 'duplicate')
        self.assertTrue(inspection.check(self.db)['ok'])
        self.assertEqual(review.listing(self.db), [])

    def test_conflicting_event_is_quarantined(self):
        self.ingest([sample(1, '20')], 'batch1')
        result = self.ingest([sample(1, '30')], 'batch2')
        self.assertEqual(result['batch']['state'], 'partial')
        self.assertEqual(result['batch']['rejected'], 1)
        quarantined = review.listing(self.db)[0]
        self.assertEqual(quarantined['reasons']['code'], 'event_conflict')
        self.assertEqual(self.db.scalar('SELECT value FROM observations'), '20')
        self.assertEqual(review.reason_counts(self.db), {'event_conflict': 1})
        self.assertTrue(inspection.check(self.db)['ok'])

    def test_keep_first_policy_records_ignored_conflict(self):
        rules.configure(self.db, 'dev1', revision=1, duplicate_policy='keep_first')
        self.ingest([sample(1, '20')], 'batch1')
        result = self.ingest([sample(1, '30')], 'batch2')
        self.assertEqual(result['batch']['accepted'], 1)
        self.assertEqual(batches.rows(self.db, 'batch2')[0]['state'], 'ignored_conflict')
        self.assertEqual(self.db.scalar('SELECT value FROM observations'), '20')
        self.assertEqual(review.listing(self.db), [])
        self.assertTrue(processor.verify_checkpoint(self.db, 'batch2')['ok'])

    def test_invalid_and_unknown_device_rows_do_not_abort_batch(self):
        content = canonical(sample()).encode() + b'\ninvalid\n' + canonical(sample(2, device='missing')).encode() + b'\n'
        batches.stage(self.db, 'batch1', 'feed1', content)
        result = processor.drain(self.db, 'batch1')
        self.assertEqual(result['batch']['accepted'], 1)
        self.assertEqual(result['batch']['rejected'], 2)
        self.assertEqual(result['batch']['state'], 'partial')
        reasons = review.reason_counts(self.db)
        self.assertEqual(reasons, {'alias_unresolved': 1, 'malformed_json': 1})
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 1)
        self.assertTrue(inspection.check(self.db)['ok'])

    def test_pause_resume_and_cancel_preserve_checkpoint(self):
        batches.stage(self.db, 'batch1', 'feed1', sensors.encode([sample(1), sample(2)]))
        first = processor.process(self.db, 'batch1', limit=1)['batch']
        paused = batches.pause(self.db, 'batch1', first['revision'])
        self.assertEqual(paused['cursor'], 1)
        with self.assertRaises(PipelineError):
            processor.process(self.db, 'batch1')
        resumed = batches.resume(self.db, 'batch1', paused['revision'])
        self.assertEqual(resumed['state'], 'processing')
        cancelled = batches.cancel(self.db, 'batch1', resumed['revision'], 'Synthetic cancellation')
        self.assertEqual(cancelled['state'], 'cancelled')
        self.assertEqual(cancelled['accepted'], 1)
        with self.assertRaises(PipelineError):
            batches.resume(self.db, 'batch1', cancelled['revision'])

    def test_transaction_failure_rolls_back_all_chunk_effects(self):
        batches.stage(self.db, 'batch1', 'feed1', sensors.encode([sample()]))
        before = journal.verify(self.db)
        with self.assertRaises(RuntimeError):
            with self.db.transaction():
                processor.process(self.db, 'batch1')
                raise RuntimeError('simulated crash')
        self.assertEqual(batches.get(self.db, 'batch1')['cursor'], 0)
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 0)
        self.assertEqual(journal.verify(self.db), before)
        self.assertEqual(processor.drain(self.db, 'batch1')['batch']['accepted'], 1)
        self.assertTrue(inspection.check(self.db)['ok'])

    def test_claim_expiry_fences_old_worker(self):
        batches.stage(self.db, 'batch1', 'feed1', sensors.encode([sample()]))
        first = claims.acquire(self.db, 'batch1', 'worker1', ttl=2)
        with self.assertRaises(PipelineError):
            claims.acquire(self.db, 'batch1', 'worker2')
        self.clock.tick(2)
        second = claims.acquire(self.db, 'batch1', 'worker2')
        self.assertEqual(second['generation'], first['generation'] + 1)
        with self.assertRaises(PipelineError) as stale:
            processor.process(self.db, 'batch1', claim=first)
        self.assertEqual(stale.exception.code, 'claim_stale')
        result = processor.process(self.db, 'batch1', claim=second)
        self.assertTrue(result['finished'])
        self.assertEqual(claims.release(self.db, 'batch1', second), {'released': True})

    def test_claim_renew_and_recovery_pipeline(self):
        batches.stage(self.db, 'batch1', 'feed1', sensors.encode([sample(1), sample(2)]))
        token = claims.acquire(self.db, 'batch1', 'worker', ttl=2)
        self.clock.tick(1)
        renewed = claims.renew(self.db, 'batch1', token, ttl=10)
        self.assertGreater(renewed['expires_at'], token['expires_at'])
        claims.release(self.db, 'batch1', renewed)
        pipeline = Pipeline(self.db)
        recovered = pipeline.recover(chunk=1)
        self.assertEqual(len(recovered), 1)
        self.assertFalse(recovered[0]['finished'])
        self.assertTrue(pipeline.recover(chunk=1)[0]['finished'])
        self.assertEqual(pipeline.recover(), [])

    def test_quarantine_replay_updates_original_batch_counts(self):
        self.ingest([sample(1, '500')])
        entry = review.listing(self.db)[0]
        self.assertEqual(entry['reasons']['code'], 'quality_rejected')
        replayed = review.replay(self.db, entry['quarantine_id'], 1, sample(1, '25'), 'Corrected synthetic sensor value')
        self.assertEqual(replayed['status'], 'replayed')
        batch = batches.get(self.db, 'batch1')
        self.assertEqual(batch['accepted'], 1)
        self.assertEqual(batch['rejected'], 0)
        self.assertEqual(batch['state'], 'complete')
        self.assertEqual(self.db.scalar('SELECT value FROM observations'), '25')
        self.assertTrue(processor.verify_checkpoint(self.db, 'batch1')['ok'])
        self.assertTrue(inspection.check(self.db)['ok'])
        with self.assertRaises(PipelineError):
            review.replay(self.db, entry['quarantine_id'], 1, sample(), 'Old review')

    def test_dismissed_quarantine_remains_rejected_data(self):
        self.ingest([sample(1, '500')])
        entry = review.listing(self.db)[0]
        dismissed = review.dismiss(self.db, entry['quarantine_id'], 1, 'Confirmed invalid measurement')
        self.assertEqual(dismissed['status'], 'dismissed')
        self.assertEqual(review.listing(self.db), [])
        self.assertEqual(len(review.listing(self.db, status='dismissed')), 1)
        self.assertEqual(batches.get(self.db, 'batch1')['rejected'], 1)
        self.assertTrue(processor.verify_checkpoint(self.db, 'batch1')['ok'])
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 0)
