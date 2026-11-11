import copy
import json
from hangar_ingest.types import PipelineError, digest
from hangar_ingest.decoders import sensors, workorders
from hangar_ingest.quality import devices
from hangar_ingest.runs import batches, partitions, inspection
from hangar_ingest.runs.database import Database
from hangar_ingest.runs.manifests import SourceReconciliation
from hangar_ingest.exports import releases, watermarks, transfer, tabular
from hangar_ingest.exports.contracts import DeliveryContracts
from tests.support import PipelineCase, sample, order, START, END, PARTITION


class ReleaseTests(PipelineCase):
    def test_unsealed_partition_blocks_release(self):
        self.ingest([sample()])
        partitions.materialize(self.db, PARTITION)
        readiness = watermarks.readiness(self.db, PARTITION)
        self.assertFalse(readiness['ready'])
        self.assertEqual(readiness['missing_watermarks'][0]['source'], 'feed1')
        with self.assertRaises(PipelineError) as rejected:
            releases.prepare(self.db, 'r1', [PARTITION])
        self.assertEqual(rejected.exception.code, 'watermark_pending')
        self.assertEqual(releases.list_releases(self.db), [])
        watermarks.advance(self.db, 'feed1', END)
        self.assertTrue(watermarks.readiness(self.db, PARTITION)['ready'])
        self.assertEqual(releases.prepare(self.db, 'r1', [PARTITION])['state'], 'prepared')

    def test_dirty_partition_and_pending_batch_block_seal(self):
        self.ingest([sample()])
        with self.assertRaises(PipelineError):
            releases.prepare(self.db, 'dirty', [PARTITION], require_watermark=False)
        partitions.materialize(self.db, PARTITION)
        watermarks.advance(self.db, 'feed1', END)
        batches.stage(self.db, 'pending', 'feed1', sensors.encode([sample(2)]))
        readiness = watermarks.readiness(self.db, PARTITION)
        self.assertFalse(readiness['ready'])
        self.assertEqual(readiness['pending_batches'][0]['batch_id'], 'pending')
        with self.assertRaises(PipelineError):
            releases.prepare(self.db, 'r1', [PARTITION])
        self.assertEqual(releases.list_releases(self.db), [])

    def test_prepare_commit_detects_late_data(self):
        self.ingest([sample()])
        partitions.materialize(self.db, PARTITION)
        watermarks.advance(self.db, 'feed1', END)
        releases.prepare(self.db, 'r1', [PARTITION])
        self.ingest([sample(2)], 'late')
        with self.assertRaises(PipelineError) as rejected:
            releases.commit(self.db, 'r1')
        self.assertEqual(rejected.exception.code, 'release_stale')
        self.assertEqual(releases.get(self.db, 'r1')['state'], 'prepared')
        self.assertEqual(releases.abort(self.db, 'r1', 'Rebuild late data')['state'], 'aborted')
        with self.assertRaises(PipelineError):
            releases.commit(self.db, 'r1')

    def test_committed_release_retains_old_partition_after_rebuild(self):
        self.ingest([sample(1, '10')])
        old = self.release('r1')
        self.ingest([sample(2, '20')], 'late')
        new = self.release('r2')
        self.assertEqual(len(old['partitions'][0]['payload']['observations']), 1)
        self.assertEqual(len(new['partitions'][0]['payload']['observations']), 2)
        self.assertEqual(releases.payloads(self.db, 'r1'), old)
        self.assertNotEqual(new['digest'], old['digest'])
        difference = tabular.compare_releases(self.db, 'r1', 'r2')
        self.assertEqual(difference['count'], 1)
        self.assertEqual(difference['changes'][0]['kind'], 'added')
        self.assertEqual(releases.commit(self.db, 'r1')['state'], 'committed')

    def test_package_digest_and_count_tampering(self):
        self.ingest([sample()])
        package = self.release()
        self.assertTrue(releases.verify_package(package)['ok'])
        damaged = copy.deepcopy(package)
        damaged['partitions'][0]['payload']['observations'][0]['value'] = '999'
        with self.assertRaises(PipelineError) as error:
            releases.verify_package(damaged)
        self.assertEqual(error.exception.code, 'package_partition')
        damaged = copy.deepcopy(package)
        damaged['partitions'].append(copy.deepcopy(damaged['partitions'][0]))
        with self.assertRaises(PipelineError):
            releases.verify_package(damaged)
        self.assertEqual(releases.verify_package(package)['partitions'], 1)

    def test_directory_export_is_complete_and_non_overwriting(self):
        self.ingest([sample()])
        package = self.release()
        target = self.path.parent / 'exported'
        exported = releases.export_directory(self.db, 'release1', target)
        self.assertGreater(exported['bytes'], 0)
        stored = json.loads((target / 'release.json').read_text())
        self.assertEqual(stored, package)
        self.assertTrue(releases.verify_package(stored)['ok'])
        with self.assertRaises(PipelineError):
            releases.export_directory(self.db, 'release1', target)
        self.assertEqual(list(self.path.parent.glob('.exported.*')), [])

    def test_tabular_export_and_window_deduplication(self):
        workorders.upsert(self.db, order())
        self.ingest([sample(1, '-20'), sample(2, '30')])
        self.release()
        csv = tabular.csv_bytes(self.db, 'release1').decode()
        self.assertIn('primary_order', csv)
        self.assertIn('order1', csv)
        lines = tabular.ndjson_bytes(self.db, 'release1').decode().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])['value'], '-20')
        self.assertEqual(tabular.safe_cell('=SUM(1,2)'), "'=SUM(1,2)")
        self.assertEqual(tabular.safe_cell('-20'), '-20')
        windows = tabular.window_rows(self.db, 'release1')
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0]['count'], 2)

    def test_watermark_is_monotone_and_versioned(self):
        first = watermarks.advance(self.db, 'feed1', START)
        self.assertEqual(first['revision'], 1)
        self.assertEqual(watermarks.advance(self.db, 'feed1', START, 1), first)
        with self.assertRaises(PipelineError):
            watermarks.advance(self.db, 'feed1', END, 0)
        with self.assertRaises(PipelineError):
            watermarks.advance(self.db, 'feed1', '2026-11-10T00:00:00Z', 1)
        second = watermarks.advance(self.db, 'feed1', END, 1)
        self.assertEqual(second['revision'], 2)
        self.assertEqual(len(watermarks.list_watermarks(self.db)), 1)
        self.assertEqual(watermarks.list_watermarks(self.db)[0]['watermark'], '2026-11-12T00:00:00.000000Z')


class TransferTests(PipelineCase):
    def setUp(self):
        super().setUp()
        self.ingest([sample(1), sample(2), sample(3)])
        self.package = self.release()
        self.outbound = transfer.make_chunks(self.db, 'release1', chunk_bytes=1024)
        self.target = Database(':memory:')
        self.receiver = transfer.Receiver(self.target)

    def tearDown(self):
        self.target.close()
        super().tearDown()

    def test_out_of_order_chunk_resume_and_idempotent_finish(self):
        data = self.outbound
        status = self.receiver.begin(data['transfer_id'], data['manifest'])
        self.assertEqual(len(status['missing']), len(data['chunks']))
        for index in reversed(range(len(data['chunks']))):
            result = self.receiver.accept(data['transfer_id'], index, data['chunks'][index])
            self.assertEqual(result['status'], 'accepted')
        self.assertEqual(self.receiver.status(data['transfer_id'])['missing'], [])
        self.assertEqual(self.receiver.finish(data['transfer_id']), self.package)
        self.assertEqual(self.receiver.finish(data['transfer_id']), self.package)
        self.assertEqual(self.receiver.status(data['transfer_id'])['state'], 'complete')
        self.assertTrue(self.target.integrity()['ok'])

    def test_partial_transfer_never_exposes_package(self):
        data = self.outbound
        self.receiver.begin(data['transfer_id'], data['manifest'])
        self.receiver.accept(data['transfer_id'], 0, data['chunks'][0])
        with self.assertRaises(PipelineError) as incomplete:
            self.receiver.finish(data['transfer_id'])
        self.assertEqual(incomplete.exception.code, 'transfer_incomplete')
        with self.assertRaises(PipelineError):
            self.receiver.package(data['transfer_id'])
        repeated = self.receiver.begin(data['transfer_id'], data['manifest'])
        self.assertEqual(repeated['received_chunks'], 1)
        self.assertEqual(self.target.scalar('SELECT COUNT(*) FROM received_packages'), 0)

    def test_corrupt_chunk_does_not_advance_received_count(self):
        data = self.outbound
        self.receiver.begin(data['transfer_id'], data['manifest'])
        with self.assertRaises(PipelineError):
            self.receiver.accept(data['transfer_id'], 0, 'AAAA')
        self.assertEqual(self.receiver.status(data['transfer_id'])['received_chunks'], 0)
        self.receiver.accept(data['transfer_id'], 0, data['chunks'][0])
        duplicate = self.receiver.accept(data['transfer_id'], 0, data['chunks'][0])
        self.assertEqual(duplicate['status'], 'duplicate')
        self.assertEqual(self.receiver.status(data['transfer_id'])['received_chunks'], 1)
        with self.assertRaises(PipelineError):
            self.receiver.accept(data['transfer_id'], len(data['chunks']), data['chunks'][0])

    def test_manifest_identity_and_descriptor_count_are_checked(self):
        data = self.outbound
        damaged = copy.deepcopy(data['manifest'])
        damaged['total_bytes'] += 1
        with self.assertRaises(PipelineError) as bad:
            self.receiver.begin(data['transfer_id'], damaged)
        self.assertEqual(bad.exception.code, 'transfer_identity')
        with self.assertRaises(PipelineError) as size:
            self.receiver.begin(digest(damaged), damaged)
        self.assertEqual(size.exception.code, 'transfer_size')
        self.assertEqual(self.target.scalar('SELECT COUNT(*) FROM transfers'), 0)


class ContractTests(PipelineCase):
    def setUp(self):
        super().setUp()
        self.contracts = DeliveryContracts(self.db)

    def test_accepted_delivery_report_has_stable_evidence(self):
        self.ingest([sample(1), sample(2)])
        self.release()
        self.contracts.configure('buyer', 0, 'plane1', ['temperature'], min_samples=2, required_evidence=['source'])
        report = self.contracts.certify('buyer', 'release1', {'source': {'document': 'synthetic'}})
        self.assertTrue(report['accepted'])
        self.assertEqual(report['metric_samples'], {'temperature': 2})
        self.assertEqual(report['quarantine_ratio'], '0')
        self.assertEqual(self.contracts.report(report['report_id']), report)
        self.assertEqual(len(report['evidence_digests']['source']), 64)
        self.assertEqual(len(self.contracts.listing()), 1)

    def test_contract_reports_multiple_business_failures(self):
        workorders.upsert(self.db, order(state='open'))
        self.ingest([sample(1), sample(2, '500')])
        self.release()
        self.contracts.configure('buyer', 0, 'plane1', ['temperature', 'pressure'], min_samples=2, required_evidence=['source'])
        report = self.contracts.assess('buyer', 'release1')
        self.assertFalse(report['accepted'])
        codes = {row['code'] for row in report['failures']}
        self.assertEqual(codes, {'missing_evidence', 'open_work_order', 'insufficient_samples', 'quarantine_ratio'})
        self.assertEqual(report['quarantine_ratio'], '0.5')
        self.assertEqual(report['metric_samples']['pressure'], 0)
        self.assertEqual(report['metric_samples']['temperature'], 1)

    def test_contract_revision_and_frozen_report(self):
        self.ingest([sample()])
        self.release()
        self.contracts.configure('buyer', 0, 'plane1', ['temperature'])
        first = self.contracts.certify('buyer', 'release1')
        self.contracts.configure('buyer', 1, 'plane1', ['pressure'])
        second = self.contracts.certify('buyer', 'release1')
        self.assertTrue(first['accepted'])
        self.assertFalse(second['accepted'])
        self.assertNotEqual(first['report_id'], second['report_id'])
        self.assertEqual(self.contracts.report(first['report_id'])['contract_revision'], 1)
        with self.assertRaises(PipelineError):
            self.contracts.configure('buyer', 1, 'plane1', ['temperature'])


class SourceManifestTests(PipelineCase):
    def setUp(self):
        super().setUp()
        self.sources = SourceReconciliation(self.db)

    def descriptor(self, value):
        normalized = sensors.normalize(value)
        return {'event_id': normalized['event_id'], 'observed_at': normalized['observed_at'], 'raw_hash': digest(normalized)}

    def test_exact_manifest_reconciles_and_seals(self):
        rows = [sample(1), sample(2)]
        self.ingest(rows)
        self.sources.register('manifest1', 'feed1', START, END, [self.descriptor(row) for row in rows])
        report = self.sources.reconcile('manifest1')
        self.assertTrue(report['matched'])
        self.assertEqual(report['expected_count'], 2)
        self.assertEqual(report['observed_count'], 2)
        sealed = self.sources.seal('manifest1', 0)
        self.assertEqual(sealed['watermark']['revision'], 1)
        self.assertEqual(self.sources.report(report['reconciliation_id']), report)
        self.assertEqual(len(self.sources.reports('manifest1')), 1)

    def test_manifest_distinguishes_missing_unexpected_and_changed(self):
        self.ingest([sample(1), sample(2)])
        expected = [self.descriptor(sample(1, '21')), self.descriptor(sample(3))]
        self.sources.register('manifest1', 'feed1', START, END, expected)
        report = self.sources.reconcile('manifest1')
        self.assertFalse(report['matched'])
        self.assertEqual(report['missing'], ['event3'])
        self.assertEqual(report['unexpected'], ['event2'])
        self.assertEqual(report['mismatches'][0]['event_id'], 'event1')
        with self.assertRaises(PipelineError):
            self.sources.seal('manifest1', 0)
        self.assertEqual(watermarks.list_watermarks(self.db), [])

    def test_manifest_revision_changes_preserve_previous_report(self):
        self.ingest([sample()])
        self.sources.register('manifest1', 'feed1', START, END, [])
        first = self.sources.reconcile('manifest1')
        self.sources.register('manifest1', 'feed1', START, END, [self.descriptor(sample())], revision=2)
        second = self.sources.reconcile('manifest1')
        self.assertFalse(first['matched'])
        self.assertTrue(second['matched'])
        self.assertEqual(first['manifest_revision'], 1)
        self.assertEqual(second['manifest_revision'], 2)
        self.assertEqual(self.sources.report(first['reconciliation_id']), first)
        self.assertEqual(len(self.sources.reports('manifest1')), 2)
        with self.assertRaises(PipelineError):
            self.sources.register('manifest1', 'feed1', START, END, [], revision=4)
