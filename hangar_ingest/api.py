from hangar_ingest.types import fail, object_fields
from hangar_ingest.decoders import workorders
from hangar_ingest.quality import devices, rules, review, recalibration, coverage, profiles, units, alignment, preflight
from hangar_ingest.runs import batches, claims, inspection, journal, partitions, processor, windows, maintenance
from hangar_ingest.runs.orchestrator import Pipeline
from hangar_ingest.exports import releases, tabular, watermarks, transfer
from hangar_ingest.exports.contracts import DeliveryContracts


class Application:
    def __init__(self, db):
        self.db = db
        self.pipeline = Pipeline(db)
        self.delivery = DeliveryContracts(db)
        self.receiver = transfer.Receiver(db)
        from hangar_ingest.runs.manifests import SourceReconciliation
        self.sources = SourceReconciliation(db)
        self.operations = {
            'preflight.source': (preflight.source_configuration, {'source', 'start', 'end'}, set()),
            'preflight.inventory': (preflight.device_inventory, set(), set()),
            'preflight.orders': (preflight.order_conflicts, {'aircraft'}, set()),
            'preflight.delivery': (preflight.delivery_readiness, {'partition_keys'}, set()),
            'preflight.orphans': (preflight.orphan_configuration, set(), set()),
            'alignment.pairs': (alignment.align, {'left_device', 'right_device', 'start', 'end'}, {'tolerance_seconds', 'reuse_right'}),
            'alignment.lag': (alignment.lag_scan, {'left_device', 'right_device', 'start', 'end', 'offsets_seconds'}, {'tolerance_seconds'}),
            'maintenance.assess': (maintenance.assess_device, {'device_id'}, set()),
            'maintenance.refresh': (maintenance.refresh_warnings, {'device_id', 'expected_rule_revision'}, set()),
            'maintenance.plan': (maintenance.rebuild_plan, set(), {'aircraft'}),
            'maintenance.rebuild': (maintenance.rebuild_selected, {'plan'}, set()),
            'maintenance.explain': (maintenance.explain_partition, {'partition_key'}, set()),
            'maintenance.impact': (maintenance.calibration_impact, {'calibration_id'}, set()),
            'transfer.create': (transfer.make_chunks, {'release_id'}, {'chunk_bytes'}),
            'device.register': (devices.register, {'device_id', 'aircraft', 'component', 'metric', 'canonical_unit'}, set()),
            'device.get': (devices.device, {'device_id'}, set()),
            'device.active': (devices.set_active, {'device_id', 'revision', 'active'}, set()),
            'device.list': (devices.list_devices, set(), {'aircraft'}),
            'alias.add': (devices.add_alias, {'source', 'external_id', 'device_id', 'valid_from', 'valid_until'}, set()),
            'alias.resolve': (devices.resolve_alias, {'source', 'external_id', 'observed_at'}, set()),
            'calibration.add': (devices.add_calibration, {'calibration_id', 'device_id', 'valid_from', 'valid_until'}, {'scale', 'offset', 'clock_offset_us'}),
            'calibration.correct': (devices.correct_calibration, {'calibration_id', 'revision', 'scale', 'offset', 'clock_offset_us'}, set()),
            'calibration.plan': (recalibration.plan, {'calibration_id'}, set()),
            'calibration.apply': (recalibration.apply, {'calibration_id', 'expected_revision'}, set()),
            'calibration.stale': (recalibration.stale_observations, set(), set()),
            'quality.configure': (rules.configure, {'device_id'}, {'revision', 'minimum', 'maximum', 'max_delta', 'max_gap_seconds', 'duplicate_policy', 'required_calibration'}),
            'quality.get': (rules.get, {'device_id'}, set()),
            'order.upsert': (workorders.upsert, {'value'}, set()),
            'order.get': (workorders.get, {'order_id'}, {'revision'}),
            'order.import': (workorders.import_orders, {'values'}, set()),
            'order.history': (workorders.history, {'order_id'}, set()),
            'order.overlap': (workorders.overlap, {'aircraft', 'component', 'start', 'end'}, set()),
            'batch.get': (batches.get, {'batch_id'}, set()),
            'batch.rows': (batches.rows, {'batch_id'}, {'after', 'limit'}),
            'batch.list': (batches.listing, set(), {'state'}),
            'batch.pause': (batches.pause, {'batch_id', 'revision'}, set()),
            'batch.resume': (batches.resume, {'batch_id', 'revision'}, set()),
            'batch.cancel': (batches.cancel, {'batch_id', 'revision', 'reason'}, set()),
            'batch.process': (processor.process, {'batch_id'}, {'limit', 'expected_revision', 'claim'}),
            'batch.drain': (processor.drain, {'batch_id'}, {'chunk'}),
            'batch.verify': (processor.verify_checkpoint, {'batch_id'}, set()),
            'claim.acquire': (claims.acquire, {'batch_id', 'owner'}, {'ttl'}),
            'claim.renew': (claims.renew, {'batch_id', 'claim'}, {'ttl'}),
            'claim.release': (claims.release, {'batch_id', 'claim'}, set()),
            'quarantine.get': (review.get, {'quarantine_id'}, set()),
            'quarantine.list': (review.listing, set(), {'status', 'batch_id', 'limit'}),
            'quarantine.replay': (review.replay, {'quarantine_id', 'revision', 'corrected', 'reason'}, set()),
            'quarantine.dismiss': (review.dismiss, {'quarantine_id', 'revision', 'reason'}, set()),
            'quarantine.reasons': (review.reason_counts, set(), set()),
            'partition.get': (partitions.get, {'partition_key'}, set()),
            'partition.list': (partitions.listing, set(), {'state'}),
            'partition.materialize': (partitions.materialize, {'partition_key'}, {'expected_revision'}),
            'partition.version': (partitions.version, {'partition_key'}, {'revision'}),
            'partition.rebuild': (partitions.rebuild_dirty, set(), {'limit'}),
            'partition.diff': (partitions.difference, {'partition_key', 'before', 'after'}, set()),
            'window.select': (windows.select, {'device_id', 'start', 'end'}, set()),
            'window.order': (windows.order_window, {'order'}, {'padding_seconds'}),
            'window.resample': (windows.resample, {'device_id', 'start', 'end', 'interval_seconds'}, set()),
            'coverage.measure': (coverage.measure, {'device_id', 'start', 'end'}, {'tolerance_seconds'}),
            'coverage.calibration': (coverage.calibration_gaps, {'device_id', 'start', 'end'}, set()),
            'coverage.order': (coverage.order_readiness, {'order_id'}, {'minimum_ratio'}),
            'coverage.duplicates': (coverage.duplicate_timestamps, {'device_id'}, set()),
            'profile.measure': (profiles.profile, {'device_id', 'start', 'end'}, set()),
            'profile.compare': (profiles.compare_periods, {'device_id', 'baseline_start', 'baseline_end', 'current_start', 'current_end'}, {'threshold'}),
            'profile.outliers': (profiles.outliers, {'device_id', 'start', 'end'}, {'multiplier'}),
            'watermark.advance': (watermarks.advance, {'source', 'watermark'}, {'revision'}),
            'watermark.list': (watermarks.list_watermarks, set(), set()),
            'watermark.readiness': (watermarks.readiness, {'partition_key'}, set()),
            'release.prepare': (releases.prepare, {'release_id', 'partition_keys'}, {'require_watermark'}),
            'release.commit': (releases.commit, {'release_id'}, set()),
            'release.abort': (releases.abort, {'release_id', 'reason'}, set()),
            'release.get': (releases.get, {'release_id'}, set()),
            'release.list': (releases.list_releases, set(), set()),
            'release.package': (releases.payloads, {'release_id'}, set()),
            'release.windows': (tabular.window_rows, {'release_id'}, set()),
            'release.diff': (tabular.compare_releases, {'before', 'after'}, set()),
            'inspect.overview': (inspection.overview, set(), set()),
            'inspect.check': (inspection.check, set(), set()),
            'inspect.observation': (inspection.observation_trace, {'observation_id'}, set()),
            'inspect.batch': (inspection.batch_report, {'batch_id'}, set()),
            'inspect.lineage': (inspection.lineage, {'release_id'}, set()),
            'journal.verify': (journal.verify, set(), set()),
            'journal.page': (journal.page, set(), {'after', 'limit'}),
        }
        self.bound = {
            'source.register': (self.sources.register, {'manifest_id', 'source', 'start', 'end', 'events'}, {'revision'}),
            'source.get': (self.sources.get, {'manifest_id'}, set()),
            'source.reconcile': (self.sources.reconcile, {'manifest_id'}, set()),
            'source.report': (self.sources.report, {'reconciliation_id'}, set()),
            'source.reports': (self.sources.reports, {'manifest_id'}, set()),
            'source.seal': (self.sources.seal, {'manifest_id', 'watermark_revision'}, set()),
            'transfer.begin': (self.receiver.begin, {'transfer_id', 'manifest'}, set()),
            'transfer.status': (self.receiver.status, {'transfer_id'}, set()),
            'transfer.accept': (self.receiver.accept, {'transfer_id', 'chunk_index', 'content'}, set()),
            'transfer.finish': (self.receiver.finish, {'transfer_id'}, set()),
            'transfer.package': (self.receiver.package, {'transfer_id'}, set()),
            'pipeline.recover': (self.pipeline.recover, set(), {'owner', 'chunk', 'limit'}),
            'pipeline.publish': (self.pipeline.publish, {'release_id', 'partition_keys'}, {'require_watermark'}),
            'pipeline.seal': (self.pipeline.finish_source_day, {'source', 'day_end', 'watermark_revision', 'release_id', 'partition_keys'}, set()),
            'delivery.configure': (self.delivery.configure, {'contract_id', 'revision', 'aircraft', 'metrics'}, {'min_samples', 'max_quarantine_ratio', 'require_closed_orders', 'required_evidence'}),
            'delivery.get': (self.delivery.get, {'contract_id'}, set()),
            'delivery.assess': (self.delivery.assess, {'contract_id', 'release_id'}, {'evidence'}),
            'delivery.certify': (self.delivery.certify, {'contract_id', 'release_id'}, {'evidence'}),
            'delivery.report': (self.delivery.report, {'report_id'}, set()),
            'delivery.list': (self.delivery.listing, set(), set()),
        }

    def call(self, operation, parameters=None):
        parameters = {} if parameters is None else parameters
        if operation in self.operations:
            function, required, optional = self.operations[operation]
            object_fields(parameters, required, optional)
            return function(self.db, **parameters)
        if operation in self.bound:
            function, required, optional = self.bound[operation]
            object_fields(parameters, required, optional)
            return function(**parameters)
        if operation == 'batch.stage':
            object_fields(parameters, {'batch_id', 'source', 'content'}, {'format'})
            content = parameters['content']
            if not isinstance(content, str):
                fail('content_type', 'Batch content must be text')
            return batches.stage(self.db, parameters['batch_id'], parameters['source'], content.encode(), parameters.get('format', 'ndjson'))
        if operation == 'package.verify':
            object_fields(parameters, {'value'})
            return releases.verify_package(parameters['value'])
        if operation == 'units.list':
            object_fields(parameters, set(), {'dimension'})
            return units.supported(**parameters)
        fail('unknown_operation', 'Unknown pipeline operation', operation=operation)

    def commands(self):
        return sorted(set(self.operations) | set(self.bound) | {'batch.stage', 'package.verify', 'units.list'})

    def batch(self, commands):
        if not isinstance(commands, list) or len(commands) > 1000:
            fail('commands_size', 'Command batch must contain at most one thousand operations')
        results = []
        with self.db.transaction():
            for request in commands:
                object_fields(request, {'operation'}, {'parameters'})
                results.append(self.call(request['operation'], request.get('parameters')))
        return results
