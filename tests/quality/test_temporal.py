from decimal import Decimal
from hangar_ingest.types import PipelineError, instant
from hangar_ingest.decoders import workorders
from hangar_ingest.quality import devices, rules, recalibration, coverage, profiles, alignment, preflight
from hangar_ingest.runs import partitions, windows, maintenance, inspection
from tests.support import PipelineCase, sample, order, START, END, PARTITION


class TemporalTests(PipelineCase):
    def test_unit_and_clock_calibration_before_window_matching(self):
        devices.correct_calibration(self.db, 'cal1', 1, '2', '1', 2000000)
        self.ingest([sample(1, '273.15', unit='K')])
        row = self.db.one('SELECT * FROM observations')
        self.assertEqual(row['value'], '1')
        self.assertEqual(row['unit'], 'C')
        self.assertEqual(row['event_time'], '2026-11-11T10:00:03.000000Z')
        self.assertEqual(row['raw_time'], '2026-11-11T10:00:01.000000Z')
        self.assertEqual(row['calibration_revision'], 2)
        self.assertEqual(row['raw_value'], '273.15')
        self.assertEqual(row['raw_unit'], 'K')

    def test_half_open_work_order_window(self):
        workorders.upsert(self.db, order(opened_at='2026-11-11T10:00:00Z', closed_at='2026-11-11T10:00:02Z'))
        self.ingest([sample(1, time='2026-11-11T10:00:00Z'), sample(2, time='2026-11-11T10:00:02Z')])
        partition = partitions.materialize(self.db, PARTITION)
        observations = partition['payload']['observations']
        self.assertEqual(observations[0]['orders']['primary'], 'order1')
        self.assertIsNone(observations[1]['orders']['primary'])
        window = partition['payload']['windows'][0]['measurements'][0]
        self.assertEqual(window['statistics']['count'], 1)
        self.assertEqual(len(window['observation_ids']), 1)
        self.assertEqual(window['statistics']['mean'], '20')

    def test_overlapping_windows_choose_smallest_then_identifier(self):
        workorders.upsert(self.db, order('wide'))
        workorders.upsert(self.db, order('narrow-b', opened_at='2026-11-11T10:00:00Z', closed_at='2026-11-11T10:01:00Z'))
        workorders.upsert(self.db, order('narrow-a', opened_at='2026-11-11T10:00:00Z', closed_at='2026-11-11T10:01:00Z'))
        self.ingest([sample()])
        row = self.db.one('SELECT * FROM observations')
        matched = windows.match_orders(self.db, row)
        self.assertEqual(matched['primary'], 'narrow-a')
        self.assertEqual(matched['all'], ['narrow-a', 'narrow-b', 'wide'])
        self.assertTrue(matched['ambiguous'])
        self.assertEqual(len(preflight.order_conflicts(self.db, 'plane1')), 3)

    def test_late_observation_dirties_all_days_of_spanning_order(self):
        workorders.upsert(self.db, order(opened_at='2026-11-11T23:00:00Z', closed_at='2026-11-12T01:00:00Z'))
        self.ingest([sample(1, time='2026-11-11T23:30:00Z')])
        first = partitions.rebuild_dirty(self.db)
        self.assertEqual(len(first), 2)
        self.assertEqual(partitions.listing(self.db, 'dirty'), [])
        self.ingest([sample(2, time='2026-11-12T00:30:00Z')], 'late')
        dirty = partitions.listing(self.db, 'dirty')
        self.assertEqual([row['partition_key'] for row in dirty], [PARTITION, 'plane1/2026-11-12'])
        rebuilt = partitions.rebuild_dirty(self.db)
        for version in rebuilt:
            self.assertEqual(version['revision'], 2)
            self.assertEqual(version['payload']['windows'][0]['measurements'][0]['statistics']['count'], 2)
        old = partitions.version(self.db, PARTITION, 1)
        self.assertEqual(old['payload']['windows'][0]['measurements'][0]['statistics']['count'], 1)

    def test_recalibration_moves_samples_across_day_without_rewriting_old_version(self):
        self.ingest([sample(1, '20', time='2026-11-11T23:59:59Z')])
        old = partitions.materialize(self.db, PARTITION)
        devices.correct_calibration(self.db, 'cal1', 1, '2', '0', 2000000)
        self.assertEqual(len(recalibration.stale_observations(self.db)), 1)
        proposed = recalibration.plan(self.db, 'cal1')
        self.assertTrue(proposed['applicable'])
        self.assertEqual(proposed['changes'][0]['new_value'], '40')
        self.assertEqual(proposed['changes'][0]['new_time'], '2026-11-12T00:00:01.000000Z')
        recalibration.apply(self.db, 'cal1', 2)
        self.assertEqual(recalibration.stale_observations(self.db), [])
        partitions.rebuild_dirty(self.db)
        self.assertEqual(partitions.version(self.db, PARTITION)['payload']['observations'], [])
        moved = partitions.version(self.db, 'plane1/2026-11-12')['payload']['observations']
        self.assertEqual(moved[0]['value'], '40')
        self.assertEqual(partitions.version(self.db, PARTITION, 1)['digest'], old['digest'])
        self.assertTrue(inspection.check(self.db)['ok'])

    def test_recalibration_rejects_quality_failure_atomically(self):
        self.ingest([sample(1, '150')])
        devices.correct_calibration(self.db, 'cal1', 1, '2', '0', 0)
        proposed = recalibration.plan(self.db, 'cal1')
        self.assertFalse(proposed['applicable'])
        with self.assertRaises(PipelineError) as error:
            recalibration.apply(self.db, 'cal1', 2)
        self.assertEqual(error.exception.code, 'recalibration_quality')
        self.assertEqual(self.db.scalar('SELECT value FROM observations'), '150')
        self.assertEqual(self.db.scalar('SELECT calibration_revision FROM observations'), 1)
        self.assertEqual(len(recalibration.stale_observations(self.db)), 1)

    def test_late_sample_checks_both_neighbors(self):
        rules.configure(self.db, 'dev1', revision=1, minimum='-100', maximum='200', max_delta='10')
        self.ingest([sample(1, '0', time='2026-11-11T10:00:00Z'), sample(2, '10', time='2026-11-11T10:00:10Z')])
        result = self.ingest([sample(3, '-5', time='2026-11-11T10:00:05Z')], 'late')
        self.assertEqual(result['batch']['rejected'], 1)
        from hangar_ingest.quality.review import listing
        error = listing(self.db)[0]['reasons']
        self.assertEqual(error['context']['errors'][0]['neighbor'], 'following')
        self.assertEqual(error['context']['errors'][0]['delta'], '15')
        self.assertEqual(self.db.scalar('SELECT COUNT(*) FROM observations'), 2)
        self.assertTrue(inspection.check(self.db)['ok'])

    def test_sampling_gap_warning_can_be_recomputed_after_late_arrival(self):
        rules.configure(self.db, 'dev1', revision=1, max_gap_seconds=10)
        self.ingest([sample(1, time='2026-11-11T10:00:00Z'), sample(2, time='2026-11-11T10:00:20Z')])
        assessment = maintenance.assess_device(self.db, 'dev1')
        self.assertGreater(assessment['warnings'], 0)
        self.ingest([sample(3, time='2026-11-11T10:00:10Z')], 'late')
        refreshed = maintenance.refresh_warnings(self.db, 'dev1', 2)
        self.assertGreaterEqual(len(refreshed['changed']), 1)
        self.assertEqual(refreshed['assessment']['warnings'], 0)
        self.assertEqual(self.db.scalar("SELECT COUNT(*) FROM observations WHERE quality!='[]'"), 0)
        self.assertEqual(partitions.get(self.db, PARTITION)['state'], 'dirty')

    def test_rebuild_plan_detects_revision_changes(self):
        self.ingest([sample()])
        plan = maintenance.rebuild_plan(self.db)
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0]['observations'], 1)
        rebuilt = maintenance.rebuild_selected(self.db, plan)
        self.assertEqual(rebuilt[0]['revision'], 1)
        with self.assertRaises(PipelineError):
            maintenance.rebuild_selected(self.db, plan)
        explanation = maintenance.explain_partition(self.db, PARTITION)
        self.assertEqual(len(explanation['versions']), 1)
        self.assertEqual(explanation['origins'][0]['batch_id'], 'batch1')


class StatisticalTests(PipelineCase):
    def test_window_statistics_and_empty_buckets(self):
        self.ingest([sample(1, '10', time='2026-11-11T10:00:00Z'), sample(2, '20', time='2026-11-11T10:00:02Z')])
        buckets = windows.resample(self.db, 'dev1', '2026-11-11T10:00:00Z', '2026-11-11T10:00:03Z', 1)
        self.assertEqual(len(buckets), 3)
        self.assertEqual([row['statistics']['count'] for row in buckets], [1, 0, 1])
        self.assertIsNone(buckets[1]['statistics']['mean'])
        values = windows.select(self.db, 'dev1', START, END)
        summary = windows.statistics(values)
        self.assertEqual(summary['mean'], '15')
        self.assertEqual(summary['median'], '15')
        self.assertEqual(summary['max_gap_us'], 2000000)
        self.assertEqual(summary['duration_us'], 2000000)

    def test_coverage_union_does_not_double_count_overlap(self):
        self.ingest([sample(1, time='2026-11-11T10:00:02Z'), sample(2, time='2026-11-11T10:00:04Z')])
        result = coverage.measure(self.db, 'dev1', '2026-11-11T10:00:00Z', '2026-11-11T10:00:10Z', tolerance_seconds=4)
        self.assertEqual(result['covered_us'], 6000000)
        self.assertEqual(result['coverage_ratio'], '0.6')
        self.assertEqual(result['missing'][0]['duration_us'], 4000000)
        self.assertEqual(coverage.merge_intervals([[1, 3], [2, 4], [4, 5]]), [[1, 5]])
        self.assertEqual(coverage.complement([[2, 4]], 0, 6), [[0, 2], [4, 6]])
        self.assertTrue(coverage.calibration_gaps(self.db, 'dev1', START, END)['complete'])

    def test_percentiles_histogram_and_population_variance(self):
        self.ingest([sample(1, '10'), sample(2, '20'), sample(3, '30')])
        profile = profiles.profile(self.db, 'dev1', START, END)
        self.assertEqual(profile['statistics']['mean'], '20')
        self.assertEqual(profile['median_absolute_deviation'], '10')
        self.assertEqual(profile['distinct_values'], 3)
        self.assertEqual(profile['p05'], '11')
        self.assertEqual(profile['p95'], '29')
        self.assertAlmostEqual(Decimal(profile['variance_population']), Decimal(200) / 3)
        histogram = profiles.histogram(['-1', '0', '9', '10'], ['0', '10'])
        self.assertEqual([row['count'] for row in histogram], [1, 2, 1])
        with self.assertRaises(PipelineError):
            profiles.histogram(['1'], ['2', '1'])

    def test_empty_and_constant_profiles_do_not_divide_by_zero(self):
        empty = profiles.profile(self.db, 'dev1', START, END)
        self.assertIsNone(empty['variance_population'])
        self.assertIsNone(empty['p95'])
        self.assertEqual(empty['distinct_values'], 0)
        self.ingest([sample(1, '7'), sample(2, '7')])
        constant = profiles.profile(self.db, 'dev1', START, END)
        self.assertEqual(constant['standard_deviation'], '0')
        self.assertEqual(constant['distinct_values'], 1)
        self.assertFalse(profiles.outliers(self.db, 'dev1', START, END)['assessed'])
        self.assertIsNone(profiles.percentile([], '.5'))

    def test_drift_and_iqr_are_observable_quality_outputs(self):
        self.ingest([sample(1, '0'), sample(2, '0'), sample(3, '0'), sample(4, '0'), sample(5, '100')])
        outliers = profiles.outliers(self.db, 'dev1', START, END, multiplier='1.5')
        self.assertTrue(outliers['assessed'])
        self.assertEqual(len(outliers['outliers']), 1)
        self.assertEqual(outliers['outliers'][0]['value'], '100')
        drift = profiles.compare_periods(self.db, 'dev1', '2026-11-11T10:00:00Z', '2026-11-11T10:00:04Z', '2026-11-11T10:00:04Z', '2026-11-11T10:00:06Z')
        self.assertTrue(drift['comparable'])
        self.assertTrue(drift['drift'])
        self.assertIsNone(drift['relative_delta'])
        self.assertEqual(drift['mean_delta'], '50')

    def test_alignment_tie_break_and_nonreuse(self):
        devices.register(self.db, 'dev2', 'plane1', 'engine1', 'pressure', 'kPa')
        devices.add_alias(self.db, 'feed1', 'sensor2', 'dev2', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z')
        devices.add_calibration(self.db, 'cal2', 'dev2', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z')
        self.ingest([sample(1, '1', time='2026-11-11T10:00:02Z'), sample(2, '2', time='2026-11-11T10:00:04Z'), sample(3, '10', time='2026-11-11T10:00:01Z', device='sensor2', unit='kPa'), sample(4, '20', time='2026-11-11T10:00:03Z', device='sensor2', unit='kPa')])
        result = alignment.align(self.db, 'dev1', 'dev2', START, END, tolerance_seconds=1)
        self.assertEqual(len(result['pairs']), 2)
        self.assertEqual(result['pairs'][0]['right_time'], '2026-11-11T10:00:01.000000Z')
        self.assertEqual(result['unmatched_right'], [])
        self.assertEqual(result['unmatched_left'], [])
        self.assertEqual(alignment.correlation(result)['coefficient'], '1')
        scan = alignment.lag_scan(self.db, 'dev1', 'dev2', START, END, [-1, 0, 1], tolerance_seconds=0)
        self.assertEqual(scan['best']['offset_seconds'], -1)
        self.assertEqual(scan['best']['pairs'], 2)
