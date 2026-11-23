import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from stage_diagnostics import diagnose, ingest_timings, summary, pool_diagnostics


class StageDiagnosticsTest(unittest.TestCase):
    def test_coalesced_write_timing_is_not_mislabeled_as_ack_only(self):
        with tempfile.TemporaryDirectory() as folder:
            run = Path(folder)
            (run/'services').mkdir()
            (run/'services/notification.log').write_text('NOTIFICATION_DISPATCH_SQL '+json.dumps({
                'stateWriteMs': 3, 'pendingMs': 1, 'sentCount': 1, 'ackCount': 1, 'fetched': 1, 'coalesced': True})+'\n')
            with patch('stage_diagnostics.rows', return_value=[]):
                stages = diagnose({'checkout': None}, run)['stages']
            self.assertEqual(3, stages['dispatch_stateWriteMs_busy']['p50'])
            self.assertNotIn('dispatch_ackMs_busy', stages)

    def test_pool_counts_are_not_latency_and_missing_logs_are_explicit(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.log') as output:
            self.assertEqual(0, pool_diagnostics(output.name)['samples'])
            for active, waiting in [(8, 20), (7, 0), (8, 5)]:
                output.write('NOTIFICATION_POOL_TIMING '+json.dumps({'active': active, 'waiting': waiting})+'\n')
            output.flush()
            result = pool_diagnostics(output.name)
            self.assertEqual(3, result['samples'])
            self.assertEqual(2, result['samples_with_waiters'])
            self.assertEqual(20, result['maximum_waiters'])

    def timestamp(self, seconds):
        return f'2026-11-17T12:00:{seconds:02d}Z'

    def test_stage_boundaries_and_replay_do_not_replace_original_timing(self):
        batch = {'batchStartedAt': self.timestamp(3), 'commitObservedAt': self.timestamp(5), 'eventIds': ['event']}
        replay = batch | {'batchStartedAt': self.timestamp(10), 'commitObservedAt': self.timestamp(11)}
        with tempfile.TemporaryDirectory() as folder:
            run = Path(folder)
            (run/'services').mkdir()
            (run/'subscribers').mkdir()
            (run/'services/notification.log').write_text(
                'INFO NOTIFICATION_INGEST_TIMING '+json.dumps(batch)+'\n'
                +'INFO NOTIFICATION_INGEST_TIMING '+json.dumps(replay)+'\n')
            (run/'subscribers/alice.jsonl').write_text(json.dumps({
                'eventId': 'event', 'notificationId': 'notice',
                'sentAt': self.timestamp(6), 'receivedAt': self.timestamp(7)})+'\n')
            sales = [{'event_id': 'event', 'completed_at': self.timestamp(0),
                      'publish_started_at': self.timestamp(1), 'broker_acked_at': self.timestamp(2)}]
            acknowledgements = [{'notification_id': 'notice', 'acknowledged_at': self.timestamp(8)}]
            with patch('stage_diagnostics.rows', side_effect=[sales, acknowledgements]):
                report = diagnose({'checkout': None, 'notification': None}, run)
            stages = report['stages']
            self.assertEqual(1000, stages['sale_to_publish_start_per_sale']['p50'])
            self.assertEqual(2000, stages['publish_start_to_consumer_callback_per_sale']['p50'])
            self.assertEqual(2000, stages['consumer_callback_to_commit_observed_per_sale']['p50'])
            self.assertEqual(1000, stages['commit_observed_to_dispatch_per_notification']['p50'])
            self.assertEqual(1000, stages['dispatch_to_client_record_per_notification']['p50'])
            self.assertEqual(1000, stages['client_record_to_ack_update_sample']['p50'])
            self.assertEqual(1, report['coverage']['sales_with_ingest_timing'])

    def test_negative_timing_is_visible_not_silently_clamped(self):
        result = summary([-2, 0, 10])
        self.assertEqual(1, result['negative_count'])
        self.assertEqual(3, result['count'])

    def test_chunk_wait_is_separate_from_transaction_and_replay_sql(self):
        batch = {'callbackReceivedAt': self.timestamp(2), 'batchStartedAt': self.timestamp(4),
                 'commitObservedAt': self.timestamp(5), 'eventIds': ['event']}
        timings = [{'insertedSales': 1, 'lockMs': 2, 'inboxSqlMs': 3, 'fanoutSqlMs': 50},
                   {'insertedSales': 0, 'lockMs': 1, 'inboxSqlMs': 2, 'fanoutSqlMs': 0}]
        with tempfile.TemporaryDirectory() as folder:
            run = Path(folder)
            (run/'services').mkdir()
            (run/'services/notification.log').write_text(
                'NOTIFICATION_INGEST_TIMING '+json.dumps(batch)+'\n'
                + ''.join('NOTIFICATION_DB_TIMING '+json.dumps(timing)+'\n' for timing in timings))
            sales = [{'event_id': 'event', 'completed_at': self.timestamp(0),
                      'publish_started_at': self.timestamp(1), 'broker_acked_at': self.timestamp(2)}]
            with patch('stage_diagnostics.rows', return_value=sales):
                stages = diagnose({'checkout': None}, run)['stages']
            self.assertEqual(1000, stages['publish_start_to_consumer_callback_per_sale']['p50'])
            self.assertEqual(2000, stages['consumer_callback_to_chunk_start_per_sale']['p50'])
            self.assertEqual(1000, stages['chunk_start_to_commit_observed_per_sale']['p50'])
            self.assertEqual(3000, stages['consumer_callback_to_commit_observed_per_sale']['p50'])
            self.assertEqual(50, stages['database_fanoutSqlMs_new_sales_per_chunk']['p50'])
            self.assertEqual(0, stages['database_fanoutSqlMs_deduplicated_per_chunk']['p50'])

    def test_empty_summary_and_logs_are_explicit(self):
        self.assertIsNone(summary([])['p95'])
        with tempfile.NamedTemporaryFile(mode='w', suffix='.log') as output:
            output.write('ordinary log entry\n')
            output.flush()
            self.assertEqual(({}, []), ingest_timings(output.name))


if __name__ == '__main__':
    unittest.main()
