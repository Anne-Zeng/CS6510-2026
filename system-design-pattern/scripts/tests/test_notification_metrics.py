import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notification_metrics import audit_metrics, counter_query_plan, storage_snapshot, delivery_io_snapshot


class NotificationMetricsTest(unittest.TestCase):
    def test_delivery_io_records_hot_updates_and_actual_index_definitions(self):
        database = Mock()
        database.sql.return_value = '{}'
        self.assertEqual({}, delivery_io_snapshot(database))
        query = database.sql.call_args.args[0]
        for expected in ('n_tup_hot_upd', 'idx_blks_read', 'indexdef', 'reloptions', 'wal_bytes'):
            self.assertIn(expected, query)
        self.assertNotIn('EXPLAIN ANALYZE', query)

    def test_storage_sample_is_bounded_and_compares_equivalent_payload(self):
        database = Mock()
        database.sql.return_value = '{}'
        self.assertEqual({}, storage_snapshot(database))
        query = database.sql.call_args.args[0]
        self.assertIn('LIMIT 1024', query)
        self.assertIn('JOIN notification_sale_items', query)
        self.assertIn('legacy_equivalent_row_bytes', query)

    def test_independent_audit_compares_base_rows_in_one_snapshot(self):
        database = Mock()
        database.sql.return_value = json.dumps({'passed': False, 'actual': {'pending': 1}, 'counters': {'pending': 0}})
        self.assertFalse(audit_metrics(database)['passed'])
        query = database.sql.call_args.args[0]
        self.assertIn('FROM notification_deliveries', query)
        self.assertIn('FROM notification_metric_totals', query)
        self.assertEqual(1, database.sql.call_count)

    def test_runtime_plan_does_not_scan_delivery_history(self):
        database = Mock()
        database.sql.return_value = '[]'
        self.assertEqual([], counter_query_plan(database))
        query = database.sql.call_args.args[0]
        self.assertNotIn('notification_deliveries', query)
        self.assertIn('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON)', query)
