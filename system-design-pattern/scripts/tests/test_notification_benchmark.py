from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notification_benchmark import prepare_inventory, assess_workload


class NotificationBenchmarkTest(unittest.TestCase):
    def report(self, errors=0, completed=10):
        return {'totalTransactions': completed, 'operations': [
            {'operation': 'START_TRANSACTION', 'successCount': 10, 'errorCount': 0},
            {'operation': 'SCAN_ITEM', 'successCount': 100, 'errorCount': 0},
            {'operation': 'COMPLETE_TRANSACTION', 'successCount': completed, 'errorCount': errors}]}

    def test_zero_errors_with_sales_passes(self):
        self.assertTrue(assess_workload(self.report())['passed'])

    def test_inventory_rejections_fail_load_even_if_notifications_pass(self):
        result = assess_workload(self.report(errors=7174, completed=1225))
        self.assertFalse(result['passed'])
        self.assertEqual(7174, result['total_operation_errors'])

    def test_missing_operations_or_zero_sales_do_not_pass(self):
        self.assertFalse(assess_workload({})['passed'])
        self.assertFalse(assess_workload(self.report(completed=0))['passed'])
        report = self.report()
        report['operations'].pop()
        self.assertFalse(assess_workload(report)['passed'])

    def test_negative_counts_or_inconsistent_totals_fail(self):
        self.assertFalse(assess_workload(self.report(errors=-1))['passed'])
        report = self.report()
        report['totalTransactions'] = 11
        self.assertFalse(assess_workload(report)['passed'])

    def test_prepares_only_supplied_test_database(self):
        database = Mock()
        database.sql.return_value = '20'
        result = prepare_inventory(database, 1_000_000)
        self.assertEqual(1_000_000, result['opening_stock_per_sku'])
        self.assertEqual(20, result['sku_count'])
        self.assertEqual('UPDATE inventory SET current_stock=1000000', database.sql.call_args_list[0].args[0])

    def test_invalid_inventory_never_executes_sql(self):
        database = Mock()
        for value in (0, -1, 2_000_000_001, '1000000', True):
            with self.assertRaises(ValueError):
                prepare_inventory(database, value)
        database.sql.assert_not_called()


if __name__ == '__main__':
    unittest.main()
