import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fixed_workload import plan_sales, run_fixed


class FixedWorkloadTest(unittest.TestCase):
    def test_plan_is_stable_and_exact(self):
        skus = [f'SKU-{index:06}' for index in range(1, 2001)]
        plan = plan_sales(600, 10, skus)
        self.assertEqual(plan, plan_sales(600, 10, list(reversed(skus))))
        self.assertEqual(600, len(plan))
        self.assertTrue(all(len(set(basket)) == 10 for basket in plan))
        self.assertEqual(3, sum(skus[0] in basket for basket in plan))
        for count, items in [(0, 10), (1, 0), (1, 2001)]:
            with self.assertRaises(ValueError):
                plan_sales(count, items, skus)

    def test_fixed_driver_finishes_exact_sales_and_records_plan(self):
        calls = []
        lock = threading.Lock()

        def post(base, path, body, expected):
            with lock:
                calls.append((path, body))
                return {'transactionId': str(len(calls))}

        with tempfile.TemporaryDirectory() as folder:
            report = run_fixed('unused', 6, 2, 2, ['A', 'B', 'C'], Path(folder), post)
            self.assertEqual(6, report['totalTransactions'])
            self.assertEqual([6, 12, 6], [item['successCount'] for item in report['operations']])
            self.assertEqual(24, len(calls))
            self.assertEqual(6, len(json.loads((Path(folder)/'fixed-workload.json').read_text())['baskets']))

    def test_http_failure_cannot_be_reported_as_success(self):
        def fail(*args):
            raise RuntimeError('HTTP error')
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(RuntimeError, 'HTTP error'):
                run_fixed('unused', 1, 1, 1, ['A'], Path(folder), fail)
