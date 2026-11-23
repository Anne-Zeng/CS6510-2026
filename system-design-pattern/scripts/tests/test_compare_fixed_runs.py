from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import json
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compare_fixed_runs import compare, read_run


class CompareFixedRunsTest(unittest.TestCase):
    def test_refuses_performance_invalid_reports_even_if_correctness_passed(self):
        for reviewed in (False, True):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for filename in ('fixed-workload.json', 'throughput.json', 'environment.json'):
                    (root/filename).write_text('{}')
                (root/'verification.json').write_text(json.dumps({'passed': True, 'performance_valid': reviewed}))
                if reviewed:
                    (root/'performance-validity.json').write_text(json.dumps({'performance_valid': False}))
                with self.assertRaisesRegex(ValueError, 'Invalid benchmark clock'):
                    read_run(root)

    def record(self, tps=100, p95=200):
        return {'run': 'example', 'plan': {'plan_sha256': 'same'}, 'notifications': 300250,
                'environment': {'same': True}, 'tps': tps, 'p50_ms': 100, 'p95_ms': p95,
                'elapsed_seconds': 20}

    def test_requires_latency_and_throughput_improvement(self):
        with patch('compare_fixed_runs.read_run', side_effect=[self.record(), self.record(tps=90, p95=50)]):
            result = compare(['before'], ['after'])
            self.assertFalse(result['passed'])
            self.assertFalse(result['checks']['median_tps_not_lower'])
        with patch('compare_fixed_runs.read_run', side_effect=[self.record(), self.record(tps=110, p95=50)]):
            self.assertTrue(compare(['before'], ['after'])['passed'])

    def test_refuses_different_notification_counts(self):
        with patch('compare_fixed_runs.read_run', side_effect=[self.record(), self.record() | {'notifications': 100}]):
            with self.assertRaisesRegex(ValueError, 'Non-comparable'):
                compare(['before'], ['after'])

    def test_refuses_different_wal_compression(self):
        before = self.record() | {'environment': {'walCompressionPerInstance': {'notification': 'off'}}}
        after = self.record() | {'environment': {'walCompressionPerInstance': {'notification': 'lz4'}}}
        with patch('compare_fixed_runs.read_run', side_effect=[before, after]):
            with self.assertRaisesRegex(ValueError, 'Non-comparable'):
                compare(['before'], ['after'])

    def test_refuses_mixed_power_sources(self):
        before = self.record() | {'environment': {'powerConditions': [{'source': 'AC Power', 'low_power_mode': 0}]}}
        after = self.record() | {'environment': {'powerConditions': [{'source': 'Battery Power', 'low_power_mode': 0}]}}
        with patch('compare_fixed_runs.read_run', side_effect=[before, after]):
            with self.assertRaisesRegex(ValueError, 'Non-comparable'):
                compare(['before'], ['after'])
