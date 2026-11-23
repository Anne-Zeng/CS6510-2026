from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark_clock import BenchmarkClock, read_power_conditions


class BenchmarkClockTest(unittest.TestCase):
    def setUp(self):
        self.power = patch('benchmark_clock.read_power_conditions', return_value={'source': 'AC Power', 'low_power_mode': 0}).start()
        self.addCleanup(patch.stopall)
    @patch('benchmark_clock.sys.platform', 'linux')
    def test_clock_jump_invalidates_performance(self):
        guard = BenchmarkClock(Mock(), monitor_power=False)
        with patch('benchmark_clock.time.time', side_effect=[100, 1100]), patch('benchmark_clock.time.monotonic', side_effect=[50, 150]):
            guard.start()
            result = guard.snapshot()
        self.assertEqual(900, result['clock_divergence_seconds'])
        self.assertFalse(result['valid'])

    @patch('benchmark_clock.sys.platform', 'darwin')
    @patch('benchmark_clock.subprocess.Popen')
    def test_guard_is_bounded_to_harness_and_cleaned_up(self, popen):
        process = popen.return_value
        process.poll.return_value = None
        guard = BenchmarkClock(MagicMock(), monitor_power=False)
        guard.start()
        self.assertIn('-w', popen.call_args.args[0])
        self.assertTrue(guard.snapshot()['valid'])
        guard.close()
        process.terminate.assert_called_once()
        process.wait.assert_called_once_with(timeout=5)

    @patch('benchmark_clock.sys.platform', 'darwin')
    @patch('benchmark_clock.subprocess.Popen')
    def test_exited_guard_invalidates_performance(self, popen):
        popen.return_value.poll.return_value = 1
        guard = BenchmarkClock(Mock(), monitor_power=False)
        guard.start()
        self.assertFalse(guard.snapshot()['valid'])


    @patch('benchmark_clock.sys.platform', 'darwin')
    @patch('benchmark_clock.subprocess.Popen')
    def test_power_transition_is_remembered_even_after_switching_back(self, popen):
        popen.return_value.poll.return_value = None
        guard = BenchmarkClock(Mock(), monitor_power=False)
        guard.start()
        self.power.return_value = {'source': 'Battery Power', 'low_power_mode': 0}
        guard.sample_power()
        self.power.return_value = {'source': 'AC Power', 'low_power_mode': 0}
        self.assertFalse(guard.snapshot()['valid'])

    @patch('benchmark_clock.sys.platform', 'darwin')
    @patch('benchmark_clock.subprocess.Popen')
    def test_unknown_power_conditions_do_not_pass(self, popen):
        popen.return_value.poll.return_value = None
        self.power.return_value = {'source': 'unknown', 'low_power_mode': None}
        guard = BenchmarkClock(Mock(), monitor_power=False)
        guard.start()
        self.assertFalse(guard.snapshot()['valid'])


class PowerConditionsTest(unittest.TestCase):
    @patch('benchmark_clock.sys.platform', 'darwin')
    @patch('benchmark_clock.subprocess.check_output')
    def test_selects_mode_for_actual_power_source(self, output):
        output.side_effect = ["Now drawing from 'AC Power'\n", 'Battery Power:\n lowpowermode 1\nAC Power:\n lowpowermode 0\n']
        self.assertEqual({'source': 'AC Power', 'low_power_mode': 0}, read_power_conditions())

    @patch('benchmark_clock.sys.platform', 'darwin')
    @patch('benchmark_clock.subprocess.check_output', side_effect=OSError('unavailable'))
    def test_missing_power_probe_is_explicitly_unknown(self, output):
        self.assertEqual({'source': 'unknown', 'low_power_mode': None}, read_power_conditions())
