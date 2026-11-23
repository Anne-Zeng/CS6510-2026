"""Exercise disk exhaustion handling without starting Kafka or PostgreSQL."""
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location('benchmark_isolated', SCRIPTS / 'benchmark-isolated.py')
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class BenchmarkSafetyTest(unittest.TestCase):
    def test_wal_override_cannot_alter_the_existing_postgresql_server(self):
        with patch.object(benchmark.subprocess, 'run') as invoked, \
                patch.object(sys, 'argv', ['benchmark-isolated.py', '--wal-sync-method', 'fsync']):
            with self.assertRaisesRegex(RuntimeError, 'requires temporary separate'):
                benchmark.main()
            invoked.assert_not_called()

    def test_low_disk_refuses_work_before_starting_any_process(self):
        usage = benchmark.shutil.disk_usage(benchmark.ROOT)._replace(free=1024**3)
        with patch.object(benchmark.shutil, 'disk_usage', return_value=usage), \
                patch.object(benchmark.subprocess, 'Popen') as started, \
                patch.object(sys, 'argv', ['benchmark-isolated.py', '--modes', 'default']):
            with self.assertRaisesRegex(RuntimeError, 'at least 4 GiB'):
                benchmark.main()
            started.assert_not_called()

    def test_disk_exhaustion_terminates_the_running_client(self):
        processes = []
        real_start = subprocess.Popen

        def start(*args, **kwargs):
            child = real_start(*args, **kwargs)
            processes.append(child)
            return child

        # One healthy check allows the real child to start; the next check
        # simulates another application filling the disk during a workload.
        with tempfile.TemporaryFile() as log, \
                patch.object(benchmark.subprocess, 'Popen', side_effect=start), \
                patch.object(benchmark, 'require_disk_space',
                             side_effect=[None, RuntimeError('disk exhausted')]):
            with self.assertRaisesRegex(RuntimeError, 'disk exhausted'):
                benchmark.run_client([sys.executable, '-c', 'import time; time.sleep(30)'], log)
        self.assertEqual(1, len(processes))
        self.assertIsNotNone(processes[0].poll(), 'The interrupted client must not remain running')

    def test_nonzero_client_exit_cannot_produce_a_successful_run(self):
        with tempfile.TemporaryFile() as log, \
                patch.object(benchmark, 'require_disk_space'):
            with self.assertRaisesRegex(RuntimeError, 'exit code 7'):
                benchmark.run_client([sys.executable, '-c', 'raise SystemExit(7)'], log)


if __name__ == '__main__':
    unittest.main()
