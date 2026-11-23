import json
import os
import re
import subprocess
import sys
import threading
import time


def read_power_conditions():
    if sys.platform != 'darwin':
        return {'source': 'not-applicable', 'low_power_mode': None}
    try:
        battery = subprocess.check_output(['/usr/bin/pmset', '-g', 'batt'], text=True, timeout=5)
        settings = subprocess.check_output(['/usr/bin/pmset', '-g', 'custom'], text=True, timeout=5)
        match = re.search(r"Now drawing from '([^']+)'", battery)
        source = match.group(1) if match else 'unknown'
        modes = {}
        profile = None
        for line in settings.splitlines():
            if line.strip().endswith('Power:'):
                profile = line.strip()[:-1]
            mode = re.search(r'lowpowermode\s+(\d+)', line)
            if mode:
                modes[profile] = int(mode.group(1))
        return {'source': source, 'low_power_mode': modes.get(source)}
    except (OSError, subprocess.SubprocessError):
        return {'source': 'unknown', 'low_power_mode': None}


class BenchmarkClock:
    def __init__(self, directory, monitor_power=True):
        self.directory = directory
        self.process = None
        self.started = None
        self.monitor_power = monitor_power
        self.power_observations = []
        self.power_lock = threading.Lock()
        self.stop_monitor = threading.Event()
        self.monitor = None

    def start(self):
        if sys.platform == 'darwin':
            self.process = subprocess.Popen(['/usr/bin/caffeinate', '-i', '-s', '-w', str(os.getpid())],
                                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                            stderr=subprocess.DEVNULL)
        self.started = (time.time(), time.monotonic())
        self.sample_power()
        if self.monitor_power and sys.platform == 'darwin':
            self.monitor = threading.Thread(target=self.watch_power, daemon=True)
            self.monitor.start()

    def sample_power(self):
        condition = read_power_conditions()
        with self.power_lock:
            if condition not in self.power_observations:
                self.power_observations.append(condition)

    def watch_power(self):
        while not self.stop_monitor.wait(2):
            self.sample_power()

    def snapshot(self):
        wall_seconds = time.time() - self.started[0]
        monotonic_seconds = time.monotonic() - self.started[1]
        drift = wall_seconds - monotonic_seconds
        alive = self.process is not None and self.process.poll() is None
        self.sample_power()
        with self.power_lock:
            conditions = list(self.power_observations)
        power_stable = len(conditions) == 1 and (sys.platform != 'darwin' or
                       (conditions[0]['source'] in ('AC Power', 'Battery Power') and
                        conditions[0]['low_power_mode'] in (0, 1)))
        return {'wall_seconds': wall_seconds, 'monotonic_seconds': monotonic_seconds,
                'clock_divergence_seconds': drift, 'sleep_guard_required': sys.platform == 'darwin',
                'sleep_guard_alive': alive,
                'power_conditions': conditions, 'power_stable': power_stable,
                'valid': abs(drift) <= 2 and (sys.platform != 'darwin' or alive) and power_stable,
                'note': 'Temporary idle/system sleep assertion on macOS; forced sleep or clock adjustment can still invalidate timing.'}

    def close(self):
        try:
            if self.started is not None:
                (self.directory/'clock-safety-final.json').write_text(json.dumps(self.snapshot(), indent=2))
        finally:
            self.stop_monitor.set()
            if self.monitor is not None:
                self.monitor.join(timeout=12)
            if self.process is not None and self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
