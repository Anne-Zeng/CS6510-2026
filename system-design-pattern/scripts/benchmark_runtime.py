import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time


def seed_catalog(databases, count, stock):
    if type(count) is not int or not 1 <= count <= 5000:
        raise ValueError('catalog-size must be between 1 and 5000')
    if type(stock) is not int or not 1 <= stock <= 2_000_000_000:
        raise ValueError('Invalid stock')
    databases['catalog'].sql(f"""
        INSERT INTO catalog_items(sku,name,price)
        SELECT 'SKU-'||lpad(number::text,6,'0'),'Store Item '||lpad(number::text,6,'0'),
               (100+number%1000)::numeric/100
        FROM generate_series(1,{count}) number
        """)
    databases['inventory'].sql(f"""
        INSERT INTO inventory(sku,name,current_stock)
        SELECT 'SKU-'||lpad(number::text,6,'0'),'Store Item '||lpad(number::text,6,'0'),{stock}
        FROM generate_series(1,{count}) number
        """)
    return {'opening_stock_per_sku': stock, 'sku_count': count}


class PostgresInstances:
    def __init__(self, binary, run, environment):
        self.binary = Path(binary)
        self.run = Path(run)
        self.env = environment
        self.started = []

    def start(self, services, base_port, deployment, notification_wal_compression='off'):
        ports = {}
        instances = services if deployment == 'separate' else ('shared',)
        for index, name in enumerate(instances):
            port = base_port + index
            compression = instance_wal_compression(name, notification_wal_compression)
            data = self.run / ('postgres-' + name)
            with (self.run / ('initdb-' + name + '.log')).open('w') as output:
                subprocess.run([str(self.binary/'initdb'), '-D', str(data), '-U', 'pattern_test',
                                '-A', 'trust', '--no-locale', '-E', 'UTF8'], env=self.env,
                               stdout=output, stderr=subprocess.STDOUT, check=True)
            self.started.append(data)
            subprocess.run([str(self.binary/'pg_ctl'), '-D', str(data), '-l', str(self.run/(name+'-postgres.log')),
                            '-o', f'-p {port} -h 127.0.0.1 -k /tmp -c fsync=on -c synchronous_commit=on -c full_page_writes=on -c shared_buffers=32MB -c wal_compression={compression}',
                            '-w', 'start'], env=self.env, check=True)
            for service in (services if deployment == 'shared' else (name,)):
                ports[service] = port
        return ports

    def stop(self):
        failures = []
        for data in reversed(self.started):
            status = subprocess.run([str(self.binary/'pg_ctl'), '-D', str(data), 'status'],
                                    env=self.env, capture_output=True)
            if status.returncode == 0:
                result = subprocess.run([str(self.binary/'pg_ctl'), '-D', str(data), '-m', 'fast', '-w', 'stop'], env=self.env)
                if result.returncode:
                    failures.append(str(data))
        if failures:
            raise RuntimeError('Unable to stop test instances: ' + ', '.join(failures))


def instance_wal_compression(name, compression):
    if compression not in ('off', 'pglz', 'lz4'):
        raise ValueError('Notification WAL compression must be off, pglz or lz4')
    return compression if name == 'notification' else 'off'


def source_hashes(root):
    root = Path(root)
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(root.rglob('*.java')) if 'target' not in path.relative_to(root).parts}


def original_snapshot(source, destination):
    shutil.copytree(source, destination, ignore=shutil.ignore_patterns(
        'target', 'reports', 'docs', '.git', '.env', '.env.*', '__pycache__', '.DS_Store', '*.log*'))
    return Path(destination)


def monitored_run(command, env, output, directory, timeout):
    process = subprocess.Popen(command, env=env, stdout=output, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + timeout
    try:
        while process.poll() is None:
            if shutil.disk_usage(directory).free < 1024**3:
                raise RuntimeError('Less than 1 GiB free; workload stopped to preserve disk space')
            if time.monotonic() >= deadline:
                raise RuntimeError('Load client timed out')
            time.sleep(.5)
        if process.returncode:
            raise RuntimeError('Load client exited with status ' + str(process.returncode))
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
