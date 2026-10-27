#!/usr/bin/env python3
"""Run the assignment's single-node Kafka broker natively in the foreground.

Uses the Kafka distribution extracted from the official apache/kafka:4.2.1
image, with the same replication factor as compose.yaml. Existing Kafka data
is preserved. Stop this helper with Ctrl+C to stop only its broker process.
"""
from pathlib import Path
import argparse
import base64
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
IMAGE = 'apache/kafka:4.2.1'


def checked(command):
    """Surface command failures, including Docker extraction and cleanup errors."""
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f'{command[0]} {command[1]} failed ({result.returncode}): {detail}')
    return result.stdout.strip()


def distribution_at(path):
    if path.exists():
        if not (path / 'libs').is_dir() or not any((path / 'libs').glob('kafka-clients-*.jar')):
            raise RuntimeError(f'Incomplete Kafka distribution: {path}; choose --distribution with a libs directory')
        return path
    if shutil.which('docker') is None:
        raise RuntimeError(f'Kafka distribution not found at {path}; supply --distribution or install Docker to extract {IMAGE}')
    # A stopped, uniquely named container supplies files only; no broker is run in it.
    name = 'selfcheckout-kafka-extract-' + uuid.uuid4().hex
    path.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='kafka-extract-', dir=path.parent))
    created = False
    try:
        checked(['docker', 'create', '--pull=never', '--name', name, '--entrypoint', '/bin/true', IMAGE])
        created = True
        checked(['docker', 'cp', name + ':/opt/kafka/.', str(stage)])
        if not any((stage / 'libs').glob('kafka-clients-*.jar')):
            raise RuntimeError('The official image did not contain the expected Kafka distribution')
        if path.exists():
            raise RuntimeError(f'Distribution path appeared during extraction; preserving it: {path}')
        stage.rename(path)
        print(f'Extracted {IMAGE} to {path}', flush=True)
    finally:
        # Remove only the temporary directory and container created by this invocation.
        if stage.exists():
            shutil.rmtree(stage)
        if created:
            checked(['docker', 'rm', name])
    return path


def require_free_ports(ports):
    probes = []
    try:
        for port in ports:
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            probes.append(probe)
            try:
                probe.bind(('127.0.0.1', port))
            except OSError as error:
                raise RuntimeError(f'Port {port} is unavailable; choose a different --port. No existing process was stopped.') from error
    finally:
        for probe in probes:
            probe.close()


def property_value(value):
    # Java Properties values need escaping even though the Java command uses an argv list.
    return str(value).replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t').replace(' ', '\\ ')


class OwnedProcess:
    def __init__(self):
        self.child = None
        self.stop_signal = None
        self.stop_started = None

    def stop(self, signum, _frame):
        if self.stop_signal is None:
            self.stop_signal = signum
            self.stop_started = time.monotonic()
        if self.child is not None and self.child.poll() is None:
            try:
                self.child.terminate()
            except ProcessLookupError:
                pass

    def run(self, command, output=None):
        if self.stop_signal is not None:
            return 128 + self.stop_signal
        # A separate session ensures Ctrl+C is handled here and forwarded only to our child.
        self.child = subprocess.Popen(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT if output else None,
                                      start_new_session=True)
        try:
            while True:
                try:
                    return self.child.wait(timeout=.25)
                except subprocess.TimeoutExpired:
                    if self.stop_signal is not None:
                        self.stop(self.stop_signal, None)
                        if time.monotonic() - self.stop_started >= 20:
                            self.child.kill()
        finally:
            # Never leave this invocation's Java process behind if the helper itself fails.
            if self.child.poll() is None:
                self.child.terminate()
                try:
                    self.child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    self.child.kill()
                    self.child.wait()
            self.child = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--distribution', type=Path, default=ROOT / 'target/kafka-dist',
                        help='Kafka distribution containing libs/; extract the official image here if absent')
    parser.add_argument('--port', type=int, default=19092,
                        help='Loopback broker port; controller uses the next port (default: 19092)')
    parser.add_argument('--data-dir', type=Path, default=ROOT / 'target/native-kafka/data',
                        help='Persistent Kafka data directory; existing data is never removed')
    args = parser.parse_args()
    if not 1024 <= args.port <= 65534:
        parser.error('--port must be between 1024 and 65534')
    java = os.environ.get('JAVA_BIN', 'java')
    if shutil.which(java) is None:
        raise RuntimeError(f'Java executable not found: {java}')
    # Check before touching configuration, distribution, or storage.
    require_free_ports((args.port, args.port + 1))
    owner = OwnedProcess()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, owner.stop)
    distribution = distribution_at(args.distribution.expanduser().resolve())
    if owner.stop_signal is not None:
        return 128 + owner.stop_signal
    data = args.data_dir.expanduser().resolve()
    metadata = data / 'meta.properties'
    if data.exists() and not data.is_dir():
        raise RuntimeError(f'Kafka data path is not a directory: {data}')
    if not metadata.exists() and data.exists() and any(data.iterdir()):
        raise RuntimeError(f'Nonempty data directory has no meta.properties; preserving it without formatting: {data}')
    runtime = ROOT / 'target/native-kafka' / str(args.port)
    logs = runtime / 'logs'
    logs.mkdir(parents=True, exist_ok=True)
    config = runtime / 'server.properties'
    properties = {
        'process.roles': 'broker,controller',
        'node.id': 1,
        'controller.quorum.voters': f'1@127.0.0.1:{args.port + 1}',
        'listeners': f'PLAINTEXT://127.0.0.1:{args.port},CONTROLLER://127.0.0.1:{args.port + 1}',
        'advertised.listeners': f'PLAINTEXT://127.0.0.1:{args.port}',
        'listener.security.protocol.map': 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
        'controller.listener.names': 'CONTROLLER',
        'inter.broker.listener.name': 'PLAINTEXT',
        'log.dirs': data,
        'num.partitions': 8,
        'offsets.topic.replication.factor': 1,
        'transaction.state.log.replication.factor': 1,
        'transaction.state.log.min.isr': 1,
        'group.initial.rebalance.delay.ms': 0,
        'num.network.threads': 3,
        'num.io.threads': 4,
    }
    config.write_text(''.join(f'{key}={property_value(value)}\n' for key, value in properties.items()))
    java_command = [java, '-Xms128m', '-Xmx256m', f'-Dkafka.logs.dir={logs}',
                    '-Dlog4j2.configurationFile=' + (distribution / 'config/log4j2.yaml').as_uri(),
                    '-cp', str(distribution / 'libs/*')]
    if not metadata.exists():
        data.mkdir(parents=True, exist_ok=True)
        cluster_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=')
        format_log = runtime / 'storage-format.log'
        with format_log.open('a') as output:
            code = owner.run([*java_command, 'kafka.tools.StorageTool', 'format', '-t', cluster_id, '-c', str(config)], output)
        if owner.stop_signal is not None:
            return 128 + owner.stop_signal
        if code:
            raise RuntimeError(f'Kafka storage format failed ({code}); see {format_log}. Existing data was not removed.')
        if not metadata.is_file():
            raise RuntimeError(f'Kafka storage format returned success without creating {metadata}')
    print(f'Kafka: 127.0.0.1:{args.port}; controller: 127.0.0.1:{args.port + 1}', flush=True)
    print(f'Data: {data}\nConfig: {config}\nLogs: {logs}\nCtrl+C stops only this broker; persistent data is retained.', flush=True)
    code = owner.run([*java_command, 'kafka.Kafka', str(config)])
    if owner.stop_signal is not None:
        return 128 + owner.stop_signal
    if code:
        raise RuntimeError(f'Kafka exited with status {code}; see {logs}')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, RuntimeError) as error:
        print('Error: ' + str(error), file=sys.stderr)
        sys.exit(1)
