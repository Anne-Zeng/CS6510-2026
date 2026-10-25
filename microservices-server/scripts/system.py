#!/usr/bin/env python3
"""管理四个私有数据库及五个本地进程；初始化和启动均保留已有数据。"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import subprocess
import sys
import time
from urllib.parse import urlsplit, unquote
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ('catalog', 'checkout', 'analytics', 'inventory')
VERSION = '0.0.1-SNAPSHOT'


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def load_environment(path=None):
    """解析配置文本而非执行 shell；显式配置文件中的值覆盖终端环境。"""
    env = dict(os.environ)
    source = Path(path).expanduser() if path else ROOT / '.env'
    if path:
        require(source.is_file(), 'Environment file not found: ' + str(source))
    if source.is_file():
        for number, raw in enumerate(source.read_text().splitlines(), 1):
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            key, separator, value = line.partition('=')
            key = key.strip()
            require(separator and re.fullmatch(r'[A-Z][A-Z0-9_]*', key), f'Invalid configuration key at line {number}')
            values = shlex.split(value, comments=True)
            require(len(values) <= 1, f'Quote configuration values containing spaces at line {number}')
            env[key] = values[0] if values else ''
    return env


class Database:
    """每个服务使用自己的连接参数；密码不进入命令行或输出。"""
    def __init__(self, service, env):
        self.service = service
        prefix = service.upper() + '_DB_'
        self.url = env.get(prefix + 'URL', f'jdbc:postgresql://localhost:5432/{service}_db')
        require(self.url.startswith('jdbc:postgresql://'), service + ': expected a PostgreSQL JDBC URL')
        parsed = urlsplit(self.url[5:])
        require(parsed.hostname and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment,
                service + ': use jdbc:postgresql://host:port/database without credentials or query options')
        self.host = parsed.hostname
        self.port = parsed.port or 5432
        self.name = unquote(parsed.path.removeprefix('/'))
        require(re.fullmatch(r'[a-z][a-z0-9_]{0,62}', self.name), service + ': invalid database name')
        self.user = env.get(prefix + 'USER', service + '_user')
        require(self.user, service + ': database user is required')
        require(prefix + 'PASSWORD' in env, 'Set ' + prefix + 'PASSWORD in the terminal or .env file')
        self.password = env[prefix + 'PASSWORD']
        self.env = env

    def sql(self, query=None, file=None):
        args = ['psql', '-X', '-h', self.host, '-p', str(self.port), '-U', self.user,
                '-d', self.name, '-qtA', '-v', 'ON_ERROR_STOP=1', '--no-password']
        args += ['-f', str(file)] if file else ['-c', query]
        # 功能：使用明确的连接字段；不继承其他连接服务或 URL 覆盖参数。
        env = {key: value for key, value in self.env.items() if not key.startswith('PG')}
        env['PGPASSWORD'] = self.password
        result = subprocess.run(args, env=env, capture_output=True, text=True)
        require(result.returncode == 0,
                f'{self.service} database connection/initialization failed: {result.stderr.strip()}')
        return result.stdout.strip()

    def initialize(self, seed=True):
        """数据库及角色由用户预先创建；服务角色仅初始化自己的表，不需要管理员权限。"""
        module = ROOT / (self.service + '-service') / 'database'
        self.sql(file=module / 'schema.sql')
        if seed and self.service in ('catalog', 'inventory'):
            table = 'catalog_items' if self.service == 'catalog' else 'inventory'
            if self.sql('SELECT count(*) FROM ' + table) == '0':
                self.sql(file=module / 'seed.sql')
        print(f'Initialized {self.service}: {self.name}; existing data preserved', flush=True)


def databases_from(env):
    databases = {service: Database(service, env) for service in SERVICES}
    identities = {(('loopback' if db.host in ('localhost', '127.0.0.1', '::1') else db.host), db.port, db.name)
                  for db in databases.values()}
    require(len(identities) == 4, 'Each service must use a different database')
    return databases


def fetch_json(url):
    with urlopen(url, timeout=3) as response:
        return json.load(response)


class Cluster:
    def __init__(self, databases, output_dir, base_port=8080, env=None, startup_timeout=120):
        require(1024 <= base_port <= 65531, 'Choose a base port from 1024 through 65531 (five consecutive ports)')
        self.databases = databases
        self.output = Path(output_dir)
        self.env = dict(env if env is not None else os.environ)
        self.ports = {name: base_port + offset for offset, name in enumerate(('gateway', *SERVICES))}
        self.base_url = f'http://127.0.0.1:{base_port}'
        self.processes = {}
        self.logs = {}
        self.startup_timeout = startup_timeout

    def check_ports(self):
        for name, port in self.ports.items():
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    probe.bind(('127.0.0.1', port))
                except OSError as error:
                    raise RuntimeError(f'{name} port {port} is occupied; stop the old service or choose --base-port') from error

    def jar(self, name):
        artifact = 'gateway' if name == 'gateway' else name + '-service'
        return ROOT / artifact / 'target' / f'{artifact}-{VERSION}.jar'

    def check_jars(self):
        for name in self.ports:
            require(self.jar(name).is_file(), f'Missing {self.jar(name)}; run ./mvnw package')

    def _launch(self, name):
        require(name not in self.processes, name + ' is already managed')
        environment = dict(self.env)
        # 功能：每个子进程只接收自己的数据库凭据。
        for service in SERVICES:
            for key in list(environment):
                if service != name and key.startswith(service.upper() + '_DB_'):
                    environment.pop(key)
        for key in ('SPRING_DATASOURCE_URL', 'SPRING_DATASOURCE_USERNAME', 'SPRING_DATASOURCE_PASSWORD', 'SPRING_APPLICATION_JSON', 'PGPASSWORD'):
            environment.pop(key, None)
        command = [self.env.get('JAVA_BIN', 'java'),
                   *shlex.split(self.env.get('MICROSERVICES_JAVA_OPTS', '-Xms32m -Xmx192m -XX:TieredStopAtLevel=1')),
                   '-jar', str(self.jar(name))]
        if name == 'gateway':
            command += [f'--port={self.ports[name]}',
                        *[f'--{service}-url=http://127.0.0.1:{self.ports[service]}' for service in SERVICES]]
        else:
            db = self.databases[name]
            environment.update({name.upper() + '_DB_URL': db.url,
                                name.upper() + '_DB_USER': db.user,
                                name.upper() + '_DB_PASSWORD': db.password,
                                'SPRING_DATASOURCE_URL': db.url,
                                'SPRING_DATASOURCE_USERNAME': db.user,
                                'SPRING_DATASOURCE_PASSWORD': db.password})
            for target in ('catalog', 'inventory', 'analytics'):
                environment[target.upper() + '_BASE_URL'] = f'http://127.0.0.1:{self.ports[target]}'
            command += [f'--server.port={self.ports[name]}', '--server.address=127.0.0.1']
        self.output.mkdir(parents=True, exist_ok=True)
        log = open(self.output / (name + '.log'), 'a', encoding='utf-8')
        try:
            self.processes[name] = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=log, stderr=log)
            self.logs[name] = log
        except Exception:
            log.close()
            raise

    def check_alive(self):
        for name, process in self.processes.items():
            require(process.poll() is None, f'{name} exited; see {self.output / (name + ".log")}')

    def _wait_ready(self, name):
        deadline = time.monotonic() + self.startup_timeout
        while time.monotonic() < deadline:
            self.check_alive()
            try:
                health = fetch_json(f'http://127.0.0.1:{self.ports[name]}/health')
                expected = 'gateway' if name == 'gateway' else name + '-service'
                if health.get('status') == 'UP' and health.get('service') == expected:
                    return
            except (OSError, ValueError):
                pass
            time.sleep(.25)
        raise RuntimeError(f'{name} startup timed out; see {self.output / (name + ".log")}')

    def start(self):
        self.check_ports()
        self.check_jars()
        for name in SERVICES:
            self._launch(name)
        for name in SERVICES:
            self._wait_ready(name)
        self.start_service('gateway')
        require(bool(fetch_json(self.base_url + '/items')['items']), 'Catalog is empty')
        fetch_json(self.base_url + '/inventory/low-stock')
        fetch_json(self.base_url + '/analytics/popular-items')

    def start_service(self, name):
        require(name in self.ports, 'Unknown service')
        self._launch(name)
        self._wait_ready(name)

    def stop_service(self, name):
        process = self.processes.get(name)
        if process is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            self.processes.pop(name)
            self.logs.pop(name).close()

    def stop(self):
        failures = []
        for name in ('gateway', 'checkout', 'analytics', 'inventory', 'catalog'):
            try:
                self.stop_service(name)
            except Exception as error:
                failures.append(name + ': ' + str(error))
        require(not failures, 'Cleanup failed: ' + '; '.join(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('init-db', 'serve'))
    parser.add_argument('--env-file', help='Defaults to microservices-server/.env when present')
    parser.add_argument('--base-port', type=int, default=8080)
    parser.add_argument('--skip-build', action='store_true')
    parser.add_argument('--skip-init', action='store_true', help='Use only after schemas/migrations are already applied')
    parser.add_argument('--no-seed', action='store_true', help='Do not initialize demo products/stock in empty tables')
    parser.add_argument('--log-dir', default=str(ROOT / 'target/runtime'))
    args = parser.parse_args()
    env = load_environment(args.env_file)
    databases = databases_from(env)
    cluster = Cluster(databases, args.log_dir, args.base_port, env)
    if args.action == 'serve':
        cluster.check_ports()
        if not args.skip_build:
            subprocess.run([str(ROOT / 'mvnw'), '-q', 'package'], cwd=ROOT, env=env, check=True)
        cluster.check_jars()
    if not args.skip_init or args.action == 'init-db':
        for database in databases.values():
            database.initialize(seed=not args.no_seed)
    if args.action == 'init-db':
        return
    try:
        cluster.start()
        print(f'All services ready: {cluster.base_url}; Ctrl+C stops these five processes', flush=True)
        print('Logs: ' + str(cluster.output), flush=True)
        while True:
            cluster.check_alive()
            time.sleep(1)
    finally:
        cluster.stop()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        main()
    except KeyboardInterrupt:
        print('Services stopped; database contents preserved.')
    except Exception as error:
        print('ERROR: ' + str(error), file=sys.stderr)
        sys.exit(1)
