#!/usr/bin/env python3
"""功能：使用标准库管理共享数据库、四个 Java 进程和可重复的正式负载测试。"""
import argparse
from datetime import datetime
import json
import os
import platform
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

# 功能：所有脚本均相对项目目录定位，不依赖调用者当前所在的文件夹。
ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.0.1-SNAPSHOT"
JAVA = os.environ.get("JAVA_BIN", "java")
JAVAC = os.environ.get("JAVAC_BIN", "javac")
JVM_OPTIONS = shlex.split(os.environ.get("CHECKOUT_JAVA_OPTS", "-Xms64m -Xmx256m"))


def require(condition, message):
    """功能：在执行会改变状态的操作之前检查前置条件。"""
    if not condition:
        raise RuntimeError(message)


def fetch_json(url, timeout=5):
    """功能：读取真实 HTTP JSON；连接失败或非成功状态直接抛出异常。"""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


class Database:
    """功能：统一执行初始化和 SQL；密码通过环境变量传给 psql，不放入命令行。"""

    def __init__(self, db_name=None):
        """功能：读取数据库连接配置，并限制数据库名称为安全的 SQL 标识符。"""
        self.name = db_name or os.environ.get("CHECKOUT_DB_NAME", "servicebaseddb")
        require(re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.name), "Invalid database name")
        self.host = os.environ.get("CHECKOUT_DB_HOST", "localhost")
        self.port = int(os.environ.get("CHECKOUT_DB_PORT", "5432"))
        self.user = os.environ.get("CHECKOUT_DB_USER", "postgres")
        self.password = os.environ.get("CHECKOUT_DB_PASSWORD", "postgres")

    def sql(self, query, admin=False):
        """功能：用参数数组调用 psql，不经过 shell；返回无表头的原始结果。"""
        result = subprocess.run(["psql", "-X", "-h", self.host, "-p", str(self.port), "-U", self.user,
                                 "-d", "postgres" if admin else self.name, "-qtA", "-v", "ON_ERROR_STOP=1",
                                 "-c", query], capture_output=True, text=True,
                                env={**os.environ, "PGPASSWORD": self.password})
        require(result.returncode == 0, "PostgreSQL failed: " + result.stderr.strip())
        return result.stdout.strip()

    def initialize(self, reset=False, catalog_size=2000, stock=1000000):
        """功能：先统一建表/填充目录，服务启动后仅验证结构；重置只允许专用测试库。"""
        require(catalog_size > 0 and stock > 0, "Catalog size and opening stock must be positive")
        if reset:
            require(self.name.startswith("servicebased_acceptance"),
                    "Only servicebased_acceptance* databases may be reset")
        exists = self.sql(f"SELECT 1 FROM pg_database WHERE datname='{self.name}'", admin=True)
        if not exists:
            self.sql(f'CREATE DATABASE "{self.name}"', admin=True)
        if reset:
            self.sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        self.sql((ROOT / "database" / "schema.sql").read_text(encoding="utf-8"))

        # 功能：同一事务内检查并填充目录和库存；重复启动不补货、不删除历史交易。
        self.sql(f"""
            BEGIN;
            SELECT pg_advisory_xact_lock(6510, 2);
            DO $$
            DECLARE products bigint; stocks bigint;
            BEGIN
                SELECT count(*) INTO products FROM catalog_items;
                SELECT count(*) INTO stocks FROM inventory;
                IF products=0 AND stocks=0 THEN
                    INSERT INTO catalog_items(sku,name,price)
                    SELECT 'SKU-' || lpad(n::text,6,'0'), 'Store Item ' || lpad(n::text,6,'0'),
                           (100 + n % 1000)::numeric / 100 FROM generate_series(1,{catalog_size}) n;
                    INSERT INTO inventory(sku,current_stock) SELECT sku,{stock} FROM catalog_items;
                ELSIF products<>{catalog_size} OR stocks<>{catalog_size} THEN
                    RAISE EXCEPTION 'Seed size mismatch: catalog %, inventory %',products,stocks;
                END IF;
            END $$;
            COMMIT;
        """)


class Cluster:
    """功能：只管理本对象启动的进程；三个业务服务连接同一个数据库。"""

    def __init__(self, database, output_dir, base_port=18090):
        """功能：分配四个独立端口，保存进程句柄及日志位置。"""
        require(1024 <= base_port <= 65532, "Base port must allow four consecutive unprivileged ports")
        self.db = database
        self.output = Path(output_dir)
        self.output.mkdir(parents=True, exist_ok=True)
        self.ports = {name: base_port + offset for offset, name in
                      enumerate(("gateway", "catalog", "checkout", "analytics"))}
        self.base_url = f"http://127.0.0.1:{base_port}"
        self.processes = {}
        self.logs = {}

    def check_ports(self):
        """功能：先检查全部端口，避免占用其他项目进程或启动一半才发现冲突。"""
        for name, port in self.ports.items():
            with socket.socket() as probe:
                # 功能：允许复用已关闭连接的 TIME_WAIT 端口，仍拒绝正在监听的其他进程。
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    probe.bind(("127.0.0.1", port))
                except OSError as error:
                    raise RuntimeError(f"{name} port {port} is occupied; choose another base port") from error

    def _launch(self, name):
        """功能：启动一个独立 JAR；各进程使用相同连接地址和各自的连接池。"""
        require(name not in self.processes, f"{name} is already managed")
        artifact = "gateway" if name == "gateway" else name + "-service"
        jar = ROOT / artifact / "target" / f"{artifact}-{VERSION}.jar"
        require(jar.is_file(), f"Missing {jar}; run ./mvnw clean package first")
        command = [JAVA, *JVM_OPTIONS, "-jar", str(jar)]
        environment = {**os.environ}
        if name == "gateway":
            command += [f"--port={self.ports[name]}",
                        *[f"--{service}-url=http://127.0.0.1:{self.ports[service]}"
                          for service in ("catalog", "checkout", "analytics")]]
        else:
            # 功能：显式统一连接参数，环境中的其他数据库 URL 不会使服务指向不同数据库。
            environment.update(CHECKOUT_DB_URL=f"jdbc:postgresql://{self.db.host}:{self.db.port}/{self.db.name}",
                               CHECKOUT_DB_USER=self.db.user, CHECKOUT_DB_PASSWORD=self.db.password)
            command += [f"--server.port={self.ports[name]}", "--server.address=127.0.0.1"]
        log = open(self.output / f"{name}.log", "a", encoding="utf-8")
        self.logs[name] = log
        self.processes[name] = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=log, stderr=log)

    def _wait_ready(self, name):
        """功能：等待目标进程及数据库检查成功；启动失败时给出具体日志路径。"""
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            require(self.processes[name].poll() is None, f"{name} exited; see {self.output / (name + '.log')}")
            try:
                if fetch_json(f"http://127.0.0.1:{self.ports[name]}/health")["status"] == "UP":
                    return
            except (OSError, ValueError, KeyError):
                pass
            time.sleep(0.25)
        raise RuntimeError(f"{name} startup timed out; see {self.output / (name + '.log')}")

    def start(self):
        """功能：先并行启动三个业务 JVM，再启动网关；每个服务都需独立通过健康检查。"""
        self.check_ports()
        for name in ("catalog", "checkout", "analytics"):
            self._launch(name)
        for name in ("catalog", "checkout", "analytics"):
            self._wait_ready(name)
        self.start_service("gateway")
        require(bool(fetch_json(self.base_url + "/items")["items"]), "Catalog is empty")

    def start_service(self, name):
        """功能：支持只重启 Analytics，以验证服务隔离和数据库恢复。"""
        require(name in self.ports, "Unknown service: " + name)
        self._launch(name)
        self._wait_ready(name)

    def stop_service(self, name):
        """功能：优雅停止本对象启动的单个进程；超时才强制终止，不扫描/杀其他进程。"""
        process = self.processes.get(name)
        if process is not None:
            try:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
            finally:
                # 功能：仅确认进程退出后才移除句柄；清理失败时保留句柄供再次处理。
                if process.poll() is not None:
                    self.processes.pop(name)
                    self.logs.pop(name).close()

    def stop(self):
        """功能：先关入口再关业务服务；失败、Ctrl+C 以及正常结束均执行清理。"""
        failures = []
        for name in ("gateway", "analytics", "checkout", "catalog"):
            try:
                self.stop_service(name)
            except Exception as error:
                failures.append(f"{name}: {error}")
        require(not failures, "Cleanup failed: " + "; ".join(failures))


def build():
    """功能：构建全部模块并执行单元测试；不 install 到全局 Maven 仓库。"""
    subprocess.run([str(ROOT / "mvnw"), "-q", "clean", "package"], cwd=ROOT, check=True)


def wait_for_analytics(cluster, database):
    """功能：等待所有到期窗口持久化，避免把异步延迟误判为结果丢失。"""
    # 功能：以原始已接受事件数为目标，不能用尚未追上的分析日志数量判断已经排空。
    raw_count = int(database.sql("SELECT count(*) FROM scan_outbox"))
    expected = raw_count // 500 * 500
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        persisted = database.sql("SELECT COALESCE(max(sequence),0),count(*),"
                                 "(SELECT count(*) FROM scan_outbox WHERE processed=false) FROM scan_events")
        last_sequence, event_count, pending = map(int, persisted.split("|"))
        if (last_sequence == raw_count and event_count == raw_count and pending == 0
                and fetch_json(cluster.base_url + "/analytics/popular-items")["windowEnd"] == expected):
            return expected
        time.sleep(0.25)
    raise RuntimeError("Analytics did not drain all due windows")


def benchmark(args):
    """功能：每种模式从相同初始数据开始，保存原始报告并做数据库与重启审计。"""
    database = Database(args.database or "servicebased_acceptance")
    require(database.name.startswith("servicebased_acceptance"), "Benchmark requires a dedicated acceptance database")
    cluster = Cluster(database, ROOT / "target" / "preflight", args.base_port)
    cluster.check_ports()
    if not args.skip_build:
        build()
    compiled = ROOT / "target" / "load-client"
    compiled.mkdir(parents=True, exist_ok=True)
    subprocess.run([JAVAC, "--release", "21", "-d", str(compiled),
                    *map(str, sorted((ROOT.parent / "load-client" / "src").glob("*.java")))], check=True)
    run_dir = ROOT / "reports" / datetime.now().strftime("run-%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True)

    # 功能：记录运行环境，报告的时长/延迟等测量值始终由原始客户端生成。
    metadata = {"database": database.name, "catalogSize": args.catalog_size, "initialStockPerSku": args.stock,
                "checkoutPersistence": "JDBC atomic basket + durable DB outbox; Analytics batches canonical numbering",
                "transactionIsolation": "READ COMMITTED", "synchronousCommit": True,
                "platform": platform.platform(), "logicalCpuCount": os.cpu_count(),
                "postgresqlVersion": database.sql("SELECT version()", admin=True),
                "basePort": args.base_port, "jvmOptions": JVM_OPTIONS,
                "javaVersion": subprocess.run([JAVA, "-version"], capture_output=True, text=True).stderr.strip(),
                "poolLimits": {"catalog": int(os.environ.get("CHECKOUT_DB_POOL", "8")),
                               "checkout": int(os.environ.get("CHECKOUT_DB_POOL", "16")),
                               "analytics": int(os.environ.get("CHECKOUT_DB_POOL", "8"))},
                "workloads": {"default": {"stations": 10, "durationSeconds": 60},
                              "stress": {"stations": 100, "durationSeconds": 120}}}
    (run_dir / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    for mode, stations, duration in (("default", 10, 60), ("stress", 100, 120)):
        output = run_dir / mode
        cluster = Cluster(database, output, args.base_port)
        try:
            print(f"Running {mode}: {stations} stations, {duration} seconds", flush=True)
            database.initialize(reset=True, catalog_size=args.catalog_size, stock=args.stock)
            cluster.start()
            command = [JAVA, "-cp", str(compiled), "Main", f"--baseUrl={cluster.base_url}", f"--reportDir={output}"]
            # 功能：Default 不覆盖负载参数；Stress 使用教授指定的 100 stations / 120 秒。
            if mode == "stress":
                command += ["--stations=100", "--duration=120"]
            with open(output / "client.log", "w", encoding="utf-8") as log:
                subprocess.run(command, stdout=log, stderr=log, check=True)
            # 功能：客户端会捕获最后两次 GET 的异常，所以还需检查日志，避免把失败误当空列表。
            client_output = (output / "client.log").read_text(encoding="utf-8")
            for marker in ("Could not fetch low-stock alerts:", "Could not fetch popular items:",
                           "not all stations finished cleanly"):
                require(marker not in client_output, "Client warning/failure: " + marker)
            reports = list(output.glob("report-*.json"))
            require(len(reports) == 1, "Expected exactly one client JSON report")
            report = reports[0]
            body = json.loads(report.read_text())
            require(body["config"]["stations"] == stations and body["config"]["durationSecondsRequested"] == duration,
                    "Client configuration differs from the required workload")
            require(body["totalTransactions"] > 0, "Workload completed no transactions")
            require(all(operation["errorCount"] == 0 for operation in body["operations"]), "Load client reported request errors")
            low_stock = fetch_json(cluster.base_url + "/inventory/low-stock")
            (output / "low-stock.json").write_text(json.dumps(low_stock, indent=2) + "\n")
            actual_alerts = [{key: alert[key] for key in ("sku", "name", "currentStock", "threshold")}
                             for alert in low_stock["alerts"]]
            require(body["lowStockAlerts"] == actual_alerts, "Client low-stock alerts disagree with the live API")
            wait_for_analytics(cluster, database)
            before = fetch_json(cluster.base_url + "/analytics/popular-items")
            (output / "popular-items.json").write_text(json.dumps(before, indent=2) + "\n")

            # 功能：单独重启分析进程，验证数据库排名不依赖进程内缓存/队列。
            cluster.stop_service("analytics")
            cluster.start_service("analytics")
            after = fetch_json(cluster.base_url + "/analytics/popular-items")
            require(before == after, "Analytics restart changed the persisted ranking")
            (output / "popular-items-after-restart.json").write_text(json.dumps(after, indent=2) + "\n")
            cluster.stop()

            # 功能：停止写入后独立审计所有交易、库存、事件和窗口，不只检查请求是否成功。
            audit = subprocess.run([sys.executable, str(ROOT / "scripts" / "verify_run.py"), "--report", str(report),
                                    "--host", database.host, "--port", str(database.port), "--dbname", database.name,
                                    "--user", database.user, "--catalog-size", str(args.catalog_size),
                                    "--initial-stock", str(args.stock)], capture_output=True, text=True,
                                   env={**os.environ, "CHECKOUT_DB_PASSWORD": database.password})
            (output / "correctness.json").write_text(audit.stdout, encoding="utf-8")
            require(audit.returncode == 0, "Database audit failed: " + audit.stdout + audit.stderr)
            shutil.copyfile(report, run_dir / f"report-{mode}.json")
            print(f"{mode}: {body['totalTransactions']} transactions, {body['transactionsPerSecond']:.2f}/s; audit passed", flush=True)
        finally:
            cluster.stop()

    # 功能：只有两组运行都通过后才更新提交目录，避免失败运行覆盖有效报告。
    submission = ROOT / "reports" / "submission"
    submission.mkdir(parents=True, exist_ok=True)
    for filename in ("report-default.json", "report-stress.json", "environment.json"):
        shutil.copyfile(run_dir / filename, submission / filename)
    for mode in ("default", "stress"):
        shutil.copyfile(run_dir / mode / "correctness.json", submission / f"correctness-{mode}.json")
    print(f"Submission reports: {submission}", flush=True)


def main():
    """功能：提供初始化、前台运行和正式 benchmark 三种可复现入口。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init-db", "serve", "benchmark"))
    parser.add_argument("--database")
    parser.add_argument("--base-port", type=int)
    parser.add_argument("--catalog-size", type=int, default=2000)
    parser.add_argument("--stock", type=int, default=1000000)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    args.base_port = args.base_port or (18090 if args.action == "benchmark" else 8080)
    if args.action == "benchmark":
        benchmark(args)
        return
    database = Database(args.database)
    database.initialize(catalog_size=args.catalog_size, stock=args.stock)
    if args.action == "init-db":
        print(f"Initialized {database.name}; existing stock and transactions preserved")
        return
    if not args.skip_build:
        build()
    cluster = Cluster(database, ROOT / "target" / "runtime", args.base_port)
    try:
        cluster.start()
        print(f"All services ready: {cluster.base_url}; Ctrl+C stops this cluster", flush=True)
        while True:
            require(all(process.poll() is None for process in cluster.processes.values()),
                    "A service exited; see target/runtime/*.log")
            time.sleep(1)
    finally:
        cluster.stop()


# 功能：将 SIGTERM 转成普通退出流程，确保 finally 能清理本脚本启动的进程。
if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        main()
    except KeyboardInterrupt:
        print("Services stopped.")
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
