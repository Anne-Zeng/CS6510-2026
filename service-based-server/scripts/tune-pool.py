#!/usr/bin/env python3
"""功能：顺序诊断数据库连接池大小；短时结果只用于调优，不替代正式作业报告。"""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys

from system import Database, Cluster, ROOT, JAVA, wait_for_analytics


def require(condition, message):
    """功能：任意前置条件或核对失败都明确中止，避免生成假成功的调优结论。"""
    if not condition:
        raise RuntimeError(message)


def positive_integer(value):
    """功能：在启动进程前拒绝零、负数和无法解析的参数。"""
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Expected a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Expected a positive integer")
    return parsed


def pool_list(value):
    """功能：解析按给定顺序测试的连接数；禁止重复值覆盖同一个输出目录。"""
    try:
        pools = [positive_integer(part.strip()) for part in value.split(",")]
    except argparse.ArgumentTypeError as error:
        raise argparse.ArgumentTypeError("Pools must be comma-separated positive integers") from error
    if len(pools) != len(set(pools)):
        raise argparse.ArgumentTypeError("Pool sizes must not repeat")
    return pools


def write_json(path, body):
    """功能：完整写入临时文件后替换摘要，保留原始客户端报告不作修改。"""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(body, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
                         encoding="utf-8")
    temporary.replace(path)


def database_counts(database):
    """功能：在无客户端写入时读取基线；同一 SQL 快照包含交易、明细与持久化事件。"""
    result = database.sql("""
        SELECT json_build_object(
            'started', (SELECT count(*) FROM checkout_transactions),
            'completed', (SELECT count(*) FROM checkout_transactions WHERE status='COMPLETED'),
            'items', (SELECT COALESCE(sum(item_count),0) FROM checkout_transactions),
            'lineQuantity', (SELECT COALESCE(sum(quantity),0) FROM transaction_items),
            'eventMax', (SELECT COALESCE(max(sequence),0) FROM scan_events),
            'eventCount', (SELECT count(*) FROM scan_events)
        );
    """)
    counts = json.loads(result)
    require(counts["started"] == counts["completed"], "Database contains unfinished transactions")
    require(counts["eventMax"] == counts["eventCount"], "Scan event sequence is not contiguous")
    return counts


def run_client(cluster, output, stations, duration):
    """功能：执行未修改的负载客户端，并检查其日志及真实 JSON；不自行合成测量值。"""
    output.mkdir(parents=True)
    command = [JAVA, "-cp", str(ROOT / "target" / "load-client"), "Main",
               f"--baseUrl={cluster.base_url}", f"--stations={stations}", f"--duration={duration}",
               f"--reportDir={output}"]
    with (output / "client.log").open("w", encoding="utf-8") as log:
        # 功能：超时或收到中断时 subprocess.run 会清理本次客户端；外围 finally 清理服务。
        subprocess.run(command, cwd=ROOT, stdout=log, stderr=log, check=True, timeout=duration + 120)
    client_output = (output / "client.log").read_text(encoding="utf-8")
    for marker in ("Could not fetch", "not all stations finished cleanly", "Failed to write JSON report:"):
        require(marker not in client_output, f"Client warning/failure: {marker}; see {output / 'client.log'}")
    reports = list(output.glob("report-*.json"))
    require(len(reports) == 1, f"Expected exactly one original client report in {output}")
    report_path = reports[0]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    require(report["config"]["stations"] == stations, "Reported stations differ from requested stations")
    require(report["config"]["durationSecondsRequested"] == duration, "Reported duration differs from requested duration")
    require(report["config"]["baseUrl"] == cluster.base_url, "Reported base URL differs from this diagnostic cluster")
    require(report["totalTransactions"] > 0 and report["totalItemsScanned"] > 0, "Client completed no workload")
    require(math.isfinite(report["transactionsPerSecond"]) and report["transactionsPerSecond"] > 0,
            "Client reported invalid throughput")
    operations = {operation["operation"]: operation for operation in report["operations"]}
    require(len(operations) == len(report["operations"]) and set(operations) == {
        "START_TRANSACTION", "SCAN_ITEM", "COMPLETE_TRANSACTION"}, "Unexpected client operation metrics")
    require(all(operation["errorCount"] == 0 for operation in operations.values()),
            f"Client reported operation errors; see {report_path}")
    return report_path, report, operations


def verify_deltas(before, after, report, operations):
    """功能：核对测量期间的数据库增量与客户端成功数，避免把失败请求算成吞吐量。"""
    deltas = {name: after[name] - before[name] for name in before}
    transactions = report["totalTransactions"]
    items = report["totalItemsScanned"]
    for name in ("started", "completed"):
        require(deltas[name] == transactions, f"Database {name} delta disagrees with client transactions")
    for name in ("items", "lineQuantity", "eventMax", "eventCount"):
        require(deltas[name] == items, f"Database {name} delta disagrees with client scans")
    expected = {"START_TRANSACTION": transactions, "SCAN_ITEM": items, "COMPLETE_TRANSACTION": transactions}
    for operation, count in expected.items():
        require(operations[operation]["successCount"] == count,
                f"Client {operation} success count disagrees with totals")
    return deltas


def main():
    """功能：每个连接数使用全新数据和独立进程，先预热再测量；全部顺序运行。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pools", type=pool_list, default=pool_list("4,8,16"))
    parser.add_argument("--stations", type=positive_integer, default=100)
    parser.add_argument("--warmup", type=positive_integer, default=10)
    parser.add_argument("--duration", type=positive_integer, default=20)
    parser.add_argument("--base-port", type=int, default=18130)
    parser.add_argument("--database", default="servicebased_acceptance_tuning")
    parser.add_argument("--output", type=Path, default=ROOT / "target" / "pool-tuning")
    args = parser.parse_args()
    require(1024 <= args.base_port <= 65532, "Base port must allow four consecutive unprivileged ports")
    require(args.database.startswith("servicebased_acceptance"), "Tuning requires a dedicated acceptance database")
    require((ROOT / "target" / "load-client" / "Main.class").is_file(),
            "Missing compiled original load client at target/load-client/Main.class")
    for artifact in ("catalog-service", "checkout-service", "analytics-service", "gateway"):
        require((ROOT / artifact / "target" / f"{artifact}-0.0.1-SNAPSHOT.jar").is_file(),
                f"Missing {artifact} JAR; build the latest sources before tuning")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    require(not (output / "summary.json").exists()
            and not any((output / f"pool-{pool}").exists() for pool in args.pools),
            "Output already contains diagnostics; choose a fresh --output directory")

    # 功能：明确标注为短时诊断，不写入 reports/submission，也不替代 60/120 秒正式报告。
    summary = {"diagnosticOnly": True, "formalSubmissionReport": False, "passed": False,
               "startedAt": datetime.now(timezone.utc).isoformat(), "database": args.database,
               "poolSizes": args.pools, "stations": args.stations, "warmupSeconds": args.warmup,
               "measurementDurationSeconds": args.duration,
               "poolSettingScope": "CHECKOUT_DB_POOL applies to all three services; runs are sequential",
               "records": []}
    write_json(output / "summary.json", summary)
    original_pool = os.environ.get("CHECKOUT_DB_POOL")
    database = Database(args.database)
    try:
        for pool in args.pools:
            os.environ["CHECKOUT_DB_POOL"] = str(pool)
            pool_output = output / f"pool-{pool}"
            cluster = Cluster(database, pool_output, args.base_port)
            try:
                cluster.check_ports()
                print(f"Diagnostic only: pool={pool}, stations={args.stations}, "
                      f"warmup={args.warmup}s, measurement={args.duration}s", flush=True)
                # 功能：只重置本脚本专用数据库；每个连接数均使用相同 2,000 商品和初始库存。
                database.initialize(reset=True, catalog_size=2000, stock=1000000)
                cluster.start()
                initial = database_counts(database)
                _, warmup_report, warmup_operations = run_client(
                    cluster, pool_output / "warmup", args.stations, args.warmup)
                # 功能：先等待异步 outbox 提升，确保规范日志计数也包含全部已接受事件。
                wait_for_analytics(cluster, database)
                baseline = database_counts(database)
                verify_deltas(initial, baseline, warmup_report, warmup_operations)
                # 功能：预热请求全部完成后再取基线；测量仅统计第二个客户端的数据库增量。
                report_path, report, operations = run_client(
                    cluster, pool_output / "measured", args.stations, args.duration)
                wait_for_analytics(cluster, database)
                after = database_counts(database)
                deltas = verify_deltas(baseline, after, report, operations)
                wait_for_analytics(cluster, database)
                record = {"pool": pool, "stations": args.stations,
                          "measurementDurationSeconds": args.duration,
                          "wallClockSeconds": report["wallClockSeconds"],
                          "transactionsPerSecond": report["transactionsPerSecond"],
                          "totalTransactions": report["totalTransactions"],
                          "totalItemsScanned": report["totalItemsScanned"],
                          "requestErrors": sum(operation["errorCount"] for operation in operations.values()),
                          "p95ScanMs": operations["SCAN_ITEM"]["p95Ms"],
                          "validationPassed": True, "databaseBaseline": baseline,
                          "databaseAfter": after, "databaseDeltas": deltas,
                          "originalReport": str(report_path)}
            finally:
                # 功能：启动、负载或核对失败也关闭本轮四个进程；不杀其他任务的进程。
                cluster.stop()
            summary["records"].append(record)
            write_json(output / "summary.json", summary)
            print(f"pool={pool}: {report['transactionsPerSecond']:.2f} transactions/s, "
                  f"scan P95={operations['SCAN_ITEM']['p95Ms']:.2f}ms; validation passed", flush=True)
        summary["passed"] = True
    except BaseException as error:
        summary["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        # 功能：恢复调用者的连接池环境；即使中断，也把未完成诊断明确记录为失败。
        if original_pool is None:
            os.environ.pop("CHECKOUT_DB_POOL", None)
        else:
            os.environ["CHECKOUT_DB_POOL"] = original_pool
        summary["finishedAt"] = datetime.now(timezone.utc).isoformat()
        write_json(output / "summary.json", summary)
    print(f"Diagnostic summary (not a formal submission report): {output / 'summary.json'}", flush=True)


if __name__ == "__main__":
    # 功能：SIGTERM 进入 Python 清理流程，确保客户端与本轮服务都能退出。
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        main()
    except KeyboardInterrupt:
        print("Pool diagnostics interrupted; managed services stopped.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
