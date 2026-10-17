#!/usr/bin/env python3
"""只读审计一次正式负载测试：独立检查库存、购物篮、事件和每个排名窗口。

仅依赖 Python 标准库和 psql；应在全部服务停止后运行，以取得稳定的数据快照。
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess

# 功能：此处独立声明题目沿用的窗口参数，避免用服务器算法验证服务器自身。
WINDOW_SIZE = 1_000
SLIDE_INTERVAL = 500


class Db:
    """功能：以只读数据库事务执行审计 SQL，不改变正式测试数据。"""

    def __init__(self, host, port, name, user, password):
        # 功能：通过参数数组和环境变量传递连接信息，避免 shell 插值。
        self.connection = ["-h", host, "-p", str(port), "-U", user, "-d", name]
        self.env = {**os.environ, "PGPASSWORD": password}

    def query(self, statement):
        """功能：返回按不可见分隔符拆分的原始字符串行，便于精确核对。"""
        sql = f"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n{statement}\nCOMMIT;"
        result = subprocess.run(
            ["psql", "-X", *self.connection, "-qtAF", "\x1f", "-v", "ON_ERROR_STOP=1", "-c", sql],
            capture_output=True, text=True, env=self.env, check=False,
        )
        if result.returncode:
            raise RuntimeError(f"psql failed: {result.stderr.strip()}")
        return [line.split("\x1f") for line in result.stdout.strip().splitlines() if line]

    def scalar(self, statement):
        """功能：读取 count、sum 等只返回单个值的审计结果。"""
        rows = self.query(statement)
        return rows[0][0] if rows else None


def main():
    """功能：读取客户端报告，独立重算业务不变量，输出机器可读的审计结果。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True)
    parser.add_argument("--host", default=os.environ.get("CHECKOUT_DB_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("CHECKOUT_DB_PORT", "5432")))
    parser.add_argument("--dbname", default="servicebaseddb")
    parser.add_argument("--user", default=os.environ.get("CHECKOUT_DB_USER", "postgres"))
    parser.add_argument("--password", default=os.environ.get("CHECKOUT_DB_PASSWORD", "postgres"))
    parser.add_argument("--low-stock-threshold", type=int, default=50)
    parser.add_argument("--catalog-size", type=int, default=2_000)
    parser.add_argument("--initial-stock", type=int, default=1_000_000)
    args = parser.parse_args()
    if args.catalog_size < 1 or args.initial_stock < 0:
        parser.error("catalog-size must be positive and initial-stock must be nonnegative")

    # 功能：客户端报告与数据库分别提供观测值，所有失败都集中列出而非只显示首个问题。
    report_path = Path(args.report).resolve()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    db = Db(args.host, args.port, args.dbname, args.user, args.password)
    failures = []

    def check(condition, description):
        """功能：记录一次业务不变量检查失败，继续收集其他独立证据。"""
        if not condition:
            failures.append(description)

    # 功能：核对统一种子数据，防止拿错误规模的数据集作性能比较。
    catalog_size = int(db.scalar("SELECT count(*) FROM catalog_items;"))
    inventory_size = int(db.scalar("SELECT count(*) FROM inventory;"))
    check(catalog_size == args.catalog_size, f"Catalog size is {catalog_size}, not {args.catalog_size}")
    check(inventory_size == args.catalog_size, f"Inventory size is {inventory_size}, not {args.catalog_size}")

    # 功能：每个 SKU 的实际库存变化必须恰好等于已完成交易售出的数量。
    inventory_mismatches = db.query(f"""
        WITH sold AS (
            SELECT ti.sku, SUM(ti.quantity) AS quantity
            FROM transaction_items ti
            JOIN checkout_transactions tx ON tx.transaction_id = ti.transaction_id
            WHERE tx.status = 'COMPLETED' GROUP BY ti.sku
        )
        SELECT i.sku, i.current_stock, COALESCE(s.quantity, 0)
        FROM inventory i LEFT JOIN sold s ON s.sku = i.sku
        WHERE i.current_stock < 0
           OR {args.initial_stock} - i.current_stock <> COALESCE(s.quantity, 0);
    """)
    check(not inventory_mismatches,
          f"Stock drawdown disagrees with completed sales for {len(inventory_mismatches)} SKU(s)")

    # 功能：低库存接口的空列表也要验证；读取失败不能伪装成“没有警报”。
    expected_alerts = [{"sku": sku, "name": name, "currentStock": int(stock),
                        "threshold": args.low_stock_threshold}
                       for sku, name, stock in db.query(f"""
        SELECT i.sku, c.name, i.current_stock FROM inventory i JOIN catalog_items c ON c.sku=i.sku
        WHERE i.current_stock < {args.low_stock_threshold} ORDER BY i.current_stock, i.sku;
    """)]
    low_stock_matches = report["lowStockAlerts"] == expected_alerts
    check(low_stock_matches, "Client low-stock alerts disagree with independently queried inventory")

    # 功能：独立汇总明细，核对购物篮缓存的数量和金额没有丢失并发更新。
    basket_mismatches = int(db.scalar("""
        SELECT count(*) FROM (
            SELECT tx.transaction_id
            FROM checkout_transactions tx
            LEFT JOIN transaction_items ti ON ti.transaction_id = tx.transaction_id
            GROUP BY tx.transaction_id, tx.item_count, tx.running_total
            HAVING tx.item_count <> COALESCE(SUM(ti.quantity), 0)
                OR ABS(tx.running_total - COALESCE(SUM(ti.unit_price * ti.quantity), 0)) > 0.00001
        ) AS bad;
    """))
    check(basket_mismatches == 0, f"{basket_mismatches} basket(s) disagree with their line items")
    price_mismatches = int(db.scalar("""
        SELECT count(*) FROM transaction_items ti JOIN catalog_items c ON c.sku = ti.sku
        WHERE ti.unit_price <> c.price;
    """))
    check(price_mismatches == 0, "Basket prices disagree with catalog prices")

    # 功能：把客户端成功次数与数据库最终事实逐项比较，发现漏写或重复提交。
    started = int(db.scalar("SELECT count(*) FROM checkout_transactions;"))
    completed = int(db.scalar("SELECT count(*) FROM checkout_transactions WHERE status = 'COMPLETED';"))
    scans = int(db.scalar("SELECT COALESCE(SUM(item_count), 0) FROM checkout_transactions;"))
    operations = {op["operation"]: op for op in report["operations"]}
    check(completed == report["totalTransactions"],
          f"Database has {completed} completed transactions, report claims {report['totalTransactions']}")
    check(scans == report["totalItemsScanned"],
          f"Database recorded {scans} scanned units, report claims {report['totalItemsScanned']}")
    for operation, actual in (("START_TRANSACTION", started), ("COMPLETE_TRANSACTION", completed),
                              ("SCAN_ITEM", scans)):
        check(operation in operations and actual == int(operations[operation]["successCount"]),
              f"{operation} success count disagrees with database")
    request_errors = sum(op["errorCount"] for op in report["operations"])
    check(request_errors == 0, f"Load client reported {request_errors} request errors")
    check(started == completed, "Workload left incomplete transactions")
    total_amount = db.scalar("SELECT COALESCE(SUM(running_total), 0) FROM checkout_transactions WHERE status='COMPLETED';")

    # 功能：原始 Outbox 必须保留每次成功扫描；其 ID 允许因数据库事务回滚而出现空号。
    outbox_events = int(db.scalar("SELECT count(*) FROM scan_outbox;"))
    pending_outbox_events = int(db.scalar("SELECT count(*) FROM scan_outbox WHERE NOT processed;"))
    check(outbox_events == scans,
          f"Outbox has {outbox_events} events, accepted baskets contain {scans} scanned units")
    check(pending_outbox_events == 0,
          f"Analytics left {pending_outbox_events} committed outbox event(s) unprocessed")
    basket_counts = {sku: int(quantity) for sku, quantity in db.query(
        "SELECT sku, SUM(quantity) FROM transaction_items GROUP BY sku;")}
    outbox_counts = {sku: int(quantity) for sku, quantity in db.query(
        "SELECT sku, count(*) FROM scan_outbox GROUP BY sku;")}
    check(outbox_counts == basket_counts, "Durable outbox SKU totals disagree with basket quantities")

    # 功能：分析服务生成的规范日志必须连续；每个原始事件只能对应一个且 SKU 相同的规范事件。
    events = db.query("SELECT sequence, sku FROM scan_events ORDER BY sequence;")
    check(len(events) == scans,
          f"Canonical log has {len(events)} events, accepted baskets contain {scans} scanned units")
    check([int(event[0]) for event in events] == list(range(1, scans + 1)),
          "Scan event sequence is incomplete, duplicated, or disagrees with accepted scans")
    event_counts = Counter(event[1] for event in events)
    check(event_counts == basket_counts, "Analytics SKU totals disagree with basket quantities")
    event_source_mismatches = int(db.scalar("""
        SELECT count(*) FROM scan_events e
        FULL OUTER JOIN scan_outbox o ON o.event_id = e.source_event_id
        WHERE e.sequence IS NULL OR o.event_id IS NULL
           OR e.sku IS DISTINCT FROM o.sku OR o.processed IS DISTINCT FROM true;
    """))
    # 功能：显式检测重复来源，避免只比较总数而漏掉“重复一个、遗漏另一个”的事件损坏。
    duplicate_sources = int(db.scalar("""
        SELECT COALESCE(SUM(copies - 1), 0) FROM (
            SELECT count(*) AS copies FROM scan_events
            WHERE source_event_id IS NOT NULL GROUP BY source_event_id HAVING count(*) > 1
        ) AS duplicates;
    """))
    event_source_mismatches += duplicate_sources
    check(event_source_mismatches == 0,
          f"{event_source_mismatches} canonical/outbox source mapping violation(s)")

    # 功能：直接从事件日志重建全部 500 扫描步长的窗口与前十排名，不调用服务内部算法。
    windows = db.query("""
        SELECT id, window_size, slide_interval, window_start, window_end
        FROM analytics_windows ORDER BY window_end;
    """)
    ends = [int(window[4]) for window in windows]
    expected_ends = list(range(SLIDE_INTERVAL, scans + 1, SLIDE_INTERVAL))
    check(ends == expected_ends, "Published windows do not match every exact 500-scan boundary")
    names = dict(db.query("SELECT sku, name FROM catalog_items;"))
    by_window = {}
    for window_id, sku, name, count, rank in db.query(
            "SELECT window_id, sku, name, scan_count, rank FROM popular_item_results ORDER BY window_id, rank;"):
        by_window.setdefault(window_id, []).append((sku, name, int(count), int(rank)))
    ranking_mismatches = []
    for window_id, size, slide, start, end in windows:
        size, slide, start, end = int(size), int(slide), int(start), int(end)
        expected_start = max(1, end - WINDOW_SIZE + 1)
        check((size, slide, start) == (WINDOW_SIZE, SLIDE_INTERVAL, expected_start),
              f"Incorrect metadata for window {end}")
        counts = Counter(sku for _, sku in events[expected_start - 1:end])
        ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10]
        expected = [(sku, names[sku], count, rank) for rank, (sku, count) in enumerate(ranked, 1)]
        if by_window.get(window_id, []) != expected:
            ranking_mismatches.append(end)
    check(not ranking_mismatches, f"Incorrect ranking in windows: {ranking_mismatches[:20]}")

    # 功能：客户端最后读取时可能尚未发布最新窗口，因此只要求其结果匹配一个已持久化窗口。
    reported = [(item["sku"], item["name"], item["scanCount"], item["rank"])
                for item in report["popularItems"]]
    if by_window or reported:
        check(reported in by_window.values(), "Client ranking matches no persisted window")

    # 功能：输出审计证据；退出码 1 会让自动化脚本阻止提交失败的正式报告。
    output = {
        "verifiedAt": datetime.now(timezone.utc).isoformat(),
        "report": report_path.name,
        "database": args.dbname,
        "passed": not failures,
        "catalogItems": catalog_size,
        "inventoryRowsChecked": inventory_size,
        "initialStockPerSku": args.initial_stock,
        "startedTransactions": started,
        "completedTransactions": completed,
        "unitsScanned": scans,
        "requestErrors": request_errors,
        "totalCompletedAmount": total_amount,
        "basketMismatches": basket_mismatches,
        "priceMismatches": price_mismatches,
        "scanEvents": len(events),
        "outboxEvents": outbox_events,
        "pendingOutboxEvents": pending_outbox_events,
        "eventSourceMismatches": event_source_mismatches,
        "expectedWindows": len(expected_ends),
        "windowsPublished": len(windows),
        "latestWindowEnd": ends[-1] if ends else 0,
        "rankingMismatches": ranking_mismatches,
        "inventoryMismatches": inventory_mismatches,
        "lowStockAlertsMatch": low_stock_matches,
        "failures": failures,
    }
    print(json.dumps(output, indent=2))
    return 0 if output["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
