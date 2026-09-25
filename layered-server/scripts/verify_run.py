#!/usr/bin/env python3
"""Read-only correctness audit of a completed load run against PostgreSQL.

Uses only the standard library plus the `psql` client, so it needs no pip
installs. Each query uses a read-only snapshot; run with the server stopped for a stable audit.
"""
import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

NOMINAL_WINDOW_SIZE = 1_000
SLIDE_INTERVAL = 500


class Db:
    def __init__(self, host, port, name, user, password):
        self.dsn = ["-h", host, "-p", str(port), "-U", user, "-d", name]
        self.env = {**os.environ, "PGPASSWORD": password}

    def query(self, sql):
        """Returns a list of rows, each a list of raw string fields."""
        wrapped = f"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n{sql}\nCOMMIT;"
        # -q suppresses the BEGIN/COMMIT command tags, leaving only result rows.
        result = subprocess.run(
            ["psql", *self.dsn, "-qtAF", "\x1f", "-v", "ON_ERROR_STOP=1", "-c", wrapped],
            capture_output=True, text=True, env=self.env,
        )
        if result.returncode != 0:
            raise RuntimeError(f"psql failed: {result.stderr.strip()}")
        return [line.split("\x1f") for line in result.stdout.strip().splitlines() if line]

    def scalar(self, sql):
        rows = self.query(sql)
        return rows[0][0] if rows else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=5432, type=int)
    parser.add_argument("--dbname", default="layereddb")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="postgres")
    parser.add_argument("--catalog-size", type=int, default=2_000,
                        help="must match checkout.catalog-size")
    parser.add_argument("--initial-stock", type=int, default=1_000_000,
                        help="must match checkout.stock-per-item")
    args = parser.parse_args()
    initial_stock = args.initial_stock
    expected_catalog_size = args.catalog_size

    report_path = Path(args.report).resolve()
    report = json.loads(report_path.read_text())
    db = Db(args.host, args.port, args.dbname, args.user, args.password)
    failures = []

    def check(condition, description):
        if not condition:
            failures.append(description)

    # --- Seed data -----------------------------------------------------------
    catalog_size = int(db.scalar("SELECT count(*) FROM catalog_items;"))
    inventory_size = int(db.scalar("SELECT count(*) FROM inventory;"))
    check(catalog_size == expected_catalog_size,
          f"Catalog size is {catalog_size}, not {expected_catalog_size}")
    check(inventory_size == expected_catalog_size,
          f"Inventory size is {inventory_size}, not {expected_catalog_size}")

    # --- The transactions layer conserved stock exactly ----------------------
    # Every unit removed from inventory must correspond to a completed sale, and
    # no completed sale may be missing from inventory. This is the invariant that
    # 100 concurrent stations are most likely to break.
    mismatches = db.query(f"""
        WITH sold AS (
            SELECT ti.sku, SUM(ti.quantity) AS quantity
            FROM transaction_items ti
            JOIN checkout_transactions tx ON tx.transaction_id = ti.transaction_id
            WHERE tx.status = 'COMPLETED'
            GROUP BY ti.sku
        )
        SELECT i.sku, i.current_stock, COALESCE(s.quantity, 0)
        FROM inventory i LEFT JOIN sold s ON s.sku = i.sku
        WHERE i.current_stock < 0
           OR {initial_stock} - i.current_stock <> COALESCE(s.quantity, 0);
    """)
    check(not mismatches, f"Stock drawdown disagrees with completed sales for {len(mismatches)} SKU(s)")

    # --- Each basket's denormalised totals match its line items --------------
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

    # --- The database agrees with what the load client observed --------------
    started = int(db.scalar("SELECT count(*) FROM checkout_transactions;"))
    completed = int(db.scalar("SELECT count(*) FROM checkout_transactions WHERE status = 'COMPLETED';"))
    persisted_scans = int(db.scalar("SELECT COALESCE(SUM(item_count), 0) FROM checkout_transactions;"))
    operations = {op["operation"]: op for op in report["operations"]}

    check(completed == report["totalTransactions"],
          f"Database has {completed} completed transactions, report claims {report['totalTransactions']}")
    check(persisted_scans == report["totalItemsScanned"],
          f"Database recorded {persisted_scans} scanned units, report claims {report['totalItemsScanned']}")
    check(started == int(operations["START_TRANSACTION"]["successCount"]),
          "Start count disagrees with database")
    check(completed == int(operations["COMPLETE_TRANSACTION"]["successCount"]),
          "Complete count disagrees with database")
    check(persisted_scans == int(operations["SCAN_ITEM"]["successCount"]),
          "Scan count disagrees with database")
    check(all(op["errorCount"] == 0 for op in report["operations"]),
          "Load client reported request errors: "
          + ", ".join(f"{op['operation']}={op['errorCount']}"
                      for op in report["operations"] if op["errorCount"]))

    # Reconstruct every exact window from the durable scan log independently.
    from collections import Counter
    events = db.query("SELECT sequence, sku FROM scan_events ORDER BY sequence;")
    check([int(e[0]) for e in events] == list(range(1, persisted_scans + 1)),
          "Scan event sequence is incomplete, duplicated, or disagrees with accepted scans")
    event_counts = Counter(e[1] for e in events)
    basket_counts = {sku: int(quantity) for sku, quantity in db.query(
        "SELECT sku, SUM(quantity) FROM transaction_items GROUP BY sku;")}
    check(event_counts == basket_counts, "Analytics SKU totals disagree with basket quantities")
    price_mismatches = int(db.scalar("""
        SELECT count(*) FROM transaction_items ti JOIN catalog_items c ON c.sku = ti.sku
        WHERE ti.unit_price <> c.price;
    """))
    check(price_mismatches == 0, "Basket prices disagree with catalog prices")
    check(started == completed, "Workload left incomplete transactions")
    total_amount = db.scalar("SELECT COALESCE(SUM(running_total), 0) FROM checkout_transactions WHERE status='COMPLETED';")

    windows = db.query("""
        SELECT id, window_size, slide_interval, window_start, window_end
        FROM analytics_windows ORDER BY window_end;
    """)
    ends = [int(w[4]) for w in windows]
    expected_ends = list(range(SLIDE_INTERVAL, persisted_scans + 1, SLIDE_INTERVAL))
    check(ends == expected_ends, "Published windows do not match every exact 500-scan boundary")
    names = dict(db.query("SELECT sku, name FROM catalog_items;"))
    all_rankings = db.query(
        "SELECT window_id, sku, name, scan_count, rank FROM popular_item_results ORDER BY window_id, rank;")
    by_window = {}
    for window_id, sku, name, count, rank in all_rankings:
        by_window.setdefault(window_id, []).append((sku, name, int(count), int(rank)))
    ranking_mismatches = []
    for window_id, size, slide, start, end in windows:
        size, slide, start, end = int(size), int(slide), int(start), int(end)
        expected_start = max(1, end - NOMINAL_WINDOW_SIZE + 1)
        check((size, slide, start) == (NOMINAL_WINDOW_SIZE, SLIDE_INTERVAL, expected_start),
              f"Incorrect metadata for window {end}")
        counts = Counter(sku for _, sku in events[expected_start - 1:end])
        ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10]
        expected = [(sku, names[sku], count, rank) for rank, (sku, count) in enumerate(ranked, 1)]
        if by_window.get(window_id, []) != expected:
            ranking_mismatches.append(end)
    check(not ranking_mismatches, f"Incorrect ranking in windows: {ranking_mismatches[:20]}")

    # --- The client's ranking matches a persisted ranking --------------------
    all_rankings = db.query(
        "SELECT window_id, sku, name, scan_count, rank FROM popular_item_results "
        "ORDER BY window_id, rank;")
    by_window = {}
    for window_id, sku, name, scan_count, rank in all_rankings:
        by_window.setdefault(window_id, []).append((sku, name, int(scan_count), int(rank)))
    reported = [(i["sku"], i["name"], i["scanCount"], i["rank"]) for i in report["popularItems"]]
    # The client reads the ranking after the run finishes, by which point the
    # publisher may have produced one more window, so the newest ranking is not
    # necessarily the one the client saw. Matching any persisted window is enough.
    if by_window or reported:
        check(reported in by_window.values(), "Client ranking matches no persisted window")

    output = {
        "verifiedAt": datetime.now(timezone.utc).isoformat(),
        "report": report_path.name,
        "database": args.dbname,
        "passed": not failures,
        "catalogItems": catalog_size,
        "inventoryRowsChecked": inventory_size,
        "initialStockPerSku": initial_stock,
        "startedTransactions": started,
        "completedTransactions": completed,
        "unitsScanned": persisted_scans,
        "requestErrors": sum(op["errorCount"] for op in report["operations"]),
        "totalCompletedAmount": total_amount,
        "basketMismatches": basket_mismatches,
        "priceMismatches": price_mismatches,
        "scanEvents": len(events),
        "expectedWindows": len(expected_ends),
        "rankingMismatches": ranking_mismatches,
        "windowsPublished": len(windows),
        "latestWindowEnd": ends[-1] if ends else 0,
        "inventoryMismatches": mismatches,
        "failures": failures,
    }
    print(json.dumps(output, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
