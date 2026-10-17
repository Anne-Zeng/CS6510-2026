#!/usr/bin/env python3
"""在专用 PostgreSQL 数据库上验证真实四进程系统的事务、故障隔离和恢复。

运行前先构建项目；本脚本仅重建 servicebased_acceptance 前缀的测试数据库。
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import threading
import time
import traceback
import urllib.error
import urllib.request

from system import Cluster, Database, ROOT


def require(condition, message):
    """功能：显式抛出检查错误；即使 Python 使用 -O，也不会跳过验收条件。"""
    if not condition:
        raise AssertionError(message)


def quoted(value):
    """功能：将测试生成的字符串安全转换为 SQL 字面量。"""
    return "'" + str(value).replace("'", "''") + "'"


def request(base_url, path, data=None):
    """功能：发送真实 HTTP 请求，正常和错误响应均保留状态码及 JSON 内容。"""
    payload = None if data is None else json.dumps(data).encode("utf-8")
    req = urllib.request.Request(
        base_url + path, data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        response = urllib.request.urlopen(req, timeout=60)
    except urllib.error.HTTPError as error:
        # 功能：业务错误是需要验证的响应，不能被当作连接失败丢弃。
        response = error
    with response:
        raw = response.read().decode("utf-8")
        try:
            body = json.loads(raw, parse_float=Decimal) if raw else None
        except json.JSONDecodeError:
            body = raw
        return response.status, body


def until(check, description, timeout=60):
    """功能：在有限时间内等待异步分析追上日志，失败时保留最后一次连接错误。"""
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, ValueError) as error:
            last_error = error
        time.sleep(0.2)
    raise AssertionError(f"Timed out: {description}; last error: {last_error}")


class Acceptance:
    """功能：通过公共 HTTP API 触发业务，并用独立 SQL 核对数据库事实。"""

    def __init__(self, database, cluster, base_port):
        self.db = database
        self.cluster = cluster
        self.base_url = cluster.base_url
        self.base_port = base_port
        self.prices = {}

    def api(self, path, data=None, expected=200):
        """功能：检查指定 HTTP 状态，并将响应内容交给场景断言。"""
        status, body = request(self.base_url, path, data)
        require(status == expected, f"{path}: expected HTTP {expected}, got {status}: {body}")
        return body

    def start_basket(self, name):
        """功能：创建独立测试购物篮，各场景使用不同站点名方便日志定位。"""
        return self.api("/transactions", {"stationId": "acceptance-" + name}, 201)["transactionId"]

    def scan(self, transaction_id, sku):
        """功能：通过网关扫描一件商品，确保事件写入路径也被覆盖。"""
        return self.api(f"/transactions/{transaction_id}/items", {"sku": sku})

    def concurrent_completions(self, transaction_ids):
        """功能：使用屏障同时发起完成请求，放大重复扣库存和最后库存竞争的机会。"""
        barrier = threading.Barrier(len(transaction_ids))

        def complete(transaction_id):
            """功能：各线程等到全部请求准备好再通过网关完成各自交易。"""
            barrier.wait(timeout=10)
            return request(self.base_url, f"/transactions/{transaction_id}/complete", {})

        with ThreadPoolExecutor(max_workers=len(transaction_ids)) as pool:
            return list(pool.map(complete, transaction_ids))

    def routes_and_health(self):
        """功能：确认四个独立进程存活、网关能路由全部业务 API 且保留业务错误码。"""
        for offset, name in enumerate(("gateway", "catalog", "checkout", "analytics")):
            status, body = request(f"http://localhost:{self.base_port + offset}", "/health")
            require(status == 200, f"{name} direct health failed: {status} {body}")
        direct_routes = ((1, "/items"), (2, "/inventory/low-stock"), (3, "/analytics/popular-items"))
        for offset, path in direct_routes:
            status, _ = request(f"http://localhost:{self.base_port + offset}", path)
            require(status == 200, f"Direct domain endpoint failed: {path}, HTTP {status}")
        catalog = self.api("/items")
        require(len(catalog["items"]) == 20, "Expected the isolated 20-item catalog")
        self.prices = {item["sku"]: item["price"] for item in catalog["items"]}
        self.api("/inventory/low-stock")
        self.api("/analytics/popular-items")
        self.api("/inventory/low-stock?threshold=-1", expected=400)
        self.api("/transactions/missing-transaction", expected=404)
        empty = self.start_basket("empty")
        self.api(f"/transactions/{empty}/complete", {}, 409)
        return {"independentProcesses": 4, "domainRoutes": 3, "errorStatusesPreserved": [400, 404, 409]}

    def invalid_scan_is_atomic(self):
        """功能：未知 SKU 必须返回 404，购物篮及原始 Outbox 日志均不得改变。"""
        transaction_id = self.start_basket("invalid-sku")
        before = self.db.sql("SELECT count(*) FROM scan_outbox;").strip()
        self.api(f"/transactions/{transaction_id}/items", {"sku": "SKU-DOES-NOT-EXIST"}, 404)
        basket = self.api(f"/transactions/{transaction_id}")
        require(basket["itemCount"] == 0 and basket["runningTotal"] == 0,
                "Unknown SKU modified the basket")
        require(self.db.sql("SELECT count(*) FROM scan_outbox;").strip() == before,
                "Unknown SKU created a durable outbox event")
        return {"basketUnchanged": True, "outboxUnchanged": True}

    def event_write_failure_rolls_back_basket(self):
        """功能：原始 Outbox 插入失败或被静默取消时，购物篮更新必须一起回滚。"""
        transaction_id = self.start_basket("injected-event-failure")
        before = int(self.db.sql("SELECT count(*) FROM scan_outbox;").strip())
        self.db.sql("""
            CREATE OR REPLACE FUNCTION acceptance_reject_scan() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.sku = 'SKU-000001' THEN
                    RAISE EXCEPTION 'injected acceptance event write failure';
                END IF;
                RETURN NEW;
            END; $$;
            CREATE TRIGGER acceptance_scan_failure BEFORE INSERT ON scan_outbox
            FOR EACH ROW EXECUTE FUNCTION acceptance_reject_scan();
        """)
        try:
            self.api(f"/transactions/{transaction_id}/items", {"sku": "SKU-000001"}, 500)
            basket = self.api(f"/transactions/{transaction_id}")
            require(basket["itemCount"] == 0 and basket["runningTotal"] == 0,
                    "Event insert failure left a partial basket update")
            rows = self.db.sql("SELECT count(*) FROM transaction_items WHERE transaction_id="
                               + quoted(transaction_id) + ";").strip()
            require(rows == "0", "Event insert failure left a persisted basket line")
            require(int(self.db.sql("SELECT count(*) FROM scan_outbox;").strip()) == before,
                    "Rejected scan created an outbox event")
            # 功能：触发器 RETURN NULL 时，Checkout 必须检查 RETURNING 结果并回滚整个事务。
            self.db.sql("""
                CREATE OR REPLACE FUNCTION acceptance_reject_scan() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.sku = 'SKU-000001' THEN RETURN NULL; END IF;
                    RETURN NEW;
                END; $$;
            """)
            self.api(f"/transactions/{transaction_id}/items", {"sku": "SKU-000001"}, 500)
            basket = self.api(f"/transactions/{transaction_id}")
            require(basket["itemCount"] == 0 and basket["runningTotal"] == 0,
                    "Silently cancelled event insert left a committed basket update")
            require(self.db.sql("SELECT count(*) FROM transaction_items WHERE transaction_id="
                               + quoted(transaction_id)) == "0", "Silently cancelled event left a basket line")
            require(int(self.db.sql("SELECT count(*) FROM scan_outbox;").strip()) == before,
                    "Silently cancelled scan created an outbox event")
        finally:
            # 功能：即使断言失败，也必须删除故障注入对象，避免影响后续运行。
            self.db.sql("DROP TRIGGER IF EXISTS acceptance_scan_failure ON scan_outbox; "
                        "DROP FUNCTION IF EXISTS acceptance_reject_scan();")
        scan = self.scan(transaction_id, "SKU-000001")
        require(scan["itemCount"] == 1 and scan["runningTotal"] == self.prices["SKU-000001"],
                "Successful retry produced wrong basket totals")
        self.api(f"/transactions/{transaction_id}/complete", {})
        # 功能：重试只增加一个原始事件；IDENTITY 可能跳号，不能把原始 ID 当分析窗口序号。
        require(int(self.db.sql("SELECT count(*) FROM scan_outbox;").strip()) == before + 1,
                "Successful retry did not persist exactly one outbox event")
        return {"failedWriteStatus": 500, "basketRolledBack": True,
                "silentlyCancelledInsertRolledBack": True, "retryProducedExactlyOneOutboxEvent": True}

    def inventory_completion_is_atomic(self):
        """功能：缺货或完成状态被触发器取消时，已执行的所有库存扣减都必须回滚。"""
        self.db.sql("UPDATE inventory SET current_stock=5 WHERE sku='SKU-000002'; "
                    "UPDATE inventory SET current_stock=0 WHERE sku='SKU-000003';")
        transaction_id = self.start_basket("inventory-rollback")
        self.scan(transaction_id, "SKU-000002")
        self.scan(transaction_id, "SKU-000003")
        self.api(f"/transactions/{transaction_id}/complete", {}, 409)
        stocks = self.db.sql("SELECT string_agg(sku || ':' || current_stock, ',' ORDER BY sku) "
                             "FROM inventory WHERE sku IN ('SKU-000002','SKU-000003');").strip()
        require(stocks == "SKU-000002:5,SKU-000003:0", "Failed completion partially debited inventory")
        basket = self.api(f"/transactions/{transaction_id}")
        require(basket["status"] == "OPEN" and basket["itemCount"] == 2,
                "Failed completion changed basket state")

        # 功能：补足两件商品后静默取消“完成”状态更新，专门验证库存 CTE 成功后的状态核对。
        self.db.sql("UPDATE inventory SET current_stock=5 WHERE sku='SKU-000003';")
        self.db.sql(f"""
            CREATE OR REPLACE FUNCTION acceptance_cancel_completion() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.transaction_id = {quoted(transaction_id)}
                   AND OLD.status = 'OPEN' AND NEW.status = 'COMPLETED' THEN
                    RETURN NULL;
                END IF;
                RETURN NEW;
            END; $$;
            CREATE TRIGGER acceptance_completion_failure BEFORE UPDATE ON checkout_transactions
            FOR EACH ROW EXECUTE FUNCTION acceptance_cancel_completion();
        """)
        try:
            self.api(f"/transactions/{transaction_id}/complete", {}, 500)
            stocks = self.db.sql("SELECT string_agg(sku || ':' || current_stock, ',' ORDER BY sku) "
                                 "FROM inventory WHERE sku IN ('SKU-000002','SKU-000003');").strip()
            require(stocks == "SKU-000002:5,SKU-000003:5",
                    "Silently cancelled completion left committed inventory debits")
            basket = self.api(f"/transactions/{transaction_id}")
            require(basket["status"] == "OPEN" and basket["itemCount"] == 2,
                    "Silently cancelled completion changed basket state")
            require(self.db.sql("SELECT count(*) FROM checkout_transactions WHERE transaction_id="
                               + quoted(transaction_id) + " AND completed_at IS NULL;").strip() == "1",
                    "Silently cancelled completion persisted a completion timestamp")
        finally:
            # 功能：无论响应或断言是否失败，都移除本次验收安装的完成故障触发器。
            self.db.sql("DROP TRIGGER IF EXISTS acceptance_completion_failure ON checkout_transactions; "
                        "DROP FUNCTION IF EXISTS acceptance_cancel_completion();")

        # 功能：撤除故障后原购物篮可成功重试；重复完成返回冲突，库存恰好只扣除一次。
        receipt = self.api(f"/transactions/{transaction_id}/complete", {})
        require(receipt["itemCount"] == 2
                and receipt["totalAmount"] == self.prices["SKU-000002"] + self.prices["SKU-000003"],
                "Completion retry produced an inconsistent receipt")
        require(self.api(f"/transactions/{transaction_id}")["status"] == "COMPLETED",
                "Successful completion retry did not persist the final transaction status")
        self.api(f"/transactions/{transaction_id}/complete", {}, 409)
        stocks = self.db.sql("SELECT string_agg(sku || ':' || current_stock, ',' ORDER BY sku) "
                             "FROM inventory WHERE sku IN ('SKU-000002','SKU-000003');").strip()
        require(stocks == "SKU-000002:4,SKU-000003:4",
                "Completion retry failed to debit each SKU exactly once")
        return {"earlySkuDebitRolledBack": True, "transactionRemainedOpen": True,
                "silentlyCancelledCompletionStatus": 500, "cancelledCompletionInventoryRolledBack": True,
                "completionRetryDebitedStockOnce": True}

    def same_basket_cannot_complete_twice(self):
        """功能：同一购物篮的并发完成请求必须只扣一次库存。"""
        transaction_id = self.start_basket("same-basket-race")
        for _ in range(3):
            self.scan(transaction_id, "SKU-000004")
        responses = self.concurrent_completions([transaction_id, transaction_id])
        require(sorted(status for status, _ in responses) == [200, 409],
                f"Expected one successful completion and one conflict: {responses}")
        stock = self.db.sql("SELECT current_stock FROM inventory WHERE sku='SKU-000004';").strip()
        require(stock == "997", "Same basket was debited more than once")
        receipt = next(body for status, body in responses if status == 200)
        require(receipt["itemCount"] == 3 and receipt["totalAmount"] == 3 * self.prices["SKU-000004"],
                "Receipt totals disagree with the scanned basket")
        require(sum(line["quantity"] for line in receipt["lines"]) == 3,
                "Receipt line quantities disagree with its item count")
        return {"statuses": sorted(status for status, _ in responses), "stockDebitedOnce": True}

    def competing_baskets_cannot_oversell(self):
        """功能：不同购物篮竞争最后一件商品时，只允许一个完成且库存不能为负。"""
        self.db.sql("UPDATE inventory SET current_stock=1 WHERE sku='SKU-000005';")
        transaction_ids = [self.start_basket("last-stock-" + str(number)) for number in range(2)]
        for transaction_id in transaction_ids:
            self.scan(transaction_id, "SKU-000005")
        responses = self.concurrent_completions(transaction_ids)
        require(sorted(status for status, _ in responses) == [200, 409],
                f"Last-stock competition produced unexpected statuses: {responses}")
        require(self.db.sql("SELECT current_stock FROM inventory WHERE sku='SKU-000005';").strip() == "0",
                "Last-stock competition oversold inventory")
        states = [self.api(f"/transactions/{transaction_id}")["status"] for transaction_id in transaction_ids]
        require(sorted(states) == ["COMPLETED", "OPEN"], "Last-stock conflict did not preserve losing basket")
        return {"statuses": sorted(status for status, _ in responses), "finalStock": 0}

    def concurrent_scans_keep_exact_totals(self):
        """功能：同一购物篮的并发扫描必须逐次累计，验证 SQL CTE 等锁后的更新不会丢失。"""
        transaction_id = self.start_basket("concurrent-scans")
        barrier = threading.Barrier(20)

        def scan_one(_):
            """功能：同时扫描相同 SKU，扩大交易行锁与明细 UPSERT 的竞争。"""
            barrier.wait(timeout=10)
            return request(self.base_url, f"/transactions/{transaction_id}/items", {"sku": "SKU-000011"})

        with ThreadPoolExecutor(max_workers=20) as pool:
            responses = list(pool.map(scan_one, range(20)))
        require(all(status == 200 for status, _ in responses), f"Concurrent scan failure: {responses}")
        require(sorted(body["itemCount"] for _, body in responses) == list(range(1, 21)),
                "Concurrent scans lost a summary update or returned duplicate counts")
        for _, body in responses:
            require(body["runningTotal"] == body["itemCount"] * self.prices["SKU-000011"],
                    "Concurrent scan response has an incorrect amount")
        receipt = self.api(f"/transactions/{transaction_id}/complete", {})
        require(receipt["itemCount"] == 20 and receipt["lines"][0]["quantity"] == 20,
                "Concurrent scans did not persist the exact line quantity")
        require(self.db.sql("SELECT current_stock FROM inventory WHERE sku='SKU-000011'") == "980",
                "Concurrent scans produced the wrong stock debit")
        return {"concurrentScans": 20, "exactSummaryAndLineQuantity": True}

    def scan_and_completion_are_serialized(self):
        """功能：扫描与完成竞争时，扫描要么进入最终收据，要么返回 409，不能出现半完成状态。"""
        transaction_id = self.start_basket("scan-complete-race")
        self.scan(transaction_id, "SKU-000012")
        barrier = threading.Barrier(2)

        def race(action):
            """功能：以同一屏障释放扫描和完成两个独立 HTTP 请求。"""
            barrier.wait(timeout=10)
            if action == "scan":
                return request(self.base_url, f"/transactions/{transaction_id}/items", {"sku": "SKU-000012"})
            return request(self.base_url, f"/transactions/{transaction_id}/complete", {})

        with ThreadPoolExecutor(max_workers=2) as pool:
            scan_result, completion = list(pool.map(race, ("scan", "complete")))
        require(completion[0] == 200 and scan_result[0] in (200, 409), "Unexpected scan/completion race status")
        expected = 2 if scan_result[0] == 200 else 1
        require(completion[1]["itemCount"] == expected, "Successful scan missing from final receipt")
        basket = self.api(f"/transactions/{transaction_id}")
        require(basket["status"] == "COMPLETED" and basket["itemCount"] == expected,
                "Completed basket changed outside the receipt transaction")
        require(self.db.sql("SELECT current_stock FROM inventory WHERE sku='SKU-000012'") == str(1000 - expected),
                "Scan/completion race produced inconsistent inventory")
        return {"scanStatus": scan_result[0], "receiptCountMatchesAcceptedScans": True}

    def mixed_skus_and_reversed_baskets(self):
        """功能：验证不同 SKU 的并发扫描，以及反向扫描顺序的购物篮完成时不会死锁。"""
        transaction_id = self.start_basket("mixed-concurrent-skus")
        barrier = threading.Barrier(20)

        def scan_mixed(index):
            """功能：同一交易同时扫描两种商品，验证明细与汇总不会受旧查询快照影响。"""
            barrier.wait(timeout=10)
            sku = "SKU-000013" if index % 2 == 0 else "SKU-000014"
            return request(self.base_url, f"/transactions/{transaction_id}/items", {"sku": sku})

        with ThreadPoolExecutor(max_workers=20) as pool:
            responses = list(pool.map(scan_mixed, range(20)))
        require(all(status == 200 for status, _ in responses), "Mixed-SKU concurrent scan failed")
        require(sorted(body["itemCount"] for _, body in responses) == list(range(1, 21)),
                "Mixed-SKU scans lost an item-count update")
        receipt = self.api(f"/transactions/{transaction_id}/complete", {})
        require(receipt["itemCount"] == 20 and len(receipt["lines"]) == 2,
                "Mixed-SKU receipt lost a line")
        require(all(line["quantity"] == 10 for line in receipt["lines"]), "Mixed-SKU quantity mismatch")
        require(receipt["totalAmount"] == 10 * (self.prices["SKU-000013"] + self.prices["SKU-000014"]),
                "Mixed-SKU total amount mismatch")

        # 功能：两个购物篮以相反顺序扫描相同商品，完成时仍须按统一 SKU 顺序锁库存。
        baskets = [self.start_basket("reversed-order-" + str(n)) for n in range(2)]
        for basket, skus in zip(baskets, (("SKU-000015", "SKU-000016"), ("SKU-000016", "SKU-000015"))):
            for sku in skus:
                self.scan(basket, sku)
        results = self.concurrent_completions(baskets)
        require(all(status == 200 for status, _ in results), f"Reversed baskets failed/deadlocked: {results}")
        for sku in ("SKU-000015", "SKU-000016"):
            require(self.db.sql("SELECT current_stock FROM inventory WHERE sku=" + quoted(sku)) == "998",
                    "Reversed-basket stock debit mismatch")
        return {"mixedSkuScans": 20, "oppositeOrderCompletions": 2, "exactTotalsAndStock": True}

    def analytics_recovers_without_direct_calls(self):
        """功能：分析离线时继续扫描，并验证批次提升失败、恢复及排名重启的完整数据链。"""
        self.cluster.stop_service("analytics")
        before = int(self.db.sql("SELECT count(*) FROM scan_outbox;").strip())
        require(before < 500, "Acceptance setup unexpectedly crossed the first analytics boundary")
        # 功能：停机时原始事件可能尚未全部提升，分别保存规范事件与处理标记的真实基线。
        canonical_before = int(self.db.sql("SELECT count(*) FROM scan_events;").strip())
        processed_before = int(self.db.sql("SELECT count(*) FROM scan_outbox WHERE processed;").strip())
        require(canonical_before == processed_before,
                "Canonical event count already disagrees with processed outbox count before failure injection")
        transaction_id = self.start_basket("analytics-offline")
        pattern = [6] * 5 + [7] * 4 + [8] * 3 + [9] * 2 + [10]
        count = 1500 - before
        total = Decimal(0)
        for index in range(count):
            sku = f"SKU-{pattern[index % len(pattern)]:06d}"
            total += self.prices[sku]
            scan = self.scan(transaction_id, sku)
            require(scan["itemCount"] == index + 1 and scan["runningTotal"] == total,
                    "Checkout lost a scan while Analytics was offline")
        receipt = self.api(f"/transactions/{transaction_id}/complete", {})
        require(receipt["itemCount"] == count and receipt["totalAmount"] == total,
                "Offline-analytics receipt is inconsistent")
        require(self.db.sql("SELECT count(*) FROM scan_outbox;").strip() == "1500",
                "Offline scans were not durably persisted in the outbox")
        require(int(self.db.sql("SELECT count(*) FROM scan_events;").strip()) == canonical_before,
                "Checkout changed the canonical log while Analytics was offline")
        require(int(self.db.sql("SELECT count(*) FROM scan_outbox WHERE processed;").strip()) == processed_before,
                "Outbox processing flags changed while Analytics was offline")
        require(self.db.sql("SELECT count(*) FROM analytics_windows;").strip() == "0",
                "An analytics window was produced while its only service was stopped")

        # 功能：让规范事件 INSERT 失败；日志证明分析进程确实尝试过提升，而不是尚未运行。
        failure_marker = "injected acceptance promotion failure " + transaction_id
        self.db.sql(f"""
            CREATE OR REPLACE FUNCTION acceptance_reject_promotion() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION {quoted(failure_marker)};
                RETURN NEW;
            END; $$;
            CREATE TRIGGER acceptance_promotion_failure BEFORE INSERT ON scan_events
            FOR EACH ROW EXECUTE FUNCTION acceptance_reject_promotion();
        """)
        try:
            self.cluster.start_service("analytics")
            analytics_log = self.cluster.output / "analytics.log"
            until(lambda: failure_marker in analytics_log.read_text(encoding="utf-8"),
                  "Analytics to attempt and log the injected outbox promotion failure")
            require(int(self.db.sql("SELECT count(*) FROM scan_events;").strip()) == canonical_before,
                    "Failed outbox promotion partially inserted canonical events")
            require(int(self.db.sql("SELECT count(*) FROM scan_outbox WHERE processed;").strip()) == processed_before,
                    "Failed promotion acknowledged raw events without committing their canonical events")
            require(self.db.sql("SELECT count(*) FROM scan_outbox;").strip() == "1500",
                    "Failed promotion removed durable raw events")
        finally:
            # 功能：先停分析线程，再删除注入触发器；无论失败发生在哪里都清理数据库对象。
            try:
                self.cluster.stop_service("analytics")
            finally:
                self.db.sql("DROP TRIGGER IF EXISTS acceptance_promotion_failure ON scan_events; "
                            "DROP FUNCTION IF EXISTS acceptance_reject_promotion();")

        # 功能：只重启分析服务；它从未确认的 Outbox 重放，结账与目录进程保持运行。
        self.cluster.start_service("analytics")
        until(lambda: self.api("/analytics/popular-items")["windowEnd"] == 1500,
              "Analytics to replay all committed scan events")
        ends = self.db.sql("SELECT string_agg(window_end::text,',' ORDER BY window_end) "
                           "FROM analytics_windows;").strip()
        require(ends == "500,1000,1500", f"Recovered windows are incomplete or duplicated: {ends}")
        contiguous = self.db.sql("SELECT count(*)=1500 AND min(sequence)=1 AND max(sequence)=1500 "
                                 "FROM scan_events;").strip()
        require(contiguous == "t", "Scan sequence contains a gap after rollback or recovery")
        require(self.db.sql("SELECT count(*) FROM scan_outbox WHERE NOT processed;").strip() == "0",
                "Analytics recovered windows but left unprocessed raw events")
        # 功能：双向关联检查来源、SKU 及处理标记，不能用“总数相同”掩盖事件丢失或替换。
        source_mismatches = int(self.db.sql("""
            SELECT count(*) FROM scan_events e
            FULL OUTER JOIN scan_outbox o ON o.event_id=e.source_event_id
            WHERE e.sequence IS NULL OR o.event_id IS NULL
               OR e.sku IS DISTINCT FROM o.sku OR o.processed IS DISTINCT FROM true;
        """).strip())
        require(source_mismatches == 0, f"Recovered outbox/canonical source mismatches: {source_mismatches}")
        require(self.db.sql("SELECT count(DISTINCT source_event_id) FROM scan_events;").strip() == "1500",
                "Canonical events reused or omitted an original outbox source ID")

        # 功能：用 PostgreSQL 的独立聚合和排序计算期望排名，双向 EXCEPT 发现缺失和多余行。
        mismatches = self.db.sql("""
            WITH expected AS (
                SELECT w.id AS window_id, e.sku, c.name, count(*) AS scan_count,
                    row_number() OVER (PARTITION BY w.id ORDER BY count(*) DESC, e.sku) AS rank
                FROM analytics_windows w
                JOIN scan_events e ON e.sequence BETWEEN w.window_start AND w.window_end
                JOIN catalog_items c ON c.sku=e.sku
                GROUP BY w.id, e.sku, c.name
            ), differences AS (
                (SELECT window_id,sku,name,scan_count,rank FROM expected WHERE rank<=10
                 EXCEPT SELECT window_id,sku,name,scan_count,rank FROM popular_item_results)
                UNION ALL
                (SELECT window_id,sku,name,scan_count,rank FROM popular_item_results
                 EXCEPT SELECT window_id,sku,name,scan_count,rank FROM expected WHERE rank<=10)
            ) SELECT count(*) FROM differences;
        """).strip()
        require(mismatches == "0", f"Recovered ranking rows disagree with independent SQL: {mismatches}")
        metadata_errors = self.db.sql("SELECT count(*) FROM analytics_windows WHERE "
            "window_size<>1000 OR slide_interval<>500 OR window_start<>greatest(1,window_end-999);").strip()
        require(metadata_errors == "0", "Recovered window metadata is incorrect")

        # 功能：再次重启时必须直接恢复相同的已保存排名，不能重复创建窗口。
        ranking_before = self.api("/analytics/popular-items")
        self.cluster.stop_service("analytics")
        self.cluster.start_service("analytics")
        ranking_after = self.api("/analytics/popular-items")
        require(ranking_after == ranking_before, "Restart changed a durable analytics ranking")
        require(self.db.sql("SELECT count(*) FROM analytics_windows;").strip() == "3",
                "Analytics restart duplicated persisted windows")
        return {"scansAcceptedWhileAnalyticsOffline": count, "outboxEvents": 1500,
                "pendingOutboxEvents": 0, "eventSourceMismatches": source_mismatches,
                "promotionFailureRolledBack": True, "exactWindowsRecovered": [500, 1000, 1500],
                "rankingMismatches": 0, "restartRankingPreserved": True}


def main():
    """功能：安全初始化专用数据库，运行验收并在成功或失败时都保存报告和关闭进程。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default=os.environ.get("SERVICEBASED_TEST_DB", "servicebased_acceptance_checks"))
    parser.add_argument("--base-port", type=int, default=18110)
    parser.add_argument("--output", type=Path, default=ROOT / "target" / "integration")
    args = parser.parse_args()
    require(args.database.startswith("servicebased_acceptance"),
            "Refusing to reset any database without the servicebased_acceptance prefix")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    database = Database(args.database)
    cluster = Cluster(database, output, base_port=args.base_port)
    result = {"verifiedAt": datetime.now(timezone.utc).isoformat(), "database": args.database,
              "passed": False, "checks": [], "failures": []}
    try:
        database.initialize(reset=True, catalog_size=20, stock=1000)
        cluster.start()
        suite = Acceptance(database, cluster, args.base_port)
        checks = [suite.routes_and_health, suite.invalid_scan_is_atomic,
                  suite.event_write_failure_rolls_back_basket, suite.inventory_completion_is_atomic,
                  suite.same_basket_cannot_complete_twice, suite.competing_baskets_cannot_oversell,
                  suite.concurrent_scans_keep_exact_totals, suite.scan_and_completion_are_serialized,
                  suite.mixed_skus_and_reversed_baskets,
                  suite.analytics_recovers_without_direct_calls]
        for check in checks:
            started = time.monotonic()
            details = check()
            record = {"name": check.__name__, "passed": True,
                      "durationSeconds": round(time.monotonic() - started, 3), **details}
            result["checks"].append(record)
            print(json.dumps(record), flush=True)
        result["passed"] = True
    except Exception as error:
        # 功能：保存失败原因和完整栈，方便从各进程日志定位问题；失败不会伪装成通过。
        result["failures"].append(f"{type(error).__name__}: {error}")
        traceback.print_exc()
    finally:
        try:
            cluster.stop()
        except Exception as error:
            result["passed"] = False
            result["failures"].append(f"Process cleanup failed: {error}")
        (output / "correctness.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"Acceptance report: {output / 'correctness.json'}")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
