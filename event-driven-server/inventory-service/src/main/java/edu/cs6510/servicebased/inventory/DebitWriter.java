package edu.cs6510.servicebased.inventory;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

/** 功能：库存扣减的单一写入线程。同时到达的购物篮合并为一条自动提交 SQL：热点库存行每批只锁一次，
 * 一次 WAL 刷盘确认整批，避免每个购物篮都在热点行上等待前一个提交刷盘。空闲时单个请求立即执行，不额外等待。
 * 每个购物篮仍独立成功或失败：批内任一购物篮违反约束时整条语句回滚，然后逐个重做，只让出错的购物篮返回 409。 */
@Component
public class DebitWriter {
    private static final int MAX_BATCH = 64;
    private static final long WAIT_SECONDS = 5;
    // 功能：一条 SQL 插入 CONFIRMED 记录、按 SKU 顺序锁行、扣减合计数量并保存明细；语句结束即提交并释放行锁。
    // 库存不足违反 CHECK(current_stock>=0)，未知 SKU 违反明细外键；已存在的 ID 不会再次扣减。
    private static final String DEBIT = """
            WITH requested AS (
             SELECT * FROM unnest(?::varchar[],?::varchar[],?::integer[]) AS r(transaction_id,sku,quantity)
            ), created AS (
             INSERT INTO reservations(transaction_id,status)
             SELECT DISTINCT transaction_id,'CONFIRMED' FROM requested ON CONFLICT DO NOTHING RETURNING transaction_id
            ), wanted AS MATERIALIZED (
             SELECT r.sku,sum(r.quantity)::integer AS quantity FROM requested r JOIN created c USING(transaction_id) GROUP BY r.sku
            ), locked AS MATERIALIZED (
             SELECT stock.sku,wanted.quantity FROM inventory stock JOIN wanted USING(sku) ORDER BY stock.sku FOR UPDATE OF stock
            ), debited AS (
             UPDATE inventory stock SET current_stock=stock.current_stock-locked.quantity
             FROM locked WHERE stock.sku=locked.sku RETURNING stock.sku
            ), saved AS (
             INSERT INTO reservation_items(transaction_id,sku,quantity)
             SELECT r.transaction_id,r.sku,r.quantity FROM requested r JOIN created c USING(transaction_id) RETURNING sku
            ) SELECT c.transaction_id,(SELECT count(*) FROM wanted) AS wanted,(SELECT count(*) FROM debited) AS debited
            FROM created c
            """;

    /** 功能：一个待扣减购物篮；lines 已按 SKU 合并。 */
    record Basket(String id, SortedMap<String, Integer> lines) {}

    private record Pending(Basket basket, CompletableFuture<Boolean> created) {}

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DebitWriter.class);
    private final JdbcTemplate jdbc;
    private final boolean performanceLog;
    private final BlockingQueue<Pending> queue = new LinkedBlockingQueue<>();
    private final Thread worker;
    private volatile boolean running = true;
    private long batches;
    private long baskets;
    private long writeNanos;

    public DebitWriter(JdbcTemplate jdbc, @Value("${inventory.performance-log:false}") boolean performanceLog) {
        this.jdbc = jdbc;
        this.performanceLog = performanceLog;
        worker = Thread.ofPlatform().name("inventory-debit-writer").daemon().start(this::run);
    }

    /** 功能：等待所在批次提交；true 表示新建并扣减成功，false 表示该 ID 已存在，交给调用方做幂等核对。 */
    boolean debit(String id, SortedMap<String, Integer> lines) {
        var pending = new Pending(new Basket(id, lines), new CompletableFuture<>());
        if (!running) throw new ResponseStatusException(SERVICE_UNAVAILABLE, "Inventory stopping; retry the same operation");
        queue.add(pending);
        try {
            return pending.created().get(WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // 功能：结果未知；该批稍后仍可能提交，重试同一 ID 时由幂等核对返回已有结果。
            throw new ResponseStatusException(SERVICE_UNAVAILABLE, "Inventory busy; retry the same operation");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(SERVICE_UNAVAILABLE, "Inventory interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException failure) throw failure;
            throw new IllegalStateException(e.getCause());
        }
    }

    /** 功能：取出当前全部排队请求组成一批；同一 ID 在一批中只出现一次，重复请求留到下一批走幂等核对。 */
    private void run() {
        List<Pending> waiting = new ArrayList<>();
        while (running) {
            try {
                if (waiting.isEmpty()) waiting.add(queue.take());
                queue.drainTo(waiting, MAX_BATCH - waiting.size());
            } catch (InterruptedException e) {
                break;
            }
            Set<String> ids = new HashSet<>();
            List<Pending> batch = new ArrayList<>(waiting.size());
            List<Pending> later = new ArrayList<>();
            for (Pending pending : waiting) (ids.add(pending.basket().id()) ? batch : later).add(pending);
            long started = System.nanoTime();
            write(batch);
            if (performanceLog) record(batch.size(), System.nanoTime() - started);
            waiting = later;
        }
        var failure = new ResponseStatusException(SERVICE_UNAVAILABLE, "Inventory stopping; retry the same operation");
        waiting.forEach(pending -> pending.created().completeExceptionally(failure));
        queue.forEach(pending -> pending.created().completeExceptionally(failure));
    }

    private void write(List<Pending> batch) {
        try {
            Set<String> created = debitAll(batch.stream().map(Pending::basket).toList());
            batch.forEach(pending -> pending.created().complete(created.contains(pending.basket().id())));
        } catch (DataIntegrityViolationException e) {
            if (batch.size() == 1) {
                batch.getFirst().created().completeExceptionally(translate(e));
                return;
            }
            // 功能：整批已回滚；逐个重做，只有违反约束的购物篮失败。
            for (Pending pending : batch) {
                try {
                    pending.created().complete(!debitAll(List.of(pending.basket())).isEmpty());
                } catch (RuntimeException failure) {
                    pending.created().completeExceptionally(translate(failure));
                }
            }
        } catch (RuntimeException e) {
            batch.forEach(pending -> pending.created().completeExceptionally(e));
        }
    }

    /** 功能：执行一条扣减 SQL，返回本次新建的 ID；成功时每个 SKU 必须恰好扣减一次。 */
    private Set<String> debitAll(List<Basket> baskets) {
        var ids = new ArrayList<String>();
        var skus = new ArrayList<String>();
        var quantities = new ArrayList<Integer>();
        for (Basket basket : baskets) {
            basket.lines().forEach((sku, quantity) -> {
                ids.add(basket.id());
                skus.add(sku);
                quantities.add(quantity);
            });
        }
        Set<String> created = new HashSet<>();
        jdbc.query(connection -> {
            var statement = connection.prepareStatement(DEBIT);
            statement.setArray(1, connection.createArrayOf("varchar", ids.toArray()));
            statement.setArray(2, connection.createArrayOf("varchar", skus.toArray()));
            statement.setArray(3, connection.createArrayOf("integer", quantities.toArray()));
            return statement;
        }, row -> {
            if (row.getLong("wanted") != row.getLong("debited")) throw new IllegalStateException("Incomplete inventory debit");
            created.add(row.getString("transaction_id"));
        });
        return created;
    }

    /** 功能：诊断日志：平均批量与每批写入耗时，只由写入线程更新。 */
    private void record(int size, long nanos) {
        batches++;
        baskets += size;
        writeNanos += nanos;
        if (batches % 200 == 0) {
            log.info("Inventory debit batches={} meanBaskets={} meanWriteMs={}", batches,
                    (double) baskets / batches, writeNanos / 1e6 / batches);
        }
    }

    /** 功能：23514 表示扣减后库存为负，23503 表示明细引用了不存在的 SKU；其他错误仍按内部错误处理。 */
    private static RuntimeException translate(RuntimeException failure) {
        if (failure instanceof DataIntegrityViolationException integrity
                && integrity.getMostSpecificCause() instanceof SQLException cause
                && ("23514".equals(cause.getSQLState()) || "23503".equals(cause.getSQLState()))) {
            return new ResponseStatusException(CONFLICT, "Insufficient stock or unknown SKU");
        }
        return failure;
    }

    /** 功能：关闭时停止写入线程；尚未执行的请求返回 503，由 Checkout 的持久化 Saga 重试。 */
    @PreDestroy
    public void stop() throws InterruptedException {
        running = false;
        worker.interrupt();
        worker.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
    }
}
