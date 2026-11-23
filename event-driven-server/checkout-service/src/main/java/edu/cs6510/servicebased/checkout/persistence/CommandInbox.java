package edu.cs6510.servicebased.checkout.persistence;

import edu.cs6510.events.Reply;
import edu.cs6510.events.Responses;
import edu.cs6510.servicebased.checkout.common.BusinessException;
import edu.cs6510.servicebased.checkout.contract.CatalogItemResponse;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import static edu.cs6510.servicebased.checkout.common.BusinessException.Kind.CONFLICT;
import static edu.cs6510.servicebased.checkout.common.BusinessException.Kind.NOT_FOUND;
// 保存 Kafka 命令执行结果，使相同命令 ID 重放时返回原结果
/**
 * A command's basket writes, outbox event and replayable response are one atomic
 * PostgreSQL statement. Normal START/SCAN takes one database round trip and one
 * durable commit, without opening a transaction across several JDBC calls.
 *
 * The unique request_id is also the concurrent-redelivery fence: if two copies
 * race before either result is visible, the losing result INSERT rolls back its
 * entire statement, including basket/outbox writes. It then reads the winner's
 * committed response using a fresh READ COMMITTED snapshot.
 */
@Repository
public class CommandInbox {
    public static final int MAX_SCAN_BATCH_SIZE=64;
    public record PreparedScan(String requestId,String transactionId,CatalogItemResponse item) {
        public PreparedScan {
            Objects.requireNonNull(requestId);
            Objects.requireNonNull(transactionId);
            Objects.requireNonNull(item);
        }
    }
    private record RejectedScan(String requestId,String basketStatus) {}
    private final JdbcTemplate jdbc;

    public CommandInbox(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public Reply cached(String id) {
        var replies = jdbc.query("SELECT status,body FROM command_results WHERE request_id=?",
                (row,index) -> new Reply(row.getInt("status"),row.getString("body")),id);
        return replies.isEmpty() ? null : replies.getFirst();
    }

    @Transactional(propagation=Propagation.NOT_SUPPORTED)
    public Reply start(String requestId, String station) {
        String transactionId = UUID.randomUUID().toString();
        Timestamp started = Timestamp.from(Instant.now());
        return recoverConcurrentReplay(requestId, () -> {
            var replies = jdbc.query("""
                    WITH existing AS MATERIALIZED (
                        SELECT status,body FROM command_results WHERE request_id=?
                    ), created AS (
                        INSERT INTO checkout_transactions
                            (transaction_id,version,station_id,status,item_count,running_total,started_at)
                        SELECT ?,0,?,'OPEN',0,0.00,? WHERE NOT EXISTS(SELECT 1 FROM existing)
                        RETURNING *
                    ), recorded AS (
                        INSERT INTO command_results(request_id,status,body)
                        SELECT ?,201,json_build_object(
                            'transactionId',transaction_id,'stationId',station_id,'status',status,
                            'itemCount',item_count,'runningTotal',running_total,'startedAt',started_at)::text
                        FROM created RETURNING status,body
                    )
                    SELECT status,body FROM existing UNION ALL SELECT status,body FROM recorded
                    """, (row,index) -> new Reply(row.getInt("status"),row.getString("body")),
                    requestId,transactionId,station,started,requestId);
            if (replies.size() != 1) throw new IllegalStateException("Command did not produce a response");
            return replies.getFirst();
        });
    }

    @Transactional(propagation=Propagation.NOT_SUPPORTED)
    public Reply scan(String requestId, String transactionId, CatalogItemResponse item) {
        return recoverConcurrentReplay(requestId, () -> {
            //WITH existing检查 command_results，判断这个命令是否已经处理， locked锁定当前购物篮，并检查状态
            //recorded AS 在 Checkout 数据库的 scan_outbox 表插入记录
            //WITH ... AS (...) 块叫 CTE，属于同一条 SQL 语句，不是分别执行的多条 SQL。PostgreSQL 执行时会把 CTE 视为临时表，整个 SQL 语句在一个事务中执行，保证原子性。
            var rows = jdbc.query("""
                    WITH existing AS MATERIALIZED (
                        SELECT status,body FROM command_results WHERE request_id=?
                    ), locked AS MATERIALIZED (
                        SELECT transaction_id,status FROM checkout_transactions
                        WHERE transaction_id=? AND NOT EXISTS(SELECT 1 FROM existing) FOR UPDATE
                    ), line AS (
                        INSERT INTO transaction_items(transaction_id,sku,item_name,unit_price,quantity)
                        SELECT transaction_id,?,?,?,1 FROM locked WHERE status='OPEN'
                        ON CONFLICT(transaction_id,sku)
                        DO UPDATE SET quantity=transaction_items.quantity+1
                        RETURNING sku,item_name,unit_price
                    ), changed AS (
                        UPDATE checkout_transactions tx SET item_count=tx.item_count+1,
                            running_total=tx.running_total+line.unit_price,version=tx.version+1
                        FROM line WHERE tx.transaction_id=? RETURNING tx.item_count,tx.running_total
                    ), event AS (
                        INSERT INTO scan_outbox(sku,item_name)
                        SELECT line.sku,line.item_name FROM line CROSS JOIN changed RETURNING event_id
                    ), recorded AS (
                        INSERT INTO command_results(request_id,status,body)
                        SELECT ?,200,json_build_object(
                            'transactionId',?,'sku',line.sku,'name',line.item_name,'unitPrice',line.unit_price,
                            'itemCount',changed.item_count,'runningTotal',changed.running_total)::text
                        FROM line CROSS JOIN changed CROSS JOIN event RETURNING status,body
                    )
                    SELECT status,body,NULL::text AS basket_status FROM existing
                    UNION ALL SELECT status,body,NULL::text FROM recorded
                    UNION ALL SELECT NULL::integer,NULL::text,status FROM locked WHERE status<>'OPEN'
                    """, (row,index) -> row.getString("body") == null ? null :
                    new Reply(row.getInt("status"),row.getString("body")),
                    requestId,transactionId,item.sku(),item.name(),item.price(),transactionId,requestId,transactionId);
            if (!rows.isEmpty() && rows.getFirst() != null) return rows.getFirst();
            // A duplicate may have waited behind a concurrently completed basket;
            // its original response can be newer than this statement's snapshot.
            var replay = cached(requestId);
            if (replay != null) return replay;
            if (rows.isEmpty()) throw new BusinessException(NOT_FOUND,"TRANSACTION_NOT_FOUND","Transaction not found");
            throw new BusinessException(CONFLICT,"TRANSACTION_NOT_OPEN","Transaction is not open");
        });
    }

    /** One durable statement for independent baskets. A failed statement rolls
     * back the entire batch; ScanWriter then retries its commands individually.
     * Basket locks are taken in a stable order across Checkout replicas.
     */
    @Transactional(propagation=Propagation.NOT_SUPPORTED)
    public Map<String,Reply> scanBatch(List<PreparedScan> scans) {
        if(scans.isEmpty()) return Map.of();
        if(scans.size()>MAX_SCAN_BATCH_SIZE) throw new IllegalArgumentException("Scan batch too large");
        var requestIds=new HashSet<String>();
        var transactionIds=new HashSet<String>();
        for(var scan:scans) {
            if(!requestIds.add(scan.requestId()) || !transactionIds.add(scan.transactionId()))
                throw new IllegalArgumentException("A scan batch requires distinct request IDs and baskets");
        }
        var replies=new HashMap<String,Reply>();
        var seen=new HashSet<String>();
        var rejected=new ArrayList<RejectedScan>();
        jdbc.query(connection->{
            var statement=connection.prepareStatement("""
                    WITH requested AS MATERIALIZED (
                        SELECT * FROM unnest(?::varchar[],?::varchar[],?::varchar[],?::varchar[],?::numeric[])
                            AS r(request_id,transaction_id,sku,item_name,unit_price)
                    ), existing AS MATERIALIZED (
                        SELECT c.request_id,c.status,c.body FROM command_results c JOIN requested r USING(request_id)
                    ), candidates AS MATERIALIZED (
                        SELECT r.* FROM requested r WHERE NOT EXISTS(
                            SELECT 1 FROM existing e WHERE e.request_id=r.request_id)
                    ), locked AS MATERIALIZED (
                        SELECT r.*,tx.status FROM checkout_transactions tx
                        JOIN candidates r USING(transaction_id) ORDER BY tx.transaction_id FOR UPDATE OF tx
                    ), eligible AS MATERIALIZED (
                        SELECT request_id,transaction_id,sku,item_name,unit_price FROM locked WHERE status='OPEN'
                    ), line AS (
                        INSERT INTO transaction_items(transaction_id,sku,item_name,unit_price,quantity)
                        SELECT transaction_id,sku,item_name,unit_price,1 FROM eligible ORDER BY transaction_id
                        ON CONFLICT(transaction_id,sku) DO UPDATE SET quantity=transaction_items.quantity+1
                        RETURNING transaction_id,sku,item_name,unit_price
                    ), changed AS (
                        UPDATE checkout_transactions tx SET item_count=tx.item_count+1,
                            running_total=tx.running_total+line.unit_price,version=tx.version+1
                        FROM line JOIN eligible r USING(transaction_id) WHERE tx.transaction_id=line.transaction_id
                        RETURNING r.request_id,tx.transaction_id,line.sku,line.item_name,line.unit_price,
                            tx.item_count,tx.running_total
                    ), events AS (
                        INSERT INTO scan_outbox(sku,item_name)
                        SELECT sku,item_name FROM changed
                        RETURNING event_id
                    ), checked AS MATERIALIZED (
                        SELECT 1/(((SELECT count(*) FROM changed)=(SELECT count(*) FROM eligible)
                            AND (SELECT count(*) FROM events)=(SELECT count(*) FROM eligible))::integer) AS valid
                    ), recorded AS (
                        INSERT INTO command_results(request_id,status,body)
                        SELECT changed.request_id,200,json_build_object(
                            'transactionId',changed.transaction_id,'sku',changed.sku,'name',changed.item_name,
                            'unitPrice',changed.unit_price,'itemCount',changed.item_count,
                            'runningTotal',changed.running_total)::text
                        FROM changed CROSS JOIN checked WHERE checked.valid=1 RETURNING request_id,status,body
                    )
                    SELECT request_id,status,body,NULL::text AS basket_status FROM existing
                    UNION ALL SELECT request_id,status,body,NULL::text FROM recorded
                    UNION ALL SELECT request_id,NULL::integer,NULL::text,status FROM locked WHERE status<>'OPEN'
                    """);
            statement.setArray(1,connection.createArrayOf("varchar",scans.stream().map(PreparedScan::requestId).toArray()));
            statement.setArray(2,connection.createArrayOf("varchar",scans.stream().map(PreparedScan::transactionId).toArray()));
            statement.setArray(3,connection.createArrayOf("varchar",scans.stream().map(scan->scan.item().sku()).toArray()));
            statement.setArray(4,connection.createArrayOf("varchar",scans.stream().map(scan->scan.item().name()).toArray()));
            statement.setArray(5,connection.createArrayOf("numeric",scans.stream().map(scan->scan.item().price()).toArray()));
            return statement;
        },row->{
            String id=row.getString("request_id"),body=row.getString("body");
            seen.add(id);
            if(body!=null) replies.put(id,new Reply(row.getInt("status"),body));
            else rejected.add(new RejectedScan(id,row.getString("basket_status")));
        });
        for(var scan:scans) if(!seen.contains(scan.requestId())) rejected.add(new RejectedScan(scan.requestId(),null));
        for(var scan:rejected) {
            // Concurrent redelivery can wait behind a basket completion whose
            // command result is newer than the batch's statement snapshot.
            var replay=cached(scan.requestId());
            replies.put(scan.requestId(),replay!=null?replay:scan.basketStatus()==null
                    ?Responses.error(404,"TRANSACTION_NOT_FOUND","Transaction not found")
                    :Responses.error(409,"TRANSACTION_NOT_OPEN","Transaction is not open"));
        }
        return Map.copyOf(replies);
    }

    private Reply recoverConcurrentReplay(String requestId, Supplier<Reply> operation) {
        try { return operation.get(); }
        catch (DuplicateKeyException conflict) {
            var replay = cached(requestId);
            if (replay != null) return replay;
            throw conflict;
        }
    }
}
