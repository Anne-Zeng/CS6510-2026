# Layered self-checkout server

Java 21+, Spring Boot and PostgreSQL. The HTTP contract and the professor's
load-client source are unchanged.

## Architecture

```text
API: controllers + CheckoutFacade
          /                 \
Transactions              Analytics
start / scan / complete   ScanWindow: exact windows and rankings
          \                 /
Persistence: JPA repositories, ScanEventStore (SQL), entities, CatalogCache
                            |
                        PostgreSQL
```

The API scan coordinator calls `TransactionService.scanItem` and then
`AnalyticsService.recordScan`. Neither service depends on the other. The API
scan method has a database transaction so the basket update and its immutable
analytics event commit together, or both roll back. Other checkout operations
keep their own service transaction boundaries. Inventory debits for completion
commit atomically; SKU ordering avoids deadlocks between baskets.

`PersistentScanWindow` owns window policy. `ScanEventStore` owns SQL and ordered
event storage. JPA repositories provide access to transactions, inventory,
catalog, window metadata and rankings. Window computation and publication run
on the analytics scheduler, outside the scan request.

## Exact analytics and recovery

- Each committed scan has a contiguous sequence in `scan_events`. A PostgreSQL
  transaction-scoped advisory lock serializes allocation through commit. Rolled
  back scans create neither events nor sequence gaps.
- Publish at exact endpoints 500, 1000, 1500, etc. Each window contains the last
  1000 events, or the first 500 events for the initial partial window.
- Rank by count descending, then SKU ascending for ties; persist the top ten.
- A slow publisher catches up through every due boundary; polling affects
  latency, not which windows exist.
- Window metadata and ranking rows save in one transaction. Advance the in-memory
  checkpoint only after commit. Failed writes retry the same window, and restart
  restores the checkpoint from the latest persisted window. Saving an already
  persisted endpoint is idempotent.
- PostgreSQL synchronous commit is enabled. Redis is no longer required.

The durable scan log intentionally adds database work and retains all events.
Sequence allocation serializes part of each scan; throughput is a trade-off for
exact ordering and recoverability. Event retention/archiving is future work.
This implementation is validated for the assignment's single server process.

## Running

```bash
# Use a new database when moving from the old Redis-based implementation.
createdb -h localhost -U postgres layered_exact
CHECKOUT_DB_URL=jdbc:postgresql://localhost:5432/layered_exact ./run.sh
```

Defaults: port 8080, database layereddb, user/password postgres, 2000 catalog
items and 1,000,000 units per SKU. Stock is configurable with
`CHECKOUT_STOCK_PER_ITEM`. The acceptance fixture uses 1,000,000 so the popular
SKUs cannot run out during the load test. Insufficient stock remains an error;
zero benchmark errors do not mean exhausted inventory is accepted.

Old Redis runs have no durable ordered scan log, so their historical windows
cannot be reconstructed exactly. Preserve those databases/reports and use a
fresh database for this implementation; the benchmark runner does so.

## Verification

```bash
./mvnw clean package
python3 scripts/test-recovery.py
BENCHMARK_PORT=18082 ./scripts/run-benchmarks.sh
```

The benchmark runner only resets database names beginning `layered_acceptance`.
It runs the unmodified load client with:

| Mode | Stations | Duration | Opening stock per SKU |
|---|---:|---:|---:|
| default | 10 | 60 seconds | 1,000,000 |
| stress | 100 | 120 seconds | 1,000,000 |

Each mode starts with fresh data, waits for every due analytics window, checks
that the API result survives a restart, stops the server, and runs the read-only
audit. The audit requires:

- Zero request errors, matching client/database start, completion and scan counts,
  and no unfinished transactions.
- Every basket's quantity and exact decimal total match its lines; unit prices
  match the catalog; every SKU's stock drawdown equals completed sales.
- One durable event per scan, a contiguous sequence, and matching per-SKU totals.
- Exactly every due 500-scan boundary, correct window metadata, and exact top-ten
  SKU/name/count/rank tuples independently reconstructed from the event log.
- The load client's ranking matches a persisted window.

`test-recovery.py` uses a separate database and 100 concurrent stations. It checks
1500 scan responses and 100 receipts, deliberately blocks publication, restarts
the server, and verifies all three pending windows recover with correct rankings.
It also injects an event-insert failure and verifies the basket rolls back with
no sequence gap. Expected injected errors are separate from workload error counts.

Reports are saved under `reports/run-*/` and `reports/recovery-*/`. Earlier failed
reports are retained as baseline evidence; use the latest passing run.
