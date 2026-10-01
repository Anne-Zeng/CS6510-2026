# Self Checkout — Pipeline Architecture

## Submitted reports

- [Default: 10 stations, 60 seconds](reports/run-20261001-025927/report-default.json)
- [Stress: 100 stations, 120 seconds](reports/run-20261001-025927/report-stress.json)

Only these two selected reports are included in this submission. Other local
benchmark and recovery outputs are excluded from version control.


This version keeps checkout, inventory, HTTP contracts and atomic scan recording
from the layered server. Only windowed analytics is refactored into pipes and filters.
It is still one Spring Boot application backed by PostgreSQL, not three services.

## Pipeline filters and pipes

The system has a three-stage analytics pipeline:

**Window Builder → Ranker → Result Writer**

The filters run on three separate worker threads and communicate through two
bounded Java `ArrayBlockingQueue` pipes, implementing `BlockingQueue`.

```text
HTTP scan → CheckoutFacade @Transactional
               ├─ TransactionService: update basket
               └─ AnalyticsService: persist scan event
                              │ atomic PostgreSQL commit
                              ▼
                   committed scan_events
                              │
          [WindowBuilderFilter: worker 1]
                              │ WindowBatch
                      ArrayBlockingQueue
                              │
               [RankerFilter: worker 2]
                              │ RankedWindow
                      ArrayBlockingQueue
                              │
            [ResultWriterFilter: worker 3]
                              │ AnalyticsWindowStore @Transactional
                              ▼
              analytics_windows + popular_item_results
```

| Filter | Purpose | Input | Output |
|---|---|---|---|
| Window Builder | Construct every exact sliding window from committed scan events | Durable ordered scan log | Immutable WindowBatch |
| Ranker | Count scans by SKU, sort by count descending and SKU ascending, take top ten | WindowBatch | Immutable RankedWindow |
| Result Writer | Atomically persist window metadata and ranking rows | RankedWindow | Saved database results |

Windows contain up to 1,000 scans and advance every 500: 1–500, 1–1000,
501–1500, etc. The initial 500-scan window is partial, matching the layered version.
Polling checks for work; it does not define window boundaries.

## Source map

All Java files are under `src/main/java/edu/cs6510/pipeline_server/`.

- `PipelineServerApplication.java`: Spring Boot entry point.
- `api/CheckoutFacade.java`: coordinates atomic basket/event writes.
- `analytics/AnalyticsService.java`: API-facing scan recording and saved-ranking reads.
- `analytics/PipelineData.java`: immutable message records and window constants.
- `analytics/WindowBuilderFilter.java`: reads and validates an exact scan range.
- `analytics/RankerFilter.java`: pure counting and ranking logic, with no SQL.
- `analytics/ResultWriterFilter.java`: sends results to the transactional store.
- `analytics/AnalyticsWindowStore.java`: atomic, idempotent window/ranking persistence.
- `analytics/AnalyticsPipeline.java`: wires queues, starts/stops workers and retries failures.
- `persistence/ScanEventStore.java`: SQL for durable, contiguous scan-event ordering.
- `transactions/`: existing checkout, catalog and inventory business rules.

## Correctness and lifecycle

- Invalid scans never enter the source log. Basket and event writes share the
  facade's database transaction; event failure rolls back the basket update.
- Each pipe defaults to capacity 32. Blocking `put` provides backpressure: a slow
  writer fills the pipes and eventually pauses window construction without drops.
- Each stage has one consumer, preserving FIFO order. If processing fails, the
  stage retains its current input and retries before processing the next window.
- Window and ranking rows commit together. A saved endpoint is the durable
  checkpoint. Duplicate saves of an existing endpoint are idempotent.
- The builder's enqueue cursor is only in memory, not a persistence checkpoint.
  After restart it starts at the last saved endpoint plus 500 and reconstructs
  lost queue contents from the durable event log.
- Workers start on ApplicationReadyEvent, after seeding. Shutdown interrupts
  queue waits and stops workers; unsaved work is replayed on the next startup.

Queue messages are bounded, but the durable scan log grows while the application
runs. Retention/archiving and coordinated multi-instance workers are outside this
assignment. A permanently invalid input stalls its stage rather than silently
skipping a window; warnings report the failure.

## Step 1 — Open this folder

Open `pipeline-server` in VS Code, or open a terminal in that folder. Keep it
beside `load-client` because the benchmark compiles the unchanged client source
from `../load-client/src`.

## Step 2 — Check prerequisites

Java 21+, PostgreSQL, Maven wrapper (included), Python 3, `psql`, `curl`, `lsof`,
and a running PostgreSQL server are required. Redis is not required.

```bash
java -version
psql --version
python3 --version
```

Defaults are PostgreSQL at localhost:5432 with user/password `postgres`.
Set `CHECKOUT_DB_USER` and `CHECKOUT_DB_PASSWORD` if your local setup differs.

## Step 3 — Build and run regression tests

```bash
./mvnw clean package
```

This produces `target/pipeline-server-0.0.1-SNAPSHOT.jar` and runs six regression
tests. The retry test intentionally logs an injected write failure; the Maven
test summary determines success.

## Step 4 — Run the required default and stress workloads

```bash
BENCHMARK_PORT=18085 ./scripts/run-benchmarks.sh
```

The script builds/tests automatically, creates its dedicated test database if
needed, seeds fresh data, launches the packaged JAR, runs both workloads, waits
for all due windows, checks restart persistence, stops the server and audits data.
Do not start a separate server for this command.

| Mode | Stations | Duration | Initial stock per SKU |
|---|---:|---:|---:|
| default | 10 | 60 seconds | 1,000,000 |
| stress | 100 | 120 seconds | 1,000,000 |

The script resets only names beginning `pipeline_acceptance`; it does not reset
monolith or layered databases. Catalog size is 2,000 SKUs. The large stock fixture
avoids stock exhaustion; insufficient-stock requests are still rejected.

The independent audit checks client/database counts, all basket amounts and prices,
every SKU's inventory conservation, contiguous scan events, every expected window,
and exact top-ten results reconstructed from the event log.

Reports appear under `reports/run-YYYYMMDD-HHMMSS/`:

```text
report-default.json   # submit this: default workload
report-stress.json    # submit this: stress workload
default/correctness.json
stress/correctness.json
```

Both correctness files must say `"passed": true` and `"requestErrors": 0`.

## Step 5 — Test failure and restart recovery

```bash
python3 scripts/test-recovery.py
```

This uses a separate `pipeline_acceptance_recovery` database and port 18086.
It tests 100 concurrent stations, scan responses and receipts; deliberately blocks
ranking writes; restarts; verifies backlog recovery and exact rankings; and injects
an event-write failure to check basket rollback. Pipes are set to capacity one.
The unit tests additionally force a backlog larger than both bounded pipes.

## Step 6 — Run the server manually (optional)

```bash
createdb -h localhost -U postgres pipelinedb
./run.sh --server.port=8082
```

Create the database only once. This is separate from automated benchmark operation.

Configurable pipeline properties:

```properties
analytics.pipeline.queue-capacity=32
analytics.pipeline.poll-ms=25
analytics.pipeline.retry-ms=250
```

## Step 7 — Submission and comparison

Submit the repository URL containing this folder and the two successful workload
reports. The filter description at the top can be used in your submission.

For a fair comparison with the layered version, keep Java/JVM settings, initial
stock, workload, hardware and background processes consistent. Both versions use
a packaged JAR, product cache, synchronous PostgreSQL commits and the same stock
fixture. A pipeline is not automatically faster: queues add overhead, and database
work or scan sequencing may remain the bottleneck.
