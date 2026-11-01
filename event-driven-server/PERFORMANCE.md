# Kafka performance and correctness

The accepted run is `reports/load-20261027-141505`, October 27, 2026, America/Los_Angeles. The current artifacts, original Load Client, audits and recovery validation are recorded in [reports/submission](reports/submission/README.md).

## Accepted results

| Implementation | Default transactions / TPS | Stress transactions / TPS |
| --- | ---: | ---: |
| Current Kafka submission | **15,866 / 264.23591** | **34,625 / 286.39168** |

Both workloads have zero request errors and pass all 17 independent database audits each.

| Current workload | Actual seconds | Scans | START mean ms | SCAN mean ms | COMPLETE mean ms | COMPLETE P95 / P99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default: 10 stations / 60 requested seconds | 60.04483 | 166,198 | 2.285 | 2.786 | 6.343 | 10.122 / 17.422 |
| Stress: 100 stations / 120 requested seconds | 120.90086 | 363,959 | 5.606 | 31.428 | 12.409 | 28.613 / 47.650 |

The original client sources match the historical Microservices client SHA-256 hashes. Only its server URL and report directory change; basket sizes remain 1–20. The two submitted JSON files are byte-for-byte copies of the original outputs. Their service JAR hashes match the current files and recovery validation.

## Bottlenecks and retained changes

- The initial command inbox required several database calls per scan. START/SCAN now atomically persist business changes, outbox events and replayable command results in one statement. Successful replies still wait for the durable commit.
- A blocking command poller serialized unrelated baskets behind business work and reply publication. Polling now dispatches independent keys asynchronously, while partition progress commits only a completed prefix after reply publication acknowledgment. Duplicate IDs fence replay.
- JFR identified allocation from excessive consumers and frequent empty fetches. The adapter uses one command poller per service, `fetch.min.bytes=1` and a 500 ms empty-fetch ceiling. Existing data can satisfy a fetch immediately. The accepted single-process deployment uses one command partition per service.
- `ScanWriter` coalesces already queued independent scans, without a collection delay, into one durable statement. Batch constraint failures fall back to the original command IDs; missing or closed baskets retain individual errors. A single writer remains the default.
- Kafka request/reply values now carry the business JSON directly. Versioned headers carry correlation, operation and status metadata, removing repeated outer JSON escaping/parsing. Readers accept legacy envelopes, and replies match the incoming request format. See [ADR 0005](docs/adr/0005-opaque-kafka-payloads.md).
- macOS was observed with Low Power Mode enabled despite AC power. The user disabled it before acceptance; both workloads record AC power and `lowpowermode=0` before and after. The runner now records these settings rather than assuming comparable host conditions.

The accepted pair combines code and deployment tuning. It is not a controlled experiment isolating the speedup of each change or of the power setting. The accepted run uses four temporary PostgreSQL instances, with AC power and Low Power Mode disabled. These are local single-node measurements, not a production capacity estimate.

Experiments with two scan writers, unbatched TCP scans, PostgreSQL `wal_sync_method=fsync`, earlier tier-4 compilation and a Unix-socket database adapter were not selected for the accepted configuration. The parallel-writer and native-socket production changes were reverted. Normal PostgreSQL TCP connections, the single batch writer and the default durable WAL method are retained. Earlier candidate reports remain historical in [reports/optimization](reports/optimization/README.md).

## Accepted deployment and reproduction

The accepted run uses five separate Java processes; four test-owned PostgreSQL 15.19 instances with 32 MiB shared buffers each; and native Apache Kafka 4.2.1, single-node KRaft, at `127.0.0.1:19092`. The broker uses a 128–256 MiB heap, three network threads and four IO threads. Command/reply/scan topics each have one partition in this measured deployment; each business service has one command poller. Service JVMs use `-Xms64m -Xmx256m -XX:TieredStopAtLevel=1` for the short assignment runs.

Pools are Catalog 8, Checkout 16, Inventory 8 and Analytics 8. Checkout permits 64 outstanding Inventory requests. Scan queue capacity is 512 and maximum batch size is 64. Diagnostic stage/batch logging is off. The dataset has 2,000 SKUs and 1,000,000 initial units per SKU; temporary database locale is `C`.

PostgreSQL `fsync`, `synchronous_commit` and `full_page_writes` stay on; `commit_delay=0`. Kafka uses `acks=all`, producer idempotence and replication factor 1. Internal business communication remains Kafka-only, and the external API waits for the actual business outcome.

Run the following commands from `event-driven-server/`. With Java, `javac`, PostgreSQL tools and the native Kafka distribution available, start the broker in one terminal:

```bash
python3 scripts/kafka-local.py
```

Then run the exact accepted configuration with Low Power Mode disabled and at least 4 GiB free disk space:

```bash
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 KAFKA_TOPIC_PARTITIONS=1 \
  ./scripts/run-benchmarks.sh --broker-deployment=native --database-servers=separate \
  --java-opts='-Xms64m -Xmx256m -XX:TieredStopAtLevel=1' --publish-submission
```

The wrapper prevents idle sleep on macOS. The original Default and Stress client parameters are unchanged. The runner owns and removes its temporary databases/servers, records actual TCP connections and configuration, and stops the workload if free disk drops below 2 GiB. Publication requires both workloads to pass all audits and both historical count/TPS thresholds.

## Verification and evidence

The current implementation passed **117 Java tests, including all 13 real-PostgreSQL tests, with no skips**, plus **4 Python safety tests**. Coverage includes concurrent duplicate commands, atomic rollback, mixed batch outcomes, cross-replica replay, queue/timeout/shutdown behavior, API contracts, offset-prefix tracking and new/legacy Kafka envelope compatibility.

The final real-Kafka recovery suite also passed. It used a freshly initialized test-owned PostgreSQL instance and removed that instance and its four temporary databases afterward. It verified Analytics outage/catch-up, duplicate UUID replay, non-debiting scans, concurrent idempotent completion and Inventory outage with Checkout restart/Saga recovery. The native-socket experiment and its earlier failed attempt are not current submission evidence.

- [Default JSON](reports/submission/report-default.json) and [Stress JSON](reports/submission/report-stress.json).
- [Default audit](reports/submission/correctness-default.json) and [Stress audit](reports/submission/correctness-stress.json).
- [Environment and hashes](reports/submission/environment.json), [comparison](reports/submission/comparison.json), and [verification](reports/submission/validation.json).
- [Architecture diagram](docs/architecture.svg) and [message flows](docs/architecture.md).
