# Accepted Kafka submission evidence

Source: `reports/load-20261027-141505`, October 27, 2026, America/Los_Angeles.

| Workload | Transactions | TPS | Previous transactions / TPS | Errors / audit |
| --- | ---: | ---: | ---: | --- |
| Default: 10 stations / 60 s | 15,866 | 264.2359069035942 | 11,031 / 183.7414278303277 | 0 / 17 passed |
| Stress: 100 stations / 120 s | 34,625 | 286.3916827369939 | 31,431 / 261.68575815703423 | 0 / 17 passed |

Both count and TPS thresholds passed before publication. `report-default.json` and `report-stress.json` are byte-for-byte original client outputs. Client sources match the historical baseline; current JAR hashes match the benchmark and the recovery validation.

- `correctness-default/stress.json`: independent basket, stock, Saga, event identity/payload and analytics-window audits.
- `environment.json`: actual JVM/broker/database configuration, source/artifact hashes, AC power and Low Power Mode off before/after both workloads.
- `database-deployment-default/stress.json`: four private temporary PostgreSQL instances, durable settings and observed TCP application connections.
- `comparison.json`: exact comparisons with the previous Microservices baseline; the older Service-Based reference is separately labeled.
- `validation.json`: 117 Java tests, including 13 real-PostgreSQL tests with no skips; 4 Python tests; real Kafka outage/replay/restart recovery; and final report/hash acceptance checks.

The test-owned databases and PostgreSQL instances have been removed. Raw logs and timestamped runs remain local. The earlier submission was backed up inside the source run directory. See [PERFORMANCE.md](../../PERFORMANCE.md), [architecture SVG](../../docs/architecture.svg) and [message flows](../../docs/architecture.md).
