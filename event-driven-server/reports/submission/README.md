# Verified Kafka submission evidence

Source run: `reports/load-20261026-225303` (October 26, 2026, America/Los_Angeles).

- `report-default.json`: original client JSON, 10 stations / 60 seconds.
- `report-stress.json`: original client JSON, 100 stations / 120 seconds.
- Both reports have zero request errors and pass all 17 independent checks in `correctness-default.json` and `correctness-stress.json`.
- `environment.json` records namespaces, settings, source/schema hashes and the hashes of the actual measured JARs.
- `validation.json` records 81 passed component tests plus the separately executed real-broker recovery suite.
- Architecture: [standalone SVG](../../docs/architecture.svg) and [message flows](../../docs/architecture.md).

Raw JSON copies were checked byte-for-byte before publication. Temporary databases have been removed; service logs and raw timestamped runs remain local. These are local single-node measurements, not production capacity claims.
