# Analytics — Kafka scan consumer

Consumes `<prefix>.scans` in the durable `analytics-scans` consumer group. Each batch enters the PostgreSQL inbox and canonical scan log atomically; event UUID deduplication makes replay safe. Kafka offsets advance only after the callback commits.

The existing pipeline publishes hopping windows of 1000 scans every 500 scans. `GET_POPULAR_ITEMS` messages include Checkout’s `minimumScans` checkpoint: the query waits up to `ANALYTICS_QUERY_CATCH_UP_MS` (default 6000 ms) for ingestion and due ranking windows, then returns a result or 503.

There are no outbound service HTTP calls. Local HTTP endpoints remain for health/diagnostics; normal application traffic uses Kafka through Gateway.

See [architecture](../docs/architecture.md), [setup](../README.md), and `python3 scripts/test-events.py` from the project root.
