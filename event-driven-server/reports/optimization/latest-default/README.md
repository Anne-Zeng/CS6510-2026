# Default-only evidence — incomplete paired run

Source: `reports/load-20261027-035732`. The report, audit, environment and database-deployment files are copied byte-for-byte from the source run.

The Default workload completed 4,935 transactions at 82.14600692026426 TPS with zero request errors and all 17 audits passing. It used the later batch SQL join simplification and temporary database locale `C`.

The matching Stress attempt failed during host disk exhaustion, stopping PostgreSQL and Kafka; there is no valid Stress report/audit. This directory does not establish a complete workload pair or achievement of the previous Microservices thresholds. Current real-Kafka recovery integration and a complete audited rerun remain pending after at least 4 GiB disk space is available.
