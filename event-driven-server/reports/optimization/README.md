# CANDIDATE — NOT SUBMISSION

The transaction/TPS requirement is **not achieved**. The existing formal `reports/submission/` reports remain unchanged at 426 / 1,501 transactions. These files preserve interim measurements for review and must not be represented as a successful assignment submission.

## Complete candidate pair

Source: `reports/load-20261027-034855`, October 27, 2026, America/Los_Angeles. The two report JSON files are byte-for-byte copies of the original Load Client outputs. Correctness/environment/deployment/comparison JSON files are also copied unchanged.

| Workload | Candidate transactions | Candidate TPS | Required Microservices transactions | Required TPS | Audit |
| --- | ---: | ---: | ---: | ---: | --- |
| Default | 5,120 | 85.18933283466515 | 11,031 | 183.7414278303277 | 17/17 passed |
| Stress | 15,112 | 125.22416657247872 | 31,431 | 261.68575815703423 | 17/17 passed |

Both workloads have zero request errors. Their artifact hashes identify an earlier optimized batch implementation **before the final batch SQL join simplification**. They are valid measurements of that earlier candidate and do not establish the throughput of the current final artifacts.

`provenance.json` records source paths, byte sizes and SHA-256 checksums for all preserved JSON files. `environment.json` records the candidate JAR/client hashes and actual configuration. `comparison.json` separately identifies the Microservices baseline and older user-provided Service-Based reference.

## Later current-source Default only

`latest-default/` preserves the audited Default from `reports/load-20261027-035732`: 4,935 transactions / 82.14600692026426 TPS, zero request errors and 17/17 audits passed. This run includes the later SQL correction and a temporary `C` database-locale experiment; its own environment/JAR hashes apply. The matching Stress attempt was invalidated when host disk exhaustion stopped PostgreSQL/Kafka. It has no valid report/audit and is excluded.

Current-source verification passed 113 tests, including 13 real-PostgreSQL tests. Updated real-Kafka recovery integration remains pending. Further load/integration work requires at least 4 GiB free disk space. See [PERFORMANCE.md](../../PERFORMANCE.md) for findings, limitations and next checks.
