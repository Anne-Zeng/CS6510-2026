# Selected submission evidence

Formal benchmark source: `reports/load-20261025-000344` (October 25, 2026, Pacific time).

- report-default.json / report-stress.json: original client measurements.
- correctness-default.json / correctness-stress.json: 17 checks per workload, both passed after Analytics drained.
- environment.json: recorded benchmark JVM, concurrency and hashes.
- database-deployment-default.json / database-deployment-stress.json: observed database durability and locale settings.
- comparison.json: both the local historical baseline and the user-provided previous submission baseline; these differ.
- validation.json: current build and isolated integration validation, recorded separately from benchmark results.

Older diagnostic and validation files may exist locally but are excluded from this submission through .gitignore. See ../../PERFORMANCE.md and ../../SUBMISSION.md for interpretation and reproduction.
