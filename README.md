# Self-Checkout System — Semester Project

A single API contract, implemented by students under a different
architecture style each week (monolith → layered → service-based →
orchestration-driven SOA / pipeline → microservices → event-driven, or
whichever subset of weeks you choose), tested every week by the **same**
unmodified load-testing client. Because the client and the contract never
change, differences students observe week to week come entirely from the
architecture, not from a different test tool.

## What's in this folder

```
self-checkout-project/
├── spec/
│   └── self-checkout-openapi.yaml   ← the shared API contract (OpenAPI 3.0.3)
├── load-client/
│   ├── src/*.java                   ← the load-testing client (zero dependencies)
│   ├── build.sh
│   └── run.sh
└── mockserver/
    ├── MockServer.java              ← optional reference server (see below)
    ├── build.sh
    └── run.sh
```

## The API contract (`spec/self-checkout-openapi.yaml`)

This is what every weekly implementation must satisfy, regardless of its
internal architecture:

| Endpoint | Purpose |
|---|---|
| `GET /items` | Full catalog (SKU, name, price) — the client fetches this once at startup |
| `POST /transactions` | Start a transaction at a station |
| `POST /transactions/{id}/items` | Scan one unit of an item into the basket |
| `POST /transactions/{id}/complete` | Pay, decrement stock, return a receipt |
| `GET /transactions/{id}` | Debugging/instructor use only, not exercised by the client |
| `GET /inventory/low-stock` | Current low-stock alerts |
| `GET /analytics/popular-items` | Most-scanned items in the current sliding window |

A few deliberate design decisions worth telling students about up front:

- **Stock is decremented at *completion*, not at scan time.** The physical
  metaphor is that the customer already has the item in hand when they scan
  it — the backend's job is just to keep an accurate count, not to gate the
  scan. This is also *exactly* where the interesting concurrency bug lives:
  see "The concurrency gotcha" below.
- **Every endpoint is synchronous**, no matter what a given week's internal
  architecture does. Event-driven or orchestration-driven weeks are free to
  use events, queues, or an orchestration engine *internally*, but the
  client-facing contract never changes. This is what keeps the same load
  client valid for every week.
- **Popular items use a hopping window**: the server considers the most
  recent `windowSize` scans (spec default 1000) and recomputes every
  `slideInterval` scans (spec default 500). The response includes
  `windowStart`/`windowEnd` so students can show their work.

## The load client (`load-client/`)

### Why Java, and why zero dependencies

Written in Java (matching the rest of the course) using nothing but the
JDK's built-in `java.net.http.HttpClient` and a small hand-rolled JSON
reader/writer — no Maven, no Gradle, no third-party jars. Two reasons:

1. **One less thing to break.** Ten different student implementations are
   already enough moving parts for one semester; the shared test client
   shouldn't add a dependency-resolution failure mode on top of that.
2. **Virtual threads make naive concurrency simulation trivial.** Each
   simulated station runs on its own virtual thread
   (`Executors.newVirtualThreadPerTaskExecutor()`, Java 21+), so scaling
   from 10 stations to 200 for a stress-test run needs no async rewrite —
   it's still conceptually "one thread per station."

### Build & run

Requires a full JDK 21+ (not just a JRE) on the student's machine, since
`javac` needs to actually be present:

```bash
cd load-client
./build.sh
./run.sh --baseUrl=http://localhost:8080 --stations=10 --duration=60
```

### CLI options

| Flag | Default | Meaning |
|---|---|---|
| `--baseUrl` | `http://localhost:8080` | Base URL of the system under test |
| `--stations` | `10` | Concurrent simulated checkout stations |
| `--duration` | `60` | Test length in seconds |
| `--minItems` / `--maxItems` | `1` / `20` | Basket size range per transaction |
| `--popularLimit` | `10` | How many popular items to request at the end |
| `--requestTimeout` | `10` | Per-request timeout, seconds |
| `--verbose` | `false` | Print every completed transaction as it happens |
| `--reportDir` | `./reports` | Where the JSON report file is written |

Run `./run.sh --help` for the same, from the tool itself.

**Stress mode** is just the same client with a bigger `--stations` value —
e.g. `--stations=200 --duration=180` — useful specifically for the weeks
where scalability differences between styles are the point (microservices,
event-driven), since at the default 10-station scale most architectures
will feel instantly fast regardless of style.

### What the report shows

At the end of a run, the client prints a console report and writes a JSON
file to `--reportDir` (default `./reports`), so results from different
weeks can be diffed or charted later:

- Per-operation (`START_TRANSACTION`, `SCAN_ITEM`, `COMPLETE_TRANSACTION`):
  success/error counts, mean, p50, p95, p99, max latency, and error rate.
  **Use the percentiles, not just the mean** — tail latency is usually
  where an architecture's weaknesses (lock contention, network hops,
  orchestration overhead) actually show up.
- Overall throughput (transactions/sec, items/sec).
- Current low-stock alerts.
- Current most-popular items.

The item-popularity sampling is intentionally **not uniform random** — it
uses a Zipf-like weighting (`ItemSampler.java`) so a small number of items
get scanned disproportionately often, the same way real retail sales work.
Without this, the popular-items feature would have nothing meaningful to
detect.

### The concurrency gotcha (worth exploiting in grading)

With 10+ stations completing transactions concurrently against the same
inventory, a naive "read stock, check it, then write stock minus one" done
as two separate steps (a read call followed by a write call, or even two
non-atomic statements against a shared database row without appropriate
locking) can let two stations both succeed in buying the last unit of an
item. In the monolith and layered weeks this is easy to get right by
accident, because it's all one process talking to one local transaction. It
gets *much* easier to get wrong once inventory becomes its own service
(service-based, microservices) and the check-then-decrement happens across
a network call.

Suggested correctness check for grading, independent of any performance
number: **for every SKU, `initial_stock - final_stock` must equal the total
number of completed-transaction line items for that SKU, and final stock
must never go negative.** A student's implementation can pass every
functional test and still fail this invariant under load — that's the
point.

## The mock server (`mockserver/`) — optional, not a weekly submission

`MockServer.java` is a bare-bones, single-file reference implementation of
the contract, built the same zero-dependency way as the client (just the
JDK's built-in `com.sun.net.httpserver`). It exists for two reasons:

1. It's what this project's own client was validated against while being
   built.
2. It gives students something real to point the client at **before**
   their Week 1 monolith exists, so they can see a full report end to end
   and understand the contract by example, and gives you a known-good
   baseline to compare a confused student's server behavior against while
   debugging.

It is **not** an example of good architecture — it's a handful of
`ConcurrentHashMap`s behind an HTTP server, deliberately uninteresting.
Do not let students submit it as a week's work.

```bash
cd mockserver
./build.sh
./run.sh 8080 2000 10000 50   # port, catalogSize, stockPerItem, lowStockThreshold
```

## Suggested grading signal per week

Since the client writes a timestamped JSON report every run, a simple
rubric addition for each architecture week is to have students submit their
`reports/report-*.json` alongside their code, and compare:

- Did the correctness invariant above hold under the default 10-station
  load, and under a 100+ station stress run?
- How do p95/p99 latencies move relative to the previous week's numbers on
  the same hardware?
- Does the popular-items ranking stay stable across implementations (it
  should — it's testing the analytics feature, not the architecture)?
