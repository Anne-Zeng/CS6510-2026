# Quality Attributes and Architectural Characteristics Analysis

## Architectural Characteristics

### Performance

- The system should support the default workload of 10 concurrent checkout stations for 60 seconds.
- Normal operations (`START_TRANSACTION`, `SCAN_ITEM`, and `COMPLETE_TRANSACTION`) should normally complete in less than one second.
- The system should provide acceptable p95 and p99 latency under the required 100-station stress test.

### Scalability

- The server must support at least 10 concurrent checkout stations during the normal workload.
- The server must remain functional under the 100-station, 120-second stress workload.
- The system should continue to process transactions without failed requests under concurrent load.

### Data Integrity

- Inventory must be decremented only when a transaction is completed.
- Inventory must never become negative.
- For each SKU, initial stock minus final stock must equal the quantity sold in completed transactions.
- A failed completion must not partially decrement inventory.

### Reliability and Persistence

- Catalog, inventory, transactions, transaction items, scan events, and popular-item results are persisted in SQLite.
- Data should remain available after an application restart.
- The database can be reset using `reset-db.sh`, then reseeded with 2,000 items and 10,000 units per item.

### API Correctness and Interoperability

- The server implements the shared OpenAPI contract.
- All client-facing endpoints are synchronous request/response endpoints.
- Invalid requests return appropriate HTTP status codes and an `ApiError` response.

### Analytics Correctness

- Every successful item scan is recorded.
- Popular-item analytics use the most recent 1,000 scans.
- Rankings are recalculated every 500 scans.
- The ten most popular items are persisted and returned by `/analytics/popular-items`.

### Maintainability

- The monolith is organized into catalog, inventory, transaction, analytics, and common error-handling packages.
- Controllers handle HTTP requests, services contain business logic, repositories handle database access, and entities represent persisted data.

## Top Three Priorities

### 1. Data Integrity

Data integrity is the highest priority because inventory errors would allow overselling or cause stock records to become incorrect. The implementation uses an atomic SQL update that decrements stock only when sufficient inventory remains. Transaction completion is wrapped in a database transaction, so a failure rolls back all inventory updates for that checkout.

Trade-off: SQLite uses serialized database access in this implementation. This improves correctness but increases waiting time under heavy concurrent load.

### 2. Performance

Performance is important because customers expect checkout operations to respond quickly. Under the normal 10-station test, the three core operations had mean response times of approximately 29-32 ms with zero errors.

Under the 100-station stress test, mean response times increased to approximately 344-346 ms, while all operations still completed with zero errors.

Trade-off: Prioritizing safe, transactional SQLite updates reduced scalability. The stress workload produced higher p95 and p99 latency because requests wait for database access.

### 3. Maintainability

The system is implemented as a monolith with clear package boundaries for catalog, transactions, inventory, and analytics. This makes the application easier to understand, debug, test, and evolve during later architecture assignments.

Trade-off: A monolith is easier to maintain initially, but it cannot independently scale the inventory or analytics components. A later microservices architecture could scale components independently but would add network communication and distributed-consistency complexity.

## Load-Test Summary

### Default Workload

- 10 stations for 60 seconds
- 1,626 completed transactions
- 17,299 scanned items
- 0 errors
- Approximately 27.0 transactions per second
- Approximately 287.0 items per second

### Stress Workload

- 100 stations for 120 seconds
- 2,855 completed transactions
- 29,646 scanned items
- 0 errors
- Approximately 23.1 transactions per second
- Approximately 239.4 items per second

The stress test increased latency significantly but did not produce failures or inventory errors. The popular-item ranking remained stable, with the same highly popular SKUs appearing near the top in both reports.