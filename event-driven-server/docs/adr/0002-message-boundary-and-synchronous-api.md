# ADR 0002 — Preserve the synchronous API across a Kafka boundary

- Status: Accepted
- Scope: All production inter-service business traffic

## Context

The unchanged Load Client and shared OpenAPI contract require synchronous HTTP responses. The assignment requires no direct calls between microservices and retains Catalog, Checkout, Inventory, Analytics and Gateway.

## Decision

Keep HTTP only at the public business boundary between Load Client and Gateway. Gateway uses `MessageBus.request` for every business route and returns the target service's status/body after the correlated Kafka reply. Checkout also uses the message boundary for Catalog and Inventory; scans publish domain events independently. The messaging interface exposes the capabilities currently used, not a universal broker abstraction.

Keep Checkout as the durable Saga orchestrator. A successful completion response requires persisted inventory success and local transaction completion. Analytics queries include a Checkout scan checkpoint and wait within a bounded budget for ingestion and due windows. Retained HTTP controllers and local `/health` probes serve diagnostics/compatibility; they are not the production service transport.

## Consequences

The client remains unchanged, and message infrastructure stays outside domain operations. HTTP requests still wait for business outcomes; broker acknowledgement alone never produces completion success. Missing replies can leave unknown outcomes, so stable Saga IDs and recovery are required. Kafka commands/queries are RPC-style messages; `ItemScanned` is the asynchronous domain event. The design makes no pure-choreography claim.
