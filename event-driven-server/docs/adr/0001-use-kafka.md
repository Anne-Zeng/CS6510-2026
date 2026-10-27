# ADR 0001 — Use Kafka for inter-service communication

- Status: Accepted
- Scope: Event Based Architecture assignment

## Context

The assignment requires replacing direct microservice calls with broker-mediated communication while retaining the existing service boundaries. The user selected Kafka. The project needs command/query request-reply and asynchronous scan propagation.

## Decision

Use Apache Kafka through the native Kafka Java clients, hidden behind the shared `MessageBus` interface and `KafkaBus` adapter in `event-messaging`. Use per-business-service request topics, per-requester reply topics and a separate scan-event topic. Keep infrastructure addresses, topic prefix, request timeout, partition count and consumer concurrency configurable. `event-messaging` is a library, not another deployable service.

For reproducible local work, Compose runs one KRaft broker with replication factor 1 and a persistent volume. RabbitMQ or ActiveMQ could meet broker-mediated communication requirements; adopting either would change the transport implementation and operational model. We follow the explicit Kafka choice without adding speculative adapters for unused brokers.

## Consequences

Kafka supplies retained records, consumer groups and partition ordering. Correlated request/reply requires application code and adds broker hops to synchronous operations. Database/Kafka atomicity, duplicate handling, bounded waiting and Saga recovery remain application responsibilities. The local single-broker setup provides no high-availability guarantee; broker settings and partitions must be recorded with performance evidence.
