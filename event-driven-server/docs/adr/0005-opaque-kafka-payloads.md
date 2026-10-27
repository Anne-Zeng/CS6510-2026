# ADR 0005 — Keep business JSON opaque across Kafka

- Status: Accepted
- Scope: Internal request/reply encoding in the Kafka adapter

## Decision

Store the existing business JSON directly as a Kafka record's value. Carry the
command ID, operation and reply topic in versioned Kafka headers; carry reply
correlation and HTTP status in headers on replies. The business key remains the
Kafka record key. Domain scan events keep their existing JSON representation.

This removes the outer JSON object's repeated escaping, parsing and copying of
business JSON on both legs of every request. It preserves the original public
HTTP body, status, correlation IDs and durable command deduplication rules.
Business handlers still commit before replying, and offset progress still waits
for reply publication acknowledgment.

Readers accept both the original JSON envelope and the new header format.
Replies match the incoming request's encoding, so a retained legacy command can
still receive a legacy reply. Reject missing, duplicate or unsupported metadata
instead of guessing a command identity or returning success.

`KAFKA_HEADER_ENVELOPES=true` selects the new outgoing request format. For a staged
upgrade of an existing deployment, first set it to `false` and upgrade every
consumer; enable it only after all command consumers can read the new format.
New header requests cannot be understood by an older consumer. Switching the
flag back does not remove already retained header requests.

## Validation

Tests cover exact opaque Unicode/escaped JSON and monetary text, independent
command/business identities, legacy requests/replies, malformed metadata and
invalid reply status. Real Kafka outage/replay/restart checks and the original
client with independent database audits verify the complete deployment.
Performance gains must be measured; this encoding change alone does not prove
that the historical transaction/TPS targets have been met.
