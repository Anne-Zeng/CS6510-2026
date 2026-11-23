-- Kafka command identity is separate from transaction identity: two intentional
-- scans have different request IDs; redelivery repeats the same request ID.
BEGIN;
CREATE TABLE IF NOT EXISTS command_results (
 request_id varchar(64) PRIMARY KEY,
 status integer NOT NULL CHECK (status BETWEEN 100 AND 599),
 body text NOT NULL
);
COMMIT;
