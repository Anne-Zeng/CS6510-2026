CREATE TABLE IF NOT EXISTS sale_outbox (
 transaction_id varchar(36) PRIMARY KEY REFERENCES checkout_transactions(transaction_id),
 payload jsonb NOT NULL,
 processed boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sale_outbox_pending ON sale_outbox(created_at) WHERE processed=false;
ALTER TABLE sale_outbox ADD COLUMN IF NOT EXISTS publish_started_at timestamptz;
ALTER TABLE sale_outbox ADD COLUMN IF NOT EXISTS broker_acked_at timestamptz;
