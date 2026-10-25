-- 功能：保留已有购物篮与 outbox，增加冻结状态和持久化 Saga 记录。
BEGIN;
ALTER TABLE checkout_transactions DROP CONSTRAINT IF EXISTS checkout_transactions_status_check;
ALTER TABLE checkout_transactions ADD CONSTRAINT checkout_transactions_status_check
 CHECK (status IN ('OPEN','CHECKOUT_PENDING','COMPLETED','CANCELLED'));
CREATE TABLE IF NOT EXISTS checkout_sagas (
 saga_id varchar(36) PRIMARY KEY,
 transaction_id varchar(36) NOT NULL REFERENCES checkout_transactions(transaction_id),
 state varchar(16) NOT NULL CHECK(state IN ('RESERVING','COMMITTING','CANCELLING','DONE','ABORTED')),
 attempts integer NOT NULL DEFAULT 0,
 last_error varchar(1000),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 next_attempt_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS checkout_one_active_saga ON checkout_sagas(transaction_id)
 WHERE state IN ('RESERVING','COMMITTING','CANCELLING');
CREATE INDEX IF NOT EXISTS checkout_saga_recovery ON checkout_sagas(next_attempt_at)
 WHERE state IN ('RESERVING','COMMITTING','CANCELLING');
COMMIT;
