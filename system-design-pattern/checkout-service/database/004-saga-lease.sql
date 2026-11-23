-- 功能：为同一 Saga 的前台与后台推进增加持久租约；保留所有已有记录。
BEGIN;
ALTER TABLE checkout_sagas ADD COLUMN IF NOT EXISTS lease_owner varchar(36);
ALTER TABLE checkout_sagas ADD COLUMN IF NOT EXISTS lease_until timestamptz;
COMMIT;
