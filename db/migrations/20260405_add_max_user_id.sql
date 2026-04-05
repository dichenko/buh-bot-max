BEGIN;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS max_user_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_users_max_user_id
    ON users (max_user_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_users_max_user_id_not_null
    ON users (max_user_id)
    WHERE max_user_id IS NOT NULL;

COMMIT;