BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'users'
      AND column_name = 'user_id'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'users'
      AND column_name = 'tg_user_id'
  ) THEN
    EXECUTE 'ALTER TABLE users RENAME COLUMN user_id TO tg_user_id';
  END IF;
END;
$$;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS tg_user_id BIGINT;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS max_user_id BIGINT;

DROP INDEX IF EXISTS idx_users_user_id;
DROP INDEX IF EXISTS ux_users_max_user_id_not_null;
DROP INDEX IF EXISTS ux_users_tg_user_id_not_null;

WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY tg_user_id
            ORDER BY user_time DESC NULLS LAST, id DESC
        ) AS rn
    FROM users
    WHERE tg_user_id IS NOT NULL
)
DELETE FROM users u
USING ranked r
WHERE u.id = r.id
  AND r.rn > 1;

WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY max_user_id
            ORDER BY user_time DESC NULLS LAST, id DESC
        ) AS rn
    FROM users
    WHERE max_user_id IS NOT NULL
)
DELETE FROM users u
USING ranked r
WHERE u.id = r.id
  AND r.rn > 1;

WITH ranked_org AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY org_id
            ORDER BY id ASC
        ) AS rn
    FROM organizations
    WHERE org_id IS NOT NULL
)
DELETE FROM organizations o
USING ranked_org r
WHERE o.id = r.id
  AND r.rn > 1;

INSERT INTO organizations (
    org_id,
    org_name,
    org_template,
    org_price,
    org_price_ip,
    org_inn,
    org_foundation,
    org_foundation_2
)
SELECT DISTINCT
    u.org_id,
    'MIGRATED placeholder org_id=' || u.org_id::TEXT,
    0,
    0,
    0,
    NULL,
    NULL,
    NULL
FROM users u
LEFT JOIN organizations o ON o.org_id = u.org_id
WHERE u.org_id IS NOT NULL
  AND o.org_id IS NULL;

ALTER TABLE organizations
    ALTER COLUMN org_id SET NOT NULL;

ALTER TABLE users
    ALTER COLUMN org_id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'organizations_org_id_uk'
  ) THEN
    ALTER TABLE organizations
        ADD CONSTRAINT organizations_org_id_uk UNIQUE (org_id);
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'users_tg_user_id_uk'
  ) THEN
    ALTER TABLE users
        ADD CONSTRAINT users_tg_user_id_uk UNIQUE (tg_user_id);
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'users_max_user_id_uk'
  ) THEN
    ALTER TABLE users
        ADD CONSTRAINT users_max_user_id_uk UNIQUE (max_user_id);
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'users_identity_required_chk'
  ) THEN
    ALTER TABLE users
        ADD CONSTRAINT users_identity_required_chk
        CHECK (tg_user_id IS NOT NULL OR max_user_id IS NOT NULL);
  END IF;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'users_org_id_fk'
  ) THEN
    ALTER TABLE users
        ADD CONSTRAINT users_org_id_fk
        FOREIGN KEY (org_id)
        REFERENCES organizations(org_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT;
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_users_tg_user_id
    ON users (tg_user_id);

CREATE INDEX IF NOT EXISTS idx_users_org_id
    ON users (org_id);

CREATE INDEX IF NOT EXISTS idx_users_max_user_id
    ON users (max_user_id);

COMMIT;
