-- Import legacy CSV dumps into current schema.
-- Expected files inside postgres container:
--   /tmp/organizations.csv
--   /tmp/users.csv
--
-- Run with:
--   psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f db/migrations/20260405_import_legacy_orgs_users.sql

BEGIN;

CREATE TEMP TABLE stage_organizations (
  id TEXT,
  org_id TEXT,
  org_name TEXT,
  org_template TEXT,
  org_price TEXT,
  org_price_ip TEXT,
  org_inn TEXT,
  org_foundation TEXT,
  org_foundation_2 TEXT
);

\copy stage_organizations FROM '/tmp/organizations.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';');

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
SELECT
  NULLIF(org_id, '')::BIGINT,
  NULLIF(org_name, ''),
  COALESCE(NULLIF(org_template, '')::BIGINT, 0),
  COALESCE(NULLIF(org_price, '')::BIGINT, 0),
  COALESCE(NULLIF(org_price_ip, '')::BIGINT, 0),
  NULLIF(org_inn, '')::BIGINT,
  NULLIF(org_foundation, ''),
  NULLIF(org_foundation_2, '')
FROM stage_organizations
WHERE NULLIF(org_id, '') IS NOT NULL
ON CONFLICT (org_id) DO UPDATE SET
  org_name = EXCLUDED.org_name,
  org_template = EXCLUDED.org_template,
  org_price = EXCLUDED.org_price,
  org_price_ip = EXCLUDED.org_price_ip,
  org_inn = EXCLUDED.org_inn,
  org_foundation = EXCLUDED.org_foundation,
  org_foundation_2 = EXCLUDED.org_foundation_2;

CREATE TEMP TABLE stage_users (
  id TEXT,
  user_id TEXT,
  org_id TEXT,
  user_time TEXT
);

\copy stage_users FROM '/tmp/users.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';');

-- Add placeholder organizations for missing org_id values from users dump.
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
  NULLIF(s.org_id, '')::BIGINT,
  'MIGRATED placeholder org_id=' || NULLIF(s.org_id, ''),
  0::BIGINT,
  0::BIGINT,
  0::BIGINT,
  NULL::BIGINT,
  NULL::VARCHAR,
  NULL::VARCHAR
FROM stage_users s
LEFT JOIN organizations o ON o.org_id = NULLIF(s.org_id, '')::BIGINT
WHERE NULLIF(s.org_id, '') IS NOT NULL
  AND o.org_id IS NULL;

INSERT INTO users (tg_user_id, max_user_id, org_id, user_time)
SELECT DISTINCT ON (NULLIF(s.user_id, '')::BIGINT)
  NULLIF(s.user_id, '')::BIGINT AS tg_user_id,
  NULL::BIGINT AS max_user_id,
  NULLIF(s.org_id, '')::BIGINT AS org_id,
  COALESCE(NULLIF(s.user_time, '')::BIGINT, EXTRACT(EPOCH FROM NOW())::BIGINT) AS user_time
FROM stage_users s
WHERE NULLIF(s.user_id, '') IS NOT NULL
  AND NULLIF(s.org_id, '') IS NOT NULL
ORDER BY
  NULLIF(s.user_id, '')::BIGINT,
  COALESCE(NULLIF(s.user_time, '')::BIGINT, 0) DESC,
  COALESCE(NULLIF(s.id, '')::BIGINT, 0) DESC
ON CONFLICT (tg_user_id) DO UPDATE SET
  org_id = EXCLUDED.org_id,
  user_time = EXCLUDED.user_time;

COMMIT;
