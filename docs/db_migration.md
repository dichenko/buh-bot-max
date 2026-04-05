# Миграция БД: users.tg_user_id + users.max_user_id

## Что изменилось в схеме

Таблица `users` теперь выглядит так:
- `tg_user_id BIGINT NULL` — бывший `user_id` из Telegram (для обратной совместимости).
- `max_user_id BIGINT NULL` — идентификатор пользователя в MAX.
- `org_id BIGINT NOT NULL` — пользователь привязан ровно к одной организации.
- `user_time BIGINT NOT NULL`.

Ограничения:
- `UNIQUE (tg_user_id)`
- `UNIQUE (max_user_id)`
- `CHECK (tg_user_id IS NOT NULL OR max_user_id IS NOT NULL)`
- `FOREIGN KEY (org_id) -> organizations(org_id)`

Это дает нужную модель:
- у одной организации может быть много пользователей;
- у пользователя ровно одна организация;
- у старых пользователей `max_user_id` может быть `NULL`.

## 1) Миграция существующей БД

Из корня проекта:

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < db/migrations/20260405_add_max_user_id.sql
```

Что делает миграция:
- переименовывает `users.user_id` -> `users.tg_user_id`;
- добавляет `users.max_user_id`;
- удаляет дубли пользователей по `tg_user_id`/`max_user_id` (оставляет самую свежую запись);
- добавляет недостающие `organizations` для сохранения связей;
- включает ограничения и индексы.

## 2) Импорт старых `organizations.csv` и `users.csv`

### 2.1 Скопировать CSV в контейнер PostgreSQL

```bash
docker cp "Перенос проекта/db/organizations.csv" buh-bot-max-postgres:/tmp/organizations.csv
docker cp "Перенос проекта/db/users.csv" buh-bot-max-postgres:/tmp/users.csv
```

### 2.2 Выполнить импорт

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
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

-- Если в users.csv есть org_id, которых нет в organizations.csv,
-- добавляем технические организации, чтобы сохранить соответствие user -> org.
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
  0,
  0,
  0,
  NULL,
  NULL,
  NULL
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
SQL
```

## 3) Проверка после импорта

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) AS organizations_total FROM organizations;"
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) AS users_total FROM users;"
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) AS users_without_max FROM users WHERE max_user_id IS NULL;"
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) AS broken_user_org_links FROM users u LEFT JOIN organizations o ON o.org_id = u.org_id WHERE o.org_id IS NULL;"
```

Ожидаемо:
- `users_without_max` > 0 для старых пользователей (это нормально);
- `broken_user_org_links` = 0.