# BUH Bot MAX

Каркас проекта для переноса Telegram-бота в MAX.

Сервисы:
- `bot/` — бот на TypeScript (`@maxhub/max-bot-api`)
- `worker/` — Python-воркер
- `worker-report` — отдельный Python-воркер ежедневной отчетности
- `postgres` + `pgadmin` — в Docker
- `db/` — SQL-схема и миграции

## Быстрый старт

```bash
cp .env.example .env
# заполнить .env

docker compose up -d --build
docker compose ps
docker compose logs --tail=200 bot worker worker-report postgres
```

## Webhook режим (без polling)

Бот принимает обновления только через HTTP webhook:
- `POST {WEBHOOK_PATH}` (по умолчанию `/webhook`)
- `GET /healthz` для health-check

Переменные:
- `BOT_SUBDOMAIN` — публичный HTTPS-домен бота (например, `https://bot.example.com`)
- `BOT_PORT` — локальный порт хоста для reverse proxy (по умолчанию `3003`, upstream в Caddy: `127.0.0.1:3003`)
- `WEBHOOK_PATH` — путь webhook (по умолчанию `/webhook`)
- `WEBHOOK_SECRET` — секрет заголовка `X-Max-Bot-Api-Secret` (рекомендуется)

## Важное по схеме users

- `users.tg_user_id` — старый Telegram ID (nullable, для обратной совместимости)
- `users.max_user_id` — MAX ID (nullable, заполняется постепенно)
- `users.org_id` — обязательная привязка к одной организации

Одна организация может иметь много пользователей.

## Миграция и импорт старых CSV

Подробная инструкция:
- [docs/db_migration.md](docs/db_migration.md)

Коротко (миграция существующей БД):

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < db/migrations/20260405_add_max_user_id.sql
```

## Worker queue

Диагностика обработки заявок и статусов очереди:
- [docs/worker_queue.md](docs/worker_queue.md)

## Timezone

- Set `BOT_TIMEZONE=Europe/Moscow` in `.env`.
- PostgreSQL runs with `-c timezone=Europe/Moscow` from `docker-compose.yml`.
- For existing databases, apply migration: `db/migrations/20260405_set_moscow_timezone.sql`.

## MAX delivery from worker

- Python `worker` sends generated PDF files to the MAX user after email is sent.
- Required worker env vars:
  - `MAX_BOT_TOKEN`
  - `MAX_API_BASE_URL` (default: `https://platform-api.max.ru`)
