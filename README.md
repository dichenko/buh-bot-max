# BUH Bot MAX

Каркас проекта для переноса Telegram-бота в MAX.

Сервисы:
- `bot/` — бот на TypeScript (`@maxhub/max-bot-api`)
- `worker/` — Python-воркер
- `postgres` + `pgadmin` — в Docker
- `db/` — SQL-схема и миграции

## Быстрый старт

```bash
cp .env.example .env
# заполнить .env

docker compose up -d --build
docker compose ps
docker compose logs --tail=200 bot worker postgres
```

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