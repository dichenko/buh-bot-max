# BUH Bot MAX (Scaffold)

Каркас нового проекта для миграции Telegram-бота в MAX:
- `bot/` — бот на TypeScript (`@maxhub/max-bot-api`)
- `worker/` — Python-воркер генерации Excel/PDF
- `db/` — PostgreSQL схема и SQL-миграции
- `pgadmin` — UI для БД в Docker

## 1. Быстрый старт

1. Скопировать `.env.example` -> `.env` и заполнить значения.
2. Поднять сервисы:

```bash
docker compose up -d --build
```

3. Проверить состояние:

```bash
docker compose ps
docker compose logs --tail=200 bot worker postgres
```

## 2. Структура

- `bot/src/index.ts` — запуск MAX-бота
- `bot/src/handlers/registerHandlers.ts` — команды и основной flow
- `db/init/001_schema.sql` — базовая схема с `users.max_user_id`
- `db/migrations/20260405_add_max_user_id.sql` — миграция для существующей базы
- `worker/src/excel_pdf_worker.py` — функции генерации документов

## 3. Деплой на VPS

```bash
git pull --ff-only origin main
docker compose up -d --build
docker compose ps
```

Если база уже существует и создана до добавления поля `max_user_id`, применить миграцию:

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < db/migrations/20260405_add_max_user_id.sql
```