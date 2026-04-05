# Worker Queue (invoices_ip)

После миграции `db/migrations/20260405_add_worker_queue_status.sql` таблица `invoices_ip` работает как очередь для `worker`.

## Статусы

- `new` - заявка создана ботом и ожидает обработки.
- `processing` - воркер забрал заявку в работу.
- `done` - документы успешно сформированы.
- `error` - ошибка в обработке (текст в `worker_error`).

## Быстрые проверки

```bash
docker compose ps
docker compose logs --tail=200 bot
docker compose logs --tail=200 worker
```

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT id, number, worker_status, worker_attempts, worker_error, worker_started_at, worker_finished_at
FROM invoices_ip
ORDER BY id DESC
LIMIT 30;
"
```

## Ручной повтор заявки из error

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
UPDATE invoices_ip
SET worker_status='new',
    worker_error=NULL,
    worker_started_at=NULL,
    worker_finished_at=NULL
WHERE id = <INVOICE_ID>;
"
```

## Где лежат файлы

По умолчанию воркер пишет в `./worker/output` (volume `./worker/output:/app/obrazec`).

