# Project Rules (MAX Bot + Backend + PostgreSQL)

## 1) Процесс работы
- После каждого завершенного изменения:
  - проверить сборку и тесты;
  - закоммитить изменения;
  - запушить в репозиторий (`origin/main`).
- В ответе всегда давать готовые команды для сервера:
  - `git pull`
  - миграции (если нужны)
  - перезапуск Docker сервисов.
- Не оставлять незавершенные локальные правки без явного комментария.

## 2) Деплой (базовый шаблон команд)
```bash

git pull --ff-only origin main
docker compose exec backend npm run prisma:migrate
docker compose up -d --build backend miniapp payment-worker photo-worker
docker compose ps
docker compose logs --tail=200 backend payment-worker
```

## 3) Кодировка и русский текст
- Все файлы: UTF-8 без BOM.
- Любые тексты для бота/miniapp хранить и отправлять корректно на русском языке.
- Не допускать «кракозябр» вида `РџР...` в сообщениях и шаблонах.


## 8) Проверка перед сдачей
- Проверить:
  - `backend`: build + tests;


