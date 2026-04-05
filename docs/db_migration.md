# DB Migration Plan

## Goal

Сохранить текущую структуру и данные старого Telegram-бота, добавив только поле `users.max_user_id` для MAX.

## Existing tables kept as-is

- `users`
- `organizations`
- `invoices_ip`
- `invoices_ua`
- `invoices_av`
- `invoices_3`

## Added field

- `users.max_user_id BIGINT` — идентификатор пользователя в MAX.

## Migration steps for existing database

1. Сделать backup:

```bash
pg_dump -Fc -h <host> -U <user> -d <db> -f before_max_migration.dump
```

2. Применить SQL:

```bash
psql -h <host> -U <user> -d <db> -f db/migrations/20260405_add_max_user_id.sql
```

3. Проверить:

```sql
\d users
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM users WHERE max_user_id IS NOT NULL;
```

4. Заполнять `max_user_id` через админ-команду бота `/bindmax` или SQL.

## Optional import from old CSV dumps

```bash
psql -h <host> -U <user> -d <db> -c "TRUNCATE users, organizations, invoices_ip, invoices_ua, invoices_av, invoices_3 RESTART IDENTITY;"
psql -h <host> -U <user> -d <db> -c "\copy organizations(id,org_id,org_name,org_template,org_price,org_price_ip,org_inn,org_foundation,org_foundation_2) FROM 'Перенос проекта/db/organizations.csv' DELIMITER ';' CSV HEADER"
psql -h <host> -U <user> -d <db> -c "\copy users(id,user_id,org_id,user_time) FROM 'Перенос проекта/db/users.csv' DELIMITER ';' CSV HEADER"
psql -h <host> -U <user> -d <db> -c "\copy invoices_ip(id,number,org_id,org_name,org_inn,org_count,org_price,date,user_id,count) FROM 'Перенос проекта/db/invoices_ip.csv' DELIMITER ';' CSV HEADER"
psql -h <host> -U <user> -d <db> -c "\copy invoices_ua(id,number,org_id,org_name,org_inn,org_count,org_price,date,user_id,count) FROM 'Перенос проекта/db/invoices_ua.csv' DELIMITER ';' CSV HEADER"
psql -h <host> -U <user> -d <db> -c "\copy invoices_av(id,number,org_id,org_name,org_inn,org_count,org_price,date,user_id,count) FROM 'Перенос проекта/db/invoices_av.csv' DELIMITER ';' CSV HEADER"
psql -h <host> -U <user> -d <db> -c "\copy invoices_3(id,number,org_id,org_name,org_inn,org_count,org_price,date,user_id,count) FROM 'Перенос проекта/db/invoices_3.csv' DELIMITER ';' CSV HEADER"
```