# Deploy Commands (VPS)

## Initial setup

```bash
cp .env.example .env
# fill real secrets in .env
```

## Build and run

```bash
docker compose up -d --build
docker compose ps
docker compose logs --tail=200 bot worker postgres
```

## Update on server

```bash
git pull --ff-only origin main
docker compose up -d --build
docker compose ps
```

## Apply migration for existing DB

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < db/migrations/20260405_add_max_user_id.sql
```