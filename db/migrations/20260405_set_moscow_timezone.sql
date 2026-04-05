BEGIN;

-- Apply Moscow timezone for current migration session.
SET TIME ZONE 'Europe/Moscow';

-- Persist default timezone at database level.
DO $$
BEGIN
    EXECUTE format(
        'ALTER DATABASE %I SET timezone TO %L',
        current_database(),
        'Europe/Moscow'
    );
END
$$;

-- Persist timezone for the current role in this database.
DO $$
BEGIN
    EXECUTE format(
        'ALTER ROLE %I IN DATABASE %I SET timezone TO %L',
        current_user,
        current_database(),
        'Europe/Moscow'
    );
END
$$;

COMMIT;
