BEGIN;

DO $$
DECLARE
  seq_name TEXT;
  next_val BIGINT;
BEGIN
  SELECT pg_get_serial_sequence('public.organizations', 'id') INTO seq_name;
  IF seq_name IS NULL THEN
    IF to_regclass('public.organizations_id_seq') IS NULL THEN
      EXECUTE 'CREATE SEQUENCE public.organizations_id_seq';
    END IF;

    EXECUTE 'ALTER SEQUENCE public.organizations_id_seq OWNED BY public.organizations.id';
    EXECUTE 'ALTER TABLE public.organizations ALTER COLUMN id SET DEFAULT nextval(''public.organizations_id_seq'')';
    seq_name := 'public.organizations_id_seq';
  END IF;

  EXECUTE 'SELECT COALESCE(MAX(id), 0) + 1 FROM public.organizations' INTO next_val;
  EXECUTE format('SELECT setval(%L::regclass, %s, false)', seq_name, next_val);
END;
$$;

DO $$
DECLARE
  seq_name TEXT;
  next_val BIGINT;
BEGIN
  SELECT pg_get_serial_sequence('public.organizations', 'org_id') INTO seq_name;
  IF seq_name IS NULL THEN
    IF to_regclass('public.organizations_org_id_seq') IS NULL THEN
      EXECUTE 'CREATE SEQUENCE public.organizations_org_id_seq';
    END IF;

    EXECUTE 'ALTER SEQUENCE public.organizations_org_id_seq OWNED BY public.organizations.org_id';
    EXECUTE 'ALTER TABLE public.organizations ALTER COLUMN org_id SET DEFAULT nextval(''public.organizations_org_id_seq'')';
    seq_name := 'public.organizations_org_id_seq';
  END IF;

  EXECUTE 'SELECT COALESCE(MAX(org_id), 0) + 1 FROM public.organizations' INTO next_val;
  EXECUTE format('SELECT setval(%L::regclass, %s, false)', seq_name, next_val);
END;
$$;

DO $$
DECLARE
  seq_name TEXT;
  next_val BIGINT;
BEGIN
  SELECT pg_get_serial_sequence('public.users', 'id') INTO seq_name;
  IF seq_name IS NULL THEN
    IF to_regclass('public.users_id_seq') IS NULL THEN
      EXECUTE 'CREATE SEQUENCE public.users_id_seq';
    END IF;

    EXECUTE 'ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id';
    EXECUTE 'ALTER TABLE public.users ALTER COLUMN id SET DEFAULT nextval(''public.users_id_seq'')';
    seq_name := 'public.users_id_seq';
  END IF;

  EXECUTE 'SELECT COALESCE(MAX(id), 0) + 1 FROM public.users' INTO next_val;
  EXECUTE format('SELECT setval(%L::regclass, %s, false)', seq_name, next_val);
END;
$$;

COMMIT;
