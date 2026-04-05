BEGIN;

CREATE OR REPLACE FUNCTION public.organizations_fill_ids_on_null()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    id_seq_name TEXT;
    org_id_seq_name TEXT;
BEGIN
    id_seq_name := pg_get_serial_sequence('public.organizations', 'id');
    org_id_seq_name := pg_get_serial_sequence('public.organizations', 'org_id');

    IF NEW.id IS NULL THEN
        IF id_seq_name IS NULL THEN
            RAISE EXCEPTION 'No sequence/default configured for public.organizations.id';
        END IF;
        EXECUTE format('SELECT nextval(%L::regclass)', id_seq_name) INTO NEW.id;
    END IF;

    IF NEW.org_id IS NULL THEN
        IF org_id_seq_name IS NULL THEN
            RAISE EXCEPTION 'No sequence/default configured for public.organizations.org_id';
        END IF;
        EXECUTE format('SELECT nextval(%L::regclass)', org_id_seq_name) INTO NEW.org_id;
    END IF;

    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS trg_organizations_fill_ids_on_null ON public.organizations;

CREATE TRIGGER trg_organizations_fill_ids_on_null
BEFORE INSERT ON public.organizations
FOR EACH ROW
EXECUTE FUNCTION public.organizations_fill_ids_on_null();

COMMIT;
