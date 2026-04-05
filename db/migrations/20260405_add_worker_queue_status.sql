BEGIN;

ALTER TABLE invoices_ip
    ADD COLUMN IF NOT EXISTS worker_status TEXT;

ALTER TABLE invoices_ip
    ADD COLUMN IF NOT EXISTS worker_attempts INTEGER;

ALTER TABLE invoices_ip
    ADD COLUMN IF NOT EXISTS worker_started_at TIMESTAMPTZ;

ALTER TABLE invoices_ip
    ADD COLUMN IF NOT EXISTS worker_finished_at TIMESTAMPTZ;

ALTER TABLE invoices_ip
    ADD COLUMN IF NOT EXISTS worker_error TEXT;

ALTER TABLE invoices_ip
    ADD COLUMN IF NOT EXISTS worker_result_files TEXT[];

ALTER TABLE invoices_ip
    ADD COLUMN IF NOT EXISTS worker_workspace_path TEXT;

ALTER TABLE invoices_ip
    ADD COLUMN IF NOT EXISTS worker_id TEXT;

UPDATE invoices_ip
SET worker_status = 'done'
WHERE worker_status IS NULL;

UPDATE invoices_ip
SET worker_attempts = 0
WHERE worker_attempts IS NULL;

ALTER TABLE invoices_ip
    ALTER COLUMN worker_status SET DEFAULT 'new';

ALTER TABLE invoices_ip
    ALTER COLUMN worker_status SET NOT NULL;

ALTER TABLE invoices_ip
    ALTER COLUMN worker_attempts SET DEFAULT 0;

ALTER TABLE invoices_ip
    ALTER COLUMN worker_attempts SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'invoices_ip_worker_status_chk'
  ) THEN
    ALTER TABLE invoices_ip
      ADD CONSTRAINT invoices_ip_worker_status_chk
      CHECK (worker_status IN ('new', 'processing', 'done', 'error'));
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_invoices_ip_worker_status
    ON invoices_ip (worker_status);

CREATE INDEX IF NOT EXISTS idx_invoices_ip_worker_status_date
    ON invoices_ip (worker_status, date DESC);

COMMIT;
