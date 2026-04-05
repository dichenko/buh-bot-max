from __future__ import annotations

import datetime as dt
import logging
import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

from .excel_pdf_worker import DocumentTask, WorkerResult, generate_documents


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


@dataclass(frozen=True)
class WorkerConfig:
    database_url: str
    poll_interval_seconds: float
    idle_log_interval_seconds: float
    templates_dir: Path
    output_dir: Path


def _parse_positive_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default

    try:
        value = float(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive number") from exc

    if value <= 0:
        raise ValueError(f"{name} must be a positive number")

    return value


def _load_config() -> WorkerConfig:
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        raise ValueError("DATABASE_URL is required for worker")

    templates_dir = Path((os.getenv("WORKER_TEMPLATES_DIR") or "/app/templates").strip())
    output_dir = Path((os.getenv("WORKER_OUTPUT_DIR") or "/app/obrazec").strip())

    return WorkerConfig(
        database_url=database_url,
        poll_interval_seconds=_parse_positive_float("WORKER_POLL_INTERVAL_SECONDS", 5.0),
        idle_log_interval_seconds=_parse_positive_float("WORKER_IDLE_LOG_INTERVAL_SECONDS", 60.0),
        templates_dir=templates_dir,
        output_dir=output_dir,
    )


def _trim_error(message: str, max_length: int = 4000) -> str:
    normalized = message.strip()
    if not normalized:
        return "Unknown worker error"
    return normalized[:max_length]


def _claim_next_invoice(conn: psycopg.Connection[Any], worker_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH candidate AS (
                SELECT id
                FROM invoices_ip
                WHERE worker_status = 'new'
                ORDER BY id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE invoices_ip AS inv
            SET
                worker_status = 'processing',
                worker_started_at = NOW(),
                worker_finished_at = NULL,
                worker_error = NULL,
                worker_result_files = NULL,
                worker_workspace_path = NULL,
                worker_attempts = COALESCE(inv.worker_attempts, 0) + 1,
                worker_id = %s
            FROM candidate
            WHERE inv.id = candidate.id
            RETURNING
                inv.id,
                inv.number,
                inv.user_id,
                inv.org_name,
                inv.org_inn,
                inv.count,
                inv.org_price,
                inv.date
            """,
            (worker_id,),
        )
        row = cur.fetchone()

    conn.commit()
    return row


def _mark_done(conn: psycopg.Connection[Any], invoice_id: int, result: WorkerResult) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE invoices_ip
            SET
                worker_status = 'done',
                worker_finished_at = NOW(),
                worker_error = NULL,
                worker_result_files = %s::TEXT[],
                worker_workspace_path = %s
            WHERE id = %s
            """,
            (result.pdf_files, result.workspace_path, invoice_id),
        )

    conn.commit()


def _mark_error(conn: psycopg.Connection[Any], invoice_id: int, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE invoices_ip
            SET
                worker_status = 'error',
                worker_finished_at = NOW(),
                worker_error = %s
            WHERE id = %s
            """,
            (_trim_error(error_message), invoice_id),
        )

    conn.commit()


def _to_positive_int(value: Any, field_name: str) -> int:
    if value is None:
        raise ValueError(f"{field_name} is required")

    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return parsed


def _build_task(row: dict[str, Any]) -> DocumentTask:
    user_id = _to_positive_int(row.get("user_id"), "user_id")
    invoice_number = _to_positive_int(row.get("number"), "number")
    count = _to_positive_int(row.get("count"), "count")

    org_name_raw = row.get("org_name")
    org_name = str(org_name_raw).strip() if org_name_raw is not None else ""
    if not org_name:
        org_name = "Без названия организации"

    org_inn = row.get("org_inn")
    price_per_item = float(row.get("org_price") or 0)

    date_value = row.get("date")
    work_date: dt.date | None = None
    if isinstance(date_value, dt.datetime):
        work_date = date_value.date()
    elif isinstance(date_value, dt.date):
        work_date = date_value

    return DocumentTask(
        user_id=user_id,
        org_name=org_name,
        org_inn="" if org_inn is None else org_inn,
        count=count,
        price_per_item=price_per_item,
        invoice_number=invoice_number,
        work_date=work_date,
    )


def run_forever() -> None:
    load_dotenv()
    config = _load_config()
    config.output_dir.mkdir(parents=True, exist_ok=True)

    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    logging.info("Python worker started: id=%s", worker_id)
    logging.info(
        "Worker config: poll=%ss templates_dir=%s output_dir=%s",
        config.poll_interval_seconds,
        config.templates_dir,
        config.output_dir,
    )

    last_idle_log_ts = 0.0

    while True:
        try:
            with psycopg.connect(config.database_url, row_factory=dict_row) as conn:
                while True:
                    row = _claim_next_invoice(conn, worker_id)
                    if row is None:
                        now = time.time()
                        if now - last_idle_log_ts >= config.idle_log_interval_seconds:
                            logging.info("Worker heartbeat: queue is empty")
                            last_idle_log_ts = now
                        time.sleep(config.poll_interval_seconds)
                        continue

                    invoice_id = int(row["id"])
                    logging.info(
                        "Picked invoice id=%s number=%s user_id=%s",
                        invoice_id,
                        row.get("number"),
                        row.get("user_id"),
                    )

                    try:
                        task = _build_task(row)
                        result = generate_documents(
                            task=task,
                            templates_dir=config.templates_dir,
                            output_dir=config.output_dir,
                        )
                        if result.status != "success":
                            raise RuntimeError(result.error_message or "Document generation failed")

                        _mark_done(conn, invoice_id, result)
                        logging.info("Invoice id=%s processed. Files: %s", invoice_id, result.pdf_files)
                    except Exception as exc:  # noqa: BLE001
                        logging.exception("Failed to process invoice id=%s", invoice_id)
                        try:
                            _mark_error(conn, invoice_id, str(exc))
                        except Exception:  # noqa: BLE001
                            conn.rollback()
                            logging.exception(
                                "Failed to mark invoice id=%s as error. Reconnecting...",
                                invoice_id,
                            )
                            raise
        except Exception:  # noqa: BLE001
            logging.exception("Worker database loop crashed, retrying in 5 seconds")
            time.sleep(5)


if __name__ == "__main__":
    run_forever()
