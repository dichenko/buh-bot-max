from __future__ import annotations

import datetime as dt
import logging
import os
import socket
import smtplib
import time
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.rows import dict_row

from .excel_pdf_worker import DocumentTask, WorkerResult, generate_documents

DEFAULT_TIMEZONE_NAME = "Europe/Moscow"


class TimezoneAwareFormatter(logging.Formatter):
    def __init__(self, fmt: str, datefmt: str, timezone: dt.tzinfo):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._timezone = timezone

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        current = dt.datetime.fromtimestamp(record.created, tz=self._timezone)
        if datefmt:
            return current.strftime(datefmt)
        return current.isoformat(timespec="seconds")


def _configure_logging(timezone: dt.tzinfo) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(
        TimezoneAwareFormatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S %z",
            timezone=timezone,
        )
    )

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


@dataclass(frozen=True)
class SmtpConfig:
    host: str
    port: int
    secure: bool
    user: str
    password: str
    from_address: str
    recipients: list[str]


@dataclass(frozen=True)
class MegaPlanConfig:
    token: str
    url: str
    responsible_id: int
    auditor_ids: list[int]
    deadline_days: int


@dataclass(frozen=True)
class WorkerConfig:
    database_url: str
    poll_interval_seconds: float
    idle_log_interval_seconds: float
    templates_dir: Path
    output_dir: Path
    timezone_name: str
    timezone: dt.tzinfo
    smtp: SmtpConfig
    megaplan: MegaPlanConfig


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


def _parse_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default

    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc

    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")

    return value


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required for worker")
    return value


def _parse_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int_list(raw: str) -> list[int]:
    values: list[int] = []
    for chunk in raw.split(","):
        candidate = chunk.strip()
        if not candidate:
            continue
        values.append(int(candidate))
    return values


def _parse_recipients(raw: str) -> list[str]:
    recipients = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
    if not recipients:
        raise ValueError("ADMIN_EMAIL must contain at least one recipient")
    return recipients


def _load_smtp_config() -> SmtpConfig:
    host = _require_env("SMTP_HOST")
    port = _parse_positive_int("SMTP_PORT", 587)
    secure = _parse_bool("SMTP_SECURE", default=False)
    user = (os.getenv("SMTP_USER") or "").strip()
    password = (os.getenv("SMTP_PASSWORD") or "").strip()
    from_address = (os.getenv("SMTP_FROM") or "").strip() or user
    recipients = _parse_recipients(_require_env("ADMIN_EMAIL"))

    if not from_address:
        raise ValueError("SMTP_FROM or SMTP_USER must be provided")
    if user and not password:
        raise ValueError("SMTP_PASSWORD is required when SMTP_USER is provided")

    return SmtpConfig(
        host=host,
        port=port,
        secure=secure,
        user=user,
        password=password,
        from_address=from_address,
        recipients=recipients,
    )


def _load_megaplan_config() -> MegaPlanConfig:
    token = _require_env("TOKEN_MEGAPLAN")
    url = _require_env("URL_MEGAPLAN")
    responsible_id = _parse_positive_int("MEGAPLAN_RESPONSIBLE_ID", 1000038)
    deadline_days = _parse_positive_int("MEGAPLAN_DEADLINE_DAYS", 14)
    auditor_raw = (os.getenv("MEGAPLAN_AUDITOR_IDS") or "1000003,1000019,1000038").strip()
    auditor_ids = _parse_int_list(auditor_raw)

    return MegaPlanConfig(
        token=token,
        url=url,
        responsible_id=responsible_id,
        auditor_ids=auditor_ids,
        deadline_days=deadline_days,
    )


def _load_config() -> WorkerConfig:
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        raise ValueError("DATABASE_URL is required for worker")

    templates_dir = Path((os.getenv("WORKER_TEMPLATES_DIR") or "/app/templates").strip())
    output_dir = Path((os.getenv("WORKER_OUTPUT_DIR") or "/app/obrazec").strip())
    timezone_name = (
        os.getenv("BOT_TIMEZONE")
        or os.getenv("APP_TIMEZONE")
        or DEFAULT_TIMEZONE_NAME
    ).strip() or DEFAULT_TIMEZONE_NAME
    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown timezone: {timezone_name}") from exc

    return WorkerConfig(
        database_url=database_url,
        poll_interval_seconds=_parse_positive_float("WORKER_POLL_INTERVAL_SECONDS", 5.0),
        idle_log_interval_seconds=_parse_positive_float("WORKER_IDLE_LOG_INTERVAL_SECONDS", 60.0),
        templates_dir=templates_dir,
        output_dir=output_dir,
        timezone_name=timezone_name,
        timezone=timezone,
        smtp=_load_smtp_config(),
        megaplan=_load_megaplan_config(),
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
                inv.org_id,
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


def _build_task(row: dict[str, Any], timezone: dt.tzinfo) -> DocumentTask:
    user_id = _to_positive_int(row.get("user_id"), "user_id")
    invoice_number = _to_positive_int(row.get("number"), "number")
    count = _to_positive_int(row.get("count"), "count")

    org_name_raw = row.get("org_name")
    org_name = str(org_name_raw).strip() if org_name_raw is not None else ""
    if not org_name:
        org_name = "Unknown organization"

    org_inn = row.get("org_inn")
    price_per_item = float(row.get("org_price") or 0)

    date_value = row.get("date")
    work_date: dt.date | None = None
    if isinstance(date_value, dt.datetime):
        if date_value.tzinfo is None:
            work_date = date_value.replace(tzinfo=timezone).date()
        else:
            work_date = date_value.astimezone(timezone).date()
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


def _extract_pdf_paths(paths: list[str]) -> list[Path]:
    pdf_paths = [Path(path) for path in paths if path.lower().endswith(".pdf")]
    if not pdf_paths:
        raise RuntimeError("Document generation completed without PDF files")
    return pdf_paths


def _send_invoice_email(config: SmtpConfig, task: DocumentTask, pdf_paths: list[Path]) -> None:
    total_sum = task.count * task.price_per_item
    message = EmailMessage()
    message["From"] = config.from_address
    message["To"] = ", ".join(config.recipients)
    message["Subject"] = f"Заявка от {task.org_name}"
    message.set_content(
        "\n".join(
            [
                f"Организация: {task.org_name}",
                f"Max user id: {task.user_id}",
                f"Номер счета: {task.invoice_number}",
                f"Количество: {task.count}",
                f"Цена за 1 услугу: {task.price_per_item}",
                f"Сумма: {total_sum}",
            ]
        )
    )

    for pdf_path in pdf_paths:
        with pdf_path.open("rb") as file:
            message.add_attachment(
                file.read(),
                maintype="application",
                subtype="pdf",
                filename=pdf_path.name,
            )

    if config.secure:
        with smtplib.SMTP_SSL(config.host, config.port, timeout=60) as server:
            if config.user:
                server.login(config.user, config.password)
            server.send_message(message)
        return

    with smtplib.SMTP(config.host, config.port, timeout=60) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        if config.user:
            server.login(config.user, config.password)
        server.send_message(message)


def _create_megaplan_task(
    config: MegaPlanConfig,
    task: DocumentTask,
    org_id: int | None,
    price_per_item: float,
    timezone: dt.tzinfo,
) -> int | None:
    total_sum = task.count * price_per_item
    deadline = (dt.datetime.now(timezone) + dt.timedelta(days=config.deadline_days)).isoformat(
        timespec="seconds"
    )

    payload: dict[str, Any] = {
        "contentType": "Task",
        "name": f"Ждем оплату от {task.org_name}",
        "responsible": {
            "contentType": "Employee",
            "id": config.responsible_id,
        },
        "subject": (
            "Выставлен счет через MAX бот\n"
            f"Организация: {task.org_name}\n"
            f"org_id: {org_id if org_id is not None else '-'}\n"
            f"Max user id: {task.user_id}\n"
            f"Количество: {task.count}\n"
            f"Счет № ИП {task.invoice_number}\n"
            f"Акт № ИП {task.invoice_number}\n"
            f"Цена за 1 услугу: {price_per_item} руб.\n"
            f"Сумма: {total_sum} руб."
        ),
        "isUrgent": False,
        "isTemplate": False,
        "deadline": {
            "contentType": "DateTime",
            "value": deadline,
        },
    }

    if config.auditor_ids:
        payload["auditors"] = [
            {"contentType": "Employee", "id": auditor_id} for auditor_id in config.auditor_ids
        ]

    response = requests.post(
        config.url,
        headers={
            "Authorization": f"Bearer {config.token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    if not response.ok:
        raise RuntimeError(
            f"MegaPlan API error [{response.status_code}]: "
            f"{(response.text or '').strip()[:1000]}"
        )

    task_id: int | None = None
    try:
        response_json = response.json()
        raw_task_id = response_json.get("data", {}).get("id")
        if raw_task_id is not None:
            task_id = int(raw_task_id)
    except Exception:  # noqa: BLE001
        pass

    return task_id


def run_forever() -> None:
    load_dotenv()
    config = _load_config()
    _configure_logging(config.timezone)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    logging.info("Python worker started: id=%s", worker_id)
    logging.info(
        "Worker config: poll=%ss templates_dir=%s output_dir=%s timezone=%s",
        config.poll_interval_seconds,
        config.templates_dir,
        config.output_dir,
        config.timezone_name,
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
                        task = _build_task(row, config.timezone)
                        result = generate_documents(
                            task=task,
                            templates_dir=config.templates_dir,
                            output_dir=config.output_dir,
                            timezone=config.timezone,
                        )
                        if result.status != "success":
                            raise RuntimeError(result.error_message or "Document generation failed")
                        pdf_paths = _extract_pdf_paths(result.pdf_files)

                        _send_invoice_email(config.smtp, task, pdf_paths)
                        megaplan_task_id = _create_megaplan_task(
                            config.megaplan,
                            task,
                            row.get("org_id"),
                            task.price_per_item,
                            config.timezone,
                        )
                        if megaplan_task_id is not None:
                            logging.info(
                                "MegaPlan task created for invoice id=%s: task_id=%s",
                                invoice_id,
                                megaplan_task_id,
                            )

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
