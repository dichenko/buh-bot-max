from __future__ import annotations

import datetime as dt
import logging
import os
import smtplib
import time
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg
from dotenv import load_dotenv
from openpyxl import Workbook
from psycopg import sql
from psycopg.rows import dict_row

DEFAULT_TIMEZONE_NAME = "Europe/Moscow"
DEFAULT_REPORT_TIME = "21:00:01"
ALLOWED_REPORT_TABLES = ("invoices_ua", "invoices_av", "invoices_3", "invoices_ip")
REPORT_HEADERS = (
    "Название организации",
    "ИНН организации",
    "Префикс счета",
    "Номер счета",
    "Прайс",
    "Количество услуг",
    "Дата",
    "ID Кто завел заявку",
)


class TimezoneAwareFormatter(logging.Formatter):
    def __init__(self, fmt: str, datefmt: str, timezone: dt.tzinfo):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._timezone = timezone

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        current = dt.datetime.fromtimestamp(record.created, tz=self._timezone)
        if datefmt:
            return current.strftime(datefmt)
        return current.isoformat(timespec="seconds")


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
class ReportWorkerConfig:
    database_url: str
    timezone_name: str
    timezone: dt.tzinfo
    poll_interval_seconds: float
    retry_seconds: float
    enabled: bool
    report_time: dt.time
    report_tables: list[str]
    reports_dir: Path
    smtp: SmtpConfig


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


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required for report worker")
    return value


def _parse_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


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


def _parse_recipients(raw: str) -> list[str]:
    recipients = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
    if not recipients:
        raise ValueError("ADMIN_EMAIL must contain at least one recipient")
    return recipients


def _parse_report_time(value: str) -> dt.time:
    normalized = value.strip()
    formats = ("%H:%M:%S", "%H:%M")
    for fmt in formats:
        try:
            return dt.datetime.strptime(normalized, fmt).time()
        except ValueError:
            continue
    raise ValueError("DAILY_REPORT_TIME must be in HH:MM or HH:MM:SS format")


def _parse_report_tables(raw: str) -> list[str]:
    parsed = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
    if not parsed:
        raise ValueError("DAILY_REPORT_TABLES must contain at least one table name")

    unique: list[str] = []
    for table in parsed:
        if table not in ALLOWED_REPORT_TABLES:
            allowed = ", ".join(ALLOWED_REPORT_TABLES)
            raise ValueError(f"Unsupported table in DAILY_REPORT_TABLES: {table}. Allowed: {allowed}")
        if table not in unique:
            unique.append(table)
    return unique


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


def _load_config() -> ReportWorkerConfig:
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        raise ValueError("DATABASE_URL is required for report worker")

    timezone_name = (
        os.getenv("BOT_TIMEZONE")
        or os.getenv("APP_TIMEZONE")
        or DEFAULT_TIMEZONE_NAME
    ).strip() or DEFAULT_TIMEZONE_NAME
    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown timezone: {timezone_name}") from exc

    report_time_raw = (os.getenv("DAILY_REPORT_TIME") or DEFAULT_REPORT_TIME).strip()
    report_tables_raw = (
        os.getenv("DAILY_REPORT_TABLES")
        or "invoices_ua,invoices_av,invoices_3,invoices_ip"
    ).strip()
    reports_dir = Path((os.getenv("DAILY_REPORT_OUTPUT_DIR") or "/app/reports").strip())

    return ReportWorkerConfig(
        database_url=database_url,
        timezone_name=timezone_name,
        timezone=timezone,
        poll_interval_seconds=_parse_positive_float("REPORT_WORKER_POLL_INTERVAL_SECONDS", 1.0),
        retry_seconds=_parse_positive_float("REPORT_WORKER_RETRY_SECONDS", 300.0),
        enabled=_parse_bool("DAILY_REPORT_ENABLED", default=True),
        report_time=_parse_report_time(report_time_raw),
        report_tables=_parse_report_tables(report_tables_raw),
        reports_dir=reports_dir,
        smtp=_load_smtp_config(),
    )


def _day_bounds(report_date: dt.date, timezone: dt.tzinfo) -> tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.combine(report_date, dt.time.min, tzinfo=timezone)
    end = start + dt.timedelta(days=1)
    return start, end


def _normalize_excel_date(value: Any, timezone: dt.tzinfo) -> Any:
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone).replace(tzinfo=None)
        return value
    return value


def _export_table_report(
    conn: psycopg.Connection[Any],
    table_name: str,
    report_date: dt.date,
    reports_dir: Path,
    timezone: dt.tzinfo,
) -> Path | None:
    day_start, day_end = _day_bounds(report_date, timezone)
    query = sql.SQL(
        """
        SELECT org_name, org_inn, org_count, number, org_price, count, date, user_id
        FROM {}
        WHERE date >= %s AND date < %s
        ORDER BY id ASC
        """
    ).format(sql.Identifier(table_name))

    with conn.cursor() as cur:
        cur.execute(query, (day_start, day_end))
        rows = cur.fetchall()

    if not rows:
        logging.info("Daily report: table %s has no rows for %s", table_name, report_date.isoformat())
        return None

    reports_dir.mkdir(parents=True, exist_ok=True)

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = table_name
    sheet.append(list(REPORT_HEADERS))

    for row in rows:
        sheet.append(
            [
                row.get("org_name"),
                row.get("org_inn"),
                row.get("org_count"),
                row.get("number"),
                row.get("org_price"),
                row.get("count"),
                _normalize_excel_date(row.get("date"), timezone),
                row.get("user_id"),
            ]
        )

    output_path = reports_dir / f"{table_name}_{report_date.isoformat()}.xlsx"
    workbook.save(output_path)
    workbook.close()

    logging.info(
        "Daily report exported: table=%s rows=%s file=%s",
        table_name,
        len(rows),
        output_path,
    )
    return output_path


def _send_daily_report_email(
    config: SmtpConfig,
    report_date: dt.date,
    attachments: list[Path],
    report_tables: list[str],
) -> None:
    message = EmailMessage()
    message["From"] = config.from_address
    message["To"] = ", ".join(config.recipients)
    message["Subject"] = f"Суточный отчет за {report_date.isoformat()}"

    if attachments:
        attachment_names = "\n".join(f"- {path.name}" for path in attachments)
        message.set_content(
            "\n".join(
                [
                    f"Отчеты за {report_date.isoformat()} сформированы.",
                    "",
                    "Вложения:",
                    attachment_names,
                ]
            )
        )
    else:
        tables = ", ".join(report_tables)
        message.set_content(
            f"За {report_date.isoformat()} данные для отчета не найдены.\nПроверенные таблицы: {tables}"
        )

    for path in attachments:
        with path.open("rb") as stream:
            message.add_attachment(
                stream.read(),
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=path.name,
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


def _cleanup_report_files(reports_dir: Path, files: list[Path]) -> None:
    for file_path in files:
        try:
            resolved_file = file_path.resolve(strict=False)
            resolved_reports_dir = reports_dir.resolve(strict=False)
            if resolved_reports_dir in resolved_file.parents:
                file_path.unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            logging.warning("Failed to remove report artifact: %s", file_path, exc_info=True)

    try:
        if reports_dir.exists() and not any(reports_dir.iterdir()):
            reports_dir.rmdir()
    except Exception:  # noqa: BLE001
        logging.warning("Failed to cleanup reports dir: %s", reports_dir, exc_info=True)


def _run_daily_report(config: ReportWorkerConfig, report_date: dt.date) -> None:
    logging.info("Daily report run started for date=%s", report_date.isoformat())
    generated_files: list[Path] = []

    try:
        with psycopg.connect(
            config.database_url,
            row_factory=dict_row,
            options=f"-c timezone={config.timezone_name}",
        ) as conn:
            for table_name in config.report_tables:
                exported = _export_table_report(
                    conn=conn,
                    table_name=table_name,
                    report_date=report_date,
                    reports_dir=config.reports_dir,
                    timezone=config.timezone,
                )
                if exported is not None:
                    generated_files.append(exported)

        _send_daily_report_email(
            config=config.smtp,
            report_date=report_date,
            attachments=generated_files,
            report_tables=config.report_tables,
        )
        logging.info(
            "Daily report email sent for date=%s attachments=%s",
            report_date.isoformat(),
            [path.name for path in generated_files],
        )
    finally:
        _cleanup_report_files(config.reports_dir, generated_files)


def run_forever() -> None:
    load_dotenv()
    config = _load_config()
    _configure_logging(config.timezone)

    logging.info(
        "Report worker started: enabled=%s report_time=%s timezone=%s tables=%s",
        config.enabled,
        config.report_time.strftime("%H:%M:%S"),
        config.timezone_name,
        ",".join(config.report_tables),
    )

    last_success_date: dt.date | None = None
    next_attempt_not_before_ts = 0.0

    while True:
        now = dt.datetime.now(config.timezone)

        if not config.enabled:
            time.sleep(config.poll_interval_seconds)
            continue

        should_run_today = now.time() >= config.report_time
        already_sent_today = last_success_date == now.date()
        can_retry = time.time() >= next_attempt_not_before_ts

        if should_run_today and not already_sent_today and can_retry:
            try:
                _run_daily_report(config, now.date())
                last_success_date = now.date()
                next_attempt_not_before_ts = 0.0
            except Exception:  # noqa: BLE001
                logging.exception("Daily report run failed for date=%s", now.date().isoformat())
                next_attempt_not_before_ts = time.time() + config.retry_seconds
                logging.warning(
                    "Next daily report retry not earlier than %ss",
                    config.retry_seconds,
                )

        time.sleep(config.poll_interval_seconds)


if __name__ == "__main__":
    run_forever()
