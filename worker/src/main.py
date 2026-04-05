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
from typing import Any, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.rows import dict_row

from .excel_pdf_worker import DocumentTask, WorkerResult, generate_documents

DEFAULT_TIMEZONE_NAME = "Europe/Moscow"
DEFAULT_MAX_API_BASE_URL = "https://platform-api.max.ru"


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
class MaxBotConfig:
    token: str
    api_base_url: str
    request_timeout_seconds: float
    retries: int
    retry_delay_seconds: float
    attachment_ready_retry_delay_seconds: float


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
    max_bot: MaxBotConfig


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


def _normalize_base_url(raw: str | None, default: str) -> str:
    candidate = (raw or "").strip()
    if not candidate:
        return default

    if not candidate.startswith(("http://", "https://")):
        candidate = f"https://{candidate}"

    return candidate.rstrip("/")


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


def _load_max_bot_config() -> MaxBotConfig:
    token = _require_env("MAX_BOT_TOKEN")
    api_base_url = _normalize_base_url(os.getenv("MAX_API_BASE_URL"), DEFAULT_MAX_API_BASE_URL)

    return MaxBotConfig(
        token=token,
        api_base_url=api_base_url,
        request_timeout_seconds=_parse_positive_float("MAX_API_TIMEOUT_SECONDS", 30.0),
        retries=_parse_positive_int("MAX_API_RETRIES", 5),
        retry_delay_seconds=_parse_positive_float("MAX_API_RETRY_DELAY_SECONDS", 2.0),
        attachment_ready_retry_delay_seconds=_parse_positive_float(
            "MAX_ATTACHMENT_READY_RETRY_DELAY_SECONDS", 1.0
        ),
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
        max_bot=_load_max_bot_config(),
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


class MaxApiError(RuntimeError):
    def __init__(
        self,
        action: str,
        status_code: int,
        code: str | None = None,
        message: str | None = None,
        retry_after_seconds: float | None = None,
    ):
        details = message or "Unknown MAX API error"
        code_part = f", code={code}" if code else ""
        super().__init__(f"MAX API {action} failed [{status_code}{code_part}]: {details}")
        self.action = action
        self.status_code = status_code
        self.code = code or ""
        self.message = details
        self.retry_after_seconds = retry_after_seconds


def _extract_retry_after_seconds(response: requests.Response) -> float | None:
    raw_value = response.headers.get("Retry-After")
    if not raw_value:
        return None

    try:
        parsed = float(raw_value)
    except ValueError:
        return None

    if parsed <= 0:
        return None
    return parsed


def _extract_max_error_payload(response: requests.Response) -> tuple[str, str]:
    try:
        payload = response.json()
    except ValueError:
        body = (response.text or "").strip()
        return "", body[:1000] if body else "Unknown MAX API error"

    if not isinstance(payload, dict):
        return "", str(payload)[:1000]

    code_raw = payload.get("code")
    message_raw = payload.get("message")

    code = str(code_raw).strip() if code_raw is not None else ""
    message = str(message_raw).strip() if message_raw is not None else ""
    if not message:
        message = str(payload)[:1000]

    return code, message


def _raise_for_max_response(action: str, response: requests.Response) -> None:
    if response.ok:
        return

    code, message = _extract_max_error_payload(response)
    raise MaxApiError(
        action=action,
        status_code=response.status_code,
        code=code,
        message=message,
        retry_after_seconds=_extract_retry_after_seconds(response),
    )


def _is_retryable_max_error(error: MaxApiError) -> bool:
    return (
        error.code == "attachment.not.ready"
        or error.status_code == 429
        or error.status_code >= 500
    )


def _max_retry_delay_seconds(config: MaxBotConfig, error: MaxApiError, attempt: int) -> float:
    if error.code == "attachment.not.ready":
        return config.attachment_ready_retry_delay_seconds

    if error.retry_after_seconds is not None:
        return error.retry_after_seconds

    exponential_backoff = config.retry_delay_seconds * max(1, attempt)
    return min(exponential_backoff, 30.0)


def _run_with_max_retries(
    config: MaxBotConfig,
    operation_name: str,
    operation: Callable[[], Any],
) -> Any:
    max_attempts = max(1, config.retries)
    attempt = 0

    while attempt < max_attempts:
        attempt += 1
        try:
            return operation()
        except MaxApiError as error:
            if not _is_retryable_max_error(error) or attempt >= max_attempts:
                raise

            sleep_seconds = _max_retry_delay_seconds(config, error, attempt)
            logging.warning(
                "MAX retryable error on %s (attempt %s/%s): %s. Retry in %.1fs",
                operation_name,
                attempt,
                max_attempts,
                error,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
        except requests.RequestException as error:
            if attempt >= max_attempts:
                raise RuntimeError(
                    f"MAX network failure on {operation_name}: {error}"
                ) from error

            sleep_seconds = min(config.retry_delay_seconds * max(1, attempt), 30.0)
            logging.warning(
                "MAX network error on %s (attempt %s/%s): %s. Retry in %.1fs",
                operation_name,
                attempt,
                max_attempts,
                error,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"MAX operation failed without details: {operation_name}")


def _max_headers(config: MaxBotConfig, include_json: bool = False) -> dict[str, str]:
    headers = {"Authorization": config.token}
    if include_json:
        headers["Content-Type"] = "application/json"
    return headers


def _max_api_url(config: MaxBotConfig, path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{config.api_base_url}{normalized_path}"


def _max_get_upload_url(config: MaxBotConfig) -> tuple[str, str | None]:
    response = requests.post(
        _max_api_url(config, "/uploads"),
        params={"type": "file"},
        headers=_max_headers(config, include_json=False),
        timeout=config.request_timeout_seconds,
    )
    _raise_for_max_response("POST /uploads", response)

    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("MAX /uploads returned invalid response type")

    upload_url_raw = payload.get("url")
    if not isinstance(upload_url_raw, str) or not upload_url_raw.strip():
        raise RuntimeError("MAX /uploads returned empty url")

    token_raw = payload.get("token")
    token = str(token_raw).strip() if token_raw is not None else None
    return upload_url_raw.strip(), token or None


def _max_upload_file_multipart(config: MaxBotConfig, upload_url: str, pdf_path: Path) -> str:
    with pdf_path.open("rb") as stream:
        response = requests.post(
            upload_url,
            files={"data": (pdf_path.name, stream, "application/pdf")},
            timeout=config.request_timeout_seconds,
        )
    _raise_for_max_response(f"upload file {pdf_path.name} (multipart)", response)

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"MAX upload response is not JSON for {pdf_path.name}: {(response.text or '').strip()[:500]}"
        ) from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"MAX upload response has invalid shape for {pdf_path.name}")

    token_raw = payload.get("token")
    token = str(token_raw).strip() if token_raw is not None else ""
    if not token:
        raise RuntimeError(f"MAX upload response does not contain token for {pdf_path.name}")

    return token


def _max_upload_file_range(
    config: MaxBotConfig,
    upload_url: str,
    upload_token: str,
    pdf_path: Path,
) -> str:
    file_size = pdf_path.stat().st_size
    if file_size <= 0:
        raise RuntimeError(f"File is empty: {pdf_path}")

    start = 0
    chunk_size = 1024 * 1024
    safe_name = pdf_path.name.replace('"', "")

    with pdf_path.open("rb") as stream:
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break

            end = start + len(chunk) - 1
            response = requests.post(
                upload_url,
                data=chunk,
                headers={
                    "Content-Disposition": f'attachment; filename="{safe_name}"',
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Content-Type": "application/x-binary; charset=x-user-defined",
                    "X-File-Name": safe_name,
                    "X-Uploading-Mode": "parallel",
                    "Connection": "keep-alive",
                },
                timeout=config.request_timeout_seconds,
            )
            _raise_for_max_response(f"upload file {pdf_path.name} (range)", response)
            start = end + 1

    if start != file_size:
        raise RuntimeError(
            f"MAX range upload did not send full file {pdf_path.name}: sent={start} expected={file_size}"
        )

    return upload_token


def _max_upload_pdf(config: MaxBotConfig, pdf_path: Path) -> str:
    if not pdf_path.exists() or not pdf_path.is_file():
        raise RuntimeError(f"PDF file does not exist: {pdf_path}")

    def _upload_once() -> str:
        upload_url, upload_token = _max_get_upload_url(config)
        if upload_token:
            return _max_upload_file_range(config, upload_url, upload_token, pdf_path)
        return _max_upload_file_multipart(config, upload_url, pdf_path)

    return _run_with_max_retries(
        config,
        operation_name=f"upload {pdf_path.name} to MAX",
        operation=_upload_once,
    )


def _max_send_message_with_attachments(
    config: MaxBotConfig,
    user_id: int,
    text: str,
    attachment_tokens: list[str],
) -> None:
    total = len(attachment_tokens)
    for index, token in enumerate(attachment_tokens, start=1):
        message_text = text if index == 1 else f"Документ {index}/{total}"
        payload = {
            "text": message_text,
            "attachments": [{"type": "file", "payload": {"token": token}}],
        }

        def _send_once() -> None:
            response = requests.post(
                _max_api_url(config, "/messages"),
                params={"user_id": user_id},
                headers=_max_headers(config, include_json=True),
                json=payload,
                timeout=config.request_timeout_seconds,
            )
            _raise_for_max_response("POST /messages", response)

        _run_with_max_retries(
            config,
            operation_name=f"send document {index}/{total} to MAX user_id={user_id}",
            operation=_send_once,
        )


def _send_documents_to_max_user(
    config: MaxBotConfig,
    task: DocumentTask,
    pdf_paths: list[Path],
) -> None:
    if not pdf_paths:
        raise RuntimeError("No PDF files to send via MAX")

    tokens: list[str] = []
    for pdf_path in pdf_paths:
        token = _max_upload_pdf(config, pdf_path)
        tokens.append(token)
        logging.info(
            "MAX file uploaded for user_id=%s invoice=%s file=%s",
            task.user_id,
            task.invoice_number,
            pdf_path.name,
        )

    _max_send_message_with_attachments(
        config=config,
        user_id=task.user_id,
        text=(
            f"Документы по заявке сформированы.\n"
            f"Счет №{task.invoice_number}.\n"
            f"Организация: {task.org_name}."
        ),
        attachment_tokens=tokens,
    )
    logging.info(
        "MAX documents sent to user_id=%s invoice=%s files=%s",
        task.user_id,
        task.invoice_number,
        [path.name for path in pdf_paths],
    )


def run_forever() -> None:
    load_dotenv()
    config = _load_config()
    _configure_logging(config.timezone)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    logging.info("Python worker started: id=%s", worker_id)
    logging.info(
        "Worker config: poll=%ss templates_dir=%s output_dir=%s timezone=%s max_api=%s",
        config.poll_interval_seconds,
        config.templates_dir,
        config.output_dir,
        config.timezone_name,
        config.max_bot.api_base_url,
    )

    last_idle_log_ts = 0.0

    while True:
        try:
            with psycopg.connect(
                config.database_url,
                row_factory=dict_row,
                options=f"-c timezone={config.timezone_name}",
            ) as conn:
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
                        _send_documents_to_max_user(config.max_bot, task, pdf_paths)
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
