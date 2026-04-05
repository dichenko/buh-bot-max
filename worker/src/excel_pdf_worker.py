from __future__ import annotations

import datetime as dt
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo

import openpyxl
from num2words import num2words

DEFAULT_TIMEZONE = ZoneInfo("Europe/Moscow")


@dataclass
class DocumentTask:
    user_id: int
    org_name: str
    org_inn: str | int
    count: int
    price_per_item: int | float
    invoice_number: int | str
    work_date: dt.date | None = None


@dataclass
class WorkerResult:
    status: str
    pdf_files: List[str]
    workspace_path: str
    error_message: str | None = None


def _sanitize_for_filename(value: str) -> str:
    value = re.sub(r"\s+", " ", value).strip()
    sanitized = "".join(ch for ch in value if ch.isalnum() or ch in " -_")
    return sanitized or "organization"


def _ensure_template(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Template not found: {path}")


def _xlsx_to_pdf(xlsx_file: Path) -> Path:
    pdf_file = xlsx_file.with_suffix(".pdf")
    unoconv_path = shutil.which("unoconv")
    libreoffice_path = shutil.which("libreoffice")

    if unoconv_path:
        completed = subprocess.run(
            [unoconv_path, "-f", "pdf", "-o", str(pdf_file), str(xlsx_file)],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "unoconv conversion failed: "
                f"{(completed.stderr or completed.stdout or '').strip()}"
            )
    elif libreoffice_path:
        completed = subprocess.run(
            [
                libreoffice_path,
                "--headless",
                "--convert-to",
                "pdf",
                "--outdir",
                str(xlsx_file.parent),
                str(xlsx_file),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "libreoffice conversion failed: "
                f"{(completed.stderr or completed.stdout or '').strip()}"
            )
    else:
        raise RuntimeError("PDF conversion tool is missing (unoconv/libreoffice not installed)")

    if not pdf_file.exists():
        raise RuntimeError(f"PDF file was not created: {pdf_file}")

    xlsx_file.unlink(missing_ok=True)
    return pdf_file


def _fill_akt(template_path: Path, output_path: Path, task: DocumentTask, total_sum: int | float, date_str: str) -> None:
    workbook = openpyxl.load_workbook(template_path)
    sheet = workbook.active

    sheet["B3"] = f"Акт № ИП-{task.invoice_number} от {date_str}"
    sheet["F7"] = f"{task.org_name}, ИНН {task.org_inn}"
    sheet["F9"] = f"Счёт-договор № {task.invoice_number} от {date_str}"
    sheet["U13"] = f"{task.count}"
    sheet["Z13"] = f"{task.price_per_item}"
    sheet["AD13"] = f"{total_sum}"
    sheet["AD15"] = f"{total_sum}"
    sheet["B18"] = f"Всего наименований 1, на сумму {total_sum} руб."
    sheet["B19"] = f"({num2words(int(total_sum), lang='ru')} рублей 00 копеек)"
    sheet["U26"] = task.org_name

    workbook.save(output_path)


def _fill_invoice(
    template_path: Path,
    output_path: Path,
    task: DocumentTask,
    total_sum: int | float,
    date_str: str,
) -> None:
    workbook = openpyxl.load_workbook(template_path)
    sheet = workbook.active

    sheet["A10"] = f"Счет-договор № {task.invoice_number} от {date_str}"
    sheet["D18"] = f"{task.org_name}, ИНН {task.org_inn}"
    sheet["G25"] = f"{task.count}"
    sheet["J25"] = f"{task.price_per_item}"
    sheet["K25"] = f"{total_sum}"
    sheet["M29"] = f"{total_sum}"
    sheet["M32"] = f"{total_sum}"
    sheet["A34"] = f"Всего наименований 1, количество - {task.count} на сумму {total_sum} рублей"
    sheet["A35"] = f"({num2words(int(total_sum), lang='ru')} рублей 00 копеек)"

    workbook.save(output_path)


def generate_documents(
    task: DocumentTask,
    templates_dir: Path | str = Path("templates"),
    output_dir: Path | str = Path("obrazec"),
    timezone: dt.tzinfo = DEFAULT_TIMEZONE,
) -> WorkerResult:
    if task.count <= 0:
        return WorkerResult(
            status="error",
            pdf_files=[],
            workspace_path="",
            error_message="count must be positive",
        )

    templates = Path(templates_dir)
    out_root = Path(output_dir)
    akt_template = templates / "akt.xlsx"
    invoice_template = templates / "invoice.xlsx"

    try:
        _ensure_template(akt_template)
        _ensure_template(invoice_template)

        now = dt.datetime.now(timezone)
        work_date = task.work_date or now.date()
        date_str = work_date.strftime("%d.%m.%Y")
        total_sum = task.count * task.price_per_item

        workspace = out_root / str(task.user_id)
        workspace.mkdir(parents=True, exist_ok=True)

        org_name_clean = _sanitize_for_filename(task.org_name)
        timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

        akt_xlsx = workspace / f"Акт-ИП-{timestamp}_{org_name_clean}.xlsx"
        invoice_xlsx = workspace / f"Счет-ИП-{timestamp}_{org_name_clean}.xlsx"

        _fill_akt(akt_template, akt_xlsx, task, total_sum, date_str)
        _fill_invoice(invoice_template, invoice_xlsx, task, total_sum, date_str)

        pdf_files = [_xlsx_to_pdf(akt_xlsx), _xlsx_to_pdf(invoice_xlsx)]

        return WorkerResult(
            status="success",
            pdf_files=[str(path) for path in pdf_files],
            workspace_path=str(workspace),
        )
    except Exception as exc:  # noqa: BLE001
        return WorkerResult(
            status="error",
            pdf_files=[],
            workspace_path=str(out_root / str(task.user_id)),
            error_message=str(exc),
        )


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    if _env_bool("WORKER_DEMO", default=False):
        demo_task = DocumentTask(
            user_id=1,
            org_name="ООО Демонстрация",
            org_inn="1234567890",
            count=10,
            price_per_item=100,
            invoice_number=1,
        )
        print(generate_documents(demo_task))
    else:
        print("Set WORKER_DEMO=true to run demo document generation")
