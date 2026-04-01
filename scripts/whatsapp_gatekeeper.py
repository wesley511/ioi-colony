from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any


# ============================================================
# CONFIG
# ============================================================

ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = ROOT / "DATA"
LOGS_DIR = ROOT / "LOGS"
RAW_INPUT_DIR = ROOT / "RAW_INPUT" / "whatsapp"
NORMALIZED_DIR = ROOT / "SIGNALS" / "normalized"
QUARANTINE_DIR = ROOT / "SIGNALS" / "quarantine_duplicates"
INVALID_DIR = ROOT / "SIGNALS" / "quarantine_invalid"
STATE_FILE = DATA_DIR / "whatsapp_gatekeeper_state.json"

ALLOWED_BRANCHES = {
    "waigani",
    "bena_road",
    "lae_5th_street",
    "lae_malaita",
}

ALLOWED_INVENTORY_STATUS = {
    "EMPTY",
    "VERY_TIGHT",
    "TIGHT",
    "NORMAL",
    "LOOSE",
    "FULL",
}

ALLOWED_BALE_STATUS = {
    "RELEASED",
    "HOLD",
    "PENDING",
}

ALLOWED_EXCEPTION_TYPES = {
    "STOCK_OUT",
    "PRICING_ERROR",
    "STAFF_ISSUE",
    "DISPLAY_ISSUE",
    "SYSTEM_ISSUE",
}

CLASSIFIERS = {
    "DAY-END SALES REPORT": "sales_report",
    "INVENTORY AVAILABILITY REPORT": "inventory_report",
    "DAILY BALE SUMMARY – RELEASED TO RAIL": "bale_report",
    "DAILY BALE SUMMARY - RELEASED TO RAIL": "bale_report",
    "STAFF PERFORMANCE REPORT": "staff_report",
    "SUPERVISOR CONTROL REPORT": "supervisor_report",
}

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{2}$")
TIME_RE = re.compile(r"^\d{2}:\d{2}$")
INT_RE = re.compile(r"^-?\d+$")
NUMBER_RE = re.compile(r"^-?\d+(?:\.\d+)?$")


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class ValidationResult:
    ok: bool
    classifier: str | None
    report_type: str | None
    errors: list[str]
    normalized: dict[str, Any] | None


# ============================================================
# HELPERS
# ============================================================

def ensure_dirs() -> None:
    for d in [
        DATA_DIR,
        LOGS_DIR,
        RAW_INPUT_DIR,
        NORMALIZED_DIR,
        QUARANTINE_DIR,
        INVALID_DIR,
    ]:
        d.mkdir(parents=True, exist_ok=True)


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {"seen_hashes": {}}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"seen_hashes": {}}


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.strip().encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def parse_key_value_line(line: str) -> tuple[str, str] | None:
    if ":" not in line:
        return None
    left, right = line.split(":", 1)
    key = left.strip()
    value = right.strip()
    if not key:
        return None
    return key, value


def is_number(value: str) -> bool:
    return bool(NUMBER_RE.fullmatch(value))


def is_int(value: str) -> bool:
    return bool(INT_RE.fullmatch(value))


def to_number(value: str) -> float:
    return float(value)


def to_int(value: str) -> int:
    return int(value)


def normalize_branch(value: str) -> str:
    return value.strip().lower().replace(" ", "_")


def validate_date(value: str) -> bool:
    return bool(DATE_RE.fullmatch(value))


def validate_time(value: str) -> bool:
    return bool(TIME_RE.fullmatch(value))


def sanitize_filename(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", value)


def split_nonempty_lines(text: str) -> list[str]:
    return [line.rstrip() for line in text.splitlines() if line.strip()]


def read_master_sections() -> set[str]:
    """
    Reads canonical section names from MASTER_DATA/branches/*_sections.yaml.

    Supports YAML shaped like:

    sections:
      - id: 1
        name: shoes_and_sandals
      - id: 2
        name: mens_tshirt
    """
    sections_dir = ROOT / "MASTER_DATA" / "branches"
    results: set[str] = set()

    if not sections_dir.exists():
        return results

    for path in sections_dir.glob("*_sections.yaml"):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue

        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("name:"):
                section_name = line.split(":", 1)[1].strip()
                if section_name:
                    results.add(section_name)

    return results


CANONICAL_SECTIONS = read_master_sections()


def validate_section_name(value: str) -> bool:
    if not value:
        return False
    if not CANONICAL_SECTIONS:
        # Fallback: allow safe slug-like section names if master data is absent.
        return bool(re.fullmatch(r"[A-Za-z0-9_]+", value))
    return value in CANONICAL_SECTIONS


def extract_header(lines: list[str]) -> tuple[str | None, str | None, list[str]]:
    if not lines:
        return None, None, []
    first = lines[0].strip()
    classifier = CLASSIFIERS.get(first)
    return first, classifier, lines[1:]


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def save_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def reject_payload(
    raw_text: str,
    classifier_line: str | None,
    inferred_type: str | None,
    reasons: list[str],
    raw_hash: str,
) -> dict[str, Any]:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    payload = {
        "status": "rejected",
        "classifier_line": classifier_line,
        "report_type": inferred_type,
        "reasons": reasons,
        "sha256": raw_hash,
        "ingested_at": utc_now_iso(),
        "raw_text": raw_text,
    }
    filename = sanitize_filename(f"{timestamp}_{raw_hash[:12]}.json")
    save_json(INVALID_DIR / filename, payload)
    return payload


def quarantine_duplicate(raw_text: str, raw_hash: str, existing_meta: dict[str, Any]) -> dict[str, Any]:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    payload = {
        "status": "duplicate",
        "sha256": raw_hash,
        "ingested_at": utc_now_iso(),
        "existing_meta": existing_meta,
        "raw_text": raw_text,
    }
    filename = sanitize_filename(f"{timestamp}_{raw_hash[:12]}.json")
    save_json(QUARANTINE_DIR / filename, payload)
    return payload


# ============================================================
# STRICT PARSERS
# ============================================================

def validate_sales_report(lines: list[str]) -> ValidationResult:
    errors: list[str] = []
    fields: dict[str, str] = {}

    for line in lines:
        kv = parse_key_value_line(line)
        if kv:
            fields[kv[0]] = kv[1]

    required = [
        "Branch",
        "Date",
        "Z Reading",
        "Cash Sales",
        "EFTPOS Sales",
        "Total Customers (Traffic)",
        "Customers Served",
        "Staff on Duty",
        "Cash Variance",
        "Over/Short Reason",
        "Supervisor Confirmed",
    ]
    for key in required:
        if key not in fields:
            errors.append(f"Missing required field: {key}")

    branch = normalize_branch(fields.get("Branch", ""))
    if branch and branch not in ALLOWED_BRANCHES:
        errors.append(f"Invalid branch: {branch}")

    report_date = fields.get("Date", "")
    if report_date and not validate_date(report_date):
        errors.append("Invalid date format; expected DD/MM/YY")

    numeric_fields = [
        "Z Reading",
        "Cash Sales",
        "EFTPOS Sales",
        "Total Customers (Traffic)",
        "Customers Served",
        "Staff on Duty",
        "Cash Variance",
    ]
    for key in numeric_fields:
        value = fields.get(key, "")
        if value and not is_number(value):
            errors.append(f"Field must be numeric: {key}")

    if not errors:
        z_reading = to_number(fields["Z Reading"])
        cash_sales = to_number(fields["Cash Sales"])
        eftpos_sales = to_number(fields["EFTPOS Sales"])
        traffic = to_int(fields["Total Customers (Traffic)"])
        served = to_int(fields["Customers Served"])
        staff_on_duty = to_int(fields["Staff on Duty"])

        if served > traffic:
            errors.append("Customers Served cannot exceed Total Customers (Traffic)")
        if staff_on_duty < 0:
            errors.append("Staff on Duty cannot be negative")

        difference = abs((cash_sales + eftpos_sales) - z_reading)
        if difference > 1.0:
            errors.append(
                f"Z Reading mismatch: Cash Sales + EFTPOS Sales differs from Z Reading by {difference:.2f}"
            )

        supervisor_confirmed = fields["Supervisor Confirmed"].upper()
        if supervisor_confirmed not in {"YES", "NO"}:
            errors.append("Supervisor Confirmed must be YES or NO")

    normalized = None
    if not errors:
        notes = fields.get("Notes", "")
        normalized = {
            "signal_type": "daily_sales_report",
            "branch": branch,
            "date": report_date,
            "report_type": "sales_report",
            "totals": {
                "z_reading": to_number(fields["Z Reading"]),
                "cash_sales": to_number(fields["Cash Sales"]),
                "eftpos_sales": to_number(fields["EFTPOS Sales"]),
                "cash_variance": to_number(fields["Cash Variance"]),
            },
            "traffic": {
                "total_customers": to_int(fields["Total Customers (Traffic)"]),
                "customers_served": to_int(fields["Customers Served"]),
            },
            "staffing": {
                "staff_on_duty": to_int(fields["Staff on Duty"]),
            },
            "control": {
                "over_short_reason": fields["Over/Short Reason"],
                "supervisor_confirmed": fields["Supervisor Confirmed"].upper(),
            },
            "notes": notes,
        }

    return ValidationResult(
        ok=not errors,
        classifier="DAY-END SALES REPORT",
        report_type="sales_report",
        errors=errors,
        normalized=normalized,
    )


def validate_inventory_report(lines: list[str]) -> ValidationResult:
    errors: list[str] = []
    header_fields: dict[str, str] = {}
    section_rows: list[dict[str, str]] = []
    notes = ""

    phase = "header"

    for line in lines:
        kv = parse_key_value_line(line)
        if not kv:
            errors.append(f"Invalid line format: {line}")
            continue

        key, value = kv

        if key in {"Branch", "Date"} and phase == "header":
            header_fields[key] = value
            continue

        phase = "body"

        if key == "Notes":
            notes = value
            continue

        section_rows.append({"section": key, "status": value})

    if "Branch" not in header_fields:
        errors.append("Missing required field: Branch")
    if "Date" not in header_fields:
        errors.append("Missing required field: Date")
    if not section_rows:
        errors.append("At least one section status row is required")

    branch = normalize_branch(header_fields.get("Branch", ""))
    if branch and branch not in ALLOWED_BRANCHES:
        errors.append(f"Invalid branch: {branch}")

    report_date = header_fields.get("Date", "")
    if report_date and not validate_date(report_date):
        errors.append("Invalid date format; expected DD/MM/YY")

    for row in section_rows:
        section = row["section"]
        status = row["status"].strip().upper()

        if not validate_section_name(section):
            errors.append(f"Invalid section name: {section}")

        if status not in ALLOWED_INVENTORY_STATUS:
            errors.append(f"Invalid inventory status for {section}: {status}")

        row["status"] = status

    normalized = None
    if not errors:
        normalized = {
            "signal_type": "inventory_availability_report",
            "branch": branch,
            "date": report_date,
            "report_type": "inventory_report",
            "sections": section_rows,
            "notes": notes,
        }

    return ValidationResult(
        ok=not errors,
        classifier="INVENTORY AVAILABILITY REPORT",
        report_type="inventory_report",
        errors=errors,
        normalized=normalized,
    )


def validate_bale_report(lines: list[str]) -> ValidationResult:
    errors: list[str] = []
    branch = ""
    report_date = ""
    released_by = ""
    total_qty = None
    total_amount = None

    blocks: list[dict[str, str]] = []
    current: dict[str, str] = {}

    def flush_current() -> None:
        nonlocal current
        if current:
            blocks.append(current)
            current = {}

    for line in lines:
        kv = parse_key_value_line(line)
        if not kv:
            errors.append(f"Invalid line format: {line}")
            continue

        key, value = kv

        if key == "Branch":
            branch = normalize_branch(value)
            continue
        if key == "Date":
            report_date = value
            continue
        if key == "Released By":
            released_by = value
            flush_current()
            continue
        if key == "Total Qty":
            total_qty = value
            flush_current()
            continue
        if key == "Total Amount":
            total_amount = value
            flush_current()
            continue

        if key == "Bale ID" and current:
            flush_current()

        current[key] = value

    flush_current()

    if not branch:
        errors.append("Missing required field: Branch")
    elif branch not in ALLOWED_BRANCHES:
        errors.append(f"Invalid branch: {branch}")

    if not report_date:
        errors.append("Missing required field: Date")
    elif not validate_date(report_date):
        errors.append("Invalid date format; expected DD/MM/YY")

    if not released_by:
        errors.append("Missing required field: Released By")

    if total_qty is None or not is_number(total_qty):
        errors.append("Missing or invalid Total Qty")
    if total_amount is None or not is_number(total_amount):
        errors.append("Missing or invalid Total Amount")

    if not blocks:
        errors.append("At least one bale block is required")

    normalized_blocks: list[dict[str, Any]] = []
    sum_qty = 0
    sum_amount = 0.0

    for idx, block in enumerate(blocks, start=1):
        required = ["Bale ID", "Section", "Qty", "Amount", "Status"]
        for key in required:
            if key not in block:
                errors.append(f"Bale block {idx}: missing {key}")

        bale_id = block.get("Bale ID", "").strip()
        section = block.get("Section", "").strip()
        qty = block.get("Qty", "").strip()
        amount = block.get("Amount", "").strip()
        status = block.get("Status", "").strip().upper()

        if bale_id == "":
            errors.append(f"Bale block {idx}: Bale ID cannot be empty")
        if section and not validate_section_name(section):
            errors.append(f"Bale block {idx}: invalid section {section}")
        if qty and not is_number(qty):
            errors.append(f"Bale block {idx}: Qty must be numeric")
        if amount and not is_number(amount):
            errors.append(f"Bale block {idx}: Amount must be numeric")
        if status and status not in ALLOWED_BALE_STATUS:
            errors.append(f"Bale block {idx}: invalid status {status}")

        if all([bale_id, section, qty, amount, status]) and not errors:
            q = to_int(qty) if is_int(qty) else int(float(qty))
            a = to_number(amount)
            sum_qty += q
            sum_amount += a
            normalized_blocks.append(
                {
                    "bale_id": bale_id,
                    "section": section,
                    "qty": q,
                    "amount": a,
                    "status": status,
                }
            )

    if total_qty is not None and is_number(total_qty):
        if int(float(total_qty)) != sum_qty:
            errors.append(f"Total Qty mismatch: declared {total_qty}, computed {sum_qty}")

    if total_amount is not None and is_number(total_amount):
        if abs(float(total_amount) - sum_amount) > 0.01:
            errors.append(f"Total Amount mismatch: declared {total_amount}, computed {sum_amount:.2f}")

    normalized = None
    if not errors:
        normalized = {
            "signal_type": "daily_bale_summary_report",
            "branch": branch,
            "date": report_date,
            "report_type": "bale_report",
            "released_by": released_by,
            "bales": normalized_blocks,
            "totals": {
                "total_qty": sum_qty,
                "total_amount": round(sum_amount, 2),
            },
        }

    return ValidationResult(
        ok=not errors,
        classifier="DAILY BALE SUMMARY – RELEASED TO RAIL",
        report_type="bale_report",
        errors=errors,
        normalized=normalized,
    )


def validate_staff_report(lines: list[str]) -> ValidationResult:
    errors: list[str] = []
    branch = ""
    report_date = ""
    notes = ""
    current: dict[str, str] = {}
    blocks: list[dict[str, str]] = []

    def flush_current() -> None:
        nonlocal current
        if current:
            blocks.append(current)
            current = {}

    for line in lines:
        kv = parse_key_value_line(line)
        if not kv:
            errors.append(f"Invalid line format: {line}")
            continue

        key, value = kv

        if key == "Branch":
            branch = normalize_branch(value)
            continue
        if key == "Date":
            report_date = value
            continue
        if key == "Notes":
            notes = value
            flush_current()
            continue

        if key == "Staff Name" and current:
            flush_current()

        current[key] = value

    flush_current()

    if not branch:
        errors.append("Missing required field: Branch")
    elif branch not in ALLOWED_BRANCHES:
        errors.append(f"Invalid branch: {branch}")

    if not report_date:
        errors.append("Missing required field: Date")
    elif not validate_date(report_date):
        errors.append("Invalid date format; expected DD/MM/YY")

    if not blocks:
        errors.append("At least one staff block is required")

    normalized_staff: list[dict[str, Any]] = []

    for idx, block in enumerate(blocks, start=1):
        required = [
            "Staff Name",
            "Section",
            "Arrangement",
            "Display",
            "Performance",
            "Customers Assisted",
            "Items Moved",
        ]
        for key in required:
            if key not in block:
                errors.append(f"Staff block {idx}: missing {key}")

        staff_name = block.get("Staff Name", "").strip()
        section = block.get("Section", "").strip()
        arrangement = block.get("Arrangement", "").strip()
        display = block.get("Display", "").strip()
        performance = block.get("Performance", "").strip()
        customers_assisted = block.get("Customers Assisted", "").strip()
        items_moved = block.get("Items Moved", "").strip()

        if not staff_name:
            errors.append(f"Staff block {idx}: Staff Name cannot be empty")

        if section and not validate_section_name(section):
            errors.append(f"Staff block {idx}: invalid section {section}")

        score_fields = {
            "Arrangement": arrangement,
            "Display": display,
            "Performance": performance,
        }
        for label, value in score_fields.items():
            if not is_int(value):
                errors.append(f"Staff block {idx}: {label} must be integer 1-5")
            else:
                score = int(value)
                if score < 1 or score > 5:
                    errors.append(f"Staff block {idx}: {label} must be between 1 and 5")

        for label, value in {
            "Customers Assisted": customers_assisted,
            "Items Moved": items_moved,
        }.items():
            if not is_int(value):
                errors.append(f"Staff block {idx}: {label} must be integer")
            elif int(value) < 0:
                errors.append(f"Staff block {idx}: {label} cannot be negative")

        if all([
            staff_name,
            section,
            arrangement,
            display,
            performance,
            customers_assisted,
            items_moved,
        ]) and not errors:
            normalized_staff.append(
                {
                    "staff_name": staff_name,
                    "section": section,
                    "arrangement": int(arrangement),
                    "display": int(display),
                    "performance": int(performance),
                    "customers_assisted": int(customers_assisted),
                    "items_moved": int(items_moved),
                }
            )

    normalized = None
    if not errors:
        normalized = {
            "signal_type": "staff_performance_report",
            "branch": branch,
            "date": report_date,
            "report_type": "staff_report",
            "staff_records": normalized_staff,
            "notes": notes,
        }

    return ValidationResult(
        ok=not errors,
        classifier="STAFF PERFORMANCE REPORT",
        report_type="staff_report",
        errors=errors,
        normalized=normalized,
    )


def validate_supervisor_report(lines: list[str]) -> ValidationResult:
    errors: list[str] = []
    branch = ""
    report_date = ""
    supervisor_confirmed = ""
    current: dict[str, str] = {}
    blocks: list[dict[str, str]] = []

    def flush_current() -> None:
        nonlocal current
        if current:
            blocks.append(current)
            current = {}

    for line in lines:
        kv = parse_key_value_line(line)
        if not kv:
            errors.append(f"Invalid line format: {line}")
            continue

        key, value = kv

        if key == "Branch":
            branch = normalize_branch(value)
            continue
        if key == "Date":
            report_date = value
            continue
        if key == "Supervisor Confirmed":
            supervisor_confirmed = value.strip().upper()
            flush_current()
            continue

        if key == "Exception Type" and current:
            flush_current()

        current[key] = value

    flush_current()

    if not branch:
        errors.append("Missing required field: Branch")
    elif branch not in ALLOWED_BRANCHES:
        errors.append(f"Invalid branch: {branch}")

    if not report_date:
        errors.append("Missing required field: Date")
    elif not validate_date(report_date):
        errors.append("Invalid date format; expected DD/MM/YY")

    if supervisor_confirmed not in {"YES", "NO"}:
        errors.append("Supervisor Confirmed must be YES or NO")

    if not blocks:
        errors.append("At least one exception block is required")

    normalized_blocks: list[dict[str, Any]] = []

    for idx, block in enumerate(blocks, start=1):
        required = [
            "Exception Type",
            "Details",
            "Action Taken",
            "Escalated By",
            "Time",
        ]
        for key in required:
            if key not in block:
                errors.append(f"Exception block {idx}: missing {key}")

        exception_type = block.get("Exception Type", "").strip().upper()
        details = block.get("Details", "").strip()
        action_taken = block.get("Action Taken", "").strip()
        escalated_by = block.get("Escalated By", "").strip()
        time_value = block.get("Time", "").strip()

        if exception_type and exception_type not in ALLOWED_EXCEPTION_TYPES:
            errors.append(f"Exception block {idx}: invalid Exception Type {exception_type}")
        if details == "":
            errors.append(f"Exception block {idx}: Details cannot be empty")
        if action_taken == "":
            errors.append(f"Exception block {idx}: Action Taken cannot be empty")
        if escalated_by == "":
            errors.append(f"Exception block {idx}: Escalated By cannot be empty")
        if time_value and not validate_time(time_value):
            errors.append(f"Exception block {idx}: invalid time {time_value}; expected HH:MM")

        if all([exception_type, details, action_taken, escalated_by, time_value]) and not errors:
            normalized_blocks.append(
                {
                    "exception_type": exception_type,
                    "details": details,
                    "action_taken": action_taken,
                    "escalated_by": escalated_by,
                    "time": time_value,
                }
            )

    normalized = None
    if not errors:
        normalized = {
            "signal_type": "supervisor_control_report",
            "branch": branch,
            "date": report_date,
            "report_type": "supervisor_report",
            "exceptions": normalized_blocks,
            "supervisor_confirmed": supervisor_confirmed,
        }

    return ValidationResult(
        ok=not errors,
        classifier="SUPERVISOR CONTROL REPORT",
        report_type="supervisor_report",
        errors=errors,
        normalized=normalized,
    )


# ============================================================
# MAIN VALIDATION ROUTER
# ============================================================

def validate_message(raw_text: str) -> ValidationResult:
    lines = split_nonempty_lines(raw_text)
    classifier_line, report_type, remaining = extract_header(lines)

    if not classifier_line:
        return ValidationResult(
            ok=False,
            classifier=None,
            report_type=None,
            errors=["Empty message"],
            normalized=None,
        )

    if not report_type:
        return ValidationResult(
            ok=False,
            classifier=classifier_line,
            report_type=None,
            errors=[f"Unknown classifier line: {classifier_line}"],
            normalized=None,
        )

    if report_type == "sales_report":
        return validate_sales_report(remaining)
    if report_type == "inventory_report":
        return validate_inventory_report(remaining)
    if report_type == "bale_report":
        return validate_bale_report(remaining)
    if report_type == "staff_report":
        return validate_staff_report(remaining)
    if report_type == "supervisor_report":
        return validate_supervisor_report(remaining)

    return ValidationResult(
        ok=False,
        classifier=classifier_line,
        report_type=report_type,
        errors=[f"No validator registered for report type: {report_type}"],
        normalized=None,
    )


# ============================================================
# INGESTION
# ============================================================

def build_output_filename(report_type: str, branch: str, report_date: str) -> str:
    dt = datetime.strptime(report_date, "%d/%m/%y").strftime("%Y-%m-%d")
    return sanitize_filename(f"{branch}_{report_type}_{dt}.json")


def ingest_file(path: Path, strict: bool = True) -> dict[str, Any]:
    ensure_dirs()
    raw_text = path.read_text(encoding="utf-8")
    raw_hash = sha256_text(raw_text)

    state = load_state()
    seen_hashes: dict[str, Any] = state.setdefault("seen_hashes", {})

    if raw_hash in seen_hashes:
        return quarantine_duplicate(raw_text, raw_hash, seen_hashes[raw_hash])

    validation = validate_message(raw_text)

    if strict and not validation.ok:
        result = reject_payload(
            raw_text=raw_text,
            classifier_line=validation.classifier,
            inferred_type=validation.report_type,
            reasons=validation.errors,
            raw_hash=raw_hash,
        )
        seen_hashes[raw_hash] = {
            "status": "rejected",
            "classifier": validation.classifier,
            "report_type": validation.report_type,
            "ingested_at": utc_now_iso(),
        }
        save_state(state)
        return result

    if validation.normalized is None:
        result = reject_payload(
            raw_text=raw_text,
            classifier_line=validation.classifier,
            inferred_type=validation.report_type,
            reasons=validation.errors or ["Validation failed without normalized payload"],
            raw_hash=raw_hash,
        )
        seen_hashes[raw_hash] = {
            "status": "rejected",
            "classifier": validation.classifier,
            "report_type": validation.report_type,
            "ingested_at": utc_now_iso(),
        }
        save_state(state)
        return result

    normalized = validation.normalized.copy()
    normalized["meta"] = {
        "source_file": str(path),
        "sha256": raw_hash,
        "ingested_at": utc_now_iso(),
        "strict_mode": strict,
    }

    branch = normalized["branch"]
    report_type = normalized["report_type"]
    report_date = normalized["date"]

    out_dir = NORMALIZED_DIR / branch
    filename = build_output_filename(report_type, branch, report_date)
    out_path = out_dir / filename
    save_json(out_path, normalized)

    seen_hashes[raw_hash] = {
        "status": "accepted",
        "classifier": validation.classifier,
        "report_type": report_type,
        "branch": branch,
        "date": report_date,
        "output_file": str(out_path),
        "ingested_at": utc_now_iso(),
    }
    save_state(state)

    return {
        "status": "accepted",
        "output_file": str(out_path),
        "report_type": report_type,
        "branch": branch,
        "date": report_date,
        "sha256": raw_hash,
    }


# ============================================================
# CLI
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Strict WhatsApp gatekeeper for IOI Colony")
    parser.add_argument("input_file", help="Path to WhatsApp text file")
    parser.add_argument("--strict", action="store_true", help="Reject anything invalid")
    parser.add_argument("--print-json", action="store_true", help="Print result JSON")
    args = parser.parse_args()

    input_path = Path(args.input_file)
    if not input_path.exists():
        raise SystemExit(f"ERROR: input file not found: {input_path}")

    result = ingest_file(input_path, strict=args.strict)

    if args.print_json:
        print(json.dumps(result, indent=2))
    else:
        print(f"[whatsapp_gatekeeper] {result['status']}")
        if "output_file" in result:
            print(f"[whatsapp_gatekeeper] output: {result['output_file']}")
        if "reasons" in result:
            for reason in result["reasons"]:
                print(f"[whatsapp_gatekeeper] reason: {reason}")


if __name__ == "__main__":
    main()
