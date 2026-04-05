from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from scripts.utils_normalization import normalize_branch as shared_normalize_branch
except ModuleNotFoundError:
    from utils_normalization import normalize_branch as shared_normalize_branch

try:
    from scripts.section_normalizer import canonical_sections as canonical_section_names, normalize_section_name
except ModuleNotFoundError:
    from section_normalizer import canonical_sections as canonical_section_names, normalize_section_name
try:
    from scripts.whatsapp_report_sections import (
        extract_selected_report_text,
        iter_attendance_rows,
        select_report_block,
    )
except ModuleNotFoundError:
    from whatsapp_report_sections import extract_selected_report_text, iter_attendance_rows, select_report_block
try:
    from scripts.parse_whatsapp_sales import extract_till_totals as extract_sales_till_totals
except ModuleNotFoundError:
    from parse_whatsapp_sales import extract_till_totals as extract_sales_till_totals
try:
    from scripts.whatsapp_intelligence import build_confidence_metadata
except ModuleNotFoundError:
    from whatsapp_intelligence import build_confidence_metadata


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
    "DAILY BALE SUMMARY": "bale_report",
    "STAFF PERFORMANCE REPORT": "staff_report",
    "DAILY STAFF PERFORMANCE REPORT": "staff_report",
    "STAFF ATTENDANCE REPORT": "staff_attendance_report",
    "DAILY STAFF ATTENDANCE REPORT": "staff_attendance_report",
    "SUPERVISOR CONTROL REPORT": "staff_attendance_report",
    "SUPERVISOR CONTROL SUMMARY": "staff_attendance_report",
}

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{2}$")
TIME_RE = re.compile(r"^\d{2}:\d{2}$")
INT_RE = re.compile(r"^-?\d+$")
NUMBER_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
STAFF_NAME_RE = re.compile(r"^\s*\d+[\.\)]*\s*(.+?)\s*$")

STAFF_FIELD_ALIASES = {
    "Arrangements": "Arrangement",
    "Assisting Customers": "Customers Assisted",
}
NULL_NUMERIC_VALUES = {"", "-", "NA", "N/A"}
ATTENDANCE_STATUS_MAP = {
    "✔": "Present",
    "PRESENT": "Present",
    "ABSENT": "Absent",
    "OFF": "Off Duty",
    "OFF DUTY": "Off Duty",
    "LEAVE": "On Leave",
    "ANNUAL LEAVE": "On Leave",
    "SICK LEAVE": "On Leave",
}


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
    warnings: list[str] = field(default_factory=list)
    lane: str = "quarantine"


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
    number = float(value)
    if not number.is_integer():
        raise ValueError(f"Value is not an integer: {value}")
    return int(number)


def normalize_label(key: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "_", (key or "").strip().lower())
    return re.sub(r"_+", "_", value).strip("_")


def lane_for(errors: list[str], warnings: list[str]) -> str:
    if errors:
        return "quarantine"
    if warnings:
        return "accepted_with_warnings"
    return "accepted"


def coerce_numeric_value(
    fields: dict[str, str],
    key: str,
    *,
    integer: bool = False,
    required: bool = False,
    warnings: list[str],
    errors: list[str],
    flags: list[str],
) -> int | float | None:
    raw = str(fields.get(key, "")).strip()
    if raw.upper() in NULL_NUMERIC_VALUES:
        if required:
            errors.append(f"Missing required numeric field: {key}")
        else:
            warnings.append(f"{key} missing; stored as null")
            flags.append(f"{normalize_label(key)}_null")
        return None
    if not raw:
        if required:
            errors.append(f"Missing required numeric field: {key}")
        else:
            warnings.append(f"{key} missing; stored as null")
            flags.append(f"{normalize_label(key)}_null")
        return None
    if not is_number(raw):
        if required:
            errors.append(f"Field must be numeric: {key}")
        else:
            warnings.append(f"{key} not numeric; stored as null")
            flags.append(f"{normalize_label(key)}_invalid")
        return None
    try:
        return to_int(raw) if integer else to_number(raw)
    except ValueError:
        if required:
            errors.append(f"Field must be integer: {key}")
        else:
            warnings.append(f"{key} not integer; stored as null")
            flags.append(f"{normalize_label(key)}_invalid")
        return None


def parse_delimited_names(value: str) -> list[str]:
    names: list[str] = []
    for raw in re.split(r"[;,/]|(?:\band\b)", value, flags=re.IGNORECASE):
        cleaned = re.sub(r"\s+", " ", raw).strip(" .")
        if cleaned:
            names.append(cleaned)
    return names


def unique_name_count(values: list[str]) -> int:
    seen: set[str] = set()
    count = 0
    for value in values:
        token = normalize_label(value)
        if not token or token in seen:
            continue
        seen.add(token)
        count += 1
    return count


def normalize_branch(value: str) -> str:
    normalized = shared_normalize_branch(
        value,
        style="canonical_slug",
        fallback="lower_token",
        match_substring=False,
        profile="literal",
    )
    return str(normalized or "")


def validate_date(value: str) -> bool:
    return bool(DATE_RE.fullmatch(value))


def normalize_report_date_value(value: str) -> str:
    raw = str(value or "").strip()
    if validate_date(raw):
        return raw
    match = re.search(r"(\d{2}/\d{2}/\d{2})", raw)
    if match:
        return match.group(1)
    return raw


def validate_time(value: str) -> bool:
    return bool(TIME_RE.fullmatch(value))


def sanitize_filename(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", value)


def split_nonempty_lines(text: str) -> list[str]:
    return [line.rstrip() for line in text.splitlines() if line.strip()]


def canonicalize_staff_field_name(key: str) -> str:
    return STAFF_FIELD_ALIASES.get(key.strip(), key.strip())


def parse_staff_name_line(line: str) -> str | None:
    match = STAFF_NAME_RE.fullmatch(line.strip())
    if not match:
        return None
    name = match.group(1).strip().strip(".")
    return name or None


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
CANONICAL_SECTIONS.update(canonical_section_names())


def validate_section_name(value: str) -> bool:
    normalized = normalize_section_name(value)
    if not normalized:
        return False
    if not CANONICAL_SECTIONS:
        return bool(re.fullmatch(r"[A-Za-z0-9_]+", normalized))
    return normalized in CANONICAL_SECTIONS or value in CANONICAL_SECTIONS


def classifier_lookup_candidates(value: str) -> list[str]:
    normalized = value.strip().upper()
    normalized = normalized.replace("—", "-").replace("–", "-")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    compact = re.sub(r"\s*-\s*", "-", normalized)
    spaced = re.sub(r"\s*-\s*", " - ", normalized)
    candidates = [normalized]
    for candidate in (compact, spaced):
        candidate = re.sub(r"\s+", " ", candidate).strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates


def extract_header(lines: list[str]) -> tuple[str | None, str | None, list[str]]:
    if not lines:
        return None, None, []
    first = lines[0].strip()
    classifier = None
    for candidate in classifier_lookup_candidates(first):
        classifier = CLASSIFIERS.get(candidate)
        if classifier is not None:
            break
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
    warnings: list[str] = []
    flags: list[str] = []
    fields: dict[str, str] = {}
    joined_text = "\n".join(lines)

    for line in lines:
        kv = parse_key_value_line(line)
        if kv:
            fields[kv[0]] = kv[1]

    till_totals = extract_sales_till_totals(joined_text)

    if "Customers Served" not in fields:
        for alias in ("Guest Served", "Guests Served", "Guest/ Customer served"):
            if alias in fields:
                fields["Customers Served"] = fields[alias]
                break

    if "Z Reading" not in fields and till_totals.get("z_reading", 0) > 0:
        fields["Z Reading"] = str(till_totals["z_reading"])
    if "Cash Sales" not in fields and till_totals.get("cash", 0) > 0:
        fields["Cash Sales"] = str(till_totals["cash"])
    if "EFTPOS Sales" not in fields and till_totals.get("card", 0) > 0:
        fields["EFTPOS Sales"] = str(till_totals["card"])

    if "Date" not in fields:
        errors.append("Missing required field: Date")
    elif fields.get("Date"):
        fields["Date"] = normalize_report_date_value(fields["Date"])
    for key in ("Z Reading", "Cash Sales", "EFTPOS Sales"):
        if key not in fields:
            errors.append(f"Missing required field: {key}")

    branch = normalize_branch(fields.get("Branch", ""))
    if not branch:
        inferred_branch = shared_normalize_branch(
            joined_text,
            style="canonical_slug",
            fallback="none",
            match_substring=True,
        )
        branch = str(inferred_branch or "")
        if branch:
            warnings.append("Branch missing; inferred from report text")
            flags.append("branch_inferred")
    if branch and branch not in ALLOWED_BRANCHES:
        errors.append(f"Invalid branch: {branch}")
    if not branch:
        errors.append("Missing required field: Branch")

    report_date = fields.get("Date", "")
    if report_date and not validate_date(report_date):
        errors.append("Invalid date format; expected DD/MM/YY")

    traffic_value = None
    traffic_source = None
    for label in ("Main Door", "Traffic", "Total Customers (Traffic)", "Door Count"):
        if label in fields:
            traffic_value = fields[label]
            traffic_source = label
            break
    if traffic_value is None:
        warnings.append("Traffic missing; stored as null")
        flags.append("traffic_null")
    elif str(traffic_value).strip().upper() in NULL_NUMERIC_VALUES or not str(traffic_value).strip():
        warnings.append("Traffic missing; stored as null")
        flags.append("traffic_null")
    elif not is_number(str(traffic_value).strip()):
        warnings.append("Traffic not numeric; stored as null")
        flags.append("traffic_invalid")

    customers_served = coerce_numeric_value(
        fields,
        "Customers Served",
        integer=True,
        required=False,
        warnings=warnings,
        errors=errors,
        flags=flags,
    )
    staff_on_duty = coerce_numeric_value(
        fields,
        "Staff on Duty",
        integer=True,
        required=False,
        warnings=warnings,
        errors=errors,
        flags=flags,
    )
    cash_variance = coerce_numeric_value(
        fields,
        "Cash Variance",
        integer=False,
        required=False,
        warnings=warnings,
        errors=errors,
        flags=flags,
    )
    z_reading = coerce_numeric_value(
        fields,
        "Z Reading",
        integer=False,
        required=True,
        warnings=warnings,
        errors=errors,
        flags=flags,
    )
    cash_sales = coerce_numeric_value(
        fields,
        "Cash Sales",
        integer=False,
        required=True,
        warnings=warnings,
        errors=errors,
        flags=flags,
    )
    eftpos_sales = coerce_numeric_value(
        fields,
        "EFTPOS Sales",
        integer=False,
        required=True,
        warnings=warnings,
        errors=errors,
        flags=flags,
    )
    traffic = None
    if traffic_value is not None and str(traffic_value).strip().upper() not in NULL_NUMERIC_VALUES and is_number(str(traffic_value).strip()):
        try:
            traffic = to_int(str(traffic_value).strip())
        except ValueError:
            warnings.append("Traffic not integer; stored as null")
            flags.append("traffic_invalid")

    if not errors:
        if customers_served is not None and traffic is not None and customers_served > traffic:
            errors.append("Customers Served cannot exceed Total Customers (Traffic)")
        if staff_on_duty is not None and staff_on_duty < 0:
            errors.append("Staff on Duty cannot be negative")

        if None not in {cash_sales, eftpos_sales, z_reading}:
            difference = abs((cash_sales + eftpos_sales) - z_reading)
        else:
            difference = 0.0
        if None not in {cash_sales, eftpos_sales, z_reading} and difference > 1.0:
            errors.append(
                f"Z Reading mismatch: Cash Sales + EFTPOS Sales differs from Z Reading by {difference:.2f}"
            )

        supervisor_confirmed = fields.get("Supervisor Confirmed", "").upper()
        if not supervisor_confirmed:
            warnings.append("Supervisor Confirmed missing; stored as empty")
            flags.append("supervisor_confirmed_missing")
        elif supervisor_confirmed not in {"YES", "NO"}:
            errors.append("Supervisor Confirmed must be YES or NO")

        if "Over/Short Reason" not in fields:
            warnings.append("Over/Short Reason missing; stored as empty")
            flags.append("over_short_reason_missing")

    normalized = None
    if not errors:
        notes = fields.get("Notes", "")
        intelligence = build_confidence_metadata(
            validation_lane=lane_for(errors, warnings),
            warnings=warnings,
            flags=flags,
            confidence_score=None,
        )
        normalized = {
            "signal_type": "daily_sales_report",
            "branch": branch,
            "date": report_date,
            "report_type": "sales_report",
            "validation_lane": lane_for(errors, warnings),
            "totals": {
                "z_reading": z_reading,
                "cash_sales": cash_sales,
                "eftpos_sales": eftpos_sales,
                "cash_variance": cash_variance,
            },
            "traffic": {
                "traffic_source": traffic_source,
                "total_customers": traffic,
                "customers_served": customers_served,
            },
            "staffing": {
                "staff_on_duty": staff_on_duty,
            },
            "control": {
                "over_short_reason": fields.get("Over/Short Reason", ""),
                "supervisor_confirmed": fields.get("Supervisor Confirmed", "").upper(),
            },
            "notes": notes,
            "flags": list(flags),
            "warnings": list(warnings),
            "intelligence": intelligence,
        }

    return ValidationResult(
        ok=not errors,
        classifier="DAY-END SALES REPORT",
        report_type="sales_report",
        errors=errors,
        normalized=normalized,
        warnings=warnings,
        lane=lane_for(errors, warnings),
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
            if key == "Date":
                value = normalize_report_date_value(value)
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
            "intelligence": build_confidence_metadata(
                validation_lane="accepted",
                warnings=[],
                inferred_field_count=0,
                confidence_score=None,
            ),
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
    warnings: list[str] = []
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
            report_date = normalize_report_date_value(value)
            continue
        if key == "Released By":
            released_by = value
            continue
        if key in {"Total Qty", "Total Quantity"}:
            total_qty = value
            continue
        if key in {"Total Amount", "Amount Total"}:
            total_amount = value
            continue

        if key in {"Bale ID", "Item", "Item Name", "Item_Name"} and current:
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
        warnings.append("Released By missing; stored as empty")

    if not blocks:
        errors.append("At least one bale block is required")

    normalized_blocks: list[dict[str, Any]] = []
    sum_qty = 0
    sum_amount = 0.0

    for idx, block in enumerate(blocks, start=1):
        bale_id = block.get("Bale ID", "").strip()
        item_name = block.get("Item", "").strip() or block.get("Item Name", "").strip() or block.get("Item_Name", "").strip()
        section = block.get("Section", "").strip()
        qty = block.get("Qty", "").strip()
        amount = block.get("Amount", "").strip()
        status = block.get("Status", "").strip().upper()

        if not item_name and not bale_id:
            errors.append(f"Bale block {idx}: missing Item or Bale ID")
        if section and not validate_section_name(section):
            errors.append(f"Bale block {idx}: invalid section {section}")
        if not qty:
            errors.append(f"Bale block {idx}: missing Qty")
        elif not is_number(qty):
            errors.append(f"Bale block {idx}: Qty must be numeric")
        if not amount:
            errors.append(f"Bale block {idx}: missing Amount")
        elif not is_number(amount):
            errors.append(f"Bale block {idx}: Amount must be numeric")
        if status and status not in ALLOWED_BALE_STATUS:
            errors.append(f"Bale block {idx}: invalid status {status}")
        if not bale_id:
            warnings.append(f"Bale block {idx}: Bale ID missing; stored as empty")
        if not section:
            warnings.append(f"Bale block {idx}: Section missing; stored as empty")
        if not status:
            warnings.append(f"Bale block {idx}: Status missing; stored as empty")

        if qty and amount and is_number(qty) and is_number(amount):
            q = to_int(qty) if is_int(qty) else int(float(qty))
            a = to_number(amount)
            sum_qty += q
            sum_amount += a
            normalized_blocks.append(
                {
                    "bale_id": bale_id,
                    "item_name": item_name,
                    "section": section,
                    "qty": q,
                    "amount": a,
                    "status": status,
                }
            )

    if total_qty is None:
        warnings.append("Total Qty missing; computed from bale rows")
    elif not is_number(total_qty):
        errors.append("Missing or invalid Total Qty")
    elif int(float(total_qty)) != sum_qty:
        errors.append(f"Total Qty mismatch: declared {total_qty}, computed {sum_qty}")

    if total_amount is None:
        warnings.append("Total Amount missing; computed from bale rows")
    elif not is_number(total_amount):
        errors.append("Missing or invalid Total Amount")
    elif abs(float(total_amount) - sum_amount) > 0.01:
        errors.append(f"Total Amount mismatch: declared {total_amount}, computed {sum_amount:.2f}")

    computed_total_qty = sum_qty
    computed_total_amount = round(sum_amount, 2)

    normalized = None
    if not errors:
        intelligence = build_confidence_metadata(
            validation_lane=lane_for(errors, warnings),
            warnings=warnings,
            confidence_score=None,
        )
        normalized = {
            "signal_type": "daily_bale_summary_report",
            "branch": branch,
            "date": report_date,
            "report_type": "bale_report",
            "released_by": released_by,
            "bales": normalized_blocks,
            "totals": {
                "total_qty": int(float(total_qty)) if total_qty is not None and is_number(total_qty) else computed_total_qty,
                "total_amount": round(float(total_amount), 2) if total_amount is not None and is_number(total_amount) else computed_total_amount,
            },
            "warnings": list(warnings),
            "intelligence": intelligence,
        }

    return ValidationResult(
        ok=not errors,
        classifier="DAILY BALE SUMMARY – RELEASED TO RAIL",
        report_type="bale_report",
        errors=errors,
        normalized=normalized,
        warnings=warnings,
        lane=lane_for(errors, warnings),
    )


def validate_staff_report(lines: list[str]) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
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
        staff_name_line = parse_staff_name_line(line)
        if staff_name_line:
            if current:
                flush_current()
            current["Staff Name"] = staff_name_line
            continue

        kv = parse_key_value_line(line)
        if not kv:
            errors.append(f"Invalid line format: {line}")
            continue

        key, value = kv
        key = canonicalize_staff_field_name(key)

        if key == "Branch":
            branch = normalize_branch(value)
            continue
        if key == "Date":
            report_date = normalize_report_date_value(value)
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

        normalized_section = normalize_section_name(section) if section else ""
        if not normalized_section:
            errors.append(f"Staff block {idx}: invalid section raw={section!r} reason=not_in_dictionary")

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
            if not value:
                warnings.append(f"Staff block {idx}: {label} missing; stored as null")
            elif not is_int(value):
                warnings.append(f"Staff block {idx}: {label} not numeric; stored as null")
            elif int(value) < 0:
                errors.append(f"Staff block {idx}: {label} cannot be negative")

        if all([
            staff_name,
            section,
            arrangement,
            display,
            performance,
        ]) and not errors:
            normalized_staff.append(
                {
                    "staff_name": staff_name,
                    "section": normalized_section,
                    "raw_section": section,
                    "arrangement": int(arrangement),
                    "display": int(display),
                    "performance": int(performance),
                    "customers_assisted": int(customers_assisted) if customers_assisted and is_int(customers_assisted) else None,
                    "items_moved": int(items_moved) if items_moved and is_int(items_moved) else None,
                }
            )

    normalized = None
    if not errors:
        intelligence = build_confidence_metadata(
            validation_lane=lane_for(errors, warnings),
            warnings=warnings,
            confidence_score=None,
        )
        normalized = {
            "signal_type": "staff_performance_report",
            "branch": branch,
            "date": report_date,
            "report_type": "staff_report",
            "staff_records": normalized_staff,
            "notes": notes,
            "intelligence": intelligence,
        }

    return ValidationResult(
        ok=not errors,
        classifier="STAFF PERFORMANCE REPORT",
        report_type="staff_report",
        errors=errors,
        normalized=normalized,
        warnings=warnings,
        lane=lane_for(errors, warnings),
    )


def normalize_attendance_status(raw_value: str) -> str | None:
    cleaned = re.sub(r"\s+", " ", (raw_value or "").strip()).upper()
    return ATTENDANCE_STATUS_MAP.get(cleaned)


def validate_staff_attendance_report(lines: list[str], classifier_line: str = "STAFF ATTENDANCE REPORT") -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    branch = ""
    report_date = ""
    summary_fields: dict[str, int] = {}
    attendance_rows: list[dict[str, str]] = []

    for line in lines:
        kv = parse_key_value_line(line)
        if kv:
            key, value = kv
            normalized_key = normalize_label(key)
            if key == "Branch":
                branch = normalize_branch(value)
                continue
            if key == "Date":
                report_date = normalize_report_date_value(value)
                continue
            if normalized_key in {
                "present",
                "absent",
                "off",
                "off_duty",
                "leave",
                "annual_leave",
                "sick_leave",
                "staff_on_duty",
                "total_staff",
            }:
                if is_int(value):
                    summary_fields[normalized_key] = int(value)
                elif value.strip().upper() not in NULL_NUMERIC_VALUES:
                    warnings.append(f"Attendance total not numeric: {key}")
                continue
            canonical_status = normalize_attendance_status(value)
            parsed_name = parse_staff_name_line(key) or key.strip()
            if canonical_status is not None and re.fullmatch(r"[A-Za-z .'-]+", parsed_name):
                attendance_rows.append({"staff_name": parsed_name, "raw_status": value.strip()})
            continue

        for staff_name, raw_status in iter_attendance_rows(line):
            attendance_rows.append({"staff_name": staff_name, "raw_status": raw_status})

    if not branch:
        errors.append("Missing required field: Branch")
    elif branch not in ALLOWED_BRANCHES:
        errors.append(f"Invalid branch: {branch}")

    if not report_date:
        errors.append("Missing required field: Date")
    elif not validate_date(report_date):
        errors.append("Invalid date format; expected DD/MM/YY")

    if not attendance_rows:
        errors.append("At least one attendance row is required")

    normalized_rows: list[dict[str, Any]] = []
    computed_totals = {
        "present": 0,
        "absent": 0,
        "off_duty": 0,
        "on_leave": 0,
    }
    seen_staff: set[str] = set()

    for idx, row in enumerate(attendance_rows, start=1):
        staff_name = row["staff_name"].strip()
        raw_status = row["raw_status"].strip()
        canonical_status = normalize_attendance_status(raw_status)
        if not staff_name:
            errors.append(f"Attendance row {idx}: missing staff name")
            continue
        if canonical_status is None:
            warnings.append(f"Attendance row {idx}: unsupported status {raw_status!r}")
            canonical_status = "Unknown"

        staff_key = normalize_label(staff_name)
        if staff_key in seen_staff:
            warnings.append(f"Duplicate attendance row for {staff_name}")
        seen_staff.add(staff_key)

        if canonical_status == "Present":
            computed_totals["present"] += 1
        elif canonical_status == "Absent":
            computed_totals["absent"] += 1
        elif canonical_status == "Off Duty":
            computed_totals["off_duty"] += 1
        elif canonical_status == "On Leave":
            computed_totals["on_leave"] += 1

        normalized_rows.append(
            {
                "staff_name": staff_name,
                "attendance_status": canonical_status,
                "raw_attendance_value": raw_status,
            }
        )

    declared_map = {
        "present": summary_fields.get("present"),
        "absent": summary_fields.get("absent"),
        "off_duty": summary_fields.get("off") if "off" in summary_fields else summary_fields.get("off_duty"),
        "on_leave": next((summary_fields.get(key) for key in ("leave", "annual_leave", "sick_leave") if key in summary_fields), None),
        "staff_on_duty": summary_fields.get("staff_on_duty"),
        "total_staff": summary_fields.get("total_staff"),
    }

    for key, declared in declared_map.items():
        if declared is None:
            continue
        if key == "staff_on_duty" and declared != computed_totals["present"]:
            warnings.append(f"Declared staff on duty {declared} does not match computed present count {computed_totals['present']}")
        elif key == "total_staff":
            computed_total = sum(computed_totals.values())
            if declared != computed_total:
                warnings.append(f"Declared total staff {declared} does not match computed total {computed_total}")
        elif key in computed_totals and declared != computed_totals[key]:
            warnings.append(f"Declared {key.replace('_', ' ')} {declared} does not match computed total {computed_totals[key]}")

    normalized = None
    if not errors:
        intelligence = build_confidence_metadata(
            validation_lane=lane_for(errors, warnings),
            warnings=warnings,
            confidence_score=None,
        )
        normalized = {
            "signal_type": "staff_attendance_report",
            "branch": branch,
            "date": report_date,
            "report_type": "staff_attendance_report",
            "validation_lane": lane_for(errors, warnings),
            "attendance_records": normalized_rows,
            "attendance_totals": {
                "present": computed_totals["present"],
                "absent": computed_totals["absent"],
                "off_duty": computed_totals["off_duty"],
                "on_leave": computed_totals["on_leave"],
                "staff_on_duty": computed_totals["present"],
                "total_staff": sum(computed_totals.values()),
            },
            "declared_totals": declared_map,
            "warnings": list(warnings),
            "legacy_classifier": classifier_line,
            "intelligence": intelligence,
        }

    return ValidationResult(
        ok=not errors,
        classifier=classifier_line,
        report_type="staff_attendance_report",
        errors=errors,
        normalized=normalized,
        warnings=warnings,
        lane=lane_for(errors, warnings),
    )


# ============================================================
# MAIN VALIDATION ROUTER
# ============================================================

def validate_message(raw_text: str) -> ValidationResult:
    selected, _, ambiguous = select_report_block(raw_text)
    if ambiguous:
        return ValidationResult(
            ok=False,
            classifier=None,
            report_type=None,
            errors=["Ambiguous report selection across mixed WhatsApp sections"],
            normalized=None,
            lane="quarantine",
        )

    selected_text = extract_selected_report_text(raw_text)
    if selected is not None:
        contextual_lines = split_nonempty_lines(selected.contextual_text)
        section_lines = split_nonempty_lines(selected.section_text)
        if section_lines:
            try:
                header_index = contextual_lines.index(section_lines[0])
            except ValueError:
                header_index = 0
            preface_lines = contextual_lines[:header_index]
            selected_text = "\n".join([section_lines[0], *preface_lines, *section_lines[1:]])
    lines = split_nonempty_lines(selected_text)
    classifier_line, report_type, remaining = extract_header(lines)

    if not classifier_line:
        return ValidationResult(
            ok=False,
            classifier=None,
            report_type=None,
            errors=["Empty message"],
            normalized=None,
            lane="quarantine",
        )

    if not report_type:
        if selected and selected.report_type == "staff_attendance":
            return validate_staff_attendance_report(lines, classifier_line=selected.header)
        return ValidationResult(
            ok=False,
            classifier=classifier_line,
            report_type=None,
            errors=[f"Unknown classifier line: {classifier_line}"],
            normalized=None,
            lane="quarantine",
        )

    if report_type == "sales_report":
        return validate_sales_report(remaining)
    if report_type == "inventory_report":
        return validate_inventory_report(remaining)
    if report_type == "bale_report":
        return validate_bale_report(remaining)
    if report_type == "staff_report":
        return validate_staff_report(remaining)
    if report_type == "staff_attendance_report":
        return validate_staff_attendance_report(remaining, classifier_line=classifier_line)

    return ValidationResult(
        ok=False,
        classifier=classifier_line,
        report_type=report_type,
        errors=[f"No validator registered for report type: {report_type}"],
        normalized=None,
        lane="quarantine",
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

    existing_meta = seen_hashes.get(raw_hash)
    if existing_meta and existing_meta.get("status") != "rejected":
        return quarantine_duplicate(raw_text, raw_hash, existing_meta)

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
    intelligence = build_confidence_metadata(
        validation_lane=validation.lane,
        warnings=validation.warnings,
        flags=(normalized.get("flags") if isinstance(normalized, dict) else None),
        confidence_score=None,
    )
    normalized["intelligence"] = intelligence
    normalized["meta"] = {
        "source_file": str(path),
        "sha256": raw_hash,
        "ingested_at": utc_now_iso(),
        "strict_mode": strict,
        "validation_lane": validation.lane,
        "validation_warnings": list(validation.warnings),
        **intelligence,
    }

    branch = normalized["branch"]
    report_type = normalized["report_type"]
    report_date = normalized["date"]

    out_dir = NORMALIZED_DIR / branch
    filename = build_output_filename(report_type, branch, report_date)
    out_path = out_dir / filename
    save_json(out_path, normalized)

    seen_hashes[raw_hash] = {
        "status": validation.lane,
        "classifier": validation.classifier,
        "report_type": report_type,
        "branch": branch,
        "date": report_date,
        "output_file": str(out_path),
        "ingested_at": utc_now_iso(),
    }
    save_state(state)

    return {
        "status": validation.lane,
        "output_file": str(out_path),
        "report_type": report_type,
        "branch": branch,
        "date": report_date,
        "sha256": raw_hash,
        "warnings": list(validation.warnings),
        **intelligence,
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
