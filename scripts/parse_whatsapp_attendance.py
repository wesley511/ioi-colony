#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

try:
    from scripts.utils_normalization import normalize_branch as shared_normalize_branch
    from scripts.whatsapp_report_sections import extract_selected_report_text, iter_attendance_rows
except ModuleNotFoundError:
    from utils_normalization import normalize_branch as shared_normalize_branch
    from whatsapp_report_sections import extract_selected_report_text, iter_attendance_rows


STATUS_MAP = {
    "✔": "Present",
    "PRESENT": "Present",
    "ABSENT": "Absent",
    "OFF": "Off Duty",
    "OFF DUTY": "Off Duty",
    "LEAVE": "On Leave",
    "ANNUAL LEAVE": "On Leave",
    "SICK LEAVE": "On Leave",
}

NON_STAFF_ROW_LABELS = {
    "branch",
    "date",
    "present",
    "absent",
    "off",
    "off_duty",
    "leave",
    "annual_leave",
    "sick_leave",
    "staff_on_duty",
    "total_staff",
}


def utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def extract_line_value(text: str, *labels: str) -> str | None:
    for label in labels:
        match = re.search(rf"^\s*{re.escape(label)}\s*[:=]\s*(.+?)\s*$", text, flags=re.IGNORECASE | re.MULTILINE)
        if match:
            return normalize_spaces(match.group(1))
    return None


def normalize_branch(raw_branch: str | None) -> str:
    normalized = shared_normalize_branch(
        raw_branch,
        style="canonical_slug",
        fallback="slugify",
        match_substring=True,
    )
    return str(normalized or "unknown")


def extract_branch(text: str) -> str:
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
    return normalize_branch(extract_line_value(text, "Branch", "Shop", "Location") or first_line)


def extract_report_date(text: str) -> str:
    raw = extract_line_value(text, "Date")
    if not raw:
        match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", text)
        if not match:
            return utc_today_iso()
        raw = match.group(0)

    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass

    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", raw)
    if not match:
        return utc_today_iso()
    day, month, year = [int(part) for part in match.groups()]
    if year < 100:
        year += 2000
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return utc_today_iso()


def normalize_status(value: str) -> str:
    cleaned = normalize_spaces(value).upper()
    return STATUS_MAP.get(cleaned, "Unknown")


def normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")


def parse_attendance_report(text: str) -> dict[str, Any]:
    selected_text = extract_selected_report_text(text, expected_report_type="staff_attendance")
    branch = extract_branch(selected_text)
    signal_date = extract_report_date(selected_text)

    records: list[dict[str, Any]] = []
    totals = {
        "present": 0,
        "absent": 0,
        "off_duty": 0,
        "on_leave": 0,
    }

    for staff_name, raw_status in iter_attendance_rows(selected_text):
        if normalize_label(staff_name) in NON_STAFF_ROW_LABELS:
            continue
        status = normalize_status(raw_status)
        records.append(
            {
                "staff_name": staff_name,
                "attendance_status": status,
                "raw_attendance_value": raw_status,
            }
        )
        if status == "Present":
            totals["present"] += 1
        elif status == "Absent":
            totals["absent"] += 1
        elif status == "Off Duty":
            totals["off_duty"] += 1
        elif status == "On Leave":
            totals["on_leave"] += 1

    warnings: list[str] = []
    if not records:
        warnings.append("no_attendance_rows_parsed")
    if any(record["attendance_status"] == "Unknown" for record in records):
        warnings.append("unknown_attendance_status_present")

    return {
        "type": "staff_attendance",
        "schema_version": "1.0",
        "branch": branch,
        "branch_slug": branch,
        "date": signal_date,
        "attendance": {
            "records": records,
            "totals": {
                **totals,
                "staff_on_duty": totals["present"],
                "total_staff": sum(totals.values()),
            },
        },
        "warnings": warnings,
        "source_format": "whatsapp_staff_attendance",
        "raw_text_preview": normalize_spaces(selected_text)[:320],
    }


def save_yaml(parsed: dict[str, Any], output_dir: str = "SIGNALS/normalized") -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    branch = parsed.get("branch") or "unknown"
    signal_date = parsed.get("date") or utc_today_iso()
    out_path = out_dir / f"{branch}_staff_attendance_{signal_date}.yaml"

    with out_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(parsed, fh, sort_keys=False, allow_unicode=True)

    print(f"Saved: {out_path}")
    return out_path


def main() -> int:
    if len(sys.argv) > 1:
        text = Path(sys.argv[1]).read_text(encoding="utf-8")
    else:
        print("Paste WhatsApp attendance report below. CTRL+D when done:")
        text = sys.stdin.read()

    if not text.strip():
        print("No input received.", file=sys.stderr)
        return 1

    parsed = parse_attendance_report(text)
    save_yaml(parsed)
    print(f"- Branch: {parsed['branch']}")
    print(f"- Date: {parsed['date']}")
    print(f"- Staff on Duty: {parsed['attendance']['totals']['staff_on_duty']}")
    print(f"- Records: {len(parsed['attendance']['records'])}")
    if parsed["warnings"]:
        print(f"- Warnings: {parsed['warnings']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
