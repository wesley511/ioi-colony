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
except ModuleNotFoundError:
    from utils_normalization import normalize_branch as shared_normalize_branch

def utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (text or "").strip().lower()).strip("_")


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    cleaned = cleaned.replace("K", "")
    cleaned = cleaned.replace(",", "")
    cleaned = cleaned.replace("%", "")
    cleaned = cleaned.strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    num = parse_float(value)
    if num is None:
        return None
    return int(round(num))


def extract_line_value(text: str, *labels: str) -> str | None:
    for label in labels:
        pattern = rf"^\s*{re.escape(label)}\s*[:=]\s*(.+?)\s*$"
        m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            return normalize_spaces(m.group(1))
    return None


def extract_first_number(text: str, patterns: list[str]) -> float | None:
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            return parse_float(m.group(1))
    return None


def normalize_branch(raw_branch: str | None) -> str:
    if not raw_branch:
        return "unknown"
    normalized = shared_normalize_branch(
        raw_branch,
        style="canonical_slug",
        fallback="slugify",
        match_substring=True,
    )
    return str(normalized or "unknown")


def extract_report_date(text: str) -> str:
    raw = extract_line_value(text, "Date")
    if not raw:
        return utc_today_iso()

    raw = raw.strip()
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass

    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", raw)
    if m:
        day = int(m.group(1))
        month = int(m.group(2))
        year = int(m.group(3))
        if year < 100:
            year += 2000
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            pass

    return utc_today_iso()


def split_sales_and_supervisor(text: str) -> tuple[str, str]:
    parts = re.split(r"^\s*SUPERVISOR CONTROL REPORT\s*$", text, flags=re.IGNORECASE | re.MULTILINE, maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return text.strip(), ""


def detect_sales_format(text: str) -> str:
    upper = text.upper()
    if "TOTALS" in upper or "T_CASH" in upper or "MAIN_DOOR" in upper or "GUEST_CUSTOMER_SERVE" in upper:
        return "structured_v2"
    return "classic_v1"


def extract_branch(text: str) -> str:
    return normalize_branch(extract_line_value(text, "Branch", "Shop", "Location"))


def extract_explicit_conversion_rate(text: str) -> float | None:
    m = re.search(
        r"\bconver(?:sion|sation)[ _]?rate\b\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?)\s*%?",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    rate = float(m.group(1))
    if 0 < rate <= 1:
        rate *= 100
    return round(rate, 2)


def compute_total_sales(z_reading: float | None, cash_sales: float | None, card_sales: float | None, explicit_total_sales: float | None) -> float:
    if explicit_total_sales is not None and explicit_total_sales > 0:
        return round(explicit_total_sales, 2)
    if z_reading is not None and z_reading > 0:
        return round(z_reading, 2)
    return round((cash_sales or 0.0) + (card_sales or 0.0), 2)


def compute_conversion_rate(traffic: int | None, served: int | None, explicit_rate: float | None) -> float:
    if traffic and traffic > 0 and served is not None:
        return round((served / traffic) * 100.0, 2)
    if explicit_rate is not None:
        return round(explicit_rate, 2)
    return 0.0


def compute_sales_per_customer(total_sales: float, served: int | None, reported: float | None) -> float:
    if served and served > 0:
        return round(total_sales / served, 2)
    if reported is not None:
        return round(reported, 2)
    return 0.0


def compute_confidence(
    branch: str,
    total_sales: float,
    cash: float | None,
    card: float | None,
    z_reading: float | None,
    traffic: int | None,
    served: int | None,
) -> float:
    score = 0.0
    if branch != "unknown":
        score += 0.20
    if total_sales > 0:
        score += 0.20
    if cash is not None:
        score += 0.10
    if card is not None:
        score += 0.10
    if z_reading is not None:
        score += 0.10
    if traffic is not None:
        score += 0.10
    if served is not None:
        score += 0.10
    if traffic and served is not None and traffic > 0:
        score += 0.10
    return round(min(score, 1.0), 2)


def derive_flags(
    branch: str,
    total_sales: float,
    cash: float | None,
    card: float | None,
    z_reading: float | None,
    traffic: int | None,
    served: int | None,
    explicit_total_sales: float | None,
    explicit_conversion_rate: float | None,
    computed_conversion_rate: float,
    cash_variance: float | None,
) -> list[str]:
    flags: list[str] = []

    if branch == "unknown":
        flags.append("missing_branch")
    if total_sales <= 0:
        flags.append("zero_sales")
    if traffic is None:
        flags.append("missing_traffic")
    if served is None:
        flags.append("missing_customers_served")
    if cash_variance is not None and abs(cash_variance) > 0:
        flags.append("cash_variance_present")
    if traffic and served is not None and served > traffic:
        flags.append("served_gt_traffic")

    if cash is not None and card is not None and explicit_total_sales is not None:
        if abs((cash + card) - explicit_total_sales) > 0.01:
            flags.append("cash_card_total_mismatch")

    if z_reading is not None and explicit_total_sales is not None:
        if abs(z_reading - explicit_total_sales) > 0.01:
            flags.append("zreading_total_mismatch")

    if explicit_conversion_rate is not None:
        if abs(explicit_conversion_rate - computed_conversion_rate) > 0.5:
            flags.append("reported_conversion_mismatch")

    return flags


def parse_sales_report(text: str) -> dict[str, Any]:
    sales_text, supervisor_text = split_sales_and_supervisor(text)
    sales_format = detect_sales_format(sales_text)

    branch = extract_branch(sales_text)
    signal_date = extract_report_date(sales_text)

    explicit_total_cash = extract_first_number(
        sales_text,
        [r"^\s*Total_Cash\s*[:=]\s*([0-9,]+(?:\.[0-9]+)?)\s*$"],
    )
    explicit_total_card = extract_first_number(
        sales_text,
        [r"^\s*Total_Card\s*[:=]\s*([0-9,]+(?:\.[0-9]+)?)\s*$"],
    )
    explicit_total_sales = extract_first_number(
        sales_text,
        [r"^\s*Total_Sales\s*[:=]\s*([0-9,]+(?:\.[0-9]+)?)\s*$"],
    )

    z_reading = extract_first_number(
        sales_text,
        [r"^\s*Z[ _]?Reading\s*[:=]\s*([0-9,]+(?:\.[0-9]+)?)\s*$"],
    )

    cash_sales = explicit_total_cash
    if cash_sales is None:
        cash_sales = extract_first_number(
            sales_text,
            [
                r"^\s*T_Cash\s*[:=]\s*([0-9,]+(?:\.[0-9]+)?)\s*$",
                r"^\s*Cash Sales\s*[:=]\s*([0-9,]+(?:\.[0-9]+)?)\s*$",
            ],
        )

    card_sales = explicit_total_card
    if card_sales is None:
        card_sales = extract_first_number(
            sales_text,
            [
                r"^\s*T_Card\s*[:=]\s*([0-9,]+(?:\.[0-9]+)?)\s*$",
                r"^\s*EFTPOS Sales\s*[:=]\s*([0-9,]+(?:\.[0-9]+)?)\s*$",
                r"^\s*Card Sales\s*[:=]\s*([0-9,]+(?:\.[0-9]+)?)\s*$",
                r"^\s*POS Sales\s*[:=]\s*([0-9,]+(?:\.[0-9]+)?)\s*$",
            ],
        )

    traffic = parse_int(
        extract_line_value(
            sales_text,
            "Total Customers (Traffic)",
            "Traffic",
            "Total Customers",
            "Main_Door",
        )
    )

    served = parse_int(
        extract_line_value(
            sales_text,
            "Customers Served",
            "Guest Served",
            "Guests Served",
            "Guest_Customer_Serve",
        )
    )

    staff_on_duty = parse_int(extract_line_value(sales_text, "Staff on Duty"))
    notes = extract_line_value(sales_text, "Notes")
    over_short_reason = extract_line_value(sales_text, "Over/Short Reason")
    supervisor_confirmed = extract_line_value(sales_text, "Supervisor Confirmed")

    cashier = extract_line_value(sales_text, "Cashier")
    assistant = extract_line_value(sales_text, "Assistant")
    till_1 = extract_line_value(sales_text, "Till_1", "Till 1", "Till")
    balanced = extract_line_value(sales_text, "Balance", "Balanced")
    balanced_by = extract_line_value(sales_text, "Balanced_By", "Balanced By")

    sales_per_labor_hours = parse_float(
        extract_line_value(sales_text, "Sales_Per_Labor_Hours", "Sales Per Labor Hours")
    )
    sales_per_customer_reported = parse_float(
        extract_line_value(sales_text, "Sale_Per_Customer", "Sales_Per_Customer", "Sale Per Customer")
    )
    explicit_conversion_rate = extract_explicit_conversion_rate(sales_text)

    supervisor_branch = extract_branch(supervisor_text) if supervisor_text else ""

    supervisor = {
        "branch": supervisor_branch,
        "branch_slug": supervisor_branch or branch,
        "date": extract_report_date(supervisor_text) if supervisor_text else "",
        "supervisor": extract_line_value(supervisor_text, "Supervisor") or "",
        "cash_variance_reported": extract_line_value(supervisor_text, "Cash_Variance", "Cash Variance") or "",
        "staffing_issues": extract_line_value(supervisor_text, "Staffing_Issues", "Staffing Issues") or "",
        "stock_issues_affecting_sales": extract_line_value(
            supervisor_text,
            "Stock_Issues_Affecting_Sales",
            "Stock Issues Affecting Sales",
        ) or "",
        "pricing_or_system_issues": extract_line_value(
            supervisor_text,
            "Pricing_Or_System_Issues",
            "Pricing Or System Issues",
        ) or "",
        "exceptions_escalated_to_ops_manager": extract_line_value(
            supervisor_text,
            "Exceptions_Escalated_To_Ops_Manager",
            "Exceptions Escalated To Ops Manager",
        ) or "",
        "supervisor_confirmation": extract_line_value(
            supervisor_text,
            "Supervisor_Confirmation",
            "Supervisor Confirmation",
        ) or "",
    }

    cash_variance_numeric = parse_float(extract_line_value(sales_text, "Cash Variance"))
    total_sales = compute_total_sales(
        z_reading=z_reading,
        cash_sales=cash_sales,
        card_sales=card_sales,
        explicit_total_sales=explicit_total_sales,
    )
    conversion_rate = compute_conversion_rate(traffic, served, explicit_conversion_rate)
    sales_per_customer = compute_sales_per_customer(total_sales, served, sales_per_customer_reported)
    confidence = compute_confidence(branch, total_sales, cash_sales, card_sales, z_reading, traffic, served)

    flags = derive_flags(
        branch=branch,
        total_sales=total_sales,
        cash=cash_sales,
        card=card_sales,
        z_reading=z_reading,
        traffic=traffic,
        served=served,
        explicit_total_sales=explicit_total_sales,
        explicit_conversion_rate=explicit_conversion_rate,
        computed_conversion_rate=conversion_rate,
        cash_variance=cash_variance_numeric,
    )

    return {
        "type": "sales_day_end",
        "schema_version": "2.0",
        "sales_format": sales_format,
        "branch": branch,
        "branch_slug": branch,
        "date": signal_date,
        "totals": {
            "sales": round(total_sales, 2),
            "cash": round(cash_sales or 0.0, 2),
            "card": round(card_sales or 0.0, 2),
            "z_reading": round(z_reading or 0.0, 2),
            "reported_total_sales": round(explicit_total_sales or 0.0, 2),
            "reported_total_cash": round(explicit_total_cash or 0.0, 2),
            "reported_total_card": round(explicit_total_card or 0.0, 2),
            "cash_variance": round(cash_variance_numeric or 0.0, 2),
        },
        "customers": {
            "traffic": traffic if traffic is not None else None,
            "served": served if served is not None else None,
            "conversion_rate": conversion_rate,
            "reported_conversion_rate": explicit_conversion_rate,
        },
        "performance": {
            "sales_per_customer": sales_per_customer,
            "sales_per_labor_hours": round(sales_per_labor_hours or 0.0, 2),
            "reported_sales_per_customer": round(sales_per_customer_reported or 0.0, 2),
        },
        "operations": {
            "staff_on_duty": staff_on_duty,
            "cashier": cashier or "",
            "assistant": assistant or "",
            "till_1": till_1 or "",
            "balanced": (balanced or "").upper(),
            "balanced_by": balanced_by or "",
            "over_short_reason": over_short_reason or "",
            "supervisor_confirmed": (supervisor_confirmed or "").upper(),
            "notes": notes or "",
        },
        "supervisor_control_report": supervisor,
        "flags": flags,
        "confidence": confidence,
        "source_format": "whatsapp_day_end_sales",
        "raw_text_preview": normalize_spaces(sales_text)[:400],
    }


def save_yaml(parsed: dict[str, Any], output_dir: str = "SIGNALS/normalized") -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    branch = parsed.get("branch") or "unknown"
    signal_date = parsed.get("date") or utc_today_iso()
    out_path = out_dir / f"{branch}_sales_{signal_date}.yaml"

    with out_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(parsed, fh, sort_keys=False, allow_unicode=True)

    print(f"Saved: {out_path}")
    return out_path


def main() -> int:
    try:
        if len(sys.argv) > 1:
            text = Path(sys.argv[1]).read_text(encoding="utf-8")
        else:
            print("Paste WhatsApp report below. CTRL+D when done:")
            text = sys.stdin.read()
    except FileNotFoundError as exc:
        print(f"ERROR: file not found: {exc}", file=sys.stderr)
        return 1

    if not text.strip():
        print("No input received.", file=sys.stderr)
        return 1

    try:
        parsed = parse_sales_report(text)
        save_yaml(parsed)

        print("\nSummary:")
        print(f"- Branch: {parsed['branch']}")
        print(f"- Sales: {parsed['totals']['sales']}")
        print(f"- Cash: {parsed['totals']['cash']}")
        print(f"- Card: {parsed['totals']['card']}")
        print(f"- Traffic: {parsed['customers']['traffic']}")
        print(f"- Served: {parsed['customers']['served']}")
        print(f"- Conversion: {parsed['customers']['conversion_rate']}")
        print(f"- Confidence: {parsed['confidence']}")
        print(f"- Format: {parsed['sales_format']}")
        if parsed["flags"]:
            print(f"- Flags: {parsed['flags']}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
