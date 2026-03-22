#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


# ----------------------------
# Helpers
# ----------------------------

def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def extract_value(
    pattern: str,
    text: str,
    default: float = 0.0,
    flags: int = re.IGNORECASE | re.MULTILINE,
) -> float:
    match = re.search(pattern, text, flags)
    if not match:
        return default
    raw = match.group(1).replace(",", "").strip()
    try:
        return float(raw)
    except ValueError:
        return default


def extract_text(
    pattern: str,
    text: str,
    default: Optional[str] = None,
    flags: int = re.IGNORECASE | re.MULTILINE,
) -> Optional[str]:
    match = re.search(pattern, text, flags)
    return match.group(1).strip() if match else default


# ----------------------------
# Core Parsing
# ----------------------------

def parse_date(text: str) -> str:
    match = re.search(
        r"Date:\s*(?:[A-Za-z]+\s+)?(\d{2}/\d{2}/\d{2})(?:\s+[A-Za-z]+)?",
        text,
        re.IGNORECASE,
    )
    if not match:
        raise ValueError("Could not find report date")

    raw_date = datetime.strptime(match.group(1), "%d/%m/%y")
    return raw_date.strftime("%Y-%m-%d")


def parse_branch(text: str) -> str:
    match = re.search(r"TTC\s+(.*?)\s+Branch", text, re.IGNORECASE)
    if not match:
        return "unknown"
    return slugify(match.group(1))


# ----------------------------
# Tills
# ----------------------------

def parse_tills(text: str) -> List[Dict[str, Any]]:
    tills: List[Dict[str, Any]] = []

    till_pattern = re.compile(
        r"Till#\s*(\d+)\s*:\s*(.*?)\n"
        r"(.*?)(?=\nTill#\s*\d+\s*:|\nTOTALS\b)",
        re.IGNORECASE | re.DOTALL,
    )

    for match in till_pattern.finditer(text):
        till_id = int(match.group(1))
        till_name = slugify(match.group(2))
        block = match.group(3)

        cashier = extract_text(r"Cashier:\s*(.+)", block)
        assistant = extract_text(r"Assistant:\s*(.+)", block)

        cash = extract_value(r"T/Cash:\s*K\s*([\d,\.]+)", block)
        card = extract_value(r"T/Card:\s*K\s*([\d,\.]+)", block)
        z_reading = extract_value(r"Z/Reading:\s*K\s*([\d,\.]+)", block)

        balance_text = extract_text(r"Balance:\s*(.+)", block, default="") or ""
        variance = 0.0
        variance_reason = None

        if balance_text and balance_text.lower() not in {"ok", "none"}:
            amount_match = re.search(r"K\s*([\d,\.]+)", balance_text, re.IGNORECASE)
            if amount_match:
                variance = float(amount_match.group(1).replace(",", ""))

            reason_match = re.search(r"\((.*?)\)", balance_text)
            if reason_match:
                variance_reason = slugify(reason_match.group(1))

        till_entry: Dict[str, Any] = {
            "till_id": till_id,
            "name": till_name,
            "cashier": slugify(cashier) if cashier else None,
            "cash": cash,
            "card": card,
            "z_reading": z_reading,
            "variance": variance,
        }

        if assistant:
            till_entry["assistant"] = slugify(assistant)
        if variance_reason:
            till_entry["variance_reason"] = variance_reason

        tills.append(till_entry)

    return tills


# ----------------------------
# Totals
# ----------------------------

def parse_totals(text: str) -> Dict[str, float]:
    return {
        "cash": extract_value(r"Total\s+Ca(?:sh|ssh):\s*K\s*([\d,\.]+)", text),
        "card": extract_value(r"Total\s+Card:\s*K\s*([\d,\.]+)", text),
        "sales": extract_value(r"Total\s+Sales:\s*K\s*([\d,\.]+)", text),
    }


# ----------------------------
# Customers (UPDATED ✔)
# ----------------------------

def parse_customers(text: str) -> Dict[str, Any]:
    customers: Dict[str, Any] = {
        "traffic": int(extract_value(r"Main\s+Door:\s*(\d+)", text)),
        "conversion_rate": extract_value(
            r"Conversion\s+rate\s*[:=]+\s*([\d\.]+)%?",
            text,
        ) / 100,
    }

    served = re.search(
        r"(?:Guest/\s*)?Customer\s+served:\s*(\d+)",
        text,
        re.IGNORECASE,
    )

    if served:
        customers["served"] = int(served.group(1))

    # fallback conversion
    if customers["conversion_rate"] == 0:
        fallback = re.search(r"(\d+\.?\d*)\s*%", text)
        if fallback:
            customers["conversion_rate"] = float(fallback.group(1)) / 100

    return customers


# ----------------------------
# Performance (UPDATED ✔)
# ----------------------------

def parse_performance(text: str) -> Dict[str, float]:
    perf: Dict[str, float] = {}

    sales_per_customer = re.search(
        r"Sale\s+per\s+customer\s*[:=]+\s*K?\s*([\d\.]+)",
        text,
        re.IGNORECASE,
    )
    if sales_per_customer:
        perf["sales_per_customer"] = float(sales_per_customer.group(1))

    sales_per_labor_hour = re.search(
        r"Sales\s+per\s+labor\s+hour\s*[:=]+\s*K?\s*([\d\.]+)",
        text,
        re.IGNORECASE,
    )
    if sales_per_labor_hour:
        perf["sales_per_labor_hour"] = float(sales_per_labor_hour.group(1))

    return perf


# ----------------------------
# Derived KPIs (NEW 🔥)
# ----------------------------

def derive_kpis(data: Dict[str, Any]) -> None:
    totals = data.get("totals", {})
    customers = data.get("customers", {})
    perf = data.get("performance", {})

    sales = totals.get("sales", 0)
    served = customers.get("served")
    traffic = customers.get("traffic")

    # fallback: sales per customer
    if "sales_per_customer" not in perf:
        if served and served > 0:
            perf["sales_per_customer"] = round(sales / served, 2)

    # fallback: sales per staff (proxy)
    if "sales_per_staff" not in perf:
        staff_count = len(data.get("tills", []))
        if staff_count > 0:
            perf["sales_per_staff"] = round(sales / staff_count, 2)

    # fallback: conversion
    if customers.get("conversion_rate", 0) == 0 and served and traffic:
        customers["conversion_rate"] = round(served / traffic, 3)

    data["performance"] = perf
    data["customers"] = customers


# ----------------------------
# Supervisor
# ----------------------------

def parse_supervisor_section(text: str) -> Dict[str, Any]:
    supervisor: Dict[str, Any] = {"name": None}

    name = extract_text(r"Supervisor:\s*(.+)", text)
    if name:
        supervisor["name"] = slugify(name)

    return supervisor


# ----------------------------
# Flags + Confidence
# ----------------------------

def derive_flags(data: Dict[str, Any]) -> Dict[str, str]:
    flags: Dict[str, str] = {}

    total_sales = data.get("totals", {}).get("sales", 0)

    if total_sales >= 10000:
        flags["performance_flag"] = "strong"
    elif total_sales >= 5000:
        flags["performance_flag"] = "moderate"
    else:
        flags["performance_flag"] = "weak"

    return flags


def derive_confidence(data: Dict[str, Any]) -> float:
    score = 0

    if data.get("totals", {}).get("sales", 0) > 0:
        score += 1
    if data.get("customers", {}).get("conversion_rate", 0) > 0:
        score += 1
    if data.get("performance", {}).get("sales_per_customer"):
        score += 1

    return round(score / 3, 2)


# ----------------------------
# Main Parse
# ----------------------------

def parse_sales_report(text: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "type": "sales_day_end",
        "branch": parse_branch(text),
        "date": parse_date(text),
        "tills": parse_tills(text),
        "totals": parse_totals(text),
        "customers": parse_customers(text),
        "performance": parse_performance(text),
        "supervisor": parse_supervisor_section(text),
    }

    derive_kpis(data)

    data["flags"] = derive_flags(data)
    data["confidence"] = derive_confidence(data)

    return data


# ----------------------------
# Save
# ----------------------------

def save_yaml(data: Dict[str, Any], output_dir: str = "SIGNALS/normalized") -> Path:
    filename = f"{data['branch']}_sales_{data['date']}.yaml"

    path = Path(output_dir) / filename
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, sort_keys=False, allow_unicode=True)

    print(f"Saved: {path}")
    return path


# ----------------------------
# CLI
# ----------------------------

def main() -> int:
    print("Paste WhatsApp report below. CTRL+D when done:\n")

    text = ""
    try:
        while True:
            text += input() + "\n"
    except EOFError:
        pass

    if not text.strip():
        print("No input received.", file=sys.stderr)
        return 1

    try:
        parsed = parse_sales_report(text)
        save_yaml(parsed)

        print("\nSummary:")
        print(f"- Sales: {parsed['totals']['sales']}")
        print(f"- Conversion: {parsed['customers'].get('conversion_rate')}")
        print(f"- Confidence: {parsed['confidence']}")

        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
