#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


BRANCH_ALIASES: dict[str, list[str]] = {
    "waigani": [
        "waigani",
        "ttc waigani",
        "pom waigani",
        "ttc pom waigani branch",
        "ttc waigani branch",
        "waigani branch",
        "port moresby waigani",
    ],
    "bena_road": [
        "bena road",
        "ttc bena road",
        "ttc bena road goroka",
        "bena road goroka",
        "goroka bena road",
        "goroka",
        "ttc goroka",
        "ttc bena",
    ],
    "lae_5th_street": [
        "lae 5th street",
        "5th street",
        "ttc 5th street lae",
        "ttc lae 5th street",
        "lae fifth street",
        "fifth street lae",
    ],
    "lae_malaita": [
        "lae malaita",
        "malaita",
        "malaita street",
        "lae malaita street",
        "ttc malaita",
        "ttc lae malaita",
    ],
}


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
    cleaned = cleaned.strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    number = parse_float(value)
    if number is None:
        return None
    return int(round(number))


def extract_line_value(text: str, *labels: str) -> str | None:
    for label in labels:
        pattern = rf"^\s*{re.escape(label)}\s*:\s*(.+?)\s*$"
        match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if match:
            return normalize_spaces(match.group(1))
    return None


def normalize_branch(raw_branch: str | None) -> str:
    if not raw_branch:
        return "unknown"

    value = normalize_spaces(raw_branch).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"\bbranch\b", " ", value)
    value = re.sub(r"\bttc\b", " ", value)
    value = re.sub(r"\bpom\b", " port moresby ", value)
    value = normalize_spaces(value)

    for canonical, aliases in BRANCH_ALIASES.items():
        if value == canonical:
            return canonical
        for alias in aliases:
            alias_norm = normalize_spaces(alias).lower()
            if alias_norm in value or value in alias_norm:
                return canonical

    compact = slugify(value)
    return compact or "unknown"


def extract_branch(text: str) -> str:
    raw = extract_line_value(text, "Branch", "Shop", "Location")
    return normalize_branch(raw)


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

    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", raw)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3))
        if year < 100:
            year += 2000
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            pass

    return utc_today_iso()


def normalize_item_name(name: str) -> str:
    text = normalize_spaces(name)
    text = re.sub(r"^[#\.\-\s]+", "", text)
    return text


def canonical_item_token(name: str) -> str:
    value = normalize_item_name(name)
    value = value.replace("/", "_")
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def parse_status(value: str | None) -> str:
    if not value:
        return ""
    return normalize_spaces(value).upper()


def extract_structured_bale_blocks(text: str) -> list[dict[str, Any]]:
    lines = [line.rstrip() for line in text.splitlines()]
    blocks: list[list[str]] = []
    current: list[str] = []

    start_pattern = re.compile(r"^\s*Bale_ID\s*:\s*.+$", re.IGNORECASE)

    for line in lines:
        if start_pattern.match(line):
            if current:
                blocks.append(current)
            current = [line]
        else:
            if current:
                current.append(line)

    if current:
        blocks.append(current)

    parsed: list[dict[str, Any]] = []

    for block in blocks:
        block_text = "\n".join(block).strip()
        if not block_text:
            continue

        bale_id = extract_line_value(block_text, "Bale_ID", "Bale ID")
        item_name = extract_line_value(block_text, "Item_Name", "Item Name")
        total_qty = extract_line_value(block_text, "Total_Qty", "Total Qty", "Qty")
        total_amount = extract_line_value(block_text, "Total_Amount", "Total Amount", "Amt", "Amount")
        status = extract_line_value(block_text, "Status")

        bale_no = parse_int(bale_id)
        qty = parse_int(total_qty)
        amount = parse_float(total_amount)

        if bale_no is None and not item_name:
            continue

        avg_unit_price = 0.0
        if qty and qty > 0 and amount is not None:
            avg_unit_price = round(amount / qty, 2)

        parsed.append(
            {
                "bale_no": bale_no or 0,
                "item_name": normalize_item_name(item_name or ""),
                "item_token": canonical_item_token(item_name or ""),
                "qty": qty or 0,
                "amount": round(amount or 0.0, 2),
                "status": parse_status(status),
                "weight_kg": None,
                "avg_unit_price": avg_unit_price,
                "source_block_type": "structured",
                "raw_block_preview": normalize_spaces(block_text)[:220],
            }
        )

    return parsed


def extract_legacy_bale_blocks(text: str) -> list[dict[str, Any]]:
    lines = text.splitlines()
    blocks: list[list[str]] = []
    current: list[str] = []

    header_pattern = re.compile(
        r"^\s*(?:Bale\s*)?#\s*\d+\s*[\.\)]?\s*.+$",
        re.IGNORECASE,
    )

    for raw_line in lines:
        line = raw_line.rstrip()
        if header_pattern.match(line):
            if current:
                blocks.append(current)
            current = [line]
        else:
            if current:
                current.append(line)

    if current:
        blocks.append(current)

    parsed: list[dict[str, Any]] = []

    for block in blocks:
        block_text = "\n".join(block).strip()
        if not block_text:
            continue

        header = block[0].strip()
        match = re.match(
            r"^\s*(?:Bale\s*)?#\s*(\d+)\s*[\.\)]?\s*(.+?)\s*$",
            header,
            flags=re.IGNORECASE,
        )
        if not match:
            continue

        bale_no = int(match.group(1))
        item_name = normalize_item_name(match.group(2))

        qty_match = re.search(r"\bqty\s*:\s*([0-9,]+)", block_text, flags=re.IGNORECASE)
        amt_match = re.search(r"\bamt\s*:\s*K?\s*([0-9,]+(?:\.[0-9]+)?)", block_text, flags=re.IGNORECASE)
        status_match = re.search(r"\bstatus\s*:\s*(.+?)\s*$", block_text, flags=re.IGNORECASE | re.MULTILINE)
        weight_match = re.search(r"(\d+(?:\.\d+)?)\s*kg\b", item_name, flags=re.IGNORECASE)

        qty = parse_int(qty_match.group(1)) if qty_match else None
        amount = parse_float(amt_match.group(1)) if amt_match else None
        weight_kg = parse_float(weight_match.group(1)) if weight_match else None
        status = status_match.group(1).strip() if status_match else ""

        avg_unit_price = 0.0
        if qty and qty > 0 and amount is not None:
            avg_unit_price = round(amount / qty, 2)

        parsed.append(
            {
                "bale_no": bale_no,
                "item_name": item_name,
                "item_token": canonical_item_token(item_name),
                "qty": qty or 0,
                "amount": round(amount or 0.0, 2),
                "status": parse_status(status),
                "weight_kg": round(weight_kg, 2) if weight_kg is not None else None,
                "avg_unit_price": avg_unit_price,
                "source_block_type": "legacy",
                "raw_block_preview": normalize_spaces(block_text)[:220],
            }
        )

    return parsed


def merge_bale_blocks(structured: list[dict[str, Any]], legacy: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if structured:
        return sorted(structured, key=lambda x: (x.get("bale_no", 0), x.get("item_name", "")))
    return sorted(legacy, key=lambda x: (x.get("bale_no", 0), x.get("item_name", "")))


def extract_total_bales(text: str) -> int | None:
    direct = extract_line_value(text, "Total bales break today", "Total Bales Break Today")
    if direct:
        digit_match = re.search(r"(\d+)", direct)
        if digit_match:
            return int(digit_match.group(1))

    count_match = re.search(r"\bTotal\s+bales?\s+break\s+today\b.*?(\d+)", text, flags=re.IGNORECASE)
    if count_match:
        return int(count_match.group(1))

    release_sentence_match = re.search(
        r"\b(\d+)\s*\(?\d*\)?\s*bales?\s+released\s+for\s+sales\b",
        text,
        flags=re.IGNORECASE,
    )
    if release_sentence_match:
        return int(release_sentence_match.group(1))

    return None


def extract_total_qty(text: str) -> int | None:
    raw = extract_line_value(text, "Total quantity", "Total Quantity")
    return parse_int(raw)


def extract_total_amount(text: str) -> float | None:
    raw = extract_line_value(text, "Total Amount", "Total amount")
    return parse_float(raw)


def extract_prepared_by(text: str) -> str:
    raw = extract_line_value(text, "Prepared by", "Prepared By")
    return raw or ""


def extract_release_status(text: str) -> str:
    lowered = text.lower()
    if "all" in lowered and "released for sales" in lowered:
        return "all_released_for_sales"
    if "released for sales" in lowered:
        return "released_for_sales"
    if "yet to release" in lowered or "pending" in lowered or "hold" in lowered:
        return "partial_or_pending"
    return ""


def compute_confidence(
    branch: str,
    bales: list[dict[str, Any]],
    total_bales: int | None,
    total_qty: int | None,
    total_amount: float | None,
) -> float:
    score = 0.0

    if branch != "unknown":
        score += 0.20
    if bales:
        score += 0.30
    if total_bales is not None:
        score += 0.15
    if total_qty is not None:
        score += 0.10
    if total_amount is not None:
        score += 0.10

    parsed_qty_sum = sum((b.get("qty") or 0) for b in bales)
    parsed_amount_sum = round(sum((b.get("amount") or 0.0) for b in bales), 2)

    if parsed_qty_sum > 0:
        score += 0.10
    if parsed_amount_sum > 0:
        score += 0.05

    return round(min(score, 1.0), 2)


def derive_flags(
    branch: str,
    bales: list[dict[str, Any]],
    total_bales: int | None,
    total_qty: int | None,
    total_amount: float | None,
) -> list[str]:
    flags: list[str] = []

    if branch == "unknown":
        flags.append("missing_branch")
    if not bales:
        flags.append("no_bale_blocks_parsed")
    if total_bales is None:
        flags.append("missing_total_bales")
    if total_qty is None:
        flags.append("missing_total_qty")
    if total_amount is None:
        flags.append("missing_total_amount")

    parsed_bales = len(bales)
    parsed_qty = sum((b.get("qty") or 0) for b in bales)
    parsed_amount = round(sum((b.get("amount") or 0.0) for b in bales), 2)

    if total_bales is not None and parsed_bales != total_bales:
        flags.append("parsed_bale_count_mismatch")
    if total_qty is not None and parsed_qty != total_qty:
        flags.append("parsed_qty_mismatch")
    if total_amount is not None and abs(parsed_amount - total_amount) > 0.01:
        flags.append("parsed_amount_mismatch")

    return flags


def parse_bale_summary(text: str) -> dict[str, Any]:
    branch = extract_branch(text)
    signal_date = extract_report_date(text)

    structured_blocks = extract_structured_bale_blocks(text)
    legacy_blocks = extract_legacy_bale_blocks(text)
    bales = merge_bale_blocks(structured_blocks, legacy_blocks)

    total_bales = extract_total_bales(text)
    total_qty = extract_total_qty(text)
    total_amount = extract_total_amount(text)
    prepared_by = extract_prepared_by(text)
    release_status = extract_release_status(text)

    parsed_bale_count = len(bales)
    parsed_qty = sum((b.get("qty") or 0) for b in bales)
    parsed_amount = round(sum((b.get("amount") or 0.0) for b in bales), 2)

    effective_total_bales = total_bales if total_bales is not None else parsed_bale_count
    effective_total_qty = total_qty if total_qty is not None else parsed_qty
    effective_total_amount = round(total_amount if total_amount is not None else parsed_amount, 2)

    confidence = compute_confidence(
        branch=branch,
        bales=bales,
        total_bales=total_bales,
        total_qty=total_qty,
        total_amount=total_amount,
    )
    flags = derive_flags(
        branch=branch,
        bales=bales,
        total_bales=total_bales,
        total_qty=total_qty,
        total_amount=total_amount,
    )

    block_format = "unknown"
    if structured_blocks:
        block_format = "structured"
    elif legacy_blocks:
        block_format = "legacy"

    return {
        "type": "bale_release_summary",
        "branch": branch,
        "date": signal_date,
        "summary": {
            "total_bales": effective_total_bales,
            "total_qty": effective_total_qty,
            "total_amount": effective_total_amount,
            "parsed_bale_count": parsed_bale_count,
            "parsed_qty": parsed_qty,
            "parsed_amount": parsed_amount,
            "release_status": release_status,
            "block_format": block_format,
        },
        "prepared_by": prepared_by,
        "bales": bales,
        "flags": flags,
        "confidence": confidence,
        "source_format": "whatsapp_bale_release_summary",
        "raw_text_preview": normalize_spaces(text)[:320],
    }


def save_yaml(parsed: dict[str, Any], output_dir: str = "SIGNALS/normalized") -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    branch = parsed.get("branch") or "unknown"
    signal_date = parsed.get("date") or utc_today_iso()
    out_path = out_dir / f"{branch}_bale_summary_{signal_date}.yaml"

    with out_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(parsed, fh, sort_keys=False, allow_unicode=True)

    print(f"Saved: {out_path}")
    return out_path


def main() -> int:
    if len(sys.argv) > 1:
        text = Path(sys.argv[1]).read_text(encoding="utf-8")
    else:
        print("Paste WhatsApp bale summary below. CTRL+D when done:")
        text = sys.stdin.read()

    if not text.strip():
        print("No input received.", file=sys.stderr)
        return 1

    try:
        parsed = parse_bale_summary(text)
        save_yaml(parsed)

        print("\nSummary:")
        print(f"- Branch: {parsed['branch']}")
        print(f"- Date: {parsed['date']}")
        print(f"- Block format: {parsed['summary']['block_format']}")
        print(f"- Total bales: {parsed['summary']['total_bales']}")
        print(f"- Total qty: {parsed['summary']['total_qty']}")
        print(f"- Total amount: {parsed['summary']['total_amount']}")
        print(f"- Parsed bale count: {parsed['summary']['parsed_bale_count']}")
        print(f"- Confidence: {parsed['confidence']}")
        print(f"- Flags: {parsed['flags']}")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
