#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


ITEM_ALIASES = {
    "mm 200kg": "mixed_items_200kg",
    "sport shoe cr + extra 25kg": "sport_shoes_cr_extra",
    "sport shoe cr extra 25kg": "sport_shoes_cr_extra",
    "osh 45kg": "osh_45kg",
    "mtsh 45kg": "mtsh_45kg",
    "original shirt 40kg": "original_shirts",
    "children sport shoe creme 25kg": "children_sport_shoes_creme",
    "chsh 45kg": "chsh_45kg",
}


@dataclass
class BaleRecord:
    bale_no: int
    raw_item: str
    item_name: str
    weight: str
    quantity: int
    amount: float


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_") or "unknown"


def normalize_branch(branch: str) -> str:
    b = branch.strip().lower()
    aliases = {
        "ttc pom waiganit": "waigani",
        "ttc waigani": "waigani",
        "waigani": "waigani",
        "ttc bena road": "bena_road",
        "bena road": "bena_road",
        "bena_road": "bena_road",
        "ttc lae 5th street": "5th_street",
        "ttc 5th street": "5th_street",
        "5th street": "5th_street",
        "5th_street": "5th_street",
        "ttc lae malaita": "lae_malaita",
        "lae malaita": "lae_malaita",
        "lae_malaita": "lae_malaita",
    }
    return aliases.get(b, slugify(b))


def normalize_item_name(raw_item: str) -> tuple[str, str]:
    item = raw_item.strip().lower()
    item = re.sub(r"^\d+\.", "", item).strip()
    item = re.sub(r"\s+", " ", item)

    weight_match = re.search(r"(\d+\s*kg)\b", item)
    weight = weight_match.group(1).replace(" ", "") if weight_match else ""

    normalized = ITEM_ALIASES.get(item)
    if not normalized:
        normalized = slugify(item)

    return normalized, weight


def extract_date(text: str) -> str:
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", text)
    if not m:
        raise ValueError("Could not find report date in input file.")
    day, month, year = m.groups()
    if len(year) == 2:
        year = f"20{year}"
    return f"{year}-{int(month):02d}-{int(day):02d}"


def extract_day(text: str) -> str:
    m = re.search(
        r"\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b",
        text,
        flags=re.IGNORECASE,
    )
    return m.group(1).lower() if m else ""


def extract_branch(text: str) -> str:
    m = re.search(r"Branch:\s*(.+)", text, flags=re.IGNORECASE)
    if not m:
        raise ValueError("Could not find branch line in input file.")
    raw = m.group(1).strip()
    raw = re.sub(r"\.$", "", raw)
    raw = re.sub(r"\bbranch\b", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\s+", " ", raw)
    return raw


def parse_int(text: str) -> int:
    cleaned = re.sub(r"[^0-9]", "", text or "")
    return int(cleaned) if cleaned else 0


def parse_float(text: str) -> float:
    cleaned = (text or "").replace(",", "")
    cleaned = re.sub(r"[^0-9.]", "", cleaned)
    return float(cleaned) if cleaned else 0.0


def compute_value_per_item(amount: float, quantity: int) -> float:
    if quantity <= 0:
        return 0.0
    return round(amount / quantity, 4)


def compute_supply_score(amount: float, quantity: int) -> float:
    score = min(quantity, 1200) * 0.03 + min(amount, 15000.0) * 0.002
    return round(min(score, 100.0), 2)


def extract_bale_blocks(text: str) -> list[str]:
    lines = text.splitlines()
    blocks: list[str] = []
    current: list[str] = []

    for line in lines:
        if re.match(r"^\s*#?\d+\.", line) or re.match(r"^\s*#\d+", line):
            if current:
                blocks.append("\n".join(current).strip())
                current = []
        if current or re.match(r"^\s*#?\d+\.", line) or re.match(r"^\s*#\d+", line):
            current.append(line)

    if current:
        blocks.append("\n".join(current).strip())

    filtered: list[str] = []
    for block in blocks:
        if "qty" in block.lower() or "amt" in block.lower() or "amount" in block.lower():
            filtered.append(block)
    return filtered


def parse_bale_block(block: str) -> BaleRecord | None:
    first_line = block.splitlines()[0].strip()

    bale_match = re.match(r"^\s*#?(\d+)\.?\s*(.+)$", first_line)
    if not bale_match:
        return None

    bale_no = int(bale_match.group(1))
    raw_item = bale_match.group(2).strip()

    item_name, weight = normalize_item_name(raw_item)

    qty_match = re.search(r"qty\s*:\s*([0-9,]+)", block, flags=re.IGNORECASE)
    amt_match = re.search(r"amt\s*:\s*K?\s*([0-9,]+\.\d{2}|[0-9,]+)", block, flags=re.IGNORECASE)

    quantity = parse_int(qty_match.group(1)) if qty_match else 0
    amount = parse_float(amt_match.group(1)) if amt_match else 0.0

    return BaleRecord(
        bale_no=bale_no,
        raw_item=raw_item,
        item_name=item_name,
        weight=weight,
        quantity=quantity,
        amount=amount,
    )


def write_bale_signal(
    outdir: Path,
    branch_code: str,
    source_name: str,
    report_date: str,
    day_name: str,
    record: BaleRecord,
    verbose: bool = False,
) -> Path:
    filename = f"bale_release_{branch_code}_{report_date}_bale_{record.bale_no:02d}.md"
    path = outdir / filename

    value_per_item = compute_value_per_item(record.amount, record.quantity)
    supply_score = compute_supply_score(record.amount, record.quantity)

    content = f"""date: {report_date}
day: {day_name}
source_type: bale_release_report
source_name: {source_name}
category: inventory_supply
signal_type: bale_release
branch: {branch_code}
bale_no: {record.bale_no}
item_name: {record.item_name}
weight: {record.weight}
quantity_released: {record.quantity}
amount_released: {record.amount:.2f}
value_per_item: {value_per_item:.4f}
confidence: 0.95
opportunity_score: {supply_score:.2f}
status: active
"""

    path.write_text(content, encoding="utf-8")

    if verbose:
        print(f"Saved: {path}")

    return path


def write_summary_signal(
    outdir: Path,
    branch_code: str,
    source_name: str,
    report_date: str,
    day_name: str,
    total_bales: int,
    total_quantity: int,
    total_amount: float,
    prepared_by: str,
    verbose: bool = False,
) -> Path:
    filename = f"bale_release_summary_{branch_code}_{report_date}.md"
    path = outdir / filename

    avg_value_per_bale = round(total_amount / total_bales, 4) if total_bales else 0.0
    avg_items_per_bale = round(total_quantity / total_bales, 4) if total_bales else 0.0
    summary_score = round(min(total_quantity * 0.02 + total_amount * 0.0015, 100.0), 2)

    content = f"""date: {report_date}
day: {day_name}
source_type: bale_release_report
source_name: {source_name}
category: inventory_supply
signal_type: bale_release_summary
branch: {branch_code}
total_bales: {total_bales}
total_quantity: {total_quantity}
total_amount: {total_amount:.2f}
avg_value_per_bale: {avg_value_per_bale:.4f}
avg_items_per_bale: {avg_items_per_bale:.4f}
prepared_by: {prepared_by}
confidence: 0.95
opportunity_score: {summary_score:.2f}
status: active
"""

    path.write_text(content, encoding="utf-8")

    if verbose:
        print(f"Saved: {path}")

    return path


def extract_summary_totals(text: str) -> tuple[int, int, float]:
    total_bales = 0
    total_quantity = 0
    total_amount = 0.0

    m_bales = re.search(r"Total\s+Bales?\s*:\s*([0-9]+)", text, flags=re.IGNORECASE)
    if m_bales:
        total_bales = parse_int(m_bales.group(1))

    if total_bales == 0:
        m_fallback = re.search(r"Total bales .*?(\d+)", text, flags=re.IGNORECASE)
        if m_fallback:
            total_bales = parse_int(m_fallback.group(1))

    m_qty = re.search(r"Total\s+Quantity\s*:\s*([0-9,]+)", text, flags=re.IGNORECASE)
    if m_qty:
        total_quantity = parse_int(m_qty.group(1))

    m_amt = re.search(r"Total\s+Amount\s*:\s*K?\s*([0-9,]+\.\d{2}|[0-9,]+)", text, flags=re.IGNORECASE)
    if m_amt:
        total_amount = parse_float(m_amt.group(1))

    return total_bales, total_quantity, total_amount


def extract_prepared_by(text: str) -> str:
    m = re.search(r"Prepared by\s*:\s*(.+)", text, flags=re.IGNORECASE)
    if not m:
        return ""
    name = m.group(1).strip()
    name = re.sub(r"\(.*?\)", "", name).strip()
    return name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse WhatsApp bale release summary into colony bale release signals."
    )
    parser.add_argument("input_file", help="Path to raw bale release summary text file")
    parser.add_argument("--outdir", default="SIGNALS/normalized", help="Output directory")
    parser.add_argument("--source-name", default="", help="Override source_name value")
    parser.add_argument("--branch", default="", help="Override branch code")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    parser.add_argument("--verbose", action="store_true", help="Print saved files")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    input_path = Path(args.input_file)
    outdir = Path(args.outdir)

    if not input_path.exists():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        return 1

    text = input_path.read_text(encoding="utf-8", errors="ignore")

    report_date = extract_date(text)
    day_name = extract_day(text)
    header_branch = extract_branch(text)

    branch_code = args.branch.strip() or normalize_branch(header_branch)
    source_name = args.source_name.strip() or branch_code

    outdir.mkdir(parents=True, exist_ok=True)

    blocks = extract_bale_blocks(text)
    written = 0

    parsed_records: list[BaleRecord] = []
    for block in blocks:
        record = parse_bale_block(block)
        if record is None:
            continue
        parsed_records.append(record)

    for record in parsed_records:
        filename = outdir / f"bale_release_{branch_code}_{report_date}_bale_{record.bale_no:02d}.md"
        if filename.exists() and not args.force:
            if args.verbose:
                print(f"Skip existing: {filename}")
            continue

        write_bale_signal(
            outdir=outdir,
            branch_code=branch_code,
            source_name=source_name,
            report_date=report_date,
            day_name=day_name,
            record=record,
            verbose=args.verbose,
        )
        written += 1

    total_bales, total_quantity, total_amount = extract_summary_totals(text)
    prepared_by = extract_prepared_by(text)

    if total_bales == 0:
        total_bales = len(parsed_records)
    if total_quantity == 0:
        total_quantity = sum(r.quantity for r in parsed_records)
    if total_amount == 0.0:
        total_amount = round(sum(r.amount for r in parsed_records), 2)

    write_summary_signal(
        outdir=outdir,
        branch_code=branch_code,
        source_name=source_name,
        report_date=report_date,
        day_name=day_name,
        total_bales=total_bales,
        total_quantity=total_quantity,
        total_amount=total_amount,
        prepared_by=prepared_by,
        verbose=args.verbose,
    )

    print(f"Parsed branch: {branch_code}")
    print(f"Date: {report_date}")
    print(f"Bale signals written: {written}")
    print(f"Summary signal written: 1")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
