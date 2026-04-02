#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import re

try:
    from scripts.utils_normalization import normalize_branch as shared_normalize_branch
except ModuleNotFoundError:
    from utils_normalization import normalize_branch as shared_normalize_branch

def clean_section_name(section):
    # remove leading numbers like "20.", "10_", "3 "
    section = re.sub(r'^\d+[\.\s_]*', '', section)
    return section.strip().lower().replace(" ", "_")

DEFAULT_CONFIDENCE = 0.90


SECTION_ALIASES = {
    "pricing room": "pricing_room",
    "mens shirts": "mens_shirts",
    "men shirts": "mens_shirts",
    "cashier till # 1": "cashier_till_1",
    "cashier till #1": "cashier_till_1",
    "cashier till # 2": "cashier_till_2",
    "cashier till #2": "cashier_till_2",
    "cashier till # 3": "cashier_till_3",
    "cashier till #3": "cashier_till_3",
    "shoe shop": "shoe_shop",
    "ladies jeans & mens shorts": "ladies_jeans_mens_shorts",
    "kids boys section": "kids_boys",
    "mens jeans,mens cotton pants": "mens_jeans_mens_cotton_pants",
    "mens jeans, mens cotton pants": "mens_jeans_mens_cotton_pants",
    "monitor till # 1 & 2": "monitor_tills",
    "monitor till #1 & 2": "monitor_tills",
    "jacket section & reflectors": "jackets_reflectors",
    "skirt,cotton caprice pants & ladies shots": "skirts_caprice_ladies_shorts",
    "skirt, cotton caprice pants & ladies shots": "skirts_caprice_ladies_shorts",
    "kids girls section": "kids_girls",
    "mix rails": "mixed_rails",
    "ladies dress": "ladies_dress",
    "ladies t shirts  & blouse": "ladies_tshirts_blouse",
    "ladies t shirts & blouse": "ladies_tshirts_blouse",
    "ladies t-shirts & blouse": "ladies_tshirts_blouse",
    "cream rails": "cream_rails",
    "ladies leggings & ladies ³/⁴ jeans": "ladies_leggings_3_4_jeans",
    "ladies leggings & ladies 3/4 jeans": "ladies_leggings_3_4_jeans",
    "beddings": "beddings",
    "main door guards": "door_guard_main",
    "small door guards": "door_guard_small",
}


@dataclass
class StaffSalesRecord:
    line_no: int
    raw_name: str
    raw_section: str
    customers_assisted: int
    items_sold: int
    status: str


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("³/⁴", "3_4")
    text = text.replace("3/4", "3_4")
    text = re.sub(r"[&/,+#]+", "_", text)
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_") or "unknown"


def normalize_branch(branch: str) -> str:
    normalized = shared_normalize_branch(
        branch,
        style="legacy_short_slug",
        fallback="slugify",
        match_substring=False,
        profile="legacy_short_exact",
    )
    return str(normalized or "unknown")


def canonical_branch_slug(branch: str) -> str:
    normalized = shared_normalize_branch(
        branch,
        style="canonical_slug",
        fallback="unknown",
        match_substring=True,
    )
    return str(normalized or "unknown")


def normalize_section(section: str) -> str:
    s = section.strip().lower()
    s = re.sub(r"\.$", "", s)
    s = re.sub(r"\s+", " ", s)
    return SECTION_ALIASES.get(s, slugify(s))


def normalize_name(name: str) -> str:
    n = name.strip()
    n = re.sub(r"\b(off|absent|leave)\b.*$", "", n, flags=re.IGNORECASE).strip()
    n = n.replace(".", " ")
    n = re.sub(r"[_*]+", " ", n)
    n = re.sub(r"\s+", " ", n)
    return n.strip()


def detect_status(text: str) -> str:
    t = text.lower()
    if "absent" in t:
        return "absent"
    if "leave" in t:
        return "leave"
    if re.search(r"\boff\b", t):
        return "off"
    return "active"


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


def extract_branch_header(text: str) -> str:
    first_lines = text.splitlines()[:5]
    joined = " ".join(x.strip() for x in first_lines if x.strip())
    m = re.search(r"(TTC\s+[A-Za-z0-9 ,]+branch|TTC\s+[A-Za-z0-9 ,]+)", joined, flags=re.IGNORECASE)
    if not m:
        raise ValueError("Could not find branch header in input file.")
    header = m.group(1)
    header = re.sub(r",\s*GKA", "", header, flags=re.IGNORECASE)
    header = re.sub(r"\bbranch\b", "", header, flags=re.IGNORECASE)
    header = re.sub(r"\s+", " ", header).strip()
    return header


def parse_int(value: str) -> int:
    value = value.strip()
    m = re.search(r"(\d+)", value)
    return int(m.group(1)) if m else 0


def compute_opportunity_score(items_sold: int, customers_assisted: int, status: str) -> float:
    if status in {"off", "absent", "leave"}:
        return 0.0

    score = (
        min(items_sold, 120) * 0.70
        + min(customers_assisted, 30) * 1.20
    )

    return round(min(score, 100.0), 2)


def build_record(entry_text: str, line_no: int) -> StaffSalesRecord | None:
    text = entry_text.strip()
    if not text:
        return None

    first_line = text.splitlines()[0].strip()

    # Guards block names without metrics
    if "(" not in text and "-" not in text and ":" not in text:
        return None

    status = detect_status(text)

    # Typical pattern: section (Name)
    m = re.search(r"^(.*?)(?:\s*\(([^()]+)\))", first_line)
    raw_section = ""
    raw_name = ""

    if m:
        raw_section = m.group(1).strip(" .*")
        raw_name = m.group(2).strip()
    else:
        # Pattern like: 24. Hupa Libula
        n = re.search(r"^\d+\.\s*(.+)$", first_line)
        if n:
            raw_name = n.group(1).strip()
            raw_section = ""
        else:
            return None

    raw_name = normalize_name(raw_name)

    customers = 0
    items = 0

    customers_patterns = [
        r"No\.?\s*Of?\s*Customers?\s*Assisted\s*:\s*([0-9]*)",
        r"No\.?\s*Customers?\s*Assisted\s*:\s*([0-9]*)",
        r"No\.?\s*customer\s*Assisted\s*:\s*([0-9]*)",
        r"No\.?\s*customer\s*assisted\s*:\s*([0-9]*)",
    ]
    items_patterns = [
        r"No\.?\s*Of?\s*Items?\s*Sold\s*:\s*([0-9]*)",
        r"No\.?\s*Items?\s*Sold\s*:\s*([0-9]*)",
        r"No\.?\s*item item sold\s*:\s*([0-9]*)",
        r"No\.?\s*items sold\s*:\s*([0-9]*)",
    ]

    for pat in customers_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            customers = parse_int(m.group(1))
            break

    for pat in items_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            items = parse_int(m.group(1))
            break

    return StaffSalesRecord(
        line_no=line_no,
        raw_name=raw_name,
        raw_section=raw_section,
        customers_assisted=customers,
        items_sold=items,
        status=status,
    )


def split_entries(text: str) -> list[str]:
    lines = text.splitlines()
    entries: list[str] = []
    current: list[str] = []

    for line in lines:
        if re.match(r"^\s*\d+\.", line):
            if current:
                entries.append("\n".join(current).strip())
                current = []
        current.append(line)

    if current:
        entries.append("\n".join(current).strip())

    clean_entries: list[str] = []
    for e in entries:
        if re.match(r"^\s*\d+\.", e):
            clean_entries.append(e)
    return clean_entries


def write_signal_file(
    outdir: Path,
    branch_code: str,
    branch_slug: str,
    source_name: str,
    report_date: str,
    day_name: str,
    rec: StaffSalesRecord,
    verbose: bool = False,
) -> Path:
    name_slug = slugify(rec.raw_name)
    section_slug = normalize_section(rec.raw_section) if rec.raw_section else "unknown"
    branch_id = "bena" if branch_code == "bena_road" else branch_code.replace("_", "-")
    staff_id = f"staff-{branch_id}-{name_slug}"
    filename = f"{staff_id}_{report_date}.md"
    path = outdir / filename

    opportunity_score = compute_opportunity_score(
        rec.items_sold,
        rec.customers_assisted,
        rec.status,
    )

    signal_type = "productivity_signal"
    if rec.status in {"off", "absent", "leave"}:
        signal_type = "attendance_signal"

    description = (
        f"{rec.raw_name} in {rec.raw_section or 'unknown'} assisted "
        f"{rec.customers_assisted} customer(s) and sold {rec.items_sold} item(s)."
    )

    content = f"""date: {report_date}
day: {day_name}
source_type: staff_sales_report
source_name: {source_name}
branch_slug: {branch_slug}
category: staff_sales
signal_type: {signal_type}
staff_id: {staff_id}
section: {section_slug}
products: {section_slug}
items_moved: {rec.items_sold}
assisting_count: {rec.customers_assisted}
description: {description}
confidence: {DEFAULT_CONFIDENCE:.2f}
opportunity_score: {opportunity_score:.2f}
status: {rec.status}
"""

    path.write_text(content, encoding="utf-8")

    if verbose:
        print(f"Saved: {path}")

    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse WhatsApp staff sales tally report into colony staff sales signals."
    )
    parser.add_argument("input_file", help="Path to raw WhatsApp staff sales report text file")
    parser.add_argument("--outdir", default="SIGNALS/normalized", help="Output directory")
    parser.add_argument("--source-name", default="", help="Override source_name value")
    parser.add_argument("--branch", default="", help="Override branch code")
    parser.add_argument("--force", action="store_true", help="Overwrite existing signal files")
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

    header_branch = extract_branch_header(text)
    source_name = args.source_name.strip() or normalize_branch(header_branch)
    branch_code = args.branch.strip() or normalize_branch(header_branch)
    branch_slug = canonical_branch_slug(args.branch.strip() or header_branch)

    outdir.mkdir(parents=True, exist_ok=True)

    entries = split_entries(text)
    written = 0
    skipped = 0

    for entry in entries:
        line_no_match = re.match(r"^\s*(\d+)\.", entry)
        line_no = int(line_no_match.group(1)) if line_no_match else 0

        rec = build_record(entry, line_no=line_no)
        if rec is None:
            skipped += 1
            continue

        if not rec.raw_name:
            skipped += 1
            continue

        path = outdir / f"staff-{branch_code.replace('_', '-')}-{slugify(rec.raw_name)}_{report_date}.md"
        if path.exists() and not args.force:
            if args.verbose:
                print(f"Skip existing: {path}")
            continue

        write_signal_file(
            outdir=outdir,
            branch_code=branch_code,
            branch_slug=branch_slug,
            source_name=source_name,
            report_date=report_date,
            day_name=day_name,
            rec=rec,
            verbose=args.verbose,
        )
        written += 1

    print(f"Parsed branch: {branch_code}")
    print(f"Date: {report_date}")
    print(f"Signals written: {written}")
    print(f"Entries skipped: {skipped}")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
