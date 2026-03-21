#!/usr/bin/env python3
"""
parse_whatsapp_staff.py

Purpose
-------
Parse a pasted WhatsApp daily staff performance report into normalized
staff signal files for IOI Colony.

Governance
----------
Advisory support only.
This script:
- reads raw text
- extracts staff performance observations
- writes normalized signal files

It does NOT:
- execute actions
- contact anyone
- make decisions
- score staff for HR control

Supported branch normalization
------------------------------
All aliases normalize to canonical branch names:

- Waigani         -> TTC Waigani
- Mataita Street  -> TTC Mataita Street
- 5th Street      -> TTC 5th Street
- Bena Road       -> TTC Bena Road

Typical input example
---------------------
TTC WAIGANI BRANCH
Friday 20/03/26

➡️STUFF PERFORMANCE....

1..MILFORD..
SECTION.. BOYS.
🔹Arrangements (5)
🔹Display (5)
🔹Performance (5)

2.GRACE
SECTION.. MANS SHOE, PACIFIC SHIRTS
🔹Arrangements (5)
🔹Display (5)
🔹Performance (5)
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


OUTDIR_DEFAULT = Path("SIGNALS/normalized")
SOURCE_TYPE = "staff_report"
CATEGORY = "staff"


CANONICAL_BRANCHES = {
    "waigani": {
        "canonical_name": "TTC Waigani",
        "slug": "waigani",
        "aliases": [
            "waigani",
            "ttc waigani",
            "waigani branch",
        ],
    },
    "mataita": {
        "canonical_name": "TTC Mataita Street",
        "slug": "mataita",
        "aliases": [
            "mataita",
            "mataita street",
            "ttc mataita",
            "mataita branch",
            "ttc mataita street",
        ],
    },
    "5th_street": {
        "canonical_name": "TTC 5th Street",
        "slug": "5th_street",
        "aliases": [
            "5th",
            "5th street",
            "fifth street",
            "branch 5",
            "ttc 5th",
            "ttc 5th street",
            "ttc fifth street",
        ],
    },
    "bena": {
        "canonical_name": "TTC Bena Road",
        "slug": "bena",
        "aliases": [
            "bena",
            "bena road",
            "ttc bena",
            "bena branch",
            "ttc bena road",
        ],
    },
}


@dataclass
class StaffRecord:
    staff_name: str
    section: str
    arrangements: Optional[int]
    display: Optional[int]
    performance: Optional[int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse a WhatsApp staff performance report into normalized colony signals."
    )
    parser.add_argument(
        "input_file",
        help="Path to pasted WhatsApp report text file",
    )
    parser.add_argument(
        "--outdir",
        default=str(OUTDIR_DEFAULT),
        help=f"Output directory for normalized files (default: {OUTDIR_DEFAULT})",
    )
    parser.add_argument(
        "--source-name",
        default="",
        help="Optional explicit canonical source name override, e.g. 'TTC Waigani'",
    )
    parser.add_argument(
        "--branch",
        default="",
        help="Optional explicit canonical branch slug override, e.g. 'waigani'",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing normalized files",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print parse diagnostics",
    )
    return parser.parse_args()


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def parse_report_date(text: str) -> tuple[str, str]:
    """
    Parse lines like:
    Friday 20/03/26
    Saturday 21/03/26
    """
    m = re.search(
        r"\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b\s+(\d{1,2})/(\d{1,2})/(\d{2})",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        raise RuntimeError(
            "Could not find report day/date in expected format like 'Friday 20/03/26'."
        )

    day_name = m.group(1).capitalize()
    dd = int(m.group(2))
    mm = int(m.group(3))
    yy = int(m.group(4))
    yyyy = 2000 + yy
    iso_date = f"{yyyy:04d}-{mm:02d}-{dd:02d}"
    return day_name, iso_date


def parse_branch_line(text: str) -> str:
    """
    Prefer a line that contains TTC and/or BRANCH, but if absent
    we will still normalize later from whatever branch text exists.
    """
    for line in text.splitlines():
        cleaned = normalize_spaces(line)
        if not cleaned:
            continue
        upper = cleaned.upper()
        if "BRANCH" in upper or "TTC" in upper:
            if any(
                keyword in upper
                for keyword in ["WAIGANI", "MATAITA", "5TH", "FIFTH", "BENA"]
            ):
                return cleaned

    # fallback: scan any line for known branch names
    for line in text.splitlines():
        cleaned = normalize_spaces(line)
        if not cleaned:
            continue
        upper = cleaned.upper()
        if any(keyword in upper for keyword in ["WAIGANI", "MATAITA", "5TH", "FIFTH", "BENA"]):
            return cleaned

    return "TTC Unknown Branch"


def canonical_branch_from_text(text: str) -> tuple[str, str]:
    """
    Return (canonical_name, slug)
    """
    t = normalize_spaces(text).lower()

    # ordered checks
    if "waigani" in t:
        meta = CANONICAL_BRANCHES["waigani"]
        return meta["canonical_name"], meta["slug"]

    if "mataita" in t:
        meta = CANONICAL_BRANCHES["mataita"]
        return meta["canonical_name"], meta["slug"]

    if "5th" in t or "fifth" in t or "branch 5" in t:
        meta = CANONICAL_BRANCHES["5th_street"]
        return meta["canonical_name"], meta["slug"]

    if "bena" in t:
        meta = CANONICAL_BRANCHES["bena"]
        return meta["canonical_name"], meta["slug"]

    cleaned = normalize_spaces(text)
    cleaned = re.sub(r"\bbranch\b", "", cleaned, flags=re.IGNORECASE).strip()
    return cleaned or "TTC Unknown Branch", slug(cleaned or "unknown_branch")


def source_name_from_title(title: str) -> str:
    canonical_name, _ = canonical_branch_from_text(title)
    return canonical_name


def branch_slug_from_title(title: str) -> str:
    _, canonical_slug = canonical_branch_from_text(title)
    return canonical_slug


def split_candidate_blocks(text: str) -> List[str]:
    """
    Split on numbered staff starts like:
    1..MILFORD..
    2.GRACE
    3..KIMSON
    """
    lines = text.splitlines()
    starts: List[int] = []

    for i, line in enumerate(lines):
        if re.match(r"^\s*\d+\s*[\.\)]", line):
            starts.append(i)

    if not starts:
        return []

    starts.append(len(lines))
    blocks: List[str] = []
    for a, b in zip(starts, starts[1:]):
        block = "\n".join(lines[a:b]).strip()
        if block:
            blocks.append(block)
    return blocks


def extract_staff_name(block: str) -> str:
    first = block.splitlines()[0]
    first = re.sub(r"^\s*\d+\s*[\.\)]*\s*", "", first)
    first = first.replace(".", " ")
    first = normalize_spaces(first).strip(" ._-")
    return first.title()


def extract_section(block: str) -> str:
    m = re.search(r"SECTION\s*[\.\-:]*\s*(.+)", block, flags=re.IGNORECASE)
    if not m:
        return "mixed"
    section = m.group(1).splitlines()[0]
    return normalize_spaces(section).strip(" .")


def extract_rating(block: str, label: str) -> Optional[int]:
    patterns = [
        rf"{label}\s*\((\d+)\)",
        rf"{label}\s*[\.\-:]*\s*(\d+)",
    ]
    for pat in patterns:
        m = re.search(pat, block, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
    return None


def parse_staff_blocks(text: str, verbose: bool = False) -> List[StaffRecord]:
    blocks = split_candidate_blocks(text)
    records: List[StaffRecord] = []

    for block in blocks:
        name = extract_staff_name(block)
        if not name:
            continue

        section = extract_section(block)
        arrangements = extract_rating(block, "Arrangements?")
        display = extract_rating(block, "Display")
        performance = extract_rating(block, "Performance")

        record = StaffRecord(
            staff_name=name,
            section=section,
            arrangements=arrangements,
            display=display,
            performance=performance,
        )
        records.append(record)

        if verbose:
            print(
                f"PARSED staff={record.staff_name!r} section={record.section!r} "
                f"arr={record.arrangements} disp={record.display} perf={record.performance}",
                file=sys.stderr,
            )

    return records


def average_present(values: List[Optional[int]]) -> float:
    present = [v for v in values if v is not None]
    if not present:
        return 0.0
    return sum(present) / len(present)


def infer_signal_type(rec: StaffRecord) -> str:
    avg = average_present([rec.arrangements, rec.display, rec.performance])
    if avg >= 4.5:
        return "productivity_signal"
    if avg >= 3.5:
        return "stability_signal"
    return "observation_signal"


def infer_confidence(rec: StaffRecord) -> float:
    present_count = sum(
        v is not None for v in [rec.arrangements, rec.display, rec.performance]
    )
    avg = average_present([rec.arrangements, rec.display, rec.performance])

    base = 0.70
    base += 0.05 * present_count
    base += min(avg / 20.0, 0.10)
    return round(min(base, 0.98), 2)


def infer_opportunity_score(rec: StaffRecord) -> int:
    avg = average_present([rec.arrangements, rec.display, rec.performance])
    if avg >= 4.7:
        return 9
    if avg >= 4.0:
        return 8
    if avg >= 3.0:
        return 6
    return 4


def infer_items_moved(rec: StaffRecord) -> int:
    """
    Placeholder proxy from ratings only.
    This is not true sales volume. It is a normalized movement proxy
    derived from observed ratings so the colony can ingest the signal.
    """
    avg = average_present([rec.arrangements, rec.display, rec.performance])
    return int(round(avg * 20)) if avg > 0 else 0


def infer_assisting_count(rec: StaffRecord) -> int:
    """
    Placeholder proxy from ratings only.
    """
    vals = [v or 0 for v in [rec.arrangements, rec.display, rec.performance]]
    return int(sum(vals))


def build_description(rec: StaffRecord) -> str:
    parts = []
    if rec.performance is not None:
        parts.append(f"performance {rec.performance}/5")
    if rec.display is not None:
        parts.append(f"display {rec.display}/5")
    if rec.arrangements is not None:
        parts.append(f"arrangements {rec.arrangements}/5")
    rating_text = ", ".join(parts) if parts else "ratings observed"
    return f"{rec.staff_name} observed in section '{rec.section}' with {rating_text}"


def build_staff_id(branch_slug: str, staff_name: str) -> str:
    return f"STAFF-{branch_slug.upper()}-{slug(staff_name).upper()}"


def build_signal_content(
    *,
    signal_id: str,
    iso_date: str,
    day_name: str,
    source_name: str,
    branch_slug: str,
    rec: StaffRecord,
) -> str:
    signal_type = infer_signal_type(rec)
    confidence = infer_confidence(rec)
    opportunity_score = infer_opportunity_score(rec)
    items_moved = infer_items_moved(rec)
    assisting_count = infer_assisting_count(rec)
    staff_id = build_staff_id(branch_slug, rec.staff_name)

    return (
        f"signal_id: {signal_id}\n"
        f"date: {iso_date}\n"
        f"day: {day_name}\n"
        f"source_type: {SOURCE_TYPE}\n"
        f"source_name: {source_name}\n"
        f"category: {CATEGORY}\n"
        f"signal_type: {signal_type}\n"
        f"staff_id: {staff_id}\n"
        f"section: {rec.section.lower()}\n"
        f"products: {rec.section.lower()}\n"
        f"items_moved: {items_moved}\n"
        f"assisting_count: {assisting_count}\n"
        f"description: {build_description(rec)}\n"
        f"confidence: {confidence}\n"
        f"opportunity_score: {opportunity_score}\n"
        f"status: new\n"
        f"arrangements_score: {rec.arrangements if rec.arrangements is not None else ''}\n"
        f"display_score: {rec.display if rec.display is not None else ''}\n"
        f"performance_score: {rec.performance if rec.performance is not None else ''}\n"
    )


def main() -> int:
    args = parse_args()

    input_path = Path(args.input_file)
    if not input_path.exists():
        raise RuntimeError(f"Input file not found: {input_path}")

    text = input_path.read_text(encoding="utf-8", errors="ignore")
    day_name, iso_date = parse_report_date(text)

    branch_title = parse_branch_line(text)

    if args.source_name:
        source_name = args.source_name
    else:
        source_name = source_name_from_title(branch_title)

    if args.branch:
        branch_slug = args.branch
    else:
        branch_slug = branch_slug_from_title(branch_title)

    records = parse_staff_blocks(text, verbose=args.verbose)
    if not records:
        raise RuntimeError("No staff records parsed from input report.")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    written = []
    skipped = []
    base_id = 5001

    for idx, rec in enumerate(records, start=base_id):
        signal_type = infer_signal_type(rec)
        filename = (
            f"{branch_slug}_staff_{slug(rec.staff_name)}_{slug(signal_type)}_{iso_date}.md"
        )
        path = outdir / filename

        if path.exists() and not args.force:
            skipped.append(path)
            continue

        signal_id = f"SIG-{iso_date}-{idx}"
        content = build_signal_content(
            signal_id=signal_id,
            iso_date=iso_date,
            day_name=day_name,
            source_name=source_name,
            branch_slug=branch_slug,
            rec=rec,
        )

        path.write_text(content, encoding="utf-8")
        if not path.exists() or path.stat().st_size == 0:
            raise RuntimeError(f"Failed to write valid signal file: {path}")

        written.append(path)

    print(f"PARSED {len(records)} STAFF RECORDS")
    print(f"SOURCE NAME: {source_name}")
    print(f"BRANCH SLUG: {branch_slug}")
    print(f"WROTE {len(written)} FILES")
    for p in written:
        print(p)

    print(f"SKIPPED {len(skipped)} EXISTING FILES")
    for p in skipped:
        print(p)

    print("\nNEXT:")
    print("  python3 update_staff_index.py")
    print(f"  ls {outdir}/{branch_slug}_staff_*_{iso_date}.md | wc -l")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
