#!/usr/bin/env python3

import argparse
import json
import re
import shutil
from pathlib import Path
from datetime import datetime

try:
    from scripts.utils_normalization import normalize_branch as shared_normalize_branch
except ModuleNotFoundError:
    from utils_normalization import normalize_branch as shared_normalize_branch

# -----------------------------
# Helpers
# -----------------------------

def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def extract_number(text: str):
    m = re.search(r"([0-9,]+)", text)
    return float(m.group(1).replace(",", "")) if m else None

def extract_money(text: str):
    m = re.search(r"K\s*([0-9,]+(?:\.[0-9]{1,2})?)", text, re.IGNORECASE)
    return float(m.group(1).replace(",", "")) if m else None

def extract_written_number(text: str):
    words = {
        "one":1,"two":2,"three":3,"four":4,"five":5,
        "six":6,"seven":7,"eight":8,"nine":9,"ten":10
    }
    lower = text.lower()

    num = extract_number(text)
    if num is not None:
        return int(num)

    for w,v in words.items():
        if w in lower:
            return v
    return None

def parse_date(text: str):
    m = re.search(r"\b(\d{2}/\d{2}/\d{2,4})\b", text)
    if not m:
        return None
    d = datetime.strptime(m.group(1), "%d/%m/%y")
    return d.strftime("%Y-%m-%d")

def normalize_branch(text: str):
    normalized = shared_normalize_branch(
        text,
        style="canonical_slug",
        fallback="lower_token",
        match_substring=True,
        profile="keyword_legacy",
    )
    return str(normalized or "")

# -----------------------------
# Archive handling (symlink safe)
# -----------------------------

def archive_raw_input(source_file: Path, raw_archive_dir: Path, branch: str, report_date: str) -> Path:
    archive_dir = raw_archive_dir / branch / report_date
    archive_dir.mkdir(parents=True, exist_ok=True)

    dest = archive_dir / "daily_bale_summary_released_to_rail.txt"

    if source_file.resolve() == dest.resolve():
        return dest

    shutil.copy2(source_file, dest)
    return dest

# -----------------------------
# Parser
# -----------------------------

def parse_file(path: Path):

    lines = path.read_text(encoding="utf-8").splitlines()

    result = {
        "branch": None,
        "branch_slug": None,
        "date": None,
        "prepared_by": None,
        "bales": [],
        "totals": {
            "bales_broken_today": None,
            "bales_released_today": None,
            "bales_pending_release": None,
            "total_quantity": None,
            "total_amount": None
        }
    }

    current_bale = None

    def flush():
        nonlocal current_bale
        if current_bale:
            result["bales"].append(current_bale)
            current_bale = None

    for line in lines:
        l = normalize_whitespace(line)
        lower = l.lower()

        if not l:
            continue

        # Branch
        if "branch:" in lower:
            result["branch"] = normalize_branch(l.split(":",1)[1])
            result["branch_slug"] = result["branch"]
            continue

        # Date
        if not result["date"]:
            d = parse_date(l)
            if d:
                result["date"] = d

        # Prepared by
        if "prepared by" in lower:
            result["prepared_by"] = l.split(":",1)[1].strip()
            continue

        # Totals
        if "total qty" in lower or "total quantity" in lower:
            result["totals"]["total_quantity"] = extract_number(l)
            continue

        if "total amount" in lower:
            result["totals"]["total_amount"] = extract_money(l)
            continue

        # Natural language
        if "break today" in lower:
            result["totals"]["bales_broken_today"] = extract_written_number(l)
            continue

        rel = re.search(r"(?:\(|\b)(\d+|\w+)\)?\s*bales?\s*released", lower)
        if rel:
            result["totals"]["bales_released_today"] = extract_written_number(rel.group(0))
            continue

        pend = re.search(r"(?:\(|\b)(\d+|\w+)\)?\s*bales?\s*(?:yet|pending)", lower)
        if pend:
            result["totals"]["bales_pending_release"] = extract_written_number(pend.group(0))
            continue

        # Bale header
        m = re.match(r"#?\s*(\d+)\.?\s*(.+)", l)
        if m and "amt" not in lower and "qty" not in lower:
            flush()
            current_bale = {
                "bale_number": int(m.group(1)),
                "item_name": m.group(2),
                "qty": None,
                "amount": None
            }
            continue

        # Combined line
        m = re.search(r"qty[: ]*([0-9,]+).*amt[: ]*k?([0-9,]+(?:\.[0-9]+)?)", lower)
        if current_bale and m:
            current_bale["qty"] = float(m.group(1).replace(",", ""))
            current_bale["amount"] = float(m.group(2).replace(",", ""))
            continue

        # Qty
        if current_bale and "qty" in lower:
            q = extract_number(l)
            if q:
                current_bale["qty"] = q
            continue

        # Amount
        if current_bale and "amt" in lower:
            a = extract_money(l)
            if a:
                current_bale["amount"] = a
            continue

    flush()

    # Derived
    if result["totals"]["bales_released_today"] and result["totals"]["bales_broken_today"]:
        result["totals"]["bales_pending_release"] = (
            result["totals"]["bales_broken_today"] -
            result["totals"]["bales_released_today"]
        )

    return result

# -----------------------------
# Output
# -----------------------------

def build_output(parsed, source, base_dir):

    branch = parsed["branch"]
    date = parsed["date"]

    out_dir = base_dir / "SIGNALS" / "normalized" / branch / date
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / f"{branch}_bale_release_{date}.json"

    total_qty = parsed["totals"]["total_quantity"] or 0
    total_amt = parsed["totals"]["total_amount"] or 0

    payload = {
        "signal_type": "bale_release_summary",
        "event_kind": "daily_bale_summary_released_to_rail",
        "branch": branch,
        "branch_slug": parsed.get("branch_slug") or branch,
        "report_date": date,
        "source_file": str(source),
        "prepared_by": parsed["prepared_by"],
        "totals": {
            **parsed["totals"],
            "avg_price_per_piece": round(total_amt/total_qty,2) if total_qty else None
        },
        "bales": parsed["bales"],
        "operational_flags": {
            "full_release_completed": parsed["totals"]["bales_pending_release"] == 0,
            "release_backlog": parsed["totals"]["bales_pending_release"] not in (0,None)
        }
    }

    out_file.write_text(json.dumps(payload, indent=2))
    return out_file

# -----------------------------
# CLI
# -----------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")
    parser.add_argument("--print-markdown", action="store_true")
    args = parser.parse_args()

    base = Path.cwd()

    parsed = parse_file(Path(args.input_file))

    archived = archive_raw_input(
        Path(args.input_file),
        base / "RAW_INPUT" / "bale_release",
        parsed["branch"],
        parsed["date"]
    )

    out = build_output(parsed, archived, base)

    print("[OK] normalized:", out)

    if args.print_markdown:
        print(json.dumps(parsed, indent=2))

if __name__ == "__main__":
    main()
