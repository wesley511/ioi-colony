from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

try:
    from scripts.section_normalizer import normalize_section_name
    from scripts.staff_master_data import resolve_staff
    from scripts.utils_normalization import normalize_branch
except ModuleNotFoundError:
    from section_normalizer import normalize_section_name
    from staff_master_data import resolve_staff
    from utils_normalization import normalize_branch


OUTPUT_DIR = Path("SIGNALS/normalized")
DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{2,4})")
STAFF_LINE_RE = re.compile(r"^\s*(\d+)[\.\)]\s*(.+?)\s*$")


@dataclass
class StaffRecord:
    staff_id: str
    staff_name: str
    section: str
    canonical_section: str
    linked_product_family: str
    canonical_product: str
    product_kind: str
    arrangement: int | None
    display: int | None
    performance: int | None
    source_name: str
    source_slug: str
    branch_slug: str
    signal_date: str
    raw_title: str


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def slugify(text: str) -> str:
    value = (text or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_")


def extract_report_date(text: str) -> str:
    match = DATE_RE.search(text)
    if not match:
        return datetime.now(timezone.utc).date().isoformat()
    day, month, year = [int(part) for part in match.groups()]
    if year < 100:
        year += 2000
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return datetime.now(timezone.utc).date().isoformat()


def canonical_branch_slug(value: str | None) -> str:
    normalized = normalize_branch(
        value,
        style="canonical_slug",
        fallback="none",
        profile="canonical",
        match_substring=True,
    )
    if not normalized:
        raise ValueError(f"Could not resolve branch from {value!r}")
    return str(normalized)


def canonical_branch_display(branch_slug: str) -> str:
    return str(
        normalize_branch(
            branch_slug,
            style="legacy_upper",
            fallback="upper_raw",
            profile="canonical",
            match_substring=False,
        )
    )


def parse_score(value: str) -> int | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    match = re.search(r"\d+", cleaned)
    if not match:
        return None
    return int(match.group(0))


def split_staff_blocks(lines: list[str]) -> list[dict[str, str]]:
    blocks: list[dict[str, str]] = []
    current: dict[str, str] | None = None

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        staff_match = STAFF_LINE_RE.match(line)
        if staff_match:
            if current:
                blocks.append(current)
            current = {"Staff Name": normalize_spaces(staff_match.group(2)).strip(".")}
            continue

        if ":" not in line:
            continue
        if current is None:
            continue

        key, value = line.split(":", 1)
        current[normalize_spaces(key)] = normalize_spaces(value)

    if current:
        blocks.append(current)

    return blocks


def infer_items_moved(rec: StaffRecord) -> int:
    scores = [x for x in [rec.arrangement, rec.display, rec.performance] if isinstance(x, int)]
    if not scores:
        return 0
    avg = sum(scores) / len(scores)
    if avg >= 5:
        return 12
    if avg >= 4:
        return 9
    if avg >= 3:
        return 6
    if avg >= 2:
        return 3
    return 1


def infer_confidence(rec: StaffRecord) -> float:
    count = sum(x is not None for x in [rec.arrangement, rec.display, rec.performance])
    base = 0.55 + (count * 0.1)
    return max(0.1, min(base, 0.95))


def narrative_summary(rec: StaffRecord) -> str:
    target = rec.canonical_section or rec.section
    return (
        f"{rec.staff_name} observed in section '{target}' with "
        f"arrangement={rec.arrangement}, display={rec.display}, performance={rec.performance}"
    )


def parse_staff_records(
    text: str,
    explicit_source_name: str | None = None,
    explicit_source_slug: str | None = None,
    debug: bool = False,
) -> list[StaffRecord]:
    lines = [line.rstrip() for line in text.splitlines()]
    title = next((line.strip() for line in lines if line.strip()), "STAFF PERFORMANCE REPORT")

    branch_line = next((line.split(":", 1)[1].strip() for line in lines if line.lower().startswith("branch:")), "")
    branch_slug = explicit_source_slug or canonical_branch_slug(branch_line or explicit_source_name)
    source_name = explicit_source_name or canonical_branch_display(branch_slug)
    source_slug = branch_slug
    signal_date = extract_report_date(text)

    blocks = split_staff_blocks(lines)
    records: list[StaffRecord] = []

    for block in blocks:
        raw_name = normalize_spaces(block.get("Staff Name", "")).strip(".")
        if not raw_name:
            raise ValueError("Missing staff name in staff block")

        section_raw = block.get("Section", "")
        canonical_section = normalize_section_name(section_raw)
        if not canonical_section:
            raise ValueError(f"Unresolved section: raw={section_raw!r} reason=not_in_dictionary")

        arrangement = parse_score(block.get("Arrangements") or block.get("Arrangement"))
        display = parse_score(block.get("Display"))
        performance = parse_score(block.get("Performance"))
        if None in {arrangement, display, performance}:
            raise ValueError(f"Missing required staff score field for {raw_name!r}")

        staff_match = resolve_staff(branch_slug, raw_name)
        if staff_match:
            staff_id = staff_match.staff_id
            staff_name = staff_match.display_name.split(" ", 1)[0]
        else:
            staff_id = f"staff-{branch_slug}-{slugify(raw_name)}"
            staff_name = raw_name.title()

        record = StaffRecord(
            staff_id=staff_id,
            staff_name=staff_name,
            section=canonical_section,
            canonical_section=canonical_section,
            linked_product_family=canonical_section,
            canonical_product=canonical_section,
            product_kind="section",
            arrangement=arrangement,
            display=display,
            performance=performance,
            source_name=source_name,
            source_slug=source_slug,
            branch_slug=branch_slug,
            signal_date=signal_date,
            raw_title=title,
        )
        if debug:
            print(
                f"[DEBUG] staff={record.staff_name!r} branch={record.branch_slug!r} section={record.canonical_section!r}",
                file=sys.stderr,
            )
        records.append(record)

    return records


def record_to_markdown(rec: StaffRecord) -> str:
    items_moved = infer_items_moved(rec)
    confidence = infer_confidence(rec)
    lines = [
        f"report_title: {rec.raw_title}",
        f"report_date: {rec.signal_date}",
        "signal_type: productivity_signal",
        f"source_name: {rec.source_name}",
        f"source_slug: {rec.source_slug}",
        f"branch_slug: {rec.branch_slug}",
        f"signal_date: {rec.signal_date}",
        f"staff_id: {rec.staff_id}",
        f"staff_name: {rec.staff_name}",
        f"section: {rec.canonical_section}",
        f"section_canonical: {rec.canonical_section}",
        f"products: {rec.canonical_product}",
        f"linked_product_family: {rec.linked_product_family}",
        f"canonical_product: {rec.canonical_product}",
        f"product_kind: {rec.product_kind}",
        f"raw_section: {rec.section}",
        f"arrangement: {'' if rec.arrangement is None else rec.arrangement}",
        f"display: {'' if rec.display is None else rec.display}",
        f"performance: {'' if rec.performance is None else rec.performance}",
        f"items_moved: {items_moved}",
        f"confidence: {confidence:.2f}",
        f"summary: {narrative_summary(rec)}",
    ]
    return "\n".join(lines) + "\n"


def output_path_for_record(rec: StaffRecord) -> Path:
    return OUTPUT_DIR / f"{rec.branch_slug}_staff_{slugify(rec.staff_name)}_{rec.signal_date}.md"


def write_records(records: Iterable[StaffRecord], output_dir: Path | None = None) -> list[Path]:
    out_dir = output_dir or OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for rec in records:
        path = out_dir / output_path_for_record(rec).name
        path.write_text(record_to_markdown(rec), encoding="utf-8")
        written.append(path)
    return written


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse WhatsApp staff performance reports into flat signal files.")
    parser.add_argument("input_file", help="Path to raw WhatsApp report text file")
    parser.add_argument("--source-name", default=None)
    parser.add_argument("--source-slug", default=None)
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--debug", action="store_true")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        return 1

    text = input_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        print("ERROR: empty input file", file=sys.stderr)
        return 1

    try:
        records = parse_staff_records(
            text,
            explicit_source_name=args.source_name,
            explicit_source_slug=args.source_slug,
            debug=args.debug,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if not records:
        print("No staff records parsed.", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps([asdict(rec) for rec in records], indent=2))
        return 0

    if args.dry_run:
        for rec in records:
            print(record_to_markdown(rec))
            print("---")
        return 0

    output_dir = Path(args.output_dir)
    written = write_records(records, output_dir=output_dir)
    print(f"Parsed {len(records)} staff records")
    print(f"Wrote {len(written)} files to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
