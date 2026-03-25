from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from scripts.section_master_data import resolve_section_from_master_data
from scripts.product_resolver import resolve_product
from scripts.utils_normalization import normalize_branch


import re


BRANCH_ID_ALIASES = {
    "waigani": "waigani",
    "ttc waigani": "waigani",
    "ttc pom waigani branch": "waigani",
    "ttc pom waiganit branch": "waigani",

    "bena": "bena_road",
    "bena road": "bena_road",
    "bena-road": "bena_road",
    "bena_road": "bena_road",
    "goroka bena road": "bena_road",
    "goroka_bena_road": "bena_road",

    "5th street": "5th_street",
    "5th_street": "5th_street",
    "lae 5th street": "5th_street",
    "lae_5th_street": "5th_street",
    "lae 5th street shop": "5th_street",

    "lae malaita": "lae_malaita",
    "lae_malaita": "lae_malaita",
    "malaita street lae": "lae_malaita",
    "lae malaita street shop": "lae_malaita",
}


def normalize_token(text: str) -> str:
    text = str(text or "").strip().lower()
    text = text.replace("&", " and ")
    text = text.replace("/", " ")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s_]", " ", text)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def normalize_staff_branch(branch: str) -> str:
    return normalize_branch(branch).lower()
    raw = str(branch or "").strip()
    if not raw:
        return "unknown_branch"

    raw_lower = raw.lower().strip()
    if raw_lower in BRANCH_ID_ALIASES:
        return BRANCH_ID_ALIASES[raw_lower]

    raw_token = normalize_token(raw)
    return BRANCH_ID_ALIASES.get(raw_token, raw_token or "unknown_branch")


def normalize_staff_name(staff_name: str) -> str:
    name = normalize_token(staff_name)
    return name if name else "unknown_staff"


def normalize_staff_id(branch: str, staff_name: str) -> str:
    branch_key = normalize_staff_branch(branch)
    staff_key = normalize_staff_name(staff_name)
    return f"staff-{branch_key}-{staff_key}"

"""
Parse WhatsApp staff performance reports into flat signal files.

Expected input style examples:

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

Key design rules:
- Staff reports are section-first, not product-first.
- We normalize sections first.
- We optionally derive a linked product family after section resolution.
- We keep raw section text for traceability.
"""


BRANCH_ALIASES = {
    "waigani": {
        "canonical_name": "TTC Waigani",
        "slug": "waigani",
        "patterns": [
            r"\bttc\s+waigani\b",
            r"\bwaigani\s+branch\b",
            r"\bwaigani\b",
        ],
    },
    "mataita_street": {
        "canonical_name": "TTC Mataita Street",
        "slug": "mataita_street",
        "patterns": [
            r"\bttc\s+mataita\s+street\b",
            r"\bmataita\s+street\b",
            r"\bmataita\b",
        ],
    },
    "fifth_street": {
        "canonical_name": "TTC 5th Street",
        "slug": "fifth_street",
        "patterns": [
            r"\bttc\s+5th\s+street\b",
            r"\b5th\s+street\b",
            r"\bfifth\s+street\b",
        ],
    },
    "bena_road": {
        "canonical_name": "TTC Bena Road",
        "slug": "bena_road",
        "patterns": [
            r"\bttc\s+bena\s+road\b",
            r"\bbena\s+road\b",
        ],
    },
}


SECTION_ALIASES = {
    "boys": {
        "canonical": "boys",
        "patterns": [
            r"\bboys\b",
            r"\bboy\b",
            r"\bboys\s+section\b",
        ],
    },
    "girls": {
        "canonical": "girls",
        "patterns": [
            r"\bgirls\b",
            r"\bgirl\b",
            r"\bgirls\s+section\b",
        ],
    },
    "mens_shoes": {
        "canonical": "mens_shoes",
        "patterns": [
            r"\bmans?\s+shoe\b",
            r"\bmens?\s+shoe\b",
            r"\bmans?\s+shoes\b",
            r"\bmens?\s+shoes\b",
        ],
    },
    "pacific_shirts": {
        "canonical": "pacific_shirts",
        "patterns": [
            r"\bpacific\s+shirt\b",
            r"\bpacific\s+shirts\b",
        ],
    },
    "mens_tshirt": {
        "canonical": "mens_tshirt",
        "patterns": [
            r"\bmens?\s+t\s*shirt\b",
            r"\bmens?\s+tshirt\b",
            r"\bmans?\s+t\s*shirt\b",
            r"\bmans?\s+tshirt\b",
            r"\bmens?\s+tee\b",
            r"\bmans?\s+tee\b",
        ],
    },
    "mens_cotton_pants": {
        "canonical": "mens_cotton_pants",
        "patterns": [
            r"\bmans?\s+cotton\s+pant\b",
            r"\bmens?\s+cotton\s+pant\b",
            r"\bmans?\s+cotton\s+pants\b",
            r"\bmens?\s+cotton\s+pants\b",
        ],
    },
    "mens_office_pants": {
        "canonical": "mens_office_pants",
        "patterns": [
            r"\bmans?\s+office\s+pant\b",
            r"\bmens?\s+office\s+pant\b",
            r"\bmans?\s+office\s+pants\b",
            r"\bmens?\s+office\s+pants\b",
        ],
    },
    "mixed_army_pants": {
        "canonical": "mixed_army_pants",
        "patterns": [
            r"\bmix\s+army\s+pants\b",
            r"\bmixed\s+army\s+pants\b",
            r"\barmy\s+pants\b",
        ],
    },
    "m6pkts": {
        "canonical": "m6pkts",
        "patterns": [
            r"\bm6pkts\b",
            r"\bm\s*6\s*pkts\b",
            r"\b6\s*pockets\b",
        ],
    },
}


SECTION_TO_PRODUCT_FAMILY = {
    "boys": "kids_boys_assorted",
    "girls": "kids_girls_assorted",
    "mens_shoes": "shoes",
    "pacific_shirts": "mens_shirts",
    "mens_tshirt": "mens_tshirt",
    "mens_cotton_pants": "mens_cotton_pants",
    "mens_office_pants": "mens_office_pants",
    "mixed_army_pants": "mixed_army_pants",
    "m6pkts": "m6pkts",
}


OUTPUT_DIR = Path("SIGNALS/normalized")


@dataclass
class StaffRecord:
    staff_id: str
    staff_name: str
    section: str
    canonical_section: str = ""
    linked_product_family: str = ""
    canonical_product: str = ""
    product_kind: str = "unknown"
    arrangement: int | None = None
    display: int | None = None
    performance: int | None = None
    source_name: str = ""
    source_slug: str = ""
    signal_date: str = ""
    raw_title: str = ""


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def slugify(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def canonical_branch_from_text(text: str) -> tuple[str, str]:
    lowered = (text or "").lower()
    haystack = str(text or "").lower()  # ✅ ADD THIS LINE

    patterns = {
        "WAIGANI": [r"\bwaigani\b"],
        "BENA_ROAD": [r"\bbena\b", r"\bbena road\b"],
        "LAE_MALAITA": [r"\bmalaita\b", r"\blae\b"],
        "5TH_STREET": [r"\b5th\b", r"\b5th street\b"],
    }

    for canonical_name, regex_list in patterns.items():
        for pattern in regex_list:
            if re.search(pattern, haystack, flags=re.IGNORECASE):
                return canonical_name, pattern

    return "", None

    for meta in BRANCH_ALIASES.values():
        for pattern in meta["patterns"]:
            if re.search(pattern, haystack, flags=re.IGNORECASE):
                return meta["canonical_name"], meta["slug"]

    first_line = (text or "").splitlines()[0].strip() if text else "unknown_branch"
    fallback_name = normalize_spaces(first_line) or "Unknown Branch"
    fallback_slug = slugify(fallback_name)
    return fallback_name, (fallback_slug or "unknown_branch")


def infer_source_name(title: str) -> str:
    canonical_name, _ = canonical_branch_from_text(title)
    return canonical_name


def infer_source_slug(title: str) -> str:
    _, canonical_slug = canonical_branch_from_text(title)
    return canonical_slug


def extract_report_date(text: str) -> str:
    """
    Extract dates like:
    - Friday 20/03/26
    - Saturday 21/03/26
    Returns ISO yyyy-mm-dd if possible, otherwise today's UTC date.
    """
    m = re.search(
        r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+(\d{1,2})/(\d{1,2})/(\d{2,4})\b",
        text,
        flags=re.IGNORECASE,
    )
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

    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", text)
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

    return datetime.utcnow().date().isoformat()


def split_staff_blocks(text: str) -> list[str]:
    """
    Split on numbered staff entries like:
    1..MILFORD..
    2.GRACE
    3..KIMSON

    Uses a compiled multiline pattern to avoid inline-flag deprecation warnings.
    """
    text = text.replace("\r\n", "\n")
    pattern = re.compile(
        r"^\s*\d+\s*[\.\)]*\s*[A-Z][A-Z\s'\-\.]{1,60}\s*[\.\)]*\s*$",
        re.MULTILINE,
    )
    starts = list(pattern.finditer(text))
    if not starts:
        return []

    blocks: list[str] = []
    for i, match in enumerate(starts):
        start = match.start()
        end = starts[i + 1].start() if i + 1 < len(starts) else len(text)
        block = text[start:end].strip()
        if block:
            blocks.append(block)
    return blocks


def extract_staff_name(block: str) -> str:
    first_line = block.splitlines()[0].strip()
    first_line = re.sub(r"^\s*\d+\s*[\.\)]*\s*", "", first_line)
    first_line = first_line.strip(" .")
    first_line = normalize_spaces(first_line)
    return first_line.title()


def extract_section(block: str) -> str:
    m = re.search(r"SECTION\s*[\.\-:]*\s*(.+)", block, flags=re.IGNORECASE)
    if not m:
        return "unknown_section"
    section = m.group(1).splitlines()[0]
    return normalize_spaces(section).strip(" .")


def split_section_candidates(section_text: str) -> list[str]:
    """
    Split multi-section text like:
    'MANS SHOE, PACIFIC SHIRTS'
    'BOYS / GIRLS'
    'MENS & PACIFIC SHIRTS'
    """
    raw_parts = re.split(r",|/|&", section_text)
    parts = [normalize_spaces(p).strip(" .") for p in raw_parts if normalize_spaces(p).strip(" .")]
    return parts or [section_text]


def resolve_section_alias(section_text: str) -> tuple[str, str]:
    normalized = normalize_spaces(section_text).lower().strip(" .")
    for meta in SECTION_ALIASES.values():
        for pattern in meta["patterns"]:
            if re.search(pattern, normalized, flags=re.IGNORECASE):
                return "section", meta["canonical"]
    return "unknown", slugify(normalized) or "unknown_section"


def resolve_section_candidates(section_text: str) -> tuple[str, str, str, str]:
    """
    Resolve one or more section candidates.

    Returns:
    - product_kind
    - canonical_section
    - linked_product_family
    - canonical_product

    Resolution strategy:
    1. section alias layer
    2. fallback to product resolver
    3. unresolved -> unknown
    """
    candidates = split_section_candidates(section_text)

    first_unknown_key = ""
    for candidate in candidates:
        candidate_clean = normalize_spaces(candidate).strip(" .")

        kind, canonical_section = resolve_section_alias(candidate_clean)
        if kind == "section":
            linked_product_family = SECTION_TO_PRODUCT_FAMILY.get(canonical_section, "")
            canonical_product = linked_product_family or canonical_section
            return "section", canonical_section, linked_product_family, canonical_product

        fallback_kind, fallback_canonical = resolve_product(candidate_clean, source="whatsapp_staff_report")
        if fallback_kind != "unknown":
            return fallback_kind, slugify(candidate_clean), fallback_canonical, fallback_canonical

        if not first_unknown_key:
            first_unknown_key = slugify(candidate_clean)

    unknown_key = first_unknown_key or slugify(section_text) or "unknown_section"
    return "unknown", unknown_key, "", unknown_key


def extract_score(block: str, label: str) -> int | None:
    patterns = [
        rf"{label}\s*\(?\s*(\d+)\s*\)?",
        rf"{label}\s*[\.\-:]*\s*(\d+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, block, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
    return None


def parse_staff_records(
    text: str,
    explicit_source_name: str | None = None,
    explicit_source_slug: str | None = None,
    debug: bool = False,
) -> list[StaffRecord]:
    lines = [line.rstrip() for line in text.splitlines()]
    title = next((line.strip() for line in lines if line.strip()), "Unknown Branch")
    signal_date = extract_report_date(text)

    source_name = explicit_source_name or infer_source_name(title)
    source_slug = explicit_source_slug or infer_source_slug(title)

    branch = normalize_branch(source_name)
    source_name = branch
    source_slug = normalize_staff_branch(branch)

    blocks = split_staff_blocks(text)
    records: list[StaffRecord] = []

    for block in blocks:
        staff_name = extract_staff_name(block)
        staff_name = normalize_staff_name(staff_name)
        section = extract_section(block)

        arrangement = extract_score(block, "Arrangements?")
        display = extract_score(block, "Display")
        performance = extract_score(block, "Performance")

        product_kind, canonical_section, linked_product_family, canonical_product = resolve_section_candidates(section)

        if debug:
            print(
                f"[DEBUG] {staff_name} -> section={section!r} -> canonical_section={canonical_section!r} "
                f"-> linked_product_family={linked_product_family!r} -> kind={product_kind!r}",
                file=sys.stderr,
            )

        record = StaffRecord(
	    staff_id=normalize_staff_id(branch, staff_name),
    	    staff_name=staff_name,
            section=section,
            canonical_section=canonical_section,
            linked_product_family=linked_product_family,
            canonical_product=canonical_product,
            product_kind=product_kind,
            arrangement=arrangement,
            display=display,
            performance=performance,
            source_name=source_name,
            source_slug=source_slug,
            signal_date=signal_date,
            raw_title=title,
        )
        records.append(record)

    return records


def infer_signal_type(_: StaffRecord) -> str:
    return "productivity_signal"


def infer_items_moved(rec: StaffRecord) -> int:
    """
    Lightweight deterministic heuristic derived from ratings.
    """
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
    score_count = sum(x is not None for x in [rec.arrangement, rec.display, rec.performance])

    base = 0.50
    if score_count == 3:
        base += 0.30
    elif score_count == 2:
        base += 0.20
    elif score_count == 1:
        base += 0.10

    if rec.product_kind == "section":
        base += 0.10
    elif rec.product_kind != "unknown":
        base += 0.05

    return max(0.10, min(base, 0.95))


def narrative_summary(rec: StaffRecord) -> str:
    rating_bits = []
    if rec.arrangement is not None:
        rating_bits.append(f"arrangement={rec.arrangement}")
    if rec.display is not None:
        rating_bits.append(f"display={rec.display}")
    if rec.performance is not None:
        rating_bits.append(f"performance={rec.performance}")
    rating_text = ", ".join(rating_bits) if rating_bits else "no ratings"

    target = rec.canonical_section or rec.section.lower()
    return f"{rec.staff_name} observed in section '{target}' with {rating_text}"


def record_to_markdown(rec: StaffRecord) -> str:
    signal_type = infer_signal_type(rec)
    items_moved = infer_items_moved(rec)
    confidence = infer_confidence(rec)

    section_value = rec.canonical_section or rec.section.lower()
    products_value = rec.linked_product_family or rec.canonical_product or rec.section.lower()

    lines = [
        f"report_title: {rec.raw_title}",
        f"report_date: {rec.signal_date}",
        f"signal_type: {signal_type}",
        f"source_name: {rec.source_name}",
        f"source_slug: {rec.source_slug}",
        f"signal_date: {rec.signal_date}",
	f"staff_id: {rec.staff_id}",
        f"staff_name: {rec.staff_name}",
        f"section: {section_value}",
        f"section_canonical: {rec.canonical_section}",
        f"products: {products_value}",
        f"linked_product_family: {rec.linked_product_family}",
        f"canonical_product: {rec.canonical_product}",
        f"product_kind: {rec.product_kind}",
        f"raw_section: {rec.section.lower()}",
        f"arrangement: {'' if rec.arrangement is None else rec.arrangement}",
        f"display: {'' if rec.display is None else rec.display}",
        f"performance: {'' if rec.performance is None else rec.performance}",
        f"items_moved: {items_moved}",
        f"confidence: {confidence:.2f}",
        f"summary: {narrative_summary(rec)}",
    ]
    return "\n".join(lines) + "\n"


def output_path_for_record(rec: StaffRecord) -> Path:
    date_part = rec.signal_date
    branch_part = rec.source_slug or "unknown_branch"
    safe_staff_id = rec.staff_id.replace("staff-", "")
    staff_part = slugify(rec.staff_name) or "unknown_staff"
    return OUTPUT_DIR / f"{branch_part}_staff_{staff_part}_{date_part}.md"


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
    parser.add_argument(
        "--source-name",
        help="Optional explicit canonical source name override, e.g. 'TTC Waigani'",
        default=None,
    )
    parser.add_argument(
        "--source-slug",
        help="Optional explicit canonical branch slug override, e.g. 'waigani'",
        default=None,
    )
    parser.add_argument(
        "--output-dir",
        help="Optional output directory override",
        default=str(OUTPUT_DIR),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and print records without writing files",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit parsed records as JSON and exit",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Emit resolver diagnostics to stderr",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        return 1

    text = input_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        print("ERROR: empty input file", file=sys.stderr)
        return 1

    records = parse_staff_records(
        text,
        explicit_source_name=args.source_name,
        explicit_source_slug=args.source_slug,
        debug=args.debug,
    )

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
    for rec in records:
        print(
            f"PARSED staff={rec.staff_name!r} section={rec.section!r} "
            f"canonical_section={rec.canonical_section!r} "
            f"linked_product_family={rec.linked_product_family!r} "
            f"canonical_product={rec.canonical_product!r} kind={rec.product_kind!r}"
        )

    print(f"Wrote {len(written)} files to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
