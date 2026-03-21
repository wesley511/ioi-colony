#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
from collections import defaultdict
import sys


MASTER_PATH = Path("STAFF/master_staff_list.md")
REFS_DIR = Path("STAFF/refs")


def clean_value(value: str) -> str:
    value = value.strip()
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        return value[1:-1]
    if value.startswith("'") and value.endswith("'") and len(value) >= 2:
        return value[1:-1]
    return value


def parse_simple_yaml_list_file(path: Path) -> dict:
    """
    Minimal parser for the current master_staff_list.md structure.

    Supports:
    - top-level key: value
    - top-level list sections: shops, staff
    - list items with key/value fields
    """
    lines = path.read_text(encoding="utf-8").splitlines()

    data: dict = {}
    current_list_name = None
    current_item = None

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()

        if not stripped:
            continue

        indent = len(line) - len(line.lstrip(" "))

        if indent == 0 and ":" in stripped and not stripped.endswith(":"):
            key, value = stripped.split(":", 1)
            data[key.strip()] = clean_value(value)
            current_list_name = None
            current_item = None
            continue

        if indent == 0 and stripped.endswith(":"):
            key = stripped[:-1].strip()
            if key in {"shops", "staff"}:
                data[key] = []
                current_list_name = key
                current_item = None
            continue

        if current_list_name and indent >= 2 and stripped.startswith("- "):
            item = {}
            remainder = stripped[2:].strip()
            if remainder and ":" in remainder:
                k, v = remainder.split(":", 1)
                item[k.strip()] = clean_value(v)
            data[current_list_name].append(item)
            current_item = item
            continue

        if current_list_name and current_item is not None and ":" in stripped:
            k, v = stripped.split(":", 1)
            current_item[k.strip()] = clean_value(v)
            continue

    return data


def load_master_staff(path: Path) -> tuple[list[dict], list[dict]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing master staff file: {path}")

    parsed = parse_simple_yaml_list_file(path)
    shops = parsed.get("shops", [])
    staff = parsed.get("staff", [])

    if not shops:
        raise ValueError("No shops found in master staff list.")
    if not staff:
        raise ValueError("No staff found in master staff list.")

    return shops, staff


def group_staff_by_shop(staff_records: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)

    for rec in staff_records:
        shop_code = rec.get("shop_code", "").strip()
        if not shop_code:
            continue
        grouped[shop_code].append(rec)

    return grouped


def normalize_display_name(rec: dict) -> str:
    full_name = rec.get("full_name", "").strip()
    return full_name if full_name else rec.get("normalized_name", "").strip()


def write_shop_ref(shop: dict, records: list[dict]) -> Path:
    shop_code = shop.get("code", "").strip()
    shop_name = shop.get("name", "").strip()
    staff_count = shop.get("staff_count", "").strip()

    output_path = REFS_DIR / f"{shop_code}_staff_ref.md"

    lines: list[str] = []
    lines.append(f"# {shop_code} Staff Reference")
    lines.append("")
    lines.append(f"**Shop:** {shop_name}")
    lines.append(f"**Shop Code:** {shop_code}")
    lines.append(f"**Registered Staff Count:** {staff_count}")
    lines.append("")

    lines.append("## Purpose")
    lines.append("")
    lines.append(
        "This file is the supervisor reference list for colony staff IDs. "
        "Use it to verify the correct staff identity when writing reports or linking signals."
    )
    lines.append("")

    lines.append("## Staff ID Map")
    lines.append("")

    for rec in sorted(records, key=lambda r: r.get("staff_id", "")):
        staff_id = rec.get("staff_id", "").strip()
        full_name = normalize_display_name(rec)
        role_type = rec.get("role_type", "").strip()
        status = rec.get("status", "").strip()
        notes = rec.get("notes", "").strip()

        lines.append(f"- **{full_name}** → `{staff_id}`")
        lines.append(f"  - role_type: `{role_type}`")
        lines.append(f"  - status: `{status}`")
        if notes:
            lines.append(f"  - notes: {notes}")

    lines.append("")
    lines.append("## Supervisor Guidance")
    lines.append("")
    lines.append("- Use the staff name exactly as shown in branch reports where possible.")
    lines.append("- Use the mapped `staff_id` when creating or updating normalized staff signals.")
    lines.append("- Do not invent or edit IDs manually.")
    lines.append("- New staff should be added to the master registry first, then this reference will be regenerated.")
    lines.append("")

    output_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output_path


def write_master_ref_index(shops: list[dict], grouped_staff: dict[str, list[dict]]) -> Path:
    output_path = REFS_DIR / "README_staff_refs.md"

    lines: list[str] = []
    lines.append("# Staff Reference Index")
    lines.append("")
    lines.append("This directory contains per-shop supervisor reference files for canonical colony staff IDs.")
    lines.append("")

    for shop in shops:
        code = shop.get("code", "").strip()
        name = shop.get("name", "").strip()
        count = len(grouped_staff.get(code, []))
        lines.append(f"- `{code}` — {name} — ref file: `{code}_staff_ref.md` — loaded staff: {count}")

    lines.append("")
    lines.append("## Operating Rule")
    lines.append("")
    lines.append("Supervisors should reference staff from these files, but the system remains the owner of all staff IDs.")
    lines.append("")

    output_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output_path


def main() -> int:
    try:
        shops, staff = load_master_staff(MASTER_PATH)
        grouped_staff = group_staff_by_shop(staff)

        REFS_DIR.mkdir(parents=True, exist_ok=True)

        written_files = []
        for shop in shops:
            code = shop.get("code", "").strip()
            records = grouped_staff.get(code, [])
            if not records:
                continue
            written_files.append(write_shop_ref(shop, records))

        index_file = write_master_ref_index(shops, grouped_staff)

        print(f"Generated {len(written_files)} shop reference file(s)")
        for p in written_files:
            print(f"- {p}")
        print(f"- {index_file}")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
