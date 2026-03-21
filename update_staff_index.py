#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
from collections import defaultdict
import sys


MASTER_PATH = Path("STAFF/master_staff_list.md")
SIGNALS_DIR = Path("SIGNALS/normalized")
OUTPUT_PATH = Path("staff_index.md")


def clean_value(value: str) -> str:
    value = value.strip()
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        return value[1:-1]
    if value.startswith("'") and value.endswith("'") and len(value) >= 2:
        return value[1:-1]
    return value


def parse_simple_yaml_list_file(path: Path) -> dict:
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


def parse_signal_file(path: Path) -> dict:
    data: dict = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = clean_value(value)
    return data


def load_master_staff(path: Path) -> tuple[list[dict], dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing master staff file: {path}")

    parsed = parse_simple_yaml_list_file(path)
    staff = parsed.get("staff", [])
    shops = parsed.get("shops", [])

    if not staff:
        raise ValueError("No staff records found in master staff list.")

    staff_by_id = {}
    for rec in staff:
        staff_id = rec.get("staff_id", "").strip()
        if not staff_id:
            continue
        if staff_id in staff_by_id:
            raise ValueError(f"Duplicate staff_id in master file: {staff_id}")
        staff_by_id[staff_id] = rec

    return shops, staff_by_id


def load_signals(signals_dir: Path) -> dict[str, list[dict]]:
    signal_map: dict[str, list[dict]] = defaultdict(list)

    if not signals_dir.exists():
        return signal_map

    for path in sorted(signals_dir.glob("*.md")):
        sig = parse_signal_file(path)

        staff_ids: list[str] = []

        if sig.get("staff_id"):
            staff_ids.append(sig["staff_id"].strip())

        if sig.get("staff_ids"):
            parts = [p.strip() for p in sig["staff_ids"].split(",")]
            staff_ids.extend([p for p in parts if p])

        for sid in staff_ids:
            signal_map[sid].append(sig)

    return signal_map


def extract_score_list(signals: list[dict], field_names: list[str]) -> list[int]:
    scores: list[int] = []
    for sig in signals:
        for field in field_names:
            val = sig.get(field, "").strip()
            if val.isdigit():
                scores.append(int(val))
    return scores


def extract_metric_list(signals: list[dict], field_name: str) -> list[int]:
    values: list[int] = []
    for sig in signals:
        raw = sig.get(field_name, "").strip()
        if not raw:
            continue

        # Supports:
        # items_moved: 186
        # items_moved: 186, 153, 141
        parts = [p.strip() for p in raw.split(",")]
        for part in parts:
            if part.isdigit():
                values.append(int(part))
    return values


def avg_or_na(values: list[int]) -> str:
    if not values:
        return "n/a"
    return f"{sum(values) / len(values):.2f}"


def latest_or_na(values: list[int]) -> str:
    if not values:
        return "n/a"
    return str(values[-1])


def determine_status(
    master_status: str,
    signals: list[dict],
    avg_perf_num: float | None,
    avg_items_num: float | None,
    avg_assist_num: float | None,
) -> str:
    signal_types = {s.get("signal_type", "") for s in signals}

    # Registry-first rule
    if master_status == "new_staff":
        return "new_staff_watch"
    if master_status == "terminated":
        return "critical_attention"

    has_quality_gap = "performance_gap" in signal_types
    has_training_gap = "training_gap" in signal_types
    has_strong_quality = "strong_performance" in signal_types
    has_productivity = "productivity_signal" in signal_types
    has_engagement = "customer_engagement" in signal_types
    has_role_execution = "role_execution" in signal_types

    # 1. Highest-confidence top performer:
    # strong quality + high movement, or strong quality + very strong backend/frontline execution
    if has_strong_quality:
        if avg_perf_num is not None and avg_perf_num >= 4.8:
            if avg_items_num is not None and avg_items_num >= 120:
                return "top_performer"
            if has_role_execution and avg_perf_num >= 5.0:
                return "top_performer"
            return "strong"

    # 2. Weak quality but still moving product = high-output needs quality
    if has_quality_gap:
        if avg_perf_num is not None and avg_perf_num <= 4.0:
            if avg_items_num is not None and avg_items_num >= 60:
                return "high_output_needs_quality"
            return "critical_attention"
        return "weak_zone"

    # 3. Training gap without hard failure
    if has_training_gap:
        return "average"

    # 4. Pure engagement strength
    if has_engagement and avg_assist_num is not None and avg_assist_num >= 18:
        if avg_items_num is not None and avg_items_num >= 60:
            return "strong"
        return "engagement_strength"

    # 5. Pure productivity signal
    if has_productivity:
        if avg_items_num is not None and avg_items_num >= 120:
            return "strong"
        if avg_items_num is not None and avg_items_num >= 60:
            return "average"
        return "average"

    # 6. Role execution without direct sales metrics
    if has_role_execution:
        return "strong"

    # 7. Fallback numeric-only logic
    if avg_perf_num is None:
        return "average"

    if avg_perf_num >= 4.8:
        return "top_performer"
    if avg_perf_num >= 4.4:
        return "strong"
    if avg_perf_num >= 4.0:
        return "average"
    return "weak_zone"

def determine_trend(master_status: str, status: str) -> str:
    if master_status == "new_staff":
        return "new_staff"
    if status == "top_performer":
        return "stable_high"
    if status == "strong":
        return "stable_high"
    if status == "average":
        return "stable_mid"
    if status in {"weak_zone", "critical_attention"}:
        return "needs_support"
    return "stable_mid"


def determine_recommended_action(status: str) -> str:
    mapping = {
        "top_performer": "replicate_standard",
        "strong": "maintain",
        "engagement_strength": "coach_conversion",
        "high_output_needs_quality": "coach_quality",
        "average": "coach_targeted",
        "weak_zone": "coach_display",
        "critical_attention": "supervise_closely",
        "new_staff_watch": "onboarding_support",
    }
    return mapping.get(status, "maintain")


def determine_opportunity_score(status: str, signals: list[dict]) -> int:
    explicit_scores = []
    for sig in signals:
        raw = sig.get("opportunity_score", "").strip()
        if raw.isdigit():
            explicit_scores.append(int(raw))

    if explicit_scores:
        return max(explicit_scores)

    defaults = {
        "top_performer": 10,
        "strong": 8,
        "engagement_strength": 8,
        "high_output_needs_quality": 7,
        "average": 6,
        "weak_zone": 4,
        "critical_attention": 3,
        "new_staff_watch": 5,
    }
    return defaults.get(status, 6)

def determine_staff_type(avg_items_num, avg_assist_num, avg_perf_num):
    if avg_items_num is None and avg_assist_num is None and avg_perf_num is None:
        return "unknown"

    # Highest class: excellent quality + strong output
    if avg_items_num is not None and avg_items_num >= 120:
        if avg_perf_num is not None and avg_perf_num >= 4.5:
            return "elite"
        return "mover"

    # High customer engagement
    if avg_assist_num is not None and avg_assist_num >= 18:
        return "engager"

    # Strong quality but not high volume
    if avg_perf_num is not None and avg_perf_num >= 4.5:
        return "quality"

    return "balanced"

def collect_sections(signals: list[dict]) -> str:
    vals = []
    for sig in signals:
        v = sig.get("section", "").strip()
        if v and v not in vals:
            vals.append(v)
    return "; ".join(vals) if vals else "n/a"


def collect_products(signals: list[dict]) -> str:
    vals = []
    for sig in signals:
        v = sig.get("products", "").strip()
        if v and v not in vals:
            vals.append(v)
    return "; ".join(vals) if vals else "n/a"


def pick_latest_date_day(master_rec: dict, signals: list[dict]) -> tuple[str, str]:
    latest_date = master_rec.get("last_seen_date", "n/a")
    latest_day = "n/a"

    for sig in signals:
        if sig.get("date"):
            latest_date = sig["date"]
        if sig.get("day"):
            latest_day = sig["day"]

    return latest_date, latest_day

def build_staff_record(master_rec: dict, signals: list[dict]) -> dict:
    arrangement_scores = extract_score_list(
        signals, ["arrangement_score", "latest_arrangement", "arrangement"]
    )
    display_scores = extract_score_list(
        signals, ["display_score", "latest_display", "display"]
    )
    performance_scores = extract_score_list(
        signals, ["performance_score", "latest_performance", "performance"]
    )

    items_moved_values = extract_metric_list(signals, "items_moved")
    assisting_count_values = extract_metric_list(signals, "assisting_count")

    avg_items_num = None
    if items_moved_values:
        avg_items_num = sum(items_moved_values) / len(items_moved_values)

    avg_assist_num = None
    if assisting_count_values:
        avg_assist_num = sum(assisting_count_values) / len(assisting_count_values)

    avg_perf_num = None
    if performance_scores:
        avg_perf_num = sum(performance_scores) / len(performance_scores)

    status = determine_status(    master_rec.get("status", "active"),    signals,    avg_perf_num,    avg_items_num,    avg_assist_num,)
    trend = determine_trend(master_rec.get("status", "active"), status)
    action = determine_recommended_action(status)
    opportunity_score = determine_opportunity_score(status, signals)
    latest_date, latest_day = pick_latest_date_day(master_rec, signals)
    staff_type = determine_staff_type(avg_items_num, avg_assist_num, avg_perf_num)

    return {
        "staff_id": master_rec.get("staff_id", ""),
        "staff_name": master_rec.get("full_name", ""),
        "shop_name": master_rec.get("shop_name", ""),
        "latest_report_date": latest_date,
        "latest_report_day": latest_day,
        "role_type": master_rec.get("role_type", "other"),
        "sections": collect_sections(signals),
        "products": collect_products(signals),
        "reports_count": str(len(signals)),
        "avg_arrangement": avg_or_na(arrangement_scores),
        "avg_display": avg_or_na(display_scores),
        "avg_performance": avg_or_na(performance_scores),
        "latest_arrangement": latest_or_na(arrangement_scores),
        "latest_display": latest_or_na(display_scores),
        "latest_performance": latest_or_na(performance_scores),
        "avg_items_moved": avg_or_na(items_moved_values),
        "avg_assisting_count": avg_or_na(assisting_count_values),
        "latest_items_moved": latest_or_na(items_moved_values),
        "latest_assisting_count": latest_or_na(assisting_count_values),
        "trend": trend,
        "staff_type": staff_type,
        "status": status,
        "opportunity_score": str(opportunity_score),
        "recommended_action": action,
        "notes": master_rec.get("notes", ""),
    }


def summarize_records(records: list[dict]) -> dict[str, int]:
    summary = {
        "total_staff_tracked": len(records),
        "top_performers": 0,
        "strong_staff": 0,
        "average_staff": 0,
        "weak_zones": 0,
        "critical_attention": 0,
        "new_staff_watch": 0,
    }

    for rec in records:
        status = rec["status"]
        if status == "top_performer":
            summary["top_performers"] += 1
        elif status == "strong":
            summary["strong_staff"] += 1
        elif status == "average":
            summary["average_staff"] += 1
        elif status == "weak_zone":
            summary["weak_zones"] += 1
        elif status == "critical_attention":
            summary["critical_attention"] += 1
        elif status == "new_staff_watch":
            summary["new_staff_watch"] += 1

    return summary


def format_record_block(rec: dict) -> str:
    keys = [
        "staff_id",
        "staff_name",
        "shop_name",
        "latest_report_date",
        "latest_report_day",
        "role_type",
        "sections",
        "products",
        "reports_count",
        "avg_arrangement",
        "avg_display",
        "avg_performance",
        "latest_arrangement",
        "latest_display",
        "latest_performance",
        "avg_items_moved",
        "avg_assisting_count",
        "latest_items_moved",
        "latest_assisting_count",
        "trend",
        "staff_type",
        "status",
        "opportunity_score",
        "recommended_action",
        "notes",
    ]

    lines = ["```yaml"]
    for key in keys:
        lines.append(f"{key}: {rec.get(key, '')}")
    lines.append("```")
    return "\n".join(lines)


def write_staff_index(output_path: Path, shops: list[dict], records: list[dict]) -> None:
    summary = summarize_records(records)

    records_by_shop: dict[str, list[dict]] = defaultdict(list)
    for rec in records:
        records_by_shop[rec["shop_name"]].append(rec)

    lines: list[str] = []
    lines.append("# Staff_Index.md")
    lines.append("")
    lines.append("## Purpose")
    lines.append("")
    lines.append(
        "This file is the colony's staff intelligence layer, generated from the canonical "
        "staff registry and normalized staff-linked signals."
    )
    lines.append("")
    lines.append("```yaml")
    lines.append("total_shops: 4")
    for key, value in summary.items():
        lines.append(f"{key}: {value}")
    lines.append("```")
    lines.append("")

    if shops:
        lines.append("## Shops Covered")
        lines.append("")
        for shop in shops:
            code = shop.get("code", "")
            name = shop.get("name", "")
            count = shop.get("staff_count", "")
            lines.append(f"- {code}: {name} ({count})")
        lines.append("")

    for shop_name in sorted(records_by_shop.keys()):
        lines.append(f"## {shop_name}")
        lines.append("")
        shop_records = sorted(records_by_shop[shop_name], key=lambda r: r["staff_id"])
        for rec in shop_records:
            lines.append(format_record_block(rec))
            lines.append("")

    output_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    try:
        shops, staff_by_id = load_master_staff(MASTER_PATH)
        signal_map = load_signals(SIGNALS_DIR)

        records = []
        for staff_id in sorted(staff_by_id.keys()):
            master_rec = staff_by_id[staff_id]
            signals = signal_map.get(staff_id, [])
            records.append(build_staff_record(master_rec, signals))

        write_staff_index(OUTPUT_PATH, shops, records)

        print(f"Updated {OUTPUT_PATH}")
        print(f"Loaded staff: {len(staff_by_id)}")
        print(f"Signals attached to staff: {sum(len(v) for v in signal_map.values())}")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
