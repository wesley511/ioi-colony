#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any

try:
    from scripts.branch_resolution import canonical_branch_slug, resolve_branch_slug
except ModuleNotFoundError:
    from branch_resolution import canonical_branch_slug, resolve_branch_slug


DEFAULT_NORMALIZED_DIR = Path("normalized")
DEFAULT_OUTPUT_DIR = Path("REPORTS/staff_performance")


@dataclass
class StaffEvent:
    source_file: Path
    branch: str
    branch_slug: str
    report_date: str
    staff_id: str
    staff_name: str
    staff_name_raw: str | None
    section_slug: str
    section_name_raw: str | None
    arrangement: float | None
    display: float | None
    performance: float | None
    average_score: float | None
    rating_band: str | None
    weak_dimensions: list[str]
    strong_dimensions: list[str]
    confidence: float | None
    raw: dict[str, Any]


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def is_staff_performance_file(path: Path) -> bool:
    return path.is_file() and path.suffix == ".json" and "staff_performance" in path.name


def extract_staff_event(path: Path) -> StaffEvent | None:
    data = load_json(path)

    event_kind = data.get("event_kind")
    if event_kind != "staff_performance":
        return None

    payload = data.get("payload", {}) or {}
    scores = payload.get("scores", {}) or {}
    derived = payload.get("derived", {}) or {}

    # Backward compatibility with older flat schema
    arrangement = safe_float(scores.get("arrangement"))
    display = safe_float(scores.get("display"))
    performance = safe_float(scores.get("performance"))

    if arrangement is None and "arrangement" in payload:
        arrangement = safe_float(payload.get("arrangement"))
    if display is None and "display" in payload:
        display = safe_float(payload.get("display"))
    if performance is None and "performance" in payload:
        performance = safe_float(payload.get("performance"))

    average_score = safe_float(derived.get("average_score"))
    if average_score is None:
        values = [v for v in [arrangement, display, performance] if v is not None]
        average_score = round(mean(values), 2) if values else None

    rating_band = derived.get("rating_band")
    if not rating_band and average_score is not None:
        if average_score >= 4.5:
            rating_band = "strong"
        elif average_score >= 3.5:
            rating_band = "acceptable"
        else:
            rating_band = "weak"

    weak_dimensions = list(derived.get("weak_dimensions", []) or [])
    strong_dimensions = list(derived.get("strong_dimensions", []) or [])

    if not weak_dimensions or not strong_dimensions:
        for key, value in {
            "arrangement": arrangement,
            "display": display,
            "performance": performance,
        }.items():
            if value is None:
                continue
            if value >= 4 and key not in strong_dimensions:
                strong_dimensions.append(key)
            elif value <= 2 and key not in weak_dimensions:
                weak_dimensions.append(key)

    branch = (
        data.get("branch")
        or payload.get("source_slug")
        or payload.get("branch")
        or "unknown"
    )
    branch_slug = resolve_branch_slug(
        payload,
        path=path,
        candidates=[data.get("branch"), payload.get("source_slug"), payload.get("branch")],
    )
    report_date = (
        data.get("report_date")
        or payload.get("signal_date")
        or payload.get("report_date")
        or "unknown-date"
    )

    staff_id = payload.get("staff_id")
    if not staff_id:
        return None

    staff_name = (
        payload.get("staff_name")
        or payload.get("staff_name_raw")
        or staff_id.lower()
    )

    section_slug = (
        payload.get("section_slug")
        or payload.get("canonical_section")
        or "unknown_section"
    )

    section_name_raw = payload.get("section_name_raw") or payload.get("section")
    confidence = safe_float(payload.get("confidence"))

    return StaffEvent(
        source_file=path,
        branch=branch,
        branch_slug=branch_slug,
        report_date=report_date,
        staff_id=staff_id,
        staff_name=staff_name,
        staff_name_raw=payload.get("staff_name_raw"),
        section_slug=section_slug,
        section_name_raw=section_name_raw,
        arrangement=arrangement,
        display=display,
        performance=performance,
        average_score=average_score,
        rating_band=rating_band,
        weak_dimensions=weak_dimensions,
        strong_dimensions=strong_dimensions,
        confidence=confidence,
        raw=data,
    )


def find_staff_events(
    normalized_dir: Path,
    branch: str | None = None,
    report_date: str | None = None,
) -> list[StaffEvent]:
    events: list[StaffEvent] = []

    requested_branch_slug = canonical_branch_slug(branch, fallback="") if branch else ""

    for path in normalized_dir.rglob("*.json"):
        if not is_staff_performance_file(path):
            continue
        try:
            event = extract_staff_event(path)
        except Exception as exc:
            print(f"[WARN] Failed to parse {path}: {exc}")
            continue

        if event is None:
            continue
        if requested_branch_slug and event.branch_slug != requested_branch_slug:
            continue
        if report_date and event.report_date != report_date:
            continue

        events.append(event)

    return sorted(events, key=lambda e: (e.branch_slug, e.report_date, e.staff_name))


def avg(values: list[float | None]) -> float | None:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return round(mean(clean), 2)


def band_from_avg(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 4.5:
        return "strong"
    if value >= 3.5:
        return "acceptable"
    return "weak"


def summarize_staff(events: list[StaffEvent]) -> dict[str, Any]:
    if not events:
        return {
            "staff_count": 0,
            "events_count": 0,
            "top_staff": [],
            "bottom_staff": [],
            "staff_members": [],
        }

    by_staff: dict[str, list[StaffEvent]] = defaultdict(list)
    for event in events:
        by_staff[event.staff_id].append(event)

    members: list[dict[str, Any]] = []
    for staff_id, staff_events in by_staff.items():
        first = staff_events[0]

        arrangement_avg = avg([e.arrangement for e in staff_events])
        display_avg = avg([e.display for e in staff_events])
        performance_avg = avg([e.performance for e in staff_events])
        overall_avg = avg([e.average_score for e in staff_events])

        weak_counter = Counter()
        strong_counter = Counter()
        section_counter = Counter()
        rating_counter = Counter()

        for e in staff_events:
            weak_counter.update(e.weak_dimensions)
            strong_counter.update(e.strong_dimensions)
            section_counter.update([e.section_slug])
            if e.rating_band:
                rating_counter.update([e.rating_band])

        dominant_section = section_counter.most_common(1)[0][0] if section_counter else None

        members.append(
            {
                "staff_id": staff_id,
                "staff_name": first.staff_name,
                "staff_name_raw": first.staff_name_raw,
                "branch": first.branch,
                "report_date": first.report_date,
                "events_count": len(staff_events),
                "section_most_seen": dominant_section,
                "arrangement_avg": arrangement_avg,
                "display_avg": display_avg,
                "performance_avg": performance_avg,
                "overall_avg": overall_avg,
                "overall_band": band_from_avg(overall_avg),
                "weak_dimensions": dict(weak_counter),
                "strong_dimensions": dict(strong_counter),
                "rating_bands_seen": dict(rating_counter),
                "confidence_avg": avg([e.confidence for e in staff_events]),
                "source_files": sorted({str(e.source_file) for e in staff_events}),
            }
        )

    sortable = [m for m in members if m["overall_avg"] is not None]
    sortable.sort(key=lambda x: (-x["overall_avg"], x["staff_name"]))

    bottom_sortable = sorted(
        sortable,
        key=lambda x: (x["overall_avg"], x["staff_name"]),
    )

    return {
        "staff_count": len(members),
        "events_count": len(events),
        "top_staff": sortable[:5],
        "bottom_staff": bottom_sortable[:5],
        "staff_members": members,
    }


def summarize_sections(events: list[StaffEvent]) -> dict[str, Any]:
    by_section: dict[str, list[StaffEvent]] = defaultdict(list)
    for event in events:
        by_section[event.section_slug].append(event)

    sections: list[dict[str, Any]] = []
    for section_slug, section_events in by_section.items():
        arrangement_avg = avg([e.arrangement for e in section_events])
        display_avg = avg([e.display for e in section_events])
        performance_avg = avg([e.performance for e in section_events])
        overall_avg = avg([e.average_score for e in section_events])

        weak_counter = Counter()
        strong_counter = Counter()
        staff_counter = Counter()

        for e in section_events:
            weak_counter.update(e.weak_dimensions)
            strong_counter.update(e.strong_dimensions)
            staff_counter.update([e.staff_id])

        sections.append(
            {
                "section_slug": section_slug,
                "events_count": len(section_events),
                "staff_count": len(staff_counter),
                "arrangement_avg": arrangement_avg,
                "display_avg": display_avg,
                "performance_avg": performance_avg,
                "overall_avg": overall_avg,
                "overall_band": band_from_avg(overall_avg),
                "weak_dimensions": dict(weak_counter),
                "strong_dimensions": dict(strong_counter),
            }
        )

    sections.sort(key=lambda x: (x["overall_avg"] is None, x["overall_avg"] or 0))
    weakest_sections = sections[:5]
    strongest_sections = sorted(
        [s for s in sections if s["overall_avg"] is not None],
        key=lambda x: (-x["overall_avg"], x["section_slug"]),
    )[:5]

    return {
        "section_count": len(sections),
        "weakest_sections": weakest_sections,
        "strongest_sections": strongest_sections,
        "sections": sections,
    }


def summarize_dimensions(events: list[StaffEvent]) -> dict[str, Any]:
    arrangement_avg = avg([e.arrangement for e in events])
    display_avg = avg([e.display for e in events])
    performance_avg = avg([e.performance for e in events])

    dimension_map = {
        "arrangement": arrangement_avg,
        "display": display_avg,
        "performance": performance_avg,
    }

    available = {k: v for k, v in dimension_map.items() if v is not None}
    strongest_dimension = max(available, key=available.get) if available else None
    weakest_dimension = min(available, key=available.get) if available else None

    weak_counter = Counter()
    strong_counter = Counter()
    for e in events:
        weak_counter.update(e.weak_dimensions)
        strong_counter.update(e.strong_dimensions)

    return {
        "averages": dimension_map,
        "strongest_dimension": strongest_dimension,
        "weakest_dimension": weakest_dimension,
        "weak_dimension_counts": dict(weak_counter),
        "strong_dimension_counts": dict(strong_counter),
    }


def build_summary(events: list[StaffEvent]) -> dict[str, Any]:
    if not events:
        return {
            "summary_type": "staff_performance_summary",
            "message": "No staff performance events found for the requested filter.",
            "branch": None,
            "report_date": None,
            "totals": {
                "events_count": 0,
                "staff_count": 0,
                "section_count": 0,
            },
            "staff": {},
            "sections": {},
            "dimensions": {},
        }

    branches = sorted({e.branch for e in events})
    dates = sorted({e.report_date for e in events})

    staff_summary = summarize_staff(events)
    section_summary = summarize_sections(events)
    dimension_summary = summarize_dimensions(events)

    overall_avg = avg([e.average_score for e in events])

    return {
        "summary_type": "staff_performance_summary",
        "branch": branches[0] if len(branches) == 1 else "multiple",
        "report_date": dates[0] if len(dates) == 1 else "multiple",
        "branches_seen": branches,
        "dates_seen": dates,
        "totals": {
            "events_count": len(events),
            "staff_count": staff_summary.get("staff_count", 0),
            "section_count": section_summary.get("section_count", 0),
            "overall_avg_score": overall_avg,
            "overall_band": band_from_avg(overall_avg),
        },
        "dimensions": dimension_summary,
        "staff": staff_summary,
        "sections": section_summary,
    }


def render_markdown(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Staff Performance Summary")
    lines.append("")

    if summary.get("message"):
        lines.append(summary["message"])
        lines.append("")
        return "\n".join(lines)

    lines.append(f"- Branch: **{summary['branch']}**")
    lines.append(f"- Report date: **{summary['report_date']}**")
    lines.append(f"- Events: **{summary['totals']['events_count']}**")
    lines.append(f"- Staff members: **{summary['totals']['staff_count']}**")
    lines.append(f"- Sections: **{summary['totals']['section_count']}**")
    lines.append(f"- Overall average score: **{summary['totals']['overall_avg_score']}**")
    lines.append(f"- Overall band: **{summary['totals']['overall_band']}**")
    lines.append("")

    dims = summary["dimensions"]
    lines.append("## Dimension Summary")
    lines.append("")
    lines.append(f"- Arrangement avg: **{dims['averages'].get('arrangement')}**")
    lines.append(f"- Display avg: **{dims['averages'].get('display')}**")
    lines.append(f"- Performance avg: **{dims['averages'].get('performance')}**")
    lines.append(f"- Strongest dimension: **{dims.get('strongest_dimension')}**")
    lines.append(f"- Weakest dimension: **{dims.get('weakest_dimension')}**")
    lines.append("")

    lines.append("## Top Staff")
    lines.append("")
    for item in summary["staff"].get("top_staff", []):
        lines.append(
            f"- **{item['staff_name']}** ({item['staff_id']}) — "
            f"overall_avg={item['overall_avg']}, "
            f"section={item['section_most_seen']}, "
            f"band={item['overall_band']}"
        )
    lines.append("")

    lines.append("## Bottom Staff")
    lines.append("")
    for item in summary["staff"].get("bottom_staff", []):
        lines.append(
            f"- **{item['staff_name']}** ({item['staff_id']}) — "
            f"overall_avg={item['overall_avg']}, "
            f"section={item['section_most_seen']}, "
            f"band={item['overall_band']}"
        )
    lines.append("")

    lines.append("## Strongest Sections")
    lines.append("")
    for item in summary["sections"].get("strongest_sections", []):
        lines.append(
            f"- **{item['section_slug']}** — overall_avg={item['overall_avg']}, "
            f"staff_count={item['staff_count']}, "
            f"events={item['events_count']}"
        )
    lines.append("")

    lines.append("## Weakest Sections")
    lines.append("")
    for item in summary["sections"].get("weakest_sections", []):
        lines.append(
            f"- **{item['section_slug']}** — overall_avg={item['overall_avg']}, "
            f"staff_count={item['staff_count']}, "
            f"events={item['events_count']}"
        )
    lines.append("")

    return "\n".join(lines)


def build_output_paths(output_dir: Path, branch: str | None, report_date: str | None) -> tuple[Path, Path]:
    safe_branch = branch or "all_branches"
    safe_date = report_date or "all_dates"
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{safe_branch}_{safe_date}_staff_performance_summary.json"
    md_path = output_dir / f"{safe_branch}_{safe_date}_staff_performance_summary.md"
    return json_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate normalized staff_performance events into a summary report."
    )
    parser.add_argument(
        "--normalized-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_DIR,
        help="Path to normalized event directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Where summary files will be written.",
    )
    parser.add_argument(
        "--branch",
        type=str,
        default=None,
        help="Filter by branch slug, e.g. waigani",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Filter by report date, e.g. 2026-03-20",
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Print JSON summary to stdout.",
    )
    parser.add_argument(
        "--print-markdown",
        action="store_true",
        help="Print markdown summary to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    events = find_staff_events(
        normalized_dir=args.normalized_dir,
        branch=args.branch,
        report_date=args.date,
    )

    summary = build_summary(events)
    json_path, md_path = build_output_paths(args.output_dir, args.branch, args.date)

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    markdown = render_markdown(summary)
    with md_path.open("w", encoding="utf-8") as f:
        f.write(markdown)

    print(f"[staff_performance_summary] Loaded events: {len(events)}")
    print(f"[staff_performance_summary] JSON: {json_path}")
    print(f"[staff_performance_summary] Markdown: {md_path}")

    if args.print_json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))

    if args.print_markdown:
        print(markdown)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
