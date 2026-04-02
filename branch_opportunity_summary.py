#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import mean
from typing import Any

try:
    from scripts.branch_resolution import canonical_branch_slug, resolve_branch_slug
except ModuleNotFoundError:
    from branch_resolution import canonical_branch_slug, resolve_branch_slug


DEFAULT_FUSION_DIR = Path("REPORTS/fusion")
DEFAULT_OUTPUT_DIR = Path("REPORTS/opportunities")


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def avg(values: list[float | None]) -> float | None:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return round(mean(clean), 2)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def severity_weight(severity: str | None) -> int:
    mapping = {
        "critical": 4,
        "high": 3,
        "medium": 2,
        "low": 1,
        "info": 0,
        None: 0,
    }
    return mapping.get(severity, 0)


def band_from_score(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 4.5:
        return "strong"
    if value >= 3.5:
        return "acceptable"
    return "weak"


def parse_fusion_filename(path: Path) -> tuple[str | None, str | None]:
    name = path.stem
    suffix = "_sales_staff_fusion_summary"
    if not name.endswith(suffix):
        return None, None

    core = name[: -len(suffix)]

    # Expect: <branch>_<YYYY-MM-DD>
    if "_" not in core:
        return None, None

    branch, report_date = core.rsplit("_", 1)

    # Basic date shape check
    if len(report_date) != 10 or report_date[4] != "-" or report_date[7] != "-":
        return None, None

    return branch, report_date


def find_fusion_files(
    fusion_dir: Path,
    branch: str | None = None,
    report_date: str | None = None,
) -> list[Path]:
    if not fusion_dir.exists():
        return []

    paths = sorted(fusion_dir.glob("*_sales_staff_fusion_summary.json"))
    selected: list[Path] = []

    requested_branch_slug = canonical_branch_slug(branch, fallback="") if branch else ""
    for path in paths:
        file_branch, file_date = parse_fusion_filename(path)
        file_branch_slug = resolve_branch_slug(path=path, candidates=[file_branch])
        if requested_branch_slug and file_branch_slug != requested_branch_slug:
            continue
        if report_date and file_date != report_date:
            continue
        selected.append(path)

    return selected


def extract_sales_risk(sales: dict[str, Any]) -> float:
    risk = 0.0

    conversion = safe_float(sales.get("conversion_rate_pct"))
    if conversion is not None:
        if conversion < 35:
            risk += 3.0
        elif conversion < 45:
            risk += 2.0
        elif conversion < 55:
            risk += 1.0

    staffing_issues = sales.get("staffing_issues")
    stock_issues = sales.get("stock_issues_affecting_sales")
    pricing_issues = sales.get("pricing_or_system_issues")

    if staffing_issues is True:
        risk += 2.5
    if stock_issues is True:
        risk += 2.5
    if pricing_issues is True:
        risk += 1.5

    foot_traffic = safe_float(sales.get("foot_traffic"))
    served_customers = safe_float(sales.get("served_customers"))
    if foot_traffic and foot_traffic > 0 and served_customers is not None:
        served_ratio = served_customers / foot_traffic
        if served_ratio < 0.40:
            risk += 2.0
        elif served_ratio < 0.55:
            risk += 1.0

    total_sales = safe_float(sales.get("total_sales"))
    if total_sales is not None and total_sales <= 0:
        risk += 4.0

    return round(risk, 2)


def extract_staff_risk(staff: dict[str, Any]) -> float:
    risk = 0.0

    if not staff.get("available"):
        return 1.5

    overall_avg = safe_float(staff.get("overall_avg_score"))
    if overall_avg is not None:
        if overall_avg < 3.5:
            risk += 3.0
        elif overall_avg < 4.0:
            risk += 2.0
        elif overall_avg < 4.5:
            risk += 1.0

    weakest_dimension = staff.get("weakest_dimension")
    if weakest_dimension == "arrangement":
        risk += 1.0
    elif weakest_dimension in {"display", "performance"}:
        risk += 1.5

    weakest_sections = staff.get("weakest_sections", []) or []
    if weakest_sections:
        weakest_avg = safe_float(weakest_sections[0].get("overall_avg"))
        if weakest_avg is not None and weakest_avg < 4.0:
            risk += 2.0
        elif weakest_avg is not None and weakest_avg < 4.5:
            risk += 1.0

    bottom_staff = staff.get("bottom_staff", []) or []
    if bottom_staff:
        bottom_avg = safe_float(bottom_staff[0].get("overall_avg"))
        if bottom_avg is not None and bottom_avg < 3.5:
            risk += 2.0
        elif bottom_avg is not None and bottom_avg < 4.0:
            risk += 1.0

    return round(risk, 2)


def extract_diagnostic_risk(diagnostics: dict[str, Any]) -> float:
    findings = diagnostics.get("findings", []) or []
    risk = 0.0
    for finding in findings:
        risk += severity_weight(finding.get("severity")) * 0.75
    return round(risk, 2)


def determine_primary_cause(summary: dict[str, Any]) -> str:
    sales = summary.get("sales", {}) or {}
    staff = summary.get("staff", {}) or {}

    if sales.get("staffing_issues") is True:
        return "staffing_coverage"
    if sales.get("stock_issues_affecting_sales") is True:
        return "inventory_availability"
    if sales.get("pricing_or_system_issues") is True:
        return "pricing_or_system"
    if safe_float(sales.get("conversion_rate_pct")) is not None and safe_float(sales.get("conversion_rate_pct")) < 45:
        return "low_conversion"
    if staff.get("available") and staff.get("weakest_dimension") == "arrangement":
        return "arrangement_execution"
    if staff.get("available") and staff.get("weakest_dimension") == "performance":
        return "customer_service_execution"
    if not sales.get("available") and staff.get("available"):
        return "missing_sales_context"
    if sales.get("available") and not staff.get("available"):
        return "missing_staff_context"
    return "mixed_operational_signal"


def determine_top_opportunity(summary: dict[str, Any]) -> str:
    sales = summary.get("sales", {}) or {}
    staff = summary.get("staff", {}) or {}

    if sales.get("staffing_issues") is True:
        return "improve staffing coverage and customer assistance"
    if sales.get("stock_issues_affecting_sales") is True:
        return "improve inventory availability and rail readiness"
    if sales.get("pricing_or_system_issues") is True:
        return "fix pricing or system bottlenecks"
    if safe_float(sales.get("conversion_rate_pct")) is not None and safe_float(sales.get("conversion_rate_pct")) < 45:
        return "increase conversion from existing foot traffic"
    if staff.get("weakest_dimension") == "arrangement":
        return "tighten arrangement and section presentation standards"
    if staff.get("weakest_dimension") == "performance":
        return "coach staff on customer engagement and selling execution"
    if not sales.get("available") and staff.get("available"):
        return "collect matching daily sales data for fusion"
    if sales.get("available") and not staff.get("available"):
        return "collect matching staff performance data for fusion"
    return "maintain performance and close minor operating gaps"


def build_priority_score(summary: dict[str, Any]) -> float:
    sales = summary.get("sales", {}) or {}
    staff = summary.get("staff", {}) or {}
    diagnostics = summary.get("diagnostics", {}) or {}

    sales_risk = extract_sales_risk(sales)
    staff_risk = extract_staff_risk(staff)
    diagnostic_risk = extract_diagnostic_risk(diagnostics)

    fusion_score = safe_float(summary.get("fusion_score"))
    weakness_penalty = 0.0
    if fusion_score is not None:
        weakness_penalty = max(0.0, 5.0 - fusion_score)

    priority_score = sales_risk + staff_risk + diagnostic_risk + weakness_penalty
    return round(priority_score, 2)


def summarize_branch(summary: dict[str, Any], source_path: Path) -> dict[str, Any]:
    branch = summary.get("branch")
    report_date = summary.get("report_date")
    fusion_score = safe_float(summary.get("fusion_score"))
    fusion_band = summary.get("fusion_band")
    sales = summary.get("sales", {}) or {}
    staff = summary.get("staff", {}) or {}
    diagnostics = summary.get("diagnostics", {}) or {}

    priority_score = build_priority_score(summary)
    primary_cause = determine_primary_cause(summary)
    top_opportunity = determine_top_opportunity(summary)

    findings = diagnostics.get("findings", []) or []
    recommended_actions = diagnostics.get("recommended_actions", []) or []

    headline = (
        f"{branch} on {report_date}: "
        f"fusion={fusion_score} ({fusion_band}), "
        f"cause={primary_cause}, "
        f"opportunity={top_opportunity}"
    )

    return {
        "branch": branch,
        "report_date": report_date,
        "fusion_score": fusion_score,
        "fusion_band": fusion_band,
        "priority_score": priority_score,
        "primary_cause": primary_cause,
        "top_opportunity": top_opportunity,
        "sales_available": sales.get("available"),
        "staff_available": staff.get("available"),
        "sales": {
            "total_sales": sales.get("total_sales"),
            "conversion_rate_pct": sales.get("conversion_rate_pct"),
            "foot_traffic": sales.get("foot_traffic"),
            "served_customers": sales.get("served_customers"),
            "staffing_issues": sales.get("staffing_issues"),
            "stock_issues_affecting_sales": sales.get("stock_issues_affecting_sales"),
            "pricing_or_system_issues": sales.get("pricing_or_system_issues"),
        },
        "staff": {
            "overall_avg_score": staff.get("overall_avg_score"),
            "overall_band": staff.get("overall_band"),
            "weakest_dimension": staff.get("weakest_dimension"),
            "weakest_sections": staff.get("weakest_sections", [])[:3],
            "bottom_staff": staff.get("bottom_staff", [])[:3],
        },
        "findings": findings,
        "recommended_actions": recommended_actions,
        "headline": headline,
        "source_file": str(source_path),
    }


def build_global_summary(entries: list[dict[str, Any]]) -> dict[str, Any]:
    if not entries:
        return {
            "summary_type": "branch_opportunity_summary",
            "message": "No fusion summaries found for the requested filter.",
            "entries_count": 0,
            "top_risk_branch": None,
            "top_opportunity_branch": None,
            "branches": [],
        }

    ranked = sorted(
        entries,
        key=lambda x: (-x["priority_score"], x["branch"], x["report_date"]),
    )

    top_risk_branch = ranked[0]

    opportunity_ranked = sorted(
        entries,
        key=lambda x: (
            x["sales_available"] is False and x["staff_available"] is False,
            -(x["priority_score"]),
            x["branch"],
        ),
    )
    top_opportunity_branch = opportunity_ranked[0]

    overall_fusion_avg = avg([safe_float(entry.get("fusion_score")) for entry in entries])

    return {
        "summary_type": "branch_opportunity_summary",
        "entries_count": len(entries),
        "overall_avg_fusion_score": overall_fusion_avg,
        "overall_fusion_band": band_from_score(overall_fusion_avg),
        "top_risk_branch": top_risk_branch,
        "top_opportunity_branch": top_opportunity_branch,
        "branches": ranked,
    }


def render_markdown(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Branch Opportunity Summary")
    lines.append("")

    if summary.get("message"):
        lines.append(summary["message"])
        lines.append("")
        return "\n".join(lines)

    lines.append(f"- Entries analysed: **{summary['entries_count']}**")
    lines.append(f"- Overall avg fusion score: **{summary['overall_avg_fusion_score']}**")
    lines.append(f"- Overall fusion band: **{summary['overall_fusion_band']}**")
    lines.append("")

    top_risk = summary.get("top_risk_branch")
    if top_risk:
        lines.append("## Top Risk Branch")
        lines.append("")
        lines.append(f"- Branch: **{top_risk['branch']}**")
        lines.append(f"- Report date: **{top_risk['report_date']}**")
        lines.append(f"- Priority score: **{top_risk['priority_score']}**")
        lines.append(f"- Fusion score: **{top_risk['fusion_score']}**")
        lines.append(f"- Primary cause: **{top_risk['primary_cause']}**")
        lines.append(f"- Top opportunity: **{top_risk['top_opportunity']}**")
        lines.append("")

    top_opportunity = summary.get("top_opportunity_branch")
    if top_opportunity:
        lines.append("## Top Improvement Opportunity")
        lines.append("")
        lines.append(f"- Branch: **{top_opportunity['branch']}**")
        lines.append(f"- Report date: **{top_opportunity['report_date']}**")
        lines.append(f"- Opportunity: **{top_opportunity['top_opportunity']}**")
        lines.append(f"- Primary cause: **{top_opportunity['primary_cause']}**")
        lines.append("")

    lines.append("## Branch Ranking")
    lines.append("")
    for entry in summary.get("branches", []):
        lines.append(
            f"- **{entry['branch']}** ({entry['report_date']}) — "
            f"priority={entry['priority_score']}, fusion={entry['fusion_score']}, "
            f"cause={entry['primary_cause']}, opportunity={entry['top_opportunity']}"
        )
    lines.append("")

    lines.append("## Detailed Actions")
    lines.append("")
    for entry in summary.get("branches", []):
        lines.append(f"### {entry['branch']} — {entry['report_date']}")
        lines.append("")
        lines.append(f"- Headline: {entry['headline']}")
        lines.append(f"- Priority score: **{entry['priority_score']}**")
        lines.append(f"- Fusion score: **{entry['fusion_score']}**")
        lines.append(f"- Primary cause: **{entry['primary_cause']}**")
        lines.append(f"- Top opportunity: **{entry['top_opportunity']}**")
        lines.append("")
        if entry.get("recommended_actions"):
            lines.append("Recommended actions:")
            for action in entry["recommended_actions"]:
                lines.append(f"- {action}")
            lines.append("")
        if entry.get("findings"):
            lines.append("Findings:")
            for finding in entry["findings"]:
                lines.append(
                    f"- [{finding.get('severity')}] {finding.get('type')}: {finding.get('message')}"
                )
            lines.append("")

    return "\n".join(lines)


def build_output_paths(
    output_dir: Path,
    branch: str | None,
    report_date: str | None,
) -> tuple[Path, Path]:
    ensure_dir(output_dir)
    safe_branch = branch or "all_branches"
    safe_date = report_date or "all_dates"
    json_path = output_dir / f"{safe_branch}_{safe_date}_branch_opportunity_summary.json"
    md_path = output_dir / f"{safe_branch}_{safe_date}_branch_opportunity_summary.md"
    return json_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rank branches and opportunities from sales + staff fusion summaries."
    )
    parser.add_argument(
        "--fusion-dir",
        type=Path,
        default=DEFAULT_FUSION_DIR,
        help="Path to fusion summary directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Path to write opportunity summaries.",
    )
    parser.add_argument(
        "--branch",
        type=str,
        default=None,
        help="Optional branch filter, e.g. waigani",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Optional report date filter, e.g. 2026-03-20",
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

    fusion_files = find_fusion_files(
        fusion_dir=args.fusion_dir,
        branch=args.branch,
        report_date=args.date,
    )

    entries: list[dict[str, Any]] = []
    for path in fusion_files:
        try:
            summary = load_json(path)
        except Exception as exc:
            print(f"[WARN] Failed to load fusion summary {path}: {exc}")
            continue
        entries.append(summarize_branch(summary, path))

    global_summary = build_global_summary(entries)

    json_path, md_path = build_output_paths(
        output_dir=args.output_dir,
        branch=args.branch,
        report_date=args.date,
    )

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(global_summary, f, indent=2, ensure_ascii=False)

    markdown = render_markdown(global_summary)
    with md_path.open("w", encoding="utf-8") as f:
        f.write(markdown)

    print(f"[branch_opportunity_summary] Loaded fusion summaries: {len(entries)}")
    print(f"[branch_opportunity_summary] JSON: {json_path}")
    print(f"[branch_opportunity_summary] Markdown: {md_path}")

    if args.print_json:
        print(json.dumps(global_summary, indent=2, ensure_ascii=False))

    if args.print_markdown:
        print(markdown)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
