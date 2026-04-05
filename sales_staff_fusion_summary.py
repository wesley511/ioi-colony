#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import mean
from typing import Any

try:
    from scripts.branch_resolution import branch_path_candidates, canonical_branch_slug, resolve_branch_slug
except ModuleNotFoundError:
    from branch_resolution import branch_path_candidates, canonical_branch_slug, resolve_branch_slug


DEFAULT_NORMALIZED_DIR = Path("normalized")
DEFAULT_STAFF_REPORTS_DIR = Path("REPORTS/staff_performance")
DEFAULT_OUTPUT_DIR = Path("REPORTS/fusion")


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


def band_from_score(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 4.5:
        return "strong"
    if value >= 3.5:
        return "acceptable"
    return "weak"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def find_daily_sales_event(
    normalized_dir: Path,
    branch: str,
    report_date: str,
) -> dict[str, Any] | None:
    branch_slug = canonical_branch_slug(branch)
    for branch_path in branch_path_candidates(branch_slug):
        branch_dir = normalized_dir / branch_path / report_date
        candidate_paths: list[Path] = []
        if branch_dir.exists():
            candidate_paths.extend(sorted(branch_dir.glob("*daily_sales_report*.json")))
        candidate_paths.extend(
            [
                normalized_dir / branch_path / f"{branch_path}_daily_sales_report_{report_date}.json",
                normalized_dir / branch_path / f"{branch_path}_sales_report_{report_date}.json",
            ]
        )
        for path in candidate_paths:
            if not path.exists():
                continue
            try:
                data = load_json(path)
            except Exception:
                continue
            if data.get("event_kind") != "daily_sales_report" and data.get("signal_type") != "daily_sales_report":
                continue
            if resolve_branch_slug(data, path=path, candidates=[data.get("branch")]) == branch_slug:
                return data
    return None


def find_staff_summary(
    staff_reports_dir: Path,
    branch: str,
    report_date: str,
) -> dict[str, Any] | None:
    branch_slug = canonical_branch_slug(branch)
    for branch_path in branch_path_candidates(branch_slug):
        path = staff_reports_dir / f"{branch_path}_{report_date}_staff_performance_summary.json"
        if not path.exists():
            continue
        data = load_json(path)
        summary_branch = resolve_branch_slug(
            data,
            path=path,
            candidates=[data.get("branch"), *(data.get("branches_seen") or [])],
        )
        if summary_branch == branch_slug:
            return data
    return None


def extract_sales_metrics(daily_sales_event: dict[str, Any] | None) -> dict[str, Any]:
    if not daily_sales_event:
        return {
            "available": False,
            "branch": None,
            "report_date": None,
            "cash": None,
            "card": None,
            "z_reading": None,
            "total_sales": None,
            "balanced": None,
            "foot_traffic": None,
            "served_customers": None,
            "sales_per_labor_hour": None,
            "sales_per_customer": None,
            "conversion_rate_pct": None,
            "staffing_issues": None,
            "stock_issues_affecting_sales": None,
            "pricing_or_system_issues": None,
            "cash_variance": None,
            "exceptions_escalated": None,
            "labels": [],
        }

    payload = daily_sales_event.get("payload", {}) or {}
    sections = payload.get("sections", {}) or {}

    financials = sections.get("financials", {}) or {}
    customers = sections.get("customers", {}) or {}
    performance = sections.get("performance", {}) or {}
    supervisor = sections.get("supervisor", {}) or {}

    gatekeeper_totals = daily_sales_event.get("totals", {}) or {}
    gatekeeper_traffic = daily_sales_event.get("traffic", {}) or {}
    gatekeeper_control = daily_sales_event.get("control", {}) or {}
    total_sales = safe_float(financials.get("total_sales"))
    if total_sales is None:
        total_sales = round(
            (safe_float(gatekeeper_totals.get("cash_sales")) or 0.0)
            + (safe_float(gatekeeper_totals.get("eftpos_sales")) or 0.0),
            2,
        )

    return {
        "available": True,
        "branch": daily_sales_event.get("branch"),
        "report_date": daily_sales_event.get("report_date") or daily_sales_event.get("date"),
        "cash": safe_float(financials.get("cash")) or safe_float(gatekeeper_totals.get("cash_sales")),
        "card": safe_float(financials.get("card")) or safe_float(gatekeeper_totals.get("eftpos_sales")),
        "z_reading": safe_float(financials.get("z_reading")) or safe_float(gatekeeper_totals.get("z_reading")),
        "total_sales": total_sales,
        "balanced": financials.get("balanced") or gatekeeper_control.get("supervisor_confirmed"),
        "foot_traffic": safe_float(customers.get("foot_traffic")) or safe_float(gatekeeper_traffic.get("total_customers")),
        "served_customers": safe_float(customers.get("served_customers")) or safe_float(gatekeeper_traffic.get("customers_served")),
        "sales_per_labor_hour": safe_float(performance.get("sales_per_labor_hour")),
        "sales_per_customer": safe_float(performance.get("sales_per_customer")),
        "conversion_rate_pct": safe_float(performance.get("conversion_rate_pct")),
        "staffing_issues": supervisor.get("staffing_issues"),
        "stock_issues_affecting_sales": supervisor.get("stock_issues_affecting_sales"),
        "pricing_or_system_issues": supervisor.get("pricing_or_system_issues"),
        "cash_variance": supervisor.get("cash_variance") or safe_float(gatekeeper_totals.get("cash_variance")),
        "exceptions_escalated": supervisor.get("exceptions_escalated"),
        "labels": daily_sales_event.get("labels", []) or [],
    }


def extract_staff_metrics(staff_summary: dict[str, Any] | None) -> dict[str, Any]:
    if not staff_summary:
        return {
            "available": False,
            "branch": None,
            "report_date": None,
            "events_count": 0,
            "staff_count": 0,
            "section_count": 0,
            "overall_avg_score": None,
            "overall_band": None,
            "arrangement_avg": None,
            "display_avg": None,
            "performance_avg": None,
            "strongest_dimension": None,
            "weakest_dimension": None,
            "top_staff": [],
            "bottom_staff": [],
            "weakest_sections": [],
            "strongest_sections": [],
        }

    totals = staff_summary.get("totals", {}) or {}
    dimensions = staff_summary.get("dimensions", {}) or {}
    sections = staff_summary.get("sections", {}) or {}
    staff = staff_summary.get("staff", {}) or {}

    return {
        "available": True,
        "branch": staff_summary.get("branch"),
        "report_date": staff_summary.get("report_date"),
        "events_count": totals.get("events_count", 0),
        "staff_count": totals.get("staff_count", 0),
        "section_count": totals.get("section_count", 0),
        "overall_avg_score": safe_float(totals.get("overall_avg_score")),
        "overall_band": totals.get("overall_band"),
        "arrangement_avg": safe_float((dimensions.get("averages") or {}).get("arrangement")),
        "display_avg": safe_float((dimensions.get("averages") or {}).get("display")),
        "performance_avg": safe_float((dimensions.get("averages") or {}).get("performance")),
        "strongest_dimension": dimensions.get("strongest_dimension"),
        "weakest_dimension": dimensions.get("weakest_dimension"),
        "top_staff": staff.get("top_staff", []) or [],
        "bottom_staff": staff.get("bottom_staff", []) or [],
        "weakest_sections": sections.get("weakest_sections", []) or [],
        "strongest_sections": sections.get("strongest_sections", []) or [],
    }


def build_fusion_diagnostics(
    sales: dict[str, Any],
    staff: dict[str, Any],
) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    actions: list[str] = []

    conversion_rate = sales.get("conversion_rate_pct")
    foot_traffic = sales.get("foot_traffic")
    served_customers = sales.get("served_customers")
    total_sales = sales.get("total_sales")
    staffing_issues = sales.get("staffing_issues")
    stock_issues = sales.get("stock_issues_affecting_sales")
    pricing_issues = sales.get("pricing_or_system_issues")

    staff_avg = staff.get("overall_avg_score")
    weakest_dimension = staff.get("weakest_dimension")
    weakest_sections = staff.get("weakest_sections") or []
    bottom_staff = staff.get("bottom_staff") or []

    if sales.get("available") and staff.get("available"):
        findings.append(
            {
                "type": "fusion_status",
                "severity": "info",
                "message": "Both daily sales and staff performance data are available for fusion.",
            }
        )

    if conversion_rate is not None:
        if conversion_rate < 40:
            findings.append(
                {
                    "type": "conversion",
                    "severity": "high",
                    "message": f"Conversion rate is low at {conversion_rate}%.",
                }
            )
            actions.append("Investigate why customer traffic is not converting into purchases.")
        elif conversion_rate < 55:
            findings.append(
                {
                    "type": "conversion",
                    "severity": "medium",
                    "message": f"Conversion rate is moderate at {conversion_rate}%.",
                }
            )

    if staffing_issues is True:
        findings.append(
            {
                "type": "staffing",
                "severity": "high",
                "message": "Supervisor flagged staffing issues in the sales report.",
            }
        )
        actions.append("Review roster coverage and customer assistance during peak traffic periods.")

    if stock_issues is True:
        findings.append(
            {
                "type": "inventory",
                "severity": "high",
                "message": "Supervisor flagged stock issues affecting sales.",
            }
        )
        actions.append("Check rail readiness and inventory availability in weak sections.")

    if pricing_issues is True:
        findings.append(
            {
                "type": "pricing_or_system",
                "severity": "medium",
                "message": "Supervisor flagged pricing or system issues.",
            }
        )
        actions.append("Review pricing flow and systems impacting checkout or merchandising.")

    if staff_avg is not None:
        if staff_avg >= 4.5:
            findings.append(
                {
                    "type": "staff_performance",
                    "severity": "info",
                    "message": f"Overall staff performance is strong at {staff_avg}.",
                }
            )
        elif staff_avg < 3.5:
            findings.append(
                {
                    "type": "staff_performance",
                    "severity": "high",
                    "message": f"Overall staff performance is weak at {staff_avg}.",
                }
            )
            actions.append("Prioritize coaching and immediate floor supervision.")
        else:
            findings.append(
                {
                    "type": "staff_performance",
                    "severity": "medium",
                    "message": f"Overall staff performance is acceptable at {staff_avg}.",
                }
            )

    if weakest_dimension:
        findings.append(
            {
                "type": "weakest_dimension",
                "severity": "medium",
                "message": f"Weakest staff dimension is {weakest_dimension}.",
            }
        )
        if weakest_dimension == "arrangement":
            actions.append("Tighten arrangement standards and section presentation checks.")
        elif weakest_dimension == "display":
            actions.append("Review display quality and visual merchandising consistency.")
        elif weakest_dimension == "performance":
            actions.append("Coach staff on assistance, speed, and customer engagement.")

    if weakest_sections:
        weakest = weakest_sections[0]
        findings.append(
            {
                "type": "weakest_section",
                "severity": "medium",
                "message": (
                    f"Weakest section is {weakest.get('section_slug')} "
                    f"with overall_avg={weakest.get('overall_avg')}."
                ),
            }
        )
        actions.append(f"Supervisor should inspect section: {weakest.get('section_slug')}.")

    if bottom_staff:
        weakest_staff = bottom_staff[0]
        findings.append(
            {
                "type": "bottom_staff",
                "severity": "medium",
                "message": (
                    f"Lowest-ranked staff in summary is {weakest_staff.get('staff_name')} "
                    f"with overall_avg={weakest_staff.get('overall_avg')}."
                ),
            }
        )
        actions.append(f"Provide targeted coaching for {weakest_staff.get('staff_name')}.")

    if (
        conversion_rate is not None
        and conversion_rate < 45
        and staff_avg is not None
        and staff_avg >= 4.5
    ):
        findings.append(
            {
                "type": "fusion_inference",
                "severity": "high",
                "message": (
                    "Staff scores are strong, but conversion is still low. "
                    "This suggests inventory, assortment, pricing, or traffic quality issues."
                ),
            }
        )
        actions.append("Check whether strong staff performance is being limited by stock or merchandise mix.")

    if (
        foot_traffic is not None
        and served_customers is not None
        and foot_traffic > 0
        and served_customers < foot_traffic * 0.5
    ):
        findings.append(
            {
                "type": "service_coverage",
                "severity": "medium",
                "message": (
                    f"Only {served_customers} served customers from {foot_traffic} foot traffic."
                ),
            }
        )
        actions.append("Increase visible customer assistance and conversion follow-through on the floor.")

    if total_sales is not None and total_sales <= 0:
        findings.append(
            {
                "type": "sales",
                "severity": "high",
                "message": "Total sales are zero or negative, which requires immediate review.",
            }
        )

    # De-duplicate actions while preserving order
    deduped_actions: list[str] = []
    seen = set()
    for action in actions:
        if action not in seen:
            seen.add(action)
            deduped_actions.append(action)

    return {
        "findings": findings,
        "recommended_actions": deduped_actions,
    }


def build_summary_text(fusion: dict[str, Any]) -> str:
    sales = fusion["sales"]
    staff = fusion["staff"]

    parts: list[str] = []

    if sales.get("available"):
        parts.append(
            f"Sales data available: total_sales={sales.get('total_sales')}, "
            f"conversion_rate={sales.get('conversion_rate_pct')}%"
        )
    else:
        parts.append("Sales data unavailable")

    if staff.get("available"):
        parts.append(
            f"Staff data available: overall_avg_score={staff.get('overall_avg_score')}, "
            f"weakest_dimension={staff.get('weakest_dimension')}"
        )
    else:
        parts.append("Staff data unavailable")

    findings = fusion.get("diagnostics", {}).get("findings", [])
    if findings:
        parts.append(f"Findings={len(findings)}")

    return " | ".join(parts)


def build_fusion_summary(
    normalized_dir: Path,
    staff_reports_dir: Path,
    branch: str,
    report_date: str,
) -> dict[str, Any]:
    daily_sales_event = find_daily_sales_event(normalized_dir, branch, report_date)
    staff_summary = find_staff_summary(staff_reports_dir, branch, report_date)

    sales_metrics = extract_sales_metrics(daily_sales_event)
    staff_metrics = extract_staff_metrics(staff_summary)
    diagnostics = build_fusion_diagnostics(sales_metrics, staff_metrics)

    fusion_score_components = []

    if sales_metrics.get("conversion_rate_pct") is not None:
        fusion_score_components.append(min(sales_metrics["conversion_rate_pct"] / 20.0, 5.0))
    if staff_metrics.get("overall_avg_score") is not None:
        fusion_score_components.append(staff_metrics["overall_avg_score"])
    if sales_metrics.get("sales_per_customer") is not None:
        fusion_score_components.append(min(sales_metrics["sales_per_customer"] / 10.0, 5.0))

    fusion_score = avg(fusion_score_components)

    summary = {
        "summary_type": "sales_staff_fusion_summary",
        "branch": branch,
        "report_date": report_date,
        "fusion_score": fusion_score,
        "fusion_band": band_from_score(fusion_score),
        "sales": sales_metrics,
        "staff": staff_metrics,
        "diagnostics": diagnostics,
    }
    summary["summary_text"] = build_summary_text(summary)
    return summary


def render_markdown(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Sales + Staff Fusion Summary")
    lines.append("")
    lines.append(f"- Branch: **{summary['branch']}**")
    lines.append(f"- Report date: **{summary['report_date']}**")
    lines.append(f"- Fusion score: **{summary.get('fusion_score')}**")
    lines.append(f"- Fusion band: **{summary.get('fusion_band')}**")
    lines.append("")

    sales = summary["sales"]
    lines.append("## Sales")
    lines.append("")
    lines.append(f"- Available: **{sales.get('available')}**")
    lines.append(f"- Total sales: **{sales.get('total_sales')}**")
    lines.append(f"- Cash: **{sales.get('cash')}**")
    lines.append(f"- Card: **{sales.get('card')}**")
    lines.append(f"- Foot traffic: **{sales.get('foot_traffic')}**")
    lines.append(f"- Served customers: **{sales.get('served_customers')}**")
    lines.append(f"- Conversion rate: **{sales.get('conversion_rate_pct')}**")
    lines.append(f"- Sales per customer: **{sales.get('sales_per_customer')}**")
    lines.append(f"- Sales per labor hour: **{sales.get('sales_per_labor_hour')}**")
    lines.append(f"- Staffing issues: **{sales.get('staffing_issues')}**")
    lines.append(f"- Stock issues affecting sales: **{sales.get('stock_issues_affecting_sales')}**")
    lines.append(f"- Pricing/system issues: **{sales.get('pricing_or_system_issues')}**")
    lines.append("")

    staff = summary["staff"]
    lines.append("## Staff")
    lines.append("")
    lines.append(f"- Available: **{staff.get('available')}**")
    lines.append(f"- Staff count: **{staff.get('staff_count')}**")
    lines.append(f"- Events count: **{staff.get('events_count')}**")
    lines.append(f"- Overall avg score: **{staff.get('overall_avg_score')}**")
    lines.append(f"- Overall band: **{staff.get('overall_band')}**")
    lines.append(f"- Arrangement avg: **{staff.get('arrangement_avg')}**")
    lines.append(f"- Display avg: **{staff.get('display_avg')}**")
    lines.append(f"- Performance avg: **{staff.get('performance_avg')}**")
    lines.append(f"- Strongest dimension: **{staff.get('strongest_dimension')}**")
    lines.append(f"- Weakest dimension: **{staff.get('weakest_dimension')}**")
    lines.append("")

    lines.append("## Top Staff")
    lines.append("")
    for item in staff.get("top_staff", [])[:5]:
        lines.append(
            f"- **{item.get('staff_name')}** — overall_avg={item.get('overall_avg')}, "
            f"section={item.get('section_most_seen')}"
        )
    lines.append("")

    lines.append("## Weakest Sections")
    lines.append("")
    for item in staff.get("weakest_sections", [])[:5]:
        lines.append(
            f"- **{item.get('section_slug')}** — overall_avg={item.get('overall_avg')}, "
            f"staff_count={item.get('staff_count')}"
        )
    lines.append("")

    lines.append("## Diagnostics")
    lines.append("")
    for finding in summary.get("diagnostics", {}).get("findings", []):
        lines.append(
            f"- **[{finding.get('severity')}] {finding.get('type')}** — {finding.get('message')}"
        )
    lines.append("")

    lines.append("## Recommended Actions")
    lines.append("")
    for action in summary.get("diagnostics", {}).get("recommended_actions", []):
        lines.append(f"- {action}")
    lines.append("")

    return "\n".join(lines)


def build_output_paths(output_dir: Path, branch: str, report_date: str) -> tuple[Path, Path]:
    ensure_dir(output_dir)
    json_path = output_dir / f"{branch}_{report_date}_sales_staff_fusion_summary.json"
    md_path = output_dir / f"{branch}_{report_date}_sales_staff_fusion_summary.md"
    return json_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fuse daily sales and staff performance summaries into one operational summary."
    )
    parser.add_argument(
        "--normalized-dir",
        type=Path,
        default=DEFAULT_NORMALIZED_DIR,
        help="Path to normalized events directory.",
    )
    parser.add_argument(
        "--staff-reports-dir",
        type=Path,
        default=DEFAULT_STAFF_REPORTS_DIR,
        help="Path to generated staff performance summary reports.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Path to write fusion reports.",
    )
    parser.add_argument(
        "--branch",
        required=True,
        help="Branch slug, e.g. waigani",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Report date, e.g. 2026-03-20",
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Print JSON result to stdout.",
    )
    parser.add_argument(
        "--print-markdown",
        action="store_true",
        help="Print markdown result to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    summary = build_fusion_summary(
        normalized_dir=args.normalized_dir,
        staff_reports_dir=args.staff_reports_dir,
        branch=args.branch,
        report_date=args.date,
    )

    json_path, md_path = build_output_paths(args.output_dir, args.branch, args.date)

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    markdown = render_markdown(summary)
    with md_path.open("w", encoding="utf-8") as f:
        f.write(markdown)

    print(f"[sales_staff_fusion_summary] JSON: {json_path}")
    print(f"[sales_staff_fusion_summary] Markdown: {md_path}")

    if args.print_json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))

    if args.print_markdown:
        print(markdown)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
