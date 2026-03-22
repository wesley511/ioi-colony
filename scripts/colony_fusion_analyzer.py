#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IOI Colony Fusion Analyzer

Purpose:
- Fuse advisory staff signal strength with normalized sales YAML data
- Produce advisory-only branch intelligence
- Detect mismatches between staff activity and sales performance

This script is READ-ONLY and ANALYSIS-ONLY.
It must not execute business actions or modify operations.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any

import yaml

from scripts.colony_analyzer import (
    ensure_governance,
    load_signals,
    aggregate_by_branch,
    normalize_label,
)

POST_PREFIX = "[IOI Colony Fusion]"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fuse staff advisory signals with sales YAML and produce advisory-only branch intelligence."
    )
    parser.add_argument(
        "--memory-dir",
        default="COLONY_MEMORY/staff_signals",
        help="Directory containing staff signal memory (default: COLONY_MEMORY/staff_signals)",
    )
    parser.add_argument(
        "--sales-dir",
        default="SIGNALS/normalized",
        help="Directory containing normalized sales YAML files (default: SIGNALS/normalized)",
    )
    parser.add_argument(
        "--rules-file",
        default="COLONY_RULES.md",
        help="Governance file to verify before analysis (default: COLONY_RULES.md)",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.5,
        help="Minimum confidence threshold for including staff signals (default: 0.5)",
    )
    parser.add_argument(
        "--staff-high-threshold",
        type=float,
        default=25.0,
        help="Threshold for high staff activity (default: 25.0)",
    )
    parser.add_argument(
        "--staff-low-threshold",
        type=float,
        default=15.0,
        help="Threshold for low staff capacity (default: 15.0)",
    )
    parser.add_argument(
        "--sales-per-staff-threshold",
        type=float,
        default=50.0,
        help="Threshold below which sales per staff is considered weak (default: 50.0)",
    )
    parser.add_argument(
        "--high-conversion-threshold",
        type=float,
        default=0.60,
        help="Threshold above which conversion is considered high (default: 0.60)",
    )
    parser.add_argument(
        "--low-conversion-threshold",
        type=float,
        default=0.30,
        help="Threshold below which conversion is considered low (default: 0.30)",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional output file path for advisory report",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show debug details for branch matching and sales loading",
    )
    return parser.parse_args()

def load_sales(sales_dir: Path, debug: bool = False) -> Dict[str, Dict[str, float]]:
    if not sales_dir.exists():
        raise RuntimeError(f"Sales directory not found: {sales_dir}")

    sales_data: Dict[str, Dict[str, float]] = {}

    for path in sorted(sales_dir.glob("*sales*.yaml")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except Exception as e:
            print(f"[WARN] Failed to read {path}: {e}", file=sys.stderr)
            continue

        raw_branch = (
            data.get("branch")
            or data.get("store")
            or data.get("location")
            or data.get("shop")
            or "unknown"
        )
        branch = normalize_label(str(raw_branch))

        totals = data.get("totals", {}) or {}
        customers = data.get("customers", {}) or {}
        performance = data.get("performance", {}) or {}

        total_sales = (
            data.get("total_sales")
            or data.get("sales")
            or totals.get("sales")
            or 0
        )

        cash_sales = (
            data.get("cash_sales")
            or totals.get("cash")
            or 0
        )

        card_sales = (
            data.get("card_sales")
            or totals.get("card")
            or 0
        )

        traffic = (
            data.get("traffic")
            or customers.get("traffic")
            or 0
        )

        served = (
            data.get("served")
            or customers.get("served")
            or 0
        )

        conversion_rate = (
            data.get("conversion_rate")
            or data.get("conversion")
            or customers.get("conversion_rate")
            or 0
        )

        sales_per_staff = (
            data.get("sales_per_staff")
            or performance.get("sales_per_staff")
            or 0
        )

        sales_per_customer = (
            data.get("sales_per_customer")
            or performance.get("sales_per_customer")
            or 0
        )

        sales_per_labor_hour = (
            data.get("sales_per_labor_hour")
            or performance.get("sales_per_labor_hour")
            or 0
        )

        record = {
            "total_sales": float(total_sales or 0),
            "cash_sales": float(cash_sales or 0),
            "card_sales": float(card_sales or 0),
            "traffic": float(traffic or 0),
            "served": float(served or 0),
            "conversion_rate": float(conversion_rate or 0),
            "sales_per_staff": float(sales_per_staff or 0),
            "sales_per_customer": float(sales_per_customer or 0),
            "sales_per_labor_hour": float(sales_per_labor_hour or 0),
        }

        sales_data[branch] = record

        if debug:
            print(f"[DEBUG] File: {path.name}", file=sys.stderr)
            print(f"[DEBUG] Raw branch: {raw_branch}", file=sys.stderr)
            print(f"[DEBUG] Normalized: {branch}", file=sys.stderr)
            print(f"[DEBUG] Sales: {record}", file=sys.stderr)
            print("", file=sys.stderr)

    return sales_data

def compute_efficiency(sales: Dict[str, float]) -> float:
    return (
        sales.get("sales_per_staff", 0.0) * 0.4
        + sales.get("sales_per_customer", 0.0) * 0.3
        + sales.get("conversion_rate", 0.0) * 100.0 * 0.3
    )


def fuse_data(
    staff_data: Dict[str, Dict[str, Any]],
    sales_data: Dict[str, Dict[str, float]],
    debug: bool = False,
) -> Dict[str, Dict[str, float]]:
    fusion: Dict[str, Dict[str, float]] = {}

    for branch, data in staff_data.items():
        normalized_branch = normalize_label(branch)

        if debug:
            print(f"{POST_PREFIX} DEBUG staff branch: {normalized_branch}", file=sys.stderr)
            print(
                f"{POST_PREFIX} DEBUG available sales keys: {list(sales_data.keys())}",
                file=sys.stderr,
            )

        sales = sales_data.get(
            normalized_branch,
            {
                "total_sales": 0.0,
                "cash_sales": 0.0,
                "card_sales": 0.0,
                "traffic": 0.0,
                "served": 0.0,
                "conversion_rate": 0.0,
                "sales_per_labor_hour": 0.0,
                "sales_per_customer": 0.0,
                "sales_per_staff": 0.0,
            },
        )

        avg_strength = data["total_strength"] / max(data["count"], 1)
        efficiency = compute_efficiency(sales)

        fusion[normalized_branch] = {
            "staff_strength": avg_strength,
            "signal_count": float(data["count"]),
            "total_sales": sales["total_sales"],
            "cash_sales": sales["cash_sales"],
            "card_sales": sales["card_sales"],
            "traffic": sales["traffic"],
            "served": sales["served"],
            "conversion_rate": sales["conversion_rate"],
            "sales_per_labor_hour": sales["sales_per_labor_hour"],
            "sales_per_customer": sales["sales_per_customer"],
            "sales_per_staff": sales["sales_per_staff"],
            "efficiency": efficiency,
        }

    return fusion


def rank_fusion(fusion: Dict[str, Dict[str, float]]) -> List[Tuple[str, Dict[str, float]]]:
    return sorted(
        fusion.items(),
        key=lambda x: (x[1]["efficiency"], x[1]["total_sales"], x[1]["staff_strength"]),
        reverse=True,
    )


def detect_inefficiencies(
    fusion: Dict[str, Dict[str, float]],
    staff_high_threshold: float,
    staff_low_threshold: float,
    sales_per_staff_threshold: float,
    high_conversion_threshold: float,
    low_conversion_threshold: float,
) -> List[Tuple[str, str]]:
    issues: List[Tuple[str, str]] = []

    for branch, data in fusion.items():
        if data["staff_strength"] > staff_high_threshold and data["conversion_rate"] < low_conversion_threshold:
            issues.append((branch, "High effort but low conversion"))

        if data["sales_per_staff"] > 0 and data["sales_per_staff"] < sales_per_staff_threshold:
            issues.append((branch, "Low sales per staff"))

        if data["conversion_rate"] > high_conversion_threshold and data["staff_strength"] < staff_low_threshold:
            issues.append((branch, "High demand but low staff capacity"))

        if data["total_sales"] == 0:
            issues.append((branch, "No matched sales data"))

    return issues


def generate_report(
    rankings: List[Tuple[str, Dict[str, float]]],
    issues: List[Tuple[str, str]],
) -> str:
    lines: List[str] = []
    lines.append(f"{POST_PREFIX} === FUSION INTELLIGENCE REPORT ===")
    lines.append("")

    if rankings:
        lines.append(f"Branches fused: {len(rankings)}")
        lines.append(f"Top fusion performer: {rankings[0][0].upper()}")
        lines.append(f"Weakest fusion performer: {rankings[-1][0].upper()}")
        lines.append("")

    lines.append("=== BRANCH FUSION RANKING ===")
    lines.append("")

    for i, (branch, data) in enumerate(rankings, 1):
        lines.append(
            f"{i}. {branch.upper()} | "
            f"sales=K{data['total_sales']:.2f} | "
            f"staff={data['staff_strength']:.2f} | "
            f"conversion={data['conversion_rate']*100:.1f}% | "
            f"sales/staff={data['sales_per_staff']:.2f} | "
            f"labor=K{data['sales_per_labor_hour']:.2f} | "
            f"efficiency={data['efficiency']:.2f}"
        )
        if data["total_sales"] == 0:
            lines.append("   ⚠️ No sales data matched for this branch")

    lines.append("")
    lines.append("=== STRATEGIC ISSUES ===")
    lines.append("")

    if issues:
        for branch, issue in issues:
            lines.append(f"- {branch.upper()}: {issue}")
    else:
        lines.append("- No major issues detected")

    lines.append("")
    lines.append("=== STRATEGIC RECOMMENDATIONS ===")
    lines.append("")

    if issues:
        seen: set[tuple[str, str]] = set()
        for branch, issue in issues:
            key = (branch, issue)
            if key in seen:
                continue
            seen.add(key)

            issue_lower = issue.lower()
            if "low conversion" in issue_lower:
                lines.append(f"- Review display, pricing, and customer engagement in {branch}")
            elif "low sales per staff" in issue_lower:
                lines.append(f"- Improve staff utilization and selling support in {branch}")
            elif "low staff capacity" in issue_lower:
                lines.append(f"- Add staff or redistribute workforce to {branch}")
            elif "no matched sales data" in issue_lower:
                lines.append(f"- Verify branch normalization and sales-file mapping for {branch}")
            else:
                lines.append(f"- Review branch performance conditions in {branch}")
    else:
        lines.append("- Continue monitoring daily branch fusion metrics")
        lines.append("- Expand sales coverage and section mapping for richer branch intelligence")

    lines.append("")
    lines.append(f"{POST_PREFIX} Advisory only. The colony informs. Humans decide.")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()

    ensure_governance(Path(args.rules_file))

    staff_signals = load_signals(
        Path(args.memory_dir),
        min_confidence=args.min_confidence,
        verbose=args.debug,
    )

    staff_data = aggregate_by_branch(staff_signals)
    sales_data = load_sales(Path(args.sales_dir), debug=args.debug)

    fusion = fuse_data(staff_data, sales_data, debug=args.debug)
    rankings = rank_fusion(fusion)
    issues = detect_inefficiencies(
        fusion=fusion,
        staff_high_threshold=args.staff_high_threshold,
        staff_low_threshold=args.staff_low_threshold,
        sales_per_staff_threshold=args.sales_per_staff_threshold,
        high_conversion_threshold=args.high_conversion_threshold,
        low_conversion_threshold=args.low_conversion_threshold,
    )

    report = generate_report(rankings, issues)

    print(report)

    if args.output:
        output_path = Path(args.output)
        output_path.write_text(report, encoding="utf-8")
        print(f"\nSaved report to {output_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"{POST_PREFIX} ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
