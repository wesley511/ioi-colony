#!/usr/bin/env python3
"""
IOI Colony Analyzer
Governed by COLONY_RULES.md — advisory only, no execution.

Purpose:
- Read persistent signal memory from COLONY_MEMORY/
- Detect patterns across staff performance signals
- Rank opportunity areas using evidence from historical signals
- Produce advisory output only

This script MUST NOT:
- execute business actions
- contact external parties
- score staff for HR control
- modify operational systems

Safe output:
- stdout
- optional advisory report file (e.g. RECOMMENDATIONS.md)
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple
import re


POST_PREFIX = "[IOI Colony Advisory]"


@dataclass
class Signal:
    source_file: Path
    date: str
    day: str
    source_type: str
    source_name: str
    category: str
    signal_type: str
    staff_id: str
    section: str
    products: str
    items_moved: float
    assisting_count: float
    description: str
    confidence: float
    opportunity_score: float
    status: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze colony memory and produce advisory-only recommendations."
    )
    parser.add_argument(
        "--memory-dir",
        default="COLONY_MEMORY/staff_signals",
        help="Directory containing dated memory folders (default: COLONY_MEMORY/staff_signals)",
    )
    parser.add_argument(
        "--rules-file",
        default="COLONY_RULES.md",
        help="Governance file to verify before analysis (default: COLONY_RULES.md)",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional output file path for advisory report (example: RECOMMENDATIONS.md)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of top items per section in the report (default: 5)",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Minimum confidence threshold for including signals (default: 0.0)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show additional parsing diagnostics",
    )
    return parser.parse_args()


def ensure_governance(rules_file: Path) -> None:
    if not rules_file.exists():
        raise RuntimeError(f"Governance file not found: {rules_file}")

    text = rules_file.read_text(encoding="utf-8", errors="ignore")
    required_phrases = [
        "READ-ONLY",
        "ANALYSIS-ONLY",
        "ADVISORY",
        "The colony informs. Humans decide.",
    ]

    missing = [phrase for phrase in required_phrases if phrase not in text]
    if missing:
        raise RuntimeError(
            f"Governance verification failed. Missing phrases in {rules_file}: {missing}"
        )


def coerce_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value.strip())
    except Exception:
        return default


def parse_key_value_file(path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = value.strip()
    return data


def build_signal(path: Path) -> Signal | None:
    data = parse_key_value_file(path)

    # Basic shape check
    required_keys = [
        "date",
        "signal_type",
        "staff_id",
        "items_moved",
        "assisting_count",
        "confidence",
        "opportunity_score",
    ]
    if any(k not in data for k in required_keys):
        return None

    return Signal(
        source_file=path,
        date=data.get("date", ""),
        day=data.get("day", ""),
        source_type=data.get("source_type", ""),
        source_name=data.get("source_name", ""),
        category=data.get("category", ""),
        signal_type=data.get("signal_type", ""),
        staff_id=data.get("staff_id", ""),
        section=data.get("section", ""),
        products=data.get("products", ""),
        items_moved=coerce_float(data.get("items_moved", "0")),
        assisting_count=coerce_float(data.get("assisting_count", "0")),
        description=data.get("description", ""),
        confidence=coerce_float(data.get("confidence", "0")),
        opportunity_score=coerce_float(data.get("opportunity_score", "0")),
        status=data.get("status", ""),
    )


def load_signals(memory_dir: Path, min_confidence: float, verbose: bool = False) -> List[Signal]:
    if not memory_dir.exists():
        raise RuntimeError(f"Memory directory not found: {memory_dir}")

    signals: List[Signal] = []
    for path in sorted(memory_dir.rglob("*.md")):
        # Skip metadata / recommendations / non-signal docs
        lowered = path.name.lower()
        if lowered in {"metadata.md", "recommendations.md"}:
            continue
        sig = build_signal(path)
        if sig is None:
            if verbose:
                print(f"SKIP unparsable: {path}", file=sys.stderr)
            continue
        if sig.confidence < min_confidence:
            continue
        signals.append(sig)

    if not signals:
        raise RuntimeError(f"No valid signal files found under: {memory_dir}")

    return signals


def normalize_label(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text or "unknown"


def advisory_strength(sig: Signal) -> float:
    """
    Advisory ranking signal, not a command score.
    Weighted toward evidence richness, confidence, and observed movement.
    """
    return (
        sig.opportunity_score * 0.45
        + sig.confidence * 10.0 * 0.20
        + min(sig.items_moved / 40.0, 10.0) * 0.20
        + min(sig.assisting_count / 4.0, 10.0) * 0.15
    )


def aggregate_staff(signals: List[Signal]) -> List[Dict]:
    by_staff: Dict[str, List[Signal]] = defaultdict(list)
    for sig in signals:
        by_staff[sig.staff_id].append(sig)

    rows: List[Dict] = []
    for staff_id, items in by_staff.items():
        total_items = sum(s.items_moved for s in items)
        total_assists = sum(s.assisting_count for s in items)
        avg_conf = sum(s.confidence for s in items) / len(items)
        avg_opp = sum(s.opportunity_score for s in items) / len(items)
        avg_strength = sum(advisory_strength(s) for s in items) / len(items)
        sections = sorted({normalize_label(s.section) for s in items})
        signal_types = sorted({normalize_label(s.signal_type) for s in items})

        rows.append(
            {
                "staff_id": staff_id,
                "signal_count": len(items),
                "total_items_moved": total_items,
                "total_assisting_count": total_assists,
                "avg_confidence": avg_conf,
                "avg_opportunity_score": avg_opp,
                "avg_advisory_strength": avg_strength,
                "sections": sections,
                "signal_types": signal_types,
                "latest_date": max(s.date for s in items if s.date),
            }
        )

    rows.sort(
        key=lambda r: (
            r["avg_advisory_strength"],
            r["total_items_moved"],
            r["total_assisting_count"],
        ),
        reverse=True,
    )
    return rows


def aggregate_sections(signals: List[Signal]) -> List[Dict]:
    by_section: Dict[str, List[Signal]] = defaultdict(list)
    for sig in signals:
        by_section[normalize_label(sig.section)].append(sig)

    rows: List[Dict] = []
    for section, items in by_section.items():
        total_items = sum(s.items_moved for s in items)
        total_assists = sum(s.assisting_count for s in items)
        avg_conf = sum(s.confidence for s in items) / len(items)
        avg_opp = sum(s.opportunity_score for s in items) / len(items)
        avg_strength = sum(advisory_strength(s) for s in items) / len(items)
        staff_count = len({s.staff_id for s in items})
        rows.append(
            {
                "section": section,
                "signal_count": len(items),
                "staff_count": staff_count,
                "total_items_moved": total_items,
                "total_assisting_count": total_assists,
                "avg_confidence": avg_conf,
                "avg_opportunity_score": avg_opp,
                "avg_advisory_strength": avg_strength,
            }
        )

    rows.sort(
        key=lambda r: (
            r["avg_advisory_strength"],
            r["total_items_moved"],
            r["total_assisting_count"],
        ),
        reverse=True,
    )
    return rows


def aggregate_products(signals: List[Signal]) -> List[Dict]:
    by_product: Dict[str, List[Signal]] = defaultdict(list)
    for sig in signals:
        product_bucket = normalize_label(sig.products)
        by_product[product_bucket].append(sig)

    rows: List[Dict] = []
    for product, items in by_product.items():
        total_items = sum(s.items_moved for s in items)
        total_assists = sum(s.assisting_count for s in items)
        avg_conf = sum(s.confidence for s in items) / len(items)
        avg_opp = sum(s.opportunity_score for s in items) / len(items)
        avg_strength = sum(advisory_strength(s) for s in items) / len(items)
        rows.append(
            {
                "products": product,
                "signal_count": len(items),
                "total_items_moved": total_items,
                "total_assisting_count": total_assists,
                "avg_confidence": avg_conf,
                "avg_opportunity_score": avg_opp,
                "avg_advisory_strength": avg_strength,
            }
        )

    rows.sort(
        key=lambda r: (
            r["avg_advisory_strength"],
            r["total_items_moved"],
            r["total_assisting_count"],
        ),
        reverse=True,
    )
    return rows


def build_opportunity_insights(
    top_staff: List[Dict],
    top_sections: List[Dict],
    top_products: List[Dict],
) -> List[str]:
    insights: List[str] = []

    if top_sections:
        sec = top_sections[0]
        insights.append(
            f"Highest current opportunity concentration appears in section '{sec['section']}', "
            f"with {sec['signal_count']} signals across {sec['staff_count']} staff, "
            f"{int(sec['total_items_moved'])} items moved, and {int(sec['total_assisting_count'])} assists observed."
        )

    if top_products:
        prod = top_products[0]
        insights.append(
            f"Strongest product-zone pattern currently appears in '{prod['products']}', "
            f"supported by {prod['signal_count']} signals and an average advisory strength of "
            f"{prod['avg_advisory_strength']:.2f}."
        )

    if top_staff:
        staff = top_staff[0]
        insights.append(
            f"Most evidence-rich contributor pattern currently appears around {staff['staff_id']}, "
            f"with {staff['signal_count']} signals, {int(staff['total_items_moved'])} items moved, "
            f"and {int(staff['total_assisting_count'])} assists."
        )

    return insights


def build_advisory_recommendations(
    top_staff: List[Dict],
    top_sections: List[Dict],
    top_products: List[Dict],
    all_signals: List[Signal],
    limit: int,
) -> List[str]:
    recs: List[str] = []

    if top_sections:
        for sec in top_sections[: min(limit, 3)]:
            recs.append(
                f"Review section '{sec['section']}' for possible revenue capture improvement. "
                f"Observed pattern: {sec['signal_count']} signals, {int(sec['total_items_moved'])} items moved, "
                f"{int(sec['total_assisting_count'])} assists, confidence {sec['avg_confidence']:.2f}."
            )

    if top_products:
        for prod in top_products[: min(limit, 2)]:
            recs.append(
                f"Monitor product zone '{prod['products']}' for stock-depth, placement, or visibility opportunity. "
                f"Observed pattern: average opportunity {prod['avg_opportunity_score']:.2f}, "
                f"average advisory strength {prod['avg_advisory_strength']:.2f}."
            )

    if top_staff:
        for staff in top_staff[: min(limit, 3)]:
            recs.append(
                f"Observe and document repeatable behaviors associated with {staff['staff_id']} in sections "
                f"{', '.join(staff['sections'])}. This is an advisory pattern-recognition recommendation only, "
                f"not a staff score or HR action."
            )

    # Identify lower-confidence areas for more observation, not punishment
    weak_candidates: List[Tuple[str, float, int]] = []
    by_section: Dict[str, List[Signal]] = defaultdict(list)
    for sig in all_signals:
        by_section[normalize_label(sig.section)].append(sig)

    for section, items in by_section.items():
        avg_strength = sum(advisory_strength(s) for s in items) / len(items)
        weak_candidates.append((section, avg_strength, len(items)))

    weak_candidates.sort(key=lambda x: (x[1], x[2]))
    for section, avg_strength, count in weak_candidates[: min(limit, 2)]:
        recs.append(
            f"Increase observation on section '{section}' before making human decisions. "
            f"Current evidence is weaker relative to other zones (signals={count}, "
            f"avg advisory strength={avg_strength:.2f})."
        )

    return recs


def build_report(
    signals: List[Signal],
    top_staff: List[Dict],
    top_sections: List[Dict],
    top_products: List[Dict],
    insights: List[str],
    recs: List[str],
    limit: int,
) -> str:
    dates = sorted({s.date for s in signals if s.date})
    sources = sorted({s.source_name for s in signals if s.source_name})

    lines: List[str] = []
    lines.append(f"{POST_PREFIX}")
    lines.append("")
    lines.append("# IOI Colony Recommendations")
    lines.append("")
    lines.append("## Compliance Note")
    lines.append(
        "This report is advisory only. It does not execute actions, issue commands, "
        "score staff for control, or commit the organisation to any opportunity."
    )
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Signals analyzed: {len(signals)}")
    lines.append(f"- Dates covered: {dates[0]} to {dates[-1]}" if dates else "- Dates covered: unknown")
    lines.append(f"- Sources observed: {', '.join(sources) if sources else 'unknown'}")
    lines.append("")

    lines.append("## Top Opportunity Insights")
    if insights:
        for item in insights:
            lines.append(f"- {item}")
    else:
        lines.append("- No major opportunity insight detected from current memory.")
    lines.append("")

    lines.append("## Top Contributor Patterns")
    if top_staff:
        for row in top_staff[:limit]:
            lines.append(
                f"- {row['staff_id']}: signals={row['signal_count']}, "
                f"items_moved={int(row['total_items_moved'])}, assists={int(row['total_assisting_count'])}, "
                f"avg_strength={row['avg_advisory_strength']:.2f}, sections={', '.join(row['sections'])}"
            )
    else:
        lines.append("- No contributor patterns available.")
    lines.append("")

    lines.append("## Top Section Patterns")
    if top_sections:
        for row in top_sections[:limit]:
            lines.append(
                f"- {row['section']}: signals={row['signal_count']}, staff={row['staff_count']}, "
                f"items_moved={int(row['total_items_moved'])}, assists={int(row['total_assisting_count'])}, "
                f"avg_strength={row['avg_advisory_strength']:.2f}"
            )
    else:
        lines.append("- No section patterns available.")
    lines.append("")

    lines.append("## Top Product-Zone Patterns")
    if top_products:
        for row in top_products[:limit]:
            lines.append(
                f"- {row['products']}: signals={row['signal_count']}, "
                f"items_moved={int(row['total_items_moved'])}, assists={int(row['total_assisting_count'])}, "
                f"avg_strength={row['avg_advisory_strength']:.2f}"
            )
    else:
        lines.append("- No product-zone patterns available.")
    lines.append("")

    lines.append("## Advisory Recommendations")
    if recs:
        for item in recs:
            lines.append(f"- {item}")
    else:
        lines.append("- No recommendations generated.")
    lines.append("")

    lines.append("## Governance Reminder")
    lines.append("- The colony informs. Humans decide.")
    lines.append("- This output must not be interpreted as an instruction or automatic decision.")
    lines.append("")

    return "\n".join(lines)


def main() -> int:
    args = parse_args()

    rules_file = Path(args.rules_file)
    memory_dir = Path(args.memory_dir)

    ensure_governance(rules_file)
    signals = load_signals(memory_dir, min_confidence=args.min_confidence, verbose=args.verbose)

    top_staff = aggregate_staff(signals)
    top_sections = aggregate_sections(signals)
    top_products = aggregate_products(signals)

    insights = build_opportunity_insights(top_staff, top_sections, top_products)
    recs = build_advisory_recommendations(
        top_staff=top_staff,
        top_sections=top_sections,
        top_products=top_products,
        all_signals=signals,
        limit=args.limit,
    )

    report = build_report(
        signals=signals,
        top_staff=top_staff,
        top_sections=top_sections,
        top_products=top_products,
        insights=insights,
        recs=recs,
        limit=args.limit,
    )

    if args.output:
        out = Path(args.output)
        out.write_text(report + "\n", encoding="utf-8")
        print(f"WROTE ADVISORY REPORT: {out}")
    else:
        print(report)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
