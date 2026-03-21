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
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


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
        help="Maximum number of top items per branch/section in the report (default: 5)",
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
    return (
        sig.opportunity_score * 0.6
        + sig.items_moved * 0.2
        + sig.assisting_count * 0.2
    )


def aggregate_by_branch(signals: List[Signal]) -> Dict[str, Dict]:
    branch_data: Dict[str, Dict] = defaultdict(
        lambda: {
            "total_strength": 0.0,
            "count": 0,
            "staff": defaultdict(float),
            "sections": defaultdict(float),
            "signals": [],
        }
    )

    for sig in signals:
        branch = normalize_label(sig.source_name or "unknown")
        section = normalize_label(sig.section or "unknown")
        staff = normalize_label(sig.staff_id or "unknown")
        strength = advisory_strength(sig)

        branch_data[branch]["total_strength"] += strength
        branch_data[branch]["count"] += 1
        branch_data[branch]["staff"][staff] += strength
        branch_data[branch]["sections"][section] += strength
        branch_data[branch]["signals"].append(sig)

    return branch_data


def rank_branches(branch_data: Dict[str, Dict]) -> List[Tuple[str, float]]:
    rankings: List[Tuple[str, float]] = []

    for branch, data in branch_data.items():
        avg = data["total_strength"] / max(data["count"], 1)
        rankings.append((branch, avg))

    return sorted(rankings, key=lambda x: x[1], reverse=True)


def detect_weak_sections(branch_data: Dict[str, Dict]) -> Dict[str, List[str]]:
    weak: Dict[str, List[str]] = {}

    for branch, data in branch_data.items():
        sections = data["sections"]
        if not sections:
            continue

        avg = sum(sections.values()) / len(sections)
        weak_sections = [sec for sec, val in sections.items() if val < avg * 0.6]

        if weak_sections:
            weak[branch] = sorted(weak_sections)

    return weak


def top_items(mapping: Dict[str, float], limit: int) -> List[Tuple[str, float]]:
    return sorted(mapping.items(), key=lambda x: x[1], reverse=True)[:limit]


def generate_report(
    rankings: List[Tuple[str, float]],
    weak_sections: Dict[str, List[str]],
    branch_data: Dict[str, Dict],
    limit: int,
) -> str:
    lines: List[str] = []
    lines.append(f"{POST_PREFIX} === COLONY INTELLIGENCE REPORT ===")
    lines.append("")

    lines.append(f"Branches analyzed: {len(rankings)}")
    lines.append(f"Signals analyzed: {sum(v['count'] for v in branch_data.values())}")
    lines.append("")

    if rankings:
        lines.append(f"TOP PERFORMER: {rankings[0][0].upper()}")
        lines.append(f"WEAKEST BRANCH: {rankings[-1][0].upper()}")
        lines.append("")

    lines.append("=== BRANCH RANKING ===")
    lines.append("")
    for i, (branch, score) in enumerate(rankings, 1):
        count = branch_data[branch]["count"]
        lines.append(f"{i}. {branch.upper()}  | avg_strength={score:.2f} | signals={count}")
    lines.append("")

    lines.append("=== BRANCH DETAIL ===")
    lines.append("")
    for branch, score in rankings:
        data = branch_data[branch]
        lines.append(f"{branch.upper()}")
        lines.append(f"- advisory_strength_avg: {score:.2f}")
        lines.append(f"- signal_count: {data['count']}")

        top_staff = top_items(data["staff"], limit)
        if top_staff:
            lines.append("- strongest_staff:")
            for staff_id, staff_score in top_staff:
                lines.append(f"  - {staff_id}: {staff_score:.2f}")

        top_sections = top_items(data["sections"], limit)
        if top_sections:
            lines.append("- strongest_sections:")
            for section, section_score in top_sections:
                lines.append(f"  - {section}: {section_score:.2f}")

        weak = weak_sections.get(branch, [])
        if weak:
            lines.append("- weak_sections:")
            for section in weak[:limit]:
                lines.append(f"  - {section}")

        lines.append("")

    lines.append("=== ISSUES DETECTED ===")
    lines.append("")
    if weak_sections:
        for branch, sections in weak_sections.items():
            lines.append(f"{branch.upper()}")
            for sec in sections[:limit]:
                lines.append(f"  - Weak section: {sec}")
            lines.append("")
    else:
        lines.append("No weak sections detected from current advisory thresholds.")
        lines.append("")

    lines.append("=== RECOMMENDATIONS ===")
    lines.append("")
    if weak_sections:
        for branch, sections in weak_sections.items():
            for sec in sections[:limit]:
                lines.append(f"- Improve display, support, and engagement in {branch} -> {sec}")
    else:
        lines.append("- Continue collecting more signals for stronger trend detection.")
        lines.append("- Maintain current observation cadence and review new weak areas daily.")

    lines.append("")
    lines.append(f"{POST_PREFIX} Advisory only. The colony informs. Humans decide.")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()

    ensure_governance(Path(args.rules_file))

    signals = load_signals(
        Path(args.memory_dir),
        args.min_confidence,
        args.verbose,
    )

    branch_data = aggregate_by_branch(signals)
    rankings = rank_branches(branch_data)
    weak_sections = detect_weak_sections(branch_data)

    report = generate_report(
        rankings=rankings,
        weak_sections=weak_sections,
        branch_data=branch_data,
        limit=args.limit,
    )

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
