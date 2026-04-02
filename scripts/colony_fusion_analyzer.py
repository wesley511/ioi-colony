#!/usr/bin/env python3
"""
IOI Colony Fusion Analyzer

Purpose
-------
Fuse staff-performance signals with normalized sales signals into a single
branch-level intelligence report.

Key design goals
----------------
- Backward-compatible `load_signals(...)` shim
- Safe parsing of markdown / yaml / json-like signal files
- Advisory-only output
- Tolerant of partially structured legacy files
- No dependency on sibling module symbol resolution

Usage
-----
python3 scripts/colony_fusion_analyzer.py
python3 scripts/colony_fusion_analyzer.py --min-confidence 0.6
"""

from __future__ import annotations

import argparse
import json
import re
import sys

from pathlib import Path
import yaml

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from scripts.branch_resolution import legacy_branch_display, legacy_branch_stem, resolve_branch_slug
except ModuleNotFoundError:
    from branch_resolution import legacy_branch_display, legacy_branch_stem, resolve_branch_slug
try:
    from scripts.section_normalizer import normalize_section_name as shared_normalize_section_name
except ModuleNotFoundError:
    from section_normalizer import normalize_section_name as shared_normalize_section_name
try:
    from scripts.staff_signal_loader import dedupe_staff_signals
except ModuleNotFoundError:
    from staff_signal_loader import dedupe_staff_signals
try:
    from scripts.section_mapper import (
        build_section_metrics,
        load_branch_sections,
        normalize_branch_name,
    )
except ModuleNotFoundError:
    from section_mapper import (
        build_section_metrics,
        load_branch_sections,
        normalize_branch_name,
    )
try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None


STAFF_SIGNALS_DIR = Path("COLONY_MEMORY/staff_signals")
NORMALIZED_SIGNALS_DIR = Path("SIGNALS/normalized")
REPORTS_DIR = Path("REPORTS")

DEFAULT_MIN_CONFIDENCE = 0.0
TOP_N = 5
branch_sections = load_branch_sections()


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def utc_now_stamp() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")


def parse_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return default

    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return default

    try:
        return float(match.group(0))
    except ValueError:
        return default


def parse_int(value: Any, default: int = 0) -> int:
    return int(round(parse_float(value, default)))


def normalize_token(text: str) -> str:
    text = str(text or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^\w\s/-]+", " ", text)
    text = re.sub(r"[/\s\-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def canonical_branch(name: str) -> str:
    return resolve_branch_slug(candidates=[name])


def is_weak_placeholder_section(section: str) -> bool:
    s = normalize_token(section)
    if not s:
        return True
    if s in {"unknown", "unknown_section", "na", "n_a", "nil"}:
        return True
    if re.fullmatch(r"\d+", s):
        return True
    return False


def normalize_section_name(section: str) -> str:
    normalized = shared_normalize_section_name(section)
    if normalized:
        return normalized
    s = normalize_token(section)
    s = re.sub(r"^\d+_", "", s)
    s = re.sub(r"(?:_)+", "_", s).strip("_")
    return s or "unknown"


def safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1").strip()


def maybe_yaml_load(text: str) -> Any:
    if not text:
        return None

    # JSON first
    try:
        return json.loads(text)
    except Exception:
        pass

    # YAML if available
    if yaml is not None:
        try:
            return yaml.safe_load(text)
        except Exception:
            return None

    return None


def parse_kv_lines(text: str) -> dict[str, Any]:
    """
    Parse loose markdown / text lines like:
    - key: value
    key: value
    KEY = value
    """
    data: dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip().lstrip("-").strip()
        if not line or line.startswith("#"):
            continue

        match = re.match(r"^([A-Za-z0-9_ /.-]+)\s*[:=]\s*(.+)$", line)
        if match:
            key = normalize_token(match.group(1))
            value = match.group(2).strip()
            data[key] = value

    return data


def infer_branch_from_filename(path: Path) -> str:
    return resolve_branch_slug(path=path, candidates=[path.stem])


def infer_date_from_filename(path: Path) -> str | None:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    if match:
        return match.group(1)
    return None


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class StaffSignal:
    branch: str
    staff_id: str
    staff_name: str
    section: str
    strength: float
    confidence: float
    timestamp: str | None = None
    source_file: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class SalesSignal:
    branch: str
    sales_value: float
    transaction_count: int
    cash_sales: float
    eftpos_sales: float
    timestamp: str | None = None
    source_file: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def branch_from_staff_id(staff_id: str) -> str:
    token = normalize_token(staff_id)
    parts = token.split("-")
    if len(parts) >= 3 and parts[0] == "staff":
        return resolve_branch_slug(candidates=[parts[1]])
    return ""

def parse_signal_file(text: str, path: Path) -> dict[str, Any] | None:
    """
    Parse a staff signal file into a dict.
    Supports yaml/json and loose markdown key-value formats.
    """
    structured = maybe_yaml_load(text)
    data: dict[str, Any] = {}

    if isinstance(structured, dict):
        data = {normalize_token(k): v for k, v in structured.items()}
    else:
        data = parse_kv_lines(text)

    if not data and text:
        # last-resort extraction from free-form markdown
        staff_name_match = re.search(r"staff[_ ]name\s*[:=]\s*(.+)", text, flags=re.I)
        section_match = re.search(r"section\s*[:=]\s*(.+)", text, flags=re.I)
        strength_match = re.search(r"strength\s*[:=]\s*([0-9.]+)", text, flags=re.I)
        confidence_match = re.search(r"confidence\s*[:=]\s*([0-9.]+)", text, flags=re.I)

        if staff_name_match:
            data["staff_name"] = staff_name_match.group(1).strip()
        if section_match:
            data["section"] = section_match.group(1).strip()
        if strength_match:
            data["strength"] = strength_match.group(1).strip()
        if confidence_match:
            data["confidence"] = confidence_match.group(1).strip()

    if not data:
        return None

    staff_id_hint = str(
        data.get("staff_id")
        or data.get("staff_slug")
        or data.get("signal_id")
        or ""
    ).strip()

    branch = resolve_branch_slug(
        data,
        path=path,
        candidates=[
            data.get("source_slug"),
            data.get("branch"),
            branch_from_staff_id(staff_id_hint),
            data.get("shop"),
            data.get("branch_name"),
            data.get("source_name"),
            infer_branch_from_filename(path),
        ],
    )

    staff_name = str(
        data.get("staff_name")
        or data.get("name")
        or data.get("staff")
        or data.get("employee")
        or ""
    ).strip()

    staff_id = str(
        data.get("staff_id")
        or data.get("staff_slug")
        or data.get("signal_id")
        or normalize_token(staff_name)
        or path.stem
    ).strip()

    section = normalize_section_name(
        str(
            data.get("section")
            or data.get("product_section")
            or data.get("section_name")
            or data.get("area")
            or "unknown"
        )
    )

    strength = parse_float(
        data.get("strength")
        or data.get("signal_strength")
        or data.get("advisory_strength")
        or data.get("score")
        or data.get("final_score")
        or data.get("opportunity_score"),
        -1.0,
    )

    if strength < 0:
        items_moved = parse_float(data.get("items_moved"), 0.0)
        assisting_count = parse_float(data.get("assisting_count"), 0.0)
        arrangement = parse_float(data.get("arrangement"), 0.0)
        display = parse_float(data.get("display"), 0.0)
        performance = parse_float(data.get("performance"), 0.0)
        strength = (
            arrangement * 2.4
            + display * 2.4
            + performance * 2.4
            + items_moved * 1.6
            + assisting_count * 1.2
        )

    confidence = parse_float(data.get("confidence"), 0.5)
    timestamp = str(
        data.get("timestamp")
        or data.get("signal_date")
        or data.get("date")
        or ""
    ).strip() or None

    return {
        "branch": branch,
        "staff_id": staff_id,
        "staff_name": staff_name or staff_id,
        "section": section,
        "strength": strength,
        "confidence": confidence,
        "timestamp": timestamp,
        "source_file": str(path),
        "signal_type": "staff",
        "raw": data,
    }


def parse_staff_signal(path: Path) -> StaffSignal | None:
    text = safe_read_text(path)
    parsed = parse_signal_file(text, path)
    if not parsed:
        return None

    return StaffSignal(
        branch=parsed["branch"],
        staff_id=parsed["staff_id"],
        staff_name=parsed["staff_name"],
        section=parsed["section"],
        strength=parsed["strength"],
        confidence=parsed["confidence"],
        timestamp=parsed["timestamp"],
        source_file=parsed["source_file"],
        raw=parsed["raw"],
    )


def parse_sales_file(path: Path) -> SalesSignal | None:
    text = safe_read_text(path)
    structured = maybe_yaml_load(text)
    data: dict[str, Any] = {}

    if isinstance(structured, dict):
        data = {normalize_token(k): v for k, v in structured.items()}
    else:
        data = parse_kv_lines(text)

    if not data:
        return None

    branch = resolve_branch_slug(
        data,
        path=path,
        candidates=[
            data.get("branch"),
            data.get("shop"),
            data.get("branch_name"),
            data.get("source_name"),
            infer_branch_from_filename(path),
        ],
    )

    totals = data.get("totals")
    if not isinstance(totals, dict):
        totals = {}

    customers = data.get("customers")
    if not isinstance(customers, dict):
        customers = {}

    sales_value = parse_float(
        data.get("sales_value")
        or data.get("total_sales")
        or data.get("net_sales")
        or data.get("z_reading")
        or totals.get("sales")
        or totals.get("total"),
        0.0,
    )

    cash_sales = parse_float(
        data.get("cash_sales")
        or totals.get("cash"),
        0.0,
    )

    eftpos_sales = parse_float(
        data.get("eftpos_sales")
        or data.get("card_sales")
        or totals.get("card"),
        0.0,
    )

    transaction_count = parse_int(
        data.get("transaction_count")
        or data.get("transactions")
        or data.get("num_transactions")
        or customers.get("served")
        or customers.get("traffic"),
        0,
    )

    timestamp = str(
        data.get("date")
        or data.get("timestamp")
        or data.get("recorded_at")
        or infer_date_from_filename(path)
        or ""
    ).strip() or None

    return SalesSignal(
        branch=branch,
        sales_value=sales_value,
        transaction_count=transaction_count,
        cash_sales=cash_sales,
        eftpos_sales=eftpos_sales,
        timestamp=timestamp,
        source_file=str(path),
        raw=data,
    )


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_signals(
    signals_dir: Path = STAFF_SIGNALS_DIR,
    min_confidence: float = 0.0,
    **kwargs,
) -> list[dict]:
    """
    Backward-compatible loader expected by legacy fusion code.

    Supports:
    - min_confidence
    - ignored legacy kwargs via **kwargs

    Returns list[dict] rather than dataclasses to preserve compatibility.
    """
    signals: list[dict] = []
    candidate_paths: list[Path] = []
    if NORMALIZED_SIGNALS_DIR.exists():
        candidate_paths.extend(sorted(NORMALIZED_SIGNALS_DIR.glob("*staff*.md")))
    if not candidate_paths and signals_dir.exists():
        candidate_paths.extend(sorted(signals_dir.glob("*")))

    def _parser(path: Path) -> dict[str, Any] | None:
        if path.suffix.lower() not in {".md", ".txt", ".yaml", ".yml", ".json"}:
            return None
        parsed = parse_staff_signal(path)
        if not parsed:
            return None
        if min_confidence and min_confidence > 0 and parsed.confidence < min_confidence:
            return None
        return {
            "branch": parsed.branch,
            "staff_id": parsed.staff_id,
            "staff_name": parsed.staff_name,
            "section": parsed.section,
            "strength": parsed.strength,
            "confidence": parsed.confidence,
            "timestamp": parsed.timestamp,
            "source_file": parsed.source_file,
            "raw": parsed.raw,
        }

    for parsed in dedupe_staff_signals(candidate_paths, _parser):
        section = normalize_section_name(str(parsed.get("section") or ""))
        if parsed.get("branch") == "unknown" or str(parsed.get("staff_name")).strip().lower() == "unknown_staff":
            continue
        if is_weak_placeholder_section(section):
            continue
        parsed["section"] = section
        signals.append(parsed)

    return signals


def load_sales_signals(normalized_dir: Path = NORMALIZED_SIGNALS_DIR) -> list[dict]:
    chosen: dict[str, dict] = {}

    if not normalized_dir.exists():
        return []

    for path in sorted(normalized_dir.glob("*_sales_*.yaml")):
        parsed = parse_sales_file(path)
        if not parsed or parsed.branch == "unknown":
            continue

        current = {
            "branch": parsed.branch,
            "sales_value": parsed.sales_value,
            "transaction_count": parsed.transaction_count,
            "cash_sales": parsed.cash_sales,
            "eftpos_sales": parsed.eftpos_sales,
            "timestamp": parsed.timestamp,
            "source_file": parsed.source_file,
            "raw": parsed.raw,
        }
        existing = chosen.get(parsed.branch)
        current_rank = (str(parsed.timestamp or ""), path.name)
        existing_rank = (
            str(existing.get("timestamp") or ""),
            Path(existing["source_file"]).name,
        ) if existing else ("", "")
        if existing is None or current_rank > existing_rank:
            chosen[parsed.branch] = current

    return list(chosen.values())


# ---------------------------------------------------------------------------
# Fusion logic
# ---------------------------------------------------------------------------

def top_items(score_map: dict[str, float], limit: int = TOP_N) -> list[tuple[str, float]]:
    return sorted(score_map.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]


def attach_bale_supply_metrics(branch_metrics: dict[str, Any], bale_summaries: dict[str, Any] | None = None) -> None:
    """
    Compatibility hook.
    Keeps older call patterns safe even if bale supply data is absent.
    """
    if not bale_summaries:
        return

    for branch, summary in bale_summaries.items():
        if branch not in branch_metrics:
            continue
        branch_metrics[branch]["supply_bales"] = summary


def build_branch_metrics(staff_signals: list[dict], sales_signals: list[dict]) -> dict[str, Any]:
    branch_metrics: dict[str, Any] = defaultdict(
        lambda: {
            "staff_signal_count": 0,
            "staff_strength_total": 0.0,
            "staff_strength_avg": 0.0,
            "staff_count": 0,
            "sales_signal_count": 0,
            "sales_total": 0.0,
            "cash_sales_total": 0.0,
            "eftpos_sales_total": 0.0,
            "transaction_total": 0,
            "section_strength": defaultdict(float),
            "staff_strength": defaultdict(float),
            "unresolved_sections": defaultdict(float),
            "weak_sections": set(),
            "issues": [],
            "recommendations": [],
            "fusion_score": 0.0,
        }
    )

    # Staff signals
    seen_staff_per_branch: dict[str, set[str]] = defaultdict(set)

    for signal in staff_signals:
        branch = resolve_branch_slug(signal, candidates=[signal.get("branch")])
        section = normalize_section_name(signal.get("section", "unknown"))
        strength = parse_float(signal.get("strength"), 0.0)
        staff_id = str(signal.get("staff_id") or signal.get("staff_name") or "unknown")
        confidence = parse_float(signal.get("confidence"), 1.0)

        weighted_strength = strength * max(confidence, 0.0)

        metrics = branch_metrics[branch]
        metrics["staff_signal_count"] += 1
        metrics["staff_strength_total"] += weighted_strength
        metrics["section_strength"][section] += weighted_strength
        metrics["staff_strength"][staff_id] += weighted_strength

        if staff_id not in seen_staff_per_branch[branch]:
            seen_staff_per_branch[branch].add(staff_id)
            metrics["staff_count"] += 1

        if is_weak_placeholder_section(section):
            metrics["unresolved_sections"][section] += weighted_strength

    # Sales signals
    for signal in sales_signals:
        branch = resolve_branch_slug(signal, candidates=[signal.get("branch")])
        metrics = branch_metrics[branch]
        metrics["sales_signal_count"] += 1
        metrics["sales_total"] += parse_float(signal.get("sales_value"), 0.0)
        metrics["cash_sales_total"] += parse_float(signal.get("cash_sales"), 0.0)
        metrics["eftpos_sales_total"] += parse_float(signal.get("eftpos_sales"), 0.0)
        metrics["transaction_total"] += parse_int(signal.get("transaction_count"), 0)

    # Derived metrics
    for branch, metrics in branch_metrics.items():
        count = metrics["staff_signal_count"]
        metrics["staff_strength_avg"] = metrics["staff_strength_total"] / count if count else 0.0

        # Weak sections: low strength or placeholder sections
        for section, score in metrics["section_strength"].items():
            if is_weak_placeholder_section(section):
                continue
            if score < 50:
                metrics["weak_sections"].add(section)

        for section in sorted(metrics["weak_sections"]):
            metrics["issues"].append(f"Weak section: {section}")
            metrics["recommendations"].append(
                f"Improve display, support, and engagement in {legacy_branch_stem(branch)} -> {section}"
            )

        # Fusion score
        metrics["fusion_score"] = (
            metrics["staff_strength_avg"] * 0.65
            + min(metrics["sales_total"] / 100.0, 500.0) * 0.25
            + min(metrics["transaction_total"], 200) * 0.10
        )

    return dict(branch_metrics)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def render_report(branch_metrics: dict[str, Any]) -> str:
    branches = sorted(
        branch_metrics.items(),
        key=lambda kv: (-parse_float(kv[1].get("fusion_score"), 0.0), kv[0]),
    )

    lines: list[str] = []
    lines.append("# IOI Colony Fusion Report")
    lines.append("")
    lines.append("Advisory only. The colony informs. Humans decide.")
    lines.append("")
    lines.append(f"Generated: {datetime.utcnow().isoformat()}Z")
    lines.append("")

    total_staff_signals = sum(m["staff_signal_count"] for _, m in branches)
    total_sales_signals = sum(m["sales_signal_count"] for _, m in branches)

    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Branches analyzed: {len(branches)}")
    lines.append(f"- Staff signals analyzed: {total_staff_signals}")
    lines.append(f"- Sales signals analyzed: {total_sales_signals}")
    lines.append("")

    if branches:
        top_performer = legacy_branch_display(branches[0][0])
        weakest_branch = legacy_branch_display(branches[-1][0])
        lines.append(f"- Top performer: {top_performer}")
        lines.append(f"- Weakest branch: {weakest_branch}")
        lines.append("")

    lines.append("## Branch Fusion Ranking")
    lines.append("")

    for idx, (branch, metrics) in enumerate(branches, start=1):
        lines.append(
            f"{idx}. {legacy_branch_display(branch)} | "
            f"fusion_score={metrics['fusion_score']:.2f} | "
            f"staff_avg={metrics['staff_strength_avg']:.2f} | "
            f"sales_total={metrics['sales_total']:.2f} | "
            f"transactions={metrics['transaction_total']}"
        )

    lines.append("")

    for branch, metrics in branches:
        lines.append(f"## {legacy_branch_display(branch)}")
        lines.append("")
        lines.append(f"- fusion_score: {metrics['fusion_score']:.2f}")
        lines.append(f"- staff_signal_count: {metrics['staff_signal_count']}")
        lines.append(f"- sales_signal_count: {metrics['sales_signal_count']}")
        lines.append(f"- staff_strength_avg: {metrics['staff_strength_avg']:.2f}")
        lines.append(f"- sales_total: {metrics['sales_total']:.2f}")
        lines.append(f"- cash_sales_total: {metrics['cash_sales_total']:.2f}")
        lines.append(f"- eftpos_sales_total: {metrics['eftpos_sales_total']:.2f}")
        lines.append(f"- transaction_total: {metrics['transaction_total']}")
        lines.append("")

        strongest_staff = top_items(metrics["staff_strength"])


        lines.append("### Strongest Staff")
        lines.append("")
        if strongest_staff:
            for name, score in strongest_staff:
                lines.append(f"- {name}: {score:.2f}")
        else:
            lines.append("- none")
        lines.append("")

        lines.append("### Strongest Sections")
        lines.append("")
        sections = metrics.get("sections", {}) or {}

        if sections:
            ranked_sections = sorted(
                sections.items(),
                key=lambda x: (x[1].get("value", 0.0), x[1].get("qty", 0)),
                reverse=True,
            )
            for name, stats in ranked_sections[:5]:
                lines.append(
                    f"- {name}: value={stats.get('value', 0.0):.2f}, qty={stats.get('qty', 0)}"
                )
        else:
            lines.append("- none")
        lines.append("")

        lines.append("### Unresolved Sections")
        lines.append("")
        unresolved = {
            name: stats
            for name, stats in sections.items()
            if name in {"unmapped", "unknown"}
        }

        if unresolved:
            ranked_unresolved = sorted(
                unresolved.items(),
                key=lambda x: (x[1].get("value", 0.0), x[1].get("qty", 0)),
                reverse=True,
            )
            for name, stats in ranked_unresolved:
                lines.append(
                    f"- {name}: value={stats.get('value', 0.0):.2f}, qty={stats.get('qty', 0)}"
                )
        else:
            lines.append("- none")
        lines.append("")

        lines.append("### Issues Detected")
        lines.append("")
        if metrics["issues"]:
            for issue in metrics["issues"]:
                lines.append(f"- {issue}")
        else:
            lines.append("- none")
        lines.append("")

        lines.append("### Recommendations")
        lines.append("")
        if metrics["recommendations"]:
            for item in metrics["recommendations"]:
                lines.append(f"- {item}")
        else:
            lines.append("- none")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def save_report(
    report_text: str,
    report_dir: Path = REPORTS_DIR,
    output_path: Path | None = None,
) -> Path:

    # ✅ If cycle provides exact path → use it
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report_text, encoding="utf-8")
        return output_path

    # fallback → timestamped file
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"fusion_{utc_now_stamp()}.md"
    path.write_text(report_text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="IOI Colony fusion analyzer")

    parser.add_argument(
        "--staff-dir",
        type=Path,
        default=STAFF_SIGNALS_DIR,
        help="Directory containing staff signals",
    )

    parser.add_argument(
        "--sales-dir",
        type=Path,
        default=NORMALIZED_SIGNALS_DIR,
        help="Directory containing normalized sales signals",
    )

    parser.add_argument(
        "--report-dir",
        type=Path,
        default=REPORTS_DIR,
        help="Directory for fusion reports",
    )

    # ✅ ADD THIS (CRITICAL FIX)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Exact output file path for fusion report",
    )

    parser.add_argument(
        "--min-confidence",
        type=float,
        default=DEFAULT_MIN_CONFIDENCE,
        help="Minimum confidence for staff signal inclusion",
    )

    return parser

def generate_section_report(section_metrics: dict, output_path: str):
    lines = []
    lines.append("# SECTION INTELLIGENCE REPORT\n")

    for branch, sections in section_metrics.items():
        lines.append(f"\n## {branch}\n")

        sorted_sections = sorted(
            sections.items(),
            key=lambda x: x[1]["value"],
            reverse=True,
        )

        for section, stats in sorted_sections:
            lines.append(
                f"- {section}: qty={stats['qty']}, value={stats['value']:.2f}"
            )

    with open(output_path, "w") as f:
        f.write("\n".join(lines))

def load_bale_section_metrics() -> dict:
    """
    Load section totals from REPORTS/section_mapping_report.yaml
    and aggregate them by normalized branch.
    """
    report_path = Path("REPORTS/section_mapping_report.yaml")
    if not report_path.exists():
        return {}

    with report_path.open("r", encoding="utf-8") as f:
        reports = yaml.safe_load(f) or []

    section_metrics: dict[str, dict[str, dict]] = {}

    for report in reports:
        branch = normalize_branch_name(report.get("branch", ""))
        if not branch:
            continue

        branch_bucket = section_metrics.setdefault(branch, {})

        for sec in report.get("section_totals", []):
            section_name = str(sec.get("section_name", "")).strip() or "unknown"
            qty = int(sec.get("qty", 0) or 0)
            value = float(sec.get("value", 0) or 0)

            if section_name not in branch_bucket:
                branch_bucket[section_name] = {
                    "qty": 0,
                    "value": 0.0,
                    "count": 0,
                }

            branch_bucket[section_name]["qty"] += qty
            branch_bucket[section_name]["value"] += value
            branch_bucket[section_name]["count"] += 1

    return section_metrics

def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        staff_signals = load_signals(
            signals_dir=args.staff_dir,
            min_confidence=args.min_confidence,
        )
        sales_signals = load_sales_signals(args.sales_dir)

        branch_sections = load_branch_sections()

        signals = load_signals()

        section_metrics = load_bale_section_metrics()
        section_metrics = {
            normalize_branch_name(k): v
            for k, v in section_metrics.items()
        }

        branch_metrics = build_branch_metrics(staff_signals, sales_signals)
        attach_bale_supply_metrics(branch_metrics)

        for branch, data in branch_metrics.items():
            normalized_branch = normalize_branch_name(branch)
            data["sections"] = section_metrics.get(normalized_branch, {})

        report_text = render_report(branch_metrics)
        print("[IOI Colony Fusion] === COLONY FUSION REPORT ===")
        print("")
        print(report_text)

        saved_path = save_report(
            report_text,
            report_dir=args.report_dir,
            output_path=args.output,
        )
        print(f"Saved report to {saved_path}")
        return 0

    except Exception as exc:
        print(f"[IOI Colony Fusion] ERROR: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
