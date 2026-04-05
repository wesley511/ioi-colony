#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
from dataclasses import dataclass, asdict, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

try:
    from scripts.branch_resolution import branch_path_candidates, canonical_branch_slug, resolve_branch_slug
except ModuleNotFoundError:
    from branch_resolution import branch_path_candidates, canonical_branch_slug, resolve_branch_slug


ROOT = Path(__file__).resolve().parent
SIGNALS_DIR = ROOT / "SIGNALS" / "normalized"
REPORTS_DIR = ROOT / "REPORTS"
INVENTORY_REPORTS_DIR = REPORTS_DIR / "inventory"
FUSION_REPORTS_DIR = REPORTS_DIR / "fusion"


@dataclass
class Diagnostic:
    severity: str
    code: str
    message: str


@dataclass
class SalesSnapshot:
    available: bool = False
    source_file: str | None = None
    total_sales: float | None = None
    cash: float | None = None
    card: float | None = None
    z_reading: float | None = None
    traffic: int | None = None
    conversion_rate_pct: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class InventorySummarySnapshot:
    available: bool = False
    source_file: str | None = None
    events_count: int = 0
    section_count: int = 0
    avg_signal_strength: float | None = None
    sections: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class InventoryReleaseSnapshot:
    available: bool = False
    source_file: str | None = None
    released_value: float | None = None
    released_qty: int | None = None
    bale_entries_count: int = 0
    raw_branch: str | None = None
    branch: str | None = None
    bales: list[dict[str, Any]] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class FusionResult:
    branch: str
    report_date: str
    data_completeness: str
    signal_families_available: int

    release_execution_score: float | None
    release_execution_band: str | None

    limited_fusion_score: float | None
    limited_fusion_band: str | None

    fusion_score: float | None
    fusion_band: str | None

    summary: str
    sales: SalesSnapshot
    inventory_summary: InventorySummarySnapshot
    inventory_release: InventoryReleaseSnapshot
    diagnostics: list[Diagnostic]
    recommended_actions: list[str]


def normalize_branch(value: str | None) -> str | None:
    if not value:
        return None
    normalized = canonical_branch_slug(value, fallback="")
    return normalized or None


def normalize_date(value: str) -> str:
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%y", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {value!r}")


def safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    text = text.replace("K", "").replace(",", "").replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def safe_int(value: Any) -> int | None:
    num = safe_float(value)
    if num is None:
        return None
    return int(round(num))


def load_structured_file(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8", errors="ignore")
    suffix = path.suffix.lower()

    if suffix == ".json":
        return json.loads(text)

    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError(
                f"PyYAML is required to read YAML files: {path}"
            )
        return yaml.safe_load(text)

    return None


def first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def glob_sorted(patterns: list[str]) -> list[Path]:
    matches: list[Path] = []
    for pattern in patterns:
        matches.extend(ROOT.glob(pattern))
    return sorted(set(matches))


def find_sales_file(branch: str, report_date: str) -> Path | None:
    branch_slug = canonical_branch_slug(branch)
    for branch_path in branch_path_candidates(branch_slug):
        base = SIGNALS_DIR / branch_path / report_date
        patterns = [
            base / f"{branch_path}_sales_{report_date}.yaml",
            base / f"{branch_path}_sales_{report_date}.yml",
            base / f"{branch_path}_sales_{report_date}.json",
            base / f"{branch_path}_daily_sales_{report_date}.yaml",
            base / f"{branch_path}_daily_sales_{report_date}.yml",
            base / f"{branch_path}_daily_sales_{report_date}.json",
        ]
        direct = first_existing(patterns)
        if direct:
            return direct

        candidates = sorted(base.glob("*sales*.*"))
        for candidate in candidates:
            if resolve_branch_slug(path=candidate, candidates=[candidate.stem]) == branch_slug:
                return candidate

        root_patterns = [
            SIGNALS_DIR / branch_path / f"{branch_path}_sales_report_{report_date}.json",
            SIGNALS_DIR / branch_path / f"{branch_path}_daily_sales_report_{report_date}.json",
        ]
        direct_root = first_existing(root_patterns)
        if direct_root:
            return direct_root

        raw_patterns = [
            ROOT / "RAW_INPUT" / report_date / "sales" / f"{branch_path}_sales_{report_date}.yaml",
            ROOT / "RAW_INPUT" / report_date / "sales" / f"{branch_path}_sales_{report_date}.yml",
            ROOT / "RAW_INPUT" / report_date / "sales" / f"{branch_path}_sales_{report_date}.json",
        ]
        direct_raw = first_existing(raw_patterns)
        if direct_raw:
            return direct_raw

    raw_dir = ROOT / "RAW_INPUT" / report_date / "sales"
    if raw_dir.exists():
        raw_candidates = sorted(raw_dir.glob("*sales*.*"))
        for candidate in raw_candidates:
            if resolve_branch_slug(path=candidate, candidates=[candidate.stem]) == branch_slug:
                return candidate

    return None


def find_inventory_summary_file(branch: str, report_date: str) -> Path | None:
    for branch_path in branch_path_candidates(branch):
        patterns = [
            INVENTORY_REPORTS_DIR / f"{branch_path}_{report_date}_inventory_summary.json",
            INVENTORY_REPORTS_DIR / f"{branch_path}_{report_date}_inventory_summary.yaml",
            INVENTORY_REPORTS_DIR / f"{branch_path}_{report_date}_inventory_summary.yml",
            INVENTORY_REPORTS_DIR / f"{branch_path}_{report_date}_inventory_summary.md",
        ]
        found = first_existing(patterns)
        if found:
            return found
    return None


def find_bale_release_file(branch: str, report_date: str) -> Path | None:
    branch_slug = canonical_branch_slug(branch)
    for branch_path in branch_path_candidates(branch_slug):
        base = SIGNALS_DIR / branch_path / report_date
        patterns = [
            base / f"{branch_path}_bale_release_{report_date}.json",
            base / f"{branch_path}_inventory_release_{report_date}.json",
            base / f"{branch_path}_bale_release_{report_date}.yaml",
            base / f"{branch_path}_bale_release_{report_date}.yml",
        ]
        direct = first_existing(patterns)
        if direct:
            return direct

        candidates = sorted(base.glob("*bale_release*.*"))
        for candidate in candidates:
            if resolve_branch_slug(path=candidate, candidates=[candidate.stem]) == branch_slug:
                return candidate

        root_patterns = [
            SIGNALS_DIR / branch_path / f"{branch_path}_bale_report_{report_date}.json",
        ]
        direct_root = first_existing(root_patterns)
        if direct_root:
            return direct_root
    return None


def extract_sales_snapshot(path: Path | None) -> SalesSnapshot:
    snapshot = SalesSnapshot()
    if path is None:
        return snapshot

    data = load_structured_file(path)
    if not isinstance(data, dict):
        return snapshot

    totals = data.get("totals", {}) if isinstance(data.get("totals"), dict) else {}
    sales_block = data.get("sales", {}) if isinstance(data.get("sales"), dict) else {}
    customers = data.get("customers", {}) if isinstance(data.get("customers"), dict) else {}
    traffic = data.get("traffic", {}) if isinstance(data.get("traffic"), dict) else {}
    performance = data.get("performance", {}) if isinstance(data.get("performance"), dict) else {}
    performance_metrics = (
        data.get("performance_metrics", {})
        if isinstance(data.get("performance_metrics"), dict)
        else {}
    )

    total_sales = (
        safe_float(totals.get("sales"))
        or safe_float(totals.get("total_sales"))
        or safe_float(sales_block.get("total_sales"))
        or safe_float(sales_block.get("sales"))
        or safe_float(data.get("total_sales"))
        or safe_float(data.get("sales_total"))
        or safe_float(data.get("sales"))
    )

    cash = (
        safe_float(totals.get("cash"))
        or safe_float(totals.get("cash_sales"))
        or safe_float(sales_block.get("cash"))
        or safe_float(data.get("cash"))
        or safe_float(data.get("cash_sales"))
    )

    card = (
        safe_float(totals.get("card"))
        or safe_float(totals.get("eftpos"))
        or safe_float(totals.get("eftpos_sales"))
        or safe_float(sales_block.get("card"))
        or safe_float(sales_block.get("eftpos"))
        or safe_float(data.get("card"))
        or safe_float(data.get("eftpos_sales"))
    )

    if total_sales is None and (cash is not None or card is not None):
        total_sales = round((cash or 0.0) + (card or 0.0), 2)

    z_reading = (
        safe_float(totals.get("z_reading"))
        or safe_float(sales_block.get("z_reading"))
        or safe_float(data.get("z_reading"))
    )

    traffic_count = (
        safe_int(customers.get("traffic"))
        or safe_int(customers.get("total_traffic"))
        or safe_int(customers.get("main_door"))
        or safe_int(traffic.get("total_customers"))
        or safe_int(traffic.get("customers_served"))
        or safe_int(data.get("traffic"))
    )

    conversion_rate_pct = (
        safe_float(performance.get("conversion_rate"))
        or safe_float(performance_metrics.get("conversion_rate"))
        or safe_float(data.get("conversion_rate"))
        or safe_float(data.get("conversion_rate_pct"))
    )

    snapshot.available = any(
        value is not None
        for value in [total_sales, cash, card, z_reading, traffic_count, conversion_rate_pct]
    )
    snapshot.source_file = str(path)
    snapshot.total_sales = total_sales
    snapshot.cash = cash
    snapshot.card = card
    snapshot.z_reading = z_reading
    snapshot.traffic = traffic_count
    snapshot.conversion_rate_pct = conversion_rate_pct
    snapshot.raw = data
    return snapshot


def parse_inventory_summary_markdown(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "events_count": 0,
        "section_count": 0,
        "avg_signal_strength": None,
        "sections": {},
    }

    patterns = {
        "events_count": r"Events count:\s*\*\*(\d+)\*\*|Events count:\s*(\d+)",
        "section_count": r"Section count:\s*\*\*(\d+)\*\*|Section count:\s*(\d+)",
        "avg_signal_strength": (
            r"Avg signal strength:\s*\*\*([0-9.]+)\*\*|"
            r"Avg signal strength:\s*([0-9.]+)"
        ),
    }

    for key, pattern in patterns.items():
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m:
            continue
        groups = [g for g in m.groups() if g is not None]
        if not groups:
            continue
        value = groups[0]
        if key in {"events_count", "section_count"}:
            result[key] = int(value)
        else:
            result[key] = float(value)

    return result


def extract_inventory_summary_snapshot(path: Path | None) -> InventorySummarySnapshot:
    snapshot = InventorySummarySnapshot()
    if path is None:
        return snapshot

    suffix = path.suffix.lower()

    if suffix in {".json", ".yaml", ".yml"}:
        data = load_structured_file(path)
        if isinstance(data, dict):
            sections = data.get("sections", {})
            if not isinstance(sections, dict):
                sections = {}
            snapshot.available = True
            snapshot.source_file = str(path)
            snapshot.events_count = safe_int(data.get("events_count")) or safe_int(
                data.get("event_count")
            ) or 0
            snapshot.section_count = safe_int(data.get("section_count")) or len(sections)
            snapshot.avg_signal_strength = safe_float(
                data.get("avg_signal_strength")
            ) or safe_float(data.get("average_signal_strength"))
            snapshot.sections = sections
            snapshot.raw = data
            return snapshot

    if suffix == ".md":
        text = path.read_text(encoding="utf-8", errors="ignore")
        parsed = parse_inventory_summary_markdown(text)
        snapshot.available = True
        snapshot.source_file = str(path)
        snapshot.events_count = int(parsed.get("events_count") or 0)
        snapshot.section_count = int(parsed.get("section_count") or 0)
        snapshot.avg_signal_strength = safe_float(parsed.get("avg_signal_strength"))
        snapshot.raw = {"markdown_source": True}
        return snapshot

    return snapshot


def extract_inventory_release_snapshot(
    normalized_release_file: Path | None,
    raw_bale_summary_file: Path | None,
    expected_branch: str,
) -> tuple[InventoryReleaseSnapshot, list[Diagnostic]]:
    snapshot = InventoryReleaseSnapshot()
    diagnostics: list[Diagnostic] = []

    source_path = normalized_release_file
    if source_path is None and raw_bale_summary_file is not None and raw_bale_summary_file.exists():
        source_path = raw_bale_summary_file

    if source_path is None:
        diagnostics.append(
            Diagnostic(
                severity="medium",
                code="inventory_release_missing",
                message="No bale release summary loaded, so release-side diagnostics are partial.",
            )
        )
        return snapshot, diagnostics

    if normalized_release_file and normalized_release_file.exists():
        data = load_structured_file(normalized_release_file)
        if isinstance(data, dict):
            bales = data.get("bales")
            if not isinstance(bales, list):
                bales = data.get("items")
            if not isinstance(bales, list):
                bales = []

            raw_branch = (
                data.get("branch")
                or data.get("source_slug")
                or data.get("branch_slug")
                or data.get("source_name")
            )
            norm_branch = resolve_branch_slug(data, path=normalized_release_file, candidates=[raw_branch])

            released_value = (
                safe_float(data.get("released_value"))
                or safe_float(data.get("total_amount"))
                or safe_float(data.get("amount"))
                or safe_float(data.get("value"))
            )
            released_qty = (
                safe_int(data.get("released_qty"))
                or safe_int(data.get("total_qty"))
                or safe_int(data.get("quantity"))
                or safe_int(data.get("qty"))
            )

            if released_value is None and bales:
                released_value = round(
                    sum(
                        safe_float(b.get("amount")) or safe_float(b.get("value")) or 0.0
                        for b in bales
                    ),
                    2,
                )
            if released_qty is None and bales:
                released_qty = sum(
                    safe_int(b.get("qty")) or safe_int(b.get("quantity")) or 0
                    for b in bales
                )

            snapshot.available = any(v is not None for v in [released_value, released_qty]) or bool(bales)
            snapshot.source_file = str(raw_bale_summary_file or normalized_release_file)
            snapshot.released_value = released_value
            snapshot.released_qty = released_qty
            snapshot.bale_entries_count = len(bales)
            snapshot.raw_branch = str(raw_branch) if raw_branch is not None else None
            snapshot.branch = norm_branch
            snapshot.bales = [b for b in bales if isinstance(b, dict)]
            snapshot.raw = data

            if snapshot.available and snapshot.bale_entries_count == 0:
                diagnostics.append(
                    Diagnostic(
                        severity="medium",
                        code="bale_entries_missing",
                        message="Bale summary parsed but no bale entries were detected.",
                    )
                )

            if norm_branch and norm_branch != expected_branch:
                diagnostics.append(
                    Diagnostic(
                        severity="medium",
                        code="bale_branch_mismatch",
                        message=f"bale summary branch mismatch: expected {expected_branch}, got {norm_branch}.",
                    )
                )

            if snapshot.available:
                diagnostics.append(
                    Diagnostic(
                        severity="info",
                        code="inventory_release_loaded",
                        message=(
                            f"Released inventory value={format_money(snapshot.released_value)}, "
                            f"qty={snapshot.released_qty if snapshot.released_qty is not None else 'N/A'}."
                        ),
                    )
                )

            return snapshot, diagnostics

    if raw_bale_summary_file is not None and raw_bale_summary_file.exists():
        text = raw_bale_summary_file.read_text(encoding="utf-8", errors="ignore")
        qty_match = re.search(r"total\s+qty[^0-9]*(\d+)", text, flags=re.IGNORECASE)
        amt_match = re.search(
            r"total\s+(?:amount|amt)[^0-9]*K?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
            text,
            flags=re.IGNORECASE,
        )

        snapshot.available = bool(qty_match or amt_match)
        snapshot.source_file = str(raw_bale_summary_file)
        snapshot.released_qty = int(qty_match.group(1)) if qty_match else None
        snapshot.released_value = safe_float(amt_match.group(1)) if amt_match else None
        snapshot.branch = resolve_branch_slug(path=raw_bale_summary_file, candidates=[raw_bale_summary_file.stem])

        if snapshot.available:
            diagnostics.append(
                Diagnostic(
                    severity="info",
                    code="inventory_release_raw_loaded",
                    message=(
                        f"Released inventory value={format_money(snapshot.released_value)}, "
                        f"qty={snapshot.released_qty if snapshot.released_qty is not None else 'N/A'}."
                    ),
                )
            )
        else:
            diagnostics.append(
                Diagnostic(
                    severity="medium",
                    code="inventory_release_raw_unparsed",
                    message="Raw bale summary exists but totals could not be parsed.",
                )
            )

    return snapshot, diagnostics


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def score_to_band(score: float | None, incomplete: bool = False) -> str | None:
    if incomplete:
        return "incomplete"
    if score is None:
        return None
    if score >= 0.85:
        return "strong"
    if score >= 0.65:
        return "good"
    if score >= 0.45:
        return "moderate"
    return "weak"


def format_money(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"K{value:,.2f}"


def score_inventory_release(release: InventoryReleaseSnapshot) -> float | None:
    if not release.available:
        return None

    qty_score = 0.0
    value_score = 0.0
    detail_score = 0.0

    if release.released_qty is not None:
        qty_score = clamp(release.released_qty / 800.0)
    if release.released_value is not None:
        value_score = clamp(release.released_value / 12000.0)
    if release.bale_entries_count > 0:
        detail_score = clamp(release.bale_entries_count / 12.0)

    weights = []
    values = []

    if release.released_qty is not None:
        weights.append(0.45)
        values.append(qty_score)
    if release.released_value is not None:
        weights.append(0.45)
        values.append(value_score)
    if release.bale_entries_count >= 0:
        weights.append(0.10)
        values.append(detail_score)

    if not weights:
        return None

    numerator = sum(w * v for w, v in zip(weights, values))
    denominator = sum(weights)
    return round(numerator / denominator, 4)


def score_limited_fusion(
    sales: SalesSnapshot,
    inv_summary: InventorySummarySnapshot,
    release: InventoryReleaseSnapshot,
) -> float | None:
    """
    Limited fusion is allowed only when at least two signal families exist.
    It is still not full fusion unless all three are present.
    """
    components: list[tuple[float, float]] = []

    if sales.available:
        sales_component = 0.0
        subweights = []
        subvalues = []

        if sales.total_sales is not None:
            subweights.append(0.45)
            subvalues.append(clamp(sales.total_sales / 20000.0))
        if sales.traffic is not None:
            subweights.append(0.20)
            subvalues.append(clamp(sales.traffic / 500.0))
        if sales.conversion_rate_pct is not None:
            subweights.append(0.20)
            subvalues.append(clamp(sales.conversion_rate_pct / 100.0))
        if sales.z_reading is not None:
            subweights.append(0.15)
            subvalues.append(clamp(sales.z_reading / 20000.0))

        if subweights:
            sales_component = sum(w * v for w, v in zip(subweights, subvalues)) / sum(subweights)
            components.append((0.45, sales_component))

    if inv_summary.available:
        inv_component = 0.0
        subweights = []
        subvalues = []

        if inv_summary.avg_signal_strength is not None:
            subweights.append(0.60)
            subvalues.append(clamp(inv_summary.avg_signal_strength))
        if inv_summary.section_count is not None:
            subweights.append(0.25)
            subvalues.append(clamp(inv_summary.section_count / 20.0))
        if inv_summary.events_count is not None:
            subweights.append(0.15)
            subvalues.append(clamp(inv_summary.events_count / 50.0))

        if subweights:
            inv_component = sum(w * v for w, v in zip(subweights, subvalues)) / sum(subweights)
            components.append((0.30, inv_component))

    release_component = score_inventory_release(release)
    if release_component is not None:
        components.append((0.25, release_component))

    if len(components) < 2:
        return None

    weighted_sum = sum(weight * score for weight, score in components)
    total_weight = sum(weight for weight, _ in components)
    return round(weighted_sum / total_weight, 4)


def build_summary_text(
    sales: SalesSnapshot,
    inv_summary: InventorySummarySnapshot,
    release: InventoryReleaseSnapshot,
) -> str:
    parts = []

    if sales.available:
        parts.append(f"sales={format_money(sales.total_sales)}")
    else:
        parts.append("sales=unavailable")

    if inv_summary.available:
        if inv_summary.avg_signal_strength is not None:
            parts.append(f"inventory_strength={inv_summary.avg_signal_strength:.2f}")
        else:
            parts.append("inventory_summary=available")
    else:
        parts.append("inventory_summary=unavailable")

    if release.available:
        parts.append(f"released_value={format_money(release.released_value)}")
        parts.append(
            f"released_qty={release.released_qty if release.released_qty is not None else 'N/A'}"
        )
    else:
        parts.append("inventory_release=unavailable")

    return " | ".join(parts)


def recommend_actions(
    sales: SalesSnapshot,
    inv_summary: InventorySummarySnapshot,
    release: InventoryReleaseSnapshot,
    diagnostics: list[Diagnostic],
) -> list[str]:
    actions: list[str] = []

    if not sales.available:
        actions.append(
            "Ingest the day-end sales report for this branch/date before treating the result as business fusion."
        )

    if not inv_summary.available:
        actions.append(
            "Generate the inventory availability summary under REPORTS/inventory for this branch/date."
        )

    if release.available and release.bale_entries_count == 0:
        actions.append(
            "Patch the bale release parser so per-bale entries are captured, not only top-level totals."
        )

    if any(d.code == "bale_branch_mismatch" for d in diagnostics):
        actions.append(
            "Add or tighten branch alias normalization so release branch names resolve to canonical branch slugs."
        )

    if sales.available and release.available and not inv_summary.available:
        actions.append(
            "Treat this as limited fusion only; do not use it as full operational truth until inventory availability exists."
        )

    return actions


def build_result(
    branch: str,
    report_date: str,
    sales: SalesSnapshot,
    inv_summary: InventorySummarySnapshot,
    release: InventoryReleaseSnapshot,
    diagnostics: list[Diagnostic],
) -> FusionResult:
    families_available = sum(
        1 for available in [sales.available, inv_summary.available, release.available] if available
    )

    release_execution_score = score_inventory_release(release)
    release_execution_band = score_to_band(release_execution_score)

    limited_fusion_score = None
    limited_fusion_band = None
    fusion_score = None
    fusion_band = None
    data_completeness = "incomplete"

    if families_available >= 2:
        limited_fusion_score = score_limited_fusion(sales, inv_summary, release)
        limited_fusion_band = score_to_band(limited_fusion_score)

    if sales.available and inv_summary.available and release.available:
        fusion_score = limited_fusion_score
        fusion_band = score_to_band(fusion_score)
        data_completeness = "full"
    elif families_available >= 2:
        data_completeness = "partial"
        fusion_band = "incomplete"
        diagnostics.append(
            Diagnostic(
                severity="medium",
                code="partial_fusion_only",
                message="At least two signal families are present, but full fusion is not allowed because one required family is still missing.",
            )
        )
    else:
        diagnostics.append(
            Diagnostic(
                severity="medium",
                code="insufficient_signal_families",
                message="Insufficient validated signal families for fusion scoring.",
            )
        )

    if not sales.available:
        diagnostics.append(
            Diagnostic(
                severity="medium",
                code="sales_missing",
                message="Daily sales event is unavailable for the requested branch/date.",
            )
        )

    if not inv_summary.available:
        diagnostics.append(
            Diagnostic(
                severity="medium",
                code="inventory_summary_missing",
                message="Inventory availability summary is unavailable for the requested branch/date.",
            )
        )

    summary = build_summary_text(sales, inv_summary, release)
    actions = recommend_actions(sales, inv_summary, release, diagnostics)

    return FusionResult(
        branch=branch,
        report_date=report_date,
        data_completeness=data_completeness,
        signal_families_available=families_available,
        release_execution_score=release_execution_score,
        release_execution_band=release_execution_band,
        limited_fusion_score=limited_fusion_score,
        limited_fusion_band=limited_fusion_band,
        fusion_score=fusion_score,
        fusion_band=fusion_band,
        summary=summary,
        sales=sales,
        inventory_summary=inv_summary,
        inventory_release=release,
        diagnostics=diagnostics,
        recommended_actions=actions,
    )


def result_to_json_ready(result: FusionResult) -> dict[str, Any]:
    def convert(obj: Any) -> Any:
        if isinstance(obj, list):
            return [convert(x) for x in obj]

        if isinstance(obj, dict):
            return {str(k): convert(v) for k, v in obj.items()}

        if hasattr(obj, "__dataclass_fields__"):
            return {k: convert(v) for k, v in asdict(obj).items()}

        if isinstance(obj, (date, datetime)):
            return obj.isoformat()

        if isinstance(obj, Path):
            return str(obj)

        return obj

    return convert(result)

def render_markdown(result: FusionResult) -> str:
    lines: list[str] = []
    lines.append("# Inventory + Sales Fusion Summary")
    lines.append("")
    lines.append(f"- Branch: **{result.branch}**")
    lines.append(f"- Report date: **{result.report_date}**")
    lines.append(f"- Data completeness: **{result.data_completeness}**")
    lines.append(f"- Signal families available: **{result.signal_families_available}**")
    lines.append(
        f"- Release execution score: **{result.release_execution_score if result.release_execution_score is not None else 'N/A'}**"
    )
    lines.append(f"- Release execution band: **{result.release_execution_band or 'N/A'}**")
    lines.append(
        f"- Limited fusion score: **{result.limited_fusion_score if result.limited_fusion_score is not None else 'N/A'}**"
    )
    lines.append(f"- Limited fusion band: **{result.limited_fusion_band or 'N/A'}**")
    lines.append(
        f"- Fusion score: **{result.fusion_score if result.fusion_score is not None else 'N/A'}**"
    )
    lines.append(f"- Fusion band: **{result.fusion_band or 'N/A'}**")
    lines.append(f"- Summary: **{result.summary}**")
    lines.append("")

    lines.append("## Sales")
    lines.append("")
    lines.append(f"- Available: **{result.sales.available}**")
    lines.append(f"- Source file: **{result.sales.source_file or 'N/A'}**")
    lines.append(f"- Total sales: **{format_money(result.sales.total_sales)}**")
    lines.append(f"- Cash: **{format_money(result.sales.cash)}**")
    lines.append(f"- Card: **{format_money(result.sales.card)}**")
    lines.append(f"- Z reading: **{format_money(result.sales.z_reading)}**")
    lines.append(f"- Traffic: **{result.sales.traffic if result.sales.traffic is not None else 'N/A'}**")
    lines.append(
        f"- Conversion rate: **{f'{result.sales.conversion_rate_pct:.2f}%' if result.sales.conversion_rate_pct is not None else 'N/A'}**"
    )
    lines.append("")

    lines.append("## Inventory Summary")
    lines.append("")
    lines.append(f"- Available: **{result.inventory_summary.available}**")
    lines.append(f"- Source file: **{result.inventory_summary.source_file or 'N/A'}**")
    lines.append(f"- Events count: **{result.inventory_summary.events_count}**")
    lines.append(f"- Section count: **{result.inventory_summary.section_count}**")
    lines.append(
        f"- Avg signal strength: **{result.inventory_summary.avg_signal_strength if result.inventory_summary.avg_signal_strength is not None else 'N/A'}**"
    )
    lines.append("")

    lines.append("## Inventory Release")
    lines.append("")
    lines.append(f"- Available: **{result.inventory_release.available}**")
    lines.append(f"- Source file: **{result.inventory_release.source_file or 'N/A'}**")
    lines.append(f"- Released value: **{format_money(result.inventory_release.released_value)}**")
    lines.append(
        f"- Released qty: **{result.inventory_release.released_qty if result.inventory_release.released_qty is not None else 'N/A'}**"
    )
    lines.append(f"- Bale entries detected: **{result.inventory_release.bale_entries_count}**")
    lines.append(
        f"- Parsed branch: **{result.inventory_release.branch or result.inventory_release.raw_branch or 'N/A'}**"
    )
    lines.append("")

    lines.append("## Diagnostics")
    lines.append("")
    if result.diagnostics:
        for diag in result.diagnostics:
            lines.append(f"- [{diag.severity}] {diag.code}: {diag.message}")
    else:
        lines.append("- None")
    lines.append("")

    lines.append("## Recommended Actions")
    lines.append("")
    if result.recommended_actions:
        for action in result.recommended_actions:
            lines.append(f"- {action}")
    else:
        lines.append("- None")

    return "\n".join(lines).rstrip() + "\n"


def write_outputs(result: FusionResult, output_stem: Path) -> tuple[Path, Path]:
    output_stem.parent.mkdir(parents=True, exist_ok=True)

    json_path = output_stem.with_suffix(".json")
    md_path = output_stem.with_suffix(".md")

    json_payload = result_to_json_ready(result)
    json_path.write_text(
        json.dumps(json_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    md_path.write_text(render_markdown(result), encoding="utf-8")
    return json_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate inventory + sales fusion summary with partial/full fusion safeguards."
    )
    parser.add_argument("--branch", required=True, help="Canonical or alias branch name.")
    parser.add_argument("--date", required=True, help="Report date. Examples: 2026-03-28 or 28/03/26")
    parser.add_argument(
        "--bale-summary-file",
        default=None,
        help="Optional raw bale summary input file used for provenance and fallback parsing.",
    )
    parser.add_argument(
        "--print-markdown",
        action="store_true",
        help="Print markdown report to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    branch = normalize_branch(args.branch)
    if not branch:
        raise SystemExit("ERROR: branch is required")

    report_date = normalize_date(args.date)

    sales_file = find_sales_file(branch, report_date)
    inv_summary_file = find_inventory_summary_file(branch, report_date)
    normalized_release_file = find_bale_release_file(branch, report_date)

    raw_bale_summary_file = Path(args.bale_summary_file).resolve() if args.bale_summary_file else None
    if raw_bale_summary_file is not None and not raw_bale_summary_file.exists():
        print(
            f"[WARNING] Bale summary file not found, continuing without it: {raw_bale_summary_file}"
        )
        raw_bale_summary_file = None

    sales = extract_sales_snapshot(sales_file)
    inv_summary = extract_inventory_summary_snapshot(inv_summary_file)
    release, release_diags = extract_inventory_release_snapshot(
        normalized_release_file=normalized_release_file,
        raw_bale_summary_file=raw_bale_summary_file,
        expected_branch=branch,
    )

    diagnostics = list(release_diags)

    result = build_result(
        branch=branch,
        report_date=report_date,
        sales=sales,
        inv_summary=inv_summary,
        release=release,
        diagnostics=diagnostics,
    )

    output_stem = FUSION_REPORTS_DIR / f"{branch}_{report_date}_inventory_sales_fusion_summary"
    json_path, md_path = write_outputs(result, output_stem)

    print(f"[inventory_sales_fusion_summary] JSON: {json_path}")
    print(f"[inventory_sales_fusion_summary] Markdown: {md_path}")

    if args.print_markdown:
        print(render_markdown(result))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
