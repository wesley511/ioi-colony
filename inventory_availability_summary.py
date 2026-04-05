#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass, field
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
RAW_INPUT_DIR = ROOT / "RAW_INPUT"
REPORTS_DIR = ROOT / "REPORTS"
INVENTORY_REPORTS_DIR = REPORTS_DIR / "inventory"
SIGNALS_DIR = ROOT / "SIGNALS" / "normalized"


STATUS_STRENGTH_MAP: dict[str, float] = {
    "empty": 0.00,
    "very_tight": 0.15,
    "tight": 0.25,
    "packed": 0.25,
    "not_too_tight": 0.50,
    "partly_tight": 0.55,
    "moderate": 0.60,
    "partly_loose": 0.70,
    "loose": 0.80,
    "slack": 0.85,
    "well_stocked": 0.95,
    "unknown": 0.50,
}


STATUS_PATTERNS: list[tuple[str, str]] = [
    (r"\bempty\b", "empty"),
    (r"\bvery\s+tight\b", "very_tight"),
    (r"\bpacked\b", "packed"),
    (r"\bnormal\b", "moderate"),
    (r"\bnot\s+too\s+tight\b", "not_too_tight"),
    (r"\bpartly\s+tight\b", "partly_tight"),
    (r"\bpartly\s+loose\b", "partly_loose"),
    (r"\bwell\s+stocked\b", "well_stocked"),
    (r"\bslack\b", "slack"),
    (r"\bloose\b", "loose"),
    (r"\btight\b", "tight"),
    (r"\bmoderate\b", "moderate"),
]


@dataclass
class Diagnostic:
    severity: str
    code: str
    message: str


@dataclass
class SectionSignal:
    section: str
    raw_label: str
    status: str
    signal_strength: float
    evidence: str | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class InventoryAvailabilitySummary:
    branch: str
    report_date: str
    available: bool
    source_file: str | None
    events_count: int
    section_count: int
    avg_signal_strength: float | None
    sections: dict[str, dict[str, Any]]
    diagnostics: list[Diagnostic]
    recommended_actions: list[str]
    raw: dict[str, Any] = field(default_factory=dict)


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


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


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


def load_structured_file(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None

    text = path.read_text(encoding="utf-8", errors="ignore")
    suffix = path.suffix.lower()

    if suffix == ".json":
        return json.loads(text)

    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError(f"PyYAML is required to read YAML files: {path}")
        return yaml.safe_load(text)

    return None


def first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def find_operations_file(branch: str, report_date: str) -> Path | None:
    raw_ops_dir = RAW_INPUT_DIR / report_date / "operations"
    branch_slug = canonical_branch_slug(branch)
    for branch_path in branch_path_candidates(branch_slug):
        candidates = [
            raw_ops_dir / f"{branch_path}_supervisor_{report_date}.yaml",
            raw_ops_dir / f"{branch_path}_supervisor_{report_date}.yml",
            raw_ops_dir / f"{branch_path}_supervisor_{report_date}.json",
        ]
        direct = first_existing(candidates)
        if direct:
            return direct

        root_candidates = [
            SIGNALS_DIR / branch_path / f"{branch_path}_inventory_report_{report_date}.json",
            SIGNALS_DIR / branch_path / f"{branch_path}_supervisor_report_{report_date}.json",
        ]
        direct_root = first_existing(root_candidates)
        if direct_root:
            return direct_root

    if raw_ops_dir.exists():
        matches = sorted(raw_ops_dir.glob("*supervisor*.*"))
        for match in matches:
            if resolve_branch_slug(path=match, candidates=[match.stem]) == branch_slug:
                return match

    for branch_path in branch_path_candidates(branch_slug):
        norm_dir = SIGNALS_DIR / branch_path / report_date
        if norm_dir.exists():
            matches = sorted(norm_dir.glob("*operations*.*")) + sorted(norm_dir.glob("*supervisor*.*"))
            for match in matches:
                if resolve_branch_slug(path=match, candidates=[match.stem]) == branch_slug:
                    return match

    return None


def find_supervisor_context_file(branch: str, report_date: str, exclude: Path | None = None) -> Path | None:
    branch_slug = canonical_branch_slug(branch)
    for branch_path in branch_path_candidates(branch_slug):
        candidates = [
            SIGNALS_DIR / branch_path / f"{branch_path}_supervisor_report_{report_date}.json",
            SIGNALS_DIR / branch_path / report_date / f"{branch_path}_supervisor_report_{report_date}.json",
        ]
        for candidate in candidates:
            if not candidate.exists():
                continue
            if exclude is not None and candidate.resolve() == exclude.resolve():
                continue
            return candidate
    return None


def append_supervisor_context_diagnostics(
    diagnostics: list[Diagnostic],
    raw_payload: dict[str, Any],
    supervisor_path: Path | None,
) -> None:
    if supervisor_path is None or not supervisor_path.exists():
        return

    data = load_structured_file(supervisor_path)
    if not isinstance(data, dict):
        return

    exceptions = data.get("exceptions")
    if not isinstance(exceptions, list):
        return

    raw_payload.setdefault("supervisor_context", data)
    for item in exceptions:
        if not isinstance(item, dict):
            continue
        detail = str(item.get("details") or item.get("exception_type") or "supervisor exception").strip()
        action = str(item.get("action_taken") or "").strip()
        message = detail if not action else f"{detail}. Action taken: {action}."
        diagnostics.append(
            Diagnostic(
                severity="low",
                code="supervisor_control_exception",
                message=message,
            )
        )


def classify_status(text: str) -> str:
    value = text.strip().lower()
    for pattern, status in STATUS_PATTERNS:
        if re.search(pattern, value, flags=re.IGNORECASE):
            return status
    return "unknown"


def status_to_strength(status: str) -> float:
    return STATUS_STRENGTH_MAP.get(status, 0.50)


def infer_section_signals_from_text(text: str) -> list[SectionSignal]:
    """
    Parses free-form supervisor text lines like:
    Ladies Skirts: Slack/few on the rail
    Men T-Shirt: Tight
    Kids Shorts Original: Empty
    """
    signals: list[SectionSignal] = []
    seen: set[str] = set()

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue

        left, right = line.split(":", 1)
        left = left.strip(" -\t")
        right = right.strip()

        if not left or not right:
            continue

        section_slug = slugify(left)
        if not section_slug:
            continue

        status = classify_status(right)
        strength = status_to_strength(status)

        notes: list[str] = []
        if "few" in right.lower():
            notes.append("few_on_rail")
        if "big size" in right.lower() or "big sizes" in right.lower():
            notes.append("size_skew_big")
        if "easy for customers to flip" in right.lower():
            notes.append("easy_flip")
        if "mostly trousers" in right.lower():
            notes.append("mix_skew_trousers")

        dedupe_key = f"{section_slug}|{status}|{right.lower()}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        signals.append(
            SectionSignal(
                section=section_slug,
                raw_label=left,
                status=status,
                signal_strength=strength,
                evidence=right,
                notes=notes,
            )
        )

    return signals


def infer_section_signals_from_structured_ops(data: dict[str, Any]) -> list[SectionSignal]:
    signals: list[SectionSignal] = []

    for key in ("rail_status", "sections", "inventory_sections", "availability"):
        block = data.get(key)
        if isinstance(block, list):
            for row in block:
                if not isinstance(row, dict):
                    continue
                raw_label_str = str(row.get("section") or row.get("name") or "").strip()
                if not raw_label_str:
                    continue
                text = str(
                    row.get("status")
                    or row.get("availability")
                    or row.get("rail_status")
                    or row.get("note")
                    or row.get("notes")
                    or ""
                )
                status = classify_status(text)
                signals.append(
                    SectionSignal(
                        section=slugify(raw_label_str),
                        raw_label=raw_label_str,
                        status=status,
                        signal_strength=status_to_strength(status),
                        evidence=text or None,
                        notes=[],
                    )
                )
            continue
        if not isinstance(block, dict):
            continue

        for raw_label, payload in block.items():
            raw_label_str = str(raw_label)
            section_slug = slugify(raw_label_str)

            if isinstance(payload, dict):
                text = str(
                    payload.get("status")
                    or payload.get("availability")
                    or payload.get("rail_status")
                    or payload.get("note")
                    or payload.get("notes")
                    or ""
                )
            else:
                text = str(payload)

            status = classify_status(text)
            strength = status_to_strength(status)

            notes: list[str] = []
            if "few" in text.lower():
                notes.append("few_on_rail")

            signals.append(
                SectionSignal(
                    section=section_slug,
                    raw_label=raw_label_str,
                    status=status,
                    signal_strength=strength,
                    evidence=text or None,
                    notes=notes,
                )
            )

    return signals


def merge_section_signals(signals: list[SectionSignal]) -> dict[str, dict[str, Any]]:
    merged: dict[str, list[SectionSignal]] = {}

    for signal in signals:
        merged.setdefault(signal.section, []).append(signal)

    result: dict[str, dict[str, Any]] = {}
    for section, items in merged.items():
        avg_strength = round(
            sum(item.signal_strength for item in items) / len(items),
            4,
        )

        status_counts: dict[str, int] = {}
        notes: list[str] = []
        evidences: list[str] = []
        raw_labels: list[str] = []

        for item in items:
            status_counts[item.status] = status_counts.get(item.status, 0) + 1
            raw_labels.append(item.raw_label)
            if item.evidence:
                evidences.append(item.evidence)
            for note in item.notes:
                if note not in notes:
                    notes.append(note)

        dominant_status = max(status_counts.items(), key=lambda x: x[1])[0]

        result[section] = {
            "raw_labels": sorted(set(raw_labels)),
            "status": dominant_status,
            "signal_strength": avg_strength,
            "observations": len(items),
            "notes": notes,
            "evidence": evidences[:5],
        }

    return result


def summarize_availability(
    branch: str,
    report_date: str,
    source_path: Path | None,
) -> InventoryAvailabilitySummary:
    diagnostics: list[Diagnostic] = []
    signals: list[SectionSignal] = []
    raw_payload: dict[str, Any] = {}

    if source_path is None:
        diagnostics.append(
            Diagnostic(
                severity="medium",
                code="operations_source_missing",
                message="No operations/supervisor source file found for this branch/date.",
            )
        )
        return InventoryAvailabilitySummary(
            branch=branch,
            report_date=report_date,
            available=False,
            source_file=None,
            events_count=0,
            section_count=0,
            avg_signal_strength=None,
            sections={},
            diagnostics=diagnostics,
            recommended_actions=[
                "Provide or ingest a supervisor/operations report for this branch/date."
            ],
            raw={},
        )

    data = load_structured_file(source_path)
    if isinstance(data, dict):
        raw_payload = data

        source_branch = resolve_branch_slug(
            data,
            path=source_path,
            candidates=[
                data.get("branch"),
                data.get("source_name"),
            ],
        )
        if source_branch and source_branch != branch:
            diagnostics.append(
                Diagnostic(
                    severity="medium",
                    code="branch_mismatch",
                    message=f"operations source branch mismatch: expected {branch}, got {source_branch}.",
                )
            )

        signals.extend(infer_section_signals_from_structured_ops(data))

        notes_blob_parts: list[str] = []
        for key in ("operations_notes", "notes", "observations", "report_text"):
            value = data.get(key)
            if isinstance(value, list):
                notes_blob_parts.extend(str(v) for v in value)
            elif isinstance(value, str):
                notes_blob_parts.append(value)

        if notes_blob_parts:
            signals.extend(infer_section_signals_from_text("\n".join(notes_blob_parts)))

    else:
        text = source_path.read_text(encoding="utf-8", errors="ignore")
        raw_payload = {"raw_text_excerpt": text[:3000]}
        signals.extend(infer_section_signals_from_text(text))

    supplemental_supervisor = find_supervisor_context_file(branch, report_date, exclude=source_path)
    append_supervisor_context_diagnostics(diagnostics, raw_payload, supplemental_supervisor)

    merged_sections = merge_section_signals(signals)

    if not merged_sections:
        diagnostics.append(
            Diagnostic(
                severity="medium",
                code="no_section_signals_detected",
                message="Operations source loaded but no section-level inventory availability signals were detected.",
            )
        )
        return InventoryAvailabilitySummary(
            branch=branch,
            report_date=report_date,
            available=False,
            source_file=str(source_path),
            events_count=0,
            section_count=0,
            avg_signal_strength=None,
            sections={},
            diagnostics=diagnostics,
            recommended_actions=[
                "Improve supervisor report structure so section availability states are explicitly captured."
            ],
            raw=raw_payload,
        )

    section_strengths = [
        section_data["signal_strength"]
        for section_data in merged_sections.values()
        if isinstance(section_data.get("signal_strength"), (int, float))
    ]
    avg_signal_strength = (
        round(sum(section_strengths) / len(section_strengths), 4)
        if section_strengths
        else None
    )

    low_sections = [
        name for name, section_data in merged_sections.items()
        if float(section_data.get("signal_strength", 0.5)) <= 0.30
    ]
    high_sections = [
        name for name, section_data in merged_sections.items()
        if float(section_data.get("signal_strength", 0.5)) >= 0.80
    ]

    if low_sections:
        diagnostics.append(
            Diagnostic(
                severity="info",
                code="tight_or_empty_sections_detected",
                message=f"Detected tight/empty sections: {', '.join(low_sections[:8])}.",
            )
        )

    if high_sections:
        diagnostics.append(
            Diagnostic(
                severity="info",
                code="slack_or_loose_sections_detected",
                message=f"Detected slack/loose sections: {', '.join(high_sections[:8])}.",
            )
        )

    recommended_actions: list[str] = []
    if low_sections:
        recommended_actions.append(
            "Review tight/empty sections for replenishment or release-side follow-through."
        )
    if high_sections:
        recommended_actions.append(
            "Review slack/loose sections for pricing, merchandising, or demand weakness."
        )
    if not recommended_actions:
        recommended_actions.append(
            "Maintain supervisor reporting discipline so inventory availability remains observable."
        )

    return InventoryAvailabilitySummary(
        branch=branch,
        report_date=report_date,
        available=True,
        source_file=str(source_path),
        events_count=len(signals),
        section_count=len(merged_sections),
        avg_signal_strength=avg_signal_strength,
        sections=merged_sections,
        diagnostics=diagnostics,
        recommended_actions=recommended_actions,
        raw=raw_payload,
    )


def json_ready(obj: Any) -> Any:
    if isinstance(obj, list):
        return [json_ready(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): json_ready(v) for k, v in obj.items()}
    if hasattr(obj, "__dataclass_fields__"):
        return {k: json_ready(v) for k, v in asdict(obj).items()}
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    return obj


def render_markdown(summary: InventoryAvailabilitySummary) -> str:
    lines: list[str] = []
    lines.append("# Inventory Availability Summary")
    lines.append("")
    lines.append(f"- Branch: **{summary.branch}**")
    lines.append(f"- Report date: **{summary.report_date}**")
    lines.append(f"- Available: **{summary.available}**")
    lines.append(f"- Source file: **{summary.source_file or 'N/A'}**")
    lines.append(f"- Events count: **{summary.events_count}**")
    lines.append(f"- Section count: **{summary.section_count}**")
    lines.append(
        f"- Avg signal strength: **{summary.avg_signal_strength if summary.avg_signal_strength is not None else 'N/A'}**"
    )
    lines.append("")

    lines.append("## Sections")
    lines.append("")
    if summary.sections:
        for section, payload in sorted(summary.sections.items()):
            status = payload.get("status", "unknown")
            strength = payload.get("signal_strength", "N/A")
            labels = ", ".join(payload.get("raw_labels", [])) or section
            evidence_items = payload.get("evidence", [])
            evidence = " | ".join(evidence_items[:3]) if evidence_items else "N/A"
            lines.append(f"### {section}")
            lines.append(f"- Raw labels: **{labels}**")
            lines.append(f"- Status: **{status}**")
            lines.append(f"- Signal strength: **{strength}**")
            lines.append(f"- Evidence: **{evidence}**")
            notes = payload.get("notes", [])
            lines.append(f"- Notes: **{', '.join(notes) if notes else 'None'}**")
            lines.append("")
    else:
        lines.append("- None")
        lines.append("")

    lines.append("## Diagnostics")
    lines.append("")
    if summary.diagnostics:
        for diag in summary.diagnostics:
            lines.append(f"- [{diag.severity}] {diag.code}: {diag.message}")
    else:
        lines.append("- None")
    lines.append("")

    lines.append("## Recommended Actions")
    lines.append("")
    if summary.recommended_actions:
        for action in summary.recommended_actions:
            lines.append(f"- {action}")
    else:
        lines.append("- None")

    return "\n".join(lines).rstrip() + "\n"


def write_outputs(summary: InventoryAvailabilitySummary) -> tuple[Path, Path]:
    INVENTORY_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    stem = INVENTORY_REPORTS_DIR / f"{summary.branch}_{summary.report_date}_inventory_summary"

    json_path = stem.with_suffix(".json")
    md_path = stem.with_suffix(".md")

    json_path.write_text(
        json.dumps(json_ready(summary), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    md_path.write_text(render_markdown(summary), encoding="utf-8")
    return json_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate inventory availability summary from supervisor/operations data."
    )
    parser.add_argument("--branch", required=True, help="Canonical or alias branch name.")
    parser.add_argument("--date", required=True, help="Date, e.g. 2026-03-23 or 23/03/26")
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

    source_path = find_operations_file(branch, report_date)
    summary = summarize_availability(branch, report_date, source_path)
    json_path, md_path = write_outputs(summary)

    print(f"[inventory_availability_summary] JSON: {json_path}")
    print(f"[inventory_availability_summary] Markdown: {md_path}")

    if args.print_markdown:
        print(render_markdown(summary))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
