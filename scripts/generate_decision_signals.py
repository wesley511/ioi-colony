#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    from scripts.branch_resolution import legacy_branch_stem, resolve_branch_slug
except ModuleNotFoundError:
    from branch_resolution import legacy_branch_stem, resolve_branch_slug

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = ROOT / "SIGNALS" / "normalized"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORTS_DIR = ROOT / "REPORTS"
DEFAULT_OUTPUT_DIR = ROOT / "SIGNALS" / "normalized"


POST_PREFIX = "[IOI Decision Signal Generator]"


@dataclass
class DecisionSignal:
    filename: str
    branch_slug: str
    title: str
    source: str
    date_identified: str
    description: str
    leverage_score: float
    risk_level: str
    status: str = "Active"


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "signal"


def latest_report(prefix: str) -> Path | None:
    files = sorted(REPORTS_DIR.glob(f"{prefix}_*.md"), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def extract_lines(text: str, start_marker: str, stop_markers: list[str]) -> list[str]:
    lines = text.splitlines()
    capture = False
    out: list[str] = []

    for line in lines:
        stripped = line.rstrip()

        if stripped.strip() == start_marker:
            capture = True
            continue

        if capture and stripped.strip() in stop_markers:
            break

        if capture:
            out.append(stripped)

    return out


def parse_branch_name(line: str) -> str | None:
    m = re.match(r"^([A-Z0-9_]+)\s*$", line.strip())
    return m.group(1) if m else None


def parse_weak_sections(advisory_text: str) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []

    lines = advisory_text.splitlines()
    current_branch: str | None = None
    in_issues = False

    for raw in lines:
        line = raw.rstrip()

        if line.strip() == "=== ISSUES DETECTED ===":
            in_issues = True
            current_branch = None
            continue

        if in_issues and line.strip().startswith("==="):
            break

        if not in_issues:
            continue

        branch = parse_branch_name(line)
        if branch:
            current_branch = branch
            continue

        m = re.match(r"^\s*-\s*Weak section:\s*(.+?)\s*$", line)
        if m and current_branch:
            section = m.group(1).strip()
            results.append((current_branch, section))

    return results


def parse_top_staff(advisory_text: str) -> list[tuple[str, str, float]]:
    results: list[tuple[str, str, float]] = []
    lines = advisory_text.splitlines()
    current_branch: str | None = None
    in_branch_detail = False
    in_staff = False

    for raw in lines:
        line = raw.rstrip()

        if line.strip() == "=== BRANCH DETAIL ===":
            in_branch_detail = True
            current_branch = None
            in_staff = False
            continue

        if in_branch_detail and line.strip() == "=== ISSUES DETECTED ===":
            break

        if not in_branch_detail:
            continue

        branch = parse_branch_name(line)
        if branch:
            current_branch = branch
            in_staff = False
            continue

        if line.strip() == "- strongest_staff:":
            in_staff = True
            continue

        if line.strip().startswith("- strongest_sections:"):
            in_staff = False
            continue

        if in_staff and current_branch:
            m = re.match(r"^\s*-\s*([a-zA-Z0-9_\-]+):\s*([0-9.]+)\s*$", line)
            if m:
                staff_name = m.group(1).strip()
                score = float(m.group(2))
                results.append((current_branch, staff_name, score))

    return results


def parse_fusion_summary(fusion_text: str) -> tuple[str | None, str | None]:
    top_branch = None
    weak_branch = None

    for line in fusion_text.splitlines():
        m_top = re.match(r"^\s*-\s*Top performer:\s*([A-Z0-9_]+)\s*$", line)
        if m_top:
            top_branch = m_top.group(1).strip()

        m_weak = re.match(r"^\s*-\s*Weakest branch:\s*([A-Z0-9_]+)\s*$", line)
        if m_weak:
            weak_branch = m_weak.group(1).strip()

    return top_branch, weak_branch


def build_signal_file_content(signal: DecisionSignal) -> str:
    signal_id = Path(signal.filename).stem
    signal_date = signal.date_identified[:10]
    source_name = legacy_branch_stem(signal.branch_slug)

    title_lower = signal.title.lower()

    if "operations" in title_lower:
        category = "operations"
    elif "staff" in title_lower:
        category = "staff"
    elif "branch performance" in title_lower:
        category = "branch_performance"
    else:
        category = "general"

    if "gap" in title_lower:
        signal_type = "performance_gap"
    elif "strong performance" in title_lower or "strength" in title_lower:
        signal_type = "strong_performance"
    else:
        signal_type = "advisory_signal"

    return (
        f"signal_id: {signal_id}\n"
        f"date: {signal_date}\n"
        f"source_type: advisory_report\n"
        f"source_name: {source_name}\n"
        f"branch_slug: {signal.branch_slug}\n"
        f"category: {category}\n"
        f"signal_type: {signal_type}\n"
        f"description: {signal.description}\n"
        f"confidence: {signal.leverage_score:.2f}\n"
        f"status: new\n"
        f"evidence:\n"
        f"  - {signal.source}\n"
        f"  - generated_from_report\n"
    )


def write_signal(out_dir: Path, signal: DecisionSignal, force: bool) -> bool:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / signal.filename

    if path.exists() and not force:
        return False

    path.write_text(build_signal_file_content(signal), encoding="utf-8")
    return True


def build_signals_from_reports(
    advisory_path: Path,
    fusion_path: Path | None,
    max_staff_signals: int,
) -> list[DecisionSignal]:
    advisory_text = advisory_path.read_text(encoding="utf-8")
    fusion_text = fusion_path.read_text(encoding="utf-8") if fusion_path and fusion_path.exists() else ""

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    advisory_stamp = advisory_path.stem.replace("advisory_", "")

    signals: list[DecisionSignal] = []

    # 1. Weak sections -> Performance Gap — Operations
    seen_sections: set[str] = set()
    for branch, section in parse_weak_sections(advisory_text):
        key = f"{branch}:{section}"
        if key in seen_sections:
            continue
        seen_sections.add(key)

        section_slug = slugify(section)
        branch_slug = slugify(branch)

        signals.append(
            DecisionSignal(
                filename=f"{branch_slug}_{section_slug}_gap_{advisory_stamp}.md",
                branch_slug=resolve_branch_slug(candidates=[branch]),
                title="Performance Gap — Operations",
                source=f"advisory_report:{advisory_path.name}",
                date_identified=now_iso,
                description=(
                    f"{branch} shows a weak operational section: {section}. "
                    f"Recommendation in advisory report: improve display, support, and engagement."
                ),
                leverage_score=0.74,
                risk_level="Medium",
            )
        )

    # 2. Top staff -> Strong Performance — Staff
    top_staff = sorted(parse_top_staff(advisory_text), key=lambda x: -x[2])[:max_staff_signals]
    for branch, staff_name, score in top_staff:
        signals.append(
            DecisionSignal(
                filename=f"{slugify(branch)}_{slugify(staff_name)}_strength_{advisory_stamp}.md",
                branch_slug=resolve_branch_slug(candidates=[branch]),
                title="Strong Performance — Staff",
                source=f"advisory_report:{advisory_path.name}",
                date_identified=now_iso,
                description=(
                    f"{staff_name} in {branch} is identified as a top performer "
                    f"with advisory strength score {score:.2f}."
                ),
                leverage_score=0.68,
                risk_level="Low",
            )
        )

    # 3. Fusion top/weak branch -> branch performance signals
    if fusion_text:
        top_branch, weak_branch = parse_fusion_summary(fusion_text)

        if top_branch:
            signals.append(
                DecisionSignal(
                    filename=f"{slugify(top_branch)}_branch_strength_{advisory_stamp}.md",
                    branch_slug=resolve_branch_slug(candidates=[top_branch]),
                    title="Strong Performance — Branch Performance",
                    source=f"fusion_report:{fusion_path.name if fusion_path else 'unknown'}",
                    date_identified=now_iso,
                    description=f"{top_branch} is the current top-performing branch in the fusion report.",
                    leverage_score=0.66,
                    risk_level="Low",
                )
            )

        if weak_branch:
            signals.append(
                DecisionSignal(
                    filename=f"{slugify(weak_branch)}_branch_gap_{advisory_stamp}.md",
                    branch_slug=resolve_branch_slug(candidates=[weak_branch]),
                    title="Performance Gap — Branch Performance",
                    source=f"fusion_report:{fusion_path.name if fusion_path else 'unknown'}",
                    date_identified=now_iso,
                    description=f"{weak_branch} is the current weakest branch in the fusion report and needs review.",
                    leverage_score=0.79,
                    risk_level="High",
                )
            )

    return signals


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate decision signals from latest colony reports.")
    parser.add_argument(
        "--advisory",
        default="",
        help="Optional explicit advisory report path. Defaults to latest advisory_*.md in REPORTS/",
    )
    parser.add_argument(
        "--fusion",
        default="",
        help="Optional explicit fusion report path. Defaults to latest fusion_*.md in REPORTS/ if present.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory to write generated decision signals into. Default: INPUTS/",
    )
    parser.add_argument(
        "--max-staff-signals",
        type=int,
        default=5,
        help="Maximum number of top-staff strength signals to emit.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing files with the same name.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    advisory_path = Path(args.advisory) if args.advisory else latest_report("advisory")
    fusion_path = Path(args.fusion) if args.fusion else latest_report("fusion")
    out_dir = Path(args.out_dir)

    if not advisory_path or not advisory_path.exists():
        print(f"{POST_PREFIX} ERROR: advisory report not found", file=sys.stderr)
        return 1

    print(f"{POST_PREFIX} Using advisory report: {advisory_path}")
    if fusion_path and fusion_path.exists():
        print(f"{POST_PREFIX} Using fusion report: {fusion_path}")
    else:
        print(f"{POST_PREFIX} No fusion report found; generating advisory-derived signals only")

    signals = build_signals_from_reports(
        advisory_path=advisory_path,
        fusion_path=fusion_path if fusion_path and fusion_path.exists() else None,
        max_staff_signals=args.max_staff_signals,
    )

    written = 0
    skipped = 0

    for signal in signals:
        if write_signal(out_dir, signal, force=args.force):
            written += 1
            print(f"{POST_PREFIX} Wrote {signal.filename} -> {signal.title}")
        else:
            skipped += 1

    print(f"{POST_PREFIX} Complete: written={written}, skipped={skipped}, out_dir={out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
