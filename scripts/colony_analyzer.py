from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import DefaultDict, Dict, List, Tuple

try:
    from scripts.branch_resolution import legacy_branch_display, legacy_branch_stem, resolve_branch_slug
except ModuleNotFoundError:
    from branch_resolution import legacy_branch_display, legacy_branch_stem, resolve_branch_slug
try:
    from scripts.section_normalizer import normalize_section_name
except ModuleNotFoundError:
    from section_normalizer import normalize_section_name
try:
    from scripts.staff_signal_loader import dedupe_staff_signals
except ModuleNotFoundError:
    from staff_signal_loader import dedupe_staff_signals
from scripts.section_master_data import resolve_section_from_master_data


STAFF_SIGNALS_DIR = Path("COLONY_MEMORY/staff_signals")
NORMALIZED_STAFF_DIR = Path("SIGNALS/normalized")
REPORTS_DIR = Path("REPORTS")


def parse_signal_file(path: Path) -> dict:
    """
    Parse flat markdown signals of the form:

    key: value
    """
    data: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = value.strip()
    data["_path"] = str(path)
    return data


def parse_float(value: str | None, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def parse_int(value: str | None, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def clean_token(text: str) -> str:
    text = (text or "").strip().lower()
    text = text.replace("&", " and ")
    text = text.replace("#", "_")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def pretty_branch(branch_slug: str) -> str:
    return legacy_branch_display(branch_slug)


def infer_branch_slug_from_path(signal: dict) -> str:
    return resolve_branch_slug(signal, path=signal.get("_path"), candidates=[signal.get("branch"), signal.get("source_slug")])


def infer_staff_name_from_path(signal: dict) -> str:
    explicit = (signal.get("staff_name") or "").strip()
    if explicit:
        return explicit

    path = str(signal.get("_path", ""))
    name = Path(path).stem.lower()

    prefixes = [
        "staff-bena-road-",
        "staff-bena-",
        "staff-waigani-",
        "staff-fifth-street-",
        "staff-5th-street-",
        "staff-5th-",
        "staff-mataita-street-",
        "staff-lae-malaita-",
    ]

    for prefix in prefixes:
        if name.startswith(prefix):
            remainder = name[len(prefix):]
            remainder = re.sub(r"_\d{4}-\d{2}-\d{2}$", "", remainder)
            return remainder

    return "unknown_staff"


def infer_staff_key(signal: dict) -> str:
    branch_slug = clean_token(resolve_branch_slug(signal, path=signal.get("_path")))
    staff_name = clean_token(
        signal.get("staff_name")
        or infer_staff_name_from_path(signal)
    )
    return f"staff-{branch_slug}-{staff_name}"


def normalize_section_key(signal: dict) -> tuple[str, str]:
    """
    Returns:
      (section_key, section_type)

    section_type ∈ {product, operational, mixed, unknown}
    """
    branch_slug = clean_token(resolve_branch_slug(signal, path=signal.get("_path")))

    raw_section = (
        signal.get("grouped_section")
        or signal.get("section_canonical")
        or signal.get("section")
        or signal.get("raw_section")
        or "unknown_section"
    )

    match = resolve_section_from_master_data(raw_section, branch_slug)
    if match:
        return clean_token(match.canonical), clean_token(match.section_type)

    cleaned = normalize_section_name(str(raw_section))
    if cleaned:
        return clean_token(cleaned), "resolved"

    fallback_cleaned = clean_token(str(raw_section))
    return fallback_cleaned or "unknown_section", "unknown"


def infer_signal_strength(signal: dict) -> float:
    """
    Deterministic staff-strength score using fields already present in normalized files.
    """
    arrangement = parse_float(signal.get("arrangement"))
    display = parse_float(signal.get("display"))
    performance = parse_float(signal.get("performance"))
    items_moved = parse_float(signal.get("items_moved"))
    confidence = parse_float(signal.get("confidence"), 0.50)

    rating_sum = arrangement + display + performance
    rating_component = rating_sum * 2.4 if rating_sum > 0 else 0.0
    movement_component = items_moved * 1.6
    confidence_component = confidence * 10.0

    return round(rating_component + movement_component + confidence_component, 2)


def load_staff_signals(signals_dir: Path = STAFF_SIGNALS_DIR) -> list[dict]:
    candidate_paths: list[Path] = []
    if NORMALIZED_STAFF_DIR.exists():
        candidate_paths.extend(sorted(NORMALIZED_STAFF_DIR.glob("*staff*.md")))
    if not candidate_paths and signals_dir.exists():
        candidate_paths.extend(sorted(signals_dir.glob("*.md")))
    if not candidate_paths:
        return []

    def _parser(path: Path) -> dict | None:
        try:
            payload = parse_signal_file(path)
            payload.setdefault("source_file", str(path))
            return payload
        except Exception:
            return None

    results = []
    for payload in dedupe_staff_signals(candidate_paths, _parser):
        section = clean_token(payload.get("section_canonical") or payload.get("section") or payload.get("raw_section"))
        if section in {"unknown", "unknown_section"}:
            continue
        results.append(payload)
    return results


def aggregate_signal(signal: dict, bucket: dict) -> None:
    strength = infer_signal_strength(signal)
    staff_key = infer_staff_key(signal)
    section_key, section_type = normalize_section_key(signal)

    bucket["signal_count"] += 1
    bucket["total_strength"] += strength
    bucket["staff_scores"][staff_key] += strength
    bucket["section_scores"][section_key] += strength

    if section_type == "product":
        bucket["product_section_scores"][section_key] += strength
    elif section_type == "operational":
        bucket["operational_section_scores"][section_key] += strength
    elif section_type == "mixed":
        bucket["mixed_section_scores"][section_key] += strength
    elif section_type == "unknown":
        bucket["unknown_section_scores"][section_key] += strength


def build_branch_data(signals: list[dict]) -> Dict[str, Dict]:
    branch_data: Dict[str, Dict] = {}

    for signal in signals:
        branch_slug = clean_token(resolve_branch_slug(signal, path=signal.get("_path")))
        staff_name = clean_token(signal.get("staff_name") or infer_staff_name_from_path(signal))
        if branch_slug == "unknown" or staff_name == "unknown_staff":
            continue

        if branch_slug not in branch_data:
            branch_data[branch_slug] = {
                "signal_count": 0,
                "total_strength": 0.0,
                "staff_scores": defaultdict(float),
                "section_scores": defaultdict(float),
                "product_section_scores": defaultdict(float),
                "operational_section_scores": defaultdict(float),
                "mixed_section_scores": defaultdict(float),
                "unknown_section_scores": defaultdict(float),
                "advisory_strength_avg": 0.0,
                "strongest_staff": [],
                "strongest_sections": [],
            }

        aggregate_signal(signal, branch_data[branch_slug])

    for branch_slug, bucket in branch_data.items():
        signal_count = bucket["signal_count"]
        avg_strength = bucket["total_strength"] / signal_count if signal_count else 0.0
        bucket["advisory_strength_avg"] = round(avg_strength, 2)

        bucket["strongest_staff"] = sorted(
            bucket["staff_scores"].items(),
            key=lambda x: x[1],
            reverse=True,
        )[:5]

        bucket["strongest_sections"] = sorted(
            bucket["section_scores"].items(),
            key=lambda x: x[1],
            reverse=True,
        )[:5]

    return branch_data


def detect_weak_sections(branch_data: Dict[str, Dict]) -> Dict[str, List[str]]:
    weak: Dict[str, List[str]] = {}

    for branch, data in branch_data.items():
        sections: dict = data.get("section_scores", {})
        if not sections:
            continue

        values = list(sections.values())
        avg = sum(values) / len(values) if values else 0.0
        threshold = avg * 0.6 if avg > 0 else 0.0

        weak_sections = [sec for sec, val in sections.items() if val < threshold]
        if weak_sections:
            weak[branch] = sorted(weak_sections)

    return weak


def build_issues(weak_sections: Dict[str, List[str]]) -> Dict[str, List[str]]:
    issues: Dict[str, List[str]] = {}
    for branch, sections in weak_sections.items():
        issues[branch] = [f"Weak section: {sec}" for sec in sections]
    return issues


def build_recommendations(weak_sections: Dict[str, List[str]]) -> List[str]:
    recs: List[str] = []
    for branch, sections in weak_sections.items():
        for sec in sections:
            recs.append(f"Improve display, support, and engagement in {legacy_branch_stem(branch)} -> {sec}")
    return recs

def ensure_governance(value) -> str:
    """
    Backward-compatible governance helper.

    Supports either:
    1. report text -> appends governance footer if missing
    2. Path / file path -> verifies the rules file exists and returns its text
    """
    governance_line = "[IOI Colony Advisory] Advisory only. The colony informs. Humans decide."

    def append_governance(report_text: str) -> str:
        if governance_line in report_text:
            return report_text
        if not report_text.endswith("\n"):
            report_text += "\n"
        return report_text + "\n" + governance_line + "\n"

    # Real Path object from fusion analyzer
    if isinstance(value, Path):
        if not value.exists():
            raise FileNotFoundError(f"Governance file not found: {value}")
        return value.read_text(encoding="utf-8", errors="replace")

    # String input: decide whether it is report text or a file path
    if isinstance(value, str):
        text = value

        # If it clearly looks like report content, do NOT treat it as a path
        if "\n" in text or "=== COLONY" in text or len(text) > 240:
            return append_governance(text)

        # Only then try path-like handling
        possible_path = Path(text)
        if possible_path.exists() and possible_path.is_file():
            return possible_path.read_text(encoding="utf-8", errors="replace")

        return append_governance(text)

    # Fallback
    return append_governance(str(value))


def render_section_block(title: str, items: List[Tuple[str, float]]) -> list[str]:
    lines: list[str] = []
    if items:
        lines.append(f"- {title}:")
        for name, score in items[:5]:
            lines.append(f"  - {name}: {score:.2f}")
    return lines


def render_named_list(title: str, items: List[str]) -> list[str]:
    lines: list[str] = []
    if items:
        lines.append(f"- {title}:")
        for item in items:
            lines.append(f"  - {item}")
    return lines


def generate_report_text(
    branch_data: Dict[str, Dict],
    weak_sections: Dict[str, List[str]],
    recommendations: List[str],
    issues_detected: Dict[str, List[str]],
) -> str:
    branch_ranking = sorted(
        branch_data.items(),
        key=lambda x: x[1].get("advisory_strength_avg", 0.0),
        reverse=True,
    )

    branches_analyzed = len(branch_data)
    signals_analyzed = sum(x["signal_count"] for x in branch_data.values())
    top_performer = pretty_branch(branch_ranking[0][0]) if branch_ranking else "N/A"
    weakest_branch = pretty_branch(branch_ranking[-1][0]) if branch_ranking else "N/A"

    lines: list[str] = []
    lines.append("=== COLONY INTELLIGENCE REPORT ===")
    lines.append("")
    lines.append(f"Branches analyzed: {branches_analyzed}")
    lines.append(f"Signals analyzed: {signals_analyzed}")
    lines.append("")
    lines.append(f"TOP PERFORMER: {top_performer}")
    lines.append(f"WEAKEST BRANCH: {weakest_branch}")
    lines.append("")
    lines.append("=== BRANCH RANKING ===")
    lines.append("")

    for idx, (branch, data) in enumerate(branch_ranking, start=1):
        lines.append(
            f"{idx}. {pretty_branch(branch)}  | "
            f"avg_strength={data['advisory_strength_avg']:.2f} | "
            f"signals={data['signal_count']}"
        )

    lines.append("")
    lines.append("=== BRANCH DETAIL ===")
    lines.append("")

    for branch, data in branch_ranking:
        lines.append(pretty_branch(branch))
        lines.append(f"- advisory_strength_avg: {data['advisory_strength_avg']:.2f}")
        lines.append(f"- signal_count: {data['signal_count']}")

        if data.get("strongest_staff"):
            lines.append("- strongest_staff:")
            for staff_name, score in data["strongest_staff"]:
                lines.append(f"  - {staff_name}: {score:.2f}")

        if data.get("strongest_sections"):
            lines.append("- strongest_sections:")
            for sec, score in data["strongest_sections"]:
                lines.append(f"  - {sec}: {score:.2f}")

        product_scores = sorted(
            data.get("product_section_scores", {}).items(),
            key=lambda x: x[1],
            reverse=True,
        )[:5]
        lines.extend(render_section_block("strongest_product_sections", product_scores))

        operational_scores = sorted(
            data.get("operational_section_scores", {}).items(),
            key=lambda x: x[1],
            reverse=True,
        )[:5]
        lines.extend(render_section_block("strongest_operational_sections", operational_scores))

        mixed_scores = sorted(
            data.get("mixed_section_scores", {}).items(),
            key=lambda x: x[1],
            reverse=True,
        )[:5]
        lines.extend(render_section_block("strongest_mixed_sections", mixed_scores))

        unknown_scores = sorted(
            data.get("unknown_section_scores", {}).items(),
            key=lambda x: x[1],
            reverse=True,
        )[:5]
        lines.extend(render_section_block("unresolved_sections", unknown_scores))

        weak = weak_sections.get(branch, [])
        lines.extend(render_named_list("weak_sections", weak))
        lines.append("")

    lines.append("=== ISSUES DETECTED ===")
    lines.append("")
    if issues_detected:
        for branch, issues in issues_detected.items():
            lines.append(pretty_branch(branch))
            for issue in issues:
                lines.append(f"  - {issue}")
            lines.append("")
    else:
        lines.append("No major issues detected")
        lines.append("")

    lines.append("=== RECOMMENDATIONS ===")
    lines.append("")
    if recommendations:
        for rec in recommendations:
            lines.append(f"- {rec}")
    else:
        lines.append("- Continue monitoring section performance")
    lines.append("")

    report_text = "\n".join(lines)
    return ensure_governance(report_text)


def save_report(report_text: str, reports_dir: Path = REPORTS_DIR) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    path = reports_dir / f"advisory_{timestamp}.md"
    path.write_text(report_text, encoding="utf-8")
    return path


def summarize_branch_scores(branch_data: Dict[str, Dict]) -> Dict[str, float]:
    """
    Compatibility helper for fusion analyzer.
    Returns advisory avg by branch slug.
    """
    return {
        branch: float(data.get("advisory_strength_avg", 0.0))
        for branch, data in branch_data.items()
    }


def run_analysis(
    signals_dir: Path = STAFF_SIGNALS_DIR,
    reports_dir: Path = REPORTS_DIR,
) -> tuple[str, Path | None, Dict[str, Dict]]:
    signals = load_staff_signals(signals_dir)
    if not signals:
        report = ensure_governance(
            "=== COLONY INTELLIGENCE REPORT ===\n\n"
            "Branches analyzed: 0\n"
            "Signals analyzed: 0\n\n"
            "No staff signals found.\n"
        )
        return report, None, {}

    branch_data = build_branch_data(signals)
    weak_sections = detect_weak_sections(branch_data)
    issues_detected = build_issues(weak_sections)
    recommendations = build_recommendations(weak_sections)
    report_text = generate_report_text(
        branch_data=branch_data,
        weak_sections=weak_sections,
        recommendations=recommendations,
        issues_detected=issues_detected,
    )
    report_path = save_report(report_text, reports_dir=reports_dir)
    return report_text, report_path, branch_data

def load_signals(
    signals_dir: Path = STAFF_SIGNALS_DIR,
    min_confidence: float = 0.0,
    **kwargs,
) -> list[dict]:
    """
    Backward-compatible alias expected by colony_fusion_analyzer.

    Supports legacy keyword arguments like min_confidence without breaking.
    """
    signals = load_staff_signals(signals_dir)

    if min_confidence and min_confidence > 0:
        filtered: list[dict] = []
        for signal in signals:
            confidence = parse_float(signal.get("confidence"), 0.0)
            if confidence >= min_confidence:
                filtered.append(signal)
        return filtered

    return signals


def analyze_signals(signals: list[dict]) -> Dict[str, Dict]:
    """
    Backward-compatible analyzer entrypoint expected by other modules.
    """
    return build_branch_data(signals)


def build_branch_summary(branch_data: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Backward-compatible alias for already-built branch metrics.
    """
    return branch_data

def aggregate_by_branch(signals: list[dict]) -> Dict[str, Dict]:
    """
    Backward-compatible alias expected by colony_fusion_analyzer.
    """
    return build_branch_data(signals)

def aggregate_by_branch(signals: list[dict]) -> Dict[str, Dict]:
    """
    Backward-compatible alias expected by colony_fusion_analyzer.
    Maps to current build_branch_data implementation.
    """
    return build_branch_data(signals)

def normalize_label(text: str) -> str:
    """
    Backward-compatible alias expected by colony_fusion_analyzer.
    """
    return clean_token(text)

def main() -> int:
    report_text, report_path, _branch_data = run_analysis()
    print("[IOI Colony Advisory] " + report_text)
    if report_path:
        print(f"Saved report to {report_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
