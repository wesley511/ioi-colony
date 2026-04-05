#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from scripts.branch_resolution import resolve_branch_slug
    from scripts.colony_analyzer import (
        NORMALIZED_STAFF_DIR,
        build_branch_data,
        clean_token,
        infer_staff_key,
        is_staff_observation_signal,
        load_staff_signals,
        normalize_section_key,
        parse_signal_file,
    )
    from scripts.generate_decision_signals import latest_report
    from scripts.section_normalizer import normalize_section_name
    from scripts.staff_signal_loader import dedupe_staff_signals
except ModuleNotFoundError:
    from branch_resolution import resolve_branch_slug
    from colony_analyzer import (
        NORMALIZED_STAFF_DIR,
        build_branch_data,
        clean_token,
        infer_staff_key,
        is_staff_observation_signal,
        load_staff_signals,
        normalize_section_key,
        parse_signal_file,
    )
    from generate_decision_signals import latest_report
    from section_normalizer import normalize_section_name
    from staff_signal_loader import dedupe_staff_signals

DATA_DIR = ROOT / "DATA"
REPORTS_DIR = ROOT / "REPORTS"


def stable_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def canonicalize_section_label(raw: str) -> tuple[str, str]:
    normalized = normalize_section_name(raw)
    if normalized:
        return normalized, "shared_normalizer"
    fallback = clean_token(raw)
    if fallback:
        return fallback, "fallback_slug"
    return "", "unresolved"


def extract_named_section_lines(text: str, title: str) -> list[str]:
    pattern = re.compile(rf"^\s*-\s*{re.escape(title)}:\s*$")
    lines = text.splitlines()
    values: list[str] = []
    in_section = False

    for raw in lines:
        if pattern.match(raw):
            in_section = True
            continue

        if in_section:
            if raw.startswith("  - "):
                item = raw.replace("  - ", "", 1).strip()
                if ":" in item:
                    item = item.split(":", 1)[0].strip()
                values.append(item)
                continue
            if raw.strip().startswith("- ") and not raw.startswith("  - "):
                in_section = False
            elif raw.strip() and not raw.startswith("  - "):
                in_section = False

    return values


def extract_weak_sections(text: str) -> list[str]:
    values: list[str] = []
    for raw in text.splitlines():
        match = re.match(r"^\s*-\s*Weak section:\s*(.+?)\s*$", raw)
        if match:
            values.append(match.group(1).strip())
    return values


def extract_fusion_strongest_sections(text: str) -> list[str]:
    values: list[str] = []
    lines = text.splitlines()
    in_section = False
    for raw in lines:
        stripped = raw.strip()
        if stripped == "### Strongest Sections":
            in_section = True
            continue
        if in_section and stripped.startswith("### "):
            in_section = False
            continue
        if in_section and stripped.startswith("- "):
            item = stripped[2:]
            if ":" in item:
                item = item.split(":", 1)[0].strip()
            values.append(item)
    return values


def build_section_canonicalization_audit() -> dict[str, Any]:
    advisory_path = latest_report("advisory")
    fusion_path = latest_report("fusion")
    entries: list[dict[str, Any]] = []

    for signal in load_staff_signals():
        raw_section = str(signal.get("raw_section") or signal.get("section") or "").strip()
        if not raw_section:
            continue
        canonical, method = canonicalize_section_label(raw_section)
        entries.append(
            {
                "source_family": "staff_observation",
                "source_ref": str(signal.get("_path") or signal.get("source_file") or ""),
                "branch": resolve_branch_slug(signal, path=signal.get("_path")),
                "raw_section": raw_section,
                "canonical_section": canonical,
                "canonical_method": method,
                "emitted_section": str(signal.get("section_canonical") or signal.get("section") or ""),
            }
        )

    if advisory_path and advisory_path.exists():
        advisory_text = advisory_path.read_text(encoding="utf-8")
        advisory_sections = extract_named_section_lines(advisory_text, "strongest_sections")
        advisory_sections.extend(extract_named_section_lines(advisory_text, "weak_sections"))
        advisory_sections.extend(extract_weak_sections(advisory_text))
        for section in advisory_sections:
            canonical, method = canonicalize_section_label(section)
            entries.append(
                {
                    "source_family": "colony_analyzer_report",
                    "source_ref": advisory_path.name,
                    "raw_section": section,
                    "canonical_section": canonical,
                    "canonical_method": method,
                    "emitted_section": section,
                }
            )

    if fusion_path and fusion_path.exists():
        fusion_text = fusion_path.read_text(encoding="utf-8")
        fusion_sections = extract_fusion_strongest_sections(fusion_text)
        fusion_sections.extend(extract_weak_sections(fusion_text))
        for section in fusion_sections:
            canonical, method = canonicalize_section_label(section)
            entries.append(
                {
                    "source_family": "fusion_report",
                    "source_ref": fusion_path.name,
                    "raw_section": section,
                    "canonical_section": canonical,
                    "canonical_method": method,
                    "emitted_section": section,
                }
            )

    for path in sorted(NORMALIZED_STAFF_DIR.glob("*.md")):
        payload = parse_signal_file(path)
        if str(payload.get("source_type") or "").strip() != "advisory_report":
            continue
        section = str(payload.get("section_canonical") or payload.get("section") or "").strip()
        if not section:
            description = str(payload.get("description") or "").strip()
            match = re.search(r"weak operational section:\s*([a-zA-Z0-9_#,\- ]+)", description)
            if not match:
                continue
            section = match.group(1).strip().rstrip(".")
        canonical, method = canonicalize_section_label(section)
        entries.append(
            {
                "source_family": "decision_signal",
                "source_ref": path.name,
                "raw_section": section,
                "canonical_section": canonical,
                "canonical_method": method,
                "emitted_section": section,
            }
        )

    unresolved = [entry for entry in entries if entry["canonical_method"] == "unresolved"]
    fallback = [entry for entry in entries if entry["canonical_method"] == "fallback_slug"]

    summary: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "advisory_report": advisory_path.name if advisory_path else None,
        "fusion_report": fusion_path.name if fusion_path else None,
        "entry_count": len(entries),
        "source_family_counts": dict(Counter(entry["source_family"] for entry in entries)),
        "canonical_method_counts": dict(Counter(entry["canonical_method"] for entry in entries)),
        "fallback_examples": fallback[:20],
        "unresolved_examples": unresolved[:20],
        "entries": entries,
    }
    return summary


def load_legacy_like_signals() -> list[dict[str, Any]]:
    candidate_paths: list[Path] = []
    if NORMALIZED_STAFF_DIR.exists():
        candidate_paths.extend(sorted(NORMALIZED_STAFF_DIR.glob("*staff*.md")))
        candidate_paths.extend(sorted(NORMALIZED_STAFF_DIR.glob("*_gap_*.md")))
        candidate_paths.extend(sorted(NORMALIZED_STAFF_DIR.glob("*_strength_*.md")))

    def _parser(path: Path) -> dict[str, Any] | None:
        try:
            payload = parse_signal_file(path)
        except Exception:
            return None
        payload.setdefault("source_file", str(path))
        payload.setdefault("_path", str(path))
        return payload

    results: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    unique_paths: list[Path] = []
    for path in candidate_paths:
        key = str(path)
        if key in seen_paths:
            continue
        seen_paths.add(key)
        unique_paths.append(path)

    for payload in dedupe_staff_signals(unique_paths, _parser):
        section, _section_type = normalize_section_key(payload)
        if section in {"unknown", "unknown_section"}:
            continue
        payload.setdefault("section", section)
        results.append(payload)
    return results


def branch_repeat_summary(signals: list[dict[str, Any]]) -> dict[str, Any]:
    by_branch: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        branch = resolve_branch_slug(signal, path=signal.get("_path"))
        by_branch[branch].append(signal)

    summary: dict[str, Any] = {}
    for branch, items in sorted(by_branch.items()):
        repeats = Counter(infer_staff_key(signal) for signal in items)
        summary[branch] = {
            "unique_staff_count": len(repeats),
            "top_repeated_staff": [
                {"staff_key": staff_key, "signal_count": count}
                for staff_key, count in repeats.most_common(10)
            ],
            "source_examples": [
                str(Path(str(signal.get("_path") or signal.get("source_file") or "")).name)
                for signal in items[:10]
            ],
        }
    return summary


def build_branch_signal_balance_audit() -> dict[str, Any]:
    legacy_signals = load_legacy_like_signals()
    balanced_signals = load_staff_signals()

    legacy_branch_data = build_branch_data(legacy_signals)
    balanced_branch_data = build_branch_data(balanced_signals)

    legacy_repeat = branch_repeat_summary(legacy_signals)
    balanced_repeat = branch_repeat_summary(balanced_signals)

    branches = sorted(set(legacy_branch_data) | set(balanced_branch_data))
    comparisons: dict[str, Any] = {}
    for branch in branches:
        legacy = legacy_branch_data.get(branch, {})
        balanced = balanced_branch_data.get(branch, {})
        comparisons[branch] = {
            "legacy_inclusive": {
                "signal_count": int(legacy.get("signal_count", 0)),
                "advisory_strength_avg": float(legacy.get("advisory_strength_avg", 0.0)),
                "unique_staff_count": int(legacy_repeat.get(branch, {}).get("unique_staff_count", 0)),
                "top_repeated_staff": legacy_repeat.get(branch, {}).get("top_repeated_staff", []),
                "source_examples": legacy_repeat.get(branch, {}).get("source_examples", []),
            },
            "balanced_staff_only": {
                "signal_count": int(balanced.get("signal_count", 0)),
                "advisory_strength_avg": float(balanced.get("advisory_strength_avg", 0.0)),
                "unique_staff_count": int(balanced_repeat.get(branch, {}).get("unique_staff_count", 0)),
                "top_repeated_staff": balanced_repeat.get(branch, {}).get("top_repeated_staff", []),
                "source_examples": balanced_repeat.get(branch, {}).get("source_examples", []),
            },
            "delta": {
                "signal_count": int(balanced.get("signal_count", 0) - legacy.get("signal_count", 0)),
                "advisory_strength_avg": round(
                    float(balanced.get("advisory_strength_avg", 0.0))
                    - float(legacy.get("advisory_strength_avg", 0.0)),
                    2,
                ),
            },
        }

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "legacy_signal_count": len(legacy_signals),
        "balanced_signal_count": len(balanced_signals),
        "legacy_non_staff_signal_count": sum(
            1 for signal in legacy_signals if not is_staff_observation_signal(signal)
        ),
        "branches": comparisons,
    }


def main() -> int:
    section_audit = build_section_canonicalization_audit()
    branch_audit = build_branch_signal_balance_audit()

    stable_write_json(DATA_DIR / "section_canonicalization_audit.json", section_audit)
    stable_write_json(DATA_DIR / "branch_signal_balance_audit.json", branch_audit)

    print(f"Wrote {DATA_DIR / 'section_canonicalization_audit.json'}")
    print(f"Wrote {DATA_DIR / 'branch_signal_balance_audit.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
