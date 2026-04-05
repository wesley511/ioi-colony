from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.branch_resolution import resolve_branch_slug
    from scripts.utils_normalization import normalize_branch as shared_normalize_branch
    from scripts.whatsapp_gatekeeper import validate_message
    from scripts.whatsapp_intelligence import EXPECTED_REPORT_TYPES
except ModuleNotFoundError:
    from branch_resolution import resolve_branch_slug
    from utils_normalization import normalize_branch as shared_normalize_branch
    from whatsapp_gatekeeper import validate_message
    from whatsapp_intelligence import EXPECTED_REPORT_TYPES


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "REPORTS"
DATA_DIR = ROOT / "DATA"
ACCEPTED_ROOT = ROOT / "RAW_INPUT" / "whatsapp" / "accepted"
PROCESSED_ROOT = ROOT / "RAW_INPUT" / "whatsapp" / "processed"
NORMALIZED_DIR = ROOT / "SIGNALS" / "normalized"
INVALID_DIR = ROOT / "SIGNALS" / "quarantine_invalid"
COLONY_MEMORY_DIR = ROOT / "COLONY_MEMORY" / "staff_signals"
EXPECTED_BRANCHES = [
    "waigani",
    "lae_5th_street",
    "lae_malaita",
    "bena_road",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _canonical_report_type(value: str | None) -> str:
    mapping = {
        "sales_report": "sales",
        "staff_report": "staff_performance",
        "staff_attendance_report": "staff_attendance",
        "bale_report": "bale_summary",
    }
    token = str(value or "").strip().lower()
    return mapping.get(token, token or "unknown")


def _infer_branch_from_text(text: str) -> str:
    normalized = shared_normalize_branch(
        text,
        style="canonical_slug",
        fallback="none",
        match_substring=True,
    )
    return str(normalized or "unknown")


def _scan_valid_accepted_reports() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for txt_path in sorted(ACCEPTED_ROOT.glob("*/*.txt")):
        text = txt_path.read_text(encoding="utf-8", errors="replace")
        result = validate_message(text)
        if not result.ok or not result.normalized:
            continue
        rows.append(
            {
                "path": str(txt_path),
                "file": txt_path.name,
                "branch": str(result.normalized["branch"]),
                "date": str(result.normalized["date"]),
                "report_type": _canonical_report_type(result.report_type),
                "validation_lane": str(result.lane),
                "warnings": list(result.warnings),
            }
        )
    return rows


def _daily_completeness(valid_reports: list[dict[str, Any]]) -> dict[str, Any]:
    dates = sorted({str(row["date"]) for row in valid_reports})
    presence: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in valid_reports:
        presence[(str(row["date"]), str(row["branch"]))].add(str(row["report_type"]))

    rows: list[dict[str, Any]] = []
    matrix: dict[str, dict[str, list[str]]] = {}
    for report_date in dates:
        matrix[report_date] = {}
        for branch in EXPECTED_BRANCHES:
            present_reports = sorted(presence.get((report_date, branch), set()))
            missing_reports = [report for report in EXPECTED_REPORT_TYPES if report not in present_reports]
            matrix[report_date][branch] = missing_reports
            rows.append(
                {
                    "date": report_date,
                    "branch": branch,
                    "present_reports": present_reports,
                    "missing_reports": missing_reports,
                    "completeness_ratio": round(
                        (len(present_reports) / len(EXPECTED_REPORT_TYPES)) if EXPECTED_REPORT_TYPES else 0.0,
                        2,
                    ),
                }
            )

    return {
        "generated_at": iso_now(),
        "expected_reports": list(EXPECTED_REPORT_TYPES),
        "rows": rows,
        "missing_report_matrix": matrix,
    }


def _failure_memory_summary(valid_reports: list[dict[str, Any]]) -> dict[str, Any]:
    rejection_causes = Counter()
    warning_causes = Counter()
    grouped: dict[tuple[str, str], dict[str, Counter[str]]] = defaultdict(
        lambda: {"rejections": Counter(), "warnings": Counter()}
    )

    for row in valid_reports:
        branch = str(row["branch"])
        report_type = str(row["report_type"])
        for warning in row["warnings"]:
            warning_causes[str(warning)] += 1
            grouped[(branch, report_type)]["warnings"][str(warning)] += 1

    for path in sorted(INVALID_DIR.glob("*.json")):
        payload = _load_json(path)
        raw_text = str(payload.get("raw_text") or "")
        current = validate_message(raw_text) if raw_text else None
        branch = (
            str((current.normalized or {}).get("branch"))
            if current and current.normalized
            else _infer_branch_from_text(raw_text)
        )
        report_type = _canonical_report_type(
            payload.get("report_type") or (current.report_type if current else None)
        )
        for reason in payload.get("reasons", []) or []:
            rejection_causes[str(reason)] += 1
            grouped[(branch, report_type)]["rejections"][str(reason)] += 1

    branch_report_rows = []
    for (branch, report_type), counters in sorted(grouped.items()):
        branch_report_rows.append(
            {
                "branch": branch,
                "report_type": report_type,
                "top_rejection_causes": counters["rejections"].most_common(5),
                "top_warning_causes": counters["warnings"].most_common(5),
            }
        )

    return {
        "generated_at": iso_now(),
        "top_rejection_causes": rejection_causes.most_common(10),
        "top_warning_causes": warning_causes.most_common(10),
        "by_branch_report_type": branch_report_rows,
    }


def _replay_audit() -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    by_branch_report_type = Counter()

    for path in sorted(INVALID_DIR.glob("*.json")):
        payload = _load_json(path)
        raw_text = str(payload.get("raw_text") or "")
        if not raw_text:
            continue
        current = validate_message(raw_text)
        if not current.ok:
            continue

        branch = (
            str((current.normalized or {}).get("branch"))
            if current.normalized
            else _infer_branch_from_text(raw_text)
        )
        report_type = _canonical_report_type(current.report_type or payload.get("report_type"))
        lane = str(current.lane)
        rows.append(
            {
                "rejection_file": path.name,
                "branch": branch,
                "date": str((current.normalized or {}).get("date") or ""),
                "report_type": report_type,
                "new_lane": lane,
                "previous_reasons": list(payload.get("reasons", []) or []),
                "current_warnings": list(current.warnings),
            }
        )
        by_branch_report_type[(branch, report_type, lane)] += 1

    return {
        "generated_at": iso_now(),
        "promoted_files": rows,
        "summary_by_branch_report_type": [
            {
                "branch": branch,
                "report_type": report_type,
                "new_lane": lane,
                "count": count,
            }
            for (branch, report_type, lane), count in sorted(by_branch_report_type.items())
        ],
    }


def _scan_staff_attendance_reports() -> dict[str, set[str]]:
    coverage: dict[str, set[str]] = defaultdict(set)
    for txt_path in sorted(ACCEPTED_ROOT.glob("*/*.txt")):
        result = validate_message(txt_path.read_text(encoding="utf-8", errors="replace"))
        if not result.ok or result.report_type != "staff_attendance_report" or not result.normalized:
            continue
        coverage[str(result.normalized["date"])].add(str(result.normalized["branch"]))
    return coverage


def _duplicate_groups(paths: list[Path], kind: str) -> list[dict[str, Any]]:
    groups: dict[str, list[str]] = defaultdict(list)
    for path in paths:
        if kind == "accepted":
            validation = validate_message(path.read_text(encoding="utf-8", errors="replace"))
            if validation.ok and validation.normalized:
                key = f"{validation.report_type}:{validation.normalized['branch']}:{validation.normalized['date']}"
            else:
                key = f"invalid:{path.name}"
        else:
            key = path.stem
        groups[key].append(path.name)

    rows = []
    for key, names in sorted(groups.items()):
        if len(names) > 1:
            rows.append({"key": key, "count": len(names), "files": sorted(names)})
    return rows


def _normalized_staff_duplicates() -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str], list[str]] = defaultdict(list)
    for path in sorted(NORMALIZED_DIR.glob("*staff*.md")):
        payload = {}
        for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if ":" not in raw:
                continue
            key, value = raw.split(":", 1)
            payload[key.strip()] = value.strip()
        branch = resolve_branch_slug(payload, path=path)
        date = payload.get("signal_date") or payload.get("report_date") or ""
        staff_name = str(payload.get("staff_name") or "").strip().lower()
        section = str(payload.get("section_canonical") or payload.get("section") or "").strip().lower()
        groups[(branch, date, staff_name, section)].append(path.name)

    rows = []
    for key, names in sorted(groups.items()):
        if len(names) > 1:
            rows.append({"key": "|".join(key), "count": len(names), "files": sorted(names)})
    return rows


def generate_audit() -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    valid_reports = _scan_valid_accepted_reports()
    completeness = _daily_completeness(valid_reports)
    failure_memory = _failure_memory_summary(valid_reports)
    replay_audit = _replay_audit()

    staff_coverage = _scan_staff_attendance_reports()
    latest_staff_date = max(staff_coverage) if staff_coverage else ""
    missing_branches = sorted(set(EXPECTED_BRANCHES) - staff_coverage.get(latest_staff_date, set()))

    accepted_duplicates = _duplicate_groups(list(ACCEPTED_ROOT.glob("*/*.txt")), "accepted")
    processed_duplicates = _duplicate_groups(list(PROCESSED_ROOT.glob("*/*/*/*.txt")), "processed")
    normalized_duplicates = _normalized_staff_duplicates()

    summary = {
        "generated_at": iso_now(),
        "latest_staff_attendance_date": latest_staff_date,
        "missing_staff_attendance_branches": missing_branches,
        "latest_staff_report_date": latest_staff_date,
        "missing_staff_branches": missing_branches,
        "accepted_duplicate_groups": accepted_duplicates,
        "processed_duplicate_groups": processed_duplicates,
        "normalized_staff_duplicate_groups": normalized_duplicates,
        "daily_completeness_engine": completeness,
        "failure_memory_summary": failure_memory,
        "replay_audit": replay_audit,
    }

    _save_json(DATA_DIR / "whatsapp_completeness_matrix.json", completeness)
    _save_json(DATA_DIR / "whatsapp_failure_memory.json", failure_memory)
    _save_json(DATA_DIR / "whatsapp_replay_audit.json", replay_audit)
    _save_json(DATA_DIR / "whatsapp_pipeline_audit.json", summary)

    latest_completeness_date = max(
        (row["date"] for row in completeness["rows"]),
        default="",
    )
    latest_missing_rows = [
        row for row in completeness["rows"] if row["date"] == latest_completeness_date and row["missing_reports"]
    ]

    lines = [
        "# WhatsApp Pipeline Audit",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- latest_staff_attendance_date: {latest_staff_date or 'none'}",
        f"- missing_staff_attendance_branches: {', '.join(missing_branches) if missing_branches else 'none'}",
        "",
        "## Duplicate Audit",
        "",
        f"- accepted_duplicate_groups: {len(accepted_duplicates)}",
        f"- processed_duplicate_groups: {len(processed_duplicates)}",
        f"- normalized_staff_duplicate_groups: {len(normalized_duplicates)}",
        "",
        "## Daily Completeness",
        "",
        f"- latest_date: {latest_completeness_date or 'none'}",
        f"- rows: {len(completeness['rows'])}",
        "",
    ]

    for row in latest_missing_rows[:12]:
        lines.append(
            f"- {row['date']} {row['branch']}: missing {', '.join(row['missing_reports']) or 'none'}"
        )
    if latest_missing_rows:
        lines.append("")

    lines.extend(
        [
            "## Failure Memory",
            "",
            f"- top_rejection_causes: {failure_memory['top_rejection_causes'][:5]}",
            f"- top_warning_causes: {failure_memory['top_warning_causes'][:5]}",
            "",
            "## Replay Audit",
            "",
            f"- historical_rejections_now_reaccepted: {len(replay_audit['promoted_files'])}",
            "",
        ]
    )

    path = REPORTS_DIR / f"whatsapp_pipeline_audit_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main() -> int:
    path = generate_audit()
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
