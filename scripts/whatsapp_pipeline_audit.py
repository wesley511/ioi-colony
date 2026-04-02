from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.branch_resolution import resolve_branch_slug
    from scripts.whatsapp_gatekeeper import validate_message
except ModuleNotFoundError:
    from branch_resolution import resolve_branch_slug
    from whatsapp_gatekeeper import validate_message


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "REPORTS"
DATA_DIR = ROOT / "DATA"
ACCEPTED_ROOT = ROOT / "RAW_INPUT" / "whatsapp" / "accepted"
PROCESSED_ROOT = ROOT / "RAW_INPUT" / "whatsapp" / "processed"
NORMALIZED_DIR = ROOT / "SIGNALS" / "normalized"
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


def _scan_staff_reports() -> dict[str, set[str]]:
    coverage: dict[str, set[str]] = defaultdict(set)
    for txt_path in sorted(ACCEPTED_ROOT.glob("*/*.txt")):
        result = validate_message(txt_path.read_text(encoding="utf-8", errors="replace"))
        if not result.ok or result.report_type != "staff_report" or not result.normalized:
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

    staff_coverage = _scan_staff_reports()
    latest_staff_date = max(staff_coverage) if staff_coverage else ""
    missing_branches = sorted(set(EXPECTED_BRANCHES) - staff_coverage.get(latest_staff_date, set()))

    accepted_duplicates = _duplicate_groups(list(ACCEPTED_ROOT.glob("*/*.txt")), "accepted")
    processed_duplicates = _duplicate_groups(list(PROCESSED_ROOT.glob("*/*/*/*.txt")), "processed")
    normalized_duplicates = _normalized_staff_duplicates()

    summary = {
        "generated_at": iso_now(),
        "latest_staff_report_date": latest_staff_date,
        "missing_staff_branches": missing_branches,
        "accepted_duplicate_groups": accepted_duplicates,
        "processed_duplicate_groups": processed_duplicates,
        "normalized_staff_duplicate_groups": normalized_duplicates,
    }

    (DATA_DIR / "whatsapp_pipeline_audit.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    lines = [
        "# WhatsApp Pipeline Audit",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- latest_staff_report_date: {latest_staff_date or 'none'}",
        f"- missing_staff_branches: {', '.join(missing_branches) if missing_branches else 'none'}",
        "",
        "## Duplicate Audit",
        "",
        f"- accepted_duplicate_groups: {len(accepted_duplicates)}",
        f"- processed_duplicate_groups: {len(processed_duplicates)}",
        f"- normalized_staff_duplicate_groups: {len(normalized_duplicates)}",
        "",
    ]
    if missing_branches:
        lines.append("## Missing Daily Staff Reports")
        lines.append("")
        lines.append(f"- date: {latest_staff_date}")
        lines.append(f"- missing_branches: {', '.join(missing_branches)}")
        lines.append("")

    path = REPORTS_DIR / f"whatsapp_pipeline_audit_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main() -> int:
    path = generate_audit()
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
