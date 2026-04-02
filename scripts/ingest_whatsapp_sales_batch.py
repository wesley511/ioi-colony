from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from scripts.parse_whatsapp_sales import parse_sales_report, save_yaml
    from scripts.whatsapp_gatekeeper import validate_message
except ModuleNotFoundError:
    from parse_whatsapp_sales import parse_sales_report, save_yaml
    from whatsapp_gatekeeper import validate_message


ROOT = PROJECT_ROOT
ACCEPTED_ROOT = ROOT / "RAW_INPUT" / "whatsapp" / "accepted"
PROCESSED_ROOT = ROOT / "RAW_INPUT" / "whatsapp" / "processed"


@dataclass
class SalesCandidate:
    txt_path: Path
    meta_path: Path
    report_date: str
    branch: str
    received_at: str
    payload: dict


def _load_meta(meta_path: Path) -> dict:
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_candidate(txt_path: Path) -> SalesCandidate | None:
    meta_path = txt_path.with_suffix(".meta.json")
    if not meta_path.exists():
        return None
    validation = validate_message(txt_path.read_text(encoding="utf-8", errors="replace"))
    if not validation.ok or validation.report_type != "sales_report":
        return None
    payload = parse_sales_report(txt_path.read_text(encoding="utf-8", errors="replace"))
    branch = str(payload.get("branch") or payload.get("branch_slug") or "").strip()
    report_date = str(payload.get("date") or "").strip()
    if not branch or not report_date or branch == "unknown":
        return None
    meta = _load_meta(meta_path)
    return SalesCandidate(
        txt_path=txt_path,
        meta_path=meta_path,
        report_date=report_date,
        branch=branch,
        received_at=str(meta.get("received_at") or ""),
        payload=payload,
    )


def _candidate_rank(candidate: SalesCandidate) -> tuple[str, str, str]:
    return (candidate.report_date, candidate.received_at, candidate.txt_path.name)


def _discover_sales_files() -> list[Path]:
    results: list[Path] = []
    if ACCEPTED_ROOT.exists():
        results.extend(sorted(ACCEPTED_ROOT.glob("*/*.txt")))
    if PROCESSED_ROOT.exists():
        results.extend(sorted(PROCESSED_ROOT.glob("*/*/sales/*.txt")))
    return results


def main() -> None:
    grouped: dict[str, list[SalesCandidate]] = {}

    for txt_path in _discover_sales_files():
        candidate = _parse_candidate(txt_path)
        if not candidate:
            continue
        grouped.setdefault(candidate.branch, []).append(candidate)

    if not grouped:
        print("No accepted or processed sales WhatsApp files found")
        return

    selected: list[SalesCandidate] = []
    for branch, candidates in sorted(grouped.items()):
        picked = max(candidates, key=_candidate_rank)
        selected.append(picked)
        print(
            f"[SELECT] branch={branch} report_date={picked.report_date} "
            f"received_at={picked.received_at or 'n/a'} file={picked.txt_path.name}"
        )

    for candidate in selected:
        save_yaml(candidate.payload)
        print(f"[OK] branch={candidate.branch} report_date={candidate.report_date}")


if __name__ == "__main__":
    main()
