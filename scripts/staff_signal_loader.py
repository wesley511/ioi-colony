from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable


def _prefer_candidate(candidate: dict[str, Any]) -> tuple[int, int, int, str]:
    branch_known = 1 if candidate.get("branch") and candidate.get("branch") != "unknown" else 0
    staff_known = 1 if candidate.get("staff_name") and candidate.get("staff_name") != "unknown_staff" else 0
    section_known = 1 if candidate.get("section") and candidate.get("section") != "unknown_section" else 0
    return (
        branch_known,
        staff_known,
        section_known,
        str(candidate.get("source_file") or candidate.get("_path") or ""),
    )


def dedupe_staff_signals(
    paths: list[Path],
    parser: Callable[[Path], dict[str, Any] | None],
) -> list[dict[str, Any]]:
    chosen: dict[tuple[str, str, str], dict[str, Any]] = {}
    fallback: list[dict[str, Any]] = []

    for path in paths:
        parsed = parser(path)
        if not parsed:
            continue

        branch = str(parsed.get("branch") or "unknown").strip()
        staff_name = str(parsed.get("staff_name") or "unknown_staff").strip()
        staff_identity = _staff_identity(parsed)
        signal_date = str(
            parsed.get("signal_date")
            or parsed.get("timestamp")
            or parsed.get("date")
            or ""
        ).strip()
        section = str(parsed.get("section") or "unknown_section").strip()
        parsed.setdefault("section", section)

        if not signal_date:
            fallback.append(parsed)
            continue

        key = (branch, signal_date, staff_identity or staff_name.lower())
        current = chosen.get(key)
        if current is None or _prefer_candidate(parsed) > _prefer_candidate(current):
            chosen[key] = parsed

    return list(chosen.values()) + fallback


def _staff_identity(candidate: dict[str, Any]) -> str:
    raw = str(
        candidate.get("staff_id")
        or candidate.get("signal_id")
        or candidate.get("staff_name")
        or candidate.get("source_file")
        or ""
    ).strip().lower()
    token = re.sub(r"[^a-z0-9]+", "_", raw)
    token = re.sub(r"_\d{4}_\d{2}_\d{2}(?:_\d{6})?$", "", token)
    token = re.sub(r"_\d{4}_\d{2}_\d{2}$", "", token)
    token = re.sub(r"_(strength|productivity_signal|customer_engagement|stability_signal)$", "", token)
    token = re.sub(r"^staff_", "", token)
    token = re.sub(r"^.*_staff_", "", token)
    token = re.sub(r"^.*staff_", "", token)
    token = re.sub(r"_+", "_", token).strip("_")
    return token
