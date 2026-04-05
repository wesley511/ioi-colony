from __future__ import annotations

import re
from typing import Iterable


EXPECTED_REPORT_TYPES = (
    "sales",
    "staff_attendance",
    "staff_performance",
    "bale_summary",
)


def count_inferred_fields(
    *,
    warnings: Iterable[str] | None = None,
    flags: Iterable[str] | None = None,
    extra_markers: Iterable[str] | None = None,
) -> int:
    markers: set[str] = set()

    for flag in flags or []:
        token = str(flag).strip()
        if not token:
            continue
        if token == "branch_inferred" or token.endswith("_null") or token.endswith("_invalid"):
            markers.add(token)

    for warning in warnings or []:
        text = re.sub(r"\s+", " ", str(warning).strip().lower())
        if not text:
            continue
        if (
            "stored as null" in text
            or "stored as empty" in text
            or "computed from bale rows" in text
            or "inferred from report text" in text
        ):
            markers.add(text)

    for marker in extra_markers or []:
        token = str(marker).strip()
        if token:
            markers.add(token)

    return len(markers)


def build_confidence_metadata(
    *,
    validation_lane: str,
    warnings: Iterable[str] | None = None,
    flags: Iterable[str] | None = None,
    inferred_field_count: int | None = None,
    confidence_score: float | None = None,
) -> dict[str, object]:
    warning_list = [str(item) for item in (warnings or [])]
    inferred_count = (
        inferred_field_count
        if inferred_field_count is not None
        else count_inferred_fields(warnings=warning_list, flags=flags)
    )
    return {
        "warning_count": len(warning_list),
        "inferred_field_count": inferred_count,
        "validation_lane": str(validation_lane or "accepted"),
        "confidence_score": confidence_score,
    }
