from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


REPORT_HEADER_MAP: dict[str, str] = {
    "DAY-END SALES REPORT": "sales",
    "STAFF PERFORMANCE REPORT": "staff_performance",
    "DAILY STAFF PERFORMANCE REPORT": "staff_performance",
    "STAFF ATTENDANCE REPORT": "staff_attendance",
    "DAILY STAFF ATTENDANCE REPORT": "staff_attendance",
    "SUPERVISOR CONTROL REPORT": "legacy_supervisor",
    "SUPERVISOR CONTROL SUMMARY": "legacy_supervisor",
    "DAILY BALE SUMMARY - RELEASED TO RAIL": "bale_summary",
    "DAILY BALE SUMMARY – RELEASED TO RAIL": "bale_summary",
    "DAILY BALE SUMMARY RELEASED TO RAIL": "bale_summary",
    "DAILY BALE SUMMARY": "bale_summary",
}

REPORT_KEYWORDS: dict[str, tuple[str, ...]] = {
    "sales": (
        "day-end sales report",
        "z reading",
        "cash sales",
        "eftpos sales",
        "total sales",
        "customers served",
        "traffic",
        "main door",
        "door count",
        "cash variance",
    ),
    "bale_summary": (
        "daily bale summary",
        "released to rail",
        "bale #",
        "bale id",
        "item name",
        "total qty",
        "total amount",
    ),
    "staff_performance": (
        "staff performance report",
        "arrangement",
        "display",
        "performance",
        "customers assisted",
        "items moved",
    ),
    "staff_attendance": (
        "staff attendance report",
        "attendance",
        "present",
        "absent",
        "annual leave",
        "sick leave",
        "off duty",
    ),
    "legacy_supervisor": (
        "supervisor control report",
        "supervisor control summary",
        "staffing issues",
        "stock issues",
        "pricing or system issues",
        "exceptions escalated",
        "supervisor confirmation",
    ),
}

HEADER_PATTERN = re.compile(
    r"^\s*(DAY-END SALES REPORT|STAFF PERFORMANCE REPORT|DAILY STAFF PERFORMANCE REPORT|"
    r"STAFF ATTENDANCE REPORT|DAILY STAFF ATTENDANCE REPORT|SUPERVISOR CONTROL REPORT|"
    r"SUPERVISOR CONTROL SUMMARY|DAILY BALE SUMMARY(?:\s*[-–]\s*RELEASED TO RAIL)?|"
    r"DAILY BALE SUMMARY RELEASED TO RAIL)\s*$",
    flags=re.IGNORECASE,
)
KEY_VALUE_LINE_RE = re.compile(r"^\s*[A-Za-z0-9 /#()._-]+\s*[:=]\s*.+$")
ATTENDANCE_STATUS_RE = re.compile(
    r"(?:^|[:\-\s])(?:✔|present|absent|off|off duty|leave|annual leave|sick leave)(?:$|[:\-\s])",
    flags=re.IGNORECASE,
)
ATTENDANCE_TOTAL_RE = re.compile(
    r"^\s*(present|absent|off|off duty|leave|annual leave|sick leave|staff on duty|total staff)\s*[:=]\s*.+$",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class ReportBlock:
    raw_type: str
    report_type: str
    header: str
    section_text: str
    contextual_text: str
    keyword_hits: int
    structural_hits: int
    density_score: float
    score: float


def normalize_title(line: str) -> str:
    value = re.sub(r"\s+", " ", (line or "").strip()).upper()
    value = value.replace("—", "-").replace("–", "-")
    value = re.sub(r"\s*-\s*", " - ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def header_candidates(line: str) -> list[str]:
    normalized = normalize_title(line)
    compact = re.sub(r"\s*-\s*", "-", normalized)
    spaced = re.sub(r"\s*-\s*", " - ", normalized)
    candidates = [normalized]
    for candidate in (compact, spaced):
        candidate = re.sub(r"\s+", " ", candidate).strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates


def lookup_report_type(line: str) -> str | None:
    for candidate in header_candidates(line):
        match = REPORT_HEADER_MAP.get(candidate)
        if match:
            return match
    return None


def resolve_header_title(line: str) -> str | None:
    for candidate in header_candidates(line):
        if candidate in REPORT_HEADER_MAP:
            return candidate
    return None


def _split_known_blocks(lines: list[str]) -> tuple[list[str], list[tuple[str, list[str]]]]:
    prefix: list[str] = []
    blocks: list[tuple[str, list[str]]] = []
    current_header = ""
    current_lines: list[str] = []
    in_block = False

    for raw_line in lines:
        header_title = resolve_header_title(raw_line)
        if header_title:
            if current_lines:
                blocks.append((current_header, current_lines))
            current_header = header_title
            current_lines = [raw_line.rstrip()]
            in_block = True
            continue

        if in_block:
            current_lines.append(raw_line.rstrip())
        else:
            prefix.append(raw_line.rstrip())

    if current_lines:
        blocks.append((current_header, current_lines))

    return prefix, blocks


def _keyword_hits(report_type: str, text: str) -> int:
    lowered = text.lower()
    return sum(1 for keyword in REPORT_KEYWORDS.get(report_type, ()) if keyword in lowered)


def _structural_hits(report_type: str, text: str) -> int:
    lowered = text.lower()
    patterns_by_type: dict[str, tuple[str, ...]] = {
        "sales": (
            r"\bz[ /_]?reading\s*[:=]",
            r"\bt/?cash\s*[:=]",
            r"\bt/?card\s*[:=]",
            r"\btotal sales\s*[:=]",
            r"\bmain door\s*[:=]",
            r"\bcustomers served\s*[:=]",
            r"\bguest/?\s*customer served\s*[:=]",
        ),
        "staff_performance": (
            r"\bsection\s*[:=]",
            r"\barrangements?\s*[:=]",
            r"\bdisplay\s*[:=]",
            r"\bperformance\s*[:=]",
            r"\bassisting customers\s*[:=]",
            r"\bitems moved\s*[:=]",
        ),
        "staff_attendance": (
            r"\battendance\b",
            r"\bpresent\s*[:=]",
            r"\babsent\s*[:=]",
            r"\boff(?: duty)?\s*[:=]",
            r"\b(?:annual leave|sick leave|leave)\s*[:=]",
            r"✔",
        ),
        "legacy_supervisor": (
            r"\bstaffing issues\s*[:=]",
            r"\bstock issues.*[:=]",
            r"\bpricing.*issues\s*[:=]",
            r"\bexceptions escalated.*[:=]",
            r"\bsupervisor confirmation\s*[:=]",
        ),
        "bale_summary": (
            r"\bbale(?: id|_id)?\s*[:=]",
            r"\bitem(?: name|_name)?\s*[:=]",
            r"\b(?:qty|total qty)\s*[:=]",
            r"\b(?:amount|total amount)\s*[:=]",
        ),
    }
    return sum(1 for pattern in patterns_by_type.get(report_type, ()) if re.search(pattern, lowered))


def is_attendance_format(text: str) -> bool:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    status_hits = sum(1 for line in lines if ATTENDANCE_STATUS_RE.search(line))
    total_hits = sum(1 for line in lines if ATTENDANCE_TOTAL_RE.search(line))
    return status_hits >= 2 or (status_hits >= 1 and total_hits >= 1)


def finalize_report_type(raw_type: str, text: str) -> str:
    if raw_type == "legacy_supervisor" and is_attendance_format(text):
        return "staff_attendance"
    return raw_type


def build_report_blocks(text: str) -> list[ReportBlock]:
    lines = (text or "").splitlines()
    prefix_lines, sections = _split_known_blocks(lines)
    prefix_text = "\n".join(prefix_lines).strip()

    blocks: list[ReportBlock] = []
    for header, raw_lines in sections:
        raw_type = REPORT_HEADER_MAP.get(header, "unknown")
        section_text = "\n".join(raw_lines).strip()
        contextual_text = section_text if not prefix_text else f"{prefix_text}\n\n{section_text}"
        report_type = finalize_report_type(raw_type, contextual_text)
        keyword_hits = _keyword_hits(report_type, contextual_text)
        structural_hits = _structural_hits(report_type, contextual_text)
        kv_lines = sum(1 for line in raw_lines if KEY_VALUE_LINE_RE.match(line))
        density_score = float(keyword_hits + structural_hits + min(kv_lines, 6)) / max(len(raw_lines), 1)
        score = 6.0 + keyword_hits + (structural_hits * 1.5) + min(kv_lines, 6) + round(density_score, 2)
        blocks.append(
            ReportBlock(
                raw_type=raw_type,
                report_type=report_type,
                header=header,
                section_text=section_text,
                contextual_text=contextual_text,
                keyword_hits=keyword_hits,
                structural_hits=structural_hits,
                density_score=round(density_score, 3),
                score=round(score, 3),
            )
        )

    return blocks


def _fallback_block(text: str) -> ReportBlock | None:
    title = normalize_title(next((line for line in (text or "").splitlines() if line.strip()), ""))
    best_type = "unknown"
    best_score = 0.0
    best_keyword_hits = 0
    best_structural_hits = 0
    for report_type in REPORT_KEYWORDS:
        keyword_hits = _keyword_hits(report_type, text)
        structural_hits = _structural_hits(report_type, text)
        score = keyword_hits + (structural_hits * 1.5)
        if score > best_score:
            best_type = finalize_report_type(report_type, text)
            best_score = score
            best_keyword_hits = keyword_hits
            best_structural_hits = structural_hits

    if best_type == "unknown" or best_score < 2.0:
        return None

    return ReportBlock(
        raw_type=best_type,
        report_type=best_type,
        header=title,
        section_text=(text or "").strip(),
        contextual_text=(text or "").strip(),
        keyword_hits=best_keyword_hits,
        structural_hits=best_structural_hits,
        density_score=0.0,
        score=round(best_score, 3),
    )


def select_report_block(text: str) -> tuple[ReportBlock | None, list[ReportBlock], bool]:
    blocks = build_report_blocks(text)
    if not blocks:
        fallback = _fallback_block(text)
        if fallback is None:
            return None, [], False
        return fallback, [fallback], False

    ranked = sorted(blocks, key=lambda item: (item.score, item.density_score, item.header), reverse=True)
    selected = ranked[0]
    ambiguous = False
    if len(ranked) > 1:
        runner_up = ranked[1]
        ambiguous = runner_up.report_type != selected.report_type and abs(selected.score - runner_up.score) < 2.0
    return selected, ranked, ambiguous


def extract_selected_report_text(text: str, expected_report_type: str | None = None) -> str:
    selected, _, _ = select_report_block(text)
    if selected and (expected_report_type is None or selected.report_type == expected_report_type):
        return selected.contextual_text
    return (text or "").strip()


def strong_signal_types(text: str) -> list[str]:
    selected, ranked, _ = select_report_block(text)
    if not ranked:
        return [selected.report_type] if selected else []
    return [block.report_type for block in ranked if block.score >= 6.5]


def iter_attendance_rows(text: str) -> Iterable[tuple[str, str]]:
    allowed_statuses = {
        "✔",
        "PRESENT",
        "ABSENT",
        "OFF",
        "OFF DUTY",
        "LEAVE",
        "ANNUAL LEAVE",
        "SICK LEAVE",
    }
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = re.match(r"^(?:\d+[\.\)]\s*)?([A-Za-z][A-Za-z .'-]+?)\s*[:\-]\s*(.+?)\s*$", line)
        if not match:
            continue
        status = re.sub(r"\s+", " ", match.group(2).strip()).upper()
        if status not in allowed_statuses:
            continue
        yield match.group(1).strip(), match.group(2).strip()
