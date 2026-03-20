import os
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

SIGNALS_PATH = "SIGNALS/normalized"
BLACKBOARD_PATH = "OPPORTUNITIES.md"
LOG_PATH = "LOGS/colony_cycle.log"

REQUIRED_FIELDS = [
    "signal_id",
    "date",
    "source_type",
    "source_name",
    "category",
    "signal_type",
    "description",
    "confidence",
    "status",
]

DEFAULT_CREATE_SCORE = 0.60
DEFAULT_REINFORCE_DELTA = 0.03
MAX_SCORE = 1.00
PRIORITY_PATH = "PRIORITY.md"
PRIORITY_THRESHOLD = 0.80
HEALTH_PATH = "HEALTH.md"
RHO = 0.07
FLOOR = 0.05

ARCHIVE_SCORE_THRESHOLD = 0.40


def log(msg: str) -> None:
    os.makedirs("LOGS", exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} - {msg}\n")


def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_file(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    result = []
    for item in items:
        value = item.strip()
        if not value:
            continue
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def normalize_blank_lines(lines: List[str]) -> List[str]:
    normalized: List[str] = []
    blank_streak = 0

    for line in lines:
        if line.strip() == "":
            blank_streak += 1
            if blank_streak <= 1:
                normalized.append("")
        else:
            blank_streak = 0
            normalized.append(line)

    while normalized and normalized[-1].strip() == "":
        normalized.pop()

    return normalized


def parse_signal(content: str) -> Dict[str, object]:
    data: Dict[str, object] = {}
    lines = content.splitlines()
    current_key = None

    for raw_line in lines:
        line = raw_line.rstrip()

        if not line.strip():
            continue

        if line.startswith("  - ") and current_key == "evidence":
            data.setdefault("evidence", [])
            evidence_list = data["evidence"]
            if isinstance(evidence_list, list):
                evidence_list.append(line.replace("  - ", "", 1).strip())
            continue

        if ": " in line:
            key, value = line.split(": ", 1)
            key = key.strip()
            value = value.strip()

            if key == "evidence":
                data["evidence"] = []
                current_key = "evidence"
            else:
                data[key] = value
                current_key = key
        elif line.endswith(":"):
            key = line[:-1].strip()
            if key == "evidence":
                data["evidence"] = []
            current_key = key

    if "evidence" not in data:
        data["evidence"] = []

    return data


def validate_signal(data: Dict[str, object]) -> Tuple[bool, str]:
    for field in REQUIRED_FIELDS:
        if field not in data or str(data[field]).strip() == "":
            return False, f"missing required field: {field}"

    try:
        confidence = float(str(data["confidence"]))
    except ValueError:
        return False, "confidence is not numeric"

    if confidence < 0.30:
        return False, "confidence below minimum threshold"

    if str(data.get("status", "")).strip().lower() != "new":
        return False, f"signal status is not new: {data.get('status')}"

    return True, "ok"


def make_title(data: Dict[str, object]) -> str:
    signal_type = str(data["signal_type"]).replace("_", " ").title()
    category = str(data["category"]).replace("_", " ").title()
    return f"{signal_type} — {category}"


def parse_sections(content: str) -> Tuple[str, List[Tuple[str, str]]]:
    matches = list(re.finditer(r"(?m)^## .+$", content))
    if not matches:
        return content.rstrip() + "\n", []

    preamble = content[:matches[0].start()].rstrip() + "\n"
    sections: List[Tuple[str, str]] = []

    for i, match in enumerate(matches):
        header_line = match.group(0).strip()
        title = header_line[3:].strip()
        body_start = match.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        body = content[body_start:body_end]
        sections.append((title, body))

    return preamble, sections


def get_section_body(sections: List[Tuple[str, str]], title: str) -> str:
    for section_title, body in sections:
        if section_title == title:
            return body
    return ""


def split_blocks(section_body: str) -> List[str]:
    parts = re.split(r"(?m)^### \[", section_body)
    if len(parts) <= 1:
        return []

    blocks = []
    for part in parts[1:]:
        block = "### [" + part
        block = block.strip()
        if block:
            blocks.append(block)
    return blocks


def extract_title(block: str) -> str:
    first = block.splitlines()[0].strip()
    if first.startswith("### [") and first.endswith("]"):
        return first[5:-1]
    return "UNKNOWN"


def extract_simple_field(block: str, field_name: str) -> Optional[str]:
    pattern = rf"(?m)^- {re.escape(field_name)}:\s*(.+?)\s*$"
    matches = re.findall(pattern, block)
    return matches[-1].strip() if matches else None


def extract_leverage_score(block: str) -> Optional[float]:
    value = extract_simple_field(block, "leverage_score")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def extract_confidence(block: str) -> Optional[float]:
    value = extract_simple_field(block, "confidence")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def extract_last_updated(block: str) -> Optional[str]:
    return extract_simple_field(block, "last_updated")


def extract_last_reinforced_date(block: str) -> Optional[str]:
    match = re.search(
        r"(?ms)^- last_reinforced:\s*$.*?^\s+- date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*$",
        block,
    )
    return match.group(1).strip() if match else None


def extract_existing_evidence(block: str) -> List[str]:
    evidence = []
    in_evidence = False

    for line in block.splitlines():
        stripped = line.strip()

        if stripped == "- evidence_sources:":
            in_evidence = True
            continue

        if in_evidence:
            if stripped.startswith("- ") and not line.startswith("  - "):
                break
            if line.startswith("  - "):
                evidence.append(line.replace("  - ", "", 1).strip())

    return evidence


def extract_score_components(block: str) -> Optional[Dict[str, float]]:
    lines = block.splitlines()
    in_components = False
    components: Dict[str, float] = {}

    for line in lines:
        stripped = line.strip()

        if stripped == "- score_components:":
            in_components = True
            continue

        if in_components:
            if stripped.startswith("- ") and not line.startswith("  - "):
                break
            if line.startswith("  - ") and ":" in stripped:
                key, value = stripped.replace("- ", "", 1).split(":", 1)
                key = key.strip()
                value = value.strip()
                try:
                    components[key] = float(value)
                except ValueError:
                    pass

    return components or None


def default_score_components() -> Dict[str, float]:
    return {
        "revenue": 0.60,
        "scalability": 0.60,
        "ease": 0.60,
        "strategic": 0.60,
        "wellbeing": 0.50,
    }


def format_score_components(components: Optional[Dict[str, float]]) -> List[str]:
    if not components:
        return []

    ordered_keys = ["revenue", "scalability", "ease", "strategic", "wellbeing"]
    lines = ["- score_components:"]
    used = set()

    for key in ordered_keys:
        if key in components:
            lines.append(f"  - {key}: {components[key]:.2f}")
            used.add(key)

    for key, value in components.items():
        if key not in used:
            lines.append(f"  - {key}: {value:.2f}")

    return lines


def parse_iso_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def replace_last_occurrence(pattern: str, replacement: str, text: str) -> str:
    matches = list(re.finditer(pattern, text, flags=re.MULTILINE))
    if not matches:
        return text
    match = matches[-1]
    return text[:match.start()] + replacement + text[match.end():]


def block_matches_signal(block: str, data: Dict[str, object]) -> bool:
    signal_id = str(data.get("signal_id", "")).strip()
    category = str(data.get("category", "")).strip().lower()
    signal_type = str(data.get("signal_type", "")).strip().lower()

    lower_block = block.lower()

    if signal_id and signal_id in block:
        return True

    category_exact = f"- category: {category}" in lower_block
    signal_type_exact = f"- signal_type: {signal_type}" in lower_block
    return category_exact and signal_type_exact


def find_matching_block(blocks: List[str], data: Dict[str, object]) -> Tuple[Optional[int], Optional[str]]:
    for idx, block in enumerate(blocks):
        if block_matches_signal(block, data):
            return idx, block
    return None, None


def build_canonical_block(
    *,
    title: str,
    source: str,
    category: str,
    signal_type: str,
    date_identified: str,
    description: str,
    leverage_score: float,
    risk_level: str,
    confidence: float,
    score_components: Optional[Dict[str, float]],
    evidence_sources: List[str],
    rationale: str,
    last_reinforced_date: str,
    last_reinforced_delta: float,
    last_reinforced_reason: str,
    status: str,
    review_status: str,
    last_updated: str,
) -> str:
    lines: List[str] = [
        f"### [{title}]",
        "",
        f"- source: {source}",
        f"- category: {category}",
        f"- signal_type: {signal_type}",
        f"- date_identified: {date_identified}",
        f"- description: {description}",
        "",
        f"- leverage_score: {leverage_score:.2f}",
        f"- risk_level: {risk_level}",
        f"- confidence: {confidence:.2f}",
    ]

    component_lines = format_score_components(score_components)
    if component_lines:
        lines.append("")
        lines.extend(component_lines)

    lines.extend(
        [
            "",
            "- evidence_sources:",
        ]
    )
    for item in evidence_sources:
        lines.append(f"  - {item}")

    lines.extend(
        [
            "",
            f"- rationale: {rationale}",
            "",
            "- last_reinforced:",
            f"  - date: {last_reinforced_date}",
            f"  - delta: {last_reinforced_delta:.2f}",
            f"  - reason: {last_reinforced_reason}",
            "",
            f"- status: {status}",
            f"- review_status: {review_status}",
            f"- last_updated: {last_updated}",
            "",
            "---",
        ]
    )

    return "\n".join(normalize_blank_lines(lines))


def build_opportunity_block(data: Dict[str, object]) -> str:
    title = make_title(data)
    today = str(data["date"])
    confidence = float(str(data["confidence"]))

    evidence_items = [
        f"signal {data['signal_id']}",
        str(data["description"]).strip(),
    ]

    if isinstance(data.get("evidence"), list):
        evidence_items.extend(str(item).strip() for item in data["evidence"])

    return build_canonical_block(
        title=title,
        source=f"signal {data['signal_id']}",
        category=str(data["category"]),
        signal_type=str(data["signal_type"]),
        date_identified=today,
        description=str(data["description"]),
        leverage_score=DEFAULT_CREATE_SCORE,
        risk_level="Low",
        confidence=confidence,
        score_components=default_score_components(),
        evidence_sources=dedupe_preserve_order(evidence_items),
        rationale="Initial opportunity created from validated normalized signal.",
        last_reinforced_date=today,
        last_reinforced_delta=0.00,
        last_reinforced_reason="initial opportunity creation from signal ingestion",
        status="Active",
        review_status="Pending",
        last_updated=today,
    )


def reinforce_block(block: str, data: Dict[str, object]) -> str:
    title = extract_title(block)
    signal_id = str(data["signal_id"]).strip()
    signal_date = str(data["date"]).strip()
    signal_description = str(data["description"]).strip()
    signal_confidence = float(str(data["confidence"]))

    source = extract_simple_field(block, "source") or f"signal {signal_id}"
    category = extract_simple_field(block, "category") or str(data["category"]).strip()
    signal_type = extract_simple_field(block, "signal_type") or str(data["signal_type"]).strip()
    date_identified = extract_simple_field(block, "date_identified") or signal_date
    description = extract_simple_field(block, "description") or signal_description
    risk_level = extract_simple_field(block, "risk_level") or "Low"
    status = extract_simple_field(block, "status") or "Active"
    review_status = extract_simple_field(block, "review_status") or "Pending"

    old_score = extract_leverage_score(block) or DEFAULT_CREATE_SCORE
    new_score = min(MAX_SCORE, old_score + DEFAULT_REINFORCE_DELTA)

    old_confidence = extract_confidence(block) or signal_confidence
    new_confidence = max(old_confidence, signal_confidence)

    existing_evidence = extract_existing_evidence(block)
    new_evidence = [
        f"signal {signal_id}",
        signal_description,
    ]
    if isinstance(data.get("evidence"), list):
        new_evidence.extend(str(item).strip() for item in data["evidence"])

    evidence_sources = dedupe_preserve_order(existing_evidence + new_evidence)

    score_components = extract_score_components(block)
    rationale = extract_simple_field(block, "rationale") or "Reinforced by new validated signal."

    return build_canonical_block(
        title=title,
        source=source,
        category=category,
        signal_type=signal_type,
        date_identified=date_identified,
        description=description,
        leverage_score=new_score,
        risk_level=risk_level,
        confidence=new_confidence,
        score_components=score_components,
        evidence_sources=evidence_sources,
        rationale=rationale,
        last_reinforced_date=signal_date,
        last_reinforced_delta=DEFAULT_REINFORCE_DELTA,
        last_reinforced_reason=f"reinforced by {signal_id}",
        status=status,
        review_status=review_status,
        last_updated=signal_date,
    )


def rebuild_blackboard(
    preamble: str,
    original_sections: List[Tuple[str, str]],
    active_blocks: List[str],
    archived_blocks: List[str],
) -> str:
    lines: List[str] = [preamble.rstrip(), ""]

    handled_active = False

    for title, body in original_sections:
        if title == "Active Opportunities":
            lines.append("## Active Opportunities")
            lines.append("")
            for idx, block in enumerate(active_blocks):
                lines.append(block.rstrip())
                if idx != len(active_blocks) - 1:
                    lines.append("")

            if archived_blocks:
                lines.append("")
                lines.append("## Archived Opportunities")
                lines.append("")
                for idx, block in enumerate(archived_blocks):
                    lines.append(block.rstrip())
                    if idx != len(archived_blocks) - 1:
                        lines.append("")

            handled_active = True
        elif title == "Archived Opportunities":
            continue
        else:
            lines.append(f"## {title}")
            body_text = body.strip("\n")
            if body_text:
                lines.append(body_text)
            lines.append("")

    if not handled_active:
        lines.append("## Active Opportunities")
        lines.append("")
        for idx, block in enumerate(active_blocks):
            lines.append(block.rstrip())
            if idx != len(active_blocks) - 1:
                lines.append("")

        if archived_blocks:
            lines.append("")
            lines.append("## Archived Opportunities")
            lines.append("")
            for idx, block in enumerate(archived_blocks):
                lines.append(block.rstrip())
                if idx != len(archived_blocks) - 1:
                    lines.append("")

    return "\n".join(normalize_blank_lines(lines)).rstrip() + "\n"


def mark_signal_processed(path: str) -> None:
    content = read_file(path)
    updated = re.sub(r"(?mi)^status:\s*new\s*$", "status: processed", content, count=1)
    write_file(path, updated)


def insert_block_into_active(blocks: List[str], block: str) -> List[str]:
    return [block] + blocks


def process_new_signals(content: str) -> str:
    preamble, sections = parse_sections(content)
    active_blocks = split_blocks(get_section_body(sections, "Active Opportunities"))
    archived_blocks = split_blocks(get_section_body(sections, "Archived Opportunities"))

    if not os.path.isdir(SIGNALS_PATH):
        log("SKIP signal processing - signals path missing")
        return content

    files = sorted(f for f in os.listdir(SIGNALS_PATH) if f.endswith(".md"))

    for filename in files:
        path = os.path.join(SIGNALS_PATH, filename)
        signal_text = read_file(path)

        if not re.search(r"(?mi)^status:\s*new\s*$", signal_text):
            continue

        data = parse_signal(signal_text)
        valid, reason = validate_signal(data)

        if not valid:
            log(f"SKIP {filename} - validation failed: {reason}")
            continue

        idx, match = find_matching_block(active_blocks, data)

        if match is not None and idx is not None:
            active_blocks[idx] = reinforce_block(match, data)
            mark_signal_processed(path)
            log(f"REINFORCE {filename} -> {make_title(data)}")
        else:
            new_block = build_opportunity_block(data)
            active_blocks = insert_block_into_active(active_blocks, new_block)
            mark_signal_processed(path)
            log(f"CREATE {filename} -> {make_title(data)}")

    return rebuild_blackboard(preamble, sections, active_blocks, archived_blocks)


def decay_score(old_score: float, days_since_update: int) -> float:
    return max(FLOOR, old_score * ((1 - RHO) ** days_since_update))


def apply_decay(content: str) -> str:
    preamble, sections = parse_sections(content)
    active_blocks = split_blocks(get_section_body(sections, "Active Opportunities"))
    archived_blocks = split_blocks(get_section_body(sections, "Archived Opportunities"))
    today = date.today()

    updated_blocks: List[str] = []

    for block in active_blocks:
        title = extract_title(block)
        last_updated_str = extract_last_updated(block)
        old_score = extract_leverage_score(block)

        if last_updated_str is None:
            log(f"SKIP_DECAY {title} - missing last_updated")
            updated_blocks.append(block)
            continue

        if old_score is None:
            log(f"SKIP_DECAY {title} - missing leverage_score")
            updated_blocks.append(block)
            continue

        last_updated_date = parse_iso_date(last_updated_str)
        if last_updated_date is None:
            log(f"SKIP_DECAY {title} - invalid last_updated {last_updated_str}")
            updated_blocks.append(block)
            continue

        days_since_update = (today - last_updated_date).days
        if days_since_update <= 0:
            log(f"NO_DECAY {title} - same day update")
            updated_blocks.append(block)
            continue

        new_score = decay_score(old_score, days_since_update)
        updated_block = block
        updated_block = replace_last_occurrence(
            r"^- leverage_score:\s*[0-9]*\.?[0-9]+\s*$",
            f"- leverage_score: {new_score:.2f}",
            updated_block,
        )
        updated_block = replace_last_occurrence(
            r"^- last_updated:\s*[0-9]{4}-[0-9]{2}-[0-9]{2}\s*$",
            f"- last_updated: {today.isoformat()}",
            updated_block,
        )

        log(
            f"DECAY {title} - old_score={old_score:.2f} "
            f"days={days_since_update} new_score={new_score:.2f}"
        )
        updated_blocks.append(updated_block)

    return rebuild_blackboard(preamble, sections, updated_blocks, archived_blocks)


def archive_stale_opportunities(content: str) -> str:
    preamble, sections = parse_sections(content)
    active_blocks = split_blocks(get_section_body(sections, "Active Opportunities"))
    archived_blocks = split_blocks(get_section_body(sections, "Archived Opportunities"))

    today_str = date.today().isoformat()
    still_active: List[str] = []
    newly_archived: List[str] = []

    for block in active_blocks:
        title = extract_title(block)
        leverage = extract_leverage_score(block)
        last_reinforced_date = extract_last_reinforced_date(block)

        should_archive = (
            leverage is not None
            and leverage < ARCHIVE_SCORE_THRESHOLD
            and last_reinforced_date != today_str
        )

        if should_archive:
            newly_archived.append(block)
            log(
                f"ARCHIVE {title} - leverage_score={leverage:.2f} "
                f"last_reinforced={last_reinforced_date or 'none'}"
            )
        else:
            still_active.append(block)

    final_archived = archived_blocks + newly_archived
    return rebuild_blackboard(preamble, sections, still_active, final_archived)

def normalize_block(block: str) -> str:
    title = extract_title(block)

    source = extract_simple_field(block, "source") or "unknown"
    category = extract_simple_field(block, "category") or "unknown"
    signal_type = extract_simple_field(block, "signal_type") or "unknown"
    date_identified = extract_simple_field(block, "date_identified") or date.today().isoformat()
    description = extract_simple_field(block, "description") or ""

    leverage_score = extract_leverage_score(block)
    if leverage_score is None:
        leverage_score = DEFAULT_CREATE_SCORE

    risk_level = extract_simple_field(block, "risk_level") or "Low"

    confidence = extract_confidence(block)
    if confidence is None:
        confidence = 0.50

    score_components = extract_score_components(block)
    evidence_sources = extract_existing_evidence(block)
    if not evidence_sources:
        evidence_sources = ["legacy block normalization"]

    rationale = extract_simple_field(block, "rationale") or "Normalized existing opportunity block."
    last_reinforced_date = extract_last_reinforced_date(block) or date_identified

    last_reinforced_delta_raw = None
    delta_match = re.search(r"(?m)^\s+- delta:\s*([0-9]*\.?[0-9]+)\s*$", block)
    if delta_match:
        try:
            last_reinforced_delta_raw = float(delta_match.group(1))
        except ValueError:
            last_reinforced_delta_raw = None
    last_reinforced_delta = last_reinforced_delta_raw if last_reinforced_delta_raw is not None else 0.00

    reason_match = re.search(r"(?m)^\s+- reason:\s*(.+?)\s*$", block)
    last_reinforced_reason = (
        reason_match.group(1).strip()
        if reason_match
        else "normalized existing block"
    )

    status = extract_simple_field(block, "status") or "Active"
    review_status = extract_simple_field(block, "review_status") or "Pending"
    last_updated = extract_last_updated(block) or date_identified

    return build_canonical_block(
        title=title,
        source=source,
        category=category,
        signal_type=signal_type,
        date_identified=date_identified,
        description=description,
        leverage_score=leverage_score,
        risk_level=risk_level,
        confidence=confidence,
        score_components=score_components,
        evidence_sources=dedupe_preserve_order(evidence_sources),
        rationale=rationale,
        last_reinforced_date=last_reinforced_date,
        last_reinforced_delta=last_reinforced_delta,
        last_reinforced_reason=last_reinforced_reason,
        status=status,
        review_status=review_status,
        last_updated=last_updated,
    )


def normalize_blackboard(content: str) -> str:
    preamble, sections = parse_sections(content)
    active_blocks = split_blocks(get_section_body(sections, "Active Opportunities"))
    archived_blocks = split_blocks(get_section_body(sections, "Archived Opportunities"))

    normalized_active = [normalize_block(block) for block in active_blocks]
    normalized_archived = [normalize_block(block) for block in archived_blocks]

    log(
        f"NORMALIZE active={len(normalized_active)} archived={len(normalized_archived)}"
    )

    return rebuild_blackboard(
        preamble,
        sections,
        normalized_active,
        normalized_archived,
    )

def build_priority_content(active_blocks: List[str]) -> str:
    priority_blocks = []

    for block in active_blocks:
        leverage = extract_leverage_score(block)
        if leverage is not None and leverage >= PRIORITY_THRESHOLD:
            priority_blocks.append(block.strip())

    lines = [
        "# IOI Colony Priority Opportunities",
        "",
        f"- generated_at: {datetime.now().isoformat()}",
        f"- threshold: {PRIORITY_THRESHOLD:.2f}",
        f"- count: {len(priority_blocks)}",
        "",
    ]

    if not priority_blocks:
        lines.append("No priority opportunities at this time.")
        lines.append("")
        return "\n".join(lines)

    lines.append("## Priority Opportunities")
    lines.append("")

    for i, block in enumerate(priority_blocks):
        lines.append(block)
        if i != len(priority_blocks) - 1:
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def update_priority_file(content: str) -> None:
    _, sections = parse_sections(content)
    active_blocks = split_blocks(get_section_body(sections, "Active Opportunities"))
    priority_content = build_priority_content(active_blocks)
    write_file(PRIORITY_PATH, priority_content)

    count = 0
    for block in active_blocks:
        leverage = extract_leverage_score(block)
        if leverage is not None and leverage >= PRIORITY_THRESHOLD:
            count += 1

    log(f"PRIORITY_UPDATE count={count} threshold={PRIORITY_THRESHOLD:.2f}")

def find_latest_log_event(keyword: str) -> Optional[str]:
    if not os.path.exists(LOG_PATH):
        return None

    try:
        lines = read_file(LOG_PATH).splitlines()
    except Exception:
        return None

    for line in reversed(lines):
        if keyword in line:
            return line.strip()

    return None


def count_priority_opportunities() -> int:
    if not os.path.exists(PRIORITY_PATH):
        return 0

    try:
        content = read_file(PRIORITY_PATH)
    except Exception:
        return 0

    match = re.search(r"(?m)^- count:\s*(\d+)\s*$", content)
    if not match:
        return 0

    try:
        return int(match.group(1))
    except ValueError:
        return 0


def update_health_summary(content: str) -> None:
    _, sections = parse_sections(content)
    active_blocks = split_blocks(get_section_body(sections, "Active Opportunities"))
    archived_blocks = split_blocks(get_section_body(sections, "Archived Opportunities"))

    priority_count = count_priority_opportunities()
    latest_reinforce = find_latest_log_event("REINFORCE")
    latest_archive = find_latest_log_event("ARCHIVE")
    latest_normalize = find_latest_log_event("NORMALIZE")

    lines = [
        "# IOI Colony Health Summary",
        "",
        f"- generated_at: {datetime.now().isoformat()}",
        f"- active_count: {len(active_blocks)}",
        f"- archived_count: {len(archived_blocks)}",
        f"- priority_count: {priority_count}",
        "",
        "## Latest Lifecycle Events",
        "",
        f"- latest_reinforce: {latest_reinforce if latest_reinforce else 'none'}",
        f"- latest_archive: {latest_archive if latest_archive else 'none'}",
        f"- latest_normalize: {latest_normalize if latest_normalize else 'none'}",
        "",
    ]

    write_file(HEALTH_PATH, "\n".join(lines).rstrip() + "\n")
    log(
        f"HEALTH_UPDATE active={len(active_blocks)} "
        f"archived={len(archived_blocks)} priority={priority_count}"
    )

def main() -> None:
    print("=== IOI Colony Unified Cycle ===")

    if not os.path.exists(BLACKBOARD_PATH):
        print("Blackboard not found.")
        return

    try:
        content = read_file(BLACKBOARD_PATH)
        content = process_new_signals(content)
        content = apply_decay(content)
        content = archive_stale_opportunities(content)
        content = normalize_blackboard(content)
        write_file(BLACKBOARD_PATH, content)
        update_priority_file(content)
        update_health_summary(content)
        print("=== Done ===")
    except Exception as exc:
        log(f"ERROR colony_cycle failed: {exc}")
        print(f"ERROR: {exc}")


if __name__ == "__main__":
    main()
