import os
import re
from datetime import datetime
from typing import Dict, List, Tuple

SIGNALS_PATH = "SIGNALS/normalized"
BLACKBOARD_PATH = "OPPORTUNITIES.md"
LOG_PATH = "LOGS/worker_decision_v2.log"

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


def split_blocks(blackboard: str) -> List[str]:
    parts = blackboard.split("### [")
    if len(parts) == 1:
        return []
    blocks = []
    for part in parts[1:]:
        blocks.append("### [" + part)
    return blocks


def extract_block_title(block: str) -> str:
    first_line = block.splitlines()[0].strip()
    if first_line.startswith("### [") and first_line.endswith("]"):
        return first_line[5:-1].strip()
    return ""


def block_matches_signal(block: str, data: Dict[str, object]) -> bool:
    title = make_title(data)
    category = str(data.get("category", "")).strip().lower()
    signal_type = str(data.get("signal_type", "")).strip().lower()
    signal_id = str(data.get("signal_id", "")).strip()

    lower_block = block.lower()

    if signal_id and signal_id in block:
        return True

    if extract_block_title(block).strip().lower() == title.strip().lower():
        return True

    category_match = f"- category: {category}" in lower_block or category in lower_block
    signal_type_match = f"- signal_type: {signal_type}" in lower_block or signal_type in lower_block

    return category_match and signal_type_match


def find_matching_block(blackboard: str, data: Dict[str, object]) -> str:
    for block in split_blocks(blackboard):
        if block_matches_signal(block, data):
            return block
    return ""


def safe_float_from_line(line: str, prefix: str) -> float:
    try:
        return float(line.replace(prefix, "", 1).strip())
    except ValueError:
        return DEFAULT_CREATE_SCORE


def extract_existing_evidence(block: str) -> List[str]:
    evidence = []
    in_evidence = False

    for line in block.splitlines():
        stripped = line.strip()

        if stripped == "- evidence_sources:":
            in_evidence = True
            continue

        if in_evidence:
            if stripped.startswith("- ") and stripped != "- evidence_sources:":
                break
            if stripped.startswith("- last_reinforced:"):
                break
            if stripped.startswith("- status:"):
                break
            if stripped.startswith("- review_status:"):
                break
            if stripped.startswith("- last_updated:"):
                break
            if stripped.startswith("- rationale:"):
                break
            if stripped.startswith("- score_components:"):
                break
            if stripped.startswith("- ") and not line.startswith("  - "):
                break
            if line.startswith("  - "):
                evidence.append(line.replace("  - ", "", 1).strip())

    return evidence


def reinforce_block(block: str, data: Dict[str, object]) -> str:
    signal_id = str(data["signal_id"])
    signal_date = str(data["date"])
    description = str(data["description"])
    confidence = float(str(data["confidence"]))

    lines = block.splitlines()
    updated_lines: List[str] = []

    score_updated = False
    confidence_updated = False
    evidence_inserted = False
    in_evidence = False
    existing_evidence = extract_existing_evidence(block)

    new_evidence_items = [f"signal {signal_id}", description]
    if isinstance(data.get("evidence"), list):
        for item in data["evidence"]:
            item_str = str(item).strip()
            if item_str:
                new_evidence_items.append(item_str)

    dedup_new_evidence = []
    for item in new_evidence_items:
        if item not in existing_evidence and item not in dedup_new_evidence:
            dedup_new_evidence.append(item)

    for idx, line in enumerate(lines):
        stripped = line.strip()

        if stripped.startswith("- leverage_score:"):
            old_score = safe_float_from_line(stripped, "- leverage_score:")
            new_score = min(MAX_SCORE, old_score + DEFAULT_REINFORCE_DELTA)
            updated_lines.append(f"- leverage_score: {new_score:.2f}")
            score_updated = True
            continue

        if stripped.startswith("- confidence:"):
            try:
                old_conf = float(stripped.replace("- confidence:", "", 1).strip())
            except ValueError:
                old_conf = confidence
            new_conf = max(old_conf, confidence)
            updated_lines.append(f"- confidence: {new_conf:.2f}")
            confidence_updated = True
            continue

        updated_lines.append(line)

        if stripped == "- evidence_sources:":
            in_evidence = True
            continue

        if in_evidence:
            next_line = lines[idx + 1] if idx + 1 < len(lines) else ""
            next_stripped = next_line.strip()

            evidence_section_ending = (
                next_line == ""
                or (
                    not next_line.startswith("  - ")
                    and next_stripped.startswith("- ")
                )
            )

            if evidence_section_ending and not evidence_inserted:
                for item in dedup_new_evidence:
                    updated_lines.append(f"  - {item}")
                evidence_inserted = True
                in_evidence = False

    if not score_updated:
        updated_lines.append(f"- leverage_score: {DEFAULT_CREATE_SCORE + DEFAULT_REINFORCE_DELTA:.2f}")

    if not confidence_updated:
        updated_lines.append(f"- confidence: {confidence:.2f}")

    if "- evidence_sources:" not in block:
        updated_lines.append("")
        updated_lines.append("- evidence_sources:")
        for item in dedup_new_evidence:
            updated_lines.append(f"  - {item}")

    if "- rationale:" in block:
        pass
    else:
        updated_lines.append("")
        updated_lines.append("- rationale: Reinforced by new validated signal.")

    updated_lines.append("")
    updated_lines.append("- last_reinforced:")
    updated_lines.append(f"  - date: {signal_date}")
    updated_lines.append(f"  - delta: {DEFAULT_REINFORCE_DELTA:.2f}")
    updated_lines.append(f"  - reason: reinforced by {signal_id}")

    review_status_present = any(l.strip().startswith("- review_status:") for l in lines)
    status_present = any(l.strip().startswith("- status:") for l in lines)

    if not status_present:
        updated_lines.append("- status: Active")
    if not review_status_present:
        updated_lines.append("- review_status: Pending")

    updated_lines.append(f"- last_updated: {signal_date}")

    cleaned = remove_duplicate_terminal_fields(updated_lines)
    return "\n".join(cleaned).rstrip() + "\n"


def remove_duplicate_terminal_fields(lines: List[str]) -> List[str]:
    single_fields = {
        "- leverage_score:",
        "- confidence:",
        "- status:",
        "- review_status:",
        "- last_updated:",
        "- rationale:",
    }

    seen = {}
    for i, line in enumerate(lines):
        stripped = line.strip()
        for prefix in single_fields:
            if stripped.startswith(prefix):
                seen[prefix] = i

    result = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        keep = True
        for prefix in single_fields:
            if stripped.startswith(prefix) and seen.get(prefix) != i:
                keep = False
                break
        if keep:
            result.append(line)

    return result


def build_opportunity_block(data: Dict[str, object]) -> str:
    title = make_title(data)
    today = str(data["date"])
    confidence = float(str(data["confidence"]))

    evidence_lines = [
        f"  - signal {data['signal_id']}",
        f"  - {data['description']}",
    ]

    if isinstance(data.get("evidence"), list):
        for item in data["evidence"]:
            item_str = str(item).strip()
            if item_str:
                evidence_lines.append(f"  - {item_str}")

    evidence_text = "\n".join(evidence_lines)

    return f"""### [{title}]

- source: signal {data['signal_id']}
- category: {data['category']}
- signal_type: {data['signal_type']}
- date_identified: {today}
- description: {data['description']}

- leverage_score: {DEFAULT_CREATE_SCORE:.2f}
- risk_level: Low
- confidence: {confidence:.2f}

- score_components:
  - revenue: 0.60
  - scalability: 0.60
  - ease: 0.60
  - strategic: 0.60
  - wellbeing: 0.50

- evidence_sources:
{evidence_text}

- rationale: Initial opportunity created from validated normalized signal.

- last_reinforced:
  - date: {today}
  - delta: 0.00
  - reason: initial opportunity creation from signal ingestion

- status: Active
- review_status: Pending
- last_updated: {today}

---
"""


def insert_into_active_opportunities(blackboard: str, block: str) -> str:
    marker = "## Active Opportunities"
    if marker not in blackboard:
        return blackboard.rstrip() + "\n\n## Active Opportunities\n\n" + block + "\n"

    parts = blackboard.split(marker, 1)
    before = parts[0]
    after = parts[1]

    after_stripped = after.lstrip("\n")
    return before + marker + "\n\n" + block + "\n" + after_stripped


def mark_signal_processed(path: str) -> None:
    content = read_file(path)
    updated = content.replace("status: new", "status: processed", 1)
    write_file(path, updated)


def process_signal_file(filename: str) -> None:
    path = os.path.join(SIGNALS_PATH, filename)
    content = read_file(path)
    data = parse_signal(content)

    valid, reason = validate_signal(data)
    if not valid:
        log(f"SKIP {filename} - validation failed: {reason}")
        return

    blackboard = read_file(BLACKBOARD_PATH)
    match = find_matching_block(blackboard, data)

    if match:
        updated_block = reinforce_block(match, data)
        updated_blackboard = blackboard.replace(match, updated_block, 1)
        write_file(BLACKBOARD_PATH, updated_blackboard)
        mark_signal_processed(path)
        log(f"REINFORCE {filename} -> {make_title(data)}")
        return

    log(
        f"NO_MATCH {filename} "
        f"category={data.get('category', '')} "
        f"signal_type={data.get('signal_type', '')}"
    )

    block = build_opportunity_block(data)
    updated_blackboard = insert_into_active_opportunities(blackboard, block)
    write_file(BLACKBOARD_PATH, updated_blackboard)
    mark_signal_processed(path)
    log(f"CREATE {filename} -> {make_title(data)}")


def main() -> None:
    print("=== Decision Worker v2 ===")

    if not os.path.isdir(SIGNALS_PATH):
        print("Signals path not found.")
        return

    files = sorted([f for f in os.listdir(SIGNALS_PATH) if f.endswith(".md")])

    if not files:
        print("No signal files found.")
        return

    processed_any = False
    for filename in files:
        content = read_file(os.path.join(SIGNALS_PATH, filename))
        if "status: new" in content:
            process_signal_file(filename)
            processed_any = True

    if not processed_any:
        print("No new signals to process.")

    print("=== Done ===")


if __name__ == "__main__":
    main()

