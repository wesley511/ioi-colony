import math
import os
import re
from datetime import datetime, date

BLACKBOARD_PATH = "OPPORTUNITIES.md"
LOG_PATH = "LOGS/decay_worker.log"

RHO = 0.07
FLOOR = 0.05


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


def parse_iso_date(value: str) -> date | None:
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def split_blackboard_sections(content: str) -> tuple[str, str, str]:
    marker = "## Active Opportunities"
    if marker not in content:
        return content, "", ""

    before, after = content.split(marker, 1)

    exploring_marker = "## Exploring Opportunities"
    if exploring_marker in after:
        active_part, rest = after.split(exploring_marker, 1)
        return before, active_part, exploring_marker + rest

    return before, after, ""


def split_blocks(active_part: str) -> list[str]:
    parts = active_part.split("### [")
    if len(parts) <= 1:
        return []

    blocks = []
    for part in parts[1:]:
        block = "### [" + part
        blocks.append(block)
    return blocks


def extract_title(block: str) -> str:
    first = block.splitlines()[0].strip()
    if first.startswith("### [") and first.endswith("]"):
        return first[5:-1]
    return "UNKNOWN"


def extract_last_updated(block: str) -> str | None:
    matches = re.findall(r"^- last_updated:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*$", block, flags=re.MULTILINE)
    if matches:
        return matches[-1]
    return None


def extract_leverage_score(block: str) -> float | None:
    matches = re.findall(r"^- leverage_score:\s*([0-9]*\.?[0-9]+)\s*$", block, flags=re.MULTILINE)
    if matches:
        try:
            return float(matches[-1])
        except ValueError:
            return None
    return None


def replace_last_occurrence(pattern: str, replacement: str, text: str) -> str:
    matches = list(re.finditer(pattern, text, flags=re.MULTILINE))
    if not matches:
        return text
    m = matches[-1]
    return text[:m.start()] + replacement + text[m.end():]


def decay_score(old_score: float, days_since_update: int) -> float:
    return max(FLOOR, old_score * ((1 - RHO) ** days_since_update))


def process_block(block: str, today: date) -> tuple[str, str]:
    title = extract_title(block)
    last_updated_str = extract_last_updated(block)
    old_score = extract_leverage_score(block)

    if last_updated_str is None:
        log(f"SKIP {title} - missing last_updated")
        return block, f"SKIP {title} - missing last_updated"

    if old_score is None:
        log(f"SKIP {title} - missing leverage_score")
        return block, f"SKIP {title} - missing leverage_score"

    last_updated_date = parse_iso_date(last_updated_str)
    if last_updated_date is None:
        log(f"SKIP {title} - invalid last_updated {last_updated_str}")
        return block, f"SKIP {title} - invalid last_updated"

    days_since_update = (today - last_updated_date).days

    if days_since_update <= 0:
        log(f"NO_DECAY {title} - same day update")
        return block, f"NO_DECAY {title}"

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
    return updated_block, f"DECAY {title}"


def rebuild_active_part(active_part: str, updated_blocks: list[str]) -> str:
    leading_text = active_part.split("### [", 1)[0]
    rebuilt = leading_text.rstrip("\n") + "\n\n"
    rebuilt += "\n".join(block.rstrip() for block in updated_blocks)
    if not rebuilt.endswith("\n"):
        rebuilt += "\n"
    return rebuilt


def main() -> None:
    print("=== IOI Colony Decay Worker ===")

    if not os.path.exists(BLACKBOARD_PATH):
        print("Blackboard not found.")
        return

    content = read_file(BLACKBOARD_PATH)
    before, active_part, after = split_blackboard_sections(content)

    if not active_part:
        print("No active opportunities section found.")
        return

    blocks = split_blocks(active_part)
    if not blocks:
        print("No opportunity blocks found.")
        return

    today = date.today()
    updated_blocks = []

    for block in blocks:
        updated_block, _ = process_block(block, today)
        updated_blocks.append(updated_block)

    rebuilt_active = rebuild_active_part(active_part, updated_blocks)
    new_content = before + "## Active Opportunities" + rebuilt_active + after
    write_file(BLACKBOARD_PATH, new_content)

    print("=== Done ===")


if __name__ == "__main__":
    main()

