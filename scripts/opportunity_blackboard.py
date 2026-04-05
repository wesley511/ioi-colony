from __future__ import annotations

ACTIVE_MARKER = "## Active Opportunities"
EXPLORING_MARKER = "## Exploring Opportunities"
BLOCK_HEADER = "### ["


def split_blackboard_sections(content: str) -> tuple[str, str, str]:
    if ACTIVE_MARKER not in content:
        return content, "", ""

    before, after = content.split(ACTIVE_MARKER, 1)
    if EXPLORING_MARKER in after:
        active_part, rest = after.split(EXPLORING_MARKER, 1)
        return before, active_part, EXPLORING_MARKER + rest

    return before, after, ""


def _finalize_block_lines(lines: list[str]) -> list[str]:
    cleaned = list(lines)
    while cleaned and not cleaned[0].strip():
        cleaned.pop(0)
    while cleaned and (not cleaned[-1].strip() or cleaned[-1].strip() == "---"):
        cleaned.pop()
    return cleaned


def split_blocks(active_part: str) -> list[str]:
    blocks: list[str] = []
    current: list[str] = []

    for line in active_part.splitlines():
        if line.startswith(BLOCK_HEADER):
            if current:
                finalized = _finalize_block_lines(current)
                if finalized:
                    blocks.append("\n".join(finalized).rstrip() + "\n")
            current = [line]
            continue

        if current:
            current.append(line)

    if current:
        finalized = _finalize_block_lines(current)
        if finalized:
            blocks.append("\n".join(finalized).rstrip() + "\n")

    return blocks


def strip_block_separators(block: str) -> str:
    lines = [line for line in block.splitlines() if line.strip() != "---"]
    return "\n".join(lines).rstrip() + "\n"


def rebuild_active_part(active_part: str, blocks: list[str]) -> str:
    leading_text = active_part.split(BLOCK_HEADER, 1)[0]
    prefix = leading_text.rstrip("\n")
    body = "\n---\n\n".join(strip_block_separators(block).rstrip() for block in blocks).rstrip()

    if prefix:
        rebuilt = prefix + "\n\n"
    else:
        rebuilt = "\n\n"

    if body:
        rebuilt += body + "\n"

    return rebuilt
