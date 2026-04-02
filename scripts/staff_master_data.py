from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MASTER_STAFF_PATH = ROOT / "STAFF" / "master_staff_list.md"

BRANCH_TO_SHOP_CODE = {
    "waigani": "WAI",
    "lae_5th_street": "5ST",
    "lae_malaita": "MAL",
    "bena_road": "BEN",
}


@dataclass(frozen=True)
class StaffMatch:
    staff_id: str
    display_name: str
    normalized_name: str
    shop_code: str


def _slug(text: str) -> str:
    value = (text or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_")


def _clean_name(text: str) -> str:
    value = (text or "").strip()
    value = value.replace(".", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


@lru_cache(maxsize=1)
def _load_staff_index() -> dict[tuple[str, str], StaffMatch]:
    index: dict[tuple[str, str], StaffMatch] = {}
    if not MASTER_STAFF_PATH.exists():
        return index

    current: dict[str, str] = {}
    for raw_line in MASTER_STAFF_PATH.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue

        if stripped.startswith("- "):
            if current:
                _store_record(index, current)
            current = {}
            stripped = stripped[2:].strip()

        if ":" not in stripped:
            continue

        key, value = stripped.split(":", 1)
        current[key.strip()] = value.strip().strip('"').strip("'")

    if current:
        _store_record(index, current)

    return index


def _store_record(index: dict[tuple[str, str], StaffMatch], record: dict[str, str]) -> None:
    shop_code = record.get("shop_code", "").strip().upper()
    staff_id = record.get("staff_id", "").strip()
    normalized_name = _slug(record.get("normalized_name", ""))
    full_name = _clean_name(record.get("full_name", ""))
    if not shop_code or not staff_id or not normalized_name:
        return

    display_name = full_name or normalized_name.replace("_", " ").title()
    match = StaffMatch(
        staff_id=staff_id,
        display_name=display_name,
        normalized_name=normalized_name,
        shop_code=shop_code,
    )
    index[(shop_code, normalized_name)] = match


def resolve_staff(branch_slug: str, raw_name: str) -> StaffMatch | None:
    shop_code = BRANCH_TO_SHOP_CODE.get(branch_slug)
    if not shop_code:
        return None
    normalized_name = _slug(_clean_name(raw_name))
    if not normalized_name:
        return None
    return _load_staff_index().get((shop_code, normalized_name))
