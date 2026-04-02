# scripts/section_master_data.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import yaml


MASTER_SECTIONS_DIR = Path("MASTER_DATA/sections")


@dataclass
class SectionMatch:
    canonical: str
    section_type: str
    product_families: list[str]


def _clean(text: str) -> str:
    text = (text or "").strip().lower()
    text = text.replace("&", " and ")
    text = text.replace("#", " ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def load_branch_section_file(branch_slug: str) -> dict:
    path = MASTER_SECTIONS_DIR / f"{branch_slug}.yaml"
    if not path.exists():
        return {"branch": branch_slug, "sections": []}

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        return {"branch": branch_slug, "sections": []}

    sections = data.get("sections", [])
    if not isinstance(sections, list):
        sections = []

    return {
        "branch": data.get("branch", branch_slug),
        "sections": sections,
    }


def resolve_section_from_master_data(raw_section: str, branch_slug: str) -> SectionMatch | None:
    raw_clean = _clean(raw_section)
    if not raw_clean:
        return None

    data = load_branch_section_file(branch_slug)

    for section in data["sections"]:
        canonical = _clean(section.get("canonical", ""))
        section_type = (section.get("section_type") or "resolved").strip().lower()
        product_families = section.get("product_families") or []
        aliases = section.get("aliases") or []

        candidates = [canonical, *aliases]
        normalized_candidates = [_clean(x) for x in candidates if x]

        for candidate in normalized_candidates:
            if raw_clean == candidate:
                return SectionMatch(
                    canonical=canonical,
                    section_type=section_type,
                    product_families=product_families,
                )

    # soft containment fallback
    for section in data["sections"]:
        canonical = _clean(section.get("canonical", ""))
        section_type = (section.get("section_type") or "resolved").strip().lower()
        product_families = section.get("product_families") or []
        aliases = section.get("aliases") or []

        candidates = [canonical, *aliases]
        normalized_candidates = [_clean(x) for x in candidates if x]

        for candidate in normalized_candidates:
            if candidate and (candidate in raw_clean or raw_clean in candidate):
                return SectionMatch(
                    canonical=canonical,
                    section_type=section_type,
                    product_families=product_families,
                )

    return None
