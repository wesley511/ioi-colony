#!/usr/bin/env python3
from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
BRANCH_SECTION_DIR = ROOT / "MASTER_DATA" / "branches"
SECTION_MASTER_DIR = ROOT / "MASTER_DATA" / "sections"

_PLACEHOLDER_VALUES = {
    "",
    "unknown",
    "unknown_section",
    "n_a",
    "na",
    "nil",
    "none",
}

_STATIC_ALIAS_MAP = {
    "cashier#1": "cashier",
    "cashier_#1": "cashier",
    "cashier #1": "cashier",
    "cashier till # 1": "cashier_till_1",
    "cashier till #1": "cashier_till_1",
    "cashier till 1": "cashier_till_1",
    "cashier#2": "cashier",
    "cashier_#2": "cashier",
    "cashier #2": "cashier",
    "cashier till # 2": "cashier_till_2",
    "cashier till #2": "cashier_till_2",
    "cashier till 2": "cashier_till_2",
    "cashier#3": "cashier",
    "cashier_#3": "cashier",
    "cashier #3": "cashier",
    "cashier till # 3": "cashier_till_3",
    "cashier till #3": "cashier_till_3",
    "cashier till 3": "cashier_till_3",
    "pricing room": "pricing_room",
    "pricing_room": "pricing_room",
    "pricing clerk": "pricing_clark",
    "pricing clark": "pricing_clark",
    "monitor till # 1 & 2": "monitor_tills",
    "monitor till #1 & 2": "monitor_tills",
    "monitor till 1 & 2": "monitor_tills",
    "monitor_tills": "monitor_tills",
    "assisting cashier": "assisting_cashier",
    "monitoring": "monitoring",
    "monitoring stuff and rails": "monitoring_stuff_and_rails",
    "mens shirts": "mens_button_shirts",
    "men shirts": "mens_button_shirts",
    "men's shirts": "mens_button_shirts",
    "mans button shirts": "mans_button_shirts",
    "mans button shirts and mans mans shorts": "mans_button_shirts_and_mans_mans_shorts",
    "mens button shirts and mens shorts": "mans_button_shirts_and_mans_mans_shorts",
    "mens shorts": "mens_shorts",
    "mans shorts": "mens_shorts",
    "mens t shirts": "mens_tshirt",
    "mens tshirts": "mens_tshirt",
    "men's t shirts": "mens_tshirt",
    "mens tshirt": "mens_tshirt",
    "mens jeans": "mens_jeans",
    "mens cotton pants": "mens_cotton_pants",
    "men's cotton pants": "mens_cotton_pants",
    "mans cotton pants": "mens_cotton_pants",
    "mens office pants": "mens_office_pants",
    "mans office pants": "mens_office_pants",
    "mans shoe": "mens_shoes",
    "mens shoe": "mens_shoes",
    "shoe shop": "shoe_shop",
    "beach wears and soccer shorts": "beach_wears_and_soccer_shorts",
    "beach_wears_and_soccer_shorts": "beach_wears_and_soccer_shorts",
    "soccer shorts and shirts": "soccer_shorts_and_shirts",
    "pacific shirts": "pacific_shirts",
    "mixed army pants": "mixed_army_pants",
    "m6pkts": "mens_6_pockets",
    "ladies jeans": "ladies_jeans",
    "ladies tops jeans": "ladies_tops_jeans",
    "ladies_tops_jeans": "ladies_tops_jeans",
    "ladies t shirts": "ladies_tshirts",
    "ladies tshirts": "ladies_tshirts",
    "ladies blouse": "ladies_blouse",
    "ladies t shirts & blouse": "ladies_tshirts_blouse",
    "ladies t shirts  & blouse": "ladies_tshirts_blouse",
    "ladies_tshirts_blouse": "ladies_tshirts_blouse",
    "ladies dress": "ladies_dress",
    "skirt": "skirts",
    "skirts": "skirts",
    "ladies shots": "ladies_shorts",
    "ladies shorts": "ladies_shorts",
    "skirt,cotton caprice pants & ladies shots": "skirts_caprice_ladies_shorts",
    "skirt, cotton caprice pants & ladies shots": "skirts_caprice_ladies_shorts",
    "skirt cotton caprice pants and ladies shots": "skirts_caprice_ladies_shorts",
    "cotton caprice pants": "cotton_caprice_pants",
    "ladies leggings & ladies 3/4 jeans": "ladies_leggings_3_4_jeans",
    "ladies leggings & ladies ³/⁴ jeans": "ladies_leggings_3_4_jeans",
    "ladies leggings and ladies 3/4 jeans": "ladies_leggings_3_4_jeans",
    "cream rails": "cream_rails",
    "mixed rails": "mixed_rails",
    "mix rails": "mixed_rails",
    "jacket section & reflectors": "jackets_reflectors",
    "jacket section and reflectors": "jackets_reflectors",
    "jackets_reflectors": "jackets_reflectors",
    "boys": "boys",
    "boys pants": "boys_pants",
    "boys shorts": "boys_shorts",
    "boys pants shorts army mix t shirt": "boys_pants_shorts_army_mix_tshirt",
    "boys_pants,_shorts,_army_mix,_t_shirt": "boys_pants_shorts_army_mix_tshirt",
    "kids boys section": "kids_boys",
    "kids boys": "kids_boys",
    "kids girls section": "kids_girls",
    "kids girls": "kids_girls",
    "ladies kids": "ladies_kids",
    "beddings": "beddings",
    "door": "door",
    "main door guards": "door_guard_main",
    "small door guards": "door_guard_small",
}


def slugify(text: str) -> str:
    value = text.lower().strip()
    value = value.replace("³/⁴", "3_4").replace("3/4", "3_4")
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_") or "unknown"


def strip_numeric_prefix(text: str) -> str:
    value = text.strip()
    value = re.sub(r"^\d+[\.\)\s_-]*", "", value)
    return value.strip()


def _normalize_lookup_key(raw: str) -> str:
    text = (raw or "").strip().lower()
    text = text.replace("³/⁴", "3/4")
    text = strip_numeric_prefix(text)
    text = re.sub(r"[.]+$", "", text)
    text = text.replace("#", " #")
    text = text.replace("ladies shots", "ladies shorts")
    text = text.replace("mix rails", "mixed rails")
    text = text.replace("t shirts", "tshirts")
    text = text.replace("t shirt", "tshirt")
    text = re.sub(r"\s+", " ", text).strip()
    return text


@lru_cache(maxsize=1)
def _section_alias_index() -> dict[str, str]:
    index = {slugify(key): value for key, value in _STATIC_ALIAS_MAP.items()}
    for value in _STATIC_ALIAS_MAP.values():
        index.setdefault(slugify(value), value)

    for path in sorted(BRANCH_SECTION_DIR.glob("*_sections.yaml")):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        sections = data.get("sections") or []
        for section in sections:
            if not isinstance(section, dict):
                continue
            name = str(section.get("name") or "").strip()
            if name:
                index.setdefault(slugify(name), slugify(name))
            for alias in section.get("items") or []:
                if alias:
                    index[slugify(str(alias))] = slugify(name)

    for path in sorted(SECTION_MASTER_DIR.glob("*.yaml")):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        sections = data.get("sections") or []
        for section in sections:
            if not isinstance(section, dict):
                continue
            canonical = slugify(str(section.get("canonical") or ""))
            if not canonical:
                continue
            index.setdefault(canonical, canonical)
            for alias in section.get("aliases") or []:
                if alias:
                    index[slugify(str(alias))] = canonical

    return index


@lru_cache(maxsize=1)
def canonical_sections() -> set[str]:
    return set(_section_alias_index().values())


def normalize_section_name(raw: str) -> str:
    key = slugify(_normalize_lookup_key(raw))
    if key in _PLACEHOLDER_VALUES:
        return ""
    return _section_alias_index().get(key, "")


def is_placeholder_section(raw: str) -> bool:
    key = slugify(_normalize_lookup_key(raw))
    return key in _PLACEHOLDER_VALUES
