#!/usr/bin/env python3
from __future__ import annotations

import re


SECTION_ALIAS_MAP = {
    # cashier
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

    # pricing / monitor
    "pricing room": "pricing_room",
    "pricing_room": "pricing_room",
    "monitor till # 1 & 2": "monitor_tills",
    "monitor till #1 & 2": "monitor_tills",
    "monitor till 1 & 2": "monitor_tills",
    "monitor_tills": "monitor_tills",
    "assisting cashier": "assisting_cashier",

    # mens
    "mens shirts": "mens_shirts",
    "men shirts": "mens_shirts",
    "men's shirts": "mens_shirts",
    "mens t shirts": "mens_tshirts",
    "mens tshirts": "mens_tshirts",
    "men's t shirts": "mens_tshirts",
    "mens jeans": "mens_jeans",
    "mens cotton pants": "mens_cotton_pants",
    "men's cotton pants": "mens_cotton_pants",
    "mens shorts": "mens_shorts",
    "men's shorts": "mens_shorts",
    "mens shorts xl": "mens_shorts_xl",
    "mens office pants": "mens_office_pants",
    "mans office pants": "mens_office_pants",
    "mans cotton pants": "mens_cotton_pants",
    "mans shoe": "mens_shoes",
    "mens shoe": "mens_shoes",
    "shoe shop": "shoe_shop",
    "beach wears and soccer shorts": "beach_wears_and_soccer_shorts",
    "beach_wears_and_soccer_shorts": "beach_wears_and_soccer_shorts",
    "pacific shirts": "pacific_shirts",
    "mixed army pants": "mixed_army_pants",
    "m6pkts": "mens_6_pockets",

    # ladies
    "ladies jeans": "ladies_jeans",
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

    # kids
    "boys": "boys",
    "boys pants": "boys_pants",
    "boys shorts": "boys_shorts",
    "boys pants shorts army mix t shirt": "boys_pants_shorts_army_mix_tshirt",
    "boys_pants,_shorts,_army_mix,_t-shirt": "boys_pants_shorts_army_mix_tshirt",
    "kids boys section": "kids_boys",
    "kids boys": "kids_boys",
    "kids girls section": "kids_girls",
    "kids girls": "kids_girls",

    # other
    "beddings": "beddings",
    "door": "door",
    "main door guards": "door_guard_main",
    "small door guards": "door_guard_small",
}


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("³/⁴", "3_4").replace("3/4", "3_4")
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_") or "unknown"


def strip_numeric_prefix(text: str) -> str:
    text = text.strip()
    text = re.sub(r"^\d+[\.\)\s_-]*", "", text)
    return text.strip()


def normalize_section_name(raw: str) -> str:
    text = (raw or "").strip().lower()
    text = text.replace("³/⁴", "3/4")
    text = strip_numeric_prefix(text)
    text = re.sub(r"[.]+$", "", text)
    text = text.replace("#", " #")
    text = re.sub(r"\s+", " ", text).strip()

    # common repairs
    text = text.replace("ladies shots", "ladies shorts")
    text = text.replace("mix rails", "mixed rails")
    text = text.replace("t shirts", "tshirts")
    text = text.replace("t shirt", "tshirt")
    text = text.replace("and", "&")
    text = re.sub(r"\s+", " ", text).strip()

    if text in SECTION_ALIAS_MAP:
        return SECTION_ALIAS_MAP[text]

    fallback = slugify(text)
    return SECTION_ALIAS_MAP.get(fallback, fallback)
