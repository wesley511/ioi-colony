from __future__ import annotations

import re
from pathlib import Path
from typing import Tuple, Dict, Any

import yaml

from scripts.log_unresolved_product import log_unresolved_product

CATALOG_DIR = Path(__file__).resolve().parent.parent / "CATALOG"

SECTION_CATALOG_PATH = CATALOG_DIR / "section_catalog.yaml"
PRODUCT_ALIASES_PATH = CATALOG_DIR / "product_aliases.yaml"
SUPPLY_TYPES_PATH = CATALOG_DIR / "supply_types.yaml"


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data or {}


SECTION_CATALOG = _load_yaml(SECTION_CATALOG_PATH)
PRODUCT_ALIASES = _load_yaml(PRODUCT_ALIASES_PATH)
SUPPLY_TYPES = _load_yaml(SUPPLY_TYPES_PATH)


def normalize_product_text(text: str) -> str:
    """
    Normalize raw product text into a stable lookup key.
    Deterministic and intended to be idempotent.
    """
    if not text:
        return ""

    value = str(text).strip().lower()

    # Normalize separators / punctuation first
    value = value.replace("&", " and ")
    value = value.replace("/", " ")
    value = value.replace("-", " ")
    value = value.replace(".", " ")
    value = value.replace("'", " ")
    value = value.replace("(", " ")
    value = value.replace(")", " ")
    value = value.replace("[", " ")
    value = value.replace("]", " ")
    value = value.replace(",", " ")

    # Collapse whitespace early
    value = re.sub(r"\s+", " ", value).strip()

    # Word-level normalization
    value = re.sub(r"\bmen\b", "mens", value)
    value = re.sub(r"\bmans\b", "mens", value)
    value = re.sub(r"\blady\b", "ladies", value)
    value = re.sub(r"\bladys\b", "ladies", value)
    value = re.sub(r"\bwoman\b", "ladies", value)
    value = re.sub(r"\bwomen\b", "ladies", value)
    value = re.sub(r"\bchildrens\b", "children", value)
    value = re.sub(r"\bchildrens\b", "children", value)
    value = re.sub(r"\bboy\b", "boys", value)
    value = re.sub(r"\bgirl\b", "girls", value)

    # Phrase normalization
    replacements = {
        "t shirt": "tshirt",
        "t shirts": "tshirt",
        "tshirt": "tshirt",
        "tee shirt": "tshirt",
        "tee shirts": "tshirt",
        "rumage": "rummage",
        "tegel": "teregal",
        "light wieght": "lightweight",
        "light weight": "lightweight",
        "work wear": "workwear",
        "6 pocket": "six_pocket",
        "6 pockets": "six_pocket",
        "round neck": "round_neck",
        "v neck": "v_neck",
        "long sleeve": "long_sleeve",
        "short sleeve": "short_sleeve",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)

    # Collapse whitespace again after replacements
    value = re.sub(r"\s+", " ", value).strip()

    # Convert to underscore key
    value = value.replace(" ", "_")

    # Remove duplicate underscores
    value = re.sub(r"_+", "_", value).strip("_")

    return value


def _build_normalized_alias_index(aliases: Dict[str, Any]) -> Dict[str, str]:
    """
    Build a normalized-key alias index so both raw and normalized forms work.
    """
    index: Dict[str, str] = {}

    for raw_key, canonical_id in aliases.items():
        raw_key_str = str(raw_key).strip()
        if not raw_key_str:
            continue

        index[raw_key_str] = canonical_id
        index[normalize_product_text(raw_key_str)] = canonical_id

    return index


def _build_supply_code_index(supply_types: Dict[str, Any]) -> Dict[str, str]:
    """
    Build direct and normalized lookup for supply source codes.
    """
    index: Dict[str, str] = {}

    for canonical_id, meta in supply_types.items():
        codes = meta.get("source_codes", []) if isinstance(meta, dict) else []
        for code in codes:
            raw_code = str(code).strip()
            if not raw_code:
                continue

            index[raw_code] = canonical_id
            index[normalize_product_text(raw_code)] = canonical_id

    return index


NORMALIZED_ALIAS_INDEX = _build_normalized_alias_index(PRODUCT_ALIASES)
SUPPLY_CODE_INDEX = _build_supply_code_index(SUPPLY_TYPES)


def resolve_product(raw_name: str, source: str = "unknown") -> Tuple[str, str]:
    """
    Resolve a raw product name into a canonical colony product/supply ID.

    Returns:
        ("section", canonical_id)
        ("supply", canonical_id)
        ("unknown", normalized_key)
    """
    normalized_key = normalize_product_text(raw_name)

    if not normalized_key:
        return ("unknown", "")

    # 1) raw alias lookup
    canonical = PRODUCT_ALIASES.get(raw_name)

    # 2) normalized alias lookup
    if canonical is None:
        canonical = NORMALIZED_ALIAS_INDEX.get(normalized_key)

    if canonical:
        if canonical in SUPPLY_TYPES:
            return ("supply", canonical)
        if canonical in SECTION_CATALOG:
            return ("section", canonical)

    # 3) supply-code lookup
    supply_canonical = SUPPLY_CODE_INDEX.get(raw_name)
    if supply_canonical is None:
        supply_canonical = SUPPLY_CODE_INDEX.get(normalized_key)

    if supply_canonical:
        return ("supply", supply_canonical)

    log_unresolved_product(
        raw_name=raw_name,
        normalized_key=normalized_key,
        source=source,
    )
    return ("unknown", normalized_key)


def get_catalog_snapshot() -> dict:
    return {
        "sections": sorted(SECTION_CATALOG.keys()),
        "supplies": sorted(SUPPLY_TYPES.keys()),
        "aliases": len(PRODUCT_ALIASES),
        "normalized_aliases": len(NORMALIZED_ALIAS_INDEX),
        "supply_codes": len(SUPPLY_CODE_INDEX),
    }
