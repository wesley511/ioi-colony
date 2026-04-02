from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
BRANCH_ALIAS_PATH = ROOT / "MASTER_DATA" / "branch_aliases.yaml"

_LEGACY_ALIAS_OVERRIDES: dict[str, tuple[str, ...]] = {
    "waigani": (
        "pom waigani",
        "ttc pom waigani branch",
        "ttc pom waiganit",
        "ttc pom waiganit branch",
        "port moresby waigani",
        "ncd waigani",
    ),
    "bena_road": (
        "bena",
        "ttc bena",
        "ttc bena road",
        "bena road goroka",
        "goroka bena road",
        "goroka bena road branch",
        "ttc goroka",
        "goroka_bena_road",
    ),
    "lae_5th_street": (
        "5th_street",
        "5thstreet",
        "fifth_street",
        "fifth street",
        "ttc lae 5th street",
        "ttc 5th street lae",
        "lae fifth street",
        "fifth street lae",
        "5th st lae",
    ),
    "lae_malaita": (
        "lae malaita street",
        "ttc lae malaita",
        "ttc malaita street",
        "lae_malaita",
    ),
}

_LEGACY_SHORT_EXACT_ALIASES: dict[str, tuple[str, ...]] = {
    "waigani": (
        "ttc pom waiganit",
        "ttc waigani",
        "waigani",
    ),
    "bena_road": (
        "ttc bena road",
        "bena road",
        "bena_road",
    ),
    "lae_5th_street": (
        "ttc lae 5th street",
        "ttc 5th street",
        "5th street",
        "5th_street",
    ),
    "lae_malaita": (
        "ttc lae malaita",
        "lae malaita",
        "lae_malaita",
    ),
}

_STYLE_MAP: dict[str, dict[str, str]] = {
    "canonical_slug": {
        "waigani": "waigani",
        "bena_road": "bena_road",
        "lae_5th_street": "lae_5th_street",
        "lae_malaita": "lae_malaita",
    },
    "legacy_short_slug": {
        "waigani": "waigani",
        "bena_road": "bena_road",
        "lae_5th_street": "5th_street",
        "lae_malaita": "lae_malaita",
    },
    "legacy_upper": {
        "waigani": "WAIGANI",
        "bena_road": "BENA_ROAD",
        "lae_5th_street": "5TH_STREET",
        "lae_malaita": "LAE_MALAITA",
    },
}


def _normalize_branch_text(value: str) -> str:
    text = (value or "").strip().lower()
    text = text.replace("&", " and ")
    text = text.replace("/", " ")
    text = re.sub(r"[_\-]+", " ", text)
    text = re.sub(r"\bbranch\b", " ", text)
    text = re.sub(r"\bttc\b", " ", text)
    text = re.sub(r"\bpom\b", " port moresby ", text)
    text = re.sub(r"\bncd\b", " port moresby ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _slugify_branch_text(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


@lru_cache(maxsize=1)
def _load_branch_aliases() -> dict[str, tuple[str, ...]]:
    payload: Any = {}
    if BRANCH_ALIAS_PATH.exists():
        with BRANCH_ALIAS_PATH.open("r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh) or {}

    aliases: dict[str, tuple[str, ...]] = {}
    if isinstance(payload, dict):
        for canonical, values in payload.items():
            if not isinstance(canonical, str):
                continue
            alias_values = tuple(str(v) for v in (values or []) if str(v).strip())
            aliases[str(canonical).strip()] = alias_values

    return aliases


@lru_cache(maxsize=None)
def _alias_index(profile: str) -> dict[str, str]:
    index: dict[str, str] = {}

    if profile == "canonical":
        source = {
            canonical: tuple([*aliases, *_LEGACY_ALIAS_OVERRIDES.get(canonical, ())])
            for canonical, aliases in _load_branch_aliases().items()
        }
    elif profile == "legacy_short_exact":
        source = _LEGACY_SHORT_EXACT_ALIASES
    else:
        raise ValueError(f"Unsupported branch normalization profile: {profile}")

    for canonical, aliases in source.items():
        candidates = [canonical, *aliases]
        for candidate in candidates:
            normalized = _normalize_branch_text(candidate)
            if normalized:
                index[normalized] = canonical

    return index


def _resolve_canonical_branch(raw_text: str, *, match_substring: bool, profile: str) -> str | None:
    normalized = _normalize_branch_text(raw_text)
    if not normalized:
        return None

    if profile == "literal":
        return None

    if profile == "keyword_legacy":
        if "bena" in normalized:
            return "bena_road"
        if "malaita" in normalized:
            return "lae_malaita"
        if "waigani" in normalized:
            return "waigani"
        if "5th" in normalized:
            return "lae_5th_street"
        return None

    alias_index = _alias_index(profile)
    if normalized in alias_index:
        return alias_index[normalized]

    if not match_substring:
        return None

    for alias in sorted(alias_index, key=len, reverse=True):
        if alias and alias in normalized:
            return alias_index[alias]

    return None


def _format_known_branch(canonical: str, style: str) -> str:
    style_map = _STYLE_MAP.get(style)
    if style_map is None:
        raise ValueError(f"Unsupported branch normalization style: {style}")
    return style_map[canonical]


def _format_unknown_branch(raw_text: str | None, style: str, fallback: str) -> str | None:
    if fallback == "none":
        return None
    if fallback == "empty":
        return ""
    if fallback == "slugify":
        return _slugify_branch_text(raw_text or "")
    if fallback == "lower_token":
        return str(raw_text or "").strip().lower().replace(" ", "_")
    if fallback == "upper_raw":
        return str(raw_text or "").strip().upper()
    if fallback == "unknown":
        if style == "legacy_upper":
            return "UNKNOWN"
        return "unknown"
    raise ValueError(f"Unsupported branch normalization fallback: {fallback}")


def normalize_branch(
    raw_text: str | None,
    *,
    style: str = "canonical_slug",
    fallback: str = "unknown",
    match_substring: bool = True,
    profile: str = "canonical",
) -> str | None:
    canonical = _resolve_canonical_branch(
        raw_text or "",
        match_substring=match_substring,
        profile=profile,
    )
    if canonical:
        return _format_known_branch(canonical, style)
    return _format_unknown_branch(raw_text, style, fallback)
