from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping

try:
    from scripts.utils_normalization import normalize_branch as shared_normalize_branch
except ModuleNotFoundError:
    from utils_normalization import normalize_branch as shared_normalize_branch


_LEGACY_SHORT_STEMS = {
    "waigani": "waigani",
    "bena_road": "bena_road",
    "lae_5th_street": "5th_street",
    "lae_malaita": "lae_malaita",
}


def canonical_branch_slug(value: Any, fallback: str = "unknown") -> str:
    normalized = shared_normalize_branch(
        str(value or ""),
        style="canonical_slug",
        fallback="none" if fallback == "none" else "unknown",
        match_substring=True,
    )
    if normalized:
        return str(normalized)
    if fallback == "none":
        return ""
    return fallback


def legacy_branch_stem(branch_slug: str | None) -> str:
    canonical = canonical_branch_slug(branch_slug, fallback="unknown")
    return _LEGACY_SHORT_STEMS.get(canonical, canonical)


def legacy_branch_display(branch_slug: str | None) -> str:
    return legacy_branch_stem(branch_slug).upper()


def canonical_branch_display(branch_slug: str | None) -> str:
    return canonical_branch_slug(branch_slug, fallback="unknown").upper()


def branch_path_candidates(value: Any) -> list[str]:
    canonical = canonical_branch_slug(value, fallback="unknown")
    candidates = [canonical, legacy_branch_stem(canonical)]
    unique: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in unique:
            unique.append(candidate)
    return unique


def branch_slug_from_path(path: Path | str | None) -> str:
    if path is None:
        return "unknown"

    p = Path(path)
    candidates = [
        p.stem,
        p.name,
        p.parent.name,
        p.parent.parent.name if p.parent != p else "",
        " ".join(p.parts[-6:]),
    ]
    for candidate in candidates:
        normalized = canonical_branch_slug(candidate, fallback="none")
        if normalized:
            return normalized
    return "unknown"


def resolve_branch_slug(
    payload: Mapping[str, Any] | None = None,
    *,
    path: Path | str | None = None,
    candidates: Iterable[Any] = (),
    fallback: str = "unknown",
) -> str:
    ordered: list[Any] = []

    if payload:
        branch_slug_value = payload.get("branch_slug")
        if branch_slug_value is not None and str(branch_slug_value).strip():
            normalized = canonical_branch_slug(branch_slug_value, fallback=fallback)
            if normalized:
                return normalized

        ordered.extend(
            [
                payload.get("branch"),
                payload.get("source_slug"),
                payload.get("source_name"),
                payload.get("branch_name"),
                payload.get("shop"),
                payload.get("raw_branch"),
                payload.get("source"),
            ]
        )

    ordered.extend(candidates)

    for candidate in ordered:
        if candidate is None or not str(candidate).strip():
            continue
        normalized = canonical_branch_slug(candidate, fallback="none")
        if normalized:
            return normalized

    from_path = branch_slug_from_path(path)
    if from_path != "unknown":
        return from_path

    return fallback
