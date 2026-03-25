from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

BRANCH_ALIASES = {
    "waigani": "WAIGANI",
    "ttc waigani": "WAIGANI",
    "ttc pom waigani branch": "WAIGANI",

    "bena": "GOROKA_BENA_ROAD",
    "bena road": "GOROKA_BENA_ROAD",
    "bena_road": "GOROKA_BENA_ROAD",
    "bena-road": "GOROKA_BENA_ROAD",
    "goroka bena road": "GOROKA_BENA_ROAD",
    "goroka_bena_road": "GOROKA_BENA_ROAD",

    "5th street": "LAE_5TH_STREET",
    "5th_street": "LAE_5TH_STREET",
    "lae 5th street": "LAE_5TH_STREET",
    "lae_5th_street": "LAE_5TH_STREET",

    "lae malaita": "LAE_MALAITA",
    "lae_malaita": "LAE_MALAITA",
    "malaita street lae": "LAE_MALAITA",
    "lae malaita street shop": "LAE_MALAITA",
}

ROOT = Path(__file__).resolve().parents[1]
MASTER_BRANCHES_DIR = ROOT / "MASTER_DATA" / "branches"
SIGNALS_BALES_DIR = ROOT / "SIGNALS" / "bales"
REPORTS_DIR = ROOT / "REPORTS"

def normalize_branch_name(branch: str) -> str:
    raw = str(branch or "").strip()
    if not raw:
        return ""

    key = raw.lower().strip()

    if key in BRANCH_ALIASES:
        return BRANCH_ALIASES[key]

    return raw.upper()


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("&", " and ")
    text = text.replace("/", " ")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s_]", " ", text)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected dict YAML in {path}")
    return data


@dataclass
class SectionRecord:
    id: int
    name: str
    items: list[str]


def load_branch_sections() -> dict[str, list[SectionRecord]]:
    result: dict[str, list[SectionRecord]] = {}
    if not MASTER_BRANCHES_DIR.exists():
        return result

    for path in MASTER_BRANCHES_DIR.glob("*_sections.yaml"):
        data = load_yaml(path)
        branch = normalize_branch_name(data.get("branch", ""))
        if not branch:
            continue

        sections: list[SectionRecord] = []
        for row in data.get("sections", []):
            if not isinstance(row, dict):
                continue
            sec_id = int(row.get("id", 0))
            sec_name = slugify(str(row.get("name", "")))
            raw_items = row.get("items", []) or []
            items = [slugify(str(x)) for x in raw_items if str(x).strip()]
            sections.append(SectionRecord(id=sec_id, name=sec_name, items=items))

        result[branch] = sections
    return result


def token_set(text: str) -> set[str]:
    return {t for t in slugify(text).split("_") if t}


def score_match(item_name: str, section: SectionRecord) -> int:
    item_tokens = token_set(item_name)
    sec_tokens = token_set(section.name)
    score = 0

    # direct section-token overlap
    score += len(item_tokens & sec_tokens) * 3

    # item catalog overlap if explicit items exist in section yaml
    for mapped_item in section.items:
        mapped_tokens = token_set(mapped_item)
        overlap = len(item_tokens & mapped_tokens)
        if overlap:
            score = max(score, overlap * 5)

        # full containment bonus
        if mapped_item == slugify(item_name):
            score = max(score, 20)

    # heuristic boosts for common clothing retail terms
    heuristics = [
        ("towel", ["towel", "towels"]),
        ("bed", ["bedding", "beddings", "bedsheet", "bedsheets", "bed_cover", "bed_covers"]),
        ("shoe", ["shoe_shop", "shoes_and_sandals", "shoes_sandals_toys"]),
        ("sand", ["shoe_shop", "shoes_and_sandals", "shoes_sandals_toys"]),
        ("toy", ["shoe_shop", "shoes_sandals_toys"]),
        ("short", ["mens_shorts", "kids_shorts", "ladies_shorts", "beachwear_soccer_shorts"]),
        ("jean", ["mens_jeans", "ladies_jeans", "ladies_skinny_jeans_cotton_pants", "mens_cotton_pants_jeans"]),
        ("dress", ["ladies_dress", "ladies_long_dress", "ladies_long_teregal_mini_dress", "girls_dress_ladies_swimwear"]),
        ("bra", ["bra_sports_bra", "ladies_bra_underwear", "ladies_underwear_bra"]),
        ("army", ["army_hardyaka_pants", "army_clothing_reflector", "creme_mens_tshirt_flannel_new_stock"]),
        ("reflector", ["reflectors", "army_clothing_reflector", "kids_girls_ladies_dresses_jackets"]),
        ("flannel", ["mens_button_flannel_shirts", "creme_mens_tshirt_flannel_new_stock"]),
        ("button", ["mens_button_flannel_shirts", "mens_button_shirts"]),
        ("jumpsuit", ["ladies_dress", "ladies_long_dress", "ladies_long_teregal_mini_dress"]),
        ("skirt", ["ladies_skirts", "ladies_skirt", "ladies_skirt_denim_skirt", "long_skirts"]),
        ("legging", ["ladies_sports_leggings", "ladies_cotton_capri_leggings", "ladies_leggings_tracksuits"]),
        ("capri", ["ladies_cotton_capri_leggings", "ladies_capri_colored_jeans"]),
        ("sports", ["mens_hawaiian_sports_tshirt", "beachwear_sportswear", "bra_sports_bra", "ladies_sports_leggings"]),
        ("polo", ["kids_boys_polo_jersey", "kids_polo_sweater", "men_section"]),
        ("kids", ["kids_boys_section", "kids_girls_tshirt_dress", "kids_girls_pants", "kids_shorts"]),
        ("baby", ["baby_light_rummage", "kids_girls_tshirt_baby_overalls"]),
	("towel", ["towels"]),
	("slipper", ["shoe_shop", "shoes_and_sandals", "shoes_sandals_toys"]),
	("sandals", ["shoe_shop", "shoes_and_sandals", "shoes_sandals_toys"]),
	("mens_tshirt", ["mens_tshirts", "mens_tshirt", "mens_tshirt_roundneck_collar"]),
	("boys_tshirt", ["mens_tshirt_roundneck_collar", "kids_boys_polo_jersey"]),
	("children_shorts", ["kids_shorts"]),
	("baby_army", ["baby_light_rummage", "kids_girls_tshirt_baby_overalls"]),
	("ladies_pants", ["ladies_capri_colored_jeans", "ladies_skinny_jeans_3_4", "ladies_skinny_jeans_cotton_pants"]),
	("mini_skirt", ["ladies_skirts", "ladies_skirt", "ladies_skirt_denim_skirt"]),
	("terigal_skirt", ["ladies_skirts", "ladies_skirt", "ladies_skirt_denim_skirt"]),
    ]

    item_slug = slugify(item_name)
    for needle, targets in heuristics:
        if needle in item_slug and section.name in {slugify(t) for t in targets}:
            score = max(score, 8)

    return score


def map_item_to_section(item_name: str, sections: list[SectionRecord]) -> dict[str, Any]:
    if not sections:
        return {
            "section_id": None,
            "section_name": "unmapped",
            "confidence": 0.0,
        }

    ranked: list[tuple[int, SectionRecord]] = []
    for sec in sections:
        ranked.append((score_match(item_name, sec), sec))
    ranked.sort(key=lambda x: x[0], reverse=True)

    best_score, best_sec = ranked[0]
    confidence = min(1.0, best_score / 20.0)

    if best_score <= 0:
        return {
            "section_id": None,
            "section_name": "unmapped",
            "confidence": 0.0,
        }

    return {
        "section_id": best_sec.id,
        "section_name": best_sec.name,
        "confidence": round(confidence, 2),
    }


def map_bale_signal(path: Path, branch_sections: dict[str, list[SectionRecord]]) -> dict[str, Any]:
    data = load_yaml(path)
    branch = normalize_branch_name(data.get("branch", ""))
    date = str(data.get("date", "")).strip()
    sections = branch_sections.get(branch, [])

    mapped_items: list[dict[str, Any]] = []
    section_totals: dict[str, dict[str, Any]] = {}

    for row in data.get("items", []):
        if not isinstance(row, dict):
            continue
        item_name = str(row.get("name", "")).strip()
        qty = int(row.get("qty", 0) or 0)
        value = float(row.get("value", 0) or 0)
        mapping = map_item_to_section(item_name, sections)

        mapped_row = {
            "item_name": item_name,
            "qty": qty,
            "value": value,
            **mapping,
        }
        mapped_items.append(mapped_row)

        sec_key = str(mapping["section_name"])
        if sec_key not in section_totals:
            section_totals[sec_key] = {
                "section_id": mapping["section_id"],
                "section_name": mapping["section_name"],
                "qty": 0,
                "value": 0.0,
                "items": [],
            }

        section_totals[sec_key]["qty"] += qty
        section_totals[sec_key]["value"] += value
        section_totals[sec_key]["items"].append(item_name)

    ranked_sections = sorted(
        section_totals.values(),
        key=lambda x: (x["value"], x["qty"]),
        reverse=True,
    )

    return {
        "source_file": str(path.relative_to(ROOT)),
        "branch": branch,
        "date": date,
        "mapped_items": mapped_items,
        "section_totals": ranked_sections,
    }



def build_section_metrics(signals: list[dict], branch_sections: dict[str, list[SectionRecord]]) -> dict:
    """
    Aggregate ALL colony signals into section-level metrics
    (not just bale files)
    """

    metrics = defaultdict(lambda: defaultdict(lambda: {
        "qty": 0,
        "value": 0.0,
        "count": 0,
    }))

    for s in signals:
        branch = normalize_branch_name(s.get("branch", ""))
        item = str(s.get("item", "")).strip()

        if not branch or not item:
            continue

        sections = branch_sections.get(branch, [])
        mapping = map_item_to_section(item, sections)

        sec_name = mapping["section_name"]

        metrics[branch][sec_name]["qty"] += int(s.get("qty", 1))
        metrics[branch][sec_name]["value"] += float(s.get("value", 0))
        metrics[branch][sec_name]["count"] += 1

    return metrics


def main():
    ...

def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    branch_sections = load_branch_sections()
    outputs: list[dict[str, Any]] = []

    if SIGNALS_BALES_DIR.exists():
        for path in sorted(SIGNALS_BALES_DIR.glob("*.yaml")):
            outputs.append(map_bale_signal(path, branch_sections))

    out_yaml = REPORTS_DIR / "section_mapping_report.yaml"
    with out_yaml.open("w", encoding="utf-8") as f:
        yaml.safe_dump(outputs, f, sort_keys=False, allow_unicode=True)

    out_md = REPORTS_DIR / "section_mapping_report.md"
    with out_md.open("w", encoding="utf-8") as f:
        f.write("# Section Mapping Report\n\n")
        for report in outputs:
            f.write(f"## {report['branch']} — {report['date']}\n\n")
            f.write("| Section | Qty | Value | Items |\n")
            f.write("|---|---:|---:|---|\n")
            for sec in report["section_totals"]:
                items = ", ".join(sec["items"])
                f.write(
                    f"| {sec['section_name']} | {sec['qty']} | {sec['value']:.2f} | {items} |\n"
                )
            f.write("\n")

    print(f"Section mapping complete: {out_yaml}")
    print(f"Markdown report written: {out_md}")


if __name__ == "__main__":
    main()
