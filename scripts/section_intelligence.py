from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "REPORTS"
INPUT_FILE = REPORTS_DIR / "section_mapping_report.yaml"


def load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def safe_num(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing section mapping report: {INPUT_FILE}")

    reports = load_yaml(INPUT_FILE) or []
    branch_totals: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for report in reports:
        branch = str(report.get("branch", "")).strip().upper()
        for sec in report.get("section_totals", []):
            branch_totals[branch].append(sec)

    out_md = REPORTS_DIR / "section_intelligence.md"
    out_yaml = REPORTS_DIR / "section_intelligence.yaml"

    intelligence: list[dict[str, Any]] = []

    with out_md.open("w", encoding="utf-8") as f:
        f.write("# Section Intelligence\n\n")

        for branch, rows in sorted(branch_totals.items()):
            rows_sorted = sorted(
                rows,
                key=lambda x: (safe_num(x.get("value")), safe_num(x.get("qty"))),
                reverse=True,
            )
            total_value = sum(safe_num(x.get("value")) for x in rows_sorted)
            total_qty = sum(safe_num(x.get("qty")) for x in rows_sorted)

            best = rows_sorted[:3]
            weak = rows_sorted[-3:] if len(rows_sorted) >= 3 else rows_sorted

            branch_block = {
                "branch": branch,
                "total_section_value": round(total_value, 2),
                "total_section_qty": int(total_qty),
                "best_sections": best,
                "weak_sections": weak,
            }
            intelligence.append(branch_block)

            f.write(f"## {branch}\n\n")
            f.write(f"- Total mapped qty: {int(total_qty)}\n")
            f.write(f"- Total mapped value: {total_value:.2f}\n\n")

            f.write("### Best sections\n\n")
            for sec in best:
                f.write(
                    f"- {sec['section_name']}: qty={sec['qty']}, value={safe_num(sec['value']):.2f}\n"
                )
            f.write("\n### Weak sections\n\n")
            for sec in weak:
                f.write(
                    f"- {sec['section_name']}: qty={sec['qty']}, value={safe_num(sec['value']):.2f}\n"
                )
            f.write("\n")

    with out_yaml.open("w", encoding="utf-8") as f:
        yaml.safe_dump(intelligence, f, sort_keys=False, allow_unicode=True)

    print(f"Section intelligence written: {out_md}")
    print(f"Section intelligence data written: {out_yaml}")


if __name__ == "__main__":
    main()
