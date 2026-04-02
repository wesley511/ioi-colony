#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:
    raise SystemExit("PyYAML is required: pip install pyyaml") from exc

try:
    from scripts.branch_resolution import canonical_branch_slug, resolve_branch_slug
except ModuleNotFoundError:
    from branch_resolution import canonical_branch_slug, resolve_branch_slug


BASE_DIR = Path(__file__).resolve().parent.parent
NORMALIZED_DIR = BASE_DIR / "SIGNALS" / "normalized"
SALES_SIGNAL_DIR = BASE_DIR / "COLONY_MEMORY" / "sales_signals"


@dataclass
class SalesRecord:
    branch: str
    branch_slug: str
    signal_date: str
    sales_total: float
    cash_sales: float
    eftpos_sales: float
    transaction_total: int
    avg_basket_value: float
    eftpos_ratio: float
    source_file: str


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = value.replace("&", "and")
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def branch_display_name_from_stem(stem: str) -> str:
    # waigani_sales_2026-03-24 -> WAIGANI
    m = re.match(r"(.+?)_sales_\d{4}-\d{2}-\d{2}$", stem)
    if m:
        return m.group(1).upper()
    return stem.upper()


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    text = text.replace("K", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return default


def to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))
    text = str(value).strip().replace(",", "")
    try:
        return int(float(text))
    except ValueError:
        return default


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def extract_date_from_name(path: Path) -> str | None:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", path.stem)
    return m.group(1) if m else None


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}")
    return data


def parse_sales_yaml(path: Path) -> SalesRecord:
    data = load_yaml(path)

    branch = (
        data.get("branch")
        or data.get("source_name")
        or data.get("shop")
        or branch_display_name_from_stem(path.stem)
    )
    branch_slug = resolve_branch_slug(
        data,
        path=path,
        candidates=[
            data.get("branch"),
            data.get("source_name"),
            data.get("shop"),
            branch_display_name_from_stem(path.stem),
        ],
    )
    signal_date = (
        data.get("signal_date")
        or data.get("date")
        or data.get("report_date")
        or extract_date_from_name(path)
    )
    if not signal_date:
        raise ValueError(f"Could not determine signal date from {path.name}")

    sales_total = to_float(
        data.get("sales_total", data.get("total_sales", data.get("sales")))
    )
    cash_sales = to_float(
        data.get("cash_sales", data.get("cash"))
    )
    eftpos_sales = to_float(
        data.get("eftpos_sales", data.get("eftpos"))
    )
    transaction_total = to_int(
        data.get("transaction_total", data.get("transactions"))
    )

    # Recover missing values where possible
    if sales_total <= 0 and (cash_sales > 0 or eftpos_sales > 0):
        sales_total = cash_sales + eftpos_sales

    avg_basket_value = safe_div(sales_total, transaction_total)
    eftpos_ratio = safe_div(eftpos_sales, sales_total)

    return SalesRecord(
        branch=str(branch).upper(),
        branch_slug=branch_slug,
        signal_date=str(signal_date),
        sales_total=sales_total,
        cash_sales=cash_sales,
        eftpos_sales=eftpos_sales,
        transaction_total=transaction_total,
        avg_basket_value=avg_basket_value,
        eftpos_ratio=eftpos_ratio,
        source_file=path.name,
    )


def classify_strength(record: SalesRecord) -> tuple[str, float]:
    """
    Simple first-pass scoring.
    You can refine later.
    """
    sales_score = min(record.sales_total / 20000.0, 1.0)
    traffic_score = min(record.transaction_total / 600.0, 1.0)
    basket_score = min(record.avg_basket_value / 40.0, 1.0)

    confidence = round((sales_score * 0.5) + (traffic_score * 0.3) + (basket_score * 0.2), 4)

    if confidence >= 0.75:
        label = "Strong Sales Performance"
    elif confidence >= 0.45:
        label = "Moderate Sales Performance"
    else:
        label = "Weak Sales Performance"

    return label, confidence


def detect_anomalies(record: SalesRecord) -> list[tuple[str, str, float]]:
    anomalies: list[tuple[str, str, float]] = []

    if record.transaction_total > 0 and record.avg_basket_value < 12:
        anomalies.append(
            (
                "Low Basket Value",
                "High traffic but weak basket value suggests conversion or pricing weakness.",
                0.78,
            )
        )

    if record.sales_total > 0 and record.eftpos_ratio > 0.60:
        anomalies.append(
            (
                "High EFTPOS Share",
                "EFTPOS share is unusually high relative to total sales.",
                0.62,
            )
        )

    if record.transaction_total == 0 and record.sales_total > 0:
        anomalies.append(
            (
                "Transaction Count Mismatch",
                "Sales total exists while transaction count is zero.",
                0.90,
            )
        )

    if record.sales_total == 0 and record.transaction_total > 0:
        anomalies.append(
            (
                "Sales Missing With Traffic",
                "Traffic exists but sales total is zero.",
                0.95,
            )
        )

    return anomalies


def write_signal_file(
    output_dir: Path,
    branch_stem: str,
    branch_value: str,
    branch_slug: str,
    date_str: str,
    signal_type: str,
    title: str,
    summary: str,
    confidence: float,
    metrics: dict[str, Any],
    source_file: str,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{branch_stem}_{signal_type}_{date_str}.md"
    path = output_dir / filename

    lines = [
        f"# {title}",
        "",
        f"- branch: {branch_value}",
        f"- branch_slug: {branch_slug}",
        f"- signal_date: {date_str}",
        f"- signal_type: {signal_type}",
        f"- confidence: {confidence:.4f}",
        f"- source_file: {source_file}",
        "",
        "## Summary",
        "",
        summary,
        "",
        "## Metrics",
        "",
    ]
    for key, value in metrics.items():
        lines.append(f"- {key}: {value}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def process_sales_file(path: Path) -> list[Path]:
    record = parse_sales_yaml(path)
    branch_stem = slugify(record.branch)
    written: list[Path] = []

    title, confidence = classify_strength(record)
    summary = (
        f"{record.branch} recorded sales of K{record.sales_total:,.2f} across "
        f"{record.transaction_total} transactions. Average basket value was "
        f"K{record.avg_basket_value:,.2f}."
    )
    metrics = {
        "sales_total": f"K{record.sales_total:,.2f}",
        "cash_sales": f"K{record.cash_sales:,.2f}",
        "eftpos_sales": f"K{record.eftpos_sales:,.2f}",
        "transaction_total": record.transaction_total,
        "avg_basket_value": f"K{record.avg_basket_value:,.2f}",
        "eftpos_ratio": round(record.eftpos_ratio, 4),
    }

    written.append(
        write_signal_file(
            SALES_SIGNAL_DIR,
            branch_stem,
            record.branch,
            record.branch_slug,
            record.signal_date,
            "sales_performance",
            title,
            summary,
            confidence,
            metrics,
            record.source_file,
        )
    )

    for anomaly_title, anomaly_summary, anomaly_conf in detect_anomalies(record):
        anomaly_slug = slugify(anomaly_title)
        written.append(
            write_signal_file(
                SALES_SIGNAL_DIR,
                branch_stem,
                record.branch,
                record.branch_slug,
                record.signal_date,
                anomaly_slug,
                anomaly_title,
                anomaly_summary,
                anomaly_conf,
                metrics,
                record.source_file,
            )
        )

    return written


def discover_sales_yaml_files(normalized_dir: Path) -> list[Path]:
    return sorted(normalized_dir.glob("*_sales_*.yaml"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate persistent sales signals from normalized sales YAML.")
    parser.add_argument("--date", help="Only process one date, e.g. 2026-03-24")
    parser.add_argument("--branch", help="Only process one branch slug/name, e.g. waigani")
    parser.add_argument("--file", help="Process one specific YAML file")
    parser.add_argument("--json", action="store_true", help="Print JSON summary")
    args = parser.parse_args()

    files: list[Path]
    if args.file:
        files = [Path(args.file)]
    else:
        files = discover_sales_yaml_files(NORMALIZED_DIR)

    requested_branch_slug = canonical_branch_slug(args.branch, fallback="") if args.branch else ""

    filtered: list[Path] = []
    for path in files:
        stem = path.stem.lower()
        if args.date and args.date not in stem:
            continue
        if requested_branch_slug and resolve_branch_slug(path=path, candidates=[path.stem]) != requested_branch_slug:
            continue
        filtered.append(path)

    written_paths: list[Path] = []
    errors: list[dict[str, str]] = []

    for path in filtered:
        try:
            written_paths.extend(process_sales_file(path))
        except Exception as exc:
            errors.append({"file": str(path), "error": str(exc)})

    if args.json:
        print(
            json.dumps(
                {
                    "processed_files": len(filtered),
                    "written_files": len(written_paths),
                    "errors": errors,
                    "written": [str(p) for p in written_paths],
                },
                indent=2,
            )
        )
    else:
        print(f"Processed sales YAML files: {len(filtered)}")
        print(f"Sales signals written: {len(written_paths)}")
        if errors:
            print("Errors:")
            for err in errors:
                print(f"- {err['file']}: {err['error']}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
