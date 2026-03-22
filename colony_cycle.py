#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from datetime import datetime
import subprocess
import sys

POST_PREFIX = "[IOI Colony Cycle]"


def run_step(cmd: list[str], label: str) -> None:
    print(f"{POST_PREFIX} Running: {label}")

    result = subprocess.run(cmd, text=True, capture_output=True)

    if result.stdout:
        print(result.stdout)

    if result.returncode != 0:
        print(f"{POST_PREFIX} ERROR in {label}:\n{result.stderr}", file=sys.stderr)
        raise RuntimeError(f"{label} failed")


def main() -> int:
    root = Path(__file__).resolve().parent
    python = sys.executable

    # Ensure REPORTS dir exists
    reports_dir = root / "REPORTS"
    reports_dir.mkdir(exist_ok=True)

    run_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    advisory_out = reports_dir / f"advisory_{run_stamp}.md"
    fusion_out = reports_dir / f"fusion_{run_stamp}.md"

    # 1. Batch sales ingestion
    run_step(
        [python, str(root / "scripts" / "ingest_whatsapp_sales_batch.py")],
        "batch sales ingestion",
    )

    # 2. Staff analyzer
    run_step(
        [
            python,
            str(root / "scripts" / "colony_analyzer.py"),
            "--min-confidence",
            "0.5",
            "--output",
            str(advisory_out),
        ],
        "staff colony analyzer",
    )

    # 3. Fusion analyzer
    run_step(
        [
            python,
            "-m",
            "scripts.colony_fusion_analyzer",
            "--output",
            str(fusion_out),
        ],
        "fusion analyzer",
    )

    print(f"{POST_PREFIX} Cycle complete")
    print(f"{POST_PREFIX} Advisory: {advisory_out}")
    print(f"{POST_PREFIX} Fusion: {fusion_out}")
    print(f"{POST_PREFIX} Timestamp: {datetime.now()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
