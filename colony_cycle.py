#!/usr/bin/env python3
from __future__ import annotations

import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REPORTS_DIR = ROOT / "REPORTS"
SIGNALS_DIR = ROOT / "SIGNALS" / "normalized"
STAFF_MEMORY_DIR = ROOT / "COLONY_MEMORY" / "staff_signals"
RULES_FILE = ROOT / "COLONY_RULES.md"

SALES_SCRIPT = ROOT / "scripts" / "ingest_whatsapp_sales_batch.py"


def now_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def run_command(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        check=False,
    )


def print_output(result: subprocess.CompletedProcess[str]) -> None:
    if result.stdout:
        print(result.stdout.rstrip())
    if result.returncode != 0 and result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)


def emit_staff_signals() -> int:
    STAFF_MEMORY_DIR.mkdir(parents=True, exist_ok=True)

    copied = 0
    for src in sorted(SIGNALS_DIR.glob("staff-*.md")):
        dst = STAFF_MEMORY_DIR / src.name
        shutil.copy2(src, dst)
        copied += 1

    print(f"[IOI Colony Cycle] Emitted {copied} staff signal(s) to colony memory")
    return copied


def run_sales_ingestion() -> None:
    if not SALES_SCRIPT.exists():
        raise FileNotFoundError(f"Sales ingestion script not found: {SALES_SCRIPT}")

    print("[IOI Colony Cycle] Running: batch sales ingestion")
    result = run_command([sys.executable, str(SALES_SCRIPT)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Sales ingestion failed with exit code {result.returncode}")


def run_staff_analyzer(ts: str) -> Path:
    print("[IOI Colony Cycle] Running: staff colony analyzer")

    advisory_path = REPORTS_DIR / f"advisory_{ts}.md"

    cmd = [
        sys.executable,
        "-m",
        "scripts.colony_analyzer",
        "--memory-dir",
        str(STAFF_MEMORY_DIR),
        "--rules-file",
        str(RULES_FILE),
        "--output",
        str(advisory_path),
    ]

    result = run_command(cmd)
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Staff analyzer failed with exit code {result.returncode}")

    return advisory_path


def run_fusion_analyzer(ts: str) -> Path:
    print("[IOI Colony Cycle] Running: fusion analyzer")

    fusion_path = REPORTS_DIR / f"fusion_{ts}.md"

    cmd = [
        sys.executable,
        "-m",
        "scripts.colony_fusion_analyzer",
        "--output",
        str(fusion_path),
    ]

    result = run_command(cmd)
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Fusion analyzer failed with exit code {result.returncode}")

    return fusion_path


def main() -> int:
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        STAFF_MEMORY_DIR.mkdir(parents=True, exist_ok=True)

        ts = now_timestamp()

        run_sales_ingestion()

        print("[IOI Colony Cycle] Running: staff signal emitter")
        emit_staff_signals()

        advisory_path = run_staff_analyzer(ts)
        fusion_path = run_fusion_analyzer(ts)

        print("[IOI Colony Cycle] Cycle complete")
        print(f"[IOI Colony Cycle] Advisory: {advisory_path}")
        print(f"[IOI Colony Cycle] Fusion: {fusion_path}")
        print(f"[IOI Colony Cycle] Timestamp: {datetime.now()}")
        return 0

    except Exception as exc:
        print(f"[IOI Colony Cycle] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
