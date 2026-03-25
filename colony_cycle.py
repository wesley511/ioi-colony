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
LOGS_DIR = ROOT / "LOGS"

SALES_SCRIPT = ROOT / "scripts" / "ingest_whatsapp_sales_batch.py"
WORKER_SCRIPT = ROOT / "worker_decision_v2.py"
DECAY_SCRIPT = ROOT / "decay_worker.py"

POST_PREFIX = "[IOI Colony Cycle]"


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
    if result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)


def require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def emit_staff_signals() -> int:
    STAFF_MEMORY_DIR.mkdir(parents=True, exist_ok=True)

    copied = 0
    for src in sorted(SIGNALS_DIR.glob("*staff*.md")):
        dst = STAFF_MEMORY_DIR / src.name
        shutil.copy2(src, dst)
        copied += 1

    print(f"{POST_PREFIX} Emitted {copied} staff signal(s) to colony memory")
    return copied


def run_sales_ingestion() -> None:
    require_file(SALES_SCRIPT, "Sales ingestion script")

    print(f"{POST_PREFIX} Running: batch sales ingestion")
    result = run_command([sys.executable, str(SALES_SCRIPT)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Sales ingestion failed with exit code {result.returncode}")


def run_reinforcement_stage() -> None:
    if not WORKER_SCRIPT.exists():
        print(f"{POST_PREFIX} Skipping reinforcement stage: {WORKER_SCRIPT.name} not found")
        return

    print(f"{POST_PREFIX} Running: reinforcement stage")
    result = run_command([sys.executable, str(WORKER_SCRIPT)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Reinforcement stage failed with exit code {result.returncode}")


def run_decay_stage() -> None:
    if not DECAY_SCRIPT.exists():
        print(f"{POST_PREFIX} Skipping decay stage: {DECAY_SCRIPT.name} not found")
        return

    print(f"{POST_PREFIX} Running: decay stage")
    result = run_command([sys.executable, str(DECAY_SCRIPT)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Decay stage failed with exit code {result.returncode}")


def run_staff_analyzer(ts: str) -> Path:
    print(f"{POST_PREFIX} Running: staff colony analyzer")

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
    print(f"{POST_PREFIX} Running: fusion analyzer")

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

def run_decision_signal_generator() -> None:
    generator = ROOT / "scripts" / "generate_decision_signals.py"
    require_file(generator, "Decision signal generator")

    print(f"{POST_PREFIX} Running: decision signal generator")
    result = run_command([sys.executable, str(generator)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Decision signal generator failed with exit code {result.returncode}")

def main() -> int:
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        STAFF_MEMORY_DIR.mkdir(parents=True, exist_ok=True)
        LOGS_DIR.mkdir(parents=True, exist_ok=True)

        ts = now_timestamp()

        run_sales_ingestion()

        print(f"{POST_PREFIX} Running: staff signal emitter")
        emit_staff_signals()

        run_decision_signal_generator()
        run_reinforcement_stage()
        run_decay_stage()

        advisory_path = run_staff_analyzer(ts)
        fusion_path = run_fusion_analyzer(ts)

        print(f"{POST_PREFIX} Cycle complete")
        print(f"{POST_PREFIX} Advisory: {advisory_path}")
        print(f"{POST_PREFIX} Fusion: {fusion_path}")
        print(f"{POST_PREFIX} Timestamp: {datetime.now()}")
        return 0

    except Exception as exc:
        print(f"{POST_PREFIX} ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
