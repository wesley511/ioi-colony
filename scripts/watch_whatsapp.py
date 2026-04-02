from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable


DEFAULT_WATCH_ROOT = Path("RAW_INPUT/whatsapp")
DEFAULT_POLL_SECONDS = 5.0
DEFAULT_ARCHIVE_ROOT = Path("RAW_INPUT/whatsapp_processed")
DEFAULT_FAILED_ROOT = Path("RAW_INPUT/whatsapp_failed")


def iter_txt_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    return sorted(
        p for p in root.rglob("*.txt")
        if p.is_file() and not p.name.startswith(".")
    )


def run_gatekeeper(file_path: Path, strict: bool = True, print_json: bool = True) -> tuple[int, dict | None, str]:
    cmd = ["python3", "scripts/whatsapp_gatekeeper.py", str(file_path)]
    if strict:
        cmd.append("--strict")
    if print_json:
        cmd.append("--print-json")

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )

    stdout = proc.stdout.strip()
    parsed = None

    if stdout:
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            parsed = None

    return proc.returncode, parsed, stdout


def move_preserving_structure(src: Path, from_root: Path, to_root: Path) -> Path:
    rel = src.relative_to(from_root)
    dst = to_root / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    src.rename(dst)
    return dst


def main() -> int:
    parser = argparse.ArgumentParser(description="Watch WhatsApp drop folder and ingest new reports.")
    parser.add_argument(
        "--watch-root",
        default=str(DEFAULT_WATCH_ROOT),
        help="Root directory to watch for incoming .txt files.",
    )
    parser.add_argument(
        "--archive-root",
        default=str(DEFAULT_ARCHIVE_ROOT),
        help="Directory to move successfully processed files into.",
    )
    parser.add_argument(
        "--failed-root",
        default=str(DEFAULT_FAILED_ROOT),
        help="Directory to move failed files into.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help="Polling interval in seconds.",
    )
    parser.add_argument(
        "--no-archive",
        action="store_true",
        help="Leave files in place after processing.",
    )
    args = parser.parse_args()

    watch_root = Path(args.watch_root)
    archive_root = Path(args.archive_root)
    failed_root = Path(args.failed_root)

    watch_root.mkdir(parents=True, exist_ok=True)
    if not args.no_archive:
        archive_root.mkdir(parents=True, exist_ok=True)
        failed_root.mkdir(parents=True, exist_ok=True)

    seen_paths: set[str] = set()

    print(f"[watch_whatsapp] watching: {watch_root}")
    print(f"[watch_whatsapp] poll_seconds={args.poll_seconds}")
    if args.no_archive:
        print("[watch_whatsapp] archive mode: disabled")
    else:
        print(f"[watch_whatsapp] archive_root={archive_root}")
        print(f"[watch_whatsapp] failed_root={failed_root}")

    try:
        while True:
            files = iter_txt_files(watch_root)

            for file_path in files:
                key = str(file_path.resolve())
                if key in seen_paths:
                    continue

                print(f"[watch_whatsapp] processing: {file_path}")
                returncode, parsed, stdout = run_gatekeeper(file_path)

                if stdout:
                    print(stdout)

                if returncode == 0:
                    status = parsed.get("status") if isinstance(parsed, dict) else None

                    if status in {"accepted", "duplicate", "rejected"}:
                        seen_paths.add(key)

                        if not args.no_archive:
                            archived = move_preserving_structure(file_path, watch_root, archive_root)
                            print(f"[watch_whatsapp] moved to archive: {archived}")
                    else:
                        if not args.no_archive:
                            failed = move_preserving_structure(file_path, watch_root, failed_root)
                            print(f"[watch_whatsapp] unknown status; moved to failed: {failed}")
                        seen_paths.add(key)
                else:
                    if not args.no_archive:
                        failed = move_preserving_structure(file_path, watch_root, failed_root)
                        print(f"[watch_whatsapp] gatekeeper error; moved to failed: {failed}")
                    seen_paths.add(key)

            time.sleep(args.poll_seconds)

    except KeyboardInterrupt:
        print("\n[watch_whatsapp] stopped")
        return 0


if __name__ == "__main__":
    sys.exit(main())
