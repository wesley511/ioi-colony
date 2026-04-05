#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.whatsapp_gatekeeper import ValidationResult, reject_payload, validate_message
except ModuleNotFoundError:
    from whatsapp_gatekeeper import ValidationResult, reject_payload, validate_message

try:
    from scripts.utils_normalization import normalize_branch as shared_normalize_branch
except ModuleNotFoundError:
    from utils_normalization import normalize_branch as shared_normalize_branch
try:
    from scripts.whatsapp_intelligence import build_confidence_metadata
except ModuleNotFoundError:
    from whatsapp_intelligence import build_confidence_metadata

# -----------------------------------------------------------------------------
# Paths / config
# -----------------------------------------------------------------------------

WORKSPACE_ROOT = Path(
    os.getenv("IOI_WORKSPACE_ROOT", str(Path.home() / ".openclaw" / "workspace" / "ioi-colony"))
)

RAW_ACCEPTED_ROOT = Path(
    os.getenv(
        "WHATSAPP_ACCEPTED_ROOT",
        str(WORKSPACE_ROOT / "RAW_INPUT" / "whatsapp" / "accepted"),
    )
)

PROCESSED_STATE_FILE = Path(
    os.getenv(
        "WHATSAPP_ACCEPTED_STATE_FILE",
        str(WORKSPACE_ROOT / "DATA" / "processed_accepted_whatsapp.json"),
    )
)

NORMALIZED_ROOT = Path(
    os.getenv(
        "WHATSAPP_NORMALIZED_ROOT",
        str(WORKSPACE_ROOT / "SIGNALS" / "normalized" / "whatsapp_ingress"),
    )
)

STAGING_ROOT = Path(
    os.getenv(
        "WHATSAPP_STAGING_ROOT",
        str(WORKSPACE_ROOT / "RAW_INPUT" / "whatsapp" / "processed"),
    )
)

LOG_DIR = Path(os.getenv("WHATSAPP_PROCESS_LOG_DIR", str(WORKSPACE_ROOT / "LOGS")))
LOG_FILE = LOG_DIR / "process_accepted_whatsapp.log"

POLL_SECONDS = int(os.getenv("WHATSAPP_PROCESS_POLL_SECONDS", "5"))

# Optional downstream parser commands via environment.
# Use "{txt}" and "{meta}" placeholders.
# Example:
# export WHATSAPP_CMD_SALES='python3 -m scripts.parse_whatsapp_sales "{txt}"'
# export WHATSAPP_CMD_STAFF_PERFORMANCE='python3 -m scripts.parse_whatsapp_staff "{txt}"'
# export WHATSAPP_CMD_BALE_SUMMARY='python3 -m scripts.parse_bale_release "{txt}"'
ENV_CMD_BY_REPORT_TYPE = {
    "sales": os.getenv("WHATSAPP_CMD_SALES", "").strip(),
    "staff_performance": os.getenv("WHATSAPP_CMD_STAFF_PERFORMANCE", "").strip(),
    "staff_attendance": os.getenv("WHATSAPP_CMD_STAFF_ATTENDANCE", "").strip(),
    "bale_summary": os.getenv("WHATSAPP_CMD_BALE_SUMMARY", "").strip(),
    "monitoring": os.getenv("WHATSAPP_CMD_MONITORING", "").strip(),
    "gap": os.getenv("WHATSAPP_CMD_GAP", "").strip(),
    "strength": os.getenv("WHATSAPP_CMD_STRENGTH", "").strip(),
    "pricing": os.getenv("WHATSAPP_CMD_PRICING", "").strip(),
}

# Conservative default commands for the parsers we already strongly expect.
# These only run if the module file exists.
DEFAULT_CMD_BY_REPORT_TYPE = {
    "sales": 'python3 -m scripts.parse_whatsapp_sales "{txt}"',
    "staff_performance": 'python3 -m scripts.parse_whatsapp_staff "{txt}"',
    "staff_attendance": 'python3 -m scripts.parse_whatsapp_attendance "{txt}"',
    "bale_summary": 'python3 -m scripts.parse_bale_summary "{txt}"',
}

REPORT_TYPE_ALIASES = {
    "sales_report": "sales",
    "staff_report": "staff_performance",
    "staff_attendance_report": "staff_attendance",
    "supervisor_control": "staff_attendance",
    "supervisor_report": "staff_attendance",
    "bale_report": "bale_summary",
}

# -----------------------------------------------------------------------------
# Data classes
# -----------------------------------------------------------------------------

@dataclass
class AcceptedMessage:
    txt_path: Path
    meta_path: Path
    text: str
    meta: dict[str, Any]
    branch_slug: str
    report_type: str
    received_at: str
    file_id: str
    validation: ValidationResult | None = None


# -----------------------------------------------------------------------------
# Utility
# -----------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc_now() -> str:
    return utc_now().replace(microsecond=0).isoformat()


def ensure_dirs() -> None:
    for path in [
        PROCESSED_STATE_FILE.parent,
        NORMALIZED_ROOT,
        STAGING_ROOT,
        LOG_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def log(message: str) -> None:
    line = f"[process_accepted_whatsapp] {iso_utc_now()} {message}"
    print(line)
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)


def safe_slug(value: str, default: str = "unknown") -> str:
    import re

    text = (value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = text.strip("_")
    return text or default


def canonical_report_type(value: str | None) -> str:
    slug = safe_slug(str(value or "unknown"))
    return REPORT_TYPE_ALIASES.get(slug, slug)


def resolve_branch_slug(meta: dict[str, Any], txt_path: Path) -> str:
    candidates = [
        meta.get("branch_slug"),
        meta.get("branch"),
        meta.get("source_name"),
        meta.get("shop"),
        txt_path.parent.name,
    ]
    for candidate in candidates:
        if not candidate:
            continue
        normalized = shared_normalize_branch(
            str(candidate),
            style="canonical_slug",
            fallback="none",
            match_substring=True,
        )
        if normalized:
            return str(normalized)
    return "unknown"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_received_date(received_at: str) -> str:
    if not received_at:
        return utc_now().strftime("%Y-%m-%d")
    try:
        dt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return utc_now().strftime("%Y-%m-%d")


def parse_received_at(received_at: str) -> datetime:
    if not received_at:
        return utc_now()
    try:
        return datetime.fromisoformat(received_at.replace("Z", "+00:00"))
    except Exception:
        return utc_now()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(stable_json_dumps(obj), encoding="utf-8")


def load_state() -> dict[str, Any]:
    state = load_json(
        PROCESSED_STATE_FILE,
        {"processed": {}, "updated_at": None},
    )
    if not isinstance(state, dict):
        state = {"processed": {}, "updated_at": None}
    if not isinstance(state.get("processed"), dict):
        state["processed"] = {}
    return state


def save_state(state: dict[str, Any]) -> None:
    state["updated_at"] = iso_utc_now()
    save_json(PROCESSED_STATE_FILE, state)


def build_file_id(txt_path: Path, meta_path: Path) -> str:
    raw = f"{txt_path.resolve()}|{meta_path.resolve()}|{sha256_file(txt_path)}|{sha256_file(meta_path)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def read_message(txt_path: Path) -> str:
    return txt_path.read_text(encoding="utf-8").strip() + "\n"


def matching_meta_path(txt_path: Path) -> Path:
    return txt_path.with_suffix(".meta.json")


def discover_txt_files() -> list[Path]:
    if not RAW_ACCEPTED_ROOT.exists():
        return []
    return sorted(RAW_ACCEPTED_ROOT.glob("*/*.txt"))


def validation_report_type(validation: ValidationResult | None, msg: AcceptedMessage) -> str:
    explicit_type = canonical_report_type(msg.report_type)
    if explicit_type and explicit_type != "unknown":
        return explicit_type
    if validation and validation.report_type:
        return canonical_report_type(validation.report_type)
    return explicit_type


def validation_branch(validation: ValidationResult | None, msg: AcceptedMessage) -> str:
    normalized = ((validation.normalized or {}).get("branch") if validation else None) or ""
    return str(normalized or msg.branch_slug)


def validation_date(validation: ValidationResult | None, msg: AcceptedMessage) -> str:
    normalized = ((validation.normalized or {}).get("date") if validation else None) or ""
    if not normalized:
        normalized = parse_received_date(msg.received_at)
    if len(normalized) == 8 and normalized.count("/") == 2:
        try:
            return datetime.strptime(normalized, "%d/%m/%y").date().isoformat()
        except ValueError:
            return normalized
    return normalized


def business_key(msg: AcceptedMessage) -> str:
    validation = msg.validation
    report_type = validation_report_type(validation, msg)
    branch = validation_branch(validation, msg)
    report_date = validation_date(validation, msg)

    if report_type == "sales":
        return f"sales:{branch}:{report_date}"
    if report_type == "staff_performance":
        return f"staff:{branch}:{report_date}"
    if report_type == "staff_attendance":
        return f"staff_attendance:{branch}:{report_date}"
    if report_type == "bale_summary":
        payload_hash = msg.meta.get("message_sha256") or msg.meta.get("raw_sha256") or msg.file_id
        return f"bale:{branch}:{report_date}:{payload_hash}"
    return f"{report_type}:{branch}:{report_date}:{msg.file_id}"


def candidate_rank(msg: AcceptedMessage) -> tuple[datetime, str, str]:
    return (
        parse_received_at(msg.received_at),
        str(msg.meta.get("message_sha256") or msg.meta.get("raw_sha256") or ""),
        msg.txt_path.name,
    )


def parse_message(txt_path: Path) -> AcceptedMessage | None:
    meta_path = matching_meta_path(txt_path)
    if not meta_path.exists():
        log(f"skip missing-meta txt={txt_path}")
        return None

    try:
        text = read_message(txt_path)
        meta = load_json(meta_path, {})
        if not isinstance(meta, dict):
            log(f"skip invalid-meta txt={txt_path}")
            return None

        branch_slug = resolve_branch_slug(meta, txt_path)
        report_type = canonical_report_type(meta.get("report_type"))
        received_at = str(meta.get("received_at") or "")
        file_id = build_file_id(txt_path, meta_path)
        validation = validate_message(text)

        return AcceptedMessage(
            txt_path=txt_path,
            meta_path=meta_path,
            text=text,
            meta=meta,
            branch_slug=branch_slug,
            report_type=report_type,
            received_at=received_at,
            file_id=file_id,
            validation=validation,
        )
    except Exception as exc:
        log(f"skip parse-error txt={txt_path} error={exc}")
        return None


def build_normalized_envelope(msg: AcceptedMessage) -> dict[str, Any]:
    received_date = parse_received_date(msg.received_at)
    resolved_branch = validation_branch(msg.validation, msg)
    resolved_type = validation_report_type(msg.validation, msg)
    resolved_date = validation_date(msg.validation, msg)
    validation_lane = str(msg.validation.lane if msg.validation else "quarantine")
    validation_warnings = list(msg.validation.warnings if msg.validation else [])
    intelligence = build_confidence_metadata(
        validation_lane=validation_lane,
        warnings=validation_warnings,
        flags=((msg.validation.normalized or {}).get("flags") if msg.validation and msg.validation.normalized else None),
        confidence_score=None,
    )
    return {
        "kind": "whatsapp_accepted_dispatch",
        "schema_version": "1.0",
        "created_at": iso_utc_now(),
        "source": {
            "channel": "whatsapp",
            "accepted_txt_path": str(msg.txt_path),
            "accepted_meta_path": str(msg.meta_path),
            "file_id": msg.file_id,
            "raw_sha256": msg.meta.get("raw_sha256"),
            "message_sha256": msg.meta.get("message_sha256") or sha256_text(msg.text),
            "received_at": msg.received_at,
            "received_date": received_date,
            "payload_kind": msg.meta.get("payload_kind"),
            "bridge_source": msg.meta.get("source"),
        },
        "routing": {
            "branch_slug": resolved_branch,
            "report_type": resolved_type,
            "report_date": resolved_date,
            "group_name": msg.meta.get("group_name"),
            "sender_name": msg.meta.get("sender_name"),
            "sender_phone": msg.meta.get("sender_phone"),
            "classifier_title": msg.meta.get("classifier_title"),
            "classifier_reason": msg.meta.get("classifier_reason"),
        },
        "content": {
            "text": msg.text.rstrip(),
            "text_preview": msg.meta.get("text_preview"),
        },
        "validation": {
            "ok": bool(msg.validation and msg.validation.ok),
            "errors": list(msg.validation.errors if msg.validation else []),
            "warnings": validation_warnings,
            "lane": validation_lane,
            "accepted_with_warnings": validation_lane == "accepted_with_warnings",
            **intelligence,
        },
        "intelligence": intelligence,
        "meta": msg.meta,
    }


def normalized_output_path(msg: AcceptedMessage) -> Path:
    received_date = validation_date(msg.validation, msg)
    base = NORMALIZED_ROOT / validation_branch(msg.validation, msg) / received_date
    stem = msg.txt_path.stem
    return base / f"{stem}.dispatch.json"


def write_normalized_envelope(msg: AcceptedMessage) -> Path:
    out_path = normalized_output_path(msg)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    save_json(out_path, build_normalized_envelope(msg))
    return out_path


def file_exists_under_workspace(rel_path: str) -> bool:
    return (WORKSPACE_ROOT / rel_path).exists()


def resolve_command_template(report_type: str) -> str | None:
    report_type = canonical_report_type(report_type)
    env_cmd = ENV_CMD_BY_REPORT_TYPE.get(report_type, "")
    if env_cmd:
        return env_cmd

    default_cmd = DEFAULT_CMD_BY_REPORT_TYPE.get(report_type)
    if not default_cmd:
        return None

    module_to_file = {
        "sales": "scripts/parse_whatsapp_sales.py",
        "staff_performance": "scripts/parse_whatsapp_staff.py",
        "staff_attendance": "scripts/parse_whatsapp_attendance.py",
        "bale_summary": "scripts/parse_bale_summary.py",
    }
    needed_file = module_to_file.get(report_type)
    if needed_file and file_exists_under_workspace(needed_file):
        return default_cmd

    return None


def run_downstream_parser(msg: AcceptedMessage) -> dict[str, Any]:
    template = resolve_command_template(msg.report_type)
    if not template:
        return {
            "executed": False,
            "status": "no_parser_configured",
            "command": None,
            "returncode": None,
            "stdout": "",
            "stderr": "",
        }

    cmd = template.format(txt=str(msg.txt_path), meta=str(msg.meta_path))
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=str(WORKSPACE_ROOT),
            capture_output=True,
            text=True,
            timeout=180,
        )
        status = "ok" if result.returncode == 0 else "error"
        return {
            "executed": True,
            "status": status,
            "command": cmd,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "executed": True,
            "status": "timeout",
            "command": cmd,
            "returncode": None,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "",
        }
    except Exception as exc:
        return {
            "executed": True,
            "status": "exception",
            "command": cmd,
            "returncode": None,
            "stdout": "",
            "stderr": str(exc),
        }


def copy_to_processed_staging(msg: AcceptedMessage) -> dict[str, str]:
    received_date = validation_date(msg.validation, msg)
    stage_dir = STAGING_ROOT / validation_branch(msg.validation, msg) / received_date / validation_report_type(msg.validation, msg)
    stage_dir.mkdir(parents=True, exist_ok=True)

    staged_txt = stage_dir / msg.txt_path.name
    staged_meta = stage_dir / msg.meta_path.name

    shutil.copy2(msg.txt_path, staged_txt)
    shutil.copy2(msg.meta_path, staged_meta)

    return {
        "staged_txt_path": str(staged_txt),
        "staged_meta_path": str(staged_meta),
    }


def reject_invalid_message(msg: AcceptedMessage, state: dict[str, Any]) -> bool:
    processed = state["processed"]
    existing = processed.get(msg.file_id)
    if existing and not record_needs_replay(existing):
        return False

    validation = msg.validation
    result = reject_payload(
        raw_text=msg.text,
        classifier_line=validation.classifier if validation else None,
        inferred_type=validation.report_type if validation else msg.report_type,
        reasons=list(validation.errors if validation else ["Validation failed"]),
        raw_hash=msg.meta.get("raw_sha256") or sha256_text(msg.text),
    )
    processed[msg.file_id] = {
        "file_id": msg.file_id,
        "processed_at": iso_utc_now(),
        "txt_path": str(msg.txt_path),
        "meta_path": str(msg.meta_path),
        "status": "rejected",
        "branch_slug": msg.branch_slug,
        "report_type": msg.report_type,
        "received_at": msg.received_at,
        "reasons": result["reasons"],
    }
    save_state(state)
    log(f"rejected file={msg.txt_path.name} reasons={'; '.join(result['reasons'])}")
    return True


def parser_result_indicates_empty_output(record: dict[str, Any]) -> bool:
    parser = record.get("parser") or {}
    stdout = str(parser.get("stdout") or "")
    report_type = canonical_report_type(record.get("report_type"))
    if report_type == "bale_summary":
        return "Parsed bale count: 0" in stdout or "Total bales: 0" in stdout
    if report_type == "staff_performance":
        return "Parsed 0 staff records" in stdout or "Wrote 0 files" in stdout
    return False


def record_needs_replay(record: dict[str, Any]) -> bool:
    if record.get("status") == "rejected":
        return True
    parser = record.get("parser") or {}
    parser_status = parser.get("status")
    if parser_status and parser_status not in {"ok", "no_parser_configured"}:
        return True
    if parser_status == "ok" and parser_result_indicates_empty_output(record):
        return True
    return False


def process_one(msg: AcceptedMessage, state: dict[str, Any], mark_processed_even_on_error: bool) -> bool:
    processed = state["processed"]
    existing = processed.get(msg.file_id)
    if existing and not record_needs_replay(existing):
        return False

    if msg.validation and not msg.validation.ok:
        return reject_invalid_message(msg, state)

    dispatch_path = write_normalized_envelope(msg)
    parser_result = run_downstream_parser(msg)
    staged = copy_to_processed_staging(msg)

    record = {
        "file_id": msg.file_id,
        "processed_at": iso_utc_now(),
        "txt_path": str(msg.txt_path),
        "meta_path": str(msg.meta_path),
        "dispatch_path": str(dispatch_path),
        "branch_slug": validation_branch(msg.validation, msg),
        "report_type": validation_report_type(msg.validation, msg),
        "report_date": validation_date(msg.validation, msg),
        "validation_lane": str(msg.validation.lane if msg.validation else "quarantine"),
        "validation_warnings": list(msg.validation.warnings if msg.validation else []),
        "business_key": business_key(msg),
        "received_at": msg.received_at,
        "parser": parser_result,
        "staging": staged,
    }

    parser_failed = parser_result["executed"] and parser_result["status"] not in {"ok", "no_parser_configured"}
    if parser_failed and not mark_processed_even_on_error:
        log(
            f"parser-failed not-marked file={msg.txt_path.name} "
            f"type={msg.report_type} status={parser_result['status']}"
        )
        return False

    processed[msg.file_id] = record
    save_state(state)

    log(
        f"processed file={msg.txt_path.name} "
        f"branch={record['branch_slug']} type={record['report_type']} lane={record['validation_lane']} "
        f"dispatch={dispatch_path} parser_status={parser_result['status']}"
    )
    return True


def choose_messages(messages: list[AcceptedMessage]) -> tuple[list[AcceptedMessage], list[dict[str, Any]]]:
    chosen_by_key: dict[str, AcceptedMessage] = {}
    audit_rows: list[dict[str, Any]] = []

    grouped: dict[str, list[AcceptedMessage]] = {}
    for msg in messages:
        key = business_key(msg)
        grouped.setdefault(key, []).append(msg)

    for key, group in sorted(grouped.items()):
        selected = max(group, key=candidate_rank)
        chosen_by_key[key] = selected
        audit_rows.append(
            {
                "business_key": key,
                "count": len(group),
                "selected_file": selected.txt_path.name,
                "selected_received_at": selected.received_at,
                "selected_validation_lane": str(selected.validation.lane if selected.validation else "quarantine"),
                "files": [item.txt_path.name for item in sorted(group, key=lambda item: item.txt_path.name)],
            }
        )

    return list(chosen_by_key.values()), audit_rows


def write_duplicate_audit(audit_rows: list[dict[str, Any]]) -> None:
    report = {
        "generated_at": iso_utc_now(),
        "accepted_root": str(RAW_ACCEPTED_ROOT),
        "groups": audit_rows,
        "duplicate_group_count": sum(1 for row in audit_rows if row["count"] > 1),
        "accepted_with_warnings_group_count": sum(
            1 for row in audit_rows if row.get("selected_validation_lane") == "accepted_with_warnings"
        ),
    }
    save_json(WORKSPACE_ROOT / "DATA" / "whatsapp_duplicate_audit.json", report)


def process_batch(mark_processed_even_on_error: bool) -> int:
    ensure_dirs()
    state = load_state()
    count = 0

    parsed_messages: list[AcceptedMessage] = []
    for txt_path in discover_txt_files():
        msg = parse_message(txt_path)
        if msg:
            parsed_messages.append(msg)

    valid_messages = [msg for msg in parsed_messages if not msg.validation or msg.validation.ok]
    invalid_messages = [msg for msg in parsed_messages if msg.validation and not msg.validation.ok]
    selected_messages, audit_rows = choose_messages(valid_messages)
    write_duplicate_audit(audit_rows)

    for msg in sorted(invalid_messages, key=lambda item: item.txt_path.name):
        if process_one(msg, state, mark_processed_even_on_error=mark_processed_even_on_error):
            count += 1

    for msg in sorted(selected_messages, key=candidate_rank):
        if process_one(msg, state, mark_processed_even_on_error=mark_processed_even_on_error):
            count += 1

    return count


def show_status() -> None:
    state = load_state()
    processed = state.get("processed", {})
    print(stable_json_dumps({
        "workspace_root": str(WORKSPACE_ROOT),
        "accepted_root": str(RAW_ACCEPTED_ROOT),
        "normalized_root": str(NORMALIZED_ROOT),
        "staging_root": str(STAGING_ROOT),
        "state_file": str(PROCESSED_STATE_FILE),
        "processed_count": len(processed),
        "updated_at": state.get("updated_at"),
    }))


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Process accepted WhatsApp bridge files and dispatch them downstream."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one scan and exit.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Continuously poll for new accepted files.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=POLL_SECONDS,
        help=f"Polling interval for --watch mode (default: {POLL_SECONDS}).",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show processor status and exit.",
    )
    parser.add_argument(
        "--mark-processed-even-on-error",
        action="store_true",
        help="Mark files processed even if a downstream parser command fails.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.status:
        show_status()
        return 0

    if not args.once and not args.watch:
        args.once = True

    ensure_dirs()

    if args.once:
        count = process_batch(mark_processed_even_on_error=args.mark_processed_even_on_error)
        log(f"batch-complete processed={count}")
        return 0

    log(f"watch-start poll_seconds={args.poll_seconds}")
    try:
        while True:
            count = process_batch(mark_processed_even_on_error=args.mark_processed_even_on_error)
            if count:
                log(f"watch-cycle processed={count}")
            time.sleep(max(1, args.poll_seconds))
    except KeyboardInterrupt:
        log("watch-stopped keyboard-interrupt")
        return 0


if __name__ == "__main__":
    sys.exit(main())
