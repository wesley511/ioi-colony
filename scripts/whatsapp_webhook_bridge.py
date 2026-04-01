#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

app = Flask(__name__)

# -----------------------------------------------------------------------------
# Environment / config
# -----------------------------------------------------------------------------

VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "")
APP_SECRET = os.getenv("WHATSAPP_APP_SECRET", "")
SKIP_SIGNATURE_VERIFY = os.getenv("WHATSAPP_SKIP_SIGNATURE_VERIFY", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

WORKSPACE_ROOT = Path(
    os.getenv("IOI_WORKSPACE_ROOT", str(Path.home() / ".openclaw" / "workspace" / "ioi-colony"))
).resolve()

RAW_ROOT = Path(os.getenv("WHATSAPP_RAW_ROOT", str(WORKSPACE_ROOT / "RAW_INPUT" / "whatsapp"))).resolve()
STATE_DIR = Path(os.getenv("WHATSAPP_STATE_DIR", str(WORKSPACE_ROOT / "DATA"))).resolve()
LOG_DIR = Path(os.getenv("WHATSAPP_LOG_DIR", str(WORKSPACE_ROOT / "LOGS"))).resolve()

DEDUP_STATE_FILE = STATE_DIR / "whatsapp_webhook_seen.json"

ACCEPTED_DIRNAME = "accepted"
QUARANTINE_DIRNAME = "quarantine"

MAX_TEXT_PREVIEW = 160

# -----------------------------------------------------------------------------
# Branch canonicalization
# -----------------------------------------------------------------------------

BRANCH_ALIASES: dict[str, tuple[str, ...]] = {
    "waigani": (
        "waigani",
        "ttc waigani",
        "pom waigani",
        "pom waigani branch",
        "ttc pom waigani branch",
        "ttc waigani branch",
        "waigani branch",
        "port moresby waigani",
        "ncd waigani",
    ),
    "bena_road": (
        "bena road",
        "ttc bena road",
        "ttc bena road goroka",
        "goroka bena road",
        "bena road goroka",
        "goroka",
        "ttc goroka",
        "ttc bena",
    ),
    "lae_5th_street": (
        "lae 5th street",
        "5th street",
        "ttc 5th street lae",
        "ttc lae 5th street",
        "lae fifth street",
        "fifth street lae",
        "5th st lae",
    ),
    "lae_malaita": (
        "lae malaita",
        "malaita",
        "malaita street",
        "lae malaita street",
        "ttc malaita",
        "ttc lae malaita",
        "ttc malaita street",
    ),
}

_ALIAS_TO_BRANCH: dict[str, str] = {}
for branch_slug, aliases in BRANCH_ALIASES.items():
    _ALIAS_TO_BRANCH[branch_slug] = branch_slug
    for alias in aliases:
        _ALIAS_TO_BRANCH[alias] = branch_slug

# -----------------------------------------------------------------------------
# Strict first-line classifier mapping
# -----------------------------------------------------------------------------

CLASSIFIER_MAP: dict[str, str] = {
    "DAY-END SALES REPORT": "sales",
    "DAILY BALE SUMMARY - RELEASED TO RAIL": "bale_summary",
    "DAILY BALE SUMMARY – RELEASED TO RAIL": "bale_summary",
    "DAILY BALE SUMMARY RELEASED TO RAIL": "bale_summary",
    "DAILY BALE SUMMARY": "bale_summary",
    "STAFF PERFORMANCE REPORT": "staff_performance",
    "DAILY STAFF PERFORMANCE REPORT": "staff_performance",
    "DAILY MONITORING REPORT": "monitoring",
    "MONITORING REPORT": "monitoring",
    "STRENGTH REPORT": "strength",
    "GAP REPORT": "gap",
    "PRICING REPORT": "pricing",
    "DAILY PRICING REPORT": "pricing",
    "SUPERVISOR CONTROL REPORT": "supervisor_control",
}

REPORT_KEYWORDS: dict[str, tuple[str, ...]] = {
    "sales": (
        "day-end sales report",
        "z reading",
        "cash sales",
        "eftpos sales",
        "customers served",
        "traffic",
        "cash variance",
    ),
    "bale_summary": (
        "daily bale summary",
        "released to rail",
        "bale #",
        "item name",
        "total qty (pcs)",
        "total amount (k)",
    ),
    "staff_performance": (
        "staff performance report",
        "arrangement",
        "display",
        "performance",
        "section",
        "assisting customers",
    ),
    "monitoring": (
        "monitoring report",
        "rail",
        "tight",
        "slack",
        "loose",
        "packed",
        "few on the rail",
    ),
    "gap": (
        "gap report",
        "missing",
        "stockout",
        "empty",
        "customer request",
        "less than",
    ),
    "strength": (
        "strength report",
        "fast moving",
        "strong movement",
        "popular items",
        "good movement",
    ),
    "pricing": (
        "pricing report",
        "grade",
        "qty",
        "amount",
        "calculator",
        "pricing clerk",
    ),
    "supervisor_control": (
        "supervisor control report",
        "staffing issues",
        "stock issues",
        "pricing/system issues",
        "supervisor confirmed",
        "exceptions escalated",
    ),
}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc_now() -> str:
    return utc_now().replace(microsecond=0).isoformat()


def ensure_dirs() -> None:
    for path in (
        RAW_ROOT / ACCEPTED_DIRNAME,
        RAW_ROOT / QUARANTINE_DIRNAME,
        RAW_ROOT / "unknown",
        STATE_DIR,
        LOG_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def slugify(text: str, default: str = "unknown") -> str:
    value = (text or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = value.strip("_")
    return value or default


def safe_filename_part(text: str, default: str = "unknown") -> str:
    value = slugify(text, default=default)
    return value[:80]


def short_preview(text: str, limit: int = MAX_TEXT_PREVIEW) -> str:
    text = normalize_whitespace(text)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_seen_state() -> dict[str, Any]:
    if not DEDUP_STATE_FILE.exists():
        return {}
    try:
        return json.loads(DEDUP_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_seen_state(data: dict[str, Any]) -> None:
    DEDUP_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    DEDUP_STATE_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def verify_meta_signature(raw_body: bytes, signature_header: str | None) -> bool:
    if SKIP_SIGNATURE_VERIFY:
        return True
    if not APP_SECRET:
        return False
    if not signature_header or not signature_header.startswith("sha256="):
        return False

    supplied = signature_header.split("=", 1)[1].strip()
    digest = hmac.new(APP_SECRET.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(digest, supplied)


def first_nonempty_line(text: str) -> str:
    for line in (text or "").splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def normalize_branch_token(value: str) -> str:
    text = (value or "").strip().lower()
    text = text.replace("&", " and ")
    text = text.replace("/", " ")
    text = re.sub(r"[_\-]+", " ", text)
    text = re.sub(r"\bbranch\b", " ", text)
    text = re.sub(r"\bttc\b", " ", text)
    text = re.sub(r"\bpom\b", " port moresby ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def branch_from_alias(value: str | None) -> str | None:
    if not value:
        return None

    raw = normalize_branch_token(value)

    if raw in _ALIAS_TO_BRANCH:
        return _ALIAS_TO_BRANCH[raw]

    for alias, branch in _ALIAS_TO_BRANCH.items():
        if alias in raw or raw in alias:
            return branch

    return None


def extract_branch_from_text(text: str) -> str | None:
    patterns = [
        r"^\s*branch\s*:\s*(.+)$",
        r"^\s*shop\s*:\s*(.+)$",
    ]
    for line in (text or "").splitlines():
        for pattern in patterns:
            m = re.match(pattern, line.strip(), re.IGNORECASE)
            if m:
                return branch_from_alias(m.group(1).strip())
    return None


def infer_branch_from_text(text: str) -> str | None:
    explicit = extract_branch_from_text(text)
    if explicit:
        return explicit

    lowered = normalize_branch_token(text)
    for alias, branch in _ALIAS_TO_BRANCH.items():
        if alias in lowered:
            return branch
    return None


def parse_date_token(raw: str) -> str | None:
    raw = raw.strip()
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def extract_report_date(text: str) -> str | None:
    for line in (text or "").splitlines():
        stripped = line.strip()
        m = re.search(r"\b(\d{2}/\d{2}/\d{2,4})\b", stripped)
        if m:
            parsed = parse_date_token(m.group(1))
            if parsed:
                return parsed
    return None


def normalize_title_for_classifier(line: str) -> str:
    value = normalize_whitespace(line).upper()
    value = value.replace("—", "-").replace("–", "-")
    value = re.sub(r"\s*-\s*", " - ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def classify_report_type(text: str) -> tuple[str, str, str]:
    title = normalize_title_for_classifier(first_nonempty_line(text))
    if title in CLASSIFIER_MAP:
        return CLASSIFIER_MAP[title], "first_line_exact", title

    lowered = (text or "").lower()
    best_type = "unknown"
    best_hits = 0
    for report_type, keywords in REPORT_KEYWORDS.items():
        hits = sum(1 for keyword in keywords if keyword in lowered)
        if hits > best_hits and hits >= 2:
            best_type = report_type
            best_hits = hits

    if best_type != "unknown":
        return best_type, "keyword_multi_hit", title

    return "unknown", "no_match", title


def has_report_structure(text: str) -> bool:
    lowered = (text or "").lower()
    structure_patterns = [
        r"\bbranch\s*:",
        r"\bdate\s*:",
        r"\bitem\s*:",
        r"\bqty\s*:",
        r"\bamount\s*:",
        r"\bz reading\s*:",
        r"\bcash sales\s*:",
        r"\beftpos sales\s*:",
        r"\barrangement\b",
        r"\bdisplay\b",
        r"\bperformance\b",
        r"\bstaffing issues\s*:",
        r"\bstock issues\s*:",
    ]
    return any(re.search(pattern, lowered) for pattern in structure_patterns)


def is_trivial_message(text: str) -> bool:
    body = normalize_whitespace(text).lower()

    trivial_values = {
        "hi",
        "hello",
        "ok",
        "okay",
        "thanks",
        "thank you",
        "test",
        "checking",
        "ping",
        "done",
        "yes",
        "no",
    }

    if body in trivial_values:
        return True

    if len(body) <= 6 and re.fullmatch(r"[a-z!?., ]+", body):
        return True

    if len(body.split()) <= 2 and not has_report_structure(text):
        return True

    return False


def detect_mixed_report_signals(text: str, primary_report_type: str | None = None) -> bool:
    lowered = (text or "").lower()
    matched_types: list[str] = []

    for report_type, keywords in REPORT_KEYWORDS.items():
        hits = sum(1 for keyword in keywords if keyword in lowered)
        if hits >= 2:
            matched_types.append(report_type)

    matched = set(matched_types)

    if len(matched) < 2:
        return False

    if primary_report_type == "bale_summary":
        sales_markers = [
            "z reading",
            "cash sales",
            "eftpos sales",
            "customers served",
            "total customers",
            "traffic",
            "main door",
            "guest/customer serve",
            "guest customer serve",
        ]
        supervisor_markers = [
            "staffing issues",
            "stock issues",
            "pricing/system issues",
            "supervisor confirmed",
            "exceptions escalated",
            "cash variance",
        ]

        has_sales = sum(1 for k in sales_markers if k in lowered) >= 2
        has_supervisor = sum(1 for k in supervisor_markers if k in lowered) >= 2

        return has_sales or has_supervisor

    return True


def build_quarantine_reason(
    *,
    text: str,
    report_kind: str,
    branch_slug: str | None,
) -> str | None:
    if is_trivial_message(text):
        return "noise_or_unclassified"
    if detect_mixed_report_signals(text, primary_report_type=report_kind) and report_kind != "bale_summary":
        return "mixed_report_signals"
    if report_kind == "unknown":
        return "missing_required_classifier"
    if not has_report_structure(text):
        return "missing_required_structure"
    if not branch_slug:
        return "missing_branch"
    return None


def payload_sha256(raw_body: bytes) -> str:
    return hashlib.sha256(raw_body).hexdigest()


def message_sha256(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)


def extract_text_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for entry in payload.get("entry", []) or []:
        entry_id = entry.get("id")
        for change in entry.get("changes", []) or []:
            value = change.get("value", {}) or {}
            phone_number_id = value.get("metadata", {}).get("phone_number_id")
            display_phone_number = value.get("metadata", {}).get("display_phone_number")
            contacts = value.get("contacts", []) or []
            messages = value.get("messages", []) or []

            sender_name = None
            if contacts:
                sender_name = ((contacts[0] or {}).get("profile") or {}).get("name")

            for message in messages:
                text = None
                payload_kind = "unknown"

                if "text" in message and isinstance(message["text"], dict):
                    text = message["text"].get("body")
                    payload_kind = "text"
                elif message.get("type") == "button":
                    text = ((message.get("button") or {}).get("text") or "").strip()
                    payload_kind = "button"
                elif message.get("type") == "interactive":
                    interactive = message.get("interactive") or {}
                    text = (
                        ((interactive.get("button_reply") or {}).get("title"))
                        or ((interactive.get("list_reply") or {}).get("title"))
                        or ((interactive.get("nfm_reply") or {}).get("body"))
                    )
                    payload_kind = "interactive"

                if not text:
                    continue

                results.append(
                    {
                        "text": str(text).strip(),
                        "message_id": message.get("id"),
                        "sender_phone": message.get("from"),
                        "sender_name": sender_name,
                        "chat_id": phone_number_id,
                        "display_phone_number": display_phone_number,
                        "phone_number_id": phone_number_id,
                        "entry_id": entry_id,
                        "field": change.get("field"),
                        "context": message.get("context") or {},
                        "source": "meta_webhook",
                        "payload_kind": payload_kind,
                    }
                )

    return results


def choose_storage_root(quarantine_reason: str | None, report_kind: str) -> Path:
    if quarantine_reason:
        return RAW_ROOT / QUARANTINE_DIRNAME / utc_now().strftime("%Y-%m-%d")
    if report_kind == "unknown":
        return RAW_ROOT / "unknown"
    return RAW_ROOT / ACCEPTED_DIRNAME / utc_now().strftime("%Y-%m-%d")


def build_storage_stem(
    *,
    ts: int,
    branch_slug: str | None,
    report_kind: str,
    sender_name: str | None,
    message_id: str | None,
) -> str:
    return "__".join(
        [
            str(ts),
            safe_filename_part(branch_slug or "unknown"),
            safe_filename_part(report_kind or "unknown"),
            safe_filename_part(sender_name or "unknown"),
            safe_filename_part(message_id or f"ts{ts}"),
        ]
    )


def is_duplicate_message(seen: dict[str, Any], message_id: str | None, msg_sha: str) -> bool:
    if message_id and message_id in seen.get("message_ids", {}):
        return True
    if msg_sha in seen.get("message_hashes", {}):
        return True
    return False


def mark_seen(seen: dict[str, Any], message_id: str | None, msg_sha: str) -> None:
    seen.setdefault("message_ids", {})
    seen.setdefault("message_hashes", {})
    now = iso_utc_now()

    if message_id:
        seen["message_ids"][message_id] = now
    seen["message_hashes"][msg_sha] = now


def prune_seen(seen: dict[str, Any], max_items: int = 5000) -> dict[str, Any]:
    for key in ("message_ids", "message_hashes"):
        values = seen.get(key, {})
        if len(values) <= max_items:
            continue
        items = list(values.items())[-max_items:]
        seen[key] = dict(items)
    return seen


def store_message(
    *,
    text: str,
    raw_sha256: str,
    meta: dict[str, Any],
) -> dict[str, Any]:
    report_kind, classifier_reason, classifier_title = classify_report_type(text)
    branch_slug = infer_branch_from_text(text)
    quarantine_reason = build_quarantine_reason(
        text=text,
        report_kind=report_kind,
        branch_slug=branch_slug,
    )

    ts = int(time.time())
    storage_root = choose_storage_root(quarantine_reason, report_kind)
    storage_root.mkdir(parents=True, exist_ok=True)

    stem = build_storage_stem(
        ts=ts,
        branch_slug=branch_slug,
        report_kind=report_kind,
        sender_name=meta.get("sender_name"),
        message_id=meta.get("message_id"),
    )

    txt_path = storage_root / f"{stem}.txt"
    meta_path = storage_root / f"{stem}.meta.json"

    txt_path.write_text(text.strip() + "\n", encoding="utf-8")

    metadata = {
        "branch_slug": branch_slug or "unknown",
        "chat_id": meta.get("chat_id"),
        "classifier_reason": classifier_reason,
        "classifier_title": classifier_title,
        "context": meta.get("context") or {},
        "display_phone_number": meta.get("display_phone_number"),
        "entry_id": meta.get("entry_id"),
        "field": meta.get("field"),
        "group_name": None,
        "message_id": meta.get("message_id"),
        "message_sha256": message_sha256(text),
        "payload_kind": meta.get("payload_kind"),
        "phone_number_id": meta.get("phone_number_id"),
        "quarantine_reason": quarantine_reason,
        "raw_sha256": raw_sha256,
        "received_at": iso_utc_now(),
        "report_type": report_kind,
        "sender_name": meta.get("sender_name"),
        "sender_phone": meta.get("sender_phone"),
        "source": meta.get("source", "meta_webhook"),
        "text_preview": short_preview(text),
    }

    meta_path.write_text(safe_json(metadata) + "\n", encoding="utf-8")

    return {
        "ok": True,
        "branch": branch_slug or "unknown",
        "report_type": report_kind,
        "stored_as": "quarantine" if quarantine_reason else ("unknown" if report_kind == "unknown" else "accepted"),
        "quarantine_reason": quarantine_reason,
        "txt_path": str(txt_path.resolve().relative_to(WORKSPACE_ROOT)),
        "meta_path": str(meta_path.resolve().relative_to(WORKSPACE_ROOT)),
    }


def process_payload(payload: dict[str, Any], raw_body: bytes) -> dict[str, Any]:
    ensure_dirs()

    seen = load_seen_state()
    raw_hash = payload_sha256(raw_body)

    stored: list[dict[str, Any]] = []
    duplicate = False
    count = 0

    for candidate in extract_text_candidates(payload):
        count += 1
        msg_id = candidate.get("message_id")
        msg_hash = message_sha256(candidate["text"])

        if is_duplicate_message(seen, msg_id, msg_hash):
            duplicate = True
            continue

        stored.append(
            store_message(
                text=candidate["text"],
                raw_sha256=raw_hash,
                meta=candidate,
            )
        )
        mark_seen(seen, msg_id, msg_hash)

    prune_seen(seen)
    save_seen_state(seen)

    append_jsonl(
        LOG_DIR / "whatsapp_webhook_bridge.log.jsonl",
        {
            "received_at": iso_utc_now(),
            "raw_sha256": raw_hash,
            "count": count,
            "stored_count": len(stored),
            "duplicate": duplicate,
        },
    )

    return {
        "ok": True,
        "count": count,
        "duplicate": duplicate,
        "stored": stored,
    }

# -----------------------------------------------------------------------------
# Flask routes
# -----------------------------------------------------------------------------

@app.get("/health")
def health() -> Any:
    return jsonify(
        {
            "ok": True,
            "service": "whatsapp_webhook_bridge",
            "time": iso_utc_now(),
            "workspace_root": str(WORKSPACE_ROOT),
            "raw_root": str(RAW_ROOT),
        }
    )


@app.get("/webhooks/whatsapp")
def verify_webhook() -> Any:
    mode = request.args.get("hub.mode", "")
    token = request.args.get("hub.verify_token", "")
    challenge = request.args.get("hub.challenge", "")

    if mode == "subscribe" and VERIFY_TOKEN and token == VERIFY_TOKEN:
        return challenge, 200

    return jsonify({"ok": False, "error": "verification failed"}), 403


@app.post("/webhooks/whatsapp")
def ingest_webhook() -> Any:
    raw_body = request.get_data() or b""
    signature = request.headers.get("X-Hub-Signature-256")

    if not verify_meta_signature(raw_body, signature):
        return jsonify({"ok": False, "error": "signature verification failed"}), 403

    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    result = process_payload(payload, raw_body)
    return jsonify(result), 200


@app.get("/")
def root() -> Any:
    return jsonify(
        {
            "ok": True,
            "service": "whatsapp_webhook_bridge",
            "routes": [
                "GET /health",
                "GET /webhooks/whatsapp",
                "POST /webhooks/whatsapp",
            ],
        }
    )


if __name__ == "__main__":
    ensure_dirs()
    host = os.getenv("WHATSAPP_BRIDGE_HOST", "0.0.0.0")
    port = int(os.getenv("WHATSAPP_BRIDGE_PORT", "8090"))
    app.run(host=host, port=port)
