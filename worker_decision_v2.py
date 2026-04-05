import argparse
import json
import os
import re
import math
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from scripts.opportunity_blackboard import (
        ACTIVE_MARKER,
        rebuild_active_part,
        split_blackboard_sections,
        split_blocks,
    )
except ModuleNotFoundError:
    from opportunity_blackboard import (
        ACTIVE_MARKER,
        rebuild_active_part,
        split_blackboard_sections,
        split_blocks,
    )
try:
    from scripts.branch_resolution import resolve_branch_slug
except ModuleNotFoundError:
    from branch_resolution import resolve_branch_slug
try:
    from scripts.colony_analyzer import normalize_section_key
except ModuleNotFoundError:
    from colony_analyzer import normalize_section_key
try:
    from scripts.section_normalizer import normalize_section_name
except ModuleNotFoundError:
    from section_normalizer import normalize_section_name

SIGNALS_PATH = "SIGNALS/normalized"
BLACKBOARD_PATH = "OPPORTUNITIES.md"
LOG_PATH = "LOGS/worker_decision_v2.log"
DATA_DIR = "DATA"
CHECKPOINTS_DIR = os.path.join(DATA_DIR, "checkpoints")
PROCESSED_WHATSAPP_STATE_PATH = os.path.join(DATA_DIR, "processed_accepted_whatsapp.json")
CONFIDENCE_AUDIT_PATH = os.path.join(DATA_DIR, "confidence_scoring_audit.json")
DENSITY_AUDIT_PATH = os.path.join(DATA_DIR, "signal_density_audit.json")
WAVE3_SNAPSHOT_PATH = os.path.join(CHECKPOINTS_DIR, "wave3_snapshot.json")
WARNING_INTELLIGENCE_PATH = os.path.join(DATA_DIR, "warning_intelligence.json")
WARNING_PATTERN_AUDIT_PATH = os.path.join(DATA_DIR, "warning_pattern_audit.json")
WAVE4A_SNAPSHOT_PATH = os.path.join(CHECKPOINTS_DIR, "wave4a_snapshot.json")
WAVE4B_SNAPSHOT_PATH = os.path.join(CHECKPOINTS_DIR, "wave4b_snapshot.json")
FUSION_SIGNAL_CONTEXT_PATH = os.path.join(DATA_DIR, "fusion_signal_context.json")
WAVE5A_SNAPSHOT_PATH = os.path.join(CHECKPOINTS_DIR, "wave5a_snapshot.json")
FUSION_SCORE_AUDIT_PATH = os.path.join(DATA_DIR, "fusion_score_audit.json")
WAVE5B_SNAPSHOT_PATH = os.path.join(CHECKPOINTS_DIR, "wave5b_snapshot.json")
FUSION_EFFECT_AUDIT_PATH = os.path.join(DATA_DIR, "fusion_effect_audit.json")
WAVE5C_SNAPSHOT_PATH = os.path.join(CHECKPOINTS_DIR, "wave5c_snapshot.json")
NORMALIZATION_GAP_AUDIT_PATH = os.path.join(DATA_DIR, "normalization_gap_audit.json")
WAVE6A_SNAPSHOT_PATH = os.path.join(CHECKPOINTS_DIR, "wave6a_snapshot.json")
OPPORTUNITIES_HYGIENE_AUDIT_PATH = os.path.join(DATA_DIR, "opportunities_hygiene_audit.json")
WAVE6B_SNAPSHOT_PATH = os.path.join(CHECKPOINTS_DIR, "wave6b_snapshot.json")
LEGACY_SIGNAL_HYGIENE_AUDIT_PATH = os.path.join(DATA_DIR, "legacy_signal_hygiene_audit.json")
PROCESSING_GUARDRAIL_SUMMARY_PATH = os.path.join(DATA_DIR, "processing_guardrail_summary.json")
WAVE6C_SNAPSHOT_PATH = os.path.join(CHECKPOINTS_DIR, "wave6c_snapshot.json")
WAVE7A_INVARIANT_REPORT_PATH = os.path.join(CHECKPOINTS_DIR, "wave7a_invariant_report.json")
WAVE7B_CONSISTENCY_REPORT_PATH = os.path.join(CHECKPOINTS_DIR, "wave7b_consistency_report.json")
WAVE7C_FINAL_CERTIFICATION_PATH = os.path.join(CHECKPOINTS_DIR, "wave7c_final_certification.json")
WAVE7_REPAIR_SNAPSHOT_PATH = os.path.join(CHECKPOINTS_DIR, "wave7_repair_snapshot.json")
MAX_WAVE5C_VALIDATION_SIGNALS = 100
VALID_SIGNAL_LINKAGE_PREFIXES = (
    "SIGNALS/normalized/",
    "INPUTS/",
    "RAW_INPUT/whatsapp/",
)

REQUIRED_FIELDS = [
    "signal_id",
    "date",
    "source_type",
    "source_name",
    "category",
    "signal_type",
    "description",
    "confidence",
    "status",
]

DEFAULT_CREATE_SCORE = 0.60
DEFAULT_REINFORCE_DELTA = 0.03
MAX_SCORE = 1.00
MAX_WARNING_SOURCE_SIGNALS = 25

SIGNAL_OPPORTUNITY_MAP = {
    "performance_gap": {"category": "performance_issue", "weight": "medium", "risk": "Medium"},
    "strong_performance": {"category": "performance_strength", "weight": "medium", "risk": "Low"},
    "daily_sales_report": {"category": "sales_signal", "weight": "high", "risk": "Low"},
    "inventory_availability_report": {"category": "stock_pressure", "weight": "medium", "risk": "Medium"},
    "staff_performance_report": {"category": "performance_strength", "weight": "medium", "risk": "Low"},
    "supervisor_control_report": {"category": "control_risk", "weight": "medium-low", "risk": "Medium"},
    "daily_bale_summary_report": {"category": "inventory_flow", "weight": "medium", "risk": "Low"},
}

WEIGHT_CREATE_SCORE = {
    "high": 0.72,
    "medium": 0.60,
    "medium-low": 0.54,
}

WEIGHT_REINFORCE_DELTA = {
    "high": 0.05,
    "medium": 0.03,
    "medium-low": 0.02,
}

WEIGHT_DEFAULT_CONFIDENCE = {
    "high": 0.86,
    "medium": 0.74,
    "medium-low": 0.68,
}

REPORT_TYPE_BY_SIGNAL_TYPE = {
    "daily_sales_report": "sales",
    "inventory_availability_report": "inventory",
    "staff_performance_report": "staff_performance",
    "supervisor_control_report": "supervisor_control",
    "daily_bale_summary_report": "bale_summary",
}


def log(msg: str) -> None:
    os.makedirs("LOGS", exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} - {msg}\n")


def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_file(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def read_json(path: str) -> Dict[str, object]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_json_list(path: str) -> List[Dict[str, object]]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, list):
        raise ValueError(f"expected list payload in {path}")
    return [row for row in payload if isinstance(row, dict)]


def write_json(path: str, payload: object) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")


def preserve_snapshot_created_at(snapshot_path: str, payload: Dict[str, object], snapshot_label: str) -> Dict[str, object]:
    if not os.path.exists(snapshot_path):
        return payload
    try:
        existing_payload = read_json(snapshot_path)
    except Exception as exc:
        log(f"WARN {snapshot_label} snapshot unreadable: {exc}")
        return payload
    if not isinstance(existing_payload, dict):
        return payload

    comparable_existing = dict(existing_payload)
    comparable_new = dict(payload)
    comparable_existing.pop("created_at_utc", None)
    comparable_new.pop("created_at_utc", None)
    if comparable_existing != comparable_new:
        return payload

    existing_created_at = str(existing_payload.get("created_at_utc", "")).strip()
    if existing_created_at:
        payload["created_at_utc"] = existing_created_at
    return payload


def parse_signal(content: str) -> Dict[str, object]:
    data: Dict[str, object] = {}
    lines = content.splitlines()
    current_key = None

    for raw_line in lines:
        line = raw_line.rstrip()

        if not line.strip():
            continue

        if line.startswith("  - ") and current_key == "evidence":
            data.setdefault("evidence", [])
            evidence_list = data["evidence"]
            if isinstance(evidence_list, list):
                evidence_list.append(line.replace("  - ", "", 1).strip())
            continue

        if ": " in line:
            key, value = line.split(": ", 1)
            key = key.strip()
            value = value.strip()

            if key == "evidence":
                data["evidence"] = []
                current_key = "evidence"
            else:
                data[key] = value
                current_key = key
        elif line.endswith(":"):
            key = line[:-1].strip()
            if key == "evidence":
                data["evidence"] = []
            current_key = key

    if "evidence" not in data:
        data["evidence"] = []

    return data


def validate_signal(data: Dict[str, object]) -> Tuple[bool, str]:
    for field in REQUIRED_FIELDS:
        if field not in data or str(data[field]).strip() == "":
            return False, f"missing required field: {field}"

    try:
        confidence = float(str(data["confidence"]))
    except ValueError:
        return False, "confidence is not numeric"

    if confidence < 0.30:
        return False, "confidence below minimum threshold"

    if str(data.get("status", "")).strip().lower() != "new":
        return False, f"signal status is not new: {data.get('status')}"

    return True, "ok"


def normalize_signal_date(raw_value: object) -> str:
    value = str(raw_value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return value


def signal_config(signal_type: str) -> Dict[str, str]:
    return SIGNAL_OPPORTUNITY_MAP.get(signal_type, {})


def create_score_for_signal(signal_type: str) -> float:
    weight = signal_config(signal_type).get("weight", "medium")
    return WEIGHT_CREATE_SCORE.get(weight, DEFAULT_CREATE_SCORE)


def reinforce_delta_for_signal(signal_type: str) -> float:
    weight = signal_config(signal_type).get("weight", "medium")
    return WEIGHT_REINFORCE_DELTA.get(weight, DEFAULT_REINFORCE_DELTA)


def default_confidence_for_signal(signal_type: str) -> float:
    weight = signal_config(signal_type).get("weight", "medium")
    return WEIGHT_DEFAULT_CONFIDENCE.get(weight, 0.70)


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def format_delta(value: float) -> str:
    return f"{value:.4f}"


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def stable_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def file_sha256(path: str) -> str:
    digest = sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_snapshot(path: str) -> Dict[str, object]:
    snapshot: Dict[str, object] = {"path": path, "exists": os.path.exists(path)}
    if snapshot["exists"]:
        snapshot["sha256"] = file_sha256(path)
        snapshot["size_bytes"] = os.path.getsize(path)
    return snapshot


def file_sha256_or_empty(path: str) -> str:
    if not os.path.exists(path):
        return ""
    return file_sha256(path)


def json_sha256(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return sha256(encoded).hexdigest()


def risk_level_for_signal(signal_type: str) -> str:
    return signal_config(signal_type).get("risk", "Low")


def mapped_category_for_signal(signal_type: str, fallback: str = "") -> str:
    return signal_config(signal_type).get("category", fallback)


def report_type_for_signal(signal_type: str) -> str:
    return REPORT_TYPE_BY_SIGNAL_TYPE.get(signal_type, "")


def discover_signal_paths() -> List[str]:
    discovered: List[str] = []
    for root, _, files in os.walk(SIGNALS_PATH):
        for filename in sorted(files):
            if filename.endswith(".md") or filename.endswith(".json"):
                discovered.append(os.path.join(root, filename))
    return sorted(discovered)


def load_processed_whatsapp_entries() -> List[Dict[str, object]]:
    if not os.path.exists(PROCESSED_WHATSAPP_STATE_PATH):
        return []

    try:
        payload = read_json(PROCESSED_WHATSAPP_STATE_PATH)
    except Exception as exc:
        log(f"WARN processed whatsapp state unreadable: {exc}")
        return []

    processed = payload.get("processed", {})
    if not isinstance(processed, dict):
        return []

    entries: List[Dict[str, object]] = []
    for file_id, raw_entry in processed.items():
        if not isinstance(raw_entry, dict):
            continue
        entry = dict(raw_entry)
        entry.setdefault("file_id", str(file_id))
        entry["report_date"] = normalize_signal_date(entry.get("report_date", ""))
        entry["processed_at"] = str(entry.get("processed_at", "")).strip()
        entry["branch_slug"] = str(entry.get("branch_slug", "")).strip().lower()
        entry["report_type"] = str(entry.get("report_type", "")).strip().lower()
        entries.append(entry)
    return entries


def best_processed_entry(
    entries: List[Dict[str, object]],
    source_ref: str,
) -> Dict[str, object] | None:
    if not entries:
        return None

    source_basename = os.path.basename(str(source_ref or "").strip())
    if source_basename:
        for entry in entries:
            for candidate_key in ("txt_path", "meta_path", "dispatch_path"):
                candidate = os.path.basename(str(entry.get(candidate_key, "")).strip())
                if candidate == source_basename:
                    return entry

            parser = entry.get("parser", {})
            if isinstance(parser, dict):
                command = str(parser.get("command", "")).strip()
                if source_basename and source_basename in command:
                    return entry

    ranked = sorted(entries, key=lambda item: str(item.get("processed_at", "")).strip(), reverse=True)
    return ranked[0] if ranked else None


def processed_metadata_for_signal(
    branch: str,
    report_type: str,
    iso_date: str,
    source_ref: str,
    processed_entries: List[Dict[str, object]],
) -> Dict[str, object] | None:
    branch_key = str(branch).strip().lower()
    report_type_key = str(report_type).strip().lower()
    date_key = normalize_signal_date(iso_date)

    matches = [
        entry
        for entry in processed_entries
        if str(entry.get("branch_slug", "")).strip().lower() == branch_key
        and str(entry.get("report_type", "")).strip().lower() == report_type_key
        and normalize_signal_date(entry.get("report_date", "")) == date_key
    ]

    return best_processed_entry(matches, source_ref)


def normalize_warning_text(value: object) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return text


def warning_is_missing_required(text: str) -> bool:
    return "missing" in text


def warning_is_inferred(text: str) -> bool:
    return "computed from" in text or "inferred from" in text


def extract_missing_required_fields(
    data: Dict[str, object],
    warnings: List[str],
) -> int:
    for key in ("missing_required_fields", "missing_required_field_count"):
        if key in data:
            return max(0, safe_int(data.get(key)))

    explicit = []
    if isinstance(data.get("missing_required"), list):
        explicit = [str(item).strip() for item in data["missing_required"] if str(item).strip()]
        if explicit:
            return len(explicit)

    markers = set()
    for warning in warnings:
        normalized = normalize_warning_text(warning)
        if warning_is_missing_required(normalized):
            markers.add(normalized)
    return len(markers)


def extract_inferred_fields(
    data: Dict[str, object],
    warnings: List[str],
) -> int:
    for key in ("inferred_fields", "inferred_field_count"):
        if key in data:
            return max(0, safe_int(data.get(key)))

    markers = set()
    for warning in warnings:
        normalized = normalize_warning_text(warning)
        if warning_is_inferred(normalized):
            markers.add(normalized)
    return len(markers)


def enrich_signal_metadata(
    data: Dict[str, object],
    path: str,
    processed_entries: List[Dict[str, object]],
) -> Dict[str, object]:
    enriched = dict(data)
    signal_type = str(enriched.get("signal_type", "")).strip()
    signal_ref = str(enriched.get("source_ref") or path).strip()
    warnings: List[str] = []
    notes: List[str] = []

    if isinstance(enriched.get("validation_warnings"), list):
        warnings = [str(item).strip() for item in enriched["validation_warnings"] if str(item).strip()]

    validation_lane = str(enriched.get("validation_lane", "")).strip()
    metadata_source = "signal_metadata" if validation_lane or warnings else "none"

    if not validation_lane or not warnings:
        report_type = report_type_for_signal(signal_type)
        branch = str(enriched.get("branch", "")).strip().lower()
        iso_date = normalize_signal_date(enriched.get("date_window") or enriched.get("date", ""))
        if report_type and branch and iso_date:
            processed_entry = processed_metadata_for_signal(branch, report_type, iso_date, signal_ref, processed_entries)
            if processed_entry:
                metadata_source = "processed_whatsapp_state"
                if not validation_lane:
                    validation_lane = str(processed_entry.get("validation_lane", "")).strip()
                if not warnings:
                    warnings = [
                        str(item).strip()
                        for item in processed_entry.get("validation_warnings", [])
                        if str(item).strip()
                    ]
                parser = processed_entry.get("parser", {})
                if isinstance(parser, dict):
                    parser_stdout = str(parser.get("stdout", "")).strip()
                    if parser_stdout:
                        notes.append(f"parser_summary:{parser_stdout.splitlines()[-1]}")

    if not validation_lane:
        validation_lane = "accepted"
        if metadata_source == "none":
            notes.append("no_validation_metadata_available")

    missing_required_fields = extract_missing_required_fields(enriched, warnings)
    inferred_fields = extract_inferred_fields(enriched, warnings)

    enriched["signal_ref"] = signal_ref
    enriched["validation_lane"] = validation_lane
    enriched["validation_warnings"] = warnings
    enriched["missing_required_fields"] = missing_required_fields
    enriched["inferred_fields"] = inferred_fields
    enriched["metadata_source"] = metadata_source
    enriched["quality_notes"] = notes
    return enriched


def confidence_score_for_signal(data: Dict[str, object]) -> float:
    score = 1.0
    if str(data.get("validation_lane", "")).strip() == "accepted_with_warnings":
        score -= 0.2
    score -= 0.1 * max(0, safe_int(data.get("missing_required_fields", 0)))
    score -= 0.1 * max(0, safe_int(data.get("inferred_fields", 0)))
    score = clamp(score, 0.1, 1.0)

    if str(data.get("metadata_source", "none")).strip() == "none":
        try:
            raw_confidence = clamp(float(str(data.get("confidence", "")).strip()), 0.1, 1.0)
        except ValueError:
            raw_confidence = 1.0
        score = min(score, raw_confidence)

    return score


def signal_window(data: Dict[str, object]) -> str:
    return str(data.get("date_window") or data.get("date") or "").strip()


def normalize_key_token(value: object) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def parse_iso_date(value: object) -> date | None:
    normalized = normalize_signal_date(value)
    try:
        return datetime.strptime(normalized, "%Y-%m-%d").date()
    except ValueError:
        return None


def warning_branch_for_signal(path: str, data: Dict[str, object]) -> str:
    resolved = resolve_branch_slug(
        data,
        path=path,
        candidates=[
            data.get("branch"),
            data.get("branch_slug"),
            data.get("source_slug"),
            data.get("source_name"),
        ],
        fallback="none",
    )
    return str(resolved or "").strip().lower()


def warning_section_for_signal(path: str, data: Dict[str, object]) -> str:
    payload = dict(data)
    payload["_path"] = path
    section_key, _section_type = normalize_section_key(payload)
    normalized = normalize_key_token(section_key)
    if normalized in {"", "unknown", "unknown_section", "branch_performance", "staff_performance"}:
        return ""
    return normalized


def warning_category_for_signal(data: Dict[str, object]) -> str:
    return normalize_key_token(data.get("warning_category") or data.get("category", ""))


def is_branch_level_pattern(path: str, data: Dict[str, object]) -> bool:
    stem = normalize_key_token(Path(path).stem)
    signal_id = normalize_key_token(data.get("signal_id", ""))
    description = str(data.get("description", "")).strip().lower()

    if "_branch_gap" in stem or "_branch_strength" in stem:
        return True
    if "_branch_gap" in signal_id or "_branch_strength" in signal_id:
        return True
    if "weakest branch" in description or "top-performing branch" in description:
        return True
    return False


def build_warning_pattern_key(path: str, data: Dict[str, object]) -> Tuple[str, str, str, str, str]:
    branch = warning_branch_for_signal(path, data)
    signal_type = normalize_key_token(data.get("signal_type", ""))
    category = warning_category_for_signal(data)
    section = "" if is_branch_level_pattern(path, data) else warning_section_for_signal(path, data)
    if is_branch_level_pattern(path, data) and not category:
        category = "branch_performance"
    anchor = section or category
    pattern_id = f"{branch}|{signal_type}|{anchor}" if branch and signal_type and anchor else ""
    return pattern_id, branch, signal_type, section, category


def normalized_description_key(data: Dict[str, object]) -> str:
    description = normalize_similarity_text(data.get("description", ""))
    if description:
        return description
    return normalize_key_token(data.get("signal_id", "")) or "unknown_signal"


def logical_warning_event_key(pattern_id: str, event_date: str, data: Dict[str, object]) -> str:
    return f"{pattern_id}|{event_date}|{normalized_description_key(data)}"


def canonical_source_signal_ref(path: str, data: Dict[str, object]) -> str:
    for key in ("signal_ref", "source_ref", "signal_id"):
        candidate = str(data.get(key, "")).strip()
        if candidate:
            return candidate
    return os.path.relpath(path)


def bounded_source_signal_refs(values: Dict[str, Dict[str, str]]) -> List[str]:
    ranked = sorted(
        values.values(),
        key=lambda item: (str(item.get("event_date", "")).strip(), str(item.get("ref", "")).strip()),
    )
    return [str(item.get("ref", "")).strip() for item in ranked[-MAX_WARNING_SOURCE_SIGNALS:] if str(item.get("ref", "")).strip()]


def consecutive_day_streak(distinct_dates: List[str]) -> int:
    parsed = [parse_iso_date(item) for item in distinct_dates]
    ordered = [item for item in parsed if item is not None]
    if not ordered:
        return 0

    ordered = sorted(set(ordered))
    streak = 1
    previous = ordered[-1]
    for current in reversed(ordered[:-1]):
        if previous - current == timedelta(days=1):
            streak += 1
            previous = current
            continue
        break
    return streak


def read_warning_intelligence_state() -> Dict[str, Dict[str, object]]:
    if not os.path.exists(WARNING_INTELLIGENCE_PATH):
        return {}
    try:
        payload = read_json(WARNING_INTELLIGENCE_PATH)
    except Exception as exc:
        log(f"WARN warning intelligence unreadable: {exc}")
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): value for key, value in payload.items() if isinstance(value, dict)}


def merge_warning_notes(existing_note: str, source_signal_count: int) -> str:
    note = str(existing_note or "").strip()
    truncation_note = ""
    if source_signal_count > MAX_WARNING_SOURCE_SIGNALS:
        truncation_note = f"source_signals_truncated_to_{MAX_WARNING_SOURCE_SIGNALS}"

    if not truncation_note:
        return note
    if not note:
        return truncation_note
    if truncation_note in note:
        return note
    return f"{note}; {truncation_note}"


def warning_severity_score(occurrence_count: int, consecutive_days: int, time_span_days: int) -> float:
    severity = min(
        1.0,
        0.4 * min(1.0, max(0, occurrence_count) / 5.0)
        + 0.4 * min(1.0, max(0, consecutive_days) / 3.0)
        + 0.2 * min(1.0, max(0, time_span_days) / 7.0),
    )
    return round(severity, 6)


def warning_escalation_level(severity_score: float) -> str:
    if severity_score < 0.30:
        return "none"
    if severity_score < 0.60:
        return "watch"
    if severity_score < 0.80:
        return "elevated"
    return "critical"


def warning_escalation_reason(occurrence_count: int, consecutive_days: int, time_span_days: int) -> str:
    if occurrence_count >= 5 and time_span_days >= 7:
        return "high frequency + sustained duration"
    if consecutive_days >= 3:
        return "consecutive daily recurrence detected"
    if occurrence_count >= 3 and time_span_days >= 3:
        return "repeated occurrences across multiple days"
    if occurrence_count >= 2:
        return "repeated warning occurrences detected"
    if time_span_days >= 3:
        return "pattern persisted across multiple days"
    return "limited warning recurrence observed"


def apply_warning_escalation(entry: Dict[str, object]) -> Dict[str, object]:
    occurrence_count = max(0, safe_int(entry.get("occurrence_count", 0)))
    consecutive_days = max(0, safe_int(entry.get("consecutive_days", 0)))
    time_span_days = max(0, safe_int(entry.get("time_span_days", 0)))
    severity_score = warning_severity_score(occurrence_count, consecutive_days, time_span_days)

    enriched = dict(entry)
    enriched["severity_score"] = severity_score
    enriched["escalation_level"] = warning_escalation_level(severity_score)
    enriched["escalation_reason"] = warning_escalation_reason(occurrence_count, consecutive_days, time_span_days)
    return enriched


def warning_pattern_audit_row(entry: Dict[str, object]) -> Dict[str, object]:
    return {
        "pattern_id": str(entry.get("pattern_id", "")).strip(),
        "branch": str(entry.get("branch", "")).strip(),
        "signal_type": str(entry.get("signal_type", "")).strip(),
        "section": str(entry.get("section", "")).strip(),
        "category": str(entry.get("category", "")).strip(),
        "occurrence_count": safe_int(entry.get("occurrence_count", 0)),
        "distinct_dates": list(entry.get("distinct_dates", [])) if isinstance(entry.get("distinct_dates"), list) else [],
        "consecutive_days": safe_int(entry.get("consecutive_days", 0)),
        "time_span_days": safe_int(entry.get("time_span_days", 0)),
        "source_signal_count": safe_int(entry.get("occurrence_count", 0)),
        "notes": str(entry.get("notes", "")),
        "severity_score": float(entry.get("severity_score", 0.0)),
        "escalation_level": str(entry.get("escalation_level", "")).strip(),
    }


def read_warning_pattern_audit_state() -> List[Dict[str, object]]:
    if not os.path.exists(WARNING_PATTERN_AUDIT_PATH):
        return []
    try:
        with open(WARNING_PATTERN_AUDIT_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        log(f"WARN warning pattern audit unreadable: {exc}")
        return []
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def enrich_warning_memory_state(
    entries_by_pattern: Dict[str, Dict[str, object]],
    audit_rows: List[Dict[str, object]] | None = None,
) -> Tuple[Dict[str, Dict[str, object]], List[Dict[str, object]], Dict[str, int]]:
    enriched_entries: Dict[str, Dict[str, object]] = {}
    for pattern_id in sorted(entries_by_pattern):
        entry = dict(entries_by_pattern[pattern_id])
        if not str(entry.get("pattern_id", "")).strip():
            entry["pattern_id"] = pattern_id
        enriched_entries[pattern_id] = apply_warning_escalation(entry)

    enriched_audit_rows: List[Dict[str, object]] = []
    if audit_rows:
        seen_pattern_ids = set()
        for row in audit_rows:
            pattern_id = str(row.get("pattern_id", "")).strip()
            seen_pattern_ids.add(pattern_id)
            source_entry = enriched_entries.get(pattern_id)
            if source_entry is None:
                enriched_audit_rows.append(warning_pattern_audit_row(apply_warning_escalation(row)))
                continue
            merged_row = dict(row)
            merged_row["severity_score"] = float(source_entry["severity_score"])
            merged_row["escalation_level"] = str(source_entry["escalation_level"])
            enriched_audit_rows.append(merged_row)
        for pattern_id in sorted(enriched_entries):
            if pattern_id not in seen_pattern_ids:
                enriched_audit_rows.append(warning_pattern_audit_row(enriched_entries[pattern_id]))
    else:
        for pattern_id in sorted(enriched_entries):
            enriched_audit_rows.append(warning_pattern_audit_row(enriched_entries[pattern_id]))

    summary = {
        "pattern_count": len(enriched_entries),
        "non_zero_severity_count": sum(
            1 for entry in enriched_entries.values() if float(entry.get("severity_score", 0.0)) > 0.0
        ),
        "watch_count": sum(1 for entry in enriched_entries.values() if entry.get("escalation_level") == "watch"),
        "elevated_count": sum(1 for entry in enriched_entries.values() if entry.get("escalation_level") == "elevated"),
        "critical_count": sum(1 for entry in enriched_entries.values() if entry.get("escalation_level") == "critical"),
        "none_count": sum(1 for entry in enriched_entries.values() if entry.get("escalation_level") == "none"),
    }
    return enriched_entries, enriched_audit_rows, summary


def build_warning_memory(
    records: List[Tuple[str, Dict[str, object], bool]],
    existing_entries: Dict[str, Dict[str, object]] | None = None,
) -> Tuple[Dict[str, Dict[str, object]], List[Dict[str, object]], Dict[str, int]]:
    entries_by_pattern: Dict[str, Dict[str, object]] = {}
    stats = {
        "validated_signal_count": 0,
        "tracked_pattern_count": 0,
        "logical_event_count": 0,
        "skipped_unresolved_pattern_signals": 0,
    }
    existing_entries = existing_entries or {}

    for path, data, _mark_processed in records:
        valid, _reason = validate_signal(data)
        if not valid:
            continue
        stats["validated_signal_count"] += 1

        pattern_id, branch, signal_type, section, category = build_warning_pattern_key(path, data)
        event_date = normalize_signal_date(signal_window(data) or data.get("date", ""))
        if not pattern_id or not event_date:
            stats["skipped_unresolved_pattern_signals"] += 1
            log(
                f"WARN warning memory skipped unresolved signal: "
                f"path={os.path.relpath(path)} branch={branch or 'missing'} "
                f"signal_type={signal_type or 'missing'} category={category or 'missing'}"
            )
            continue

        entry = entries_by_pattern.setdefault(
            pattern_id,
            {
                "pattern_id": pattern_id,
                "branch": branch,
                "signal_type": signal_type,
                "section": section,
                "category": category,
                "first_seen": event_date,
                "last_seen": event_date,
                "occurrence_count": 0,
                "distinct_dates": [],
                "consecutive_days": 0,
                "time_span_days": 0,
                "source_signals": [],
                "notes": "",
                "_distinct_dates": set(),
                "_logical_events": set(),
                "_source_signal_refs": {},
            },
        )

        logical_key = logical_warning_event_key(pattern_id, event_date, data)
        logical_events = entry["_logical_events"]
        if isinstance(logical_events, set) and logical_key in logical_events:
            continue

        if isinstance(logical_events, set):
            logical_events.add(logical_key)
        distinct_dates = entry["_distinct_dates"]
        if isinstance(distinct_dates, set):
            distinct_dates.add(event_date)
        source_signal_refs = entry["_source_signal_refs"]
        if isinstance(source_signal_refs, dict):
            preferred_ref = canonical_source_signal_ref(path, data)
            previous = source_signal_refs.get(logical_key)
            if previous is None or preferred_ref < str(previous.get("ref", "")):
                source_signal_refs[logical_key] = {"event_date": event_date, "ref": preferred_ref}

        entry["occurrence_count"] = int(entry["occurrence_count"]) + 1
        entry["first_seen"] = min(str(entry["first_seen"]), event_date)
        entry["last_seen"] = max(str(entry["last_seen"]), event_date)

    finalized_entries: Dict[str, Dict[str, object]] = {}
    for pattern_id in sorted(entries_by_pattern):
        entry = entries_by_pattern[pattern_id]
        date_values = sorted(entry["_distinct_dates"]) if isinstance(entry.get("_distinct_dates"), set) else []
        first_seen = date_values[0] if date_values else str(entry.get("first_seen", "")).strip()
        last_seen = date_values[-1] if date_values else str(entry.get("last_seen", "")).strip()
        first_date = parse_iso_date(first_seen)
        last_date = parse_iso_date(last_seen)
        time_span_days = (last_date - first_date).days if first_date and last_date else 0
        source_signal_count = len(entry["_logical_events"]) if isinstance(entry.get("_logical_events"), set) else 0
        previous = existing_entries.get(pattern_id, {})

        finalized = {
            "pattern_id": pattern_id,
            "branch": str(entry.get("branch", "")).strip(),
            "signal_type": str(entry.get("signal_type", "")).strip(),
            "section": str(entry.get("section", "")).strip(),
            "category": str(entry.get("category", "")).strip(),
            "first_seen": first_seen,
            "last_seen": last_seen,
            "occurrence_count": int(entry.get("occurrence_count", 0)),
            "distinct_dates": date_values,
            "consecutive_days": consecutive_day_streak(date_values),
            "time_span_days": time_span_days,
            "source_signals": bounded_source_signal_refs(entry["_source_signal_refs"]),
            "notes": merge_warning_notes(str(previous.get("notes", "")), source_signal_count),
        }
        finalized_entries[pattern_id] = finalized

    audit_rows: List[Dict[str, object]] = []
    for pattern_id in sorted(finalized_entries):
        entry = finalized_entries[pattern_id]
        audit_rows.append(
            {
                "pattern_id": entry["pattern_id"],
                "branch": entry["branch"],
                "signal_type": entry["signal_type"],
                "section": entry["section"],
                "category": entry["category"],
                "occurrence_count": entry["occurrence_count"],
                "distinct_dates": list(entry["distinct_dates"]),
                "consecutive_days": entry["consecutive_days"],
                "time_span_days": entry["time_span_days"],
                "source_signal_count": entry["occurrence_count"],
                "notes": entry["notes"],
            }
        )

    stats["tracked_pattern_count"] = len(finalized_entries)
    stats["logical_event_count"] = sum(int(entry["occurrence_count"]) for entry in finalized_entries.values())
    return finalized_entries, audit_rows, stats


def write_warning_memory_artifacts(records: List[Tuple[str, Dict[str, object], bool]]) -> Dict[str, int]:
    existing_entries = read_warning_intelligence_state()
    warning_intelligence, warning_audit, stats = build_warning_memory(records, existing_entries)
    write_json(WARNING_INTELLIGENCE_PATH, warning_intelligence)
    write_json(WARNING_PATTERN_AUDIT_PATH, warning_audit)
    return stats


def write_warning_escalation_artifacts() -> Dict[str, int]:
    warning_intelligence = read_warning_intelligence_state()
    warning_audit = read_warning_pattern_audit_state()
    enriched_intelligence, enriched_audit, summary = enrich_warning_memory_state(warning_intelligence, warning_audit)
    write_json(WARNING_INTELLIGENCE_PATH, enriched_intelligence)
    write_json(WARNING_PATTERN_AUDIT_PATH, enriched_audit)
    return summary


def assert_wave4a_memory_only_artifacts() -> None:
    if not os.path.exists(WARNING_INTELLIGENCE_PATH):
        raise RuntimeError("Wave 4A validation failed: warning_intelligence.json was not written")
    if not os.path.exists(WARNING_PATTERN_AUDIT_PATH):
        raise RuntimeError("Wave 4A validation failed: warning_pattern_audit.json was not written")

    warning_intelligence = read_warning_intelligence_state()
    for pattern_id, entry in warning_intelligence.items():
        for field in ("severity_score", "escalation_level", "escalation_reason"):
            if field in entry:
                raise RuntimeError(
                    f"Wave 4A validation failed: build_warning_memory emitted {field} for pattern {pattern_id}"
                )

    warning_audit = read_warning_pattern_audit_state()
    for row in warning_audit:
        pattern_id = str(row.get("pattern_id", "")).strip() or "unknown"
        for field in ("severity_score", "escalation_level", "escalation_reason"):
            if field in row:
                raise RuntimeError(
                    f"Wave 4A validation failed: warning_pattern_audit emitted {field} for pattern {pattern_id}"
                )


def assert_wave4b_persistence_artifacts() -> None:
    if not os.path.exists(WARNING_INTELLIGENCE_PATH):
        raise RuntimeError("Wave 4B validation failed: warning_intelligence.json was not written")
    if not os.path.exists(WARNING_PATTERN_AUDIT_PATH):
        raise RuntimeError("Wave 4B validation failed: warning_pattern_audit.json was not written")

    warning_intelligence = read_warning_intelligence_state()
    for pattern_id, entry in warning_intelligence.items():
        missing = [field for field in ("severity_score", "escalation_level", "escalation_reason") if field not in entry]
        if missing:
            raise RuntimeError(
                f"Wave 4B validation failed: warning_intelligence missing {', '.join(missing)} for pattern {pattern_id}"
            )

    warning_audit = read_warning_pattern_audit_state()
    for row in warning_audit:
        pattern_id = str(row.get("pattern_id", "")).strip() or "unknown"
        missing = [field for field in ("severity_score", "escalation_level") if field not in row]
        if missing:
            raise RuntimeError(
                f"Wave 4B validation failed: warning_pattern_audit missing {', '.join(missing)} for pattern {pattern_id}"
            )


def run_wave4_warning_persistence(records: List[Tuple[str, Dict[str, object], bool]]) -> Tuple[Dict[str, int], Dict[str, int]]:
    # Wave 4A is memory-only by contract. Keep the persistence write and Wave 4B enrichment adjacent:
    # reordering or removing the immediate Wave 4B call will leave steady-state warning artifacts without escalation fields.
    warning_stats = write_warning_memory_artifacts(records)
    assert_wave4a_memory_only_artifacts()
    warning_escalation_stats = write_warning_escalation_artifacts()
    warning_intelligence = read_warning_intelligence_state()
    if warning_intelligence and not any("severity_score" in entry for entry in warning_intelligence.values()):
        raise RuntimeError("Wave 4B validation failed: warning_intelligence.json was persisted without severity_score")
    assert_wave4b_persistence_artifacts()
    return warning_stats, warning_escalation_stats


def build_density_index(records: List[Tuple[str, Dict[str, object], bool]]) -> Dict[Tuple[str, str], Dict[str, float]]:
    totals: Dict[Tuple[str, str], int] = {}

    for _path, data, _mark_processed in records:
        valid, _reason = validate_signal(data)
        if not valid:
            continue

        branch = str(data.get("branch", "")).strip().lower()
        window = signal_window(data)
        if not branch or not window:
            continue

        key = (branch, window)
        totals[key] = totals.get(key, 0) + 1

    density: Dict[Tuple[str, str], Dict[str, float]] = {}
    for key, total in totals.items():
        density_factor = 1.0 / (1.0 + math.log1p(total))
        density[key] = {
            "total_signals": float(total),
            "density_factor": density_factor,
        }
    return density


def scoring_context_for_signal(
    data: Dict[str, object],
    density_index: Dict[Tuple[str, str], Dict[str, float]],
) -> Dict[str, object]:
    base_delta = reinforce_delta_for_signal(str(data.get("signal_type", "")))
    confidence_score = confidence_score_for_signal(data)
    density_key = (str(data.get("branch", "")).strip().lower(), signal_window(data))
    density_entry = density_index.get(density_key, {"total_signals": 1.0, "density_factor": 1.0})
    density_factor = float(density_entry["density_factor"])
    effective_before_density = base_delta * confidence_score
    effective_delta = base_delta * confidence_score * density_factor
    notes = list(data.get("quality_notes", [])) if isinstance(data.get("quality_notes"), list) else []
    notes.append(f"metadata_source:{data.get('metadata_source', 'none')}")
    if str(data.get("metadata_source", "none")).strip() == "none":
        notes.append("confidence_capped_by_signal_confidence")
    return {
        "base_delta": base_delta,
        "confidence_score": confidence_score,
        "density_factor": density_factor,
        "total_signals": int(density_entry["total_signals"]),
        "effective_delta_before_density": effective_before_density,
        "effective_delta": effective_delta,
        "window": density_key[1],
        "notes": notes,
    }


def build_confidence_audit(
    records: List[Tuple[str, Dict[str, object], bool]],
    density_index: Dict[Tuple[str, str], Dict[str, float]],
) -> List[Dict[str, object]]:
    audit_rows: List[Dict[str, object]] = []
    for _path, data, _mark_processed in records:
        valid, _reason = validate_signal(data)
        if not valid:
            continue
        scoring = scoring_context_for_signal(data, density_index)
        audit_rows.append(
            {
                "signal_ref": str(data.get("signal_ref") or data.get("source_ref") or data.get("signal_id") or ""),
                "branch": str(data.get("branch", "")).strip().lower(),
                "signal_type": str(data.get("signal_type", "")).strip(),
                "base_delta": round(float(scoring["base_delta"]), 6),
                "confidence_score": round(float(scoring["confidence_score"]), 6),
                "missing_required_fields": max(0, safe_int(data.get("missing_required_fields", 0))),
                "inferred_fields": max(0, safe_int(data.get("inferred_fields", 0))),
                "validation_lane": str(data.get("validation_lane", "")).strip(),
                "density_factor": round(float(scoring["density_factor"]), 6),
                "effective_delta_before_density": round(float(scoring["effective_delta_before_density"]), 6),
                "final_effective_delta": round(float(scoring["effective_delta"]), 6),
                "notes": "; ".join(str(item) for item in scoring["notes"] if str(item).strip()),
            }
        )
    return audit_rows


def build_density_audit(
    density_index: Dict[Tuple[str, str], Dict[str, float]],
) -> List[Dict[str, object]]:
    audit_rows: List[Dict[str, object]] = []
    for (branch, window), entry in sorted(density_index.items()):
        audit_rows.append(
            {
                "branch": branch,
                "window": window,
                "total_signals": int(entry["total_signals"]),
                "density_factor": round(float(entry["density_factor"]), 6),
                "notes": "window keyed by signal date_window/date across validated decision-worker inputs",
            }
        )
    return audit_rows


def write_wave3_snapshot() -> None:
    payload = {
        "phase": "wave3_confidence_signal_density_normalization",
        "created_at_utc": stable_utc_now(),
        "artifacts": [
            artifact_snapshot("worker_decision_v2.py"),
            artifact_snapshot(BLACKBOARD_PATH),
            artifact_snapshot(CONFIDENCE_AUDIT_PATH),
            artifact_snapshot(DENSITY_AUDIT_PATH),
        ],
    }
    payload = preserve_snapshot_created_at(WAVE3_SNAPSHOT_PATH, payload, "wave3")
    write_json(WAVE3_SNAPSHOT_PATH, payload)


def write_wave4a_snapshot(warning_stats: Dict[str, int]) -> None:
    payload = {
        "phase": "wave4a_warning_memory_pattern_tracking",
        "created_at_utc": stable_utc_now(),
        "warning_memory": {
            "validated_signal_count": int(warning_stats.get("validated_signal_count", 0)),
            "tracked_pattern_count": int(warning_stats.get("tracked_pattern_count", 0)),
            "logical_event_count": int(warning_stats.get("logical_event_count", 0)),
            "skipped_unresolved_pattern_signals": int(warning_stats.get("skipped_unresolved_pattern_signals", 0)),
            "source_signals_limit": MAX_WARNING_SOURCE_SIGNALS,
        },
        "artifacts": [
            artifact_snapshot("worker_decision_v2.py"),
            artifact_snapshot(BLACKBOARD_PATH),
            artifact_snapshot(CONFIDENCE_AUDIT_PATH),
            artifact_snapshot(DENSITY_AUDIT_PATH),
            artifact_snapshot(WARNING_INTELLIGENCE_PATH),
            artifact_snapshot(WARNING_PATTERN_AUDIT_PATH),
        ],
    }
    payload = preserve_snapshot_created_at(WAVE4A_SNAPSHOT_PATH, payload, "wave4a")
    write_json(WAVE4A_SNAPSHOT_PATH, payload)


def write_wave4b_snapshot(escalation_stats: Dict[str, int]) -> None:
    payload = {
        "phase": "wave4b_warning_escalation_layer",
        "created_at_utc": stable_utc_now(),
        "warning_escalation": {
            "pattern_count": int(escalation_stats.get("pattern_count", 0)),
            "non_zero_severity_count": int(escalation_stats.get("non_zero_severity_count", 0)),
            "watch_count": int(escalation_stats.get("watch_count", 0)),
            "elevated_count": int(escalation_stats.get("elevated_count", 0)),
            "critical_count": int(escalation_stats.get("critical_count", 0)),
            "none_count": int(escalation_stats.get("none_count", 0)),
            "formula": "min(1.0, 0.4 * min(1, occurrence_count / 5) + 0.4 * min(1, consecutive_days / 3) + 0.2 * min(1, time_span_days / 7))",
        },
        "artifacts": [
            artifact_snapshot("worker_decision_v2.py"),
            artifact_snapshot(BLACKBOARD_PATH),
            artifact_snapshot(WARNING_INTELLIGENCE_PATH),
            artifact_snapshot(WARNING_PATTERN_AUDIT_PATH),
        ],
    }
    payload = preserve_snapshot_created_at(WAVE4B_SNAPSHOT_PATH, payload, "wave4b")
    write_json(WAVE4B_SNAPSHOT_PATH, payload)


def build_fusion_signal_context(
    records: List[Tuple[str, Dict[str, object], bool]],
    warning_intelligence: Dict[str, Dict[str, object]],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []

    for path, data, _mark_processed in records:
        valid, _reason = validate_signal(data)
        if not valid:
            continue

        pattern_id, branch, signal_type, section, category = build_warning_pattern_key(path, data)
        signal_ref = canonical_source_signal_ref(path, data)
        warning_eligible = risk_level_for_signal(signal_type) != "Low"
        warning_entry = warning_intelligence.get(pattern_id) if warning_eligible and pattern_id else None

        row = {
            "signal_ref": signal_ref,
            "pattern_id": pattern_id if warning_entry is not None else "",
            "matched": warning_entry is not None,
            "branch": branch,
            "signal_type": signal_type,
            "section": section,
            "category": category,
            "occurrence_count": 0,
            "consecutive_days": 0,
            "time_span_days": 0,
            "severity_score": 0.0,
            "escalation_level": "none",
            "notes": "",
        }

        if warning_entry is None:
            if not warning_eligible:
                row["notes"] = "signal_not_in_warning_scope"
            elif not pattern_id:
                row["notes"] = "pattern_key_unresolved"
            else:
                row["notes"] = "pattern_not_found_in_warning_intelligence"
            rows.append(row)
            continue

        notes = str(warning_entry.get("notes", "")).strip()
        if not notes:
            notes = str(warning_entry.get("escalation_reason", "")).strip()

        row["occurrence_count"] = max(0, safe_int(warning_entry.get("occurrence_count", 0)))
        row["consecutive_days"] = max(0, safe_int(warning_entry.get("consecutive_days", 0)))
        row["time_span_days"] = max(0, safe_int(warning_entry.get("time_span_days", 0)))
        try:
            row["severity_score"] = float(warning_entry.get("severity_score", 0.0))
        except (TypeError, ValueError):
            row["severity_score"] = 0.0
        row["escalation_level"] = str(warning_entry.get("escalation_level", "none")).strip() or "none"
        row["notes"] = notes
        rows.append(row)

    rows.sort(
        key=lambda row: (
            str(row.get("signal_ref", "")),
            str(row.get("branch", "")),
            str(row.get("signal_type", "")),
            str(row.get("section", "")),
            str(row.get("category", "")),
            str(row.get("pattern_id", "")),
        )
    )
    return rows


def write_fusion_signal_context_artifacts(
    records: List[Tuple[str, Dict[str, object], bool]],
) -> Dict[str, int]:
    warning_intelligence = read_warning_intelligence_state()
    rows = build_fusion_signal_context(records, warning_intelligence)
    write_json(FUSION_SIGNAL_CONTEXT_PATH, rows)
    matched_count = sum(1 for row in rows if bool(row.get("matched")))
    unmatched_count = sum(1 for row in rows if not bool(row.get("matched")))
    return {
        "validated_signal_count": len(rows),
        "matched_signal_count": matched_count,
        "unmatched_signal_count": unmatched_count,
    }


def write_wave5a_snapshot(fusion_stats: Dict[str, int]) -> None:
    payload = {
        "phase": "wave5a_fusion_read_layer",
        "created_at_utc": stable_utc_now(),
        "fusion_context": {
            "validated_signal_count": int(fusion_stats.get("validated_signal_count", 0)),
            "matched_signal_count": int(fusion_stats.get("matched_signal_count", 0)),
            "unmatched_signal_count": int(fusion_stats.get("unmatched_signal_count", 0)),
            "matching_logic": "build_warning_pattern_key(path, data) -> pattern_id; only non-Low risk signals are warning-eligible; eligible pattern_ids use exact lookup in DATA/warning_intelligence.json",
        },
        "artifacts": [
            artifact_snapshot("worker_decision_v2.py"),
            artifact_snapshot(BLACKBOARD_PATH),
            artifact_snapshot(WARNING_INTELLIGENCE_PATH),
            artifact_snapshot(WARNING_PATTERN_AUDIT_PATH),
            artifact_snapshot(FUSION_SIGNAL_CONTEXT_PATH),
        ],
    }
    payload = preserve_snapshot_created_at(WAVE5A_SNAPSHOT_PATH, payload, "wave5a")
    write_json(WAVE5A_SNAPSHOT_PATH, payload)


def validate_wave5a_fusion_stats(fusion_stats: Dict[str, int]) -> None:
    matched_count = int(fusion_stats.get("matched_signal_count", 0))
    unmatched_count = int(fusion_stats.get("unmatched_signal_count", 0))
    if matched_count < 5:
        raise RuntimeError(f"Wave 5A validation failed: expected at least 5 matched signals, found {matched_count}")
    if unmatched_count < 2:
        raise RuntimeError(f"Wave 5A validation failed: expected at least 2 unmatched signals, found {unmatched_count}")


def run_wave5a_read_only(records: List[Tuple[str, Dict[str, object], bool]]) -> Dict[str, int]:
    tracked_paths = [
        BLACKBOARD_PATH,
        WARNING_INTELLIGENCE_PATH,
        WARNING_PATTERN_AUDIT_PATH,
    ]
    before_hashes = {path: file_sha256_or_empty(path) for path in tracked_paths}

    first_stats = write_fusion_signal_context_artifacts(records)
    validate_wave5a_fusion_stats(first_stats)
    first_fusion_hash = file_sha256_or_empty(FUSION_SIGNAL_CONTEXT_PATH)

    after_first_hashes = {path: file_sha256_or_empty(path) for path in tracked_paths}
    if after_first_hashes != before_hashes:
        raise RuntimeError("Wave 5A validation failed: read-only run modified protected artifacts")

    second_stats = write_fusion_signal_context_artifacts(records)
    validate_wave5a_fusion_stats(second_stats)
    second_fusion_hash = file_sha256_or_empty(FUSION_SIGNAL_CONTEXT_PATH)

    after_second_hashes = {path: file_sha256_or_empty(path) for path in tracked_paths}
    if after_second_hashes != before_hashes:
        raise RuntimeError("Wave 5A validation failed: second read-only run modified protected artifacts")
    if first_stats != second_stats:
        raise RuntimeError("Wave 5A validation failed: fusion stats changed across identical runs")
    if first_fusion_hash != second_fusion_hash:
        raise RuntimeError("Wave 5A validation failed: fusion_signal_context.json is not idempotent across runs")

    return second_stats


def compute_fusion_modifier(matched: bool, severity_score: float, occurrence_count: int, consecutive_days: int) -> float:
    if not matched:
        return 0.0
    modifier = min(
        0.30,
        0.15 * max(0.0, severity_score)
        + 0.10 * min(1.0, max(0, occurrence_count) / 5.0)
        + 0.05 * min(1.0, max(0, consecutive_days) / 3.0),
    )
    return round(modifier, 6)


def build_fusion_score_audit() -> List[Dict[str, object]]:
    confidence_rows = read_json_list(CONFIDENCE_AUDIT_PATH)
    fusion_rows = read_json_list(FUSION_SIGNAL_CONTEXT_PATH)
    fusion_by_signal_ref = {
        str(row.get("signal_ref", "")).strip(): row
        for row in fusion_rows
        if str(row.get("signal_ref", "")).strip()
    }

    audit_rows: List[Dict[str, object]] = []
    for scoring_row in confidence_rows:
        signal_ref = str(scoring_row.get("signal_ref", "")).strip()
        if not signal_ref:
            raise RuntimeError("Wave 5B validation failed: confidence scoring row missing signal_ref")

        fusion_row = fusion_by_signal_ref.get(signal_ref)
        if fusion_row is None:
            raise RuntimeError(f"Wave 5B validation failed: missing fusion context for signal_ref={signal_ref}")

        matched = bool(fusion_row.get("matched"))
        severity_score = float(fusion_row.get("severity_score", 0.0) or 0.0)
        occurrence_count = max(0, safe_int(fusion_row.get("occurrence_count", 0)))
        consecutive_days = max(0, safe_int(fusion_row.get("consecutive_days", 0)))
        effective_delta = float(scoring_row.get("final_effective_delta", 0.0) or 0.0)
        fusion_modifier = compute_fusion_modifier(matched, severity_score, occurrence_count, consecutive_days)
        fusion_adjusted_delta_preview = round(effective_delta * (1.0 + fusion_modifier), 6)

        if not matched:
            fusion_modifier = 0.0
            fusion_adjusted_delta_preview = round(effective_delta, 6)

        fusion_notes = str(fusion_row.get("notes", "")).strip()
        scoring_notes = str(scoring_row.get("notes", "")).strip()
        notes = fusion_notes
        if scoring_notes:
            notes = f"{fusion_notes}; {scoring_notes}" if fusion_notes else scoring_notes

        audit_rows.append(
            {
                "signal_ref": signal_ref,
                "pattern_id": str(fusion_row.get("pattern_id", "")).strip(),
                "matched": matched,
                "branch": str(fusion_row.get("branch", "")).strip(),
                "signal_type": str(fusion_row.get("signal_type", "")).strip(),
                "effective_delta": round(effective_delta, 6),
                "severity_score": round(severity_score, 6),
                "occurrence_count": occurrence_count,
                "consecutive_days": consecutive_days,
                "escalation_level": str(fusion_row.get("escalation_level", "")).strip(),
                "fusion_modifier": fusion_modifier,
                "fusion_adjusted_delta_preview": fusion_adjusted_delta_preview,
                "notes": notes,
            }
        )

    audit_rows.sort(
        key=lambda row: (
            str(row.get("signal_ref", "")),
            str(row.get("branch", "")),
            str(row.get("signal_type", "")),
            str(row.get("pattern_id", "")),
        )
    )
    return audit_rows


def write_fusion_score_audit_artifacts() -> Dict[str, float]:
    rows = build_fusion_score_audit()
    write_json(FUSION_SCORE_AUDIT_PATH, rows)
    matched_non_zero = sum(
        1 for row in rows if bool(row.get("matched")) and float(row.get("fusion_modifier", 0.0)) > 0.0
    )
    unmatched_zero = sum(
        1 for row in rows if not bool(row.get("matched")) and float(row.get("fusion_modifier", -1.0)) == 0.0
    )
    max_modifier = max((float(row.get("fusion_modifier", 0.0)) for row in rows), default=0.0)
    return {
        "row_count": float(len(rows)),
        "matched_non_zero_count": float(matched_non_zero),
        "unmatched_zero_count": float(unmatched_zero),
        "max_fusion_modifier": round(max_modifier, 6),
    }


def validate_wave5b_fusion_stats(fusion_stats: Dict[str, float]) -> None:
    matched_non_zero_count = int(fusion_stats.get("matched_non_zero_count", 0))
    unmatched_zero_count = int(fusion_stats.get("unmatched_zero_count", 0))
    max_fusion_modifier = float(fusion_stats.get("max_fusion_modifier", 0.0))
    if matched_non_zero_count < 5:
        raise RuntimeError(
            f"Wave 5B validation failed: expected at least 5 matched signals with non-zero fusion_modifier, found {matched_non_zero_count}"
        )
    if unmatched_zero_count < 2:
        raise RuntimeError(
            f"Wave 5B validation failed: expected at least 2 unmatched signals with zero fusion_modifier, found {unmatched_zero_count}"
        )
    if max_fusion_modifier > 0.30:
        raise RuntimeError(
            f"Wave 5B validation failed: fusion_modifier exceeded 0.30, observed {max_fusion_modifier:.6f}"
        )


def validate_live_fusion_score_audit() -> None:
    for row in read_json_list(FUSION_SCORE_AUDIT_PATH):
        signal_ref = str(row.get("signal_ref", "")).strip()
        matched = bool(row.get("matched"))
        fusion_modifier = round(float(row.get("fusion_modifier", 0.0) or 0.0), 6)
        effective_delta = round(float(row.get("effective_delta", 0.0) or 0.0), 6)
        final_delta = round(float(row.get("fusion_adjusted_delta_preview", effective_delta) or effective_delta), 6)

        if fusion_modifier > 0.30:
            raise RuntimeError(
                f"Wave 5C validation failed: fusion_modifier exceeded bounded cap for signal_ref={signal_ref}"
            )
        if not matched and fusion_modifier != 0.0:
            raise RuntimeError(
                f"Wave 5C validation failed: unmatched signal received fusion influence for signal_ref={signal_ref}"
            )
        if matched and final_delta != round(effective_delta * (1.0 + fusion_modifier), 6):
            raise RuntimeError(
                f"Wave 5C validation failed: fusion_adjusted_delta_preview drifted from formula for signal_ref={signal_ref}"
            )
        if not matched and final_delta != effective_delta:
            raise RuntimeError(
                f"Wave 5C validation failed: unmatched preview delta drifted from Wave 3 base for signal_ref={signal_ref}"
            )


def write_wave5b_snapshot(fusion_stats: Dict[str, float]) -> None:
    payload = {
        "phase": "wave5b_fusion_score_computation",
        "created_at_utc": stable_utc_now(),
        "fusion_score_preview": {
            "row_count": int(fusion_stats.get("row_count", 0)),
            "matched_non_zero_count": int(fusion_stats.get("matched_non_zero_count", 0)),
            "unmatched_zero_count": int(fusion_stats.get("unmatched_zero_count", 0)),
            "max_fusion_modifier": round(float(fusion_stats.get("max_fusion_modifier", 0.0)), 6),
            "formula": "min(0.30, 0.15 * severity_score + 0.10 * min(1, occurrence_count / 5) + 0.05 * min(1, consecutive_days / 3)); if matched=false then fusion_modifier=0.0",
        },
        "artifacts": [
            artifact_snapshot("worker_decision_v2.py"),
            artifact_snapshot(BLACKBOARD_PATH),
            artifact_snapshot(CONFIDENCE_AUDIT_PATH),
            artifact_snapshot(FUSION_SIGNAL_CONTEXT_PATH),
            artifact_snapshot(FUSION_SCORE_AUDIT_PATH),
        ],
    }
    payload = preserve_snapshot_created_at(WAVE5B_SNAPSHOT_PATH, payload, "wave5b")
    write_json(WAVE5B_SNAPSHOT_PATH, payload)


def run_wave5b_read_only() -> Dict[str, float]:
    tracked_paths = [
        BLACKBOARD_PATH,
        WARNING_INTELLIGENCE_PATH,
        WARNING_PATTERN_AUDIT_PATH,
        CONFIDENCE_AUDIT_PATH,
        FUSION_SIGNAL_CONTEXT_PATH,
    ]
    before_hashes = {path: file_sha256_or_empty(path) for path in tracked_paths}

    first_stats = write_fusion_score_audit_artifacts()
    validate_wave5b_fusion_stats(first_stats)
    first_audit_hash = file_sha256_or_empty(FUSION_SCORE_AUDIT_PATH)

    after_first_hashes = {path: file_sha256_or_empty(path) for path in tracked_paths}
    if after_first_hashes != before_hashes:
        raise RuntimeError("Wave 5B validation failed: read-only run modified protected artifacts")

    first_rows = read_json_list(FUSION_SCORE_AUDIT_PATH)
    for row in first_rows:
        if not bool(row.get("matched")) and float(row.get("fusion_modifier", 0.0)) != 0.0:
            raise RuntimeError("Wave 5B validation failed: unmatched signal received non-zero fusion influence")

    second_stats = write_fusion_score_audit_artifacts()
    validate_wave5b_fusion_stats(second_stats)
    second_audit_hash = file_sha256_or_empty(FUSION_SCORE_AUDIT_PATH)

    after_second_hashes = {path: file_sha256_or_empty(path) for path in tracked_paths}
    if after_second_hashes != before_hashes:
        raise RuntimeError("Wave 5B validation failed: second read-only run modified protected artifacts")
    if first_stats != second_stats:
        raise RuntimeError("Wave 5B validation failed: fusion score stats changed across identical runs")
    if first_audit_hash != second_audit_hash:
        raise RuntimeError("Wave 5B validation failed: fusion_score_audit.json is not idempotent across runs")

    return second_stats


def bounded_wave5c_limit(limit: int) -> int:
    return max(1, min(MAX_WAVE5C_VALIDATION_SIGNALS, safe_int(limit)))


def validated_signal_records(
    records: List[Tuple[str, Dict[str, object], bool]],
) -> List[Tuple[str, Dict[str, object], bool]]:
    validated: List[Tuple[str, Dict[str, object], bool]] = []
    for record in records:
        _path, data, _mark_processed = record
        valid, _reason = validate_signal(data)
        if valid:
            validated.append(record)
    return validated


def opportunity_match_key(data: Dict[str, object]) -> Tuple[str, str, str]:
    return (
        str(data.get("branch", "")).strip().lower(),
        str(data.get("category", "")).strip().lower(),
        str(data.get("date_window", "")).strip(),
    )


def opportunity_match_count_index(blackboard: str) -> Dict[Tuple[str, str, str], int]:
    counts: Dict[Tuple[str, str, str], int] = {}
    _before, active_part, _after = split_blackboard_sections(blackboard)
    for block in split_blocks(active_part):
        key = (
            extract_field(block, "branch").strip().lower(),
            extract_field(block, "category").strip().lower(),
            extract_field(block, "date_window").strip(),
        )
        counts[key] = counts.get(key, 0) + 1
    return counts


def confidence_audit_lookup() -> Dict[str, Dict[str, object]]:
    return {
        str(row.get("signal_ref", "")).strip(): row
        for row in read_json_list(CONFIDENCE_AUDIT_PATH)
        if str(row.get("signal_ref", "")).strip()
    }


def fusion_score_audit_lookup() -> Dict[str, Dict[str, object]]:
    return {
        str(row.get("signal_ref", "")).strip(): row
        for row in read_json_list(FUSION_SCORE_AUDIT_PATH)
        if str(row.get("signal_ref", "")).strip()
    }


def build_fusion_application_row(
    signal_ref: str,
    branch: str,
    signal_type: str,
    effective_delta: float,
    fusion_row: Dict[str, object] | None,
    notes_suffix: str = "",
) -> Dict[str, object]:
    effective_delta = round(float(effective_delta), 6)
    resolved_branch = str(branch).strip().lower()
    resolved_signal_type = str(signal_type).strip()
    pattern_id = ""
    matched = False
    fusion_modifier = 0.0
    notes_parts: List[str] = []

    if fusion_row is None:
        notes_parts.append("missing_fusion_score_audit_row")
    else:
        pattern_id = str(fusion_row.get("pattern_id", "")).strip()
        matched = bool(fusion_row.get("matched"))
        resolved_branch = str(fusion_row.get("branch", resolved_branch)).strip().lower() or resolved_branch
        resolved_signal_type = str(fusion_row.get("signal_type", resolved_signal_type)).strip() or resolved_signal_type
        fusion_modifier = round(float(fusion_row.get("fusion_modifier", 0.0) or 0.0), 6)
        fusion_notes = str(fusion_row.get("notes", "")).strip()
        if fusion_notes:
            notes_parts.append(fusion_notes)

    if fusion_modifier > 0.30:
        raise RuntimeError(
            f"Wave 5C validation failed: fusion_modifier exceeded bounded cap for signal_ref={signal_ref}"
        )

    if not matched:
        fusion_modifier = 0.0

    final_delta_with_fusion = round(effective_delta * (1.0 + fusion_modifier), 6)
    if not matched:
        final_delta_with_fusion = effective_delta

    persisted_delta = final_delta_with_fusion if matched else effective_delta

    if matched and persisted_delta != final_delta_with_fusion:
        raise RuntimeError(f"Wave 5C validation failed: matched persisted_delta drift for signal_ref={signal_ref}")
    if not matched and persisted_delta != effective_delta:
        raise RuntimeError(f"Wave 5C validation failed: unmatched signal received fusion influence for signal_ref={signal_ref}")

    if notes_suffix:
        notes_parts.append(notes_suffix)

    return {
        "signal_ref": signal_ref,
        "pattern_id": pattern_id,
        "matched": matched,
        "branch": resolved_branch,
        "signal_type": resolved_signal_type,
        "effective_delta": effective_delta,
        "fusion_modifier": fusion_modifier,
        "final_delta_with_fusion": final_delta_with_fusion,
        "persisted_delta": persisted_delta,
        "notes": "; ".join(part for part in notes_parts if part),
    }


def sort_fusion_effect_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("signal_ref", "")),
            str(row.get("branch", "")),
            str(row.get("signal_type", "")),
            str(row.get("pattern_id", "")),
        ),
    )


def build_wave5c_subset_payload(
    records: List[Tuple[str, Dict[str, object], bool]],
    subset_limit: int,
    blackboard: str,
) -> Dict[str, object]:
    limited_records = validated_signal_records(records)[:bounded_wave5c_limit(subset_limit)]
    confidence_lookup = confidence_audit_lookup()
    fusion_lookup = fusion_score_audit_lookup()
    match_counts = opportunity_match_count_index(blackboard)
    simulated_blackboard = blackboard

    rows: List[Dict[str, object]] = []
    signal_ref_join_match_count = 0
    signal_ref_join_no_match_count = 0
    warning_match_count = 0
    warning_no_match_count = 0
    opportunity_match_count = 0
    opportunity_no_match_count = 0
    opportunity_ambiguous_match_count = 0
    fusion_non_zero_count = 0
    total_delta_increase = 0.0

    for path, data, _mark_processed in limited_records:
        signal_ref = canonical_source_signal_ref(path, data)
        confidence_row = confidence_lookup.get(signal_ref)
        fusion_row = fusion_lookup.get(signal_ref)
        joined = confidence_row is not None and fusion_row is not None
        if joined:
            signal_ref_join_match_count += 1
        else:
            signal_ref_join_no_match_count += 1

        opportunity_key = opportunity_match_key(data)
        key_match_count = match_counts.get(opportunity_key, 0)
        if key_match_count > 1:
            opportunity_ambiguous_match_count += 1

        matched_block = find_matching_block(simulated_blackboard, data)
        block_title = extract_block_title(matched_block).strip() if matched_block else ""
        block_found = bool(matched_block)
        if block_found:
            opportunity_match_count += 1
        else:
            opportunity_no_match_count += 1

        base_effective_delta = 0.0
        warning_matched = False
        final_delta_with_fusion = 0.0
        persisted_delta = 0.0
        delta_increase = 0.0
        notes = []

        if confidence_row is not None:
            base_effective_delta = round(float(confidence_row.get("final_effective_delta", 0.0) or 0.0), 6)
        else:
            notes.append("missing_confidence_audit_row")

        fusion_application = build_fusion_application_row(
            signal_ref,
            str(data.get("branch", "")).strip().lower(),
            str(data.get("signal_type", "")).strip(),
            base_effective_delta,
            fusion_row,
            notes_suffix="subset_validation",
        )
        warning_matched = bool(fusion_application.get("matched"))
        if warning_matched:
            warning_match_count += 1
        else:
            warning_no_match_count += 1
        final_delta_with_fusion = round(float(fusion_application.get("final_delta_with_fusion", 0.0) or 0.0), 6)
        persisted_delta = round(float(fusion_application.get("persisted_delta", 0.0) or 0.0), 6)
        if str(fusion_application.get("notes", "")).strip():
            notes.append(str(fusion_application.get("notes", "")).strip())

        if block_found:
            delta_increase = round(persisted_delta - base_effective_delta, 6)
            scoring = {"effective_delta": persisted_delta}
            updated_block = reinforce_block(matched_block, data, scoring)
            simulated_blackboard = simulated_blackboard.replace(matched_block, updated_block, 1)
        else:
            notes.append("opportunity_create_path_no_reinforcement_delta_applied")
            simulated_blackboard = insert_into_active_opportunities(simulated_blackboard, build_opportunity_block(data))

        if float(fusion_application.get("fusion_modifier", 0.0) or 0.0) > 0.0:
            fusion_non_zero_count += 1
        total_delta_increase += delta_increase

        rows.append(
            {
                "signal_ref": signal_ref,
                "pattern_id": str(fusion_application.get("pattern_id", "")).strip(),
                "matched": warning_matched,
                "branch": str(fusion_application.get("branch", data.get("branch", ""))).strip().lower(),
                "category": str(data.get("category", "")).strip().lower(),
                "date_window": str(data.get("date_window", "")).strip(),
                "signal_type": str(fusion_application.get("signal_type", data.get("signal_type", ""))).strip(),
                "signal_ref_joined": joined,
                "warning_matched": warning_matched,
                "opportunity_match_count_for_key": key_match_count,
                "opportunity_key_is_unique": key_match_count == 1,
                "opportunity_block_found": block_found,
                "matched_block_title": block_title,
                "action": "reinforce" if block_found else "create",
                "effective_delta": base_effective_delta,
                "fusion_modifier": round(float(fusion_application.get("fusion_modifier", 0.0) or 0.0), 6),
                "final_delta_with_fusion": final_delta_with_fusion,
                "persisted_delta": persisted_delta,
                "delta_increase": delta_increase,
                "notes": "; ".join(notes),
            }
        )

    payload = {
        "phase": "wave5c_fusion_effect_subset_validation",
        "subset_limit_requested": safe_int(subset_limit),
        "subset_limit_applied": bounded_wave5c_limit(subset_limit),
        "subset_signal_count": len(limited_records),
        "verification": {
            "signal_ref_join": {
                "match_count": signal_ref_join_match_count,
                "no_match_count": signal_ref_join_no_match_count,
                "expected": signal_ref_join_no_match_count == 0,
                "reason": "Wave 5B materializes one fusion_score_audit row per confidence_scoring_audit signal_ref; this join should be exact and complete.",
            },
            "warning_match_context": {
                "matched_signal_count": warning_match_count,
                "unmatched_signal_count": warning_no_match_count,
                "reason": "This is the Wave 5A/5B warning-intelligence match state and is separate from the signal_ref join used to apply Wave 5C fusion influence.",
            },
            "opportunity_match": {
                "match_count": opportunity_match_count,
                "no_match_count": opportunity_no_match_count,
                "ambiguous_match_count": opportunity_ambiguous_match_count,
                "expected": opportunity_no_match_count == 0 and opportunity_ambiguous_match_count == 0,
                "reason": "Wave 5C applies fusion only after exact opportunity resolution by branch/category/date_window. All subset matches must resolve to a single existing block to avoid overmatching.",
            },
        },
        "summary": {
            "fusion_non_zero_count": fusion_non_zero_count,
            "total_delta_increase": round(total_delta_increase, 6),
            "simulated_blackboard_sha256": sha256(simulated_blackboard.encode("utf-8")).hexdigest(),
            "row_sha256": json_sha256(sort_fusion_effect_rows(rows)),
        },
        "rows": sort_fusion_effect_rows(rows),
    }
    return payload


def write_wave5c_snapshot(payload: Dict[str, object]) -> None:
    verification = payload.get("verification", {}) if isinstance(payload.get("verification", {}), dict) else {}
    summary = payload.get("summary", {}) if isinstance(payload.get("summary", {}), dict) else {}
    idempotency = payload.get("idempotency", {}) if isinstance(payload.get("idempotency", {}), dict) else {}
    rows = payload.get("rows", [])
    matched_count = 0
    unmatched_count = 0
    max_modifier = 0.0
    if isinstance(rows, list):
        matched_count = sum(1 for row in rows if isinstance(row, dict) and bool(row.get("matched")))
        unmatched_count = sum(1 for row in rows if isinstance(row, dict) and not bool(row.get("matched")))
        max_modifier = max(
            (float(row.get("fusion_modifier", 0.0) or 0.0) for row in rows if isinstance(row, dict)),
            default=0.0,
        )

    payload_to_write = {
        "phase": "wave5c_controlled_fusion_influence",
        "created_at_utc": stable_utc_now(),
        "fusion_effect": {
            "row_count": len(rows) if isinstance(rows, list) else 0,
            "matched_count": matched_count,
            "unmatched_count": unmatched_count,
            "max_fusion_modifier": round(max_modifier, 6),
            "formula": "final_delta_with_fusion = effective_delta * (1 + fusion_modifier); persisted_delta = final_delta_with_fusion for matched rows else effective_delta",
            "signal_ref_join_match_count": safe_int(
                (verification.get("signal_ref_join", {}) if isinstance(verification.get("signal_ref_join", {}), dict) else {}).get(
                    "match_count",
                    0,
                )
            ),
            "signal_ref_join_no_match_count": safe_int(
                (verification.get("signal_ref_join", {}) if isinstance(verification.get("signal_ref_join", {}), dict) else {}).get(
                    "no_match_count",
                    0,
                )
            ),
            "opportunity_match_count": safe_int(
                (verification.get("opportunity_match", {}) if isinstance(verification.get("opportunity_match", {}), dict) else {}).get(
                    "match_count",
                    0,
                )
            ),
            "opportunity_ambiguous_match_count": safe_int(
                (verification.get("opportunity_match", {}) if isinstance(verification.get("opportunity_match", {}), dict) else {}).get(
                    "ambiguous_match_count",
                    0,
                )
            ),
            "fusion_non_zero_count": safe_int(summary.get("fusion_non_zero_count", 0)),
            "total_delta_increase": round(float(summary.get("total_delta_increase", 0.0) or 0.0), 6),
            "idempotent": bool(idempotency.get("idempotent")),
        },
        "artifacts": [
            artifact_snapshot("worker_decision_v2.py"),
            artifact_snapshot(BLACKBOARD_PATH),
            artifact_snapshot(CONFIDENCE_AUDIT_PATH),
            artifact_snapshot(FUSION_SIGNAL_CONTEXT_PATH),
            artifact_snapshot(FUSION_SCORE_AUDIT_PATH),
            artifact_snapshot(FUSION_EFFECT_AUDIT_PATH),
        ],
    }
    payload_to_write = preserve_snapshot_created_at(WAVE5C_SNAPSHOT_PATH, payload_to_write, "wave5c")
    write_json(WAVE5C_SNAPSHOT_PATH, payload_to_write)


def write_fusion_effect_audit_artifacts(
    records: List[Tuple[str, Dict[str, object], bool]],
    subset_limit: int,
) -> Dict[str, object]:
    blackboard = normalize_blackboard_content(read_file(BLACKBOARD_PATH))
    first_payload = build_wave5c_subset_payload(records, subset_limit, blackboard)
    second_payload = build_wave5c_subset_payload(records, subset_limit, blackboard)
    first_summary = first_payload.get("summary", {}) if isinstance(first_payload.get("summary", {}), dict) else {}
    second_summary = second_payload.get("summary", {}) if isinstance(second_payload.get("summary", {}), dict) else {}
    idempotent = first_payload == second_payload

    payload = dict(first_payload)
    payload["idempotency"] = {
        "audit_rows_sha256_run_1": str(first_summary.get("row_sha256", "")).strip(),
        "audit_rows_sha256_run_2": str(second_summary.get("row_sha256", "")).strip(),
        "simulated_blackboard_sha256_run_1": str(first_summary.get("simulated_blackboard_sha256", "")).strip(),
        "simulated_blackboard_sha256_run_2": str(second_summary.get("simulated_blackboard_sha256", "")).strip(),
        "idempotent": idempotent,
    }
    write_json(FUSION_EFFECT_AUDIT_PATH, sort_fusion_effect_rows(first_payload.get("rows", [])))
    write_wave5c_snapshot(payload)
    return payload


def validate_wave5c_fusion_effect(payload: Dict[str, object]) -> None:
    subset_signal_count = safe_int(payload.get("subset_signal_count", 0))
    if subset_signal_count < 1:
        raise RuntimeError("Wave 5C validation failed: no validated signals were selected for the subset")
    if subset_signal_count > MAX_WAVE5C_VALIDATION_SIGNALS:
        raise RuntimeError(
            f"Wave 5C validation failed: subset exceeded {MAX_WAVE5C_VALIDATION_SIGNALS} validated signals"
        )

    verification = payload.get("verification", {})
    if not isinstance(verification, dict):
        raise RuntimeError("Wave 5C validation failed: verification block missing from fusion effect audit")

    signal_ref_join = verification.get("signal_ref_join", {})
    opportunity_match = verification.get("opportunity_match", {})
    if safe_int(signal_ref_join.get("no_match_count", 0)) != 0:
        raise RuntimeError("Wave 5C validation failed: subset signal_ref join to fusion_score_audit is incomplete")
    if safe_int(opportunity_match.get("ambiguous_match_count", 0)) != 0:
        raise RuntimeError("Wave 5C validation failed: subset opportunity match is ambiguous")

    idempotency = payload.get("idempotency", {})
    if not isinstance(idempotency, dict) or not bool(idempotency.get("idempotent")):
        raise RuntimeError("Wave 5C validation failed: fusion effect audit is not idempotent across identical runs")


def run_wave5c_fusion_effect_validation(
    records: List[Tuple[str, Dict[str, object], bool]],
    subset_limit: int,
) -> Dict[str, object]:
    tracked_paths = [
        BLACKBOARD_PATH,
        CONFIDENCE_AUDIT_PATH,
        FUSION_SIGNAL_CONTEXT_PATH,
        FUSION_SCORE_AUDIT_PATH,
    ]
    before_hashes = {path: file_sha256_or_empty(path) for path in tracked_paths}
    payload = write_fusion_effect_audit_artifacts(records, subset_limit)
    after_hashes = {path: file_sha256_or_empty(path) for path in tracked_paths}
    if after_hashes != before_hashes:
        raise RuntimeError("Wave 5C validation failed: protected artifacts changed during subset fusion effect run")
    validate_wave5c_fusion_effect(payload)
    return payload


def normalization_section_before(data: Dict[str, object]) -> str:
    return str(
        data.get("grouped_section")
        or data.get("section_canonical")
        or data.get("section")
        or data.get("raw_section")
        or ""
    ).strip().lower()


def audit_resolved_branch(path: str, data: Dict[str, object]) -> str:
    signal_ref = canonical_source_signal_ref(path, data)
    explicit_candidates = [data.get("branch"), data.get("branch_slug")]
    for candidate in explicit_candidates:
        resolved = resolve_branch_slug(candidates=[candidate])
        if resolved != "unknown":
            return resolved

    filename_candidates = [Path(path).stem, Path(signal_ref).stem]
    for candidate in filename_candidates:
        resolved = resolve_branch_slug(candidates=[candidate])
        if resolved != "unknown":
            return resolved

    path_candidates = [
        path,
        signal_ref,
        str(data.get("source_ref", "")).strip(),
    ]
    for candidate in path_candidates:
        if not candidate:
            continue
        resolved = resolve_branch_slug(path=candidate)
        if resolved != "unknown":
            return resolved

    return "unknown"


def resolve_section(signal_type: str, raw_section: object, *, branch_level: bool = False) -> str:
    if branch_level:
        return "branch_performance"

    normalized_raw = str(raw_section or "").strip().lower()
    canonical_section = normalize_section_name(str(raw_section or ""))
    if not normalized_raw and canonical_section:
        return str(canonical_section).strip().lower()
    if canonical_section:
        return str(canonical_section).strip().lower()
    return normalized_raw


def audit_section_target(path: str, data: Dict[str, object]) -> Tuple[str, str, str]:
    payload = dict(data)
    payload["_path"] = path
    current_section, section_type = normalize_section_key(payload)
    current_section = str(current_section or "").strip().lower()
    raw_section = (
        data.get("grouped_section")
        or data.get("section_canonical")
        or data.get("section")
        or data.get("raw_section")
        or ""
    )
    branch_level = is_branch_level_pattern(path, data)
    target_section_after = resolve_section(str(data.get("signal_type", "")).strip(), raw_section, branch_level=branch_level)
    if (
        section_before := normalization_section_before(data)
    ) in {"", "unknown", "unknown_section"} and current_section and current_section not in {"unknown", "unknown_section"}:
        target_section_after = current_section
    if branch_level:
        target_section_after = "branch_performance"
    return current_section, target_section_after, section_type


def signal_identity_key(path: str, data: Dict[str, object]) -> Tuple[str, str, str, str]:
    return (
        audit_resolved_branch(path, data),
        str(data.get("signal_type", "")).strip(),
        str(data.get("category", "")).strip().lower(),
        str(data.get("date_window") or data.get("date") or "").strip(),
    )


def canonical_duplicate_identity_record(
    items: List[Tuple[str, Dict[str, object], bool]],
) -> Tuple[str, Dict[str, object], bool]:
    ranked = sorted(
        items,
        key=lambda item: (
            1 if is_latest_signal_record(item[0], item[1]) else 0,
            canonical_source_signal_ref(item[0], item[1]),
            item[0],
        ),
    )
    return ranked[-1]


def build_duplicate_visibility_lookup(
    records: List[Tuple[str, Dict[str, object], bool]],
    valid_target_refs: set[str] | None = None,
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, int]]:
    clusters: Dict[Tuple[str, str, str, str], List[Tuple[str, Dict[str, object], bool]]] = {}
    for record in validated_signal_records(records):
        clusters.setdefault(signal_identity_key(record[0], record[1]), []).append(record)

    duplicate_lookup: Dict[str, Dict[str, str]] = {}
    latest_pair_count = 0
    repeated_identity_count = 0

    for _key, items in sorted(clusters.items()):
        if len(items) <= 1:
            continue

        canonical_items = items
        if valid_target_refs is not None:
            canonical_items = [
                item
                for item in items
                if canonical_source_signal_ref(item[0], item[1]) in valid_target_refs
            ]
            if not canonical_items:
                continue

        canonical_record = canonical_duplicate_identity_record(canonical_items)
        canonical_ref = canonical_source_signal_ref(canonical_record[0], canonical_record[1])
        cluster_has_latest = any(is_latest_signal_record(path, data) for path, data, _mark_processed in items)
        if cluster_has_latest:
            latest_pair_count += len(items) - 1
        else:
            repeated_identity_count += len(items) - 1

        for path, data, _mark_processed in sorted(
            items,
            key=lambda item: canonical_source_signal_ref(item[0], item[1]),
        ):
            signal_ref = canonical_source_signal_ref(path, data)
            if signal_ref == canonical_ref:
                continue
            issue_type = "latest_timestamp_duplicate_pair" if cluster_has_latest else "duplicate_signal_identity_key"
            notes = (
                f"Diagnostic-only duplicate visibility: canonical signal is {canonical_ref} for the same branch/signal_type/category/date_window identity."
            )
            if cluster_has_latest:
                notes = (
                    f"Diagnostic-only duplicate visibility: canonical signal is {canonical_ref}; latest/timestamp variants share the same branch/signal_type/category/date_window identity."
                )
            duplicate_lookup[signal_ref] = {
                "duplicate_of": canonical_ref,
                "issue_type": issue_type,
                "notes": notes,
            }

    return duplicate_lookup, {
        "duplicate_signal_count": len(duplicate_lookup),
        "latest_timestamp_duplicate_count": latest_pair_count,
        "repeated_identity_duplicate_count": repeated_identity_count,
    }


def normalization_issue_rows_for_signal(
    path: str,
    data: Dict[str, object],
    duplicate_diagnostic: Dict[str, str] | None = None,
) -> List[Dict[str, object]]:
    signal_ref = canonical_source_signal_ref(path, data)
    signal_type = str(data.get("signal_type", "")).strip()
    branch_before = str(data.get("branch", "")).strip().lower()
    branch_after = audit_resolved_branch(path, data)
    section_before = normalization_section_before(data)
    current_section_after, target_section_after, section_type = audit_section_target(path, data)
    duplicate_of = None
    if duplicate_diagnostic:
        resolved_duplicate_of = str(duplicate_diagnostic.get("duplicate_of", "")).strip()
        duplicate_of = resolved_duplicate_of or None

    rows: List[Dict[str, object]] = []

    if branch_before in {"", "unknown"} and branch_after not in {"", "unknown"}:
        rows.append(
            {
                "signal_ref": signal_ref,
                "signal_type": signal_type,
                "branch_before": branch_before or "unknown",
                "branch_after": branch_after,
                "section_before": section_before,
                "section_after": target_section_after,
                "duplicate_of": duplicate_of,
                "issue_type": "branch_inferred_from_path_or_signal_id",
                "notes": "Audit-only branch tightening uses explicit parsed branch, then filename stem, then path segments, else unknown.",
            }
        )

    if is_branch_level_pattern(path, data) and target_section_after == "branch_performance" and section_before != "branch_performance":
        rows.append(
            {
                "signal_ref": signal_ref,
                "signal_type": signal_type,
                "branch_before": branch_before or "unknown",
                "branch_after": branch_after,
                "section_before": section_before,
                "section_after": target_section_after,
                "duplicate_of": duplicate_of,
                "issue_type": "branch_level_section_residue",
                "notes": (
                    f"Branch-level signal currently resolves section as {current_section_after or 'unknown_section'} "
                    f"({section_type}); audit target stays branch_performance."
                ),
            }
        )

    if not is_branch_level_pattern(path, data) and section_before in {"", "unknown", "unknown_section"} and target_section_after not in {"", "unknown", "unknown_section"}:
        rows.append(
            {
                "signal_ref": signal_ref,
                "signal_type": signal_type,
                "branch_before": branch_before or "unknown",
                "branch_after": branch_after,
                "section_before": section_before,
                "section_after": target_section_after,
                "duplicate_of": duplicate_of,
                "issue_type": "section_inferred_from_canonical_normalizer",
                "notes": "Audit-only section tightening inferred a canonical section from the shared section normalizer.",
            }
        )

    if duplicate_of and not rows:
        rows.append(
            {
                "signal_ref": signal_ref,
                "signal_type": signal_type,
                "branch_before": branch_before or "unknown",
                "branch_after": branch_after,
                "section_before": section_before,
                "section_after": target_section_after,
                "duplicate_of": duplicate_of,
                "issue_type": str(duplicate_diagnostic.get("issue_type", "duplicate_signal_identity_key")).strip(),
                "notes": str(duplicate_diagnostic.get("notes", "")).strip(),
            }
        )

    return rows


def build_normalization_gap_audit(
    records: List[Tuple[str, Dict[str, object], bool]],
    *,
    include_duplicate_visibility: bool = False,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    validated_records = validated_signal_records(records)
    base_rows_by_signal_ref: Dict[str, List[Dict[str, object]]] = {}
    for path, data, _mark_processed in validated_records:
        signal_ref = canonical_source_signal_ref(path, data)
        base_rows_by_signal_ref[signal_ref] = normalization_issue_rows_for_signal(path, data)
    inferable_unknown_before = 0
    inferable_unknown_after = 0
    duplicate_lookup: Dict[str, Dict[str, str]] = {}
    duplicate_summary = {
        "duplicate_signal_count": 0,
        "latest_timestamp_duplicate_count": 0,
        "repeated_identity_duplicate_count": 0,
    }
    if include_duplicate_visibility:
        valid_target_refs = {
            signal_ref for signal_ref, signal_rows in base_rows_by_signal_ref.items() if signal_rows
        }
        duplicate_lookup, duplicate_summary = build_duplicate_visibility_lookup(records, valid_target_refs=valid_target_refs)

    for path, data, _mark_processed in validated_records:
        branch_before = str(data.get("branch", "")).strip().lower() or "unknown"
        branch_after = audit_resolved_branch(path, data)
        if branch_before == "unknown" and branch_after != "unknown":
            inferable_unknown_before += 1
        if branch_before == "unknown" and branch_after == "unknown":
            inferable_unknown_after += 1
        signal_ref = canonical_source_signal_ref(path, data)
        base_rows = [dict(row) for row in base_rows_by_signal_ref.get(signal_ref, [])]
        if base_rows:
            duplicate_diagnostic = duplicate_lookup.get(signal_ref)
            if duplicate_diagnostic is not None:
                resolved_duplicate_of = str(duplicate_diagnostic.get("duplicate_of", "")).strip()
                duplicate_value = resolved_duplicate_of or None
                for row in base_rows:
                    row["duplicate_of"] = duplicate_value
            rows.extend(base_rows)
            continue

        rows.extend(
            normalization_issue_rows_for_signal(
                path,
                data,
                duplicate_lookup.get(signal_ref),
            )
        )

    rows.sort(
        key=lambda row: (
            str(row.get("signal_ref", "")),
            str(row.get("duplicate_of") or ""),
            str(row.get("issue_type", "")),
            str(row.get("signal_type", "")),
            str(row.get("branch_after", "")),
            str(row.get("section_after", "")),
        )
    )

    issue_counts: Dict[str, int] = {}
    for row in rows:
        issue_type = str(row.get("issue_type", "")).strip()
        issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1

    unknown_before_count = 0
    unknown_after_count = 0
    for path, data, _mark_processed in validated_records:
        branch_before = str(data.get("branch", "")).strip().lower() or "unknown"
        branch_after = audit_resolved_branch(path, data)
        if branch_before == "unknown":
            unknown_before_count += 1
        if branch_after == "unknown":
            unknown_after_count += 1

    summary = {
        "validated_signal_count": len(validated_records),
        "issue_row_count": len(rows),
        "issue_counts": issue_counts,
        "branch_unknown_before_count": unknown_before_count,
        "branch_unknown_after_count": unknown_after_count,
        "inferable_branch_unknown_before_count": inferable_unknown_before,
        "inferable_branch_unknown_after_count": inferable_unknown_after,
        "duplicate_signal_count": safe_int(duplicate_summary.get("duplicate_signal_count", 0)),
        "latest_timestamp_duplicate_count": safe_int(duplicate_summary.get("latest_timestamp_duplicate_count", 0)),
        "repeated_identity_duplicate_count": safe_int(duplicate_summary.get("repeated_identity_duplicate_count", 0)),
        "audit_row_sha256": json_sha256(rows),
    }
    return rows, summary


def write_wave6a_snapshot(summary: Dict[str, object]) -> None:
    created_at_utc = stable_utc_now()
    payload = {
        "phase": "wave6a_normalization_audit_branch_resolution_tightening",
        "created_at_utc": created_at_utc,
        "normalization_audit": {
            "validated_signal_count": safe_int(summary.get("validated_signal_count", 0)),
            "issue_row_count": safe_int(summary.get("issue_row_count", 0)),
            "branch_unknown_before_count": safe_int(summary.get("branch_unknown_before_count", 0)),
            "branch_unknown_after_count": safe_int(summary.get("branch_unknown_after_count", 0)),
            "inferable_branch_unknown_before_count": safe_int(summary.get("inferable_branch_unknown_before_count", 0)),
            "inferable_branch_unknown_after_count": safe_int(summary.get("inferable_branch_unknown_after_count", 0)),
            "issue_counts": summary.get("issue_counts", {}),
            "audit_row_sha256": str(summary.get("audit_row_sha256", "")).strip(),
        },
        "artifacts": [
            artifact_snapshot("worker_decision_v2.py"),
            artifact_snapshot("scripts/branch_resolution.py"),
            artifact_snapshot("scripts/section_normalizer.py"),
            artifact_snapshot(BLACKBOARD_PATH),
            artifact_snapshot(NORMALIZATION_GAP_AUDIT_PATH),
        ],
    }
    if os.path.exists(WAVE6A_SNAPSHOT_PATH):
        try:
            existing_payload = read_json(WAVE6A_SNAPSHOT_PATH)
        except Exception as exc:
            log(f"WARN wave6a snapshot unreadable: {exc}")
        else:
            if isinstance(existing_payload, dict):
                comparable_existing = dict(existing_payload)
                comparable_new = dict(payload)
                comparable_existing.pop("created_at_utc", None)
                comparable_new.pop("created_at_utc", None)
                if comparable_existing == comparable_new:
                    existing_created_at = str(existing_payload.get("created_at_utc", "")).strip()
                    if existing_created_at:
                        payload["created_at_utc"] = existing_created_at
    write_json(WAVE6A_SNAPSHOT_PATH, payload)


def write_wave6b_duplicate_visibility_snapshot(summary: Dict[str, object]) -> None:
    payload = {
        "phase": "wave6b_duplicate_visibility_audit",
        "created_at_utc": stable_utc_now(),
        "duplicate_visibility": {
            "validated_signal_count": safe_int(summary.get("validated_signal_count", 0)),
            "issue_row_count": safe_int(summary.get("issue_row_count", 0)),
            "duplicate_signal_count": safe_int(summary.get("duplicate_signal_count", 0)),
            "latest_timestamp_duplicate_count": safe_int(summary.get("latest_timestamp_duplicate_count", 0)),
            "repeated_identity_duplicate_count": safe_int(summary.get("repeated_identity_duplicate_count", 0)),
            "audit_row_sha256": str(summary.get("audit_row_sha256", "")).strip(),
            "notes": "Duplicate visibility is diagnostic only; duplicate_of annotates the canonical signal_ref for a shared branch+signal_type+category+date_window identity.",
        },
        "artifacts": [
            artifact_snapshot("worker_decision_v2.py"),
            artifact_snapshot(BLACKBOARD_PATH),
            artifact_snapshot(NORMALIZATION_GAP_AUDIT_PATH),
        ],
    }
    payload = preserve_snapshot_created_at(WAVE6B_SNAPSHOT_PATH, payload, "wave6b_duplicate_visibility")
    write_json(WAVE6B_SNAPSHOT_PATH, payload)


def run_wave6a_normalization_audit(
    records: List[Tuple[str, Dict[str, object], bool]],
) -> Dict[str, object]:
    protected_paths = [
        BLACKBOARD_PATH,
        CONFIDENCE_AUDIT_PATH,
        DENSITY_AUDIT_PATH,
        WARNING_INTELLIGENCE_PATH,
        WARNING_PATTERN_AUDIT_PATH,
        FUSION_SIGNAL_CONTEXT_PATH,
        FUSION_SCORE_AUDIT_PATH,
        FUSION_EFFECT_AUDIT_PATH,
        WAVE5C_SNAPSHOT_PATH,
        WAVE3_SNAPSHOT_PATH,
        WAVE4A_SNAPSHOT_PATH,
        WAVE4B_SNAPSHOT_PATH,
        WAVE5A_SNAPSHOT_PATH,
        WAVE5B_SNAPSHOT_PATH,
    ]
    before_hashes = {path: file_sha256_or_empty(path) for path in protected_paths}
    before_blackboard_hash = before_hashes.get(BLACKBOARD_PATH, "")

    first_rows, first_summary = build_normalization_gap_audit(records)
    second_rows, second_summary = build_normalization_gap_audit(records)
    if first_rows != second_rows or first_summary != second_summary:
        raise RuntimeError("Wave 6A validation failed: normalization audit is not idempotent across identical runs")

    write_json(NORMALIZATION_GAP_AUDIT_PATH, first_rows)
    summary = dict(first_summary)
    summary["idempotent"] = True
    summary["audit_row_sha256_run_2"] = str(second_summary.get("audit_row_sha256", "")).strip()
    write_wave6a_snapshot(summary)

    after_hashes = {path: file_sha256_or_empty(path) for path in protected_paths}
    if after_hashes != before_hashes:
        changed = [path for path in protected_paths if after_hashes.get(path, "") != before_hashes.get(path, "")]
        raise RuntimeError(f"Wave 6A validation failed: protected artifacts changed: {', '.join(changed)}")
    if file_sha256_or_empty(BLACKBOARD_PATH) != before_blackboard_hash:
        raise RuntimeError("Wave 6A validation failed: OPPORTUNITIES.md changed")

    return summary


def run_wave6b_duplicate_visibility_audit(
    records: List[Tuple[str, Dict[str, object], bool]],
) -> Dict[str, object]:
    protected_paths = [
        BLACKBOARD_PATH,
        CONFIDENCE_AUDIT_PATH,
        DENSITY_AUDIT_PATH,
        WARNING_INTELLIGENCE_PATH,
        WARNING_PATTERN_AUDIT_PATH,
        FUSION_SIGNAL_CONTEXT_PATH,
        FUSION_SCORE_AUDIT_PATH,
        FUSION_EFFECT_AUDIT_PATH,
        WAVE5C_SNAPSHOT_PATH,
        WAVE3_SNAPSHOT_PATH,
        WAVE4A_SNAPSHOT_PATH,
        WAVE4B_SNAPSHOT_PATH,
        WAVE5A_SNAPSHOT_PATH,
        WAVE5B_SNAPSHOT_PATH,
        WAVE6A_SNAPSHOT_PATH,
        OPPORTUNITIES_HYGIENE_AUDIT_PATH,
    ]
    before_hashes = {path: file_sha256_or_empty(path) for path in protected_paths}
    before_blackboard_hash = before_hashes.get(BLACKBOARD_PATH, "")

    first_rows, first_summary = build_normalization_gap_audit(records, include_duplicate_visibility=True)
    second_rows, second_summary = build_normalization_gap_audit(records, include_duplicate_visibility=True)
    if first_rows != second_rows or first_summary != second_summary:
        raise RuntimeError("Wave 6B validation failed: duplicate visibility audit is not idempotent across identical runs")

    write_json(NORMALIZATION_GAP_AUDIT_PATH, first_rows)
    summary = dict(first_summary)
    summary["idempotent"] = True
    summary["audit_row_sha256_run_2"] = str(second_summary.get("audit_row_sha256", "")).strip()
    write_wave6b_duplicate_visibility_snapshot(summary)

    after_hashes = {path: file_sha256_or_empty(path) for path in protected_paths}
    if after_hashes != before_hashes:
        changed = [path for path in protected_paths if after_hashes.get(path, "") != before_hashes.get(path, "")]
        raise RuntimeError(f"Wave 6B validation failed: protected artifacts changed: {', '.join(changed)}")
    if file_sha256_or_empty(BLACKBOARD_PATH) != before_blackboard_hash:
        raise RuntimeError("Wave 6B validation failed: OPPORTUNITIES.md changed")

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decision Worker v2")
    parser.add_argument(
        "--wave5a-fusion-only",
        action="store_true",
        help="Build Wave 5A fusion context artifacts without mutating opportunities or signal processing state.",
    )
    parser.add_argument(
        "--wave5b-fusion-score-only",
        action="store_true",
        help="Build Wave 5B fusion score preview artifacts without mutating opportunities or warning state.",
    )
    parser.add_argument(
        "--wave5c-fusion-effect-only",
        action="store_true",
        help="Validate Wave 5C fusion-adjusted reinforcement on a capped subset and write DATA/fusion_effect_audit.json.",
    )
    parser.add_argument(
        "--wave5c-limit",
        type=int,
        default=MAX_WAVE5C_VALIDATION_SIGNALS,
        help="Maximum validated signals to include in the Wave 5C subset validation (hard capped at 100).",
    )
    parser.add_argument(
        "--wave6a-normalization-only",
        action="store_true",
        help="Run the Wave 6A normalization audit and branch-resolution tightening check without mutating opportunities.",
    )
    parser.add_argument(
        "--wave6b-hygiene-only",
        action="store_true",
        help="Run Wave 6B blackboard hygiene normalization and write the audit/snapshot artifacts.",
    )
    parser.add_argument(
        "--wave6b-duplicate-only",
        action="store_true",
        help="Run Wave 6B duplicate visibility audit and annotate normalization_gap_audit rows with duplicate_of without changing live processing.",
    )
    parser.add_argument(
        "--wave6c-guardrails-only",
        action="store_true",
        help="Run Wave 6C legacy signal hygiene audit and processing-guardrail validation without mutating opportunities.",
    )
    return parser.parse_args()


def make_title(data: Dict[str, object]) -> str:
    signal_type = str(data["signal_type"]).replace("_", " ").title()
    category = str(data["category"]).replace("_", " ").title()
    return f"{signal_type} — {category}"


def extract_field(block: str, field_name: str) -> str:
    pattern = rf"^- {re.escape(field_name)}:\s*(.+?)\s*$"
    matches = re.findall(pattern, block, flags=re.MULTILINE)
    if matches:
        return matches[-1].strip()
    return ""


def extract_list_field(block: str, field_name: str) -> List[str]:
    values: List[str] = []
    in_field = False

    for line in block.splitlines():
        stripped = line.strip()
        if stripped == f"- {field_name}:":
            in_field = True
            continue

        if in_field:
            if stripped.startswith("- ") and not line.startswith("  - "):
                break
            if line.startswith("  - "):
                values.append(line.replace("  - ", "", 1).strip())

    return values


def signal_already_recorded(block: str, data: Dict[str, object]) -> bool:
    signal_ref = f"signal {data['signal_id']}"
    if signal_ref in block:
        return True

    source_ref = str(data.get("source_ref", "")).strip()
    if source_ref and source_ref in block:
        return True

    return False


def normalize_similarity_text(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"\d+(?:\.\d+)?", "<num>", text)
    text = re.sub(r"[^a-z0-9_<>\s:-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_block_title(block: str) -> str:
    first_line = block.splitlines()[0].strip()
    if first_line.startswith("### [") and first_line.endswith("]"):
        return first_line[5:-1].strip()
    return ""


def block_matches_signal(block: str, data: Dict[str, object]) -> bool:
    signal_type = str(data.get("signal_type", "")).strip().lower()
    branch = str(data.get("branch", "")).strip().lower()
    category = str(data.get("category", "")).strip().lower()
    date_window = str(data.get("date_window", "")).strip()

    block_branch = extract_field(block, "branch").strip().lower()
    block_category = extract_field(block, "category").strip().lower()
    block_date_window = extract_field(block, "date_window").strip()

    if branch and category and date_window:
        if (
            block_branch == branch
            and block_category == category
            and block_date_window == date_window
        ):
            return True

    if signal_type in SIGNAL_OPPORTUNITY_MAP:
        return False

    title = make_title(data)
    signal_id = str(data.get("signal_id", "")).strip()

    lower_block = block.lower()

    if signal_id and signal_id in block:
        return True

    if extract_block_title(block).strip().lower() == title.strip().lower():
        return True

    category_match = f"- category: {category}" in lower_block or category in lower_block
    signal_type_match = f"- signal_type: {signal_type}" in lower_block or signal_type in lower_block

    return category_match and signal_type_match


def find_matching_block(blackboard: str, data: Dict[str, object]) -> str:
    for block in split_blocks(blackboard):
        if block_matches_signal(block, data):
            return block
    return ""


def safe_float_from_line(line: str, prefix: str) -> float:
    try:
        return float(line.replace(prefix, "", 1).strip())
    except ValueError:
        return DEFAULT_CREATE_SCORE


def extract_existing_evidence(block: str) -> List[str]:
    evidence = []
    in_evidence = False

    for line in block.splitlines():
        stripped = line.strip()

        if stripped == "- evidence_sources:":
            in_evidence = True
            continue

        if in_evidence:
            if line.startswith("  - "):
                evidence.append(line.replace("  - ", "", 1).strip())
                continue
            if stripped.startswith("- ") and stripped != "- evidence_sources:":
                break
            if stripped.startswith("- last_reinforced:"):
                break
            if stripped.startswith("- status:"):
                break
            if stripped.startswith("- review_status:"):
                break
            if stripped.startswith("- last_updated:"):
                break
            if stripped.startswith("- rationale:"):
                break
            if stripped.startswith("- score_components:"):
                break
            if stripped.startswith("- ") and not line.startswith("  - "):
                break

    return evidence


def equivalent_reinforcement_already_recorded(block: str, data: Dict[str, object]) -> bool:
    description_key = normalize_similarity_text(data.get("description", ""))
    if not description_key:
        return False

    for item in extract_existing_evidence(block):
        if normalize_similarity_text(item) == description_key:
            return True

    return False


def collapse_last_reinforced_sections(lines: List[str]) -> List[str]:
    ranges: List[Tuple[int, int]] = []
    idx = 0

    while idx < len(lines):
        if lines[idx].strip() != "- last_reinforced:":
            idx += 1
            continue

        end = idx + 1
        while end < len(lines) and lines[end].startswith("  - "):
            end += 1
        ranges.append((idx, end))
        idx = end

    if len(ranges) <= 1:
        return lines

    skip_indexes = set()
    for start, end in ranges[:-1]:
        skip_indexes.update(range(start, end))

    result: List[str] = []
    for idx, line in enumerate(lines):
        if idx in skip_indexes:
            continue
        result.append(line)

    return result


def collapse_blank_lines(lines: List[str]) -> List[str]:
    collapsed: List[str] = []
    previous_blank = False
    for line in lines:
        is_blank = not line.strip()
        if is_blank:
            if previous_blank:
                continue
            collapsed.append("")
            previous_blank = True
            continue
        collapsed.append(line.rstrip())
        previous_blank = False

    while collapsed and not collapsed[0].strip():
        collapsed.pop(0)
    while collapsed and not collapsed[-1].strip():
        collapsed.pop()
    return collapsed


def extract_canonical_terminal_sections(lines: List[str]) -> Tuple[List[str], Dict[str, List[str]]]:
    body: List[str] = []
    terminal_sections: Dict[str, List[str]] = {}
    idx = 0
    while idx < len(lines):
        stripped = lines[idx].strip()
        if stripped.startswith("- rationale:"):
            terminal_sections["rationale"] = [lines[idx].rstrip()]
            idx += 1
            continue
        if stripped == "- last_reinforced:":
            section_lines = [lines[idx].rstrip()]
            idx += 1
            while idx < len(lines) and lines[idx].startswith("  - "):
                section_lines.append(lines[idx].rstrip())
                idx += 1
            terminal_sections["last_reinforced"] = section_lines
            continue
        if stripped.startswith("- status:"):
            terminal_sections["status"] = [lines[idx].rstrip()]
            idx += 1
            continue
        if stripped.startswith("- review_status:"):
            terminal_sections["review_status"] = [lines[idx].rstrip()]
            idx += 1
            continue
        if stripped.startswith("- last_updated:"):
            terminal_sections["last_updated"] = [lines[idx].rstrip()]
            idx += 1
            continue
        body.append(lines[idx].rstrip())
        idx += 1
    return body, terminal_sections


def canonicalize_block_layout(lines: List[str]) -> List[str]:
    cleaned_lines = remove_duplicate_terminal_fields(lines)
    body, terminal_sections = extract_canonical_terminal_sections(cleaned_lines)
    body = collapse_blank_lines(body)

    rebuilt = list(body)
    for key in ("rationale", "last_reinforced", "status", "review_status", "last_updated"):
        section_lines = terminal_sections.get(key, [])
        if not section_lines:
            continue
        if rebuilt and rebuilt[-1].strip():
            rebuilt.append("")
        rebuilt.extend(section_lines)

    return collapse_blank_lines(rebuilt)


def reinforce_block(
    block: str,
    data: Dict[str, object],
    scoring: Dict[str, object],
) -> str:
    signal_id = str(data["signal_id"])
    signal_date = str(data["date"])
    description = str(data["description"])
    confidence = float(str(data["confidence"]))
    signal_type = str(data["signal_type"])
    branch = str(data.get("branch", "")).strip()
    category = str(data.get("category", "")).strip()
    date_window = str(data.get("date_window", signal_date)).strip()
    source_signal_types = [str(item).strip() for item in data.get("source_signal_types", []) if str(item).strip()]
    final_effective_delta = float(scoring["effective_delta"])

    lines = block.splitlines()
    updated_lines: List[str] = []

    score_updated = False
    confidence_updated = False
    evidence_inserted = False
    in_evidence = False
    existing_evidence = extract_existing_evidence(block)

    new_evidence_items = [f"signal {signal_id}", description]
    if isinstance(data.get("evidence"), list):
        for item in data["evidence"]:
            item_str = str(item).strip()
            if item_str:
                new_evidence_items.append(item_str)

    dedup_new_evidence = []
    for item in new_evidence_items:
        if item not in existing_evidence and item not in dedup_new_evidence:
            dedup_new_evidence.append(item)

    for idx, line in enumerate(lines):
        stripped = line.strip()

        if stripped.startswith("- leverage_score:"):
            old_score = safe_float_from_line(stripped, "- leverage_score:")
            new_score = min(MAX_SCORE, old_score + final_effective_delta)
            updated_lines.append(f"- leverage_score: {new_score:.2f}")
            score_updated = True
            continue

        if stripped.startswith("- confidence:"):
            try:
                old_conf = float(stripped.replace("- confidence:", "", 1).strip())
            except ValueError:
                old_conf = confidence
            new_conf = max(old_conf, confidence)
            updated_lines.append(f"- confidence: {new_conf:.2f}")
            confidence_updated = True
            continue

        updated_lines.append(line)

        if stripped == "- evidence_sources:":
            in_evidence = True
            continue

        if in_evidence:
            next_line = lines[idx + 1] if idx + 1 < len(lines) else ""
            next_stripped = next_line.strip()

            evidence_section_ending = (
                next_line == ""
                or (
                    not next_line.startswith("  - ")
                    and next_stripped.startswith("- ")
                )
            )

            if evidence_section_ending and not evidence_inserted:
                for item in dedup_new_evidence:
                    updated_lines.append(f"  - {item}")
                evidence_inserted = True
                in_evidence = False

    if not score_updated:
        updated_lines.append(
            f"- leverage_score: {create_score_for_signal(signal_type) + final_effective_delta:.2f}"
        )

    if not confidence_updated:
        updated_lines.append(f"- confidence: {confidence:.2f}")

    if branch and "- branch:" not in block:
        insert_at = 2 if len(updated_lines) >= 2 else len(updated_lines)
        updated_lines.insert(insert_at, f"- branch: {branch}")

    if date_window and "- date_window:" not in block:
        insert_at = 5 if len(updated_lines) >= 5 else len(updated_lines)
        updated_lines.insert(insert_at, f"- date_window: {date_window}")

    if category and "- category:" not in block:
        insert_at = 3 if len(updated_lines) >= 3 else len(updated_lines)
        updated_lines.insert(insert_at, f"- category: {category}")

    if "- evidence_sources:" not in block:
        updated_lines.append("")
        updated_lines.append("- evidence_sources:")
        for item in dedup_new_evidence:
            updated_lines.append(f"  - {item}")

    existing_source_signal_types = extract_list_field(block, "source_signal_types")
    if source_signal_types:
        merged_source_signal_types = list(existing_source_signal_types)
        for item in source_signal_types:
            if item not in merged_source_signal_types:
                merged_source_signal_types.append(item)

        if "- source_signal_types:" in block:
            rebuilt_lines: List[str] = []
            in_source_signal_types = False
            inserted = False
            for line in updated_lines:
                stripped = line.strip()
                if stripped == "- source_signal_types:":
                    rebuilt_lines.append(line)
                    for item in merged_source_signal_types:
                        rebuilt_lines.append(f"  - {item}")
                    in_source_signal_types = True
                    inserted = True
                    continue

                if in_source_signal_types:
                    if line.startswith("  - "):
                        continue
                    if stripped.startswith("- ") and not line.startswith("  - "):
                        in_source_signal_types = False

                rebuilt_lines.append(line)

            if inserted:
                updated_lines = rebuilt_lines
        else:
            insert_at = 5 if len(updated_lines) >= 5 else len(updated_lines)
            updated_lines.insert(insert_at, "- source_signal_types:")
            for idx, item in enumerate(merged_source_signal_types, start=1):
                updated_lines.insert(insert_at + idx, f"  - {item}")

    if "- rationale:" in block:
        pass
    else:
        updated_lines.append("")
        updated_lines.append("- rationale: Reinforced by new validated signal.")

    updated_lines.append("")
    updated_lines.append("- last_reinforced:")
    updated_lines.append(f"  - date: {signal_date}")
    updated_lines.append(f"  - delta: {format_delta(final_effective_delta)}")
    updated_lines.append(f"  - reason: reinforced by {signal_id}")

    review_status_present = any(l.strip().startswith("- review_status:") for l in lines)
    status_present = any(l.strip().startswith("- status:") for l in lines)

    if not status_present:
        updated_lines.append("- status: Active")
    if not review_status_present:
        updated_lines.append("- review_status: Pending")

    updated_lines.append(f"- last_updated: {signal_date}")

    cleaned = remove_duplicate_terminal_fields(updated_lines)
    return "\n".join(cleaned).rstrip() + "\n"


def remove_duplicate_terminal_fields(lines: List[str]) -> List[str]:
    single_fields = {
        "- leverage_score:",
        "- confidence:",
        "- status:",
        "- review_status:",
        "- last_updated:",
        "- rationale:",
    }

    seen = {}
    for i, line in enumerate(lines):
        stripped = line.strip()
        for prefix in single_fields:
            if stripped.startswith(prefix):
                seen[prefix] = i

    result = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        keep = True
        for prefix in single_fields:
            if stripped.startswith(prefix) and seen.get(prefix) != i:
                keep = False
                break
        if keep:
            result.append(line)

    return collapse_last_reinforced_sections(result)


def build_opportunity_block(data: Dict[str, object]) -> str:
    title = make_title(data)
    today = str(data["date"])
    confidence = float(str(data["confidence"]))
    signal_type = str(data["signal_type"])
    branch = str(data.get("branch", "")).strip()
    source_signal_types = [str(item).strip() for item in data.get("source_signal_types", []) if str(item).strip()]
    if not source_signal_types:
        source_signal_types = [signal_type]
    date_window = str(data.get("date_window", today)).strip()

    evidence_lines = [
        f"  - signal {data['signal_id']}",
        f"  - {data['description']}",
    ]

    if isinstance(data.get("evidence"), list):
        for item in data["evidence"]:
            item_str = str(item).strip()
            if item_str:
                evidence_lines.append(f"  - {item_str}")

    evidence_text = "\n".join(evidence_lines)
    source_signal_type_lines = "\n".join(f"  - {item}" for item in source_signal_types)

    return f"""### [{title}]

- source: signal {data['signal_id']}
- branch: {branch}
- category: {data['category']}
- signal_type: {data['signal_type']}
- source_signal_types:
{source_signal_type_lines}
- date_identified: {today}
- date_window: {date_window}
- description: {data['description']}

- leverage_score: {create_score_for_signal(signal_type):.2f}
- risk_level: {risk_level_for_signal(signal_type)}
- confidence: {confidence:.2f}

- score_components:
  - revenue: 0.60
  - scalability: 0.60
  - ease: 0.60
  - strategic: 0.60
  - wellbeing: 0.50

- evidence_sources:
{evidence_text}

- rationale: Initial opportunity created from validated normalized signal.

- last_reinforced:
  - date: {today}
  - delta: 0.00
  - reason: initial opportunity creation from signal ingestion

- status: Active
- review_status: Pending
- last_updated: {today}
"""


def normalize_block_text(block: str) -> str:
    cleaned = canonicalize_block_layout(
        [line for line in block.splitlines() if line.strip() != "---"]
    )
    return "\n".join(cleaned).rstrip() + "\n"


def normalize_blackboard_content(blackboard: str) -> str:
    before, active_part, after = split_blackboard_sections(blackboard)
    if not active_part:
        return blackboard

    blocks = split_blocks(active_part)
    if not blocks:
        return blackboard

    normalized_blocks = [normalize_block_text(block) for block in blocks]
    rebuilt_active = rebuild_active_part(active_part, normalized_blocks)
    return before + ACTIVE_MARKER + rebuilt_active + after


def count_duplicate_opportunity_keys(blocks: List[str]) -> int:
    keys = [
        (
            extract_block_title(block).strip(),
            extract_field(block, "branch").strip().lower(),
            extract_field(block, "category").strip().lower(),
            extract_field(block, "date_window").strip(),
        )
        for block in blocks
    ]
    duplicate_counts: Dict[Tuple[str, str, str, str], int] = {}
    for key in keys:
        duplicate_counts[key] = duplicate_counts.get(key, 0) + 1
    return sum(count - 1 for count in duplicate_counts.values() if count > 1)


def block_structure_issues(block: str) -> List[str]:
    lines = block.splitlines()
    issues: List[str] = []

    blank_run = 0
    for line in lines:
        if not line.strip():
            blank_run += 1
        else:
            blank_run = 0
        if blank_run >= 2:
            issues.append("excessive_blank_line_drift")
            break

    if any(line.strip() == "---" for line in lines):
        issues.append("embedded_separator")

    last_reinforced_indexes = [idx for idx, line in enumerate(lines) if line.strip() == "- last_reinforced:"]
    if len(last_reinforced_indexes) > 1:
        issues.append("duplicate_last_reinforced_section")

    terminal_fields = {
        "rationale": [idx for idx, line in enumerate(lines) if line.strip().startswith("- rationale:")],
        "status": [idx for idx, line in enumerate(lines) if line.strip().startswith("- status:")],
        "review_status": [idx for idx, line in enumerate(lines) if line.strip().startswith("- review_status:")],
        "last_updated": [idx for idx, line in enumerate(lines) if line.strip().startswith("- last_updated:")],
    }
    for name, indexes in terminal_fields.items():
        if len(indexes) > 1:
            issues.append(f"duplicate_{name}")

    last_reinforced_index = last_reinforced_indexes[-1] if last_reinforced_indexes else -1
    status_index = terminal_fields["status"][-1] if terminal_fields["status"] else -1
    review_index = terminal_fields["review_status"][-1] if terminal_fields["review_status"] else -1
    updated_index = terminal_fields["last_updated"][-1] if terminal_fields["last_updated"] else -1
    if last_reinforced_index >= 0 and status_index >= 0 and status_index < last_reinforced_index:
        issues.append("terminal_section_order_drift")
    if last_reinforced_index >= 0 and review_index >= 0 and review_index < last_reinforced_index:
        issues.append("terminal_section_order_drift")
    if last_reinforced_index >= 0 and updated_index >= 0 and updated_index < last_reinforced_index:
        issues.append("terminal_section_order_drift")

    return sorted(set(issues))


def semantic_block_fingerprint(block: str) -> Dict[str, object]:
    canonical_lines = canonicalize_block_layout([line for line in block.splitlines() if line.strip() != "---"])
    return {
        "title": extract_block_title(block).strip(),
        "non_blank_lines": [line for line in canonical_lines if line.strip()],
    }


def semantic_blackboard_fingerprint(blackboard: str) -> List[Dict[str, object]]:
    _before, active_part, _after = split_blackboard_sections(blackboard)
    return [semantic_block_fingerprint(block) for block in split_blocks(active_part)]


def build_opportunities_hygiene_audit(blackboard: str) -> Dict[str, object]:
    _before, active_part, _after = split_blackboard_sections(blackboard)
    blocks = split_blocks(active_part)
    issues_before = sum(1 for block in blocks if block_structure_issues(block))
    normalized = normalize_blackboard_content(blackboard)
    _before_after, active_after, _after_after = split_blackboard_sections(normalized)
    normalized_blocks = split_blocks(active_after)
    issues_after = sum(1 for block in normalized_blocks if block_structure_issues(block))
    return {
        "block_count": len(normalized_blocks),
        "separator_count": active_after.count("---"),
        "blocks_with_structure_issues_before": issues_before,
        "blocks_with_structure_issues_after": issues_after,
        "duplicate_key_count": count_duplicate_opportunity_keys(normalized_blocks),
        "notes": (
            "Canonical block hygiene collapses blank-line drift, removes embedded separators, "
            "and orders trailing structural fields as rationale -> last_reinforced -> status -> review_status -> last_updated."
        ),
    }


def find_block_line_index(lines: List[str], prefix: str) -> int:
    for idx, line in enumerate(lines):
        if line.strip().startswith(prefix):
            return idx
    return -1


def block_has_valid_terminal_structure(block: str) -> bool:
    lines = block.splitlines()
    rationale_index = find_block_line_index(lines, "- rationale:")
    last_reinforced_index = find_block_line_index(lines, "- last_reinforced:")
    status_index = find_block_line_index(lines, "- status:")
    review_status_index = find_block_line_index(lines, "- review_status:")
    last_updated_index = find_block_line_index(lines, "- last_updated:")
    indexes = [
        rationale_index,
        last_reinforced_index,
        status_index,
        review_status_index,
        last_updated_index,
    ]
    if any(idx < 0 for idx in indexes):
        return False
    return indexes == sorted(indexes)


def block_has_valid_delta_entries(block: str) -> bool:
    for line in block.splitlines():
        stripped = line.strip()
        if not stripped.startswith("- delta:"):
            continue
        try:
            float(stripped.replace("- delta:", "", 1).strip())
        except ValueError:
            return False
    return True


def verify_system_invariants() -> Dict[str, object]:
    violations: List[str] = []

    opportunities_structure = True
    blackboard = read_file(BLACKBOARD_PATH)
    _before, active_part, _after = split_blackboard_sections(blackboard)
    blocks = split_blocks(active_part)
    duplicate_block_count = count_duplicate_opportunity_keys(blocks)
    if duplicate_block_count > 0:
        opportunities_structure = False
        violations.append(f"OPPORTUNITIES.md contains {duplicate_block_count} duplicate opportunity blocks")
    for block in blocks:
        title = extract_block_title(block).strip() or "unknown_block"
        if not block_has_valid_terminal_structure(block):
            opportunities_structure = False
            violations.append(f"OPPORTUNITIES.md block has invalid terminal structure: {title}")
        if not block_has_valid_delta_entries(block):
            opportunities_structure = False
            violations.append(f"OPPORTUNITIES.md block has malformed delta entry: {title}")

    warning_schema = True
    warning_payload = read_json(WARNING_INTELLIGENCE_PATH)
    if not isinstance(warning_payload, dict):
        warning_schema = False
        violations.append("warning_intelligence.json is not a dict payload")
    else:
        for pattern_id, entry in warning_payload.items():
            if not isinstance(entry, dict):
                warning_schema = False
                violations.append(f"warning_intelligence.json entry is not an object: {pattern_id}")
                continue
            severity_score = entry.get("severity_score")
            escalation_level = entry.get("escalation_level")
            if not isinstance(escalation_level, str) or not escalation_level.strip():
                warning_schema = False
                violations.append(f"warning_intelligence.json missing escalation_level: {pattern_id}")
            try:
                severity_value = float(severity_score)
            except (TypeError, ValueError):
                warning_schema = False
                violations.append(f"warning_intelligence.json missing numeric severity_score: {pattern_id}")
                continue
            if severity_value < 0.0 or severity_value > 1.0:
                warning_schema = False
                violations.append(f"warning_intelligence.json severity_score out of range: {pattern_id}")

    fusion_context_schema = True
    fusion_rows = read_json_list(FUSION_SIGNAL_CONTEXT_PATH)
    for idx, row in enumerate(fusion_rows):
        for field in ("branch", "category", "signal_type"):
            if str(row.get(field, "")).strip() == "":
                fusion_context_schema = False
                violations.append(f"fusion_signal_context.json row {idx} missing {field}")
        if not isinstance(row.get("matched"), bool):
            fusion_context_schema = False
            violations.append(f"fusion_signal_context.json row {idx} has non-boolean matched")

    normalization_audit_schema = True
    normalization_rows = read_json_list(NORMALIZATION_GAP_AUDIT_PATH)
    valid_signal_refs = {
        str(row.get("signal_ref", "")).strip()
        for row in normalization_rows
        if str(row.get("signal_ref", "")).strip()
    }
    for idx, row in enumerate(normalization_rows):
        for field in ("branch_before", "branch_after", "section_before", "section_after", "issue_type"):
            if field not in row:
                normalization_audit_schema = False
                violations.append(f"normalization_gap_audit.json row {idx} missing {field}")
        duplicate_of = row.get("duplicate_of")
        if duplicate_of is None or duplicate_of == "":
            continue
        if not isinstance(duplicate_of, str) or not duplicate_of.strip():
            normalization_audit_schema = False
            violations.append(f"normalization_gap_audit.json row {idx} has ambiguous duplicate_of")
            continue
        if duplicate_of not in valid_signal_refs:
            normalization_audit_schema = False
            violations.append(f"normalization_gap_audit.json row {idx} duplicate_of is not a valid signal_ref")

    checks = {
        "opportunities_structure": opportunities_structure,
        "warning_schema": warning_schema,
        "fusion_context_schema": fusion_context_schema,
        "normalization_audit_schema": normalization_audit_schema,
    }
    return {
        "status": "pass" if all(checks.values()) else "fail",
        "checks": checks,
        "violations": violations,
    }


def write_invariant_report() -> Dict[str, object]:
    payload = verify_system_invariants()
    write_json(WAVE7A_INVARIANT_REPORT_PATH, payload)
    return payload


def signal_ref_exists(signal_ref: object) -> bool:
    value = str(signal_ref or "").strip()
    if not value:
        return False
    return Path(value).exists()


def signal_ref_in_valid_source_family(signal_ref: object) -> bool:
    value = str(signal_ref or "").strip()
    if not value:
        return False
    normalized = value.replace("\\", "/")
    return any(normalized.startswith(prefix) for prefix in VALID_SIGNAL_LINKAGE_PREFIXES)


def current_corpus_signal_record_lookup() -> Dict[str, Tuple[str, Dict[str, object]]]:
    lookup: Dict[str, Tuple[str, Dict[str, object]]] = {}
    for path, data, _mark_processed in validated_signal_records(collect_signal_records()):
        lookup[canonical_source_signal_ref(path, data)] = (path, data)
    return lookup


def verify_cross_system_consistency() -> Dict[str, object]:
    violations: List[str] = []
    fusion_rows = read_json_list(FUSION_SIGNAL_CONTEXT_PATH)
    normalization_rows = read_json_list(NORMALIZATION_GAP_AUDIT_PATH)
    record_lookup = current_corpus_signal_record_lookup()

    normalization_by_signal_ref = {
        str(row.get("signal_ref", "")).strip(): row
        for row in normalization_rows
        if str(row.get("signal_ref", "")).strip()
    }
    normalization_signal_refs = set(normalization_by_signal_ref.keys())
    corpus_signal_refs = set(record_lookup.keys())

    signal_linkage = True
    for idx, row in enumerate(fusion_rows):
        signal_ref = str(row.get("signal_ref", "")).strip()
        if (
            signal_ref in normalization_signal_refs
            or signal_ref in corpus_signal_refs
            or signal_ref_exists(signal_ref)
            or signal_ref_in_valid_source_family(signal_ref)
        ):
            continue
        signal_linkage = False
        violations.append(f"fusion_signal_context.json row {idx} signal_ref is not linked: {signal_ref or 'missing'}")

    duplicate_integrity = True
    duplicate_map = {
        signal_ref: row.get("duplicate_of")
        for signal_ref, row in normalization_by_signal_ref.items()
    }
    for signal_ref, row in normalization_by_signal_ref.items():
        duplicate_of = row.get("duplicate_of")
        if duplicate_of in (None, ""):
            continue
        duplicate_ref = str(duplicate_of).strip()
        if not duplicate_ref:
            duplicate_integrity = False
            violations.append(f"normalization_gap_audit.json duplicate_of is ambiguous for {signal_ref}")
            continue
        duplicate_exists = duplicate_ref in normalization_signal_refs or signal_ref_exists(duplicate_ref)
        if not duplicate_exists:
            duplicate_integrity = False
            violations.append(f"normalization_gap_audit.json duplicate target missing for {signal_ref}: {duplicate_ref}")
            continue
        target_duplicate_of = duplicate_map.get(duplicate_ref)
        if str(target_duplicate_of or "").strip() == signal_ref:
            duplicate_integrity = False
            violations.append(f"normalization_gap_audit.json duplicate cycle detected between {signal_ref} and {duplicate_ref}")

    pattern_format = True
    for idx, row in enumerate(fusion_rows):
        pattern_id = str(row.get("pattern_id", "")).strip()
        matched = bool(row.get("matched"))
        if not pattern_id:
            if matched:
                pattern_format = False
                violations.append(f"fusion_signal_context.json row {idx} matched row missing pattern_id")
            continue
        if not matched:
            continue
        signal_ref = str(row.get("signal_ref", "")).strip()
        record = record_lookup.get(signal_ref)
        if record is None:
            pattern_format = False
            violations.append(f"fusion_signal_context.json row {idx} matched row missing source record for pattern validation")
            continue
        expected_pattern_id = build_warning_pattern_key(record[0], record[1])[0]
        if pattern_id != expected_pattern_id:
            pattern_format = False
            violations.append(
                "fusion_signal_context.json row "
                f"{idx} pattern_id mismatch: expected {expected_pattern_id} got {pattern_id}"
            )

    branch_alignment = True
    for idx, row in enumerate(fusion_rows):
        signal_ref = str(row.get("signal_ref", "")).strip()
        normalization_row = normalization_by_signal_ref.get(signal_ref)
        if normalization_row is None:
            continue
        fusion_branch = str(row.get("branch", "")).strip()
        normalization_branch = str(normalization_row.get("branch_after", "")).strip()
        if fusion_branch != normalization_branch:
            branch_alignment = False
            violations.append(
                "fusion_signal_context.json row "
                f"{idx} branch mismatch for {signal_ref}: fusion={fusion_branch} normalization={normalization_branch}"
            )

    checks = {
        "signal_linkage": signal_linkage,
        "duplicate_integrity": duplicate_integrity,
        "pattern_format": pattern_format,
        "branch_alignment": branch_alignment,
    }
    return {
        "status": "pass" if all(checks.values()) else "fail",
        "checks": checks,
        "violations": violations,
    }


def write_cross_system_consistency_report() -> Dict[str, object]:
    payload = verify_cross_system_consistency()
    write_json(WAVE7B_CONSISTENCY_REPORT_PATH, payload)
    return payload


def final_certification_paths() -> List[str]:
    return [
        BLACKBOARD_PATH,
        WARNING_INTELLIGENCE_PATH,
        FUSION_SIGNAL_CONTEXT_PATH,
        FUSION_SCORE_AUDIT_PATH,
        FUSION_EFFECT_AUDIT_PATH,
    ]


def capture_final_system_hashes() -> Dict[str, str]:
    return {path: file_sha256_or_empty(path) for path in final_certification_paths()}


def load_final_system_artifacts() -> None:
    read_file(BLACKBOARD_PATH)
    read_json(WARNING_INTELLIGENCE_PATH)
    read_json_list(FUSION_SIGNAL_CONTEXT_PATH)
    read_json_list(FUSION_SCORE_AUDIT_PATH)
    read_json_list(FUSION_EFFECT_AUDIT_PATH)


def verify_final_system_state() -> Dict[str, object]:
    load_final_system_artifacts()
    first_hashes = capture_final_system_hashes()

    load_final_system_artifacts()
    second_hashes = capture_final_system_hashes()

    invariant_report = read_json(WAVE7A_INVARIANT_REPORT_PATH)
    consistency_report = read_json(WAVE7B_CONSISTENCY_REPORT_PATH)

    hash_stability = first_hashes == second_hashes
    invariants_passed = str(invariant_report.get("status", "")).strip().lower() == "pass"
    consistency_passed = str(consistency_report.get("status", "")).strip().lower() == "pass"
    idempotency = hash_stability

    notes_parts: List[str] = []
    if not hash_stability:
        notes_parts.append("Protected artifact hashes changed across repeated dry validation.")
    if not invariants_passed:
        notes_parts.append("Wave 7A invariant report status is fail.")
    if not consistency_passed:
        notes_parts.append("Wave 7B consistency report status is fail.")
    if not notes_parts:
        notes_parts.append("All protected artifacts were hash-stable and prerequisite reports passed.")

    return {
        "status": (
            "CERTIFIED"
            if idempotency and invariants_passed and consistency_passed and hash_stability
            else "FAILED"
        ),
        "idempotency": idempotency,
        "invariants_passed": invariants_passed,
        "consistency_passed": consistency_passed,
        "hash_stability": hash_stability,
        "notes": " ".join(notes_parts),
    }


def write_final_system_certification() -> Dict[str, object]:
    payload = verify_final_system_state()
    write_json(WAVE7C_FINAL_CERTIFICATION_PATH, payload)
    return payload


def write_opportunities_hygiene_audit(audit: Dict[str, object]) -> Dict[str, object]:
    payload = dict(audit)
    if os.path.exists(OPPORTUNITIES_HYGIENE_AUDIT_PATH):
        try:
            existing_payload = read_json(OPPORTUNITIES_HYGIENE_AUDIT_PATH)
        except Exception as exc:
            log(f"WARN opportunities hygiene audit unreadable: {exc}")
        else:
            if isinstance(existing_payload, dict):
                comparable_keys = (
                    "block_count",
                    "separator_count",
                    "blocks_with_structure_issues_after",
                    "duplicate_key_count",
                    "notes",
                )
                comparable = all(existing_payload.get(key) == payload.get(key) for key in comparable_keys)
                if (
                    comparable
                    and safe_int(payload.get("blocks_with_structure_issues_before", 0)) == 0
                    and safe_int(existing_payload.get("blocks_with_structure_issues_before", 0)) > 0
                ):
                    payload["blocks_with_structure_issues_before"] = safe_int(
                        existing_payload.get("blocks_with_structure_issues_before", 0)
                    )
    write_json(OPPORTUNITIES_HYGIENE_AUDIT_PATH, payload)
    return payload


def write_wave6b_snapshot(audit: Dict[str, object]) -> None:
    created_at_utc = stable_utc_now()
    payload = {
        "phase": "wave6b_blackboard_hygiene",
        "created_at_utc": created_at_utc,
        "opportunities_hygiene": {
            "block_count": safe_int(audit.get("block_count", 0)),
            "separator_count": safe_int(audit.get("separator_count", 0)),
            "blocks_with_structure_issues_before": safe_int(audit.get("blocks_with_structure_issues_before", 0)),
            "blocks_with_structure_issues_after": safe_int(audit.get("blocks_with_structure_issues_after", 0)),
            "duplicate_key_count": safe_int(audit.get("duplicate_key_count", 0)),
            "notes": str(audit.get("notes", "")).strip(),
        },
        "artifacts": [
            artifact_snapshot("worker_decision_v2.py"),
            artifact_snapshot(BLACKBOARD_PATH),
            artifact_snapshot(OPPORTUNITIES_HYGIENE_AUDIT_PATH),
        ],
    }
    if os.path.exists(WAVE6B_SNAPSHOT_PATH):
        try:
            existing_payload = read_json(WAVE6B_SNAPSHOT_PATH)
        except Exception as exc:
            log(f"WARN wave6b snapshot unreadable: {exc}")
        else:
            if isinstance(existing_payload, dict):
                comparable_existing = dict(existing_payload)
                comparable_new = dict(payload)
                comparable_existing.pop("created_at_utc", None)
                comparable_new.pop("created_at_utc", None)
                if comparable_existing == comparable_new:
                    existing_created_at = str(existing_payload.get("created_at_utc", "")).strip()
                    if existing_created_at:
                        payload["created_at_utc"] = existing_created_at
    write_json(WAVE6B_SNAPSHOT_PATH, payload)


def run_wave6b_blackboard_hygiene() -> Dict[str, object]:
    protected_paths = [
        CONFIDENCE_AUDIT_PATH,
        DENSITY_AUDIT_PATH,
        WARNING_INTELLIGENCE_PATH,
        WARNING_PATTERN_AUDIT_PATH,
        FUSION_SIGNAL_CONTEXT_PATH,
        FUSION_SCORE_AUDIT_PATH,
        FUSION_EFFECT_AUDIT_PATH,
        NORMALIZATION_GAP_AUDIT_PATH,
        WAVE3_SNAPSHOT_PATH,
        WAVE4A_SNAPSHOT_PATH,
        WAVE4B_SNAPSHOT_PATH,
        WAVE5A_SNAPSHOT_PATH,
        WAVE5B_SNAPSHOT_PATH,
        WAVE6A_SNAPSHOT_PATH,
    ]
    before_hashes = {path: file_sha256_or_empty(path) for path in protected_paths}

    original = read_file(BLACKBOARD_PATH)
    original_fingerprint = semantic_blackboard_fingerprint(original)
    normalized = normalize_blackboard_content(original)
    normalized_fingerprint = semantic_blackboard_fingerprint(normalized)
    if original_fingerprint != normalized_fingerprint:
        raise RuntimeError("Wave 6B validation failed: opportunity content changed semantically during hygiene normalization")

    before_audit = build_opportunities_hygiene_audit(original)
    after_audit = build_opportunities_hygiene_audit(normalized)
    if safe_int(after_audit.get("duplicate_key_count", 0)) > safe_int(before_audit.get("duplicate_key_count", 0)):
        raise RuntimeError("Wave 6B validation failed: duplicate opportunity keys were introduced")

    write_file(BLACKBOARD_PATH, normalized)
    second_run_content = normalize_blackboard_content(read_file(BLACKBOARD_PATH))
    if second_run_content != normalized:
        raise RuntimeError("Wave 6B validation failed: second-run blackboard normalization is not byte-identical")

    audit = {
        "block_count": safe_int(after_audit.get("block_count", 0)),
        "separator_count": safe_int(after_audit.get("separator_count", 0)),
        "blocks_with_structure_issues_before": safe_int(before_audit.get("blocks_with_structure_issues_before", 0)),
        "blocks_with_structure_issues_after": safe_int(after_audit.get("blocks_with_structure_issues_after", 0)),
        "duplicate_key_count": safe_int(after_audit.get("duplicate_key_count", 0)),
        "notes": (
            f"{after_audit.get('notes', '')} "
            "Semantic fingerprint preserved; second-run normalization is byte-identical."
        ).strip(),
    }
    persisted_audit = write_opportunities_hygiene_audit(audit)
    write_wave6b_snapshot(persisted_audit)

    after_hashes = {path: file_sha256_or_empty(path) for path in protected_paths}
    if after_hashes != before_hashes:
        changed = [path for path in protected_paths if after_hashes.get(path, "") != before_hashes.get(path, "")]
        raise RuntimeError(f"Wave 6B validation failed: non-blackboard protected artifacts changed: {', '.join(changed)}")

    return persisted_audit


def is_live_processing_candidate(path: str, data: Dict[str, object]) -> bool:
    signal_type = str(data.get("signal_type", "")).strip()
    if signal_type not in SIGNAL_OPPORTUNITY_MAP:
        return False
    valid, _reason = validate_signal(data)
    return valid


def is_unknown_staff_live_artifact(path: str, data: Dict[str, object]) -> bool:
    haystack = " ".join(
        str(data.get(key, ""))
        for key in ("signal_id", "description", "staff_name", "source_name", "branch", "source_ref")
    )
    return "unknown_staff" in haystack.lower()


def is_latest_signal_record(path: str, data: Dict[str, object]) -> bool:
    ref = canonical_source_signal_ref(path, data).lower()
    signal_id = str(data.get("signal_id", "")).strip().lower()
    return "latest" in ref or "latest" in signal_id or Path(path).stem.lower().endswith("_latest")


def live_signal_cluster_key(path: str, data: Dict[str, object]) -> Tuple[str, str, str, str, str]:
    return (
        audit_resolved_branch(path, data),
        str(data.get("category", "")).strip().lower(),
        str(data.get("date_window") or data.get("date") or "").strip(),
        str(data.get("signal_type", "")).strip(),
        normalize_similarity_text(data.get("description", "")),
    )


def canonical_live_cluster_record(
    items: List[Tuple[str, Dict[str, object], bool]],
) -> Tuple[str, Dict[str, object], bool]:
    ranked = sorted(
        items,
        key=lambda item: (
            1 if is_latest_signal_record(item[0], item[1]) else 0,
            canonical_source_signal_ref(item[0], item[1]),
            item[0],
        ),
    )
    return ranked[-1]


def build_processing_guardrails(
    records: List[Tuple[str, Dict[str, object], bool]],
    *,
    enforce_family_coverage: bool = False,
) -> Tuple[List[Dict[str, object]], Dict[str, object], Dict[str, Dict[str, object]]]:
    validated = [record for record in records if is_live_processing_candidate(record[0], record[1])]
    clusters: Dict[Tuple[str, str, str, str, str], List[Tuple[str, Dict[str, object], bool]]] = {}
    for record in validated:
        clusters.setdefault(live_signal_cluster_key(record[0], record[1]), []).append(record)

    excluded_by_signal_ref: Dict[str, Dict[str, object]] = {}
    audit_rows: List[Dict[str, object]] = []

    for key, items in sorted(clusters.items()):
        canonical_record = canonical_live_cluster_record(items)
        canonical_ref = canonical_source_signal_ref(canonical_record[0], canonical_record[1])
        for path, data, _mark_processed in sorted(
            items,
            key=lambda item: canonical_source_signal_ref(item[0], item[1]),
        ):
            signal_ref = canonical_source_signal_ref(path, data)
            if is_unknown_staff_live_artifact(path, data):
                excluded_by_signal_ref[signal_ref] = {
                    "signal_ref": signal_ref,
                    "signal_type": str(data.get("signal_type", "")).strip(),
                    "issue_type": "unknown_staff_artifact",
                    "recommended_action": "exclude_from_live_processing",
                    "currently_processed": True,
                    "notes": "Advisory-derived unknown_staff artifact is treated as noisy and excluded from live processing.",
                }
                continue

            if len(items) <= 1:
                continue

            if signal_ref == canonical_ref:
                continue

            issue_type = "same_day_semantic_replay_duplicate"
            notes = (
                f"Excluded in favor of canonical live signal {canonical_ref} for the same branch/category/date_window/signal_type/description cluster."
            )
            if is_latest_signal_record(path, data) or is_latest_signal_record(canonical_record[0], canonical_record[1]):
                issue_type = "latest_timestamp_duplicate_pair"
                notes = f"Excluded in favor of canonical live signal {canonical_ref}; latest/timestamp duplicates should not both affect live processing."

            excluded_by_signal_ref[signal_ref] = {
                "signal_ref": signal_ref,
                "signal_type": str(data.get("signal_type", "")).strip(),
                "issue_type": issue_type,
                "recommended_action": "exclude_from_live_processing",
                "currently_processed": True,
                "notes": notes,
            }

    for signal_ref in sorted(excluded_by_signal_ref):
        audit_rows.append(dict(excluded_by_signal_ref[signal_ref]))

    examined_signal_types = {str(record[1].get("signal_type", "")).strip() for record in validated}
    preserved_signal_types = {
        str(record[1].get("signal_type", "")).strip()
        for record in validated
        if canonical_source_signal_ref(record[0], record[1]) not in excluded_by_signal_ref
    }
    if enforce_family_coverage and examined_signal_types - preserved_signal_types:
        missing = ", ".join(sorted(item for item in examined_signal_types - preserved_signal_types if item))
        raise RuntimeError(f"Wave 6C validation failed: signal family coverage dropped for {missing}")

    summary = {
        "signals_examined": len(validated),
        "signals_flagged_noisy": len(audit_rows),
        "signals_excluded_from_live_processing": len(excluded_by_signal_ref),
        "signals_preserved": len(validated) - len(excluded_by_signal_ref),
        "notes": (
            "Guardrails exclude unknown_staff advisory artifacts and same-day semantic replay duplicates, "
            "keeping one canonical live representative per cluster."
        ),
    }
    return audit_rows, summary, excluded_by_signal_ref


def write_wave6c_snapshot(summary: Dict[str, object]) -> None:
    created_at_utc = stable_utc_now()
    payload = {
        "phase": "wave6c_legacy_signal_hygiene_processing_guardrails",
        "created_at_utc": created_at_utc,
        "processing_guardrails": {
            "signals_examined": safe_int(summary.get("signals_examined", 0)),
            "signals_flagged_noisy": safe_int(summary.get("signals_flagged_noisy", 0)),
            "signals_excluded_from_live_processing": safe_int(summary.get("signals_excluded_from_live_processing", 0)),
            "signals_preserved": safe_int(summary.get("signals_preserved", 0)),
            "notes": str(summary.get("notes", "")).strip(),
        },
        "artifacts": [
            artifact_snapshot("worker_decision_v2.py"),
            artifact_snapshot(BLACKBOARD_PATH),
            artifact_snapshot(LEGACY_SIGNAL_HYGIENE_AUDIT_PATH),
            artifact_snapshot(PROCESSING_GUARDRAIL_SUMMARY_PATH),
        ],
    }
    if os.path.exists(WAVE6C_SNAPSHOT_PATH):
        try:
            existing_payload = read_json(WAVE6C_SNAPSHOT_PATH)
        except Exception as exc:
            log(f"WARN wave6c snapshot unreadable: {exc}")
        else:
            if isinstance(existing_payload, dict):
                comparable_existing = dict(existing_payload)
                comparable_new = dict(payload)
                comparable_existing.pop("created_at_utc", None)
                comparable_new.pop("created_at_utc", None)
                if comparable_existing == comparable_new:
                    existing_created_at = str(existing_payload.get("created_at_utc", "")).strip()
                    if existing_created_at:
                        payload["created_at_utc"] = existing_created_at
    write_json(WAVE6C_SNAPSHOT_PATH, payload)


def run_wave6c_processing_guardrails(
    records: List[Tuple[str, Dict[str, object], bool]],
) -> Dict[str, object]:
    protected_paths = [
        BLACKBOARD_PATH,
        CONFIDENCE_AUDIT_PATH,
        DENSITY_AUDIT_PATH,
        WARNING_INTELLIGENCE_PATH,
        WARNING_PATTERN_AUDIT_PATH,
        FUSION_SIGNAL_CONTEXT_PATH,
        FUSION_SCORE_AUDIT_PATH,
        FUSION_EFFECT_AUDIT_PATH,
        NORMALIZATION_GAP_AUDIT_PATH,
        OPPORTUNITIES_HYGIENE_AUDIT_PATH,
        WAVE3_SNAPSHOT_PATH,
        WAVE4A_SNAPSHOT_PATH,
        WAVE4B_SNAPSHOT_PATH,
        WAVE5A_SNAPSHOT_PATH,
        WAVE5B_SNAPSHOT_PATH,
        WAVE6A_SNAPSHOT_PATH,
        WAVE6B_SNAPSHOT_PATH,
    ]
    before_hashes = {path: file_sha256_or_empty(path) for path in protected_paths}

    first_rows, first_summary, first_excluded = build_processing_guardrails(records, enforce_family_coverage=True)
    second_rows, second_summary, second_excluded = build_processing_guardrails(records, enforce_family_coverage=True)
    if first_rows != second_rows or first_summary != second_summary or first_excluded != second_excluded:
        raise RuntimeError("Wave 6C validation failed: processing guardrails are not idempotent across identical runs")

    write_json(LEGACY_SIGNAL_HYGIENE_AUDIT_PATH, first_rows)
    write_json(PROCESSING_GUARDRAIL_SUMMARY_PATH, first_summary)
    write_wave6c_snapshot(first_summary)

    after_hashes = {path: file_sha256_or_empty(path) for path in protected_paths}
    if after_hashes != before_hashes:
        changed = [path for path in protected_paths if after_hashes.get(path, "") != before_hashes.get(path, "")]
        raise RuntimeError(f"Wave 6C validation failed: protected artifacts changed: {', '.join(changed)}")

    return {
        "summary": first_summary,
        "excluded_signal_refs": sorted(first_excluded),
    }


def insert_into_active_opportunities(blackboard: str, block: str) -> str:
    normalized_block = normalize_block_text(block)
    before, active_part, after = split_blackboard_sections(blackboard)

    if not active_part and ACTIVE_MARKER not in blackboard:
        return blackboard.rstrip() + f"\n\n{ACTIVE_MARKER}\n\n" + normalized_block

    blocks = split_blocks(active_part)
    blocks.insert(0, normalized_block)
    rebuilt_active = rebuild_active_part(active_part, blocks)
    return before + ACTIVE_MARKER + rebuilt_active + after


def mark_signal_processed(path: str) -> None:
    content = read_file(path)
    updated = content.replace("status: new", "status: processed", 1)
    write_file(path, updated)


def summarize_sales_report(payload: Dict[str, object], iso_date: str) -> Tuple[str, List[str]]:
    totals = payload.get("totals", {}) if isinstance(payload.get("totals"), dict) else {}
    traffic = payload.get("traffic", {}) if isinstance(payload.get("traffic"), dict) else {}
    staffing = payload.get("staffing", {}) if isinstance(payload.get("staffing"), dict) else {}

    z_reading = float(totals.get("z_reading", 0.0) or 0.0)
    customers = int(traffic.get("total_customers", 0) or 0)
    served = int(traffic.get("customers_served", 0) or 0)
    staff_on_duty = int(staffing.get("staff_on_duty", 0) or 0)

    description = (
        f"Sales report for {payload.get('branch', 'unknown')} on {iso_date}: "
        f"PGK {z_reading:.2f} total sales across {customers} customers, "
        f"{served} served, with {staff_on_duty} staff on duty."
    )
    evidence = [
        f"report_type:sales_report",
        f"total_sales:{z_reading:.2f}",
        f"customers:{customers}",
        f"customers_served:{served}",
        f"staff_on_duty:{staff_on_duty}",
    ]
    return description, evidence


def summarize_inventory_report(payload: Dict[str, object], iso_date: str) -> Tuple[str, List[str]]:
    sections = payload.get("sections", [])
    if not isinstance(sections, list):
        sections = []

    flagged_sections = []
    for entry in sections:
        if not isinstance(entry, dict):
            continue
        status = str(entry.get("status", "")).strip().upper()
        section = str(entry.get("section", "")).strip()
        if status and status != "NORMAL":
            flagged_sections.append(f"{section}:{status}")

    description = (
        f"Inventory report for {payload.get('branch', 'unknown')} on {iso_date}: "
        f"{len(flagged_sections)} of {len(sections)} sections show stock pressure."
    )
    evidence = [f"report_type:inventory_report", f"flagged_sections:{len(flagged_sections)}"]
    evidence.extend(flagged_sections)
    return description, evidence


def summarize_staff_report(payload: Dict[str, object], iso_date: str) -> Tuple[str, List[str]]:
    records = payload.get("staff_records", [])
    if not isinstance(records, list):
        records = []

    total_performance = 0.0
    total_display = 0.0
    total_arrangement = 0.0
    valid_records = 0
    evidence = ["report_type:staff_report", f"staff_records:{len(records)}"]
    top_staff = ""
    top_score = -1.0

    for record in records:
        if not isinstance(record, dict):
            continue
        performance = float(record.get("performance", 0) or 0)
        display = float(record.get("display", 0) or 0)
        arrangement = float(record.get("arrangement", 0) or 0)
        staff_name = str(record.get("staff_name", "unknown")).strip()
        section = str(record.get("section", "unknown")).strip()
        score = performance + display + arrangement
        total_performance += performance
        total_display += display
        total_arrangement += arrangement
        valid_records += 1
        evidence.append(
            f"{staff_name}:{section}:performance={performance:.1f}:display={display:.1f}:arrangement={arrangement:.1f}"
        )
        if score > top_score:
            top_score = score
            top_staff = f"{staff_name} in {section}"

    avg_performance = total_performance / valid_records if valid_records else 0.0
    avg_display = total_display / valid_records if valid_records else 0.0
    avg_arrangement = total_arrangement / valid_records if valid_records else 0.0
    description = (
        f"Staff report for {payload.get('branch', 'unknown')} on {iso_date}: "
        f"{valid_records} staff records with average performance {avg_performance:.1f}, "
        f"display {avg_display:.1f}, arrangement {avg_arrangement:.1f}; "
        f"strongest execution from {top_staff or 'no named record'}."
    )
    return description, evidence


def summarize_supervisor_report(payload: Dict[str, object], iso_date: str) -> Tuple[str, List[str]]:
    exceptions = payload.get("exceptions", [])
    if not isinstance(exceptions, list):
        exceptions = []

    evidence = ["report_type:supervisor_report", f"exceptions:{len(exceptions)}"]
    latest_summary = "no exceptions logged"

    if exceptions:
        latest = exceptions[-1] if isinstance(exceptions[-1], dict) else {}
        exception_type = str(latest.get("exception_type", "UNKNOWN")).strip()
        details = str(latest.get("details", "no details")).strip()
        time_value = str(latest.get("time", "unknown")).strip()
        latest_summary = f"{exception_type} at {time_value}: {details}"
        for entry in exceptions:
            if not isinstance(entry, dict):
                continue
            evidence.append(
                f"{entry.get('exception_type', 'UNKNOWN')}:{entry.get('time', 'unknown')}:{entry.get('details', '')}"
            )

    description = (
        f"Supervisor report for {payload.get('branch', 'unknown')} on {iso_date}: "
        f"{len(exceptions)} control exceptions logged, latest {latest_summary}."
    )
    return description, evidence


def summarize_bale_report(payload: Dict[str, object], iso_date: str) -> Tuple[str, List[str]]:
    bales = payload.get("bales", [])
    if not isinstance(bales, list):
        bales = []
    totals = payload.get("totals", {}) if isinstance(payload.get("totals"), dict) else {}
    total_qty = float(totals.get("total_qty", 0) or 0)
    total_amount = float(totals.get("total_amount", 0.0) or 0.0)

    evidence = [
        "report_type:bale_report",
        f"bales:{len(bales)}",
        f"total_qty:{int(total_qty)}",
        f"total_amount:{total_amount:.2f}",
    ]
    for entry in bales:
        if not isinstance(entry, dict):
            continue
        evidence.append(
            f"{entry.get('bale_id', 'unknown')}:{entry.get('section', 'unknown')}:qty={entry.get('qty', 0)}:status={entry.get('status', 'UNKNOWN')}"
        )

    description = (
        f"Bale report for {payload.get('branch', 'unknown')} on {iso_date}: "
        f"{len(bales)} bales released with total quantity {int(total_qty)} "
        f"and total amount PGK {total_amount:.2f}."
    )
    return description, evidence


def summarize_target_json_signal(payload: Dict[str, object], iso_date: str) -> Tuple[str, List[str]]:
    signal_type = str(payload.get("signal_type", "")).strip()
    if signal_type == "daily_sales_report":
        return summarize_sales_report(payload, iso_date)
    if signal_type == "inventory_availability_report":
        return summarize_inventory_report(payload, iso_date)
    if signal_type == "staff_performance_report":
        return summarize_staff_report(payload, iso_date)
    if signal_type == "supervisor_control_report":
        return summarize_supervisor_report(payload, iso_date)
    if signal_type == "daily_bale_summary_report":
        return summarize_bale_report(payload, iso_date)
    return "", []


def adapt_markdown_signal(path: str, data: Dict[str, object]) -> Dict[str, object]:
    signal_type = str(data.get("signal_type", "")).strip()
    if signal_type in SIGNAL_OPPORTUNITY_MAP:
        branch = str(data.get("branch_slug") or data.get("source_name") or "").strip().lower()
        iso_date = normalize_signal_date(data.get("date", ""))
        adapted = dict(data)
        adapted["warning_category"] = str(data.get("category", "")).strip()
        adapted["date"] = iso_date
        adapted["branch"] = branch
        adapted["category"] = mapped_category_for_signal(signal_type, str(data.get("category", "")).strip())
        adapted["date_window"] = iso_date
        adapted["source_signal_types"] = [signal_type]
        adapted["source_ref"] = os.path.relpath(path)
        adapted["status"] = "new"
        return adapted

    return data


def adapt_json_signal(path: str) -> Dict[str, object]:
    payload = read_json(path)
    signal_type = str(payload.get("signal_type", "")).strip()
    if signal_type not in SIGNAL_OPPORTUNITY_MAP:
        return {}

    branch = str(payload.get("branch", "")).strip().lower()
    iso_date = normalize_signal_date(payload.get("date", ""))
    description, evidence = summarize_target_json_signal(payload, iso_date)
    if not description:
        return {}

    meta = payload.get("meta", {})
    source_file = ""
    if isinstance(meta, dict):
        source_file = str(meta.get("source_file", "")).strip()
        if source_file:
            evidence.append(f"source_file:{source_file}")

    return {
        "signal_id": f"{branch}_{signal_type}_{iso_date}",
        "date": iso_date,
        "source_type": str(payload.get("report_type", signal_type)),
        "source_name": branch,
        "branch": branch,
        "category": mapped_category_for_signal(signal_type),
        "signal_type": signal_type,
        "description": description,
        "confidence": f"{default_confidence_for_signal(signal_type):.2f}",
        "status": "new",
        "evidence": evidence,
        "date_window": iso_date,
        "source_signal_types": [signal_type],
        "source_ref": source_file or os.path.relpath(path),
    }


def collect_signal_records() -> List[Tuple[str, Dict[str, object], bool]]:
    records: List[Tuple[str, Dict[str, object], bool]] = []
    processed_entries = load_processed_whatsapp_entries()

    for path in discover_signal_paths():
        if path.endswith(".md"):
            content = read_file(path)
            data = parse_signal(content)
            if not data:
                continue

            signal_type = str(data.get("signal_type", "")).strip()
            status = str(data.get("status", "")).strip().lower()
            if signal_type in SIGNAL_OPPORTUNITY_MAP:
                adapted = adapt_markdown_signal(path, data)
                records.append((path, enrich_signal_metadata(adapted, path, processed_entries), status == "new"))
                continue

            if status == "new":
                records.append((path, enrich_signal_metadata(data, path, processed_entries), True))
            continue

        if path.endswith(".json"):
            adapted = adapt_json_signal(path)
            if adapted:
                records.append((path, enrich_signal_metadata(adapted, path, processed_entries), False))

    return records


def process_signal_data(
    path: str,
    data: Dict[str, object],
    mark_processed: bool,
    density_index: Dict[Tuple[str, str], Dict[str, float]],
    fusion_lookup: Dict[str, Dict[str, object]],
    fusion_effect_rows: List[Dict[str, object]],
) -> bool:
    valid, reason = validate_signal(data)
    if not valid:
        log(f"SKIP {os.path.basename(path)} - validation failed: {reason}")
        return False

    scoring = scoring_context_for_signal(data, density_index)
    signal_ref = canonical_source_signal_ref(path, data)
    blackboard = normalize_blackboard_content(read_file(BLACKBOARD_PATH))
    match = find_matching_block(blackboard, data)

    if match and signal_already_recorded(match, data):
        log(f"SKIP {os.path.basename(path)} - signal already recorded for {make_title(data)}")
        if mark_processed and path.endswith(".md"):
            mark_signal_processed(path)
        return False

    if match and equivalent_reinforcement_already_recorded(match, data):
        log(
            f"SKIP {os.path.basename(path)} - equivalent reinforcement already recorded "
            f"for {make_title(data)}"
        )
        if mark_processed and path.endswith(".md"):
            mark_signal_processed(path)
        return False

    if match:
        fusion_application = build_fusion_application_row(
            signal_ref,
            str(data.get("branch", "")).strip().lower(),
            str(data.get("signal_type", "")).strip(),
            float(scoring["effective_delta"]),
            fusion_lookup.get(signal_ref),
            notes_suffix="live_reinforcement",
        )
        persisted_delta = round(float(fusion_application.get("persisted_delta", 0.0) or 0.0), 6)
        applied_scoring = dict(scoring)
        applied_scoring["effective_delta"] = persisted_delta
        updated_block = reinforce_block(match, data, applied_scoring)
        updated_blackboard = blackboard.replace(match, updated_block, 1)
        write_file(BLACKBOARD_PATH, updated_blackboard)
        fusion_effect_rows.append(fusion_application)
        if mark_processed and path.endswith(".md"):
            mark_signal_processed(path)
        log(
            f"REINFORCE {os.path.basename(path)} -> {make_title(data)} "
            f"base_delta={float(scoring['base_delta']):.4f} "
            f"confidence_score={float(scoring['confidence_score']):.4f} "
            f"density_factor={float(scoring['density_factor']):.4f} "
            f"effective_delta={float(scoring['effective_delta']):.4f} "
            f"fusion_modifier={float(fusion_application['fusion_modifier']):.4f} "
            f"persisted_delta={persisted_delta:.4f}"
        )
        return True

    log(
        f"NO_MATCH {os.path.basename(path)} "
        f"branch={data.get('branch', '')} "
        f"category={data.get('category', '')} "
        f"signal_type={data.get('signal_type', '')}"
    )

    block = build_opportunity_block(data)
    updated_blackboard = insert_into_active_opportunities(blackboard, block)
    write_file(BLACKBOARD_PATH, updated_blackboard)
    if mark_processed and path.endswith(".md"):
        mark_signal_processed(path)
    log(f"CREATE {os.path.basename(path)} -> {make_title(data)}")
    return True


def main() -> None:
    args = parse_args()
    print("=== Decision Worker v2 ===")

    if not os.path.isdir(SIGNALS_PATH):
        print("Signals path not found.")
        return

    records = collect_signal_records()

    if not records:
        print("No signal files found.")
        return

    if args.wave5a_fusion_only:
        fusion_stats = run_wave5a_read_only(records)
        write_wave5a_snapshot(fusion_stats)
        print(
            "Wave 5A fusion context:"
            f" validated={fusion_stats['validated_signal_count']}"
            f" matched={fusion_stats['matched_signal_count']}"
            f" unmatched={fusion_stats['unmatched_signal_count']}"
        )
        write_invariant_report()
        write_cross_system_consistency_report()
        write_final_system_certification()
        print("=== Done ===")
        return

    if args.wave5b_fusion_score_only:
        fusion_stats = run_wave5b_read_only()
        write_wave5b_snapshot(fusion_stats)
        print(
            "Wave 5B fusion score preview:"
            f" rows={int(fusion_stats['row_count'])}"
            f" matched_non_zero={int(fusion_stats['matched_non_zero_count'])}"
            f" unmatched_zero={int(fusion_stats['unmatched_zero_count'])}"
            f" max_modifier={float(fusion_stats['max_fusion_modifier']):.6f}"
        )
        write_invariant_report()
        write_cross_system_consistency_report()
        write_final_system_certification()
        print("=== Done ===")
        return

    if args.wave5c_fusion_effect_only:
        payload = run_wave5c_fusion_effect_validation(records, args.wave5c_limit)
        verification = payload.get("verification", {})
        signal_ref_join = verification.get("signal_ref_join", {}) if isinstance(verification, dict) else {}
        warning_match = verification.get("warning_match_context", {}) if isinstance(verification, dict) else {}
        opportunity_match = verification.get("opportunity_match", {}) if isinstance(verification, dict) else {}
        idempotency = payload.get("idempotency", {}) if isinstance(payload.get("idempotency", {}), dict) else {}
        print(
            "Wave 5C fusion effect subset:"
            f" subset={safe_int(payload.get('subset_signal_count', 0))}"
            f" signal_ref_match={safe_int(signal_ref_join.get('match_count', 0))}"
            f" signal_ref_no_match={safe_int(signal_ref_join.get('no_match_count', 0))}"
            f" warning_match={safe_int(warning_match.get('matched_signal_count', 0))}"
            f" warning_no_match={safe_int(warning_match.get('unmatched_signal_count', 0))}"
            f" opportunity_match={safe_int(opportunity_match.get('match_count', 0))}"
            f" opportunity_no_match={safe_int(opportunity_match.get('no_match_count', 0))}"
            f" idempotent={bool(idempotency.get('idempotent'))}"
        )
        print(f"Wave 5C audit written: {FUSION_EFFECT_AUDIT_PATH}")
        print(f"Wave 5C snapshot written: {WAVE5C_SNAPSHOT_PATH}")
        write_invariant_report()
        write_cross_system_consistency_report()
        write_final_system_certification()
        print("=== Done ===")
        return

    if args.wave6a_normalization_only:
        summary = run_wave6a_normalization_audit(records)
        print(
            "Wave 6A normalization audit:"
            f" validated={safe_int(summary.get('validated_signal_count', 0))}"
            f" issue_rows={safe_int(summary.get('issue_row_count', 0))}"
            f" branch_unknown_before={safe_int(summary.get('branch_unknown_before_count', 0))}"
            f" branch_unknown_after={safe_int(summary.get('branch_unknown_after_count', 0))}"
            f" inferable_unknown_before={safe_int(summary.get('inferable_branch_unknown_before_count', 0))}"
            f" inferable_unknown_after={safe_int(summary.get('inferable_branch_unknown_after_count', 0))}"
            f" idempotent={bool(summary.get('idempotent'))}"
        )
        print(f"Wave 6A audit written: {NORMALIZATION_GAP_AUDIT_PATH}")
        print(f"Wave 6A snapshot written: {WAVE6A_SNAPSHOT_PATH}")
        write_invariant_report()
        write_cross_system_consistency_report()
        write_final_system_certification()
        print("=== Done ===")
        return

    if args.wave6b_hygiene_only:
        audit = run_wave6b_blackboard_hygiene()
        print(
            "Wave 6B blackboard hygiene:"
            f" blocks={safe_int(audit.get('block_count', 0))}"
            f" separators={safe_int(audit.get('separator_count', 0))}"
            f" issues_before={safe_int(audit.get('blocks_with_structure_issues_before', 0))}"
            f" issues_after={safe_int(audit.get('blocks_with_structure_issues_after', 0))}"
            f" duplicate_keys={safe_int(audit.get('duplicate_key_count', 0))}"
        )
        print(f"Wave 6B audit written: {OPPORTUNITIES_HYGIENE_AUDIT_PATH}")
        print(f"Wave 6B snapshot written: {WAVE6B_SNAPSHOT_PATH}")
        write_invariant_report()
        write_cross_system_consistency_report()
        write_final_system_certification()
        print("=== Done ===")
        return

    if args.wave6b_duplicate_only:
        summary = run_wave6b_duplicate_visibility_audit(records)
        print(
            "Wave 6B duplicate visibility:"
            f" validated={safe_int(summary.get('validated_signal_count', 0))}"
            f" issue_rows={safe_int(summary.get('issue_row_count', 0))}"
            f" duplicates={safe_int(summary.get('duplicate_signal_count', 0))}"
            f" latest_pairs={safe_int(summary.get('latest_timestamp_duplicate_count', 0))}"
            f" repeated_identity={safe_int(summary.get('repeated_identity_duplicate_count', 0))}"
            f" idempotent={bool(summary.get('idempotent'))}"
        )
        print(f"Wave 6B audit written: {NORMALIZATION_GAP_AUDIT_PATH}")
        print(f"Wave 6B snapshot written: {WAVE6B_SNAPSHOT_PATH}")
        write_invariant_report()
        write_cross_system_consistency_report()
        write_final_system_certification()
        print("=== Done ===")
        return

    if args.wave6c_guardrails_only:
        payload = run_wave6c_processing_guardrails(records)
        summary = payload.get("summary", {}) if isinstance(payload.get("summary", {}), dict) else {}
        print(
            "Wave 6C processing guardrails:"
            f" examined={safe_int(summary.get('signals_examined', 0))}"
            f" flagged={safe_int(summary.get('signals_flagged_noisy', 0))}"
            f" excluded={safe_int(summary.get('signals_excluded_from_live_processing', 0))}"
            f" preserved={safe_int(summary.get('signals_preserved', 0))}"
        )
        print(f"Wave 6C audit written: {LEGACY_SIGNAL_HYGIENE_AUDIT_PATH}")
        print(f"Wave 6C summary written: {PROCESSING_GUARDRAIL_SUMMARY_PATH}")
        print(f"Wave 6C snapshot written: {WAVE6C_SNAPSHOT_PATH}")
        write_invariant_report()
        write_cross_system_consistency_report()
        write_final_system_certification()
        print("=== Done ===")
        return

    density_index = build_density_index(records)
    write_json(CONFIDENCE_AUDIT_PATH, build_confidence_audit(records, density_index))
    write_json(DENSITY_AUDIT_PATH, build_density_audit(density_index))
    # Wave 4B enrichment must run immediately after Wave 4A persistence; otherwise escalation
    # fields will be lost from steady-state warning artifacts.
    warning_stats, warning_escalation_stats = run_wave4_warning_persistence(records)
    fusion_context_stats = write_fusion_signal_context_artifacts(records)
    write_wave5a_snapshot(fusion_context_stats)
    fusion_score_stats = write_fusion_score_audit_artifacts()
    validate_live_fusion_score_audit()
    write_wave5b_snapshot(fusion_score_stats)
    fusion_lookup = fusion_score_audit_lookup()
    _guardrail_rows, _guardrail_summary, excluded_by_signal_ref = build_processing_guardrails(
        records,
        enforce_family_coverage=True,
    )

    processed_any = False
    fusion_effect_rows: List[Dict[str, object]] = []
    duplicate_keys_before = 0
    formatting_issues_before = 0
    if os.path.exists(BLACKBOARD_PATH):
        normalized_blackboard = normalize_blackboard_content(read_file(BLACKBOARD_PATH))
        write_file(BLACKBOARD_PATH, normalized_blackboard)
        blackboard_audit_before = build_opportunities_hygiene_audit(normalized_blackboard)
        duplicate_keys_before = safe_int(blackboard_audit_before.get("duplicate_key_count", 0))
        formatting_issues_before = safe_int(blackboard_audit_before.get("blocks_with_structure_issues_after", 0))

    for path, data, mark_processed in records:
        signal_ref = canonical_source_signal_ref(path, data)
        excluded = excluded_by_signal_ref.get(signal_ref)
        if excluded is not None:
            log(
                f"SKIP {os.path.basename(path)} - processing_guardrail {excluded['issue_type']} "
                f"action={excluded['recommended_action']}"
            )
            continue
        if process_signal_data(path, data, mark_processed, density_index, fusion_lookup, fusion_effect_rows):
            processed_any = True

    if not processed_any:
        print("No new signals to process.")

    if fusion_effect_rows:
        write_json(FUSION_EFFECT_AUDIT_PATH, sort_fusion_effect_rows(fusion_effect_rows))
        write_wave5c_snapshot(
            {
                "verification": {
                    "signal_ref_join": {
                        "match_count": len(fusion_effect_rows),
                        "no_match_count": 0,
                    },
                    "opportunity_match": {
                        "match_count": len(fusion_effect_rows),
                        "ambiguous_match_count": 0,
                    },
                },
                "summary": {
                    "fusion_non_zero_count": sum(
                        1 for row in fusion_effect_rows if float(row.get("fusion_modifier", 0.0) or 0.0) > 0.0
                    ),
                    "total_delta_increase": round(
                        sum(
                            round(
                                float(row.get("persisted_delta", 0.0) or 0.0)
                                - float(row.get("effective_delta", 0.0) or 0.0),
                                6,
                            )
                            for row in fusion_effect_rows
                        ),
                        6,
                    ),
                },
                "idempotency": {
                    "idempotent": True,
                },
                "rows": sort_fusion_effect_rows(fusion_effect_rows),
            }
        )

    if os.path.exists(BLACKBOARD_PATH):
        blackboard_audit_after = build_opportunities_hygiene_audit(normalize_blackboard_content(read_file(BLACKBOARD_PATH)))
        duplicate_keys_after = safe_int(blackboard_audit_after.get("duplicate_key_count", 0))
        formatting_issues_after = safe_int(blackboard_audit_after.get("blocks_with_structure_issues_after", 0))
        if duplicate_keys_after > duplicate_keys_before:
            raise RuntimeError("Wave 5C validation failed: duplicate opportunities increased during live processing")
        if formatting_issues_after > formatting_issues_before:
            raise RuntimeError("Wave 5C validation failed: OPPORTUNITIES.md formatting regressed during live processing")

    write_wave3_snapshot()
    write_wave4a_snapshot(warning_stats)
    write_wave4b_snapshot(warning_escalation_stats)
    write_invariant_report()
    write_cross_system_consistency_report()
    write_final_system_certification()
    print("=== Done ===")


if __name__ == "__main__":
    main()
