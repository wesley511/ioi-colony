#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
REPORTS_DIR = BASE_DIR / "REPORTS"
LOGS_DIR = BASE_DIR / "LOGS"
STAFF_MEMORY_DIR = BASE_DIR / "COLONY_MEMORY" / "staff_signals"
SIGNALS_DIR = BASE_DIR / "SIGNALS" / "normalized"
RAW_WHATSAPP_ACCEPTED_DIR = BASE_DIR / "RAW_INPUT" / "whatsapp" / "accepted"
DATA_DIR = BASE_DIR / "DATA"

HEALTH_FILE = BASE_DIR / "HEALTH.md"
ALERTS_FILE = BASE_DIR / "ALERTS.md"
ANOMALIES_FILE = BASE_DIR / "ANOMALIES.md"
CYCLE_LOG = LOGS_DIR / "colony_cycle.log"

POST_PREFIX = "[IOI Colony Cycle]"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def log_event(message: str) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    with CYCLE_LOG.open("a", encoding="utf-8") as fh:
        fh.write(f"{utc_now().isoformat()} - {message}\n")


def require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def run_command(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd or BASE_DIR),
        text=True,
        capture_output=True,
        check=False,
    )


def print_output(result: subprocess.CompletedProcess[str]) -> None:
    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)


def list_recent_report(report_glob: str) -> list[Path]:
    return sorted(REPORTS_DIR.glob(report_glob))


def count_active_opportunities() -> tuple[int, int, int]:
    active_count = 0
    archived_count = 0
    priority_count = 0

    opportunities_file = BASE_DIR / "OPPORTUNITIES.md"
    priority_file = BASE_DIR / "PRIORITY.md"

    if opportunities_file.exists():
        text = opportunities_file.read_text(encoding="utf-8", errors="ignore")
        active_count = text.count("## ")
        archived_count = text.lower().count("archived")

    if priority_file.exists():
        text = priority_file.read_text(encoding="utf-8", errors="ignore")
        priority_count = text.count("## ")

    return active_count, archived_count, priority_count


def tail_lines(path: Path, count: int = 400) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="ignore").splitlines()[-count:]


def extract_last_event(lines: list[str], marker: str) -> str:
    for line in reversed(lines):
        if marker in line:
            return line
    return "none"


def count_recent_events(lines: list[str], marker: str) -> int:
    return sum(1 for line in lines if marker in line)


def write_health(advisory_path: Path, fusion_path: Path) -> None:
    active_count, archived_count, priority_count = count_active_opportunities()
    recent_lines = tail_lines(CYCLE_LOG, 500)

    latest_reinforce = extract_last_event(recent_lines, "REINFORCE")
    latest_archive = extract_last_event(recent_lines, "ARCHIVE")
    latest_normalize = extract_last_event(recent_lines, "NORMALIZE")

    content = [
        "# IOI Colony Health Summary",
        "",
        f"- generated_at: {utc_now().isoformat()}",
        f"- active_count: {active_count}",
        f"- archived_count: {archived_count}",
        f"- priority_count: {priority_count}",
        "",
        "## Latest Lifecycle Events",
        "",
        f"- latest_reinforce: {latest_reinforce}",
        f"- latest_archive: {latest_archive}",
        f"- latest_normalize: {latest_normalize}",
        "",
        "## Latest Outputs",
        "",
        f"- advisory_report: {advisory_path}",
        f"- fusion_report: {fusion_path}",
        f"- status: cycle_complete",
        "",
    ]
    HEALTH_FILE.write_text("\n".join(content), encoding="utf-8")
    log_event(
        f"HEALTH_UPDATE active={active_count} archived={archived_count} priority={priority_count}"
    )


def write_alerts() -> None:
    recent_lines = tail_lines(CYCLE_LOG, 500)
    alerts: list[str] = []

    recent_reinforce_count = count_recent_events(recent_lines, "REINFORCE")
    if recent_reinforce_count == 0:
        alerts.append("LOW: no recent reinforcement events in recent log window")

    if not RAW_WHATSAPP_ACCEPTED_DIR.exists():
        alerts.append("MEDIUM: whatsapp accepted directory missing")

    processed_state = DATA_DIR / "processed_accepted_whatsapp.json"
    if not processed_state.exists():
        alerts.append("LOW: processed_accepted_whatsapp.json missing")

    pipeline_audit = DATA_DIR / "whatsapp_pipeline_audit.json"
    if pipeline_audit.exists():
        try:
            import json

            payload = json.loads(pipeline_audit.read_text(encoding="utf-8"))
            missing = payload.get("missing_staff_branches") or []
            latest_date = payload.get("latest_staff_report_date") or "unknown"
            if missing:
                alerts.append(
                    f"MEDIUM: missing daily staff reports for {latest_date}: {', '.join(missing)}"
                )
        except Exception:
            alerts.append("LOW: whatsapp_pipeline_audit.json unreadable")

    content = [
        "# IOI Colony Alerts",
        "",
        f"- generated_at: {utc_now().isoformat()}",
        f"- alert_count: {len(alerts)}",
        "",
        "## Active Alerts",
        "",
    ]
    if alerts:
        content.extend(f"- {alert}" for alert in alerts)
    else:
        content.append("- none")
    content.append("")

    ALERTS_FILE.write_text("\n".join(content), encoding="utf-8")


def write_anomalies() -> None:
    active_count, archived_count, priority_count = count_active_opportunities()
    recent_lines = tail_lines(CYCLE_LOG, 500)

    recent_error_count = count_recent_events(recent_lines, "ERROR")
    recent_archive_count = count_recent_events(recent_lines, "ARCHIVE")
    recent_reinforce_count = count_recent_events(recent_lines, "REINFORCE")
    cron_missing_end_count = 0

    anomalies: list[str] = []

    if recent_reinforce_count == 0:
        anomalies.append("no_recent_reinforcement")
    if recent_error_count > 0:
        anomalies.append("recent_errors_present")

    content = [
        "# IOI Colony Anomaly Summary",
        "",
        f"- generated_at: {utc_now().isoformat()}",
        f"- anomaly_count: {len(anomalies)}",
        "",
        "## Metrics",
        "",
        f"- active_count: {active_count}",
        f"- archived_count: {archived_count}",
        f"- priority_count: {priority_count}",
        f"- recent_error_count: {recent_error_count}",
        f"- recent_archive_count: {recent_archive_count}",
        f"- recent_reinforce_count: {recent_reinforce_count}",
        f"- cron_missing_end_count: {cron_missing_end_count}",
        "",
        "## Detected Anomalies",
        "",
    ]
    if anomalies:
        content.extend(f"- {item}" for item in anomalies)
    else:
        content.append("- none")
    content.append("")

    ANOMALIES_FILE.write_text("\n".join(content), encoding="utf-8")


def run_sales_ingestion() -> None:
    parser = BASE_DIR / "scripts" / "ingest_whatsapp_sales_batch.py"
    if not parser.exists():
        print(f"{POST_PREFIX} Skipping: batch sales ingestion (script not found)")
        return

    print(f"{POST_PREFIX} Running: batch sales ingestion")
    result = run_command([sys.executable, str(parser)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Sales ingestion failed with exit code {result.returncode}")


def run_whatsapp_processor() -> None:
    processor = BASE_DIR / "scripts" / "process_accepted_whatsapp.py"
    if not processor.exists():
        print(f"{POST_PREFIX} Skipping: accepted WhatsApp processor (script not found)")
        return

    print(f"{POST_PREFIX} Running: accepted WhatsApp processor")
    result = run_command([sys.executable, str(processor)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Accepted WhatsApp processor failed with exit code {result.returncode}")


def run_whatsapp_audit() -> None:
    auditor = BASE_DIR / "scripts" / "whatsapp_pipeline_audit.py"
    if not auditor.exists():
        print(f"{POST_PREFIX} Skipping: whatsapp pipeline audit (script not found)")
        return

    print(f"{POST_PREFIX} Running: whatsapp pipeline audit")
    result = run_command([sys.executable, str(auditor)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"WhatsApp pipeline audit failed with exit code {result.returncode}")


def emit_staff_signals() -> None:
    emitter = BASE_DIR / "generate_staff_refs.py"
    if not emitter.exists():
        print(f"{POST_PREFIX} Skipping: staff signal emitter (script not found)")
        return

    result = run_command([sys.executable, str(emitter)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Staff signal emitter failed with exit code {result.returncode}")

    emitted_count = len(list(STAFF_MEMORY_DIR.rglob("*"))) if STAFF_MEMORY_DIR.exists() else 0
    print(f"{POST_PREFIX} Emitted {emitted_count} staff signal(s) to colony memory")


def run_decision_signal_generator() -> None:
    generator = BASE_DIR / "scripts" / "generate_decision_signals.py"
    require_file(generator, "Decision signal generator")

    print(f"{POST_PREFIX} Running: decision signal generator")
    result = run_command([sys.executable, str(generator)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Decision signal generator failed with exit code {result.returncode}")


def run_reinforcement_stage() -> None:
    worker = BASE_DIR / "worker_decision_v2.py"
    if not worker.exists():
        print(f"{POST_PREFIX} Skipping: reinforcement stage (script not found)")
        return

    print(f"{POST_PREFIX} Running: reinforcement stage")
    result = run_command([sys.executable, str(worker)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Reinforcement stage failed with exit code {result.returncode}")


def run_decay_stage() -> None:
    worker = BASE_DIR / "decay_worker.py"
    if not worker.exists():
        print(f"{POST_PREFIX} Skipping: decay stage (script not found)")
        return

    print(f"{POST_PREFIX} Running: decay stage")
    result = run_command([sys.executable, str(worker)])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Decay stage failed with exit code {result.returncode}")


def run_staff_analyzer(ts: str) -> Path:
    print(f"{POST_PREFIX} Running: staff colony analyzer")
    result = run_command([sys.executable, "-m", "scripts.colony_analyzer"])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Staff analyzer failed with exit code {result.returncode}")

    reports = list_recent_report("advisory_*.md")
    if not reports:
        raise RuntimeError("No advisory report produced")
    return reports[-1]


def run_fusion_analyzer(ts: str) -> Path:
    print(f"{POST_PREFIX} Running: fusion analyzer")
    result = run_command([sys.executable, "-m", "scripts.colony_fusion_analyzer"])
    print_output(result)

    if result.returncode != 0:
        raise RuntimeError(f"Fusion analyzer failed with exit code {result.returncode}")

    reports = list_recent_report("fusion_*.md")
    if not reports:
        raise RuntimeError("No fusion report produced")
    return reports[-1]


def write_observability(advisory_path: Path, fusion_path: Path) -> None:
    write_health(advisory_path, fusion_path)
    write_alerts()
    write_anomalies()


def main() -> int:
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        STAFF_MEMORY_DIR.mkdir(parents=True, exist_ok=True)
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        ts = now_timestamp()

        run_sales_ingestion()
        run_whatsapp_processor()
        run_whatsapp_audit()

        print(f"{POST_PREFIX} Running: staff signal emitter")
        emit_staff_signals()

        run_decision_signal_generator()
        run_reinforcement_stage()
        run_decay_stage()

        advisory_path = run_staff_analyzer(ts)
        fusion_path = run_fusion_analyzer(ts)

        write_observability(advisory_path, fusion_path)

        print(f"{POST_PREFIX} Cycle complete")
        print(f"{POST_PREFIX} Advisory: {advisory_path}")
        print(f"{POST_PREFIX} Fusion: {fusion_path}")
        print(f"{POST_PREFIX} Timestamp: {datetime.now()}")
        return 0

    except Exception as exc:
        print(f"{POST_PREFIX} ERROR: {exc}", file=sys.stderr)
        log_event(f"ERROR {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
