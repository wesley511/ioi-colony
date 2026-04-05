from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import colony_cycle


def write_wave4_artifacts(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    warning_intelligence = {
        "waigani|performance_gap|monitoring": {
            "pattern_id": "waigani|performance_gap|monitoring",
            "branch": "waigani",
            "signal_type": "performance_gap",
            "section": "monitoring",
            "category": "operations",
            "first_seen": "2026-03-24",
            "last_seen": "2026-03-27",
            "occurrence_count": 3,
            "distinct_dates": ["2026-03-24", "2026-03-25", "2026-03-27"],
            "consecutive_days": 1,
            "time_span_days": 3,
            "source_signals": ["SIGNALS/normalized/waigani_monitoring_gap_2026-03-27_163003.md"],
            "notes": "",
            "severity_score": 0.459048,
            "escalation_level": "watch",
            "escalation_reason": "repeated occurrences across multiple days",
        }
    }
    warning_pattern_audit = [
        {
            "pattern_id": "waigani|performance_gap|monitoring",
            "branch": "waigani",
            "signal_type": "performance_gap",
            "section": "monitoring",
            "category": "operations",
            "occurrence_count": 3,
            "distinct_dates": ["2026-03-24", "2026-03-25", "2026-03-27"],
            "consecutive_days": 1,
            "time_span_days": 3,
            "source_signal_count": 3,
            "notes": "",
            "severity_score": 0.459048,
            "escalation_level": "watch",
        }
    ]
    (data_dir / "warning_intelligence.json").write_text(
        json.dumps(warning_intelligence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (data_dir / "warning_pattern_audit.json").write_text(
        json.dumps(warning_pattern_audit, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


class ColonyCycleWave4Tests(unittest.TestCase):
    def test_validate_wave4_warning_artifacts_rejects_missing_wave4b_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            data_dir = base_dir / "DATA"
            write_wave4_artifacts(data_dir)

            intelligence_path = data_dir / "warning_intelligence.json"
            payload = json.loads(intelligence_path.read_text(encoding="utf-8"))
            del payload["waigani|performance_gap|monitoring"]["escalation_reason"]
            intelligence_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            with mock.patch.multiple(
                colony_cycle,
                BASE_DIR=base_dir,
                DATA_DIR=data_dir,
            ):
                with self.assertRaisesRegex(RuntimeError, "escalation_reason"):
                    colony_cycle.validate_wave4_warning_artifacts()

    def test_main_preserves_wave4b_fields_through_cycle_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            reports_dir = base_dir / "REPORTS"
            logs_dir = base_dir / "LOGS"
            staff_memory_dir = base_dir / "COLONY_MEMORY" / "staff_signals"
            signals_dir = base_dir / "SIGNALS" / "normalized"
            raw_whatsapp_dir = base_dir / "RAW_INPUT" / "whatsapp" / "accepted"
            data_dir = base_dir / "DATA"
            health_file = base_dir / "HEALTH.md"
            alerts_file = base_dir / "ALERTS.md"
            anomalies_file = base_dir / "ANOMALIES.md"
            cycle_log = logs_dir / "colony_cycle.log"

            def fake_reinforcement_stage() -> None:
                write_wave4_artifacts(data_dir)

            def fake_staff_analyzer(_ts: str) -> Path:
                reports_dir.mkdir(parents=True, exist_ok=True)
                advisory_path = reports_dir / "advisory_test.md"
                advisory_path.write_text("# advisory\n", encoding="utf-8")
                return advisory_path

            def fake_fusion_analyzer(_ts: str) -> Path:
                reports_dir.mkdir(parents=True, exist_ok=True)
                fusion_path = reports_dir / "fusion_test.md"
                fusion_path.write_text("# fusion\n", encoding="utf-8")
                return fusion_path

            with mock.patch.multiple(
                colony_cycle,
                BASE_DIR=base_dir,
                REPORTS_DIR=reports_dir,
                LOGS_DIR=logs_dir,
                STAFF_MEMORY_DIR=staff_memory_dir,
                SIGNALS_DIR=signals_dir,
                RAW_WHATSAPP_ACCEPTED_DIR=raw_whatsapp_dir,
                DATA_DIR=data_dir,
                HEALTH_FILE=health_file,
                ALERTS_FILE=alerts_file,
                ANOMALIES_FILE=anomalies_file,
                CYCLE_LOG=cycle_log,
            ):
                with mock.patch.object(colony_cycle, "run_sales_ingestion"), mock.patch.object(
                    colony_cycle, "run_whatsapp_processor"
                ), mock.patch.object(colony_cycle, "run_whatsapp_audit"), mock.patch.object(
                    colony_cycle, "emit_staff_signals"
                ), mock.patch.object(
                    colony_cycle, "run_decision_signal_generator"
                ), mock.patch.object(
                    colony_cycle, "run_reinforcement_stage", side_effect=fake_reinforcement_stage
                ), mock.patch.object(
                    colony_cycle, "run_decay_stage"
                ), mock.patch.object(
                    colony_cycle, "write_observability"
                ), mock.patch.object(
                    colony_cycle, "run_staff_analyzer", side_effect=fake_staff_analyzer
                ), mock.patch.object(
                    colony_cycle, "run_fusion_analyzer", side_effect=fake_fusion_analyzer
                ):
                    self.assertEqual(colony_cycle.main(), 0)

            warning_intelligence = json.loads((data_dir / "warning_intelligence.json").read_text(encoding="utf-8"))
            warning_pattern_audit = json.loads((data_dir / "warning_pattern_audit.json").read_text(encoding="utf-8"))

            entry = warning_intelligence["waigani|performance_gap|monitoring"]
            self.assertIn("severity_score", entry)
            self.assertIn("escalation_level", entry)
            self.assertIn("escalation_reason", entry)
            self.assertIn("severity_score", warning_pattern_audit[0])
            self.assertIn("escalation_level", warning_pattern_audit[0])

            log_text = cycle_log.read_text(encoding="utf-8")
            self.assertIn("WAVE4_VALIDATED", log_text)


if __name__ == "__main__":
    unittest.main()
