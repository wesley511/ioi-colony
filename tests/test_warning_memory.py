from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import worker_decision_v2 as worker


def make_record(
    *,
    path: str,
    signal_id: str,
    date: str,
    source_name: str,
    category: str,
    signal_type: str,
    description: str,
    confidence: str = "0.74",
    extra: dict[str, object] | None = None,
) -> tuple[str, dict[str, object], bool]:
    payload: dict[str, object] = {
        "signal_id": signal_id,
        "date": date,
        "source_type": "advisory_report",
        "source_name": source_name,
        "category": category,
        "signal_type": signal_type,
        "description": description,
        "confidence": confidence,
        "status": "new",
    }
    if extra:
        payload.update(extra)
    return path, payload, False


class WarningMemoryTests(unittest.TestCase):
    def test_pattern_key_uses_inferred_section_when_available(self) -> None:
        path, data, _ = make_record(
            path="SIGNALS/normalized/waigani_assisting_cashier_gap_2026-03-24_163003.md",
            signal_id="waigani_assisting_cashier_gap_2026-03-24_163003",
            date="2026-03-24",
            source_name="waigani",
            category="operations",
            signal_type="performance_gap",
            description="WAIGANI shows a weak operational section: assisting_cashier.",
        )

        pattern_id, branch, signal_type, section, category = worker.build_warning_pattern_key(path, data)

        self.assertEqual(branch, "waigani")
        self.assertEqual(signal_type, "performance_gap")
        self.assertEqual(section, "assisting_cashier")
        self.assertEqual(category, "operations")
        self.assertEqual(pattern_id, "waigani|performance_gap|assisting_cashier")

    def test_pattern_key_falls_back_to_category_for_branch_level_signal(self) -> None:
        path, data, _ = make_record(
            path="SIGNALS/normalized/lae_malaita_branch_gap_2026-03-24_163003.md",
            signal_id="lae_malaita_branch_gap_2026-03-24_163003",
            date="2026-03-24",
            source_name="lae_malaita",
            category="branch_performance",
            signal_type="performance_gap",
            description="LAE_MALAITA is the current weakest branch in the fusion report and needs review.",
        )

        pattern_id, branch, signal_type, section, category = worker.build_warning_pattern_key(path, data)

        self.assertEqual(branch, "lae_malaita")
        self.assertEqual(signal_type, "performance_gap")
        self.assertEqual(section, "")
        self.assertEqual(category, "branch_performance")
        self.assertEqual(pattern_id, "lae_malaita|performance_gap|branch_performance")

    def test_warning_memory_accumulates_dates_and_dedupes_replays(self) -> None:
        records = [
            make_record(
                path="SIGNALS/normalized/waigani_assisting_cashier_gap_2026-03-24_163003.md",
                signal_id="waigani_assisting_cashier_gap_2026-03-24_163003",
                date="2026-03-24",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: assisting_cashier.",
            ),
            make_record(
                path="SIGNALS/normalized/waigani_assisting_cashier_gap_latest.md",
                signal_id="waigani_assisting_cashier_gap_latest",
                date="2026-03-24",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: assisting_cashier.",
            ),
            make_record(
                path="SIGNALS/normalized/waigani_assisting_cashier_gap_2026-03-25_011728.md",
                signal_id="waigani_assisting_cashier_gap_2026-03-25_011728",
                date="2026-03-25",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: assisting_cashier.",
            ),
        ]

        warning_intelligence, audit_rows, stats = worker.build_warning_memory(records)
        entry = warning_intelligence["waigani|performance_gap|assisting_cashier"]

        self.assertEqual(entry["occurrence_count"], 2)
        self.assertEqual(entry["distinct_dates"], ["2026-03-24", "2026-03-25"])
        self.assertEqual(entry["first_seen"], "2026-03-24")
        self.assertEqual(entry["last_seen"], "2026-03-25")
        self.assertEqual(entry["consecutive_days"], 2)
        self.assertEqual(entry["time_span_days"], 1)
        self.assertNotIn("severity_score", entry)
        self.assertNotIn("escalation_level", entry)
        self.assertNotIn("escalation_reason", entry)
        self.assertEqual(len(audit_rows), 1)
        self.assertEqual(audit_rows[0]["source_signal_count"], 2)
        self.assertNotIn("severity_score", audit_rows[0])
        self.assertNotIn("escalation_level", audit_rows[0])
        self.assertEqual(stats["logical_event_count"], 2)

    def test_warning_memory_rebuild_is_idempotent(self) -> None:
        records = [
            make_record(
                path="SIGNALS/normalized/waigani_monitoring_gap_2026-03-24_163003.md",
                signal_id="waigani_monitoring_gap_2026-03-24_163003",
                date="2026-03-24",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: monitoring.",
            ),
            make_record(
                path="SIGNALS/normalized/waigani_monitoring_gap_2026-03-25_163003.md",
                signal_id="waigani_monitoring_gap_2026-03-25_163003",
                date="2026-03-25",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: monitoring.",
            ),
            make_record(
                path="SIGNALS/normalized/waigani_monitoring_gap_2026-03-27_163003.md",
                signal_id="waigani_monitoring_gap_2026-03-27_163003",
                date="2026-03-27",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: monitoring.",
            ),
        ]

        first_entries, first_audit, first_stats = worker.build_warning_memory(records)
        second_entries, second_audit, second_stats = worker.build_warning_memory(records, first_entries)
        entry = second_entries["waigani|performance_gap|monitoring"]

        self.assertEqual(first_entries, second_entries)
        self.assertEqual(first_audit, second_audit)
        self.assertEqual(first_stats, second_stats)
        self.assertEqual(entry["occurrence_count"], 3)
        self.assertEqual(entry["distinct_dates"], ["2026-03-24", "2026-03-25", "2026-03-27"])
        self.assertEqual(entry["consecutive_days"], 1)
        self.assertEqual(entry["time_span_days"], 3)

    def test_warning_escalation_severity_increases_with_metrics(self) -> None:
        base = worker.warning_severity_score(1, 1, 0)
        more_occurrences = worker.warning_severity_score(3, 1, 0)
        more_consecutive_days = worker.warning_severity_score(3, 2, 0)
        longer_span = worker.warning_severity_score(3, 2, 5)

        self.assertGreater(more_occurrences, base)
        self.assertGreater(more_consecutive_days, more_occurrences)
        self.assertGreater(longer_span, more_consecutive_days)

    def test_warning_escalation_enrichment_is_idempotent(self) -> None:
        entries = {
            "waigani|performance_gap|monitoring": {
                "pattern_id": "waigani|performance_gap|monitoring",
                "branch": "waigani",
                "signal_type": "performance_gap",
                "section": "monitoring",
                "category": "operations",
                "first_seen": "2026-03-24",
                "last_seen": "2026-03-31",
                "occurrence_count": 4,
                "distinct_dates": ["2026-03-24", "2026-03-25", "2026-03-30", "2026-03-31"],
                "consecutive_days": 2,
                "time_span_days": 7,
                "source_signals": ["SIGNALS/normalized/waigani_monitoring_gap_latest.md"],
                "notes": "",
            }
        }
        audit_rows = [
            {
                "pattern_id": "waigani|performance_gap|monitoring",
                "branch": "waigani",
                "signal_type": "performance_gap",
                "section": "monitoring",
                "category": "operations",
                "occurrence_count": 4,
                "distinct_dates": ["2026-03-24", "2026-03-25", "2026-03-30", "2026-03-31"],
                "consecutive_days": 2,
                "time_span_days": 7,
                "source_signal_count": 4,
                "notes": "",
            }
        ]

        first_entries, first_audit, first_summary = worker.enrich_warning_memory_state(entries, audit_rows)
        second_entries, second_audit, second_summary = worker.enrich_warning_memory_state(first_entries, first_audit)

        self.assertEqual(first_entries, second_entries)
        self.assertEqual(first_audit, second_audit)
        self.assertEqual(first_summary, second_summary)
        self.assertEqual(first_entries["waigani|performance_gap|monitoring"]["severity_score"], 0.786667)
        self.assertEqual(first_entries["waigani|performance_gap|monitoring"]["escalation_level"], "elevated")

    def test_main_persists_wave4b_warning_fields_and_is_idempotent(self) -> None:
        records = [
            make_record(
                path="SIGNALS/normalized/waigani_monitoring_gap_2026-03-24_163003.md",
                signal_id="waigani_monitoring_gap_2026-03-24_163003",
                date="2026-03-24",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: monitoring.",
                extra={"branch": "waigani", "date_window": "2026-03-24", "source_signal_types": ["performance_gap"]},
            ),
            make_record(
                path="SIGNALS/normalized/waigani_monitoring_gap_2026-03-25_163003.md",
                signal_id="waigani_monitoring_gap_2026-03-25_163003",
                date="2026-03-25",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: monitoring.",
                extra={"branch": "waigani", "date_window": "2026-03-25", "source_signal_types": ["performance_gap"]},
            ),
            make_record(
                path="SIGNALS/normalized/waigani_monitoring_gap_2026-03-27_163003.md",
                signal_id="waigani_monitoring_gap_2026-03-27_163003",
                date="2026-03-27",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: monitoring.",
                extra={"branch": "waigani", "date_window": "2026-03-27", "source_signal_types": ["performance_gap"]},
            ),
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            signals_path = tmp_path / "SIGNALS" / "normalized"
            data_path = tmp_path / "DATA"
            checkpoints_path = data_path / "checkpoints"
            signals_path.mkdir(parents=True)
            checkpoints_path.mkdir(parents=True)

            blackboard_path = tmp_path / "OPPORTUNITIES.md"
            blackboard_text = "# IOI Colony Opportunities Blackboard\n\n## Active Opportunities\n"
            blackboard_path.write_text(blackboard_text, encoding="utf-8")

            patched_paths = {
                "SIGNALS_PATH": str(signals_path),
                "BLACKBOARD_PATH": str(blackboard_path),
                "LOG_PATH": str(tmp_path / "LOGS" / "worker.log"),
                "DATA_DIR": str(data_path),
                "CHECKPOINTS_DIR": str(checkpoints_path),
                "CONFIDENCE_AUDIT_PATH": str(data_path / "confidence_scoring_audit.json"),
                "DENSITY_AUDIT_PATH": str(data_path / "signal_density_audit.json"),
                "WAVE3_SNAPSHOT_PATH": str(checkpoints_path / "wave3_snapshot.json"),
                "WARNING_INTELLIGENCE_PATH": str(data_path / "warning_intelligence.json"),
                "WARNING_PATTERN_AUDIT_PATH": str(data_path / "warning_pattern_audit.json"),
                "WAVE4A_SNAPSHOT_PATH": str(checkpoints_path / "wave4a_snapshot.json"),
                "WAVE4B_SNAPSHOT_PATH": str(checkpoints_path / "wave4b_snapshot.json"),
            }

            with mock.patch.multiple(worker, **patched_paths):
                with mock.patch.object(worker, "collect_signal_records", return_value=records):
                    with mock.patch.object(worker, "process_signal_data", return_value=False):
                        with mock.patch.object(sys, "argv", ["worker_decision_v2.py"]):
                            worker.main()

                        intelligence_path = Path(worker.WARNING_INTELLIGENCE_PATH)
                        audit_path = Path(worker.WARNING_PATTERN_AUDIT_PATH)
                        wave3_snapshot_path = Path(worker.WAVE3_SNAPSHOT_PATH)
                        wave4a_snapshot_path = Path(worker.WAVE4A_SNAPSHOT_PATH)
                        wave4b_snapshot_path = Path(worker.WAVE4B_SNAPSHOT_PATH)

                        intelligence_first = json.loads(intelligence_path.read_text(encoding="utf-8"))
                        audit_first = json.loads(audit_path.read_text(encoding="utf-8"))
                        intelligence_hash_first = intelligence_path.read_text(encoding="utf-8")
                        audit_hash_first = audit_path.read_text(encoding="utf-8")
                        wave3_snapshot_first = wave3_snapshot_path.read_text(encoding="utf-8")
                        wave4a_snapshot_first = wave4a_snapshot_path.read_text(encoding="utf-8")
                        wave4b_snapshot_first = wave4b_snapshot_path.read_text(encoding="utf-8")

                        entry = intelligence_first["waigani|performance_gap|monitoring"]
                        self.assertNotIn("severity_score", worker.build_warning_memory(records)[0]["waigani|performance_gap|monitoring"])
                        self.assertEqual(entry["severity_score"], 0.459048)
                        self.assertEqual(entry["escalation_level"], "watch")
                        self.assertEqual(entry["escalation_reason"], "repeated occurrences across multiple days")
                        self.assertEqual(audit_first[0]["severity_score"], 0.459048)
                        self.assertEqual(audit_first[0]["escalation_level"], "watch")
                        self.assertNotIn("escalation_reason", audit_first[0])
                        self.assertTrue(wave4b_snapshot_path.exists())
                        self.assertEqual(blackboard_path.read_text(encoding="utf-8"), blackboard_text)

                        with mock.patch.object(sys, "argv", ["worker_decision_v2.py"]):
                            worker.main()

                        intelligence_second = intelligence_path.read_text(encoding="utf-8")
                        audit_second = audit_path.read_text(encoding="utf-8")
                        self.assertEqual(intelligence_hash_first, intelligence_second)
                        self.assertEqual(audit_hash_first, audit_second)
                        self.assertEqual(wave3_snapshot_first, wave3_snapshot_path.read_text(encoding="utf-8"))
                        self.assertEqual(wave4a_snapshot_first, wave4a_snapshot_path.read_text(encoding="utf-8"))
                        self.assertEqual(wave4b_snapshot_first, wave4b_snapshot_path.read_text(encoding="utf-8"))
                        self.assertEqual(blackboard_path.read_text(encoding="utf-8"), blackboard_text)


if __name__ == "__main__":
    unittest.main()
