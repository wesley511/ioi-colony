from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import worker_decision_v2 as worker


def make_record(
    *,
    path: str,
    signal_id: str,
    date: str,
    source_name: str,
    branch: str,
    category: str,
    signal_type: str,
    description: str,
) -> tuple[str, dict[str, object], bool]:
    payload: dict[str, object] = {
        "signal_id": signal_id,
        "date": date,
        "date_window": date,
        "source_type": "advisory_report",
        "source_name": source_name,
        "branch": branch,
        "category": category,
        "signal_type": signal_type,
        "description": description,
        "confidence": "0.74",
        "status": "new",
        "source_signal_types": [signal_type],
    }
    return path, payload, False


class ProcessingGuardrailTests(unittest.TestCase):
    def test_unknown_staff_artifact_is_excluded(self) -> None:
        record = make_record(
            path="SIGNALS/normalized/waigani_staff_waigani_unknown_staff_strength_2026-04-01_094026.md",
            signal_id="waigani_staff_waigani_unknown_staff_strength_2026-04-01_094026",
            date="2026-04-03",
            source_name="waigani",
            branch="waigani",
            category="performance_strength",
            signal_type="strong_performance",
            description="staff-waigani-unknown_staff in WAIGANI is identified as a top performer with advisory strength score 1808.80.",
        )

        audit_rows, summary, excluded = worker.build_processing_guardrails([record])

        self.assertEqual(summary["signals_examined"], 1)
        self.assertEqual(summary["signals_excluded_from_live_processing"], 1)
        self.assertEqual(audit_rows[0]["issue_type"], "unknown_staff_artifact")
        self.assertIn("waigani_staff_waigani_unknown_staff_strength_2026-04-01_094026", excluded)

    def test_latest_timestamp_pair_keeps_single_canonical_live_signal(self) -> None:
        latest = make_record(
            path="SIGNALS/normalized/5th_street_branch_gap_latest.md",
            signal_id="5th_street_branch_gap_latest",
            date="2026-04-03",
            source_name="lae_5th_street",
            branch="lae_5th_street",
            category="performance_issue",
            signal_type="performance_gap",
            description="5TH_STREET is the current weakest branch in the fusion report and needs review.",
        )
        stamped = make_record(
            path="SIGNALS/normalized/5th_street_branch_gap_2026-04-03_140536.md",
            signal_id="5th_street_branch_gap_2026-04-03_140536",
            date="2026-04-03",
            source_name="lae_5th_street",
            branch="lae_5th_street",
            category="performance_issue",
            signal_type="performance_gap",
            description="5TH_STREET is the current weakest branch in the fusion report and needs review.",
        )

        audit_rows, summary, excluded = worker.build_processing_guardrails([latest, stamped])

        self.assertEqual(summary["signals_examined"], 2)
        self.assertEqual(summary["signals_excluded_from_live_processing"], 1)
        self.assertIn("5th_street_branch_gap_2026-04-03_140536", excluded)
        self.assertNotIn("5th_street_branch_gap_latest", excluded)
        self.assertEqual(audit_rows[0]["issue_type"], "latest_timestamp_duplicate_pair")

    def test_same_day_replay_cluster_without_latest_keeps_one_canonical_signal(self) -> None:
        first = make_record(
            path="SIGNALS/normalized/lae_malaita_branch_gap_2026-03-24_163003.md",
            signal_id="lae_malaita_branch_gap_2026-03-24_163003",
            date="2026-03-24",
            source_name="lae_malaita",
            branch="lae_malaita",
            category="performance_issue",
            signal_type="performance_gap",
            description="LAE_MALAITA is the current weakest branch in the fusion report and needs review.",
        )
        second = make_record(
            path="SIGNALS/normalized/lae_malaita_branch_gap_2026-03-24_173003.md",
            signal_id="lae_malaita_branch_gap_2026-03-24_173003",
            date="2026-03-24",
            source_name="lae_malaita",
            branch="lae_malaita",
            category="performance_issue",
            signal_type="performance_gap",
            description="LAE_MALAITA is the current weakest branch in the fusion report and needs review.",
        )

        audit_rows, summary, excluded = worker.build_processing_guardrails([first, second])

        self.assertEqual(summary["signals_examined"], 2)
        self.assertEqual(summary["signals_excluded_from_live_processing"], 1)
        self.assertEqual(audit_rows[0]["issue_type"], "same_day_semantic_replay_duplicate")
        self.assertIn("lae_malaita_branch_gap_2026-03-24_163003", excluded)

    def test_wave6c_guardrail_run_is_read_only_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            blackboard_path = tmp_path / "OPPORTUNITIES.md"
            blackboard_path.write_text("## Active Opportunities\n", encoding="utf-8")

            audit_path = tmp_path / "legacy_signal_hygiene_audit.json"
            summary_path = tmp_path / "processing_guardrail_summary.json"
            snapshot_path = tmp_path / "wave6c_snapshot.json"

            protected = {
                "BLACKBOARD_PATH": blackboard_path,
                "CONFIDENCE_AUDIT_PATH": tmp_path / "confidence.json",
                "DENSITY_AUDIT_PATH": tmp_path / "density.json",
                "WARNING_INTELLIGENCE_PATH": tmp_path / "warning_intelligence.json",
                "WARNING_PATTERN_AUDIT_PATH": tmp_path / "warning_pattern_audit.json",
                "FUSION_SIGNAL_CONTEXT_PATH": tmp_path / "fusion_signal_context.json",
                "FUSION_SCORE_AUDIT_PATH": tmp_path / "fusion_score_audit.json",
                "FUSION_EFFECT_AUDIT_PATH": tmp_path / "fusion_effect_audit.json",
                "NORMALIZATION_GAP_AUDIT_PATH": tmp_path / "normalization_gap_audit.json",
                "OPPORTUNITIES_HYGIENE_AUDIT_PATH": tmp_path / "opportunities_hygiene_audit.json",
                "WAVE3_SNAPSHOT_PATH": tmp_path / "wave3_snapshot.json",
                "WAVE4A_SNAPSHOT_PATH": tmp_path / "wave4a_snapshot.json",
                "WAVE4B_SNAPSHOT_PATH": tmp_path / "wave4b_snapshot.json",
                "WAVE5A_SNAPSHOT_PATH": tmp_path / "wave5a_snapshot.json",
                "WAVE5B_SNAPSHOT_PATH": tmp_path / "wave5b_snapshot.json",
                "WAVE6A_SNAPSHOT_PATH": tmp_path / "wave6a_snapshot.json",
                "WAVE6B_SNAPSHOT_PATH": tmp_path / "wave6b_snapshot.json",
            }
            for path in protected.values():
                path.write_text("[]\n" if path.suffix == ".json" else "## Active Opportunities\n", encoding="utf-8")

            records = [
                make_record(
                    path="SIGNALS/normalized/5th_street_branch_gap_latest.md",
                    signal_id="5th_street_branch_gap_latest",
                    date="2026-04-03",
                    source_name="lae_5th_street",
                    branch="lae_5th_street",
                    category="performance_issue",
                    signal_type="performance_gap",
                    description="5TH_STREET is the current weakest branch in the fusion report and needs review.",
                ),
                make_record(
                    path="SIGNALS/normalized/5th_street_branch_gap_2026-04-03_140536.md",
                    signal_id="5th_street_branch_gap_2026-04-03_140536",
                    date="2026-04-03",
                    source_name="lae_5th_street",
                    branch="lae_5th_street",
                    category="performance_issue",
                    signal_type="performance_gap",
                    description="5TH_STREET is the current weakest branch in the fusion report and needs review.",
                ),
                make_record(
                    path="SIGNALS/normalized/waigani_staff_waigani_unknown_staff_strength_2026-04-01_094026.md",
                    signal_id="waigani_staff_waigani_unknown_staff_strength_2026-04-01_094026",
                    date="2026-04-03",
                    source_name="waigani",
                    branch="waigani",
                    category="performance_strength",
                    signal_type="strong_performance",
                    description="staff-waigani-unknown_staff in WAIGANI is identified as a top performer with advisory strength score 1808.80.",
                ),
                make_record(
                    path="SIGNALS/normalized/waigani_staff_waigani_grace_strength_latest.md",
                    signal_id="waigani_staff_waigani_grace_strength_latest",
                    date="2026-04-03",
                    source_name="waigani",
                    branch="waigani",
                    category="performance_strength",
                    signal_type="strong_performance",
                    description="staff-waigani-grace in WAIGANI is identified as a top performer with advisory strength score 128.40.",
                ),
            ]

            old_values: dict[str, object] = {
                "LEGACY_SIGNAL_HYGIENE_AUDIT_PATH": worker.LEGACY_SIGNAL_HYGIENE_AUDIT_PATH,
                "PROCESSING_GUARDRAIL_SUMMARY_PATH": worker.PROCESSING_GUARDRAIL_SUMMARY_PATH,
                "WAVE6C_SNAPSHOT_PATH": worker.WAVE6C_SNAPSHOT_PATH,
            }
            try:
                worker.LEGACY_SIGNAL_HYGIENE_AUDIT_PATH = str(audit_path)
                worker.PROCESSING_GUARDRAIL_SUMMARY_PATH = str(summary_path)
                worker.WAVE6C_SNAPSHOT_PATH = str(snapshot_path)
                for name, path in protected.items():
                    old_values[name] = getattr(worker, name)
                    setattr(worker, name, str(path))

                payload = worker.run_wave6c_processing_guardrails(records)
                first_snapshot = snapshot_path.read_text(encoding="utf-8")
                second_payload = worker.run_wave6c_processing_guardrails(records)
                second_snapshot = snapshot_path.read_text(encoding="utf-8")
                blackboard_after = blackboard_path.read_text(encoding="utf-8")
                written_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            finally:
                worker.LEGACY_SIGNAL_HYGIENE_AUDIT_PATH = old_values["LEGACY_SIGNAL_HYGIENE_AUDIT_PATH"]
                worker.PROCESSING_GUARDRAIL_SUMMARY_PATH = old_values["PROCESSING_GUARDRAIL_SUMMARY_PATH"]
                worker.WAVE6C_SNAPSHOT_PATH = old_values["WAVE6C_SNAPSHOT_PATH"]
                for name, old_value in old_values.items():
                    if name in protected:
                        setattr(worker, name, old_value)

        self.assertEqual(payload, second_payload)
        self.assertEqual(first_snapshot, second_snapshot)
        self.assertEqual(payload["summary"], written_summary)
        self.assertEqual(payload["summary"]["signals_examined"], 4)
        self.assertEqual(payload["summary"]["signals_excluded_from_live_processing"], 2)
        self.assertEqual(blackboard_after, "## Active Opportunities\n")


if __name__ == "__main__":
    unittest.main()
