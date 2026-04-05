from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import worker_decision_v2 as worker


class FusionScoreAuditTests(unittest.TestCase):
    def test_compute_fusion_modifier_uses_bounded_formula(self) -> None:
        modifier = worker.compute_fusion_modifier(
            True,
            severity_score=0.786667,
            occurrence_count=4,
            consecutive_days=2,
        )

        self.assertEqual(modifier, 0.231333)

    def test_compute_fusion_modifier_forces_zero_when_unmatched(self) -> None:
        modifier = worker.compute_fusion_modifier(
            False,
            severity_score=1.0,
            occurrence_count=9,
            consecutive_days=9,
        )

        self.assertEqual(modifier, 0.0)

    def test_build_fusion_score_audit_is_deterministic_and_keeps_unmatched_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            confidence_path = tmp_path / "confidence_scoring_audit.json"
            fusion_path = tmp_path / "fusion_signal_context.json"

            confidence_rows = [
                {
                    "signal_ref": "sig-matched",
                    "branch": "waigani",
                    "signal_type": "performance_gap",
                    "final_effective_delta": 0.011293,
                    "notes": "wave3",
                },
                {
                    "signal_ref": "sig-unmatched",
                    "branch": "waigani",
                    "signal_type": "daily_sales_report",
                    "final_effective_delta": 0.040000,
                    "notes": "wave3",
                },
            ]
            fusion_rows = [
                {
                    "signal_ref": "sig-matched",
                    "pattern_id": "waigani|performance_gap|branch_performance",
                    "matched": True,
                    "branch": "waigani",
                    "signal_type": "performance_gap",
                    "severity_score": 0.459048,
                    "occurrence_count": 3,
                    "consecutive_days": 1,
                    "escalation_level": "watch",
                    "notes": "wave5a",
                },
                {
                    "signal_ref": "sig-unmatched",
                    "pattern_id": "",
                    "matched": False,
                    "branch": "waigani",
                    "signal_type": "daily_sales_report",
                    "severity_score": 0.0,
                    "occurrence_count": 0,
                    "consecutive_days": 0,
                    "escalation_level": "none",
                    "notes": "signal_not_in_warning_scope",
                },
            ]
            confidence_path.write_text(json.dumps(confidence_rows), encoding="utf-8")
            fusion_path.write_text(json.dumps(fusion_rows), encoding="utf-8")

            old_confidence_path = worker.CONFIDENCE_AUDIT_PATH
            old_fusion_path = worker.FUSION_SIGNAL_CONTEXT_PATH
            try:
                worker.CONFIDENCE_AUDIT_PATH = str(confidence_path)
                worker.FUSION_SIGNAL_CONTEXT_PATH = str(fusion_path)
                first = worker.build_fusion_score_audit()
                second = worker.build_fusion_score_audit()
            finally:
                worker.CONFIDENCE_AUDIT_PATH = old_confidence_path
                worker.FUSION_SIGNAL_CONTEXT_PATH = old_fusion_path

        self.assertEqual(first, second)
        self.assertEqual(len(first), 2)

        matched = next(row for row in first if row["signal_ref"] == "sig-matched")
        unmatched = next(row for row in first if row["signal_ref"] == "sig-unmatched")

        self.assertEqual(matched["fusion_modifier"], 0.145524)
        self.assertEqual(matched["fusion_adjusted_delta_preview"], 0.012936)
        self.assertEqual(matched["notes"], "wave5a; wave3")

        self.assertEqual(unmatched["fusion_modifier"], 0.0)
        self.assertEqual(unmatched["fusion_adjusted_delta_preview"], 0.04)
        self.assertEqual(unmatched["notes"], "signal_not_in_warning_scope; wave3")

    def test_write_wave5b_snapshot_is_idempotent_when_inputs_are_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            snapshot_path = tmp_path / "wave5b_snapshot.json"
            audit_path = tmp_path / "fusion_score_audit.json"

            audit_path.write_text("[]\n", encoding="utf-8")

            old_snapshot_path = worker.WAVE5B_SNAPSHOT_PATH
            old_audit_path = worker.FUSION_SCORE_AUDIT_PATH
            try:
                worker.WAVE5B_SNAPSHOT_PATH = str(snapshot_path)
                worker.FUSION_SCORE_AUDIT_PATH = str(audit_path)

                stats = {
                    "row_count": 7.0,
                    "matched_non_zero_count": 5.0,
                    "unmatched_zero_count": 2.0,
                    "max_fusion_modifier": 0.231333,
                }

                worker.write_wave5b_snapshot(stats)
                first = snapshot_path.read_text(encoding="utf-8")
                worker.write_wave5b_snapshot(stats)
                second = snapshot_path.read_text(encoding="utf-8")
            finally:
                worker.WAVE5B_SNAPSHOT_PATH = old_snapshot_path
                worker.FUSION_SCORE_AUDIT_PATH = old_audit_path

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
