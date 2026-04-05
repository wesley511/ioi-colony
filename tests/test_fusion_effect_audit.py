from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import worker_decision_v2 as worker


def make_record(
    *,
    signal_id: str,
    branch: str,
    category: str,
    signal_type: str,
    date: str,
    description: str,
    confidence: str = "0.74",
) -> tuple[str, dict[str, object], bool]:
    payload: dict[str, object] = {
        "signal_id": signal_id,
        "date": date,
        "date_window": date,
        "source_type": "advisory_report",
        "source_name": branch,
        "branch": branch,
        "category": category,
        "signal_type": signal_type,
        "description": description,
        "confidence": confidence,
        "status": "new",
        "source_signal_types": [signal_type],
    }
    return f"SIGNALS/normalized/{signal_id}.md", payload, False


class FusionEffectAuditTests(unittest.TestCase):
    def test_bounded_wave5c_limit_hard_caps_to_100(self) -> None:
        self.assertEqual(worker.bounded_wave5c_limit(250), 100)
        self.assertEqual(worker.bounded_wave5c_limit(100), 100)
        self.assertEqual(worker.bounded_wave5c_limit(0), 1)

    def test_wave5c_subset_audit_distinguishes_join_from_warning_match_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            confidence_path = tmp_path / "confidence_scoring_audit.json"
            fusion_score_path = tmp_path / "fusion_score_audit.json"
            fusion_context_path = tmp_path / "fusion_signal_context.json"
            audit_path = tmp_path / "fusion_effect_audit.json"
            snapshot_path = tmp_path / "wave5c_snapshot.json"
            blackboard_path = tmp_path / "OPPORTUNITIES.md"

            matched_record = make_record(
                signal_id="waigani_gap_2026-04-03",
                branch="waigani",
                category="performance_issue",
                signal_type="performance_gap",
                date="2026-04-03",
                description="WAIGANI shows a weak operational section: assisting_cashier.",
                confidence="0.79",
            )
            unmatched_record = make_record(
                signal_id="waigani_strength_2026-04-03",
                branch="waigani",
                category="performance_strength",
                signal_type="strong_performance",
                date="2026-04-03",
                description="WAIGANI is the current top-performing branch in the fusion report.",
                confidence="0.66",
            )
            records = [matched_record, unmatched_record]

            blackboard = (
                worker.build_opportunity_block(matched_record[1])
                + "\n"
                + worker.build_opportunity_block(unmatched_record[1])
            )
            blackboard_path.write_text(blackboard, encoding="utf-8")

            confidence_rows = [
                {
                    "signal_ref": "waigani_gap_2026-04-03",
                    "branch": "waigani",
                    "signal_type": "performance_gap",
                    "final_effective_delta": 0.011293,
                    "notes": "wave3",
                },
                {
                    "signal_ref": "waigani_strength_2026-04-03",
                    "branch": "waigani",
                    "signal_type": "strong_performance",
                    "final_effective_delta": 0.009435,
                    "notes": "wave3",
                },
            ]
            fusion_rows = [
                {
                    "signal_ref": "waigani_gap_2026-04-03",
                    "pattern_id": "waigani|performance_gap|assisting_cashier",
                    "matched": True,
                    "branch": "waigani",
                    "signal_type": "performance_gap",
                    "fusion_modifier": 0.145524,
                    "fusion_adjusted_delta_preview": 0.012936,
                    "notes": "wave5b",
                },
                {
                    "signal_ref": "waigani_strength_2026-04-03",
                    "pattern_id": "",
                    "matched": False,
                    "branch": "waigani",
                    "signal_type": "strong_performance",
                    "fusion_modifier": 0.0,
                    "fusion_adjusted_delta_preview": 0.009435,
                    "notes": "signal_not_in_warning_scope",
                },
            ]
            confidence_path.write_text(json.dumps(confidence_rows), encoding="utf-8")
            fusion_score_path.write_text(json.dumps(fusion_rows), encoding="utf-8")
            fusion_context_path.write_text("[]\n", encoding="utf-8")

            old_confidence_path = worker.CONFIDENCE_AUDIT_PATH
            old_fusion_score_path = worker.FUSION_SCORE_AUDIT_PATH
            old_fusion_context_path = worker.FUSION_SIGNAL_CONTEXT_PATH
            old_blackboard_path = worker.BLACKBOARD_PATH
            old_audit_path = worker.FUSION_EFFECT_AUDIT_PATH
            old_snapshot_path = worker.WAVE5C_SNAPSHOT_PATH
            try:
                worker.CONFIDENCE_AUDIT_PATH = str(confidence_path)
                worker.FUSION_SCORE_AUDIT_PATH = str(fusion_score_path)
                worker.FUSION_SIGNAL_CONTEXT_PATH = str(fusion_context_path)
                worker.BLACKBOARD_PATH = str(blackboard_path)
                worker.FUSION_EFFECT_AUDIT_PATH = str(audit_path)
                worker.WAVE5C_SNAPSHOT_PATH = str(snapshot_path)

                payload = worker.run_wave5c_fusion_effect_validation(records, 100)
                written_rows = json.loads(audit_path.read_text(encoding="utf-8"))
                written_snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            finally:
                worker.CONFIDENCE_AUDIT_PATH = old_confidence_path
                worker.FUSION_SCORE_AUDIT_PATH = old_fusion_score_path
                worker.FUSION_SIGNAL_CONTEXT_PATH = old_fusion_context_path
                worker.BLACKBOARD_PATH = old_blackboard_path
                worker.FUSION_EFFECT_AUDIT_PATH = old_audit_path
                worker.WAVE5C_SNAPSHOT_PATH = old_snapshot_path

        self.assertEqual(payload["subset_signal_count"], 2)
        self.assertEqual(payload["verification"]["signal_ref_join"]["match_count"], 2)
        self.assertEqual(payload["verification"]["signal_ref_join"]["no_match_count"], 0)
        self.assertEqual(payload["verification"]["warning_match_context"]["matched_signal_count"], 1)
        self.assertEqual(payload["verification"]["warning_match_context"]["unmatched_signal_count"], 1)
        self.assertEqual(payload["verification"]["opportunity_match"]["match_count"], 2)
        self.assertEqual(payload["verification"]["opportunity_match"]["no_match_count"], 0)
        self.assertEqual(payload["verification"]["opportunity_match"]["ambiguous_match_count"], 0)
        self.assertTrue(payload["idempotency"]["idempotent"])
        self.assertEqual(written_snapshot["fusion_effect"]["row_count"], 2)
        self.assertTrue(written_snapshot["fusion_effect"]["idempotent"])

        matched_row = next(row for row in written_rows if row["signal_ref"] == "waigani_gap_2026-04-03")
        unmatched_row = next(row for row in written_rows if row["signal_ref"] == "waigani_strength_2026-04-03")

        self.assertEqual(matched_row["action"], "reinforce")
        self.assertEqual(matched_row["persisted_delta"], 0.012936)
        self.assertEqual(matched_row["delta_increase"], 0.001643)
        self.assertTrue(matched_row["warning_matched"])

        self.assertEqual(unmatched_row["action"], "reinforce")
        self.assertEqual(unmatched_row["persisted_delta"], 0.009435)
        self.assertEqual(unmatched_row["delta_increase"], 0.0)
        self.assertFalse(unmatched_row["warning_matched"])

    def test_live_reinforcement_uses_single_fusion_adjusted_delta_and_skips_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            blackboard_path = tmp_path / "OPPORTUNITIES.md"
            record = make_record(
                signal_id="waigani_gap_2026-04-03",
                branch="waigani",
                category="performance_issue",
                signal_type="performance_gap",
                date="2026-04-03",
                description="WAIGANI shows a weak operational section: assisting_cashier.",
                confidence="0.79",
            )
            existing_data = dict(record[1])
            existing_data["signal_id"] = "waigani_gap_existing_2026-04-03"
            existing_data["description"] = "Existing WAIGANI performance issue already tracked for the same date window."
            blackboard_path.write_text(worker.build_opportunity_block(existing_data), encoding="utf-8")

            old_blackboard_path = worker.BLACKBOARD_PATH
            try:
                worker.BLACKBOARD_PATH = str(blackboard_path)
                density_index = worker.build_density_index([record])
                fusion_lookup = {
                    "waigani_gap_2026-04-03": {
                        "signal_ref": "waigani_gap_2026-04-03",
                        "pattern_id": "waigani|performance_gap|assisting_cashier",
                        "matched": True,
                        "branch": "waigani",
                        "signal_type": "performance_gap",
                        "fusion_modifier": 0.145524,
                        "notes": "wave5b",
                    }
                }
                fusion_rows: list[dict[str, object]] = []
                expected_effective_delta = round(
                    float(worker.scoring_context_for_signal(record[1], density_index)["effective_delta"]),
                    6,
                )
                expected_final_delta = round(expected_effective_delta * (1.0 + 0.145524), 6)

                first_processed = worker.process_signal_data(
                    record[0],
                    record[1],
                    record[2],
                    density_index,
                    fusion_lookup,
                    fusion_rows,
                )
                first_blackboard = blackboard_path.read_text(encoding="utf-8")

                second_processed = worker.process_signal_data(
                    record[0],
                    record[1],
                    record[2],
                    density_index,
                    fusion_lookup,
                    fusion_rows,
                )
                second_blackboard = blackboard_path.read_text(encoding="utf-8")
            finally:
                worker.BLACKBOARD_PATH = old_blackboard_path

        self.assertTrue(first_processed)
        self.assertFalse(second_processed)
        self.assertEqual(len(fusion_rows), 1)
        self.assertEqual(fusion_rows[0]["effective_delta"], expected_effective_delta)
        self.assertEqual(fusion_rows[0]["fusion_modifier"], 0.145524)
        self.assertEqual(fusion_rows[0]["final_delta_with_fusion"], expected_final_delta)
        self.assertEqual(fusion_rows[0]["persisted_delta"], expected_final_delta)
        self.assertEqual(first_blackboard, second_blackboard)
        self.assertIn(f"- delta: {expected_final_delta:.4f}", first_blackboard)


if __name__ == "__main__":
    unittest.main()
