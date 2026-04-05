from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import worker_decision_v2 as worker


class OpportunitiesHygieneTests(unittest.TestCase):
    def test_normalize_block_text_collapses_blank_drift_and_orders_terminal_sections(self) -> None:
        block = """### [Strong Performance — Performance Strength]

- source: signal demo
- branch: waigani
- category: performance_strength
- signal_type: strong_performance

- rationale: Initial opportunity created from validated normalized signal.


- status: Active
- review_status: Pending




- last_reinforced:
  - date: 2026-04-03
  - delta: 0.03
  - reason: reinforced by demo
- last_updated: 2026-04-04
"""

        normalized = worker.normalize_block_text(block)

        self.assertNotIn("\n\n\n", normalized)
        self.assertLess(normalized.index("- rationale:"), normalized.index("- last_reinforced:"))
        self.assertLess(normalized.index("- last_reinforced:"), normalized.index("- status:"))
        self.assertLess(normalized.index("- review_status:"), normalized.index("- last_updated:"))

    def test_semantic_blackboard_fingerprint_is_preserved_by_hygiene_normalization(self) -> None:
        blackboard = """# IOI Colony Opportunities Blackboard

## Active Opportunities

### [Strong Performance — Performance Strength]

- source: signal demo
- branch: waigani
- category: performance_strength
- signal_type: strong_performance

- rationale: Initial opportunity created from validated normalized signal.

- status: Active
- review_status: Pending

- last_reinforced:
  - date: 2026-04-03
  - delta: 0.03
  - reason: reinforced by demo
- last_updated: 2026-04-04
"""
        normalized = worker.normalize_blackboard_content(blackboard)

        self.assertEqual(
            worker.semantic_blackboard_fingerprint(blackboard),
            worker.semantic_blackboard_fingerprint(normalized),
        )

    def test_run_wave6b_blackboard_hygiene_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            blackboard_path = tmp_path / "OPPORTUNITIES.md"
            blackboard_path.write_text(
                """# IOI Colony Opportunities Blackboard

## Active Opportunities

### [Strong Performance — Performance Strength]

- source: signal demo
- branch: waigani
- category: performance_strength
- signal_type: strong_performance

- rationale: Initial opportunity created from validated normalized signal.

- status: Active
- review_status: Pending



- last_reinforced:
  - date: 2026-04-03
  - delta: 0.03
  - reason: reinforced by demo
- last_updated: 2026-04-04
""",
                encoding="utf-8",
            )

            audit_path = tmp_path / "opportunities_hygiene_audit.json"
            snapshot_path = tmp_path / "wave6b_snapshot.json"
            protected = {
                "CONFIDENCE_AUDIT_PATH": tmp_path / "confidence.json",
                "DENSITY_AUDIT_PATH": tmp_path / "density.json",
                "WARNING_INTELLIGENCE_PATH": tmp_path / "warning_intelligence.json",
                "WARNING_PATTERN_AUDIT_PATH": tmp_path / "warning_pattern_audit.json",
                "FUSION_SIGNAL_CONTEXT_PATH": tmp_path / "fusion_signal_context.json",
                "FUSION_SCORE_AUDIT_PATH": tmp_path / "fusion_score_audit.json",
                "FUSION_EFFECT_AUDIT_PATH": tmp_path / "fusion_effect_audit.json",
                "NORMALIZATION_GAP_AUDIT_PATH": tmp_path / "normalization_gap_audit.json",
                "WAVE3_SNAPSHOT_PATH": tmp_path / "wave3_snapshot.json",
                "WAVE4A_SNAPSHOT_PATH": tmp_path / "wave4a_snapshot.json",
                "WAVE4B_SNAPSHOT_PATH": tmp_path / "wave4b_snapshot.json",
                "WAVE5A_SNAPSHOT_PATH": tmp_path / "wave5a_snapshot.json",
                "WAVE5B_SNAPSHOT_PATH": tmp_path / "wave5b_snapshot.json",
                "WAVE6A_SNAPSHOT_PATH": tmp_path / "wave6a_snapshot.json",
            }
            for path in protected.values():
                path.write_text("[]\n", encoding="utf-8")

            old_values: dict[str, object] = {
                "BLACKBOARD_PATH": worker.BLACKBOARD_PATH,
                "OPPORTUNITIES_HYGIENE_AUDIT_PATH": worker.OPPORTUNITIES_HYGIENE_AUDIT_PATH,
                "WAVE6B_SNAPSHOT_PATH": worker.WAVE6B_SNAPSHOT_PATH,
            }
            try:
                worker.BLACKBOARD_PATH = str(blackboard_path)
                worker.OPPORTUNITIES_HYGIENE_AUDIT_PATH = str(audit_path)
                worker.WAVE6B_SNAPSHOT_PATH = str(snapshot_path)
                for name, path in protected.items():
                    old_values[name] = getattr(worker, name)
                    setattr(worker, name, str(path))

                audit = worker.run_wave6b_blackboard_hygiene()
                first_blackboard = blackboard_path.read_text(encoding="utf-8")
                first_snapshot = snapshot_path.read_text(encoding="utf-8")
                second_audit = worker.run_wave6b_blackboard_hygiene()
                second_blackboard = blackboard_path.read_text(encoding="utf-8")
                second_snapshot = snapshot_path.read_text(encoding="utf-8")
                written_audit = json.loads(audit_path.read_text(encoding="utf-8"))
            finally:
                worker.BLACKBOARD_PATH = old_values["BLACKBOARD_PATH"]
                worker.OPPORTUNITIES_HYGIENE_AUDIT_PATH = old_values["OPPORTUNITIES_HYGIENE_AUDIT_PATH"]
                worker.WAVE6B_SNAPSHOT_PATH = old_values["WAVE6B_SNAPSHOT_PATH"]
                for name, old_value in old_values.items():
                    if name in protected:
                        setattr(worker, name, old_value)

        self.assertEqual(audit, second_audit)
        self.assertEqual(second_audit, written_audit)
        self.assertEqual(first_blackboard, second_blackboard)
        self.assertEqual(first_snapshot, second_snapshot)
        self.assertEqual(audit["blocks_with_structure_issues_before"], 1)
        self.assertEqual(audit["blocks_with_structure_issues_after"], 0)
        self.assertEqual(audit["duplicate_key_count"], 0)


if __name__ == "__main__":
    unittest.main()
