from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import worker_decision_v2 as worker


VALID_BLOCK = """## Active Opportunities

### [Performance Gap — Performance Issue]

- source: signal demo_signal
- branch: waigani
- category: performance_issue
- signal_type: performance_gap
- source_signal_types:
  - performance_gap
- date_identified: 2026-04-05
- date_window: 2026-04-05
- description: Demo block.

- leverage_score: 0.60
- risk_level: Medium
- confidence: 0.74

- score_components:
  - revenue: 0.60
  - scalability: 0.60
  - ease: 0.60
  - strategic: 0.60
  - wellbeing: 0.50

- evidence_sources:
  - signal demo_signal
  - Demo block.

- rationale: Initial opportunity created from validated normalized signal.

- last_reinforced:
  - date: 2026-04-05
  - delta: 0.0000
  - reason: initial opportunity creation from signal ingestion

- status: Active
- review_status: Pending
- last_updated: 2026-04-05
"""


class SystemInvariantTests(unittest.TestCase):
    def test_verify_system_invariants_passes_for_valid_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            blackboard_path = tmp_path / "OPPORTUNITIES.md"
            warning_path = tmp_path / "warning_intelligence.json"
            fusion_path = tmp_path / "fusion_signal_context.json"
            normalization_path = tmp_path / "normalization_gap_audit.json"
            report_path = tmp_path / "wave7a_invariant_report.json"

            blackboard_path.write_text(VALID_BLOCK, encoding="utf-8")
            warning_path.write_text(
                json.dumps(
                    {
                        "waigani|performance_gap|branch_performance": {
                            "severity_score": 0.4,
                            "escalation_level": "watch",
                        }
                    }
                ),
                encoding="utf-8",
            )
            fusion_path.write_text(
                json.dumps(
                    [
                        {
                            "signal_ref": "demo_signal",
                            "branch": "waigani",
                            "category": "performance_issue",
                            "signal_type": "performance_gap",
                            "matched": True,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            normalization_path.write_text(
                json.dumps(
                    [
                        {
                            "signal_ref": "demo_signal",
                            "signal_type": "performance_gap",
                            "branch_before": "waigani",
                            "branch_after": "waigani",
                            "section_before": "",
                            "section_after": "branch_performance",
                            "duplicate_of": None,
                            "issue_type": "branch_level_section_residue",
                            "notes": "ok",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            old_values = {
                "BLACKBOARD_PATH": worker.BLACKBOARD_PATH,
                "WARNING_INTELLIGENCE_PATH": worker.WARNING_INTELLIGENCE_PATH,
                "FUSION_SIGNAL_CONTEXT_PATH": worker.FUSION_SIGNAL_CONTEXT_PATH,
                "NORMALIZATION_GAP_AUDIT_PATH": worker.NORMALIZATION_GAP_AUDIT_PATH,
                "WAVE7A_INVARIANT_REPORT_PATH": worker.WAVE7A_INVARIANT_REPORT_PATH,
            }
            try:
                worker.BLACKBOARD_PATH = str(blackboard_path)
                worker.WARNING_INTELLIGENCE_PATH = str(warning_path)
                worker.FUSION_SIGNAL_CONTEXT_PATH = str(fusion_path)
                worker.NORMALIZATION_GAP_AUDIT_PATH = str(normalization_path)
                worker.WAVE7A_INVARIANT_REPORT_PATH = str(report_path)

                payload = worker.write_invariant_report()
                written = json.loads(report_path.read_text(encoding="utf-8"))
            finally:
                for name, value in old_values.items():
                    setattr(worker, name, value)

        self.assertEqual(payload, written)
        self.assertEqual(payload["status"], "pass")
        self.assertTrue(all(payload["checks"].values()))
        self.assertEqual(payload["violations"], [])

    def test_verify_system_invariants_fails_for_invalid_duplicate_of(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            blackboard_path = tmp_path / "OPPORTUNITIES.md"
            warning_path = tmp_path / "warning_intelligence.json"
            fusion_path = tmp_path / "fusion_signal_context.json"
            normalization_path = tmp_path / "normalization_gap_audit.json"

            blackboard_path.write_text(VALID_BLOCK, encoding="utf-8")
            warning_path.write_text(
                json.dumps({"demo": {"severity_score": 0.4, "escalation_level": "watch"}}),
                encoding="utf-8",
            )
            fusion_path.write_text(
                json.dumps(
                    [
                        {
                            "signal_ref": "demo_signal",
                            "branch": "waigani",
                            "category": "performance_issue",
                            "signal_type": "performance_gap",
                            "matched": False,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            normalization_path.write_text(
                json.dumps(
                    [
                        {
                            "signal_ref": "demo_signal",
                            "signal_type": "performance_gap",
                            "branch_before": "waigani",
                            "branch_after": "waigani",
                            "section_before": "",
                            "section_after": "branch_performance",
                            "duplicate_of": "missing_signal_ref",
                            "issue_type": "branch_level_section_residue",
                            "notes": "bad",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            old_values = {
                "BLACKBOARD_PATH": worker.BLACKBOARD_PATH,
                "WARNING_INTELLIGENCE_PATH": worker.WARNING_INTELLIGENCE_PATH,
                "FUSION_SIGNAL_CONTEXT_PATH": worker.FUSION_SIGNAL_CONTEXT_PATH,
                "NORMALIZATION_GAP_AUDIT_PATH": worker.NORMALIZATION_GAP_AUDIT_PATH,
            }
            try:
                worker.BLACKBOARD_PATH = str(blackboard_path)
                worker.WARNING_INTELLIGENCE_PATH = str(warning_path)
                worker.FUSION_SIGNAL_CONTEXT_PATH = str(fusion_path)
                worker.NORMALIZATION_GAP_AUDIT_PATH = str(normalization_path)

                payload = worker.verify_system_invariants()
            finally:
                for name, value in old_values.items():
                    setattr(worker, name, value)

        self.assertEqual(payload["status"], "fail")
        self.assertFalse(payload["checks"]["normalization_audit_schema"])
        self.assertTrue(any("duplicate_of is not a valid signal_ref" in item for item in payload["violations"]))


if __name__ == "__main__":
    unittest.main()
