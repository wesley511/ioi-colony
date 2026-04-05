from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import worker_decision_v2 as worker


class FinalSystemCertificationTests(unittest.TestCase):
    def test_verify_final_system_state_certifies_when_hashes_stable_and_reports_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            blackboard_path = tmp_path / "OPPORTUNITIES.md"
            warning_path = tmp_path / "warning_intelligence.json"
            fusion_context_path = tmp_path / "fusion_signal_context.json"
            fusion_score_path = tmp_path / "fusion_score_audit.json"
            fusion_effect_path = tmp_path / "fusion_effect_audit.json"
            invariant_path = tmp_path / "wave7a_invariant_report.json"
            consistency_path = tmp_path / "wave7b_consistency_report.json"
            certification_path = tmp_path / "wave7c_final_certification.json"

            blackboard_path.write_text("demo", encoding="utf-8")
            warning_path.write_text(json.dumps({}), encoding="utf-8")
            fusion_context_path.write_text(json.dumps([]), encoding="utf-8")
            fusion_score_path.write_text(json.dumps([]), encoding="utf-8")
            fusion_effect_path.write_text(json.dumps([]), encoding="utf-8")
            invariant_path.write_text(json.dumps({"status": "pass"}), encoding="utf-8")
            consistency_path.write_text(json.dumps({"status": "pass"}), encoding="utf-8")

            old_values = {
                "BLACKBOARD_PATH": worker.BLACKBOARD_PATH,
                "WARNING_INTELLIGENCE_PATH": worker.WARNING_INTELLIGENCE_PATH,
                "FUSION_SIGNAL_CONTEXT_PATH": worker.FUSION_SIGNAL_CONTEXT_PATH,
                "FUSION_SCORE_AUDIT_PATH": worker.FUSION_SCORE_AUDIT_PATH,
                "FUSION_EFFECT_AUDIT_PATH": worker.FUSION_EFFECT_AUDIT_PATH,
                "WAVE7A_INVARIANT_REPORT_PATH": worker.WAVE7A_INVARIANT_REPORT_PATH,
                "WAVE7B_CONSISTENCY_REPORT_PATH": worker.WAVE7B_CONSISTENCY_REPORT_PATH,
                "WAVE7C_FINAL_CERTIFICATION_PATH": worker.WAVE7C_FINAL_CERTIFICATION_PATH,
            }
            try:
                worker.BLACKBOARD_PATH = str(blackboard_path)
                worker.WARNING_INTELLIGENCE_PATH = str(warning_path)
                worker.FUSION_SIGNAL_CONTEXT_PATH = str(fusion_context_path)
                worker.FUSION_SCORE_AUDIT_PATH = str(fusion_score_path)
                worker.FUSION_EFFECT_AUDIT_PATH = str(fusion_effect_path)
                worker.WAVE7A_INVARIANT_REPORT_PATH = str(invariant_path)
                worker.WAVE7B_CONSISTENCY_REPORT_PATH = str(consistency_path)
                worker.WAVE7C_FINAL_CERTIFICATION_PATH = str(certification_path)

                payload = worker.write_final_system_certification()
                written = json.loads(certification_path.read_text(encoding="utf-8"))
            finally:
                for name, value in old_values.items():
                    setattr(worker, name, value)

        self.assertEqual(payload, written)
        self.assertEqual(payload["status"], "CERTIFIED")
        self.assertTrue(payload["idempotency"])
        self.assertTrue(payload["invariants_passed"])
        self.assertTrue(payload["consistency_passed"])
        self.assertTrue(payload["hash_stability"])

    def test_verify_final_system_state_fails_when_prerequisite_reports_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            blackboard_path = tmp_path / "OPPORTUNITIES.md"
            warning_path = tmp_path / "warning_intelligence.json"
            fusion_context_path = tmp_path / "fusion_signal_context.json"
            fusion_score_path = tmp_path / "fusion_score_audit.json"
            fusion_effect_path = tmp_path / "fusion_effect_audit.json"
            invariant_path = tmp_path / "wave7a_invariant_report.json"
            consistency_path = tmp_path / "wave7b_consistency_report.json"

            blackboard_path.write_text("demo", encoding="utf-8")
            warning_path.write_text(json.dumps({}), encoding="utf-8")
            fusion_context_path.write_text(json.dumps([]), encoding="utf-8")
            fusion_score_path.write_text(json.dumps([]), encoding="utf-8")
            fusion_effect_path.write_text(json.dumps([]), encoding="utf-8")
            invariant_path.write_text(json.dumps({"status": "fail"}), encoding="utf-8")
            consistency_path.write_text(json.dumps({"status": "fail"}), encoding="utf-8")

            old_values = {
                "BLACKBOARD_PATH": worker.BLACKBOARD_PATH,
                "WARNING_INTELLIGENCE_PATH": worker.WARNING_INTELLIGENCE_PATH,
                "FUSION_SIGNAL_CONTEXT_PATH": worker.FUSION_SIGNAL_CONTEXT_PATH,
                "FUSION_SCORE_AUDIT_PATH": worker.FUSION_SCORE_AUDIT_PATH,
                "FUSION_EFFECT_AUDIT_PATH": worker.FUSION_EFFECT_AUDIT_PATH,
                "WAVE7A_INVARIANT_REPORT_PATH": worker.WAVE7A_INVARIANT_REPORT_PATH,
                "WAVE7B_CONSISTENCY_REPORT_PATH": worker.WAVE7B_CONSISTENCY_REPORT_PATH,
            }
            try:
                worker.BLACKBOARD_PATH = str(blackboard_path)
                worker.WARNING_INTELLIGENCE_PATH = str(warning_path)
                worker.FUSION_SIGNAL_CONTEXT_PATH = str(fusion_context_path)
                worker.FUSION_SCORE_AUDIT_PATH = str(fusion_score_path)
                worker.FUSION_EFFECT_AUDIT_PATH = str(fusion_effect_path)
                worker.WAVE7A_INVARIANT_REPORT_PATH = str(invariant_path)
                worker.WAVE7B_CONSISTENCY_REPORT_PATH = str(consistency_path)

                payload = worker.verify_final_system_state()
            finally:
                for name, value in old_values.items():
                    setattr(worker, name, value)

        self.assertEqual(payload["status"], "FAILED")
        self.assertTrue(payload["idempotency"])
        self.assertFalse(payload["invariants_passed"])
        self.assertFalse(payload["consistency_passed"])
        self.assertTrue(payload["hash_stability"])


if __name__ == "__main__":
    unittest.main()
