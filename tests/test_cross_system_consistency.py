from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import worker_decision_v2 as worker


class CrossSystemConsistencyTests(unittest.TestCase):
    def test_verify_cross_system_consistency_passes_for_aligned_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source_path = tmp_path / "SIGNALS" / "normalized" / "demo_signal.md"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text("demo", encoding="utf-8")
            fusion_path = tmp_path / "fusion_signal_context.json"
            normalization_path = tmp_path / "normalization_gap_audit.json"
            report_path = tmp_path / "wave7b_consistency_report.json"

            fusion_path.write_text(
                json.dumps(
                    [
                        {
                            "signal_ref": str(source_path),
                            "branch": "waigani",
                            "category": "operations",
                            "signal_type": "performance_gap",
                            "pattern_id": "waigani|performance_gap|assisting_cashier",
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
                            "signal_ref": str(source_path),
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
                "FUSION_SIGNAL_CONTEXT_PATH": worker.FUSION_SIGNAL_CONTEXT_PATH,
                "NORMALIZATION_GAP_AUDIT_PATH": worker.NORMALIZATION_GAP_AUDIT_PATH,
                "WAVE7B_CONSISTENCY_REPORT_PATH": worker.WAVE7B_CONSISTENCY_REPORT_PATH,
                "collect_signal_records": worker.collect_signal_records,
            }
            try:
                worker.FUSION_SIGNAL_CONTEXT_PATH = str(fusion_path)
                worker.NORMALIZATION_GAP_AUDIT_PATH = str(normalization_path)
                worker.WAVE7B_CONSISTENCY_REPORT_PATH = str(report_path)
                worker.collect_signal_records = lambda: [
                    (
                        str(source_path),
                        {
                            "signal_ref": str(source_path),
                            "signal_id": "demo_signal",
                            "branch": "waigani",
                            "category": "operations",
                            "signal_type": "performance_gap",
                            "description": "WAIGANI section needs review.",
                            "section": "assisting_cashier",
                            "date": "2026-04-05",
                            "date_window": "2026-04-05",
                            "source_type": "advisory_report",
                            "source_name": "waigani",
                            "confidence": "0.79",
                            "status": "new",
                        },
                        False,
                    )
                ]

                payload = worker.write_cross_system_consistency_report()
                written = json.loads(report_path.read_text(encoding="utf-8"))
            finally:
                for name, value in old_values.items():
                    setattr(worker, name, value)

        self.assertEqual(payload, written)
        self.assertEqual(payload["status"], "pass")
        self.assertTrue(all(payload["checks"].values()))
        self.assertEqual(payload["violations"], [])

    def test_verify_cross_system_consistency_flags_cycle_and_pattern_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            signal_a = tmp_path / "SIGNALS" / "normalized" / "a.md"
            signal_b = tmp_path / "SIGNALS" / "normalized" / "b.md"
            raw_signal_ref = "RAW_INPUT/whatsapp/waigani/demo.txt"
            signal_a.parent.mkdir(parents=True, exist_ok=True)
            signal_a.write_text("a", encoding="utf-8")
            signal_b.write_text("b", encoding="utf-8")
            fusion_path = tmp_path / "fusion_signal_context.json"
            normalization_path = tmp_path / "normalization_gap_audit.json"

            fusion_path.write_text(
                json.dumps(
                    [
                        {
                            "signal_ref": str(signal_a),
                            "branch": "waigani",
                            "category": "operations",
                            "signal_type": "performance_gap",
                            "pattern_id": "waigani|performance_gap|branch_performance",
                            "matched": True,
                        },
                        {
                            "signal_ref": raw_signal_ref,
                            "branch": "waigani",
                            "category": "sales_signal",
                            "signal_type": "daily_sales_report",
                            "pattern_id": "",
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
                            "signal_ref": str(signal_a),
                            "signal_type": "performance_gap",
                            "branch_before": "waigani",
                            "branch_after": "waigani",
                            "section_before": "",
                            "section_after": "branch_performance",
                            "duplicate_of": str(signal_b),
                            "issue_type": "branch_level_section_residue",
                            "notes": "duplicate",
                        },
                        {
                            "signal_ref": str(signal_b),
                            "signal_type": "performance_gap",
                            "branch_before": "waigani",
                            "branch_after": "waigani",
                            "section_before": "",
                            "section_after": "branch_performance",
                            "duplicate_of": str(signal_a),
                            "issue_type": "branch_level_section_residue",
                            "notes": "duplicate",
                        },
                    ]
                ),
                encoding="utf-8",
            )

            old_values = {
                "FUSION_SIGNAL_CONTEXT_PATH": worker.FUSION_SIGNAL_CONTEXT_PATH,
                "NORMALIZATION_GAP_AUDIT_PATH": worker.NORMALIZATION_GAP_AUDIT_PATH,
                "collect_signal_records": worker.collect_signal_records,
            }
            try:
                worker.FUSION_SIGNAL_CONTEXT_PATH = str(fusion_path)
                worker.NORMALIZATION_GAP_AUDIT_PATH = str(normalization_path)
                worker.collect_signal_records = lambda: [
                    (
                        str(signal_a),
                        {
                            "signal_ref": str(signal_a),
                            "signal_id": "demo_signal",
                            "branch": "waigani",
                            "category": "operations",
                            "signal_type": "performance_gap",
                            "description": "WAIGANI section needs review.",
                            "section": "assisting_cashier",
                            "date": "2026-04-05",
                            "date_window": "2026-04-05",
                            "source_type": "advisory_report",
                            "source_name": "waigani",
                            "confidence": "0.79",
                            "status": "new",
                        },
                        False,
                    )
                ]
                payload = worker.verify_cross_system_consistency()
            finally:
                for name, value in old_values.items():
                    setattr(worker, name, value)

        self.assertEqual(payload["status"], "fail")
        self.assertFalse(payload["checks"]["duplicate_integrity"])
        self.assertFalse(payload["checks"]["pattern_format"])
        self.assertTrue(payload["checks"]["signal_linkage"])
        self.assertTrue(any("duplicate cycle detected" in item for item in payload["violations"]))
        self.assertTrue(any("pattern_id mismatch" in item for item in payload["violations"]))


if __name__ == "__main__":
    unittest.main()
