from __future__ import annotations

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
        "confidence": "0.79",
        "status": "new",
        "source_signal_types": [signal_type],
    }
    return path, payload, False


class NormalizationGapAuditTests(unittest.TestCase):
    def test_branch_inference_uses_shared_resolution_for_unknown_branch(self) -> None:
        path, data, _ = make_record(
            path="SIGNALS/normalized/lae_5th_street_branch_gap_2026-03-31_065138.md",
            signal_id="lae_5th_street_branch_gap_2026-03-31_065138",
            date="2026-03-31",
            source_name="unknown",
            branch="unknown",
            category="performance_issue",
            signal_type="performance_gap",
            description="LAE_5TH_STREET is the current weakest branch in the fusion report and needs review.",
        )

        rows = worker.normalization_issue_rows_for_signal(path, data)
        branch_row = next(row for row in rows if row["issue_type"] == "branch_inferred_from_path_or_signal_id")

        self.assertEqual(branch_row["branch_before"], "unknown")
        self.assertEqual(branch_row["branch_after"], "lae_5th_street")

    def test_branch_inference_uses_filename_stem_before_path_segments(self) -> None:
        path, data, _ = make_record(
            path="RAW_INPUT/whatsapp/archive/unknown/report.md",
            signal_id="waigani_branch_gap_latest",
            date="2026-04-03",
            source_name="unknown",
            branch="unknown",
            category="performance_issue",
            signal_type="performance_gap",
            description="WAIGANI is the current weakest branch in the fusion report and needs review.",
        )

        branch_after = worker.audit_resolved_branch(path, data)

        self.assertEqual(branch_after, "waigani")

    def test_branch_level_section_residue_is_audited_without_mutating_behavior(self) -> None:
        path, data, _ = make_record(
            path="SIGNALS/normalized/5th_street_branch_gap_2026-04-03_140536.md",
            signal_id="5th_street_branch_gap_2026-04-03_140536",
            date="2026-04-03",
            source_name="lae_5th_street",
            branch="lae_5th_street",
            category="performance_issue",
            signal_type="performance_gap",
            description="LAE_5TH_STREET is the current weakest branch in the fusion report and needs review.",
        )

        rows = worker.normalization_issue_rows_for_signal(path, data)
        section_row = next(row for row in rows if row["issue_type"] == "branch_level_section_residue")

        self.assertEqual(section_row["section_before"], "")
        self.assertEqual(section_row["section_after"], "branch_performance")

    def test_empty_section_can_be_inferred_from_shared_section_normalizer(self) -> None:
        path, data, _ = make_record(
            path="SIGNALS/normalized/waigani_assisting_cashier_gap_2026-04-03_140536.md",
            signal_id="waigani_assisting_cashier_gap_2026-04-03_140536",
            date="2026-04-03",
            source_name="waigani",
            branch="waigani",
            category="performance_issue",
            signal_type="performance_gap",
            description="WAIGANI shows a weak operational section and needs review.",
        )

        rows = worker.normalization_issue_rows_for_signal(path, data)
        section_row = next(row for row in rows if row["issue_type"] == "section_inferred_from_canonical_normalizer")

        self.assertEqual(section_row["section_before"], "")
        self.assertEqual(section_row["section_after"], "assisting_cashier")

    def test_wave6a_audit_is_read_only_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            blackboard_path = tmp_path / "OPPORTUNITIES.md"
            blackboard_path.write_text("## Active Opportunities\n", encoding="utf-8")

            audit_path = tmp_path / "normalization_gap_audit.json"
            snapshot_path = tmp_path / "wave6a_snapshot.json"

            protected = {
                "BLACKBOARD_PATH": blackboard_path,
                "CONFIDENCE_AUDIT_PATH": tmp_path / "confidence.json",
                "DENSITY_AUDIT_PATH": tmp_path / "density.json",
                "WARNING_INTELLIGENCE_PATH": tmp_path / "warning_intelligence.json",
                "WARNING_PATTERN_AUDIT_PATH": tmp_path / "warning_pattern_audit.json",
                "FUSION_SIGNAL_CONTEXT_PATH": tmp_path / "fusion_signal_context.json",
                "FUSION_SCORE_AUDIT_PATH": tmp_path / "fusion_score_audit.json",
                "FUSION_EFFECT_AUDIT_PATH": tmp_path / "fusion_effect_audit.json",
                "WAVE5C_SNAPSHOT_PATH": tmp_path / "wave5c_snapshot.json",
                "WAVE3_SNAPSHOT_PATH": tmp_path / "wave3_snapshot.json",
                "WAVE4A_SNAPSHOT_PATH": tmp_path / "wave4a_snapshot.json",
                "WAVE4B_SNAPSHOT_PATH": tmp_path / "wave4b_snapshot.json",
                "WAVE5A_SNAPSHOT_PATH": tmp_path / "wave5a_snapshot.json",
                "WAVE5B_SNAPSHOT_PATH": tmp_path / "wave5b_snapshot.json",
            }
            for path in protected.values():
                path.write_text("[]\n" if path.suffix == ".json" else "## Active Opportunities\n", encoding="utf-8")

            record = make_record(
                path="SIGNALS/normalized/lae_5th_street_branch_gap_2026-03-31_065138.md",
                signal_id="lae_5th_street_branch_gap_2026-03-31_065138",
                date="2026-03-31",
                source_name="unknown",
                branch="unknown",
                category="performance_issue",
                signal_type="performance_gap",
                description="LAE_5TH_STREET is the current weakest branch in the fusion report and needs review.",
            )

            old_values: dict[str, object] = {}
            try:
                old_values["NORMALIZATION_GAP_AUDIT_PATH"] = worker.NORMALIZATION_GAP_AUDIT_PATH
                old_values["WAVE6A_SNAPSHOT_PATH"] = worker.WAVE6A_SNAPSHOT_PATH
                worker.NORMALIZATION_GAP_AUDIT_PATH = str(audit_path)
                worker.WAVE6A_SNAPSHOT_PATH = str(snapshot_path)

                for name, path in protected.items():
                    old_values[name] = getattr(worker, name)
                    setattr(worker, name, str(path))

                summary = worker.run_wave6a_normalization_audit([record])
                first_snapshot = snapshot_path.read_text(encoding="utf-8")
                second_summary = worker.run_wave6a_normalization_audit([record])
                second_snapshot = snapshot_path.read_text(encoding="utf-8")
                blackboard_after = blackboard_path.read_text(encoding="utf-8")
            finally:
                worker.NORMALIZATION_GAP_AUDIT_PATH = old_values["NORMALIZATION_GAP_AUDIT_PATH"]
                worker.WAVE6A_SNAPSHOT_PATH = old_values["WAVE6A_SNAPSHOT_PATH"]
                for name, old_value in old_values.items():
                    if name in protected:
                        setattr(worker, name, old_value)

        self.assertEqual(summary, second_summary)
        self.assertEqual(first_snapshot, second_snapshot)
        self.assertTrue(summary["idempotent"])
        self.assertEqual(summary["branch_unknown_before_count"], 1)
        self.assertEqual(summary["branch_unknown_after_count"], 0)
        self.assertEqual(blackboard_after, "## Active Opportunities\n")

    def test_duplicate_visibility_marks_timestamped_row_with_duplicate_of_latest(self) -> None:
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

        rows, summary = worker.build_normalization_gap_audit(
            [latest, stamped],
            include_duplicate_visibility=True,
        )

        duplicate_rows = [row for row in rows if row["signal_ref"] == "5th_street_branch_gap_2026-04-03_140536"]
        self.assertTrue(duplicate_rows)
        self.assertTrue(all(row["duplicate_of"] == "5th_street_branch_gap_latest" for row in duplicate_rows))
        self.assertEqual(summary["duplicate_signal_count"], 1)
        self.assertEqual(summary["latest_timestamp_duplicate_count"], 1)

    def test_duplicate_visibility_skips_non_audit_canonical_targets(self) -> None:
        latest = make_record(
            path="SIGNALS/normalized/waigani_kids_boys_gap_latest.md",
            signal_id="waigani_kids_boys_gap_latest",
            date="2026-04-03",
            source_name="waigani",
            branch="waigani",
            category="operations",
            signal_type="performance_gap",
            description="WAIGANI needs review.",
        )
        latest[1]["section"] = "kids_boys"
        stamped = make_record(
            path="SIGNALS/normalized/waigani_assisting_cashier_gap_2026-04-03_140536.md",
            signal_id="waigani_assisting_cashier_gap_2026-04-03_140536",
            date="2026-04-03",
            source_name="waigani",
            branch="waigani",
            category="operations",
            signal_type="performance_gap",
            description="WAIGANI needs review.",
        )

        rows, summary = worker.build_normalization_gap_audit(
            [latest, stamped],
            include_duplicate_visibility=True,
        )

        duplicate_rows = [row for row in rows if row["signal_ref"] == "waigani_kids_boys_gap_latest"]
        self.assertTrue(duplicate_rows)
        self.assertTrue(all(row["duplicate_of"] == "waigani_assisting_cashier_gap_2026-04-03_140536" for row in duplicate_rows))
        self.assertEqual(summary["duplicate_signal_count"], 1)

    def test_wave6b_duplicate_audit_is_read_only_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            blackboard_path = tmp_path / "OPPORTUNITIES.md"
            blackboard_path.write_text("## Active Opportunities\n", encoding="utf-8")

            audit_path = tmp_path / "normalization_gap_audit.json"
            snapshot_path = tmp_path / "wave6b_snapshot.json"

            protected = {
                "BLACKBOARD_PATH": blackboard_path,
                "CONFIDENCE_AUDIT_PATH": tmp_path / "confidence.json",
                "DENSITY_AUDIT_PATH": tmp_path / "density.json",
                "WARNING_INTELLIGENCE_PATH": tmp_path / "warning_intelligence.json",
                "WARNING_PATTERN_AUDIT_PATH": tmp_path / "warning_pattern_audit.json",
                "FUSION_SIGNAL_CONTEXT_PATH": tmp_path / "fusion_signal_context.json",
                "FUSION_SCORE_AUDIT_PATH": tmp_path / "fusion_score_audit.json",
                "FUSION_EFFECT_AUDIT_PATH": tmp_path / "fusion_effect_audit.json",
                "WAVE5C_SNAPSHOT_PATH": tmp_path / "wave5c_snapshot.json",
                "WAVE3_SNAPSHOT_PATH": tmp_path / "wave3_snapshot.json",
                "WAVE4A_SNAPSHOT_PATH": tmp_path / "wave4a_snapshot.json",
                "WAVE4B_SNAPSHOT_PATH": tmp_path / "wave4b_snapshot.json",
                "WAVE5A_SNAPSHOT_PATH": tmp_path / "wave5a_snapshot.json",
                "WAVE5B_SNAPSHOT_PATH": tmp_path / "wave5b_snapshot.json",
                "WAVE6A_SNAPSHOT_PATH": tmp_path / "wave6a_snapshot.json",
                "OPPORTUNITIES_HYGIENE_AUDIT_PATH": tmp_path / "opportunities_hygiene_audit.json",
            }
            for path in protected.values():
                path.write_text("[]\n" if path.suffix == ".json" else "## Active Opportunities\n", encoding="utf-8")

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

            old_values: dict[str, object] = {}
            try:
                old_values["NORMALIZATION_GAP_AUDIT_PATH"] = worker.NORMALIZATION_GAP_AUDIT_PATH
                old_values["WAVE6B_SNAPSHOT_PATH"] = worker.WAVE6B_SNAPSHOT_PATH
                worker.NORMALIZATION_GAP_AUDIT_PATH = str(audit_path)
                worker.WAVE6B_SNAPSHOT_PATH = str(snapshot_path)

                for name, path in protected.items():
                    old_values[name] = getattr(worker, name)
                    setattr(worker, name, str(path))

                summary = worker.run_wave6b_duplicate_visibility_audit([latest, stamped])
                first_snapshot = snapshot_path.read_text(encoding="utf-8")
                second_summary = worker.run_wave6b_duplicate_visibility_audit([latest, stamped])
                second_snapshot = snapshot_path.read_text(encoding="utf-8")
                written_rows = worker.read_json_list(str(audit_path))
                blackboard_after = blackboard_path.read_text(encoding="utf-8")
            finally:
                worker.NORMALIZATION_GAP_AUDIT_PATH = old_values["NORMALIZATION_GAP_AUDIT_PATH"]
                worker.WAVE6B_SNAPSHOT_PATH = old_values["WAVE6B_SNAPSHOT_PATH"]
                for name, old_value in old_values.items():
                    if name in protected:
                        setattr(worker, name, old_value)

        self.assertEqual(summary, second_summary)
        self.assertEqual(first_snapshot, second_snapshot)
        self.assertTrue(summary["idempotent"])
        self.assertEqual(summary["duplicate_signal_count"], 1)
        self.assertEqual(
            next(row for row in written_rows if row["signal_ref"] == "5th_street_branch_gap_2026-04-03_140536")["duplicate_of"],
            "5th_street_branch_gap_latest",
        )
        self.assertEqual(blackboard_after, "## Active Opportunities\n")


if __name__ == "__main__":
    unittest.main()
