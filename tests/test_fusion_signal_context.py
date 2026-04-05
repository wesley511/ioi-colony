from __future__ import annotations

import unittest

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


class FusionSignalContextTests(unittest.TestCase):
    def test_build_fusion_signal_context_uses_exact_warning_lookup(self) -> None:
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
                path="SIGNALS/normalized/waigani_monitoring_gap_2026-03-24_163003.md",
                signal_id="waigani_monitoring_gap_2026-03-24_163003",
                date="2026-03-24",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: monitoring.",
            ),
        ]
        warning_intelligence = {
            "waigani|performance_gap|assisting_cashier": {
                "pattern_id": "waigani|performance_gap|assisting_cashier",
                "occurrence_count": 3,
                "consecutive_days": 2,
                "time_span_days": 4,
                "severity_score": 0.6,
                "escalation_level": "elevated",
                "escalation_reason": "repeated warning occurrences detected",
            }
        }

        rows = worker.build_fusion_signal_context(records, warning_intelligence)
        matched_row = next(row for row in rows if row["signal_ref"] == records[0][1]["signal_id"])
        unmatched_row = next(row for row in rows if row["signal_ref"] == records[1][1]["signal_id"])

        self.assertTrue(matched_row["matched"])
        self.assertEqual(matched_row["pattern_id"], "waigani|performance_gap|assisting_cashier")
        self.assertEqual(matched_row["occurrence_count"], 3)
        self.assertEqual(matched_row["consecutive_days"], 2)
        self.assertEqual(matched_row["time_span_days"], 4)
        self.assertEqual(matched_row["severity_score"], 0.6)
        self.assertEqual(matched_row["escalation_level"], "elevated")

        self.assertFalse(unmatched_row["matched"])
        self.assertEqual(unmatched_row["pattern_id"], "")
        self.assertEqual(unmatched_row["occurrence_count"], 0)
        self.assertEqual(unmatched_row["consecutive_days"], 0)
        self.assertEqual(unmatched_row["time_span_days"], 0)
        self.assertEqual(unmatched_row["severity_score"], 0.0)
        self.assertEqual(unmatched_row["escalation_level"], "none")
        self.assertEqual(unmatched_row["notes"], "pattern_not_found_in_warning_intelligence")

    def test_build_fusion_signal_context_keeps_low_risk_signals_out_of_warning_scope(self) -> None:
        records = [
            make_record(
                path="SIGNALS/normalized/lae_malaita_branch_strength_2026-03-24_163003.md",
                signal_id="lae_malaita_branch_strength_2026-03-24_163003",
                date="2026-03-24",
                source_name="lae_malaita",
                category="branch_performance",
                signal_type="strong_performance",
                description="LAE_MALAITA is the current top-performing branch in the fusion report.",
            )
        ]
        warning_intelligence = {
            "lae_malaita|strong_performance|branch_performance": {
                "pattern_id": "lae_malaita|strong_performance|branch_performance",
                "occurrence_count": 5,
                "consecutive_days": 3,
                "time_span_days": 7,
                "severity_score": 1.0,
                "escalation_level": "critical",
            }
        }

        rows = worker.build_fusion_signal_context(records, warning_intelligence)

        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]["matched"])
        self.assertEqual(rows[0]["pattern_id"], "")
        self.assertEqual(rows[0]["notes"], "signal_not_in_warning_scope")
        self.assertEqual(rows[0]["severity_score"], 0.0)
        self.assertEqual(rows[0]["escalation_level"], "none")

    def test_build_fusion_signal_context_is_deterministic(self) -> None:
        records = [
            make_record(
                path="SIGNALS/normalized/waigani_assisting_cashier_gap_2026-03-24_163003.md",
                signal_id="waigani_assisting_cashier_gap_2026-03-24_163003",
                date="2026-03-24",
                source_name="waigani",
                category="operations",
                signal_type="performance_gap",
                description="WAIGANI shows a weak operational section: assisting_cashier.",
            )
        ]
        warning_intelligence = {
            "waigani|performance_gap|assisting_cashier": {
                "pattern_id": "waigani|performance_gap|assisting_cashier",
                "occurrence_count": 1,
                "consecutive_days": 1,
                "time_span_days": 0,
                "severity_score": 0.213333,
                "escalation_level": "none",
            }
        }

        first = worker.build_fusion_signal_context(records, warning_intelligence)
        second = worker.build_fusion_signal_context(records, warning_intelligence)

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
