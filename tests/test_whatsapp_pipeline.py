from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import scripts.process_accepted_whatsapp as accepted_processor
import scripts.parse_bale_summary as bale_parser
import scripts.parse_whatsapp_attendance as attendance_parser
import scripts.parse_whatsapp_sales as sales_parser
import scripts.parse_whatsapp_staff as staff_parser
import scripts.whatsapp_pipeline_audit as pipeline_audit
import scripts.whatsapp_gatekeeper as gatekeeper
import scripts.whatsapp_webhook_bridge as webhook_bridge
from scripts.section_normalizer import normalize_section_name
from scripts.process_accepted_whatsapp import canonical_report_type, resolve_command_template
from scripts.whatsapp_gatekeeper import ingest_file, validate_message
from scripts.whatsapp_webhook_bridge import classify_report_type


ROOT = Path(__file__).resolve().parents[1]
SALES_SAMPLE = ROOT / "RAW_INPUT" / "whatsapp" / "accepted" / "waigani" / "1774892135__waigani__sales__wesley__ts1774892135.txt"
STAFF_SAMPLE = ROOT / "RAW_INPUT" / "whatsapp" / "accepted" / "waigani" / "1774892293__waigani__staff_performance__wesley__ts1774892293.txt"
BALE_SAMPLE = ROOT / "RAW_INPUT" / "whatsapp" / "accepted" / "2026-03-31" / "1774960962__lae_malaita__bale_summary__wesley__wamid_hbglnjc1nzkxnty2mdavagasgbyzruiwm0q0mtazq0e2rtdbqty2nduyaa.txt"
MIXED_SALES_SAMPLE = ROOT / "RAW_INPUT" / "whatsapp" / "accepted" / "2026-04-01" / "1775082898__waigani__supervisor_control__wesley__wamid_hbglnjc1nzkxnty2mdavagasgbyzruiwrjg5rjzgrtrbotzbrti2muu1aa.txt"


class WhatsAppPipelineTests(unittest.TestCase):
    def store_bridge_message(self, tmp_path: Path, text: str) -> tuple[dict[str, object], dict[str, object]]:
        meta = {
            "sender_name": "Wesley",
            "message_id": "wamid.test",
            "source": "meta_webhook",
        }
        with patch.object(webhook_bridge, "WORKSPACE_ROOT", tmp_path), patch.object(
            webhook_bridge, "RAW_ROOT", tmp_path / "RAW_INPUT" / "whatsapp"
        ), patch.object(webhook_bridge.time, "time", return_value=1775000000):
            result = webhook_bridge.store_message(text=text, raw_sha256="raw-sha", meta=meta)

        meta_path = tmp_path / str(result["meta_path"])
        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        return result, metadata

    def test_sales_sample_classifies_from_first_line(self) -> None:
        report_type, classifier_reason, _ = classify_report_type(SALES_SAMPLE.read_text(encoding="utf-8"))
        self.assertEqual(report_type, "sales")
        self.assertEqual(classifier_reason, "first_line_exact")

    def test_staff_sample_passes_gatekeeper_validation(self) -> None:
        result = validate_message(STAFF_SAMPLE.read_text(encoding="utf-8"))
        self.assertTrue(result.ok)
        self.assertEqual(result.report_type, "staff_report")
        self.assertIsNotNone(result.normalized)
        self.assertEqual(result.normalized["staff_records"][0]["staff_name"], "Milford")

    def test_report_type_aliases_route_existing_parsers(self) -> None:
        self.assertEqual(canonical_report_type("sales_report"), "sales")
        self.assertEqual(canonical_report_type("staff_report"), "staff_performance")
        self.assertEqual(canonical_report_type("staff_attendance_report"), "staff_attendance")
        self.assertEqual(canonical_report_type("supervisor_control"), "staff_attendance")
        self.assertEqual(canonical_report_type("bale_summary"), "bale_summary")
        self.assertEqual(resolve_command_template("sales_report"), 'python3 -m scripts.parse_whatsapp_sales "{txt}"')
        self.assertEqual(resolve_command_template("staff_report"), 'python3 -m scripts.parse_whatsapp_staff "{txt}"')
        self.assertEqual(resolve_command_template("staff_attendance_report"), 'python3 -m scripts.parse_whatsapp_attendance "{txt}"')
        self.assertEqual(resolve_command_template("bale_summary"), 'python3 -m scripts.parse_bale_summary "{txt}"')
        self.assertEqual(classify_report_type(BALE_SAMPLE.read_text(encoding="utf-8"))[0], "bale_summary")

    def test_malformed_staff_sample_is_quarantined_with_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            malformed_text = STAFF_SAMPLE.read_text(encoding="utf-8").replace("Display: 5", "Display: 8", 1)
            input_file = tmp_path / "bad_staff.txt"
            input_file.write_text(malformed_text, encoding="utf-8")

            with patch.object(gatekeeper, "DATA_DIR", tmp_path / "DATA"), patch.object(gatekeeper, "LOGS_DIR", tmp_path / "LOGS"), patch.object(gatekeeper, "RAW_INPUT_DIR", tmp_path / "RAW_INPUT" / "whatsapp"), patch.object(gatekeeper, "NORMALIZED_DIR", tmp_path / "SIGNALS" / "normalized"), patch.object(gatekeeper, "QUARANTINE_DIR", tmp_path / "SIGNALS" / "quarantine_duplicates"), patch.object(gatekeeper, "INVALID_DIR", tmp_path / "SIGNALS" / "quarantine_invalid"), patch.object(gatekeeper, "STATE_FILE", tmp_path / "DATA" / "whatsapp_gatekeeper_state.json"):
                result = ingest_file(input_file, strict=True)

            self.assertEqual(result["status"], "rejected")
            self.assertTrue(any("Display must be between 1 and 5" in reason for reason in result["reasons"]))
            quarantined = list((tmp_path / "SIGNALS" / "quarantine_invalid").glob("*.json"))
            self.assertEqual(len(quarantined), 1)

    def test_staff_parser_eliminates_unknowns_for_valid_sample(self) -> None:
        records = staff_parser.parse_staff_records(STAFF_SAMPLE.read_text(encoding="utf-8"))
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record.branch_slug, "waigani")
        self.assertNotEqual(record.staff_name.lower(), "unknown_staff")
        self.assertNotEqual(record.canonical_section, "unknown_section")
        self.assertEqual(record.canonical_section, "kids_boys")

    def test_malformed_sales_sample_is_rejected(self) -> None:
        broken = SALES_SAMPLE.read_text(encoding="utf-8").replace("Z Reading: 4000\n", "", 1)
        result = validate_message(broken)
        self.assertFalse(result.ok)
        self.assertTrue(any("Missing required field: Z Reading" in reason for reason in result.errors))

    def test_bridge_sales_storage_uses_report_date_for_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            text = SALES_SAMPLE.read_text(encoding="utf-8").replace("Date: 30/03/26", "Date: 01/04/26", 1)

            result, metadata = self.store_bridge_message(tmp_path, text)

            self.assertEqual(result["stored_as"], "accepted")
            self.assertIn("RAW_INPUT/whatsapp/accepted/2026-04-01/", str(result["txt_path"]))
            self.assertEqual(metadata["report_date"], "2026-04-01")

    def test_bridge_quarantine_storage_uses_report_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            text = (
                SALES_SAMPLE.read_text(encoding="utf-8")
                .replace("Date: 30/03/26", "Date: 01/04/26", 1)
                .replace("Branch: Waigani", "Branch:", 1)
            )

            result, metadata = self.store_bridge_message(tmp_path, text)

            self.assertEqual(result["stored_as"], "quarantine")
            self.assertEqual(result["quarantine_reason"], "missing_branch")
            self.assertIn("RAW_INPUT/whatsapp/quarantine/2026-04-01/", str(result["txt_path"]))
            self.assertEqual(metadata["report_date"], "2026-04-01")

    def test_bridge_storage_falls_back_to_utc_now_when_date_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            text = SALES_SAMPLE.read_text(encoding="utf-8").replace("Date: 30/03/26\n\n", "", 1)
            fallback_now = datetime(2026, 4, 2, 11, 30, tzinfo=timezone.utc)

            with patch.object(webhook_bridge, "utc_now", return_value=fallback_now):
                result, metadata = self.store_bridge_message(tmp_path, text)

            self.assertEqual(result["stored_as"], "accepted")
            self.assertIn("RAW_INPUT/whatsapp/accepted/2026-04-02/", str(result["txt_path"]))
            self.assertEqual(metadata["report_date"], "2026-04-02")

    def test_bridge_metadata_includes_report_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            text = SALES_SAMPLE.read_text(encoding="utf-8").replace("Date: 30/03/26", "Date: 01/04/26", 1)

            result, metadata = self.store_bridge_message(tmp_path, text)

            txt_path = tmp_path / str(result["txt_path"])
            self.assertTrue(txt_path.exists())
            self.assertIn("report_date", metadata)
            self.assertEqual(metadata["report_date"], "2026-04-01")

    def test_mixed_sales_dump_selects_sales_without_cross_section_leakage(self) -> None:
        report_type, classifier_reason, _ = classify_report_type(MIXED_SALES_SAMPLE.read_text(encoding="utf-8"))
        self.assertEqual(report_type, "sales")
        self.assertEqual(classifier_reason, "section_best_score")

        parsed = sales_parser.parse_sales_report(MIXED_SALES_SAMPLE.read_text(encoding="utf-8"))
        self.assertEqual(parsed["customers"]["traffic"], 383)
        self.assertEqual(parsed["customers"]["traffic_source"], "Main Door")
        self.assertEqual(parsed["totals"]["sales"], 10896.0)
        self.assertEqual(parsed["totals"]["sales_source"], "explicit_total_sales")
        self.assertNotIn("supervisor_control_report", parsed)
        self.assertNotIn("All material issues have been escalated.", parsed["operations"]["notes"])

    def test_sales_parser_prefers_main_door_then_fallback_aliases(self) -> None:
        main_door_text = """DAY-END SALES REPORT
Branch: Waigani
Date: 01/04/26
Z Reading: 100
Cash Sales: 50
EFTPOS Sales: 50
Traffic: 220
Main Door: 180
Door Count: 200
Customers Served: 90
Supervisor Confirmed: YES
Over/Short Reason: NONE
"""
        parsed = sales_parser.parse_sales_report(main_door_text)
        self.assertEqual(parsed["customers"]["traffic"], 180)
        self.assertEqual(parsed["customers"]["traffic_source"], "Main Door")

        fallback_text = main_door_text.replace("Main Door: 180\n", "")
        parsed_fallback = sales_parser.parse_sales_report(fallback_text)
        self.assertEqual(parsed_fallback["customers"]["traffic"], 220)
        self.assertEqual(parsed_fallback["customers"]["traffic_source"], "Traffic")

        total_customers_text = main_door_text.replace("Main Door: 180\n", "").replace("Traffic: 220\n", "Total Customers: 210\n")
        parsed_total_customers = sales_parser.parse_sales_report(total_customers_text)
        self.assertEqual(parsed_total_customers["customers"]["traffic"], 210)
        self.assertEqual(parsed_total_customers["customers"]["traffic_source"], "Total Customers (Traffic)")

        door_count_text = main_door_text.replace("Main Door: 180\n", "").replace("Traffic: 220\n", "")
        parsed_door_count = sales_parser.parse_sales_report(door_count_text)
        self.assertEqual(parsed_door_count["customers"]["traffic"], 200)
        self.assertEqual(parsed_door_count["customers"]["traffic_source"], "Door Count")

    def test_sales_parser_staff_on_duty_uses_unique_reported_names_then_unique_cashiers(self) -> None:
        reported_text = """DAY-END SALES REPORT
Branch: Waigani
Date: 01/04/26
Z Reading: 100
Cash Sales: 50
EFTPOS Sales: 50
Total Customers (Traffic): 10
Customers Served: 5
Staff on Duty: Alice, Alice, Bob
Supervisor Confirmed: YES
Over/Short Reason: NONE
"""
        parsed_reported = sales_parser.parse_sales_report(reported_text)
        self.assertEqual(parsed_reported["operations"]["staff_on_duty"], 2)
        self.assertEqual(parsed_reported["operations"]["staff_on_duty_source"], "reported_staff_on_duty")

        fallback_text = """DAY-END SALES REPORT
Branch: Waigani
Date: 01/04/26
Till#1: Front
Cashier: Alice
T/Cash: 30
T/Card: 20
Till#2: Back
Cashier: Alice
T/Cash: 40
T/Card: 10
Till#3: Side
Cashier: Bob
T/Cash: 50
T/Card: 0
Total Customers (Traffic): 20
Customers Served: 10
Supervisor Confirmed: YES
Over/Short Reason: NONE
"""
        parsed_fallback = sales_parser.parse_sales_report(fallback_text)
        self.assertEqual(parsed_fallback["operations"]["staff_on_duty"], 2)
        self.assertEqual(parsed_fallback["operations"]["staff_on_duty_source"], "unique_cashiers")

    def test_staff_attendance_migrates_legacy_supervisor_header(self) -> None:
        attendance_text = """SUPERVISOR CONTROL REPORT
Branch: Waigani
Date: 01/04/26
1. Alice: ✔
2. Bob: Off
3. Carol: Sick Leave
Present: 1
Off: 1
Sick Leave: 1
"""
        report_type, _, _ = classify_report_type(attendance_text)
        self.assertEqual(report_type, "staff_attendance")

        result = validate_message(attendance_text)
        self.assertTrue(result.ok)
        self.assertEqual(result.report_type, "staff_attendance_report")
        self.assertEqual(result.normalized["attendance_totals"]["staff_on_duty"], 1)
        self.assertEqual(result.normalized["attendance_records"][2]["attendance_status"], "On Leave")

        parsed = attendance_parser.parse_attendance_report(attendance_text)
        self.assertEqual(parsed["attendance"]["totals"]["on_leave"], 1)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            stored, metadata = self.store_bridge_message(tmp_path, attendance_text)
            self.assertEqual(stored["stored_as"], "accepted")
            self.assertIsNone(stored["quarantine_reason"])
            self.assertEqual(metadata["report_type"], "staff_attendance")

    def test_attendance_semantics_normalize_symbols_and_labels(self) -> None:
        text = """STAFF ATTENDANCE REPORT
Branch: Waigani
Date: 01/04/26
1. Alice: ✔
2. Bob: Off
3. Carol: Leave
"""
        parsed = attendance_parser.parse_attendance_report(text)
        self.assertEqual(parsed["attendance"]["records"][0]["attendance_status"], "Present")
        self.assertEqual(parsed["attendance"]["records"][1]["attendance_status"], "Off Duty")
        self.assertEqual(parsed["attendance"]["records"][2]["attendance_status"], "On Leave")

    def test_true_legacy_supervisor_report_is_quarantined(self) -> None:
        legacy_text = """SUPERVISOR CONTROL REPORT
Branch: Waigani
Date: 01/04/26
Exception Type: STAFF_ISSUE
Details: Late opening
Action Taken: Escalated
Escalated By: Francis
Time: 08:30
Supervisor Confirmed: YES
"""
        report_type, _, _ = classify_report_type(legacy_text)
        self.assertEqual(report_type, "legacy_supervisor")

        result = validate_message(legacy_text)
        self.assertFalse(result.ok)
        self.assertIn("At least one attendance row is required", result.errors)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            stored, metadata = self.store_bridge_message(tmp_path, legacy_text)
            self.assertEqual(stored["stored_as"], "quarantine")
            self.assertEqual(stored["quarantine_reason"], "legacy_supervisor_unsupported")
            self.assertEqual(metadata["report_type"], "legacy_supervisor")

    def test_staff_attendance_totals_mismatch_moves_to_warning_lane(self) -> None:
        attendance_text = """STAFF ATTENDANCE REPORT
Branch: Waigani
Date: 01/04/26
1. Alice: ✔
2. Bob: Absent
Present: 2
Absent: 1
"""
        result = validate_message(attendance_text)
        self.assertTrue(result.ok)
        self.assertEqual(result.lane, "accepted_with_warnings")
        self.assertTrue(any("Declared present" in warning for warning in result.warnings))

    def test_sales_numeric_na_becomes_warning_lane_not_rejection(self) -> None:
        text = """DAY-END SALES REPORT
Branch: bena_road
Date: 31/03/26
Z Reading: 5738.00
Cash Sales: 5161.00
EFTPOS Sales: 577.00
Total Customers (Traffic): 468
Customers Served: NA
Staff on Duty: -
Cash Variance: 0
Over/Short Reason: NONE
Supervisor Confirmed: YES
"""
        result = validate_message(text)
        self.assertTrue(result.ok)
        self.assertEqual(result.lane, "accepted_with_warnings")
        self.assertIsNone(result.normalized["traffic"]["customers_served"])
        self.assertIsNone(result.normalized["staffing"]["staff_on_duty"])
        self.assertIn("customers_served_null", result.normalized["flags"])
        self.assertIn("staff_on_duty_null", result.normalized["flags"])

    def test_sales_numeric_strings_coerce_to_ints(self) -> None:
        text = """DAY-END SALES REPORT
Branch: bena_road
Date: 31/03/26
Z Reading: 5738.00
Cash Sales: 5161.00
EFTPOS Sales: 577.00
Total Customers (Traffic): 468.0
Customers Served: 200.0
Staff on Duty: 2.0
Cash Variance: 0
Over/Short Reason: NONE
Supervisor Confirmed: YES
"""
        result = validate_message(text)
        self.assertTrue(result.ok)
        self.assertEqual(result.lane, "accepted")
        self.assertEqual(result.normalized["traffic"]["total_customers"], 468)
        self.assertEqual(result.normalized["traffic"]["customers_served"], 200)
        self.assertEqual(result.normalized["staffing"]["staff_on_duty"], 2)
        self.assertEqual(result.normalized["flags"], [])

    def test_sales_optional_invalid_numeric_is_partial_acceptance(self) -> None:
        text = """DAY-END SALES REPORT
Branch: bena_road
Date: 31/03/26
Z Reading: 5738.00
Cash Sales: 5161.00
EFTPOS Sales: 577.00
Total Customers (Traffic): 468
Customers Served: later
Staff on Duty: team-a
Cash Variance: unknown
Over/Short Reason: NONE
Supervisor Confirmed: YES
"""
        result = validate_message(text)
        self.assertTrue(result.ok)
        self.assertEqual(result.lane, "accepted_with_warnings")
        self.assertIsNone(result.normalized["traffic"]["customers_served"])
        self.assertIsNone(result.normalized["staffing"]["staff_on_duty"])
        self.assertIsNone(result.normalized["totals"]["cash_variance"])
        self.assertIn("customers_served_invalid", result.normalized["flags"])
        self.assertIn("staff_on_duty_invalid", result.normalized["flags"])
        self.assertIn("cash_variance_invalid", result.normalized["flags"])

    def test_sales_parser_multitill_aggregation_precedence(self) -> None:
        explicit_text = """DAY-END SALES REPORT
Branch: Waigani
Date: 01/04/26
Till#1: One
Cashier: Alice
T/Cash: 30
T/Card: 20
Till#2: Two
Cashier: Bob
T/Cash: 40
T/Card: 10
Total Sales: 120
Total Customers (Traffic): 20
Customers Served: 10
Supervisor Confirmed: YES
Over/Short Reason: NONE
"""
        explicit = sales_parser.parse_sales_report(explicit_text)
        self.assertEqual(explicit["totals"]["sales"], 120.0)
        self.assertEqual(explicit["totals"]["sales_source"], "explicit_total_sales")

        till_sum_text = explicit_text.replace("Total Sales: 120\n", "")
        till_sum = sales_parser.parse_sales_report(till_sum_text)
        self.assertEqual(till_sum["totals"]["sales"], 100.0)
        self.assertEqual(till_sum["totals"]["sales_source"], "summed_tills")

        z_fallback_text = """DAY-END SALES REPORT
Branch: Waigani
Date: 01/04/26
Z Reading: 250
Total Customers (Traffic): 20
Customers Served: 10
Supervisor Confirmed: YES
Over/Short Reason: NONE
"""
        z_fallback = sales_parser.parse_sales_report(z_fallback_text)
        self.assertEqual(z_fallback["totals"]["sales"], 250.0)
        self.assertEqual(z_fallback["totals"]["sales_source"], "z_reading_fallback")

    def test_notes_kpis_are_extracted_and_malformed_tokens_flagged(self) -> None:
        text = """DAY-END SALES REPORT
Branch: bena_road
Date: 31/03/26
Z Reading: 5738.00
Cash Sales: 5161.00
EFTPOS Sales: 577.00
Total Customers (Traffic): 468
Customers Served: 200
Staff on Duty: 2
Cash Variance: 0
Over/Short Reason: NONE
Supervisor Confirmed: YES
Notes: sales per labor hour=135.00; sales per customer=12.30; conversion rate=104%; broken
"""
        parsed = sales_parser.parse_sales_report(text)
        self.assertEqual(parsed["performance"]["parsed_notes_kpis"]["sales_per_labor_hour"], 135.0)
        self.assertEqual(parsed["performance"]["parsed_notes_kpis"]["sales_per_customer"], 12.3)
        self.assertEqual(parsed["customers"]["reported_conversion_rate"], 104.0)
        self.assertTrue(any(flag.startswith("malformed_note:") for flag in parsed["flags"]))

    def test_bale_summary_ignores_total_rows_when_flattening(self) -> None:
        text = """DAILY BALE SUMMARY
Branch: Waigani
Date: 01/04/26
Bale_ID: 1
Item_Name: Mens Shorts
Total_Qty: 10
Total_Amount: 100
Status: Released

Bale_ID: 2
Item_Name: Total Amount
Total_Qty: 999
Total_Amount: 999
Status: Released
"""
        parsed = bale_parser.parse_bale_summary(text)
        self.assertEqual(len(parsed["bales"]), 1)
        self.assertEqual(parsed["bales"][0]["item_token"], "mens_shorts")

    def test_section_normalizer_cleans_adjacent_duplicate_tokens(self) -> None:
        self.assertEqual(normalize_section_name("Kids Boys"), "kids_boys")
        self.assertEqual(normalize_section_name("KIDS boys"), "kids_boys")
        self.assertEqual(normalize_section_name("Mans Mans Shorts"), "mens_shorts")

    def test_choose_messages_prefers_latest_sales_by_received_at(self) -> None:
        older = accepted_processor.AcceptedMessage(
            txt_path=Path("older.txt"),
            meta_path=Path("older.meta.json"),
            text=SALES_SAMPLE.read_text(encoding="utf-8"),
            meta={"received_at": "2026-03-30T17:35:35+00:00"},
            branch_slug="waigani",
            report_type="sales",
            received_at="2026-03-30T17:35:35+00:00",
            file_id="older",
            validation=validate_message(SALES_SAMPLE.read_text(encoding="utf-8")),
        )
        newer_text = SALES_SAMPLE.read_text(encoding="utf-8").replace("Z Reading: 4000", "Z Reading: 4500", 1)
        newer = accepted_processor.AcceptedMessage(
            txt_path=Path("newer.txt"),
            meta_path=Path("newer.meta.json"),
            text=newer_text,
            meta={"received_at": "2026-03-30T18:35:35+00:00"},
            branch_slug="waigani",
            report_type="sales",
            received_at="2026-03-30T18:35:35+00:00",
            file_id="newer",
            validation=validate_message(newer_text),
        )
        chosen, audit_rows = accepted_processor.choose_messages([older, newer])
        self.assertEqual(len(chosen), 1)
        self.assertEqual(chosen[0].file_id, "newer")
        self.assertEqual(audit_rows[0]["count"], 2)

    def test_pipeline_audit_reports_missing_staff_attendance_branches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            accepted_root = tmp_path / "RAW_INPUT" / "whatsapp" / "accepted" / "waigani"
            accepted_root.mkdir(parents=True, exist_ok=True)
            attendance_text = """STAFF ATTENDANCE REPORT
Branch: Waigani
Date: 30/03/26
1. Milford: ✔
"""
            (accepted_root / "attendance.txt").write_text(attendance_text, encoding="utf-8")

            with patch.object(pipeline_audit, "ROOT", tmp_path), patch.object(pipeline_audit, "REPORTS_DIR", tmp_path / "REPORTS"), patch.object(pipeline_audit, "DATA_DIR", tmp_path / "DATA"), patch.object(pipeline_audit, "ACCEPTED_ROOT", tmp_path / "RAW_INPUT" / "whatsapp" / "accepted"), patch.object(pipeline_audit, "PROCESSED_ROOT", tmp_path / "RAW_INPUT" / "whatsapp" / "processed"), patch.object(pipeline_audit, "NORMALIZED_DIR", tmp_path / "SIGNALS" / "normalized"), patch.object(pipeline_audit, "COLONY_MEMORY_DIR", tmp_path / "COLONY_MEMORY" / "staff_signals"):
                report_path = pipeline_audit.generate_audit()

            self.assertTrue(report_path.exists())
            payload = json.loads((tmp_path / "DATA" / "whatsapp_pipeline_audit.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["latest_staff_attendance_date"], "30/03/26")
            self.assertIn("bena_road", payload["missing_staff_attendance_branches"])


if __name__ == "__main__":
    unittest.main()
