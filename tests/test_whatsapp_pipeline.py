from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import scripts.process_accepted_whatsapp as accepted_processor
import scripts.parse_whatsapp_staff as staff_parser
import scripts.whatsapp_pipeline_audit as pipeline_audit
import scripts.whatsapp_gatekeeper as gatekeeper
import scripts.whatsapp_webhook_bridge as webhook_bridge
from scripts.process_accepted_whatsapp import canonical_report_type, resolve_command_template
from scripts.whatsapp_gatekeeper import ingest_file, validate_message
from scripts.whatsapp_webhook_bridge import classify_report_type


ROOT = Path(__file__).resolve().parents[1]
SALES_SAMPLE = ROOT / "RAW_INPUT" / "whatsapp" / "accepted" / "waigani" / "1774892135__waigani__sales__wesley__ts1774892135.txt"
STAFF_SAMPLE = ROOT / "RAW_INPUT" / "whatsapp" / "accepted" / "waigani" / "1774892293__waigani__staff_performance__wesley__ts1774892293.txt"
BALE_SAMPLE = ROOT / "RAW_INPUT" / "whatsapp" / "accepted" / "2026-03-31" / "1774960962__lae_malaita__bale_summary__wesley__wamid_hbglnjc1nzkxnty2mdavagasgbyzruiwm0q0mtazq0e2rtdbqty2nduyaa.txt"


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
        self.assertEqual(canonical_report_type("bale_summary"), "bale_summary")
        self.assertEqual(resolve_command_template("sales_report"), 'python3 -m scripts.parse_whatsapp_sales "{txt}"')
        self.assertEqual(resolve_command_template("staff_report"), 'python3 -m scripts.parse_whatsapp_staff "{txt}"')
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

    def test_pipeline_audit_reports_missing_staff_branches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            accepted_root = tmp_path / "RAW_INPUT" / "whatsapp" / "accepted" / "waigani"
            accepted_root.mkdir(parents=True, exist_ok=True)
            (accepted_root / STAFF_SAMPLE.name).write_text(STAFF_SAMPLE.read_text(encoding="utf-8"), encoding="utf-8")

            with patch.object(pipeline_audit, "ROOT", tmp_path), patch.object(pipeline_audit, "REPORTS_DIR", tmp_path / "REPORTS"), patch.object(pipeline_audit, "DATA_DIR", tmp_path / "DATA"), patch.object(pipeline_audit, "ACCEPTED_ROOT", tmp_path / "RAW_INPUT" / "whatsapp" / "accepted"), patch.object(pipeline_audit, "PROCESSED_ROOT", tmp_path / "RAW_INPUT" / "whatsapp" / "processed"), patch.object(pipeline_audit, "NORMALIZED_DIR", tmp_path / "SIGNALS" / "normalized"), patch.object(pipeline_audit, "COLONY_MEMORY_DIR", tmp_path / "COLONY_MEMORY" / "staff_signals"):
                report_path = pipeline_audit.generate_audit()

            self.assertTrue(report_path.exists())
            payload = json.loads((tmp_path / "DATA" / "whatsapp_pipeline_audit.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["latest_staff_report_date"], "30/03/26")
            self.assertIn("bena_road", payload["missing_staff_branches"])


if __name__ == "__main__":
    unittest.main()
