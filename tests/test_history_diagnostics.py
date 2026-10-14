"""History diagnostics stay private, bounded and honest about correlation."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
from history_diagnostics import history_diagnostics


class HistoryDiagnosticTests(unittest.TestCase):
    def test_boundary_coverage_does_not_expose_snapshot_or_identifiers(self):
        result = history_diagnostics(
            {
                "experiment": "oldest_boundary",
                "attempts": [
                    {
                        "label": "failed-1",
                        "variant": "oldest_boundary",
                        "new_older_message_ids": 2,
                        "coverage_before": {
                            "stored_rows": 10,
                            "unique_message_ids": 8,
                            "snapshot_row_id": 1234,
                            "jid": "private-address",
                        },
                        "coverage_after": {"stored_rows": 14, "unique_message_ids": 10},
                    }
                ],
            }
        )
        self.assertEqual(result["experiment"], "oldest_boundary")
        self.assertEqual(result["attempts"][0]["new_older_message_ids"], 2)
        self.assertEqual(
            result["attempts"][0]["coverage_before"]["unique_message_ids"], 8
        )
        self.assertNotIn("snapshot_row_id", str(result))
        self.assertNotIn("private", str(result))

    def test_alias_comparison_is_described_without_addresses(self):
        result = history_diagnostics(
            {
                "experiment": "alias_comparison",
                "request_budget": 4,
                "attempts": [
                    {
                        "label": "failed-1",
                        "variant": "alternate_alias",
                        "chat_type": "phone_number",
                        "jid": "private-address",
                    }
                ],
            }
        )
        self.assertEqual(result["experiment"], "alias_comparison")
        self.assertEqual(result["attempts"][0]["variant"], "alternate_alias")
        self.assertEqual(result["attempts"][0]["chat_type"], "phone_number")
        self.assertNotIn("private", str(result))

    def test_stages_survive_without_private_references(self):
        result = history_diagnostics(
            {
                "run_id": "fixture-1",
                "status": "finished",
                "diagnostics_available": True,
                "requests_started": 8,
                "inserted_rows": 0,
                "attempts": [
                    {
                        "number": 2,
                        "label": "failed-1",
                        "variant": "failed_boundary",
                        "result": "response_timeout",
                        "anchor_validation": "stored_exact",
                        "acknowledged_at": "2026-10-03T12:00:00Z",
                        "request_id": "private-id",
                        "jid": "private-jid",
                        "anchor": {"id": "private-anchor"},
                    }
                ],
                "responses": [
                    {
                        "number": 1,
                        "stored_at": "2026-10-03T12:00:01Z",
                        "duplicate_entries": 50,
                        "conversations": [
                            {
                                "labels": ["control"],
                                "entries": 50,
                                "end_flag": "unspecified",
                                "jid": "private-jid",
                            }
                        ],
                    }
                ],
            }
        )
        self.assertEqual(result["attempts"][0]["result"], "response_timeout")
        self.assertEqual(result["responses"][0]["duplicate_entries"], 50)
        self.assertNotIn("private", str(result))
        self.assertEqual(result["correlation"], "chat_and_time_only")
        self.assertIn("late replies", result["notice"])

    def test_invalid_types_dates_and_arbitrary_errors_are_filtered(self):
        result = history_diagnostics(
            {
                "status": {},
                "requests_started": True,
                "decoded_responses": -1,
                "updated_at": "2026-10-03T12:00:00",
                "attempts": [{"label": "failed-1", "result": [], "number": -1}],
                "responses": [
                    {"storage_error": "private raw exception"},
                    {"storage_error": {}},
                ],
                "protocol_details": [
                    {"kind": {}},
                    {
                        "kind": "history_notification",
                        "matching_attempts": [True, 1, 9, "private"],
                        "session_id": "private",
                    },
                ],
            }
        )
        self.assertNotIn("status", result)
        self.assertNotIn("requests_started", result)
        self.assertNotIn("decoded_responses", result)
        self.assertIsNone(result["updated_at"])
        self.assertNotIn("result", result["attempts"][0])
        self.assertNotIn("storage_error", result["responses"][0])
        self.assertEqual(result["protocol_details"][0]["matching_attempts"], [1])
        self.assertNotIn("private", str(result))

    def test_collections_are_bounded_and_unknown_stays_unknown(self):
        self.assertIsNone(history_diagnostics(None))
        result = history_diagnostics(
            {
                "attempts": [{"label": "control"}] * 100,
                "responses": [{"conversations": [{}] * 100}] * 100,
                "protocol_details": [{"kind": "peer_response"}] * 100,
            }
        )
        self.assertEqual(len(result["attempts"]), 8)
        self.assertEqual(len(result["responses"]), 16)
        self.assertEqual(len(result["responses"][0]["conversations"]), 32)
        self.assertEqual(len(result["protocol_details"]), 16)


if __name__ == "__main__":
    unittest.main()
