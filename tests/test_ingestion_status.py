"""Operational results must stay bounded and exclude private journal contents."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
from ingestion_status import ingestion_status


class IngestionStatusTests(unittest.TestCase):
    def test_operational_metadata_survives_validation(self):
        result = ingestion_status(
            {
                "status": "degraded",
                "diagnostics_available": True,
                "pending_batches": 2,
                "failed_batches": 1,
                "failed_write_attempts": 3,
                "last_write_error": "disk_full",
                "restart_count": 2,
                "last_committed_at": "2026-10-03T12:00:00Z",
                "oldest_pending_age_seconds": 100.5,
                "recent_connection_events": [
                    {
                        "at": "2026-10-03T12:01:00Z",
                        "state": "disconnected",
                        "reason": "connection_lost",
                    }
                ],
            }
        )
        self.assertEqual(result["pending_batches"], 2)
        self.assertEqual(result["last_write_error"], "disk_full")
        self.assertEqual(len(result["recent_connection_events"]), 1)
        self.assertEqual(result["last_committed_at"], "2026-10-03T12:00:00+00:00")

    def test_private_payloads_and_raw_error_text_are_never_forwarded(self):
        result = ingestion_status(
            {
                "status": "degraded",
                "payload": "private-message-body",
                "message_id": "private-id",
                "last_write_error": "private error details",
                "coverage_notice": "private-message-body",
                "recent_connection_events": [
                    {
                        "at": "2026-10-03T12:00:00Z",
                        "state": "connected",
                        "reason": "private error",
                    }
                ],
            }
        )
        self.assertNotIn("private", str(result))
        self.assertNotIn("last_write_error", result)
        self.assertEqual(result["recent_connection_events"], [])

    def test_unknown_diagnostics_stay_unknown(self):
        self.assertIsNone(ingestion_status(None))
        self.assertIsNone(ingestion_status("healthy"))

    def test_bad_counts_dates_and_ages_are_not_reported(self):
        result = ingestion_status(
            {
                "status": "degraded",
                "pending_batches": True,
                "failed_batches": -1,
                "restart_count": 2**64,
                "oldest_pending_age_seconds": float("nan"),
                "last_committed_at": "2026-10-03T12:00:00",
                "recent_connection_events": [None, {"at": "not-a-date"}],
            }
        )
        self.assertNotIn("pending_batches", result)
        self.assertNotIn("failed_batches", result)
        self.assertNotIn("restart_count", result)
        self.assertNotIn("oldest_pending_age_seconds", result)
        self.assertIsNone(result["last_committed_at"])

    def test_connection_event_history_is_bounded(self):
        event = {
            "at": "2026-10-03T12:00:00Z",
            "state": "connected",
            "reason": "connection_established",
        }
        self.assertEqual(
            len(
                ingestion_status(
                    {"status": "healthy", "recent_connection_events": [event] * 200}
                )["recent_connection_events"]
            ),
            100,
        )

    def test_history_download_metadata_survives_validation(self):
        result = ingestion_status(
            {
                "history_downloads": {
                    "pending_notifications": 2,
                    "awaiting_storage": 1,
                    "pending_cleanup": 0,
                    "retrying_notifications": 1,
                    "failed_attempts": 3,
                    "received_notifications": 4,
                    "last_error": "download_failed",
                    "last_received_at": "2026-10-03T12:00:00Z",
                    "last_downloaded_at": "2026-10-03T12:01:00Z",
                    "last_committed_at": "2026-10-03T12:02:00Z",
                }
            }
        )["history_downloads"]
        self.assertEqual(result["pending_notifications"], 2)
        self.assertEqual(result["awaiting_storage"], 1)
        self.assertEqual(result["received_notifications"], 4)
        self.assertEqual(result["last_error"], "download_failed")
        self.assertEqual(result["last_downloaded_at"], "2026-10-03T12:01:00+00:00")

    def test_nested_history_metadata_never_forwards_private_values(self):
        result = ingestion_status(
            {
                "history_downloads": {
                    "pending_notifications": 1,
                    "last_error": "private download details",
                    "notification": {"media_key": "private-key"},
                    "message_id": "private-id",
                    "payload": "private-message-body",
                },
                "initial_history": {
                    "phase": "private-phase",
                    "transfer_type": "private-type",
                    "last_received_at": "private-date",
                    "history_complete": "private-assertion",
                    "notice": "private-message-body",
                    "payload": "private-message-body",
                    "conversation_id": "private-id",
                },
            }
        )
        self.assertNotIn("private", str(result))
        self.assertNotIn("last_error", result["history_downloads"])
        self.assertNotIn("phase", result["initial_history"])
        self.assertNotIn("transfer_type", result["initial_history"])
        self.assertIsNone(result["initial_history"]["history_complete"])

    def test_bad_nested_counts_progress_dates_and_shapes_are_not_reported(self):
        result = ingestion_status(
            {
                "history_downloads": {
                    "pending_notifications": True,
                    "awaiting_storage": -1,
                    "pending_cleanup": 2**63,
                    "retrying_notifications": 1.5,
                    "failed_attempts": "2",
                    "last_error": ["download_failed"],
                    "last_committed_at": "2026-10-03T12:00:00",
                },
                "initial_history": {
                    "phase": [],
                    "transfer_type": {},
                    "transfer_progress": True,
                },
            }
        )
        downloads = result["history_downloads"]
        for key in (
            "pending_notifications",
            "awaiting_storage",
            "pending_cleanup",
            "retrying_notifications",
            "failed_attempts",
            "last_error",
        ):
            self.assertNotIn(key, downloads)
        self.assertIsNone(downloads["last_committed_at"])
        self.assertNotIn("transfer_progress", result["initial_history"])
        for invalid_progress in (-1, 101, 99.5, "100", float("nan")):
            status = ingestion_status(
                {"initial_history": {"transfer_progress": invalid_progress}}
            )["initial_history"]
            self.assertNotIn("transfer_progress", status)
        for invalid_shape in (None, [], "private-metadata"):
            status = ingestion_status(
                {
                    "history_downloads": invalid_shape,
                    "initial_history": invalid_shape,
                }
            )
            self.assertNotIn("history_downloads", status)
            self.assertNotIn("initial_history", status)

    def test_transfer_completion_never_claims_complete_history(self):
        for phase in ("receiving", "waiting_for_storage", "backfill_ready"):
            for transfer_type in ("INITIAL_BOOTSTRAP", "RECENT", "FULL"):
                result = ingestion_status(
                    {
                        "initial_history": {
                            "phase": phase,
                            "transfer_type": transfer_type,
                            "transfer_progress": 100,
                            "history_complete": True,
                        }
                    }
                )["initial_history"]
                self.assertEqual(result["phase"], phase)
                self.assertEqual(result["transfer_type"], transfer_type)
                self.assertEqual(result["transfer_progress"], 100)
                self.assertIsNone(result["history_complete"])

    def test_download_error_categories_are_bounded(self):
        for category in (
            "download_failed",
            "receipt_failed",
            "cleanup_failed",
            "disk_full",
            "database_locked",
            "",
        ):
            result = ingestion_status({"history_downloads": {"last_error": category}})
            self.assertEqual(result["history_downloads"]["last_error"], category)


if __name__ == "__main__":
    unittest.main()
