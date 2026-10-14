"""Continuous recovery reports expose operational state without private data."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
from archive_status import backfill_recovery_status


class BackfillRecoveryStatusTests(unittest.TestCase):
    def test_waiting_state_keeps_completeness_unknown_and_omits_private_fields(self):
        result = backfill_recovery_status(
            {
                "status": "waiting_for_phone",
                "automatic_resumes": 2,
                "phone_pauses": 3,
                "next_attempt_at": "2026-10-04T01:00:00Z",
                "chat_jid": "private-address",
                "content": "private message",
                "history_complete": True,
            }
        )
        self.assertEqual(result["status"], "waiting_for_phone")
        self.assertEqual(result["automatic_resumes"], 2)
        self.assertEqual(result["phone_pauses"], 3)
        self.assertIsNotNone(result["next_attempt_at"])
        self.assertIsNone(result["history_complete"])
        self.assertNotIn("chat_jid", result)
        self.assertNotIn("content", result)

    def test_invalid_fields_do_not_leak_or_break_status(self):
        for status in ["private message", [], {}]:
            result = backfill_recovery_status(
                {
                    "status": status,
                    "automatic_resumes": True,
                    "phone_pauses": -1,
                    "next_attempt_at": "private",
                }
            )
            self.assertEqual(result["status"], "unknown")
            self.assertNotIn("automatic_resumes", result)
            self.assertNotIn("phone_pauses", result)
            self.assertIsNone(result["next_attempt_at"])
        self.assertIsNone(backfill_recovery_status(None))


if __name__ == "__main__":
    unittest.main()
