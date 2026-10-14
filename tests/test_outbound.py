import hashlib
import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
from outbound import send_text_once


class OutboundTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "outbound.db"
        self.send = Mock(return_value=(True, "private bridge reply"))

    def call(self, **changes):
        args = dict(
            path=self.path,
            client="dot",
            recipient="5511999999999",
            message="Synthetic text",
            request_id="request_123",
            send=self.send,
        )
        args.update(changes)
        return send_text_once(**args)

    def test_success_survives_reopening_and_conflicting_reuse(self):
        self.assertEqual(self.call()["status"], "sent")
        self.assertEqual(self.call()["status"], "sent")
        self.assertEqual(self.call(message="Different text")["status"], "conflict")
        self.assertEqual(self.call(recipient="5511888888888")["status"], "conflict")
        self.send.assert_called_once()
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        raw = self.path.read_bytes()
        for private in (
            b"Synthetic text",
            b"5511999999999",
            b"request_123",
            b"private bridge reply",
        ):
            self.assertNotIn(private, raw)

    def test_timeout_or_failure_is_never_retried(self):
        self.send.side_effect = TimeoutError("private error")
        self.assertEqual(self.call()["status"], "unknown")
        self.send.side_effect = None
        self.assertEqual(self.call()["status"], "unknown")
        self.send.assert_called_once()
        self.assertEqual(self.call(request_id="new_attempt_123")["status"], "sent")

    def test_crash_after_reservation_never_retries(self):
        self.send.side_effect = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.call()
        self.assertEqual(self.call()["status"], "unknown")
        self.send.assert_called_once()

    def test_failure_to_save_success_keeps_the_attempt_reserved(self):
        self.call()
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "CREATE TRIGGER reject_status BEFORE UPDATE ON sends BEGIN SELECT RAISE(ABORT, 'fixture storage failure'); END"
            )
        self.assertEqual(
            self.call(request_id="update_failure_123")["status"], "unknown"
        )
        self.assertEqual(
            self.call(request_id="update_failure_123")["status"], "unknown"
        )
        self.assertEqual(self.send.call_count, 2)

    def test_unavailable_storage_never_sends(self):
        missing = self.path.parent / "missing" / "outbound.db"
        self.assertEqual(self.call(path=missing)["status"], "unknown")
        self.send.assert_not_called()

    def test_invalid_requests_never_send(self):
        for change in (
            {"recipient": "Alex"},
            {"message": " "},
            {"message": "a" * 65537},
            {"request_id": "tiny"},
        ):
            self.assertEqual(self.call(**change)["status"], "rejected")
        self.send.assert_not_called()

    def test_concurrent_retry_does_not_dispatch_twice(self):
        entered, release = threading.Event(), threading.Event()

        def send(*args):
            entered.set()
            self.assertTrue(release.wait(5))
            return True, "sent"

        with ThreadPoolExecutor(max_workers=2) as workers:
            original = workers.submit(self.call, send=send)
            self.assertTrue(entered.wait(5))
            try:
                self.assertEqual(
                    workers.submit(self.call, send=send).result(5)["status"], "unknown"
                )
                with sqlite3.connect(self.path) as conn:
                    self.assertEqual(
                        conn.execute("SELECT count(*) FROM sends").fetchone()[0], 1
                    )
            finally:
                release.set()
            self.assertEqual(original.result(5)["status"], "sent")

    def test_request_ids_are_scoped_to_the_authenticated_client(self):
        self.call()
        self.call(client="other")
        self.assertEqual(self.send.call_count, 2)

    def test_message_reference_survives_retry_without_storing_recipient(self):
        self.send.return_value = (
            True,
            {"message_id": "sent_reference_123", "archive_status": "pending"},
        )
        first = self.call()
        self.assertEqual(first["message_id"], "sent_reference_123")
        self.assertEqual(first["chat_jid"], "5511999999999@s.whatsapp.net")
        self.assertEqual(first["archive_status"], "pending")
        self.assertEqual(self.call(), first)
        self.send.assert_called_once()
        self.assertNotIn(b"5511999999999", self.path.read_bytes())

    def test_upgrade_preserves_old_successful_and_uncertain_reservations(self):
        payload = hashlib.sha256(
            json.dumps(["5511999999999", "Synthetic text"], ensure_ascii=False).encode()
        ).hexdigest()
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "CREATE TABLE sends(client TEXT,request_hash TEXT,payload_hash TEXT,status TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(client,request_hash))"
            )
            for request_id, status in (
                ("request_123", "sent"),
                ("unknown_123", "unknown"),
            ):
                conn.execute(
                    "INSERT INTO sends(client,request_hash,payload_hash,status) VALUES(?,?,?,?)",
                    (
                        "dot",
                        hashlib.sha256(request_id.encode()).hexdigest(),
                        payload,
                        status,
                    ),
                )
        self.assertEqual(self.call()["status"], "sent")
        self.assertEqual(self.call(request_id="unknown_123")["status"], "unknown")
        self.send.assert_not_called()

    def test_archive_failure_does_not_change_accepted_send_to_retryable_failure(self):
        self.send.return_value = (
            True,
            {"message_id": "archive_failed_123", "archive_status": "failed"},
        )
        self.assertEqual(self.call()["status"], "sent")
        self.assertEqual(self.call()["archive_status"], "failed")
        self.send.assert_called_once()
