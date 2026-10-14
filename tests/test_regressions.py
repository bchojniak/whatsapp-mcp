"""Offline integration tests with real SQLite; external HTTP/MCP are mocked."""

import os
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
try:
    import requests
except ImportError:
    requests = types.ModuleType("requests")
    requests.RequestException = type("RequestException", (Exception,), {})
    requests.get = requests.post = lambda *a, **k: None
    sys.modules["requests"] = requests

import whatsapp as wa
from identity import IdentityIndex


class RegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        wa.MESSAGES_DB_PATH = str(self.root / "messages.db")
        conn = sqlite3.connect(wa.MESSAGES_DB_PATH)
        conn.executescript("""
            CREATE TABLE chats(jid TEXT PRIMARY KEY, name TEXT, last_message_time TEXT);
            CREATE TABLE messages(id TEXT, chat_jid TEXT, sender TEXT, content TEXT,
                                  timestamp TEXT, is_from_me INTEGER, media_type TEXT);
            INSERT INTO chats VALUES('999@lid', '999', '2026-10-01T04:00:00+00:00');
            INSERT INTO chats VALUES('group@g.us', 'Group', '2026-10-01T03:00:00+00:00');
            INSERT INTO messages VALUES('a', '999@lid', '999', 'first', '2026-10-01T04:00:00+00:00', 0, 'image');
            INSERT INTO messages VALUES('b', '999@lid', '999', 'second', '2026-10-01T04:00:00+00:00', 0, NULL);
            INSERT INTO messages VALUES('c', 'group@g.us', '999', 'group message', '2026-10-01T03:00:00+00:00', 0, NULL);
        """)
        conn.commit()
        conn.close()
        self.session = self.root / "whatsapp.db"
        conn = sqlite3.connect(self.session)
        conn.executescript("""
            CREATE TABLE whatsmeow_lid_map(lid TEXT, pn TEXT);
            CREATE TABLE whatsmeow_contacts(our_jid TEXT, their_jid TEXT, first_name TEXT,
                full_name TEXT, push_name TEXT, business_name TEXT);
            INSERT INTO whatsmeow_lid_map VALUES('999','5511999999999');
            INSERT INTO whatsmeow_contacts VALUES('me', '5511999999999@s.whatsapp.net',
                '', 'João Oliveira', '', '');
        """)
        conn.commit()
        conn.close()
        wa.identities = IdentityIndex(str(self.session))

    def test_chat_previews_optional(self):
        self.assertEqual(len(wa.list_chats(include_last_message=False)), 2)
        self.assertIsNone(wa.get_chat("999@lid", False).last_message)

    def test_timestamp_ties_do_not_duplicate_chats(self):
        chats = wa.list_chats()
        self.assertEqual(len(chats), 2)
        self.assertEqual(chats[0].last_message, "second")

    def test_database_failure_is_not_empty_result(self):
        Path(wa.MESSAGES_DB_PATH).unlink()
        with self.assertRaises(RuntimeError):
            wa.list_chats()
        self.assertFalse(Path(wa.MESSAGES_DB_PATH).exists())

    def test_schema_error_is_explicit(self):
        conn = sqlite3.connect(wa.MESSAGES_DB_PATH)
        conn.execute("DROP TABLE chats")
        conn.commit()
        conn.close()
        with self.assertRaises(RuntimeError):
            wa.search_contacts("Joao")

    def test_lid_and_phone_resolve_name(self):
        self.assertEqual(wa.get_sender_name("999"), "João Oliveira")
        self.assertEqual(wa.get_chat("999@lid").name, "João Oliveira")

    def test_accent_insensitive_contact_search(self):
        result = wa.search_contacts("joao")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].phone_number, "5511999999999")

    def test_chat_search_uses_accent_insensitive_enriched_names(self):
        self.assertEqual(wa.list_chats(query="Joao")[0].jid, "999@lid")

    def test_phone_search_finds_lid_chat(self):
        self.assertEqual(wa.get_direct_chat_by_contact("5511999999999").jid, "999@lid")
        self.assertIn(
            "second",
            wa.list_messages(
                sender_phone_number="5511999999999", include_context=False
            ),
        )
        self.assertIn("second", wa.get_last_interaction("5511999999999"))

    def test_context_preserves_order_media_and_chat_boundary(self):
        with sqlite3.connect(wa.MESSAGES_DB_PATH) as conn:
            conn.executemany(
                "INSERT INTO messages VALUES(?, '999@lid', '999', ?, ?, 0, NULL)",
                [
                    ("older", "older", "2026-10-01T02:00:00+00:00"),
                    ("nearer", "nearer", "2026-10-01T03:00:00+00:00"),
                    ("later", "later", "2026-10-01T05:00:00+00:00"),
                    ("latest", "latest", "2026-10-01T06:00:00+00:00"),
                ],
            )
        context = wa.get_message_context("a", before=2, after=2)
        self.assertEqual(context.message.media_type, "image")
        self.assertEqual(
            [message.id for message in context.before], ["nearer", "older"]
        )
        self.assertEqual([message.id for message in context.after], ["b", "later"])
        self.assertEqual(wa.get_message_context("b", 0, 0).before, [])
        self.assertNotIn("group message", wa.list_messages(query="first", limit=1))

    def test_message_date_and_text_filters(self):
        result = wa.list_messages(
            after="2026-09-30T03:30:00+00:00",
            before="2026-10-02T04:30:00+00:00",
            chat_jid="999@lid",
            query="SECOND",
            include_context=False,
        )
        self.assertIn("second", result)
        self.assertNotIn("first", result)
        self.assertEqual(
            wa.list_messages(query="missing", include_context=False),
            "No messages to display.",
        )
        with self.assertRaisesRegex(ValueError, "Invalid date format for 'after'"):
            wa.list_messages(after="not a date")

    def test_read_connections_close_on_success_and_error(self):
        for action, error in [
            (lambda: wa.get_chat("999@lid"), None),
            (lambda: wa.get_message_context("missing"), ValueError),
        ]:
            connection = Mock(wraps=sqlite3.connect(wa.MESSAGES_DB_PATH))
            with patch.object(wa, "open_messages_db", return_value=connection):
                if error is None:
                    action()
                else:
                    with self.assertRaises(error):
                        action()
            connection.close.assert_called_once()

    def test_partial_phone_does_not_select_another_person(self):
        self.assertIsNone(wa.get_direct_chat_by_contact("9999"))

    def test_contact_chats_unique_with_latest_preview(self):
        chats = wa.get_contact_chats("5511999999999")
        self.assertEqual(len(chats), 2)
        self.assertEqual(chats[0].last_message, "second")

    def test_unmapped_lid_not_reported_as_phone_number(self):
        self.assertIsNone(wa.identities.resolve("12345@lid")["phone_number"])

    def test_missing_optional_identity_db_preserves_chat_names(self):
        wa.identities = IdentityIndex(str(self.root / "missing.db"))
        self.assertEqual(wa.get_chat("999@lid").name, "999")

    def test_ambiguous_accounts_do_not_mix_contact_names(self):
        conn = sqlite3.connect(self.session)
        conn.execute(
            "INSERT INTO whatsmeow_contacts VALUES('other','5511999999999@s.whatsapp.net','','Different name','','')"
        )
        conn.commit()
        conn.close()
        with patch.dict(os.environ, {"WHATSAPP_ACCOUNT_JID": ""}):
            self.assertIsNone(wa.identities.resolve("999@lid")["phone_number"])
            self.assertEqual(wa.identities.status, "account_selection_required")

    def test_health_does_not_infer_connected_from_http_200(self):
        response = types.SimpleNamespace(status_code=200, json=lambda: {"status": "ok"})
        with patch.object(requests, "get", return_value=response):
            status = wa.get_sync_status()
        self.assertTrue(status["bridge_api_reachable"])
        self.assertIsNone(status["connected"])
        self.assertIsNone(status["last_successful_sync_at"])
        self.assertEqual(status["message_count"], 3)

    def test_health_preserves_explicit_reauth_status(self):
        response = types.SimpleNamespace(
            status_code=200, json=lambda: {"connected": False, "requires_qr": True}
        )
        with patch.object(requests, "get", return_value=response):
            status = wa.get_sync_status()
        self.assertFalse(status["connected"])
        self.assertTrue(status["reauthentication_required"])

    def test_health_preserves_current_process_observations(self):
        response = types.SimpleNamespace(
            status_code=200,
            json=lambda: {
                "connected": True,
                "last_successful_sync_at": "2026-10-03T10:00:00Z",
                "last_event_at": "2026-10-03T10:00:01Z",
                "observation_scope": "current_process",
            },
        )
        with patch.object(requests, "get", return_value=response):
            status = wa.get_sync_status()
        self.assertTrue(status["connected"])
        self.assertEqual(status["last_successful_sync_at"], "2026-10-03T10:00:00+00:00")
        self.assertEqual(status["observation_scope"], "current_process")
        self.assertFalse(status["sync_heartbeat_available"])

    def test_health_http_timeout_still_checks_database(self):
        with patch.object(requests, "get", side_effect=requests.RequestException()):
            status = wa.get_sync_status()
        self.assertFalse(status["bridge_api_reachable"])
        self.assertTrue(status["database_readable"])

    def test_health_reports_persistent_ingestion_separately_from_connection(self):
        response = types.SimpleNamespace(
            status_code=200,
            json=lambda: {
                "connected": True,
                "observation_scope": "persistent",
                "last_successful_sync_at": "2026-10-03T10:00:00Z",
                "ingestion": {
                    "status": "degraded",
                    "diagnostics_available": True,
                    "pending_batches": 2,
                    "failed_batches": 1,
                    "unpersisted_events": 0,
                    "restart_count": 3,
                    "payload": "private-message-body",
                },
            },
        )
        with patch.object(requests, "get", return_value=response):
            status = wa.get_sync_status()
        self.assertTrue(status["connected"])
        self.assertEqual(status["observation_scope"], "persistent")
        self.assertEqual(status["ingestion"]["status"], "degraded")
        self.assertEqual(status["ingestion"]["failed_batches"], 1)
        self.assertNotIn("payload", status["ingestion"])
        self.assertFalse(status["sync_heartbeat_available"])


if __name__ == "__main__":
    unittest.main()
