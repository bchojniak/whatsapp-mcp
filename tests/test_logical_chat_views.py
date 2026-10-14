"""Logical chat previews retain real archive locators and page by conversation."""

import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
try:
    import requests  # noqa: F401
except ImportError:
    requests = types.ModuleType("requests")
    requests.RequestException = type("RequestException", (Exception,), {})
    requests.get = requests.post = lambda *args, **kwargs: None
    sys.modules["requests"] = requests

import whatsapp as wa
from identity import IdentityIndex

PHONE = "15550001111@s.whatsapp.net"
LID = "555@lid"
OTHER = "18880000000@s.whatsapp.net"
GROUP = "team@g.us"
TIME = "2026-10-03T10:00:00+00:00"
SCHEMA = """
CREATE TABLE chats(jid TEXT PRIMARY KEY, name TEXT, last_message_time TEXT);
CREATE TABLE messages(id TEXT, chat_jid TEXT, sender TEXT, content TEXT,
                      timestamp TEXT, is_from_me INTEGER, media_type TEXT);
"""


class LogicalChatViewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "messages.db"
        self.session = self.root / "session.db"
        with sqlite3.connect(self.path) as conn:
            conn.executescript(SCHEMA)
            conn.executemany(
                "INSERT INTO chats VALUES(?,?,?)",
                [
                    (LID, "Original title", TIME),
                    (PHONE, "Secondary title", TIME),
                    (GROUP, "Team", "2026-10-03T09:30:00+00:00"),
                    (OTHER, "Other person", "2026-10-03T08:00:00+00:00"),
                ],
            )
            conn.executemany(
                "INSERT INTO messages VALUES(?,?,?,?,?,?,?)",
                [
                    ("a", LID, "555", "first", TIME, 0, None),
                    ("a", PHONE, "15550001111", "first", TIME, 0, None),
                    ("b", LID, "555", "tie winner", TIME, 0, None),
                    (
                        "participant",
                        GROUP,
                        "555",
                        "contact participated",
                        "2026-10-03T09:00:00+00:00",
                        0,
                        None,
                    ),
                    (
                        "group-latest",
                        GROUP,
                        "18880000000",
                        "latest group message",
                        "2026-10-03T09:30:00+00:00",
                        0,
                        None,
                    ),
                    (
                        "other",
                        OTHER,
                        "18880000000",
                        "unrelated message",
                        "2026-10-03T08:00:00+00:00",
                        0,
                        None,
                    ),
                ],
            )
        with sqlite3.connect(self.session) as conn:
            conn.executescript(
                "CREATE TABLE whatsmeow_lid_map(lid TEXT,pn TEXT);"
                "INSERT INTO whatsmeow_lid_map VALUES('555','15550001111');"
                "INSERT INTO whatsmeow_lid_map VALUES('888','18880000000');"
            )
        self.identities = IdentityIndex(str(self.session))
        self.identities.refresh()
        self.identities.names[PHONE] = "João Oliveira"
        for name, value in (
            ("MESSAGES_DB_PATH", str(self.path)),
            ("identities", self.identities),
        ):
            patcher = patch.object(wa, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def execute(self, sql, params=()):
        with sqlite3.connect(self.path) as conn:
            conn.execute(sql, params)

    def add_message(self, message_id, chat=PHONE, sender="15550001111", **values):
        record = {
            "id": message_id,
            "chat_jid": chat,
            "sender": sender,
            "content": "latest alias message",
            "timestamp": "2026-10-03T11:00:00+00:00",
            "is_from_me": 0,
            "media_type": None,
            **values,
        }
        self.execute(
            "INSERT INTO messages(" + ",".join(record) + ") VALUES(?,?,?,?,?,?,?)",
            tuple(record.values()),
        )

    def test_alias_chats_collapse_before_limit_and_offset(self):
        pages = [wa.list_chats(limit=1, page=page) for page in range(4)]
        self.assertEqual([page[0].jid for page in pages[:3]], [LID, GROUP, OTHER])
        self.assertEqual(pages[3], [])
        self.assertEqual(len(wa.list_chats(limit=2)), 2)

    def test_alias_replay_does_not_change_timestamp_tie_winner(self):
        self.add_message("a", content="first", timestamp=TIME)
        chats = wa.list_chats()
        self.assertEqual(len(chats), 3)
        self.assertEqual(chats[0].last_message, "tie winner")

    def test_latest_preview_uses_all_aliases_without_changing_returned_jid(self):
        self.add_message("new")
        self.execute(
            "UPDATE chats SET last_message_time=? WHERE jid=?",
            ("2026-10-03T11:00:00+00:00", PHONE),
        )
        for requested in (LID, PHONE):
            chat = wa.get_chat(requested)
            self.assertEqual(chat.jid, LID)
            self.assertEqual(chat.last_message, "latest alias message")
            self.assertEqual(chat.name, "João Oliveira")
            self.assertEqual(chat.last_message_time.hour, 11)
        self.assertEqual(wa.list_chats()[0], wa.get_chat(PHONE))

    def test_optional_previews_preserve_chat_family_and_timestamp(self):
        chats = wa.list_chats(include_last_message=False)
        self.assertEqual(len(chats), 3)
        for chat in chats:
            self.assertIsNone(chat.last_message)
            self.assertIsNone(chat.last_sender)
            self.assertIsNone(chat.last_is_from_me)
        self.assertEqual(wa.get_chat(PHONE, False), chats[0])

    def test_search_matches_secondary_alias_titles_and_enriched_names(self):
        for query in ("Secondary", "joao", "15550001111", "555@lid"):
            with self.subTest(query=query):
                chats = wa.list_chats(query=query, limit=1)
                self.assertEqual([chat.jid for chat in chats], [LID])
                self.assertEqual(wa.list_chats(query=query, limit=1, page=1), [])

    def test_phone_or_lid_search_works_without_stored_counterpart_or_contact_name(self):
        self.identities.names.clear()
        self.execute("DELETE FROM chats WHERE jid=?", (PHONE,))
        self.assertEqual(wa.list_chats(query="15550001111")[0].jid, LID)
        self.execute("UPDATE chats SET jid=? WHERE jid=?", (PHONE, LID))
        self.assertEqual(wa.list_chats(query="555@lid")[0].jid, PHONE)

    def test_get_chat_resolves_verified_alias_without_alias_chat_row(self):
        self.execute("DELETE FROM chats WHERE jid=?", (PHONE,))
        self.add_message("new")
        self.assertEqual(wa.get_chat(PHONE).jid, LID)
        self.assertEqual(wa.get_chat(PHONE).last_message, "latest alias message")
        self.assertEqual(wa.get_direct_chat_by_contact("15550001111").jid, LID)
        self.assertIsNone(wa.get_direct_chat_by_contact("5550001111"))
        self.assertIsNone(wa.get_chat("missing@lid"))

    def test_contact_chats_page_once_and_keep_full_group_preview(self):
        for address in (LID, PHONE, "15550001111", "555"):
            with self.subTest(address=address):
                chats = wa.get_contact_chats(address)
                self.assertEqual([chat.jid for chat in chats], [LID, GROUP])
                self.assertEqual(chats[0].last_message, "tie winner")
                self.assertEqual(chats[1].last_message, "latest group message")
                self.assertEqual(
                    wa.get_contact_chats(address, limit=1, page=1), chats[1:]
                )
                self.assertEqual(wa.get_contact_chats(address, limit=1, page=2), [])
        self.assertEqual(wa.get_contact_chats("missing@lid"), [])

    def test_last_interaction_reads_alias_and_rejects_unrelated_contact(self):
        self.add_message("new")
        for address in (LID, PHONE):
            self.assertIn("latest alias message", wa.get_last_interaction(address))
            self.assertNotIn("unrelated message", wa.get_last_interaction(address))
        self.assertIsNone(wa.get_last_interaction("missing@lid"))

    def test_last_interaction_media_reference_is_an_actual_source(self):
        self.add_message("photo", chat=LID, sender="555", content="")
        self.add_message("photo", content="caption", media_type="image")
        formatted = wa.get_last_interaction(LID)
        self.assertIn("caption", formatted)
        self.assertIn("Message ID: photo", formatted)
        self.assertIn("Chat JID: " + PHONE, formatted)
        self.execute("DELETE FROM chats WHERE jid=?", (PHONE,))
        self.assertIn("Chat JID: " + PHONE, wa.get_last_interaction(PHONE))

    def test_unverified_same_name_chats_are_not_collapsed(self):
        self.identities.path = str(self.root / "missing.db")
        self.identities.loaded_at = 0
        self.execute(
            "UPDATE chats SET name='Same name' WHERE jid IN (?,?)", (LID, PHONE)
        )
        self.assertEqual(len(wa.list_chats()), 4)
        self.assertEqual(wa.get_chat(PHONE).jid, PHONE)
        self.assertEqual(wa.get_chat(LID).jid, LID)

    def test_namespace_collision_does_not_select_another_person(self):
        with sqlite3.connect(self.session) as conn:
            conn.execute(
                "INSERT INTO whatsmeow_lid_map VALUES('15550001111','19990000000')"
            )
        self.identities.loaded_at = 0
        for chat in ("wrong@g.us", "15550001111@lid"):
            self.execute(
                "INSERT INTO chats VALUES(?,?,?)", (chat, "Wrong person", TIME)
            )
            self.add_message("wrong", chat=chat, sender="15550001111@lid")
        self.add_message(
            "ambiguous",
            chat="wrong@g.us",
            sender="15550001111",
            content="ambiguous sender",
        )
        self.assertEqual(
            [chat.jid for chat in wa.get_contact_chats(PHONE)], [LID, GROUP]
        )
        self.assertNotIn("latest alias message", wa.get_last_interaction(PHONE))
        self.assertNotIn("ambiguous sender", wa.get_last_interaction(PHONE))
        self.assertEqual(wa.get_direct_chat_by_contact("15550001111").jid, LID)

    def test_offset_timestamps_choose_latest_instant_for_preview_and_order(self):
        earlier = "2026-10-03T23:00:00+14:00"
        later = "2026-10-03T08:00:00-03:00"
        self.execute("UPDATE chats SET last_message_time=? WHERE jid=?", (earlier, LID))
        self.execute("UPDATE chats SET last_message_time=? WHERE jid=?", (later, PHONE))
        self.execute(
            "UPDATE chats SET last_message_time=? WHERE jid=?",
            ("2026-10-03T10:30:00+00:00", OTHER),
        )
        self.add_message(
            "earlier",
            chat=LID,
            sender="555",
            timestamp=earlier,
            content="older instant",
        )
        self.add_message("later", timestamp=later, content="newest instant")
        chat = wa.get_chat(PHONE)
        self.assertEqual(chat.last_message, "newest instant")
        self.assertEqual(chat.last_message_time.isoformat(), later)
        self.assertEqual([item.jid for item in wa.list_chats()], [LID, OTHER, GROUP])
        self.assertIn("newest instant", wa.get_last_interaction(PHONE))
        self.assertEqual(wa.get_contact_chats(PHONE)[0].last_message, "newest instant")

    def test_media_type_matches_selected_physical_media_source(self):
        for name, kind in (
            ("url", "TEXT"),
            ("media_key", "BLOB"),
            ("file_sha256", "BLOB"),
            ("file_enc_sha256", "BLOB"),
            ("file_length", "INTEGER"),
        ):
            self.execute(f"ALTER TABLE messages ADD COLUMN {name} {kind}")
        self.add_message("variant", chat=LID, sender="555", media_type="image")
        self.add_message("variant", media_type="audio")
        self.execute(
            "UPDATE messages SET url='fixture-reference',media_key=X'01',"
            "file_sha256=X'01',file_enc_sha256=X'01',file_length=1 WHERE id=? AND chat_jid=?",
            ("variant", PHONE),
        )
        formatted = wa.get_last_interaction(PHONE)
        self.assertIn("[audio - Message ID: variant - Chat JID: " + PHONE, formatted)

    def test_unambiguous_bare_lid_direct_lookup_is_preserved(self):
        self.assertEqual(wa.get_direct_chat_by_contact("555").jid, LID)

    def test_preparing_logical_chats_twice_is_safe(self):
        with sqlite3.connect(self.path) as conn:
            wa._prepare_logical_chats(conn)
            first = conn.execute("SELECT * FROM _logical_chats ORDER BY jid").fetchall()
            wa._prepare_logical_chats(conn)
            self.assertEqual(
                first,
                conn.execute("SELECT * FROM _logical_chats ORDER BY jid").fetchall(),
            )

    def test_queries_leave_raw_archive_unchanged(self):
        with sqlite3.connect(self.path) as conn:
            before = conn.execute(
                "SELECT rowid,* FROM messages ORDER BY rowid"
            ).fetchall()
            chats_before = conn.execute(
                "SELECT rowid,* FROM chats ORDER BY rowid"
            ).fetchall()
        wa.list_chats()
        wa.get_contact_chats(PHONE)
        wa.get_last_interaction(PHONE)
        wa.get_chat(PHONE)
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(
                before,
                conn.execute("SELECT rowid,* FROM messages ORDER BY rowid").fetchall(),
            )
            self.assertEqual(
                chats_before,
                conn.execute("SELECT rowid,* FROM chats ORDER BY rowid").fetchall(),
            )


if __name__ == "__main__":
    unittest.main()
