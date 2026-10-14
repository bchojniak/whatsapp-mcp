"""Logical identity is conservative, read-only, and independent of content."""

import json
import re
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))

from identity import IdentityIndex
from logical_messages import logical_identity, logical_key, prepare_logical_messages


class LogicalMessageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.session = self.path / "session.db"
        with sqlite3.connect(self.session) as conn:
            conn.execute("CREATE TABLE whatsmeow_lid_map(lid TEXT,pn TEXT)")
            conn.execute("INSERT INTO whatsmeow_lid_map VALUES('555','15550001111')")
        self.identities = IdentityIndex(str(self.session))
        self.conn = sqlite3.connect(self.path / "messages.db")
        self.addCleanup(self.conn.close)
        self.conn.execute(
            "CREATE TABLE messages(id TEXT,chat_jid TEXT,sender TEXT,content TEXT,"
            "timestamp TEXT,is_from_me INTEGER,media_type TEXT,filename TEXT,"
            "file_length INTEGER,url TEXT,media_key BLOB)"
        )

    def add(self, message_id="same", chat="555@lid", sender="555", **values):
        record = {
            "id": message_id,
            "chat_jid": chat,
            "sender": sender,
            "content": "original",
            "timestamp": "2026-10-03T10:00:00Z",
            "is_from_me": 0,
            "media_type": "",
            "filename": "",
            "file_length": 0,
            "url": "secret-download-url",
            "media_key": b"secret-media-key",
            **values,
        }
        self.conn.execute(
            "INSERT INTO messages("
            + ",".join(record)
            + ") VALUES("
            + ",".join("?" for _ in record)
            + ")",
            list(record.values()),
        )
        return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def prepare(self, **kwargs):
        return prepare_logical_messages(self.conn, self.identities, **kwargs)

    def logical_count(self):
        return self.conn.execute("SELECT COUNT(*) FROM logical_messages").fetchone()[0]

    def test_verified_aliases_share_one_logical_id_and_keep_raw_identifiers(self):
        first = self.add()
        self.add(chat="15550001111@s.whatsapp.net", sender="15550001111")
        self.prepare()
        self.assertEqual(self.logical_count(), 1)
        key = logical_key(self.conn, "same", "555@lid")
        self.assertEqual(
            key, logical_key(self.conn, "same", "15550001111@s.whatsapp.net")
        )
        result = logical_identity(self.conn, "same", "555@lid")
        self.assertEqual(result["source_row_count"], 2)
        self.assertEqual(result["content_variant_count"], 1)
        self.assertEqual(result["canonical_chat_jid"], "15550001111@s.whatsapp.net")
        self.assertEqual(result["source_identifiers"][0]["source_rowid"], first)
        self.assertEqual(
            {row["sender"] for row in result["source_identifiers"]},
            {"555", "15550001111"},
        )
        self.assertNotIn("secret", json.dumps(result))
        self.assertFalse(result["has_more"])

    def test_message_id_author_direction_chat_and_time_are_identity_boundaries(self):
        self.add()
        self.add("different")
        self.add(chat="group@g.us")
        self.add(sender="someone_else")
        self.add(is_from_me=1)
        self.add(timestamp="2026-10-03T10:00:01Z")
        self.prepare()
        self.assertEqual(self.logical_count(), 6)
        with self.assertRaisesRegex(ValueError, "conflicting logical identities"):
            logical_identity(self.conn, "same", "555@lid")

    def test_nonempty_text_differences_are_disclosed_as_variants(self):
        self.add(content="earlier text")
        self.add(
            chat="15550001111@s.whatsapp.net",
            sender="15550001111",
            content="edited text",
        )
        self.prepare()
        self.assertEqual(self.logical_count(), 1)
        self.assertEqual(
            self.conn.execute("SELECT content FROM logical_messages").fetchone()[0],
            "earlier text",
        )
        result = logical_identity(self.conn, "same", "555@lid")
        self.assertEqual(result["content_variant_count"], 2)
        self.assertEqual(
            {row["content_preview"] for row in result["source_identifiers"]},
            {"earlier text", "edited text"},
        )

    def test_missing_fields_are_enriched_without_changing_representative_locator(self):
        first = self.add(content="", media_type="", filename=None, file_length=0)
        self.add(
            chat="15550001111@s.whatsapp.net",
            sender="15550001111",
            content="caption",
            media_type="image",
            filename="picture.jpg",
            file_length=42,
        )
        self.prepare()
        row = self.conn.execute(
            "SELECT rowid,chat_jid,content,media_type,filename,file_length FROM logical_messages"
        ).fetchone()
        self.assertEqual(row, (first, "555@lid", "caption", "image", "picture.jpg", 42))
        self.assertEqual(
            self.conn.execute(
                "SELECT content FROM main.messages WHERE rowid=?", (first,)
            ).fetchone()[0],
            "",
        )

    def test_empty_id_and_missing_identity_evidence_remain_standalone(self):
        for values in (
            {"message_id": ""},
            {"sender": ""},
            {"timestamp": None},
            {"timestamp": "not-a-time"},
            {"is_from_me": None},
        ):
            self.add(**values)
            self.add(**values)
        self.prepare()
        self.assertEqual(self.logical_count(), 10)

    def test_exact_timestamp_normalization_preserves_submicrosecond_collisions(self):
        self.add(timestamp="2026-10-03T10:00:00.123456789Z")
        self.add(
            chat="15550001111@s.whatsapp.net",
            sender="15550001111",
            timestamp="2026-10-03T07:00:00.123456789-03:00",
        )
        self.add(timestamp="2026-10-03T10:00:00.123456790Z")
        self.prepare()
        self.assertEqual(self.logical_count(), 2)

    def test_unknown_aliases_and_matching_display_names_do_not_merge(self):
        self.identities.names = {
            "888@lid": "Same Person",
            "1888@s.whatsapp.net": "Same Person",
        }
        self.add(chat="888@lid", sender="888")
        self.add(chat="1888@s.whatsapp.net", sender="1888")
        self.prepare()
        self.assertEqual(self.logical_count(), 2)

    def test_conflicting_protocol_edges_are_rejected_in_both_directions(self):
        with sqlite3.connect(self.session) as conn:
            conn.executemany(
                "INSERT INTO whatsmeow_lid_map VALUES(?,?)",
                [
                    ("555", "15550002222"),
                    ("666", "16660000000"),
                    ("777", "16660000000"),
                ],
            )
        self.add()
        self.add(chat="15550001111@s.whatsapp.net", sender="15550001111")
        self.prepare()
        self.assertEqual(self.logical_count(), 2)
        self.assertEqual(self.identities.lid_to_phone, {})
        self.assertEqual(self.identities.phone_to_lid, {})

    def test_ambiguous_bare_namespace_does_not_merge_with_either_person(self):
        with sqlite3.connect(self.session) as conn:
            conn.execute(
                "INSERT INTO whatsmeow_lid_map VALUES('15550001111','19990000000')"
            )
        self.add(sender="15550001111")
        self.add(chat="15550001111@s.whatsapp.net", sender="15550001111@s.whatsapp.net")
        self.prepare()
        self.assertEqual(self.logical_count(), 2)

    def test_same_ambiguous_bare_sender_cannot_merge_two_alias_rows(self):
        with sqlite3.connect(self.session) as conn:
            conn.execute(
                "INSERT INTO whatsmeow_lid_map VALUES('15550001111','19990000000')"
            )
        self.add(sender="15550001111")
        self.add(chat="15550001111@s.whatsapp.net", sender="15550001111")
        self.prepare()
        self.assertEqual(self.logical_count(), 2)
        self.assertIsNone(
            logical_identity(self.conn, "same", "555@lid")["canonical_sender"]
        )

    def test_ambiguous_bare_alias_lookup_does_not_choose_a_person(self):
        with sqlite3.connect(self.session) as conn:
            conn.execute(
                "INSERT INTO whatsmeow_lid_map VALUES('15550001111','19990000000')"
            )
        self.assertEqual(self.identities.aliases("15550001111"), ["15550001111"])
        self.assertIsNone(self.identities.resolve("15550001111")["phone_number"])

    def test_unknown_address_namespace_does_not_become_phone_alias(self):
        self.assertEqual(self.identities.aliases("555@newsletter"), ["555@newsletter"])
        self.assertIsNone(self.identities.resolve("555@newsletter")["phone_number"])

    def test_snapshot_is_stable_metadata_lookup_does_not_expand_it(self):
        first = self.add()
        self.add(chat="15550001111@s.whatsapp.net", sender="15550001111")
        self.add("later")
        self.prepare(snapshot=first)
        result = logical_identity(self.conn, "same", "555@lid")
        self.assertEqual(result["source_row_count"], 1)
        self.assertEqual(self.logical_count(), 1)
        self.prepare()
        self.assertEqual(self.logical_count(), 2)

    def test_preparation_is_cached_but_detects_mutation_and_alias_revision(self):
        self.add()
        revision = self.prepare()
        changes = self.conn.total_changes
        self.assertEqual(self.prepare(), revision)
        self.assertEqual(self.conn.total_changes, changes)
        self.add("new")
        self.prepare()
        self.assertEqual(self.logical_count(), 2)
        self.identities.lid_to_phone = {}
        self.identities.phone_to_lid = {}
        self.assertNotEqual(self.prepare(), revision)

    def test_readonly_preparation_does_not_commit_or_mutate_main_database(self):
        self.add()
        self.conn.commit()
        path = self.path / "messages.db"
        before = path.read_bytes()
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        self.addCleanup(connection.close)
        connection.execute("BEGIN")
        prepare_logical_messages(connection, self.identities)
        self.assertTrue(connection.in_transaction)
        self.assertEqual(path.read_bytes(), before)
        tables = connection.execute(
            "SELECT name FROM main.sqlite_master WHERE type='table'"
        ).fetchall()
        self.assertEqual(tables, [("messages",)])

    def test_connection_proxy_and_minimal_legacy_schema_are_supported(self):
        self.add()
        proxy = Mock(wraps=self.conn)
        prepare_logical_messages(proxy, self.identities)
        self.assertEqual(self.logical_count(), 1)
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        conn.execute(
            "CREATE TABLE messages(id TEXT,chat_jid TEXT,sender TEXT,content TEXT,timestamp TEXT,is_from_me INTEGER,media_type TEXT)"
        )
        prepare_logical_messages(conn, self.identities)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM logical_messages").fetchone()[0], 0
        )

    def test_source_identifiers_are_paginated_without_silent_loss(self):
        for _ in range(55):
            self.add()
        self.prepare()
        first = logical_identity(self.conn, "same", "555@lid")
        second = logical_identity(self.conn, "same", "555@lid", page=1)
        self.assertEqual(first["source_identifiers_total"], 55)
        self.assertEqual(len(first["source_identifiers"]), 50)
        self.assertTrue(first["source_identifiers_truncated"])
        self.assertEqual(first["next_page"], 1)
        self.assertEqual(len(second["source_identifiers"]), 5)
        self.assertFalse(second["has_more"])
        self.assertEqual(
            len(
                {
                    row["source_rowid"]
                    for row in first["source_identifiers"]
                    + second["source_identifiers"]
                }
            ),
            55,
        )

    def test_retained_source_versions_are_bounded_and_allowlisted(self):
        self.add()
        self.conn.execute(
            "CREATE TABLE message_metadata_versions(version_id INTEGER PRIMARY KEY,message_id TEXT,chat_jid TEXT,message_type TEXT,metadata_json TEXT,source_format TEXT,source_payload BLOB,source_kind TEXT,provenance_json TEXT,observed_at TEXT)"
        )
        self.conn.executemany(
            "INSERT INTO message_metadata_versions VALUES(?, 'same','555@lid','conversation','secret-metadata','waE2E.Message',?,'history','secret-provenance','2026-10-03T12:00:00Z')",
            [(n, b"secret-protobuf") for n in range(1, 56)],
        )
        self.prepare()
        first = logical_identity(self.conn, "same", "555@lid")
        second = logical_identity(self.conn, "same", "555@lid", page=1)
        self.assertEqual(first["retained_source_version_count"], 55)
        self.assertEqual(len(first["source_versions"]), 50)
        self.assertTrue(first["has_more"])
        self.assertEqual(len(second["source_versions"]), 5)
        self.assertFalse(second["has_more"])
        self.assertNotIn("secret", json.dumps(first))

    def create_retention_tables(self):
        project = Path(__file__).resolve().parents[1]
        for filename, constant in (
            ("message_provenance.go", "messageMetadataVersionsSchema"),
            ("message_row_provenance.go", "messageRowVersionsSchema"),
        ):
            source = (project / "whatsapp-bridge" / filename).read_text()
            schema = re.search(r"const " + constant + r" = `(.*?)`", source, re.S)
            self.conn.executescript(schema.group(1))

    def test_provider_ids_and_reply_ids_are_safe_without_becoming_alias_evidence(self):
        self.add()
        self.create_retention_tables()
        provenance = {
            "ID": "original-provider-id",
            "Chat": "555@lid",
            "Sender": "555@lid",
            "SenderAlt": "19990000000@s.whatsapp.net",
            "RecipientAlt": "123@lid",
            "conversation_jid": "555@lid",
            "PushName": "secret-personal-name",
            "MediaKey": "secret-key",
            "DeviceSentMeta": {"DestinationJID": "secret-unreviewed-field"},
        }
        metadata = {
            "context": {
                "reply_to_message_id": "replied-id",
                "reply_sender_id": "sender-id",
                "reply_chat_jid": "reply-chat",
                "quoted_payload": "secret-quoted-payload",
            },
            "media": {"media_key": "secret-media-key"},
        }
        self.conn.execute(
            "INSERT INTO message_metadata_versions VALUES(1,'same','555@lid','conversation',?,'waE2E.Message',?,'message',?,'2026-10-03T12:00:00Z')",
            (json.dumps(metadata), b"secret-protobuf", json.dumps(provenance)),
        )
        self.prepare()
        result = logical_identity(self.conn, "same", "555@lid")
        version = result["source_versions"][0]
        self.assertEqual(
            version["provider_identifiers"]["sender_alt_jid"],
            "19990000000@s.whatsapp.net",
        )
        self.assertEqual(
            version["context_identifiers"]["reply_to_message_id"], "replied-id"
        )
        self.assertEqual(result["canonical_sender"], "15550001111@s.whatsapp.net")
        self.assertNotIn("secret", json.dumps(result))

    def test_observation_times_do_not_invent_first_retrieval_or_last_seen(self):
        self.add()
        self.prepare()
        result = logical_identity(self.conn, "same", "555@lid")
        self.assertIsNone(result["observation_times"]["first_retrieved_at"])
        self.assertIsNone(result["observation_times"]["last_seen_at"])
        self.assertFalse(result["observation_times"]["repeat_sightings_recorded"])
        self.assertIsNone(result["first_retained_row_snapshot_at"])

    def test_historical_rows_preserve_edits_and_identify_reused_key_collisions(self):
        rowid = self.add()
        self.create_retention_tables()
        for version_id, sender, content in (
            (1, "555", "original"),
            (2, "555", "edited"),
            (3, "other-author", "reused key"),
        ):
            self.conn.execute(
                "INSERT INTO message_row_versions(version_id,message_rowid,message_id,chat_jid,sender,content,timestamp,is_from_me,media_type,filename,url,media_key,captured_at) VALUES(?,?,'same','555@lid',?,?,'2026-10-03T10:00:00Z',0,'','','secret-url',?,'2026-10-03T12:00:00Z')",
                (version_id, rowid, sender, content, b"secret-key"),
            )
        self.prepare()
        result = logical_identity(self.conn, "same", "555@lid", limit=2)
        self.assertEqual(self.logical_count(), 1)
        self.assertEqual(result["historical_row_version_count"], 3)
        self.assertEqual(
            result["first_retained_row_snapshot_at"], "2026-10-03T12:00:00Z"
        )
        self.assertEqual(
            result["latest_retained_row_change_at"], "2026-10-03T12:00:00Z"
        )
        self.assertIsNone(result["observation_times"]["first_retrieved_at"])
        self.assertEqual(
            [
                row["matches_current_logical_identity"]
                for row in result["historical_row_versions"]
            ],
            [True, True],
        )
        self.assertEqual(
            [row["content_preview"] for row in result["historical_row_versions"]],
            ["original", "edited"],
        )
        self.assertTrue(result["has_more"])
        second = logical_identity(self.conn, "same", "555@lid", limit=2, page=1)
        self.assertFalse(
            second["historical_row_versions"][0]["matches_current_logical_identity"]
        )
        self.assertFalse(second["has_more"])
        self.assertNotIn("secret", json.dumps(result))
        self.assertNotIn("secret", json.dumps(second))

    def test_lowercase_history_provider_ids_are_retained_and_allowlisted(self):
        self.add()
        self.create_retention_tables()
        provenance = {
            "message_id": "historical-provider-id",
            "remote_jid": "999@lid",
            "participant_jid": "200@s.whatsapp.net",
            "conversation_jid": "555@lid",
            "media_key": "secret-key",
            "history_trace_id": "secret-internal-trace",
        }
        self.conn.execute(
            "INSERT INTO message_metadata_versions VALUES(1,'same','555@lid',"
            "'conversation','{}','waWeb.WebMessageInfo',?,'history',?,"
            "'2026-10-03T12:00:00Z')",
            (b"secret-protobuf", json.dumps(provenance)),
        )
        self.prepare()
        result = logical_identity(self.conn, "same", "555@lid")
        self.assertEqual(
            result["source_versions"][0]["provider_identifiers"],
            {
                "message_id": "historical-provider-id",
                "remote_jid": "999@lid",
                "participant_jid": "200@s.whatsapp.net",
                "conversation_jid": "555@lid",
            },
        )
        self.assertNotIn("secret", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
