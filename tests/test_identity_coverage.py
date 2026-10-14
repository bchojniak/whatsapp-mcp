"""Coverage uses the same conservative logical identities as retrieval."""

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))

from archive_status import archive_coverage
from identity import IdentityIndex
from logical_messages import prepare_logical_messages
from message_metadata import (
    metadata_coverage,
    public_media_metadata,
    public_message_metadata,
    source_metadata,
)

PHONE = "5511000000000@s.whatsapp.net"
LID = "555@lid"
SENT_AT = "2026-10-03T10:00:00Z"
SCHEMA = """
CREATE TABLE chats (jid TEXT PRIMARY KEY, name TEXT, last_message_time TIMESTAMP);
CREATE TABLE messages (
    id TEXT, chat_jid TEXT, sender TEXT, content TEXT, timestamp TIMESTAMP,
    is_from_me BOOLEAN, media_type TEXT, filename TEXT, url TEXT, media_key BLOB,
    file_sha256 BLOB, file_enc_sha256 BLOB, file_length INTEGER,
    PRIMARY KEY (id, chat_jid)
);
CREATE TABLE message_metadata (
    message_id TEXT NOT NULL, chat_jid TEXT NOT NULL, message_type TEXT NOT NULL,
    metadata_json TEXT NOT NULL, source_format TEXT NOT NULL,
    source_payload BLOB NOT NULL, observed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (message_id, chat_jid)
);
"""


class IdentityCoverageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database_path = self.root / "messages.db"
        self.conn = sqlite3.connect(self.database_path)
        self.addCleanup(self.conn.close)
        self.conn.executescript(SCHEMA)
        with sqlite3.connect(self.root / "session.db") as session:
            session.executescript(
                "CREATE TABLE whatsmeow_lid_map (lid TEXT, pn TEXT);"
                "INSERT INTO whatsmeow_lid_map VALUES ('555', '5511000000000');"
            )
        self.identities = IdentityIndex(str(self.root / "session.db"))

    def add(
        self,
        message_id,
        chat=LID,
        sender="555",
        timestamp=SENT_AT,
        from_me=False,
        text="Same source message",
        media=None,
    ):
        self.conn.execute(
            "INSERT INTO messages "
            "(id,chat_jid,sender,content,timestamp,is_from_me,media_type) "
            "VALUES (?,?,?,?,?,?,?)",
            (message_id, chat, sender, text, timestamp, from_me, media),
        )

    def metadata(self, message_id, chat=LID, kind="text", observed_at=SENT_AT):
        self.conn.execute(
            "INSERT INTO message_metadata VALUES (?,?,?,?,?,?,?)",
            (
                message_id,
                chat,
                kind,
                json.dumps(
                    {
                        "message_type": kind,
                        "media": {"duration_seconds": 7, "media_key": "secret"},
                    }
                ),
                "waE2E.Message",
                b"private-source-payload",
                observed_at,
            ),
        )

    def coverage(self, aliases=None):
        return archive_coverage(
            self.conn, self.database_path, aliases, identity_index=self.identities
        )

    def test_archive_chat_and_metadata_counts_share_logical_identity(self):
        self.add("photo", media="image")
        self.add("photo", chat=PHONE, sender="5511000000000", media="image")
        self.add("later", timestamp="2026-10-03T10:00:01Z")
        self.add("photo", chat="separate@g.us", media="image")
        self.metadata("photo", chat=PHONE, kind="imageMessage")
        archive = self.coverage()
        chat = self.coverage([LID, PHONE])
        self.assertEqual(archive["stored_messages"], 3)
        self.assertEqual(archive["raw_source_rows"], 4)
        self.assertEqual(archive["duplicate_alias_rows"], 1)
        self.assertEqual(chat["stored_messages"], 2)
        self.assertEqual(chat["stored_unique_message_ids"], 2)
        self.assertEqual(chat["raw_source_rows"], 3)
        self.assertEqual(chat["duplicate_alias_rows"], 1)
        for coverage in (archive, chat):
            metadata = coverage["metadata_coverage"]
            self.assertEqual(
                metadata["stored_message_rows"], coverage["stored_messages"]
            )
            self.assertEqual(metadata["raw_source_rows"], coverage["raw_source_rows"])
            self.assertEqual(metadata["rows_with_source_metadata"], 1)
            self.assertEqual(metadata["media_rows_with_source_metadata"], 1)
            self.assertEqual(metadata["captured_message_types"], {"imageMessage": 1})
            self.assertIsNone(coverage["history_complete"])
            self.assertIsNone(metadata["phone_history_complete"])
        self.assertEqual(
            archive["metadata_coverage"]["stored_media_types"], {"image": 2}
        )
        self.assertEqual(chat["metadata_coverage"]["stored_media_types"], {"image": 1})
        self.assertEqual(chat["oldest_stored_at"], "2026-10-03T10:00:00.000Z")
        self.assertEqual(chat["newest_stored_at"], "2026-10-03T10:00:01.000Z")

    def test_same_id_collisions_with_conflicting_evidence_remain_distinct(self):
        cases = (
            ("sender", {"sender": "999"}),
            ("direction", {"from_me": True}),
            ("timestamp", {"timestamp": "2026-10-03T10:00:01Z"}),
        )
        for message_id, conflict in cases:
            self.add(message_id)
            self.add(message_id, chat=PHONE, **{"sender": "5511000000000", **conflict})
            self.metadata(message_id)
            self.metadata(message_id, chat=PHONE)
        coverage = self.coverage([LID, PHONE])
        self.assertEqual(coverage["stored_messages"], 6)
        self.assertEqual(coverage["stored_unique_message_ids"], 6)
        self.assertEqual(coverage["duplicate_alias_rows"], 0)
        self.assertEqual(coverage["metadata_coverage"]["rows_with_source_metadata"], 6)
        self.assertEqual(
            coverage["metadata_coverage"]["captured_message_types"], {"text": 6}
        )

    def test_empty_missing_or_different_ids_are_not_content_deduplicated(self):
        for message_id in ("", None):
            self.add(message_id)
            self.add(message_id, chat=PHONE, sender="5511000000000")
        self.add("one")
        self.add("two", chat=PHONE, sender="5511000000000")
        coverage = self.coverage([LID, PHONE])
        self.assertEqual(coverage["stored_messages"], 6)
        self.assertEqual(coverage["stored_unique_message_ids"], 6)
        self.assertEqual(coverage["duplicate_alias_rows"], 0)

    def test_unverified_aliases_are_not_collapsed(self):
        self.add("same")
        self.add("same", chat=PHONE, sender="5511000000000")
        self.identities = IdentityIndex(str(self.root / "missing-session.db"))
        coverage = self.coverage()
        self.assertEqual(coverage["stored_messages"], 2)
        self.assertEqual(coverage["duplicate_alias_rows"], 0)

    def test_content_variants_count_once_without_changing_raw_rows(self):
        self.add("version", text="Original source text")
        self.add(
            "version", chat=PHONE, sender="5511000000000", text="Alias replay text"
        )
        before = self.conn.execute(
            "SELECT rowid,* FROM messages ORDER BY rowid"
        ).fetchall()
        coverage = self.coverage()
        self.assertEqual(coverage["stored_messages"], 1)
        self.assertEqual(coverage["raw_source_rows"], 2)
        self.assertEqual(coverage["duplicate_alias_rows"], 1)
        after = self.conn.execute(
            "SELECT rowid,* FROM messages ORDER BY rowid"
        ).fetchall()
        self.assertEqual(before, after)

    def test_metadata_alias_fallback_and_counts_use_same_source(self):
        self.add("voice", media="audio")
        self.add("voice", chat=PHONE, sender="5511000000000", media="audio")
        self.metadata("voice", chat=PHONE, kind="audioMessage")
        self.coverage()
        public = public_message_metadata(self.conn, "voice", LID)
        self.assertEqual(public["source_metadata"], "captured")
        self.assertEqual(public["message_type"], "audioMessage")
        self.assertNotIn("secret", json.dumps(public))
        self.assertNotIn("source_payload", json.dumps(public))
        self.assertEqual(
            public_media_metadata(self.conn, "voice", LID),
            {"source_metadata": "not_captured"},
        )
        self.assertEqual(
            public_media_metadata(self.conn, "voice", PHONE)["duration_seconds"], 7
        )
        self.assertIsNone(source_metadata(self.conn, "voice", "unrelated@g.us"))

    def test_metadata_prefers_representative_and_never_counts_alias_twice(self):
        self.add("same", media="image")
        self.add("same", chat=PHONE, sender="5511000000000", media="image")
        self.metadata("same", kind="imageMessage")
        self.metadata(
            "same", chat=PHONE, kind="alternateType", observed_at="2026-10-04T00:00:00Z"
        )
        coverage = self.coverage()["metadata_coverage"]
        self.assertEqual(coverage["rows_with_source_metadata"], 1)
        self.assertEqual(coverage["captured_message_types"], {"imageMessage": 1})
        self.assertEqual(
            source_metadata(self.conn, "same", PHONE)["message_type"], "imageMessage"
        )

    def test_media_metadata_keeps_the_selected_physical_media_source(self):
        self.add("voice", text="Original text")
        self.add("voice", chat=PHONE, sender="5511000000000", media="audio")
        self.metadata("voice", kind="text")
        self.metadata("voice", chat=PHONE, kind="audioMessage")
        self.conn.execute(
            "UPDATE message_metadata SET metadata_json=? WHERE chat_jid=?",
            (json.dumps({"message_type": "text"}), LID),
        )
        self.conn.execute(
            "UPDATE message_metadata SET metadata_json=? WHERE chat_jid=?",
            (
                json.dumps(
                    {
                        "message_type": "audioMessage",
                        "media": {
                            "subtype": "voice_note",
                            "duration_seconds": 12,
                            "mime_type": "audio/ogg",
                            "media_key": "secret",
                        },
                    }
                ),
                PHONE,
            ),
        )
        coverage = self.coverage()["metadata_coverage"]
        self.assertEqual(coverage["captured_message_types"], {"text": 1})
        media = public_media_metadata(self.conn, "voice", PHONE)
        self.assertEqual(media["subtype"], "voice_note")
        self.assertEqual(media["duration_seconds"], 12)
        self.assertEqual(media["mime_type"], "audio/ogg")
        self.assertNotIn("secret", json.dumps(media))
        self.assertEqual(
            source_metadata(self.conn, "voice", PHONE)["message_type"], "text"
        )

    def test_missing_exact_media_metadata_never_borrows_alias_audio_fields(self):
        self.add("mixed", media="image")
        self.add("mixed", chat=PHONE, sender="5511000000000", media="audio")
        self.conn.execute(
            "UPDATE messages SET filename='cached.png' WHERE chat_jid=?", (LID,)
        )
        self.metadata("mixed", chat=PHONE, kind="audioMessage")
        self.conn.execute(
            "UPDATE message_metadata SET metadata_json=? WHERE chat_jid=?",
            (
                json.dumps(
                    {
                        "message_type": "audioMessage",
                        "media": {"mime_type": "audio/ogg", "duration_seconds": 45},
                    }
                ),
                PHONE,
            ),
        )
        self.coverage()
        self.assertIsNone(source_metadata(self.conn, "mixed", LID, prefer_exact=True))
        self.assertEqual(
            public_media_metadata(self.conn, "mixed", LID),
            {"source_metadata": "not_captured"},
        )
        self.assertEqual(
            public_message_metadata(self.conn, "mixed", LID)["source_metadata"],
            "captured",
        )
        self.assertEqual(
            public_media_metadata(self.conn, "mixed", PHONE)["mime_type"], "audio/ogg"
        )

    def test_standalone_metadata_coverage_supports_legacy_and_empty_archives(self):
        self.conn.execute("DROP TABLE message_metadata")
        self.add("legacy", media="document")
        result = metadata_coverage(self.conn, identity_index=self.identities)
        self.assertEqual(result["stored_message_rows"], 1)
        self.assertEqual(result["rows_without_source_metadata"], 1)
        self.assertEqual(result["media_rows_without_source_metadata"], 1)
        self.assertEqual(result["stored_media_types"], {"document": 1})
        empty = self.coverage(["empty@g.us"])
        self.assertEqual(empty["stored_messages"], 0)
        self.assertEqual(empty["raw_source_rows"], 0)
        self.assertIsNone(empty["oldest_stored_at"])
        self.assertEqual(empty["metadata_coverage"]["rows_without_source_metadata"], 0)

    def test_coverage_preserves_prepared_identity_projection(self):
        self.add("same")
        self.add("same", chat=PHONE, sender="5511000000000")
        prepare_logical_messages(self.conn, identity_index=self.identities)
        unverified = IdentityIndex(str(self.root / "missing-session.db"))
        result = archive_coverage(
            self.conn, self.database_path, identity_index=unverified
        )
        self.assertEqual(result["stored_messages"], 1)
        self.assertEqual(result["metadata_coverage"]["stored_message_rows"], 1)

    def test_read_only_archive_uses_only_reversible_temporary_projection(self):
        self.add("same")
        self.add("same", chat=PHONE, sender="5511000000000")
        self.conn.commit()
        schema_before = self.conn.execute(
            "SELECT name,sql FROM sqlite_master ORDER BY name"
        ).fetchall()
        with sqlite3.connect(
            self.database_path.as_uri() + "?mode=ro", uri=True
        ) as read_only:
            coverage = archive_coverage(
                read_only, self.database_path, identity_index=self.identities
            )
        self.assertEqual(coverage["stored_messages"], 1)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 2
        )
        schema_after = self.conn.execute(
            "SELECT name,sql FROM sqlite_master ORDER BY name"
        ).fetchall()
        self.assertEqual(schema_before, schema_after)


if __name__ == "__main__":
    unittest.main()
