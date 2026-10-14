"""Retrieval workflows using the bridge's shipped SQLite schema and migrations."""

import json
import os
import re
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
import whatsapp as wa
from identity import IdentityIndex

PROJECT = Path(__file__).resolve().parents[1]


class RetrievalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "messages.db"
        self.original_path, self.original_identities = (
            wa.MESSAGES_DB_PATH,
            wa.identities,
        )
        self.addCleanup(self.restore)
        wa.MESSAGES_DB_PATH = str(self.path)
        wa.identities = IdentityIndex(str(self.root / "session.db"))
        with sqlite3.connect(self.root / "session.db") as conn:
            conn.executescript(
                "CREATE TABLE whatsmeow_lid_map(lid TEXT,pn TEXT); INSERT INTO whatsmeow_lid_map VALUES('555','5511000000000');"
            )
        source = (PROJECT / "whatsapp-bridge/store.go").read_text()
        schema = re.search(
            r"CREATE TABLE IF NOT EXISTS chats.*?(?=\n\t`)", source, re.S
        ).group()
        migrations = (PROJECT / "whatsapp-bridge/search_store.go").read_text()
        scripts = re.findall(r"tx.Exec\(`(.*?)`\)", migrations, re.S)
        with sqlite3.connect(self.path) as conn:
            conn.executescript(schema)
            for script in scripts:
                conn.executescript(script)
            conn.executemany(
                "INSERT INTO chats VALUES(?,?,?)",
                [
                    ("555@lid", "João", "2026-10-03T10:00:00Z"),
                    ("other@g.us", "Other chat", "2026-10-03T10:00:00Z"),
                ],
            )
        self.add("a", "O João confirmou o orçamento amanhã", "2026-10-03T10:00:00Z")
        self.add("b", "same-second reply", "2026-10-03T10:00:00Z")
        self.add("c", "We approved the budget", "2026-10-03T10:00:01Z")

    def restore(self):
        wa.MESSAGES_DB_PATH, wa.identities = (
            self.original_path,
            self.original_identities,
        )

    def add(
        self,
        mid,
        text,
        timestamp,
        chat="555@lid",
        media=None,
        filename=None,
        length=None,
    ):
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT INTO messages(id,chat_jid,sender,content,timestamp,is_from_me,media_type,filename,file_length) VALUES(?,?,'555',?,?,0,?,?,?)",
                (mid, chat, text, timestamp, media, filename, length),
            )

    def search(self, **kwargs):
        return wa.list_messages(
            response_format="structured", include_context=False, **kwargs
        )

    def test_exact_context_rejects_ambiguous_id_and_keeps_chat(self):
        self.add("a", "wrong chat", "2026-10-03T09:00:00Z", chat="other@g.us")
        with self.assertRaisesRegex(ValueError, "multiple chats"):
            wa.get_message_context("a")
        result = wa.get_message_context("a", chat_jid="555@lid")
        self.assertIn("João", result.message.content)
        self.assertNotIn(
            "wrong chat", wa.list_messages(chat_jid="555@lid", query="João")
        )

    def test_same_second_context_and_deduplication(self):
        result = wa.get_message_context("a", before=5, after=5, chat_jid="555@lid")
        self.assertEqual([msg.id for msg in result.after], ["b", "c"])
        text = wa.list_messages(chat_jid="555@lid")
        self.assertEqual(text.count("same-second reply"), 1)
        structured = wa.list_messages(response_format="structured")
        keys = [
            (row["chat_jid"], row["message_id"])
            for row in structured["matches"] + structured["context"]
        ]
        self.assertEqual(len(keys), len(set(keys)))

    def test_ranked_full_text_is_accent_and_word_order_insensitive(self):
        result = self.search(query="amanha orcamento joao", query_mode="full_text")
        self.assertEqual([row["message_id"] for row in result["matches"]], ["a"])
        self.assertEqual(result["matches"][0]["chat_jid"], "555@lid")

    def test_substring_default_remains_compatible(self):
        self.assertEqual(
            wa.list_messages(query="joao", include_context=False),
            "No messages to display.",
        )
        self.assertIn("João", wa.list_messages(query="João", include_context=False))

    def test_phone_chat_aliases_return_lid_messages(self):
        result = self.search(chat_jid="5511000000000@s.whatsapp.net")
        self.assertEqual(len(result["matches"]), 3)
        self.assertTrue(all(row["chat_jid"] == "555@lid" for row in result["matches"]))

    def test_cursor_pages_have_no_duplicates_and_reach_end(self):
        rows, cursor = [], None
        while True:
            result = self.search(limit=1, cursor=cursor)
            rows.extend(row["message_id"] for row in result["matches"])
            cursor = result["next_cursor"]
            if not result["has_more"]:
                break
        self.assertEqual(rows, ["c", "b", "a"])

    def test_ranked_cursor_keeps_rank_then_date_order(self):
        self.add("d", "budget budget budget", "2026-10-03T09:00:00Z")
        first = self.search(query="budget", query_mode="full_text", limit=1)
        second = self.search(
            query="budget", query_mode="full_text", limit=1, cursor=first["next_cursor"]
        )
        self.assertEqual(first["matches"][0]["message_id"], "d")
        self.assertEqual(second["matches"][0]["message_id"], "c")
        self.assertFalse(second["has_more"])

    def test_cursor_rejects_changed_filters_invalid_data_and_modified_archive(self):
        cursor = self.search(limit=1)["next_cursor"]
        with self.assertRaisesRegex(ValueError, "changed search filters"):
            self.search(query="budget", cursor=cursor)
        with self.assertRaisesRegex(ValueError, "Invalid cursor"):
            self.search(cursor="not-valid")
        with sqlite3.connect(self.path) as conn:
            conn.execute("UPDATE messages SET content='modified' WHERE id='a'")
        with self.assertRaisesRegex(ValueError, "Archive changed"):
            self.search(cursor=cursor)

    def test_chronological_cursor_ignores_new_arrivals(self):
        first = self.search(limit=1)
        self.add("new", "new arrival", "2026-10-03T11:00:00Z")
        result = self.search(limit=5, cursor=first["next_cursor"])
        self.assertEqual([row["message_id"] for row in result["matches"]], ["b", "a"])

    def test_ranked_cursor_rejects_new_arrivals_that_change_rank(self):
        self.add("d", "budget budget", "2026-10-03T09:00:00Z")
        first = self.search(limit=1, query="budget", query_mode="full_text")
        self.add("new", "new arrival", "2026-10-03T11:00:00Z")
        with self.assertRaisesRegex(ValueError, "Archive changed"):
            self.search(
                limit=1,
                query="budget",
                query_mode="full_text",
                cursor=first["next_cursor"],
            )

    def test_date_filters_compare_instants_across_timezones(self):
        self.add("offset", "timezone", "2026-10-03T07:00:02-03:00")
        result = self.search(
            after="2026-10-03T10:00:01Z", before="2026-10-03T10:00:03Z"
        )
        self.assertEqual([row["message_id"] for row in result["matches"]], ["offset"])

    def test_media_discovery_and_filename_search(self):
        self.add(
            "pdf",
            "",
            "2026-10-03T11:00:00Z",
            media="document",
            filename="orçamento.pdf",
            length=321,
        )
        result = self.search(
            media_type="document", query="orcamento", query_mode="full_text"
        )
        media = result["matches"][0]["media"]
        self.assertEqual(media["filename"], "orçamento.pdf")
        self.assertEqual(media["byte_length"], 321)
        self.assertEqual(media["availability"], "not_checked")
        self.assertNotIn("path", media)

    def test_text_budget_has_continuation_without_losing_hits(self):
        for n in range(8):
            self.add("large" + str(n), "x" * 5000, f"2026-10-03T12:00:0{n}Z")
        result = self.search(limit=20)
        self.assertTrue(result["truncated"])
        self.assertTrue(result["has_more"])
        self.assertLess(len(json.dumps(result, ensure_ascii=False)), 20_000)
        following = self.search(limit=20, cursor=result["next_cursor"])
        seen = {row["message_id"] for row in result["matches"]}
        self.assertFalse(seen & {row["message_id"] for row in following["matches"]})

    def test_media_variants_keep_source_fields_and_hide_download_secrets(self):
        for subtype in ("gif", "voice_note", "video_note"):
            kind = "audio" if subtype == "voice_note" else "video"
            self.add(subtype, "", "2026-10-03T11:00:00Z", media=kind, length=123)
            metadata = {
                "message_type": kind + "Message",
                "media": {
                    "type": kind,
                    "subtype": subtype,
                    "mime_type": "video/mp4",
                    "duration_seconds": 12,
                    "width": 0,
                    "direct_path": "/private-download-path",
                    "media_key": "private-download-key",
                },
            }
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    "INSERT INTO message_metadata VALUES(?,?,?,?,?,?,?)",
                    (
                        subtype,
                        "555@lid",
                        kind,
                        json.dumps(metadata),
                        "waE2E.Message",
                        b"private-original-payload",
                        "2026-10-03T11:00:00Z",
                    ),
                )
            result = self.search(media_type=subtype)
            self.assertEqual(
                [row["message_id"] for row in result["matches"]], [subtype]
            )
            media = result["matches"][0]["media"]
            self.assertEqual(media["source_metadata"], "captured")
            self.assertEqual(media["duration_seconds"], 12)
            self.assertEqual(media["width"], 0)
            self.assertNotIn("private-", json.dumps(result))

    def test_metadata_coverage_keeps_legacy_and_phone_gaps_unconfirmed(self):
        self.add("sticker", "", "2026-10-03T11:00:00Z", media="sticker")
        result = self.search(media_type="sticker")
        self.assertEqual(
            result["matches"][0]["media"]["source_metadata"], "not_captured"
        )
        coverage = result["coverage"]["metadata_coverage"]
        self.assertEqual(coverage["stored_media_types"], {"sticker": 1})
        self.assertEqual(coverage["media_rows_without_source_metadata"], 1)
        self.assertIsNone(coverage["phone_history_complete"])

    def test_source_context_is_bounded_and_scoped_to_exact_message(self):
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT INTO message_metadata VALUES(?,?,?,?,?,?,?)",
                (
                    "a",
                    "555@lid",
                    "text",
                    json.dumps(
                        {
                            "message_type": "text",
                            "context": {
                                "reply_to_message_id": "target",
                                "private_key": "secret",
                            },
                            "details": {"latitude": 1.5, "longitude": -2.5},
                        }
                    ),
                    "waE2E.Message",
                    b"raw",
                    "2026-10-03T11:00:00Z",
                ),
            )
        record = self.search(query="João")["matches"][0]
        self.assertEqual(record["message_context"], {"reply_to_message_id": "target"})
        self.assertEqual(
            record["message_details"], {"latitude": 1.5, "longitude": -2.5}
        )

    def test_coverage_exposes_timeouts_without_claiming_complete_history(self):
        directory = self.root / "backfill" / "fixture"
        directory.mkdir(parents=True)
        (directory / "report.json").write_text(
            json.dumps(
                {
                    "status": "finished",
                    "updated_at": "2026-10-03T10:00:00Z",
                    "chats": [
                        {"jid": "555@lid", "status": "response_timeout", "requests": 2}
                    ],
                }
            )
        )
        with patch.dict(os.environ, {"WHATSAPP_HISTORY_BACKFILL_RUN_ID": "fixture"}):
            result = self.search(chat_jid="555@lid")["coverage"]
        self.assertIsNone(result["history_complete"])
        self.assertEqual(result["stored_messages"], 3)
        self.assertEqual(
            result["chat_records"][0]["history_status"], "response_timeout"
        )

    def test_search_index_tracks_update_and_delete(self):
        with sqlite3.connect(self.path) as conn:
            conn.execute("UPDATE messages SET content='changed keyword' WHERE id='a'")
        self.assertEqual(
            self.search(query="changed", query_mode="full_text")["matches"][0][
                "message_id"
            ],
            "a",
        )
        with sqlite3.connect(self.path) as conn:
            conn.execute("DELETE FROM messages WHERE id='a'")
        self.assertEqual(
            self.search(query="changed", query_mode="full_text")["matches"], []
        )

    def test_linked_coverage_counts_unique_messages_and_checks_preference(self):
        phone = "5511000000000@s.whatsapp.net"
        self.add("a", "alias copy", "2026-10-03T10:00:00Z", chat=phone)
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT INTO history_request_preferences VALUES(?,?,?)",
                ("555@lid", phone, "2026-10-03T12:00:00Z"),
            )
        directory = self.root / "backfill" / "fixture"
        directory.mkdir(parents=True)
        (directory / "report.json").write_text(
            json.dumps(
                {
                    "status": "finished",
                    "chats": [
                        {"jid": "555@lid", "status": "response_timeout", "requests": 2},
                        {"jid": phone, "status": "no_backward_progress", "requests": 3},
                    ],
                }
            )
        )
        with (
            patch.dict(os.environ, {"WHATSAPP_HISTORY_BACKFILL_RUN_ID": "fixture"}),
            patch("archive_status.identities", wa.identities),
        ):
            coverage = self.search(chat_jid="555@lid")["coverage"]
        self.assertEqual(coverage["stored_unique_message_ids"], 3)
        self.assertEqual(coverage["duplicate_alias_rows"], 1)
        self.assertEqual(coverage["preferred_request_jid"], phone)
        self.assertEqual(
            coverage["conversation_history_status"], "no_backward_progress"
        )
        self.assertEqual(
            coverage["backfill"]["logical_conversation_outcomes"],
            {"no_backward_progress": 1},
        )
        self.assertEqual(
            coverage["backfill"]["chat_record_outcomes"]["response_timeout"], 1
        )
        self.assertIsNone(coverage["history_complete"])
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "UPDATE history_request_preferences SET request_jid='unrelated@s.whatsapp.net'"
            )
        self.assertIsNone(
            self.search(chat_jid="555@lid")["coverage"]["preferred_request_jid"]
        )

    def add_phone_copy(
        self, mid="a", text="alias copy", timestamp="2026-10-03T10:00:00Z"
    ):
        phone = "5511000000000@s.whatsapp.net"
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO chats VALUES(?,?,?)",
                (phone, "João phone", timestamp),
            )
        self.add(mid, text, timestamp, chat=phone)
        return phone

    def test_logical_dedup_precedes_pagination_and_context(self):
        self.add_phone_copy()
        results, cursor = [], None
        while True:
            response = self.search(limit=1, cursor=cursor)
            results.extend(response["matches"])
            if not response["has_more"]:
                break
            cursor = response["next_cursor"]
        self.assertEqual([row["message_id"] for row in results], ["c", "b", "a"])
        self.assertEqual(len({row["identity"]["logical_id"] for row in results}), 3)
        self.assertEqual(
            [row.id for row in wa.get_message_context("a").after], ["b", "c"]
        )
        coverage = self.search()["coverage"]
        self.assertEqual(coverage["stored_messages"], 3)
        self.assertEqual(coverage["raw_source_rows"], 4)

    def test_alias_only_match_uses_stable_representative(self):
        self.add_phone_copy(text="unique searchable variant")
        for mode in ("substring", "full_text"):
            response = self.search(query="searchable", query_mode=mode)
            self.assertEqual(len(response["matches"]), 1)
            self.assertEqual(response["matches"][0]["chat_jid"], "555@lid")
            self.assertEqual(response["matches"][0]["message_id"], "a")

    def test_alias_context_accepts_either_original_reference(self):
        phone = self.add_phone_copy()
        self.assertEqual(
            wa.get_message_context("a", chat_jid="555@lid").message,
            wa.get_message_context("a", chat_jid=phone).message,
        )

    def test_same_content_different_ids_and_cross_chat_collisions_survive(self):
        phone = self.add_phone_copy(mid="different", text="same-second reply")
        self.add("a", "unrelated", "2026-10-03T10:00:00Z", chat="other@g.us")
        self.assertEqual(len(self.search(limit=50)["matches"]), 5)
        with self.assertRaisesRegex(ValueError, "multiple chats"):
            wa.get_message_context("a")
        self.assertEqual(
            wa.get_message_context("different", chat_jid=phone).message.id, "different"
        )

    def test_same_id_conflicting_sender_direction_or_time_remains_distinct(self):
        phone = self.add_phone_copy()
        for field, value in (
            ("sender", "unrelated@s.whatsapp.net"),
            ("is_from_me", 1),
            ("timestamp", "2026-10-03T11:00:00Z"),
        ):
            with sqlite3.connect(self.path) as conn:
                conn.execute(
                    "UPDATE messages SET sender='555',is_from_me=0,timestamp='2026-10-03T10:00:00Z' WHERE chat_jid=?",
                    (phone,),
                )
                conn.execute(
                    f"UPDATE messages SET {field}=? WHERE chat_jid=?", (value, phone)
                )
            self.assertEqual(len(self.search()["matches"]), 4, field)

    def test_new_alias_does_not_duplicate_chronological_cursor(self):
        first = self.search(limit=2)
        self.add_phone_copy()
        second = self.search(cursor=first["next_cursor"])
        self.assertEqual([row["message_id"] for row in second["matches"]], ["a"])
        self.assertFalse(second["has_more"])

    def test_identity_enumeration_has_both_source_references(self):
        phone = self.add_phone_copy()
        identity = wa.get_message_identity("a", phone)
        self.assertEqual(
            identity["logical_id"],
            self.search(query="João")["matches"][0]["identity"]["logical_id"],
        )
        self.assertEqual(
            {s["chat_jid"] for s in identity["source_identifiers"]}, {phone, "555@lid"}
        )
        self.assertNotIn("private", json.dumps(identity))

    def test_alias_only_media_has_downloadable_physical_reference(self):
        phone = self.add_phone_copy()
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "UPDATE messages SET media_type='audio',filename='note.ogg',file_length=42 WHERE id='a' AND chat_jid=?",
                (phone,),
            )
            conn.execute(
                "INSERT INTO message_metadata VALUES(?,?,?,?,?,?,?)",
                (
                    "a",
                    phone,
                    "audio",
                    json.dumps(
                        {
                            "message_type": "audio",
                            "media": {
                                "subtype": "voice_note",
                                "mime_type": "audio/ogg",
                                "duration_seconds": 3,
                            },
                        }
                    ),
                    "waE2E.Message",
                    b"secret",
                    "2026-10-03T11:00:00Z",
                ),
            )
        result = self.search(media_type="voice_note")["matches"][0]
        self.assertEqual(
            result["media"]["source_reference"], {"message_id": "a", "chat_jid": phone}
        )
        self.assertEqual(result["media"]["filename"], "note.ogg")
        self.assertEqual(result["media"]["subtype"], "voice_note")
        self.assertEqual(result["media"]["duration_seconds"], 3)
        self.assertEqual(
            wa.get_message_context("a", chat_jid="555@lid").message.metadata["media"][
                "source_reference"
            ]["chat_jid"],
            phone,
        )
        from test_media import load_server

        main = load_server()
        with patch.object(main, "whatsapp_download_media") as download:
            media = main.download_media("a", "555@lid", metadata_only=True)
            download.assert_not_called()
        self.assertEqual(media.structuredContent["source_reference"]["chat_jid"], phone)

    def test_metadata_only_change_invalidates_existing_source_cursor(self):
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT INTO message_metadata VALUES(?,?,?,?,?,?,?)",
                (
                    "a",
                    "555@lid",
                    "text",
                    '{"message_type":"text"}',
                    "waE2E.Message",
                    b"raw",
                    "2026-10-03T11:00:00Z",
                ),
            )
        first = self.search(limit=1)
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "UPDATE message_metadata SET metadata_json=? WHERE message_id='a'",
                ('{"message_type":"changed"}',),
            )
        with self.assertRaisesRegex(ValueError, "Archive changed"):
            self.search(cursor=first["next_cursor"])

    def test_new_message_metadata_does_not_invalidate_source_snapshot(self):
        first = self.search(limit=1)
        self.add("new", "arrived", "2026-10-03T11:00:00Z")
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT INTO message_metadata VALUES(?,?,?,?,?,?,?)",
                (
                    "new",
                    "555@lid",
                    "text",
                    '{"message_type":"text"}',
                    "waE2E.Message",
                    b"raw",
                    "2026-10-03T11:00:00Z",
                ),
            )
        self.assertEqual(
            [
                r["message_id"]
                for r in self.search(cursor=first["next_cursor"])["matches"]
            ],
            ["b", "a"],
        )

    def prepare_complete_alias(self, original_type="audio"):
        phone = self.add_phone_copy()
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "UPDATE messages SET media_type=? WHERE id='a'", (original_type,)
            )
            conn.execute(
                "UPDATE messages SET media_type='audio',url='https://example.invalid/media',media_key=?,file_sha256=?,file_enc_sha256=?,file_length=42,filename='usable.ogg' WHERE id='a' AND chat_jid=?",
                (b"key", b"sha", b"enc", phone),
            )
        return phone

    def test_complete_alias_media_reference_preferred_to_incomplete_representative(
        self,
    ):
        phone = self.prepare_complete_alias()
        result = self.search(media_type="audio")["matches"][0]
        self.assertEqual(result["media"]["source_reference"]["chat_jid"], phone)
        self.assertEqual(result["media"]["filename"], "usable.ogg")
        self.assertNotIn("example.invalid", json.dumps(result))
        self.assertNotIn("media_key", json.dumps(result))

    def test_alias_download_failure_falls_back_to_original_cached_source(self):
        phone = self.prepare_complete_alias()
        from test_media import load_server

        main = load_server()
        cached = self.root / "cached.ogg"
        cached.write_bytes(b"cached fixture")
        with (
            patch.dict(os.environ, {"WHATSAPP_MEDIA_ROOT": str(self.root)}),
            patch.object(
                main,
                "whatsapp_download_media",
                side_effect=[(None, "expired"), (str(cached), None)],
            ) as download,
        ):
            result = main.download_media("a", "555@lid")
        self.assertEqual(
            [call.args for call in download.call_args_list],
            [("a", phone), ("a", "555@lid")],
        )
        self.assertEqual(
            result.structuredContent["source_reference"],
            {"message_id": "a", "chat_jid": "555@lid"},
        )
        self.assertNotIn(str(self.root), json.dumps(result.structuredContent))

    def test_cached_fallback_does_not_borrow_alias_mime_type(self):
        phone = self.prepare_complete_alias(original_type="image")
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT INTO message_metadata VALUES(?,?,?,?,?,?,?)",
                (
                    "a",
                    phone,
                    "audio",
                    '{"media":{"mime_type":"audio/ogg"}}',
                    "waE2E.Message",
                    b"raw",
                    "2026-10-03T11:00:00Z",
                ),
            )
        from test_media import load_server

        main = load_server()
        cached = self.root / "cached.png"
        cached.write_bytes(b"cached fixture")
        with (
            patch.dict(os.environ, {"WHATSAPP_MEDIA_ROOT": str(self.root)}),
            patch.object(
                main,
                "whatsapp_download_media",
                side_effect=[(None, "expired"), (str(cached), None)],
            ),
        ):
            result = main.download_media("a", "555@lid")
        self.assertEqual(result.structuredContent["mime_type"], "image/png")
        self.assertEqual(result.content[1].type, "image")

    def test_sync_counts_match_logical_coverage_with_explicit_raw_counts(self):
        self.add_phone_copy()
        import requests

        with patch.object(
            requests, "get", side_effect=requests.RequestException("offline")
        ):
            status = wa.get_sync_status()
        self.assertEqual(status["message_count"], 3)
        self.assertEqual(status["raw_message_count"], 4)
        self.assertEqual(status["chat_count"], 2)
        self.assertEqual(status["raw_chat_count"], 3)
        self.assertEqual(status["message_count"], status["archive"]["stored_messages"])

    def test_indexed_query_plan_uses_chat_time_index(self):
        with sqlite3.connect(self.path) as conn:
            plan = conn.execute(
                "EXPLAIN QUERY PLAN SELECT id FROM messages WHERE chat_jid=? ORDER BY julianday(timestamp) DESC,id DESC LIMIT 20",
                ("555@lid",),
            ).fetchall()
        self.assertTrue(any("messages_chat_time_idx" in row[3] for row in plan))


if __name__ == "__main__":
    unittest.main()
