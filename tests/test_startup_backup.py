import contextlib
import importlib.util
import io
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deploy" / "backup_store.py"
SPEC = importlib.util.spec_from_file_location("startup_backup", SCRIPT)
backup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backup)


class StartupBackupTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = Path(self.directory.name)

    def database(self, name="messages.db", *, wal=False):
        connection = sqlite3.connect(self.store / name)
        self.addCleanup(connection.close)
        if wal:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.executescript(
            "CREATE TABLE chats(jid TEXT PRIMARY KEY);"
            "CREATE TABLE messages(id INTEGER PRIMARY KEY,chat_jid TEXT REFERENCES chats(jid),content TEXT);"
            "CREATE TABLE message_metadata(message_id INTEGER PRIMARY KEY REFERENCES messages(id),payload BLOB);"
            "INSERT INTO chats VALUES('synthetic-chat');"
            "INSERT INTO messages VALUES(1,'synthetic-chat','synthetic text');"
            "INSERT INTO message_metadata VALUES(1,X'0001FF');"
        )
        if name == "whatsapp.db":
            connection.execute("CREATE TABLE whatsmeow_device(jid TEXT PRIMARY KEY)")
        return connection

    def prepare(self):
        with contextlib.redirect_stdout(io.StringIO()), backup.store_lock(self.store):
            return backup.prepare_backups(self.store)

    def attempt(self, name="messages.db"):
        return self.store / ".startup-backups" / backup.BACKUP_TRANSITION / name

    def test_online_backup_includes_committed_wal_and_private_audit_counts(self):
        source = self.database(wal=True)
        source.execute(
            "INSERT INTO messages VALUES(2,'synthetic-chat','committed in WAL')"
        )
        source.commit()
        self.assertGreater((self.store / "messages.db-wal").stat().st_size, 0)
        manifests = self.prepare()
        snapshot = self.attempt() / "snapshot.sqlite3"
        with sqlite3.connect(snapshot) as restored:
            self.assertEqual(
                restored.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 2
            )
            self.assertEqual(
                restored.execute("PRAGMA integrity_check").fetchone()[0], "ok"
            )
            self.assertEqual(
                restored.execute("PRAGMA journal_mode").fetchone()[0], "delete"
            )
        self.assertEqual(
            manifests[0]["verification"]["counts"],
            {"messages": 2, "chats": 1, "message_metadata": 1},
        )
        self.assertEqual(manifests[0]["sha256"], backup._sha256(snapshot))
        for path in (snapshot, self.attempt() / "manifest.json"):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.attempt().stat().st_mode), 0o700)
        self.assertFalse(snapshot.with_name(snapshot.name + "-wal").exists())

    def test_restart_reuses_verified_snapshot_without_copying_changed_source(self):
        source = self.database()
        self.prepare()
        before = {
            str(p): (p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns)
            for p in self.attempt().iterdir()
        }
        source.execute(
            "INSERT INTO messages VALUES(2,'synthetic-chat','later ingestion')"
        )
        source.commit()
        with mock.patch.object(
            backup, "_create_backup", side_effect=AssertionError("copied twice")
        ):
            manifests = self.prepare()
        after = {
            str(p): (p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns)
            for p in self.attempt().iterdir()
        }
        self.assertEqual(before, after)
        self.assertEqual(manifests[0]["verification"]["counts"]["messages"], 1)

    def test_completed_migration_reuse_only_requires_runtime_headroom(self):
        source = self.database()
        self.prepare()
        source.executescript(
            "CREATE TABLE message_metadata_versions(version_id INTEGER);"
            "CREATE TABLE message_row_versions(version_id INTEGER);"
        )
        for table, prefix in (
            ("messages", "message_rows"),
            ("message_metadata", "message_metadata"),
        ):
            for suffix, event in (
                ("insert", "AFTER INSERT"),
                ("before_update", "BEFORE UPDATE"),
                ("after_update", "AFTER UPDATE"),
            ):
                source.execute(
                    f"CREATE TRIGGER {prefix}_retain_{suffix} {event} ON {table} BEGIN SELECT 1; END"
                )
        source.execute("UPDATE messages SET content=?", ("x" * 2_000_000,))
        source.commit()
        free = backup.MINIMUM_HEADROOM + 1024 * 1024
        usage = shutil._ntuple_diskusage(10**12, 0, free)
        with (
            mock.patch.object(backup.shutil, "disk_usage", return_value=usage),
            mock.patch.object(
                backup, "_create_backup", side_effect=AssertionError("copied twice")
            ),
        ):
            self.prepare()

    def test_missing_previously_backed_sources_never_initialize_new_store(self):
        for name in backup.DATABASE_NAMES:
            self.database(name).close()
        self.prepare()
        (self.store / "whatsapp.db").rename(self.store / "preserved-session.sqlite3")
        with self.assertRaisesRegex(
            backup.BackupFailure, "previously_backed_source_missing"
        ):
            self.prepare()
        (self.store / "messages.db").rename(self.store / "preserved-archive.sqlite3")
        with self.assertRaisesRegex(
            backup.BackupFailure, "previously_backed_source_missing"
        ):
            self.prepare()

    def test_production_requires_both_existing_databases_even_without_manifests(self):
        with (
            backup.store_lock(self.store),
            self.assertRaisesRegex(
                backup.BackupFailure, "required_source_database_missing"
            ),
        ):
            backup.prepare_backups(self.store, require_existing=True)
        self.database()
        with (
            backup.store_lock(self.store),
            self.assertRaisesRegex(
                backup.BackupFailure, "required_source_database_missing"
            ),
        ):
            backup.prepare_backups(self.store, require_existing=True)

    def test_container_device_remount_is_informational_but_replaced_inode_blocks(self):
        self.database().close()
        manifests = self.prepare()
        altered = dict(backup._source_details(self.store / "messages.db"))
        altered["device"] += 1
        self.assertEqual(
            backup._reuse_backup(self.attempt(), self.store / "messages.db", altered),
            manifests[0],
        )
        (self.store / "messages.db").rename(self.store / "preserved-original.sqlite3")
        shutil.copyfile(self.attempt() / "snapshot.sqlite3", self.store / "messages.db")
        with self.assertRaisesRegex(
            backup.BackupFailure, "source_identity_changed_requires_review"
        ):
            self.prepare()

    def test_required_empty_or_wrong_schema_session_is_not_initialized(self):
        self.database()
        session = self.store / "whatsapp.db"
        session.touch()
        with (
            backup.store_lock(self.store),
            self.assertRaisesRegex(
                backup.BackupFailure, "required_source_database_uninitialized"
            ),
        ):
            backup.prepare_backups(self.store, require_existing=True)
        self.assertEqual(session.stat().st_size, 0)
        with sqlite3.connect(session) as wrong:
            wrong.execute("CREATE TABLE unrelated(value TEXT)")
        with (
            backup.store_lock(self.store),
            self.assertRaisesRegex(
                backup.BackupFailure, "required_source_database_uninitialized"
            ),
        ):
            backup.prepare_backups(self.store, require_existing=True)
        self.assertFalse(self.attempt().exists())

    def test_missing_sources_create_no_snapshot_and_later_database_gets_one(self):
        self.assertEqual(self.prepare(), [])
        self.assertFalse((self.store / ".startup-backups").exists())
        self.database()
        self.prepare()
        original_hash = backup._sha256(self.attempt() / "snapshot.sqlite3")
        self.database("whatsapp.db")
        self.assertEqual(len(self.prepare()), 2)
        self.assertEqual(
            backup._sha256(self.attempt() / "snapshot.sqlite3"), original_hash
        )
        self.assertTrue((self.attempt("whatsapp.db") / "manifest.json").is_file())

    def test_corrupt_source_blocks_without_overwriting_it(self):
        source = self.store / "messages.db"
        source.write_bytes(b"synthetic corrupt database")
        before = source.read_bytes()
        with self.assertRaises(sqlite3.DatabaseError):
            self.prepare()
        self.assertEqual(source.read_bytes(), before)
        self.assertFalse(self.attempt().exists())

    def test_foreign_key_violation_blocks_completion(self):
        source = self.database()
        source.execute("INSERT INTO messages VALUES(2,'missing-chat','orphan')")
        source.commit()
        with self.assertRaisesRegex(
            backup.BackupFailure, "database_foreign_key_failed"
        ):
            self.prepare()
        self.assertFalse((self.attempt() / "manifest.json").exists())
        self.assertTrue((self.attempt() / "snapshot.sqlite3").exists())

    def test_interrupted_attempt_stays_preserved_and_restart_does_not_fill_disk(self):
        self.database()
        real_create = backup._create_file

        def interrupted(path, contents=b""):
            if path.name == "manifest.json":
                raise OSError("synthetic interruption")
            real_create(path, contents)

        with (
            mock.patch.object(backup, "_create_file", side_effect=interrupted),
            self.assertRaises(OSError),
        ):
            self.prepare()
        before = {str(p): p.read_bytes() for p in self.attempt().iterdir()}
        for _ in range(3):
            with self.assertRaisesRegex(
                backup.BackupFailure, "incomplete_backup_requires_review"
            ):
                self.prepare()
        self.assertEqual(
            before, {str(p): p.read_bytes() for p in self.attempt().iterdir()}
        )

    def test_torn_manifest_is_not_replaced(self):
        self.database()
        self.prepare()
        manifest = self.attempt() / "manifest.json"
        manifest.write_text('{"complete":')
        with self.assertRaisesRegex(backup.BackupFailure, "backup_manifest_invalid"):
            self.prepare()
        self.assertEqual(manifest.read_text(), '{"complete":')

    def test_changed_snapshot_hash_and_sidecars_block_reuse(self):
        self.database()
        self.prepare()
        snapshot = self.attempt() / "snapshot.sqlite3"
        sidecar = snapshot.with_name(snapshot.name + "-wal")
        sidecar.write_bytes(b"unexpected WAL")
        with self.assertRaisesRegex(backup.BackupFailure, "backup_has_sidecars"):
            self.prepare()
        sidecar.rename(self.attempt() / "preserved-unexpected-wal")
        with snapshot.open("ab") as output:
            output.write(b"changed")
        with self.assertRaisesRegex(backup.BackupFailure, "backup_manifest_mismatch"):
            self.prepare()

    def test_insufficient_disk_preflight_includes_additive_migration_budget(self):
        self.database()
        size = backup._source_details(self.store / "messages.db")["estimated_bytes"]
        free = backup.MINIMUM_HEADROOM + 3 * size - 1
        usage = shutil._ntuple_diskusage(10**12, 0, free)
        with (
            mock.patch.object(backup.shutil, "disk_usage", return_value=usage),
            self.assertRaisesRegex(backup.BackupFailure, "insufficient_disk_space"),
        ):
            self.prepare()
        self.assertFalse(self.attempt().exists())

    def test_low_space_during_copy_keeps_incomplete_attempt(self):
        self.database()
        enough = shutil._ntuple_diskusage(10**12, 0, 10**12)
        exhausted = shutil._ntuple_diskusage(10**12, 10**12, 0)
        with (
            mock.patch.object(
                backup.shutil, "disk_usage", side_effect=[enough, exhausted]
            ),
            self.assertRaisesRegex(backup.BackupFailure, "insufficient_disk_space"),
        ):
            self.prepare()
        self.assertTrue((self.attempt() / "snapshot.sqlite3").exists())
        self.assertFalse((self.attempt() / "manifest.json").exists())

    def test_concurrent_wal_transactions_produce_one_consistent_snapshot(self):
        source = self.database(wal=True)
        source.executescript(
            "CREATE TABLE paired(a INTEGER,b INTEGER);INSERT INTO paired VALUES(0,0);"
        )
        stopped = threading.Event()
        errors = []

        def writer():
            try:
                with sqlite3.connect(self.store / "messages.db") as connection:
                    for value in range(1, 80):
                        connection.execute("UPDATE paired SET a=?,b=?", (value, value))
                        connection.commit()
                        if stopped.wait(0.001):
                            break
            except sqlite3.Error as error:
                errors.append(error)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            self.prepare()
        finally:
            stopped.set()
            thread.join()
        self.assertEqual(errors, [])
        with sqlite3.connect(self.attempt() / "snapshot.sqlite3") as restored:
            left, right = restored.execute("SELECT a,b FROM paired").fetchone()
            self.assertEqual(left, right)

    def test_symlinked_source_and_backup_directory_fail_closed(self):
        real = self.store / "outside.db"
        real.write_bytes(b"do not read")
        (self.store / "messages.db").symlink_to(real)
        with self.assertRaisesRegex(backup.BackupFailure, "unsafe_source_file"):
            self.prepare()
        self.assertEqual(real.read_bytes(), b"do not read")

    def test_lock_is_exclusive_and_survives_exec_for_bridge_lifetime(self):
        self.database()
        bridge = self.store / "synthetic_bridge.py"
        bridge.write_text(
            f"#!{sys.executable}\nimport time\nprint('SYNTHETIC_BRIDGE_READY',flush=True)\ntime.sleep(30)\n"
        )
        bridge.chmod(0o700)
        process = subprocess.Popen(
            [
                sys.executable,
                str(SCRIPT),
                "--store-dir",
                str(self.store),
                "--bridge",
                str(bridge),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.addCleanup(lambda: process.poll() is None and process.kill())
        try:
            for _ in range(5):
                line = process.stdout.readline()
                if "SYNTHETIC_BRIDGE_READY" in line:
                    break
            else:
                self.fail("bridge was not executed after backup")
            with (
                self.assertRaisesRegex(backup.BackupFailure, "writer_lock_busy"),
                backup.store_lock(self.store),
            ):
                self.fail("second writer acquired the lock")
        finally:
            process.terminate()
            process.communicate(timeout=5)
        with backup.store_lock(self.store):
            pass

    def test_cli_failure_never_executes_bridge_and_logs_no_payload(self):
        secret = "synthetic-secret-content-that-must-not-be-logged"
        (self.store / "messages.db").write_text(secret)
        with mock.patch.object(backup.os, "execv") as execute:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                status = backup.main(
                    ["--store-dir", str(self.store), "--bridge", "/not-executed"]
                )
        self.assertEqual(status, 1)
        execute.assert_not_called()
        self.assertNotIn(secret, stderr.getvalue())
        self.assertIn("status=blocked", stderr.getvalue())

    def test_exec_store_binding_and_ready_marker_follow_verified_manifest(self):
        self.database()
        ready = self.store / "ready"
        ready.touch(mode=0o600)

        def execute(path, args):
            self.assertEqual(os.environ["WHATSAPP_STORE_DIR"], str(self.store))
            self.assertTrue((self.attempt() / "manifest.json").is_file())
            self.assertEqual(ready.read_text(), "verified\n")

        with (
            mock.patch.dict(
                os.environ, {"WHATSAPP_STORE_DIR": "/not-the-verified-store"}
            ),
            mock.patch.object(backup.os, "execv", side_effect=execute) as invoked,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            status = backup.main(
                [
                    "--store-dir",
                    str(self.store),
                    "--ready-file",
                    str(ready),
                    "--bridge",
                    "/verified-bridge",
                ]
            )
        self.assertEqual(status, 0)
        invoked.assert_called_once()

    def test_entrypoint_and_docker_install_gate_before_bridge(self):
        entrypoint = (ROOT / "deploy" / "entrypoint.sh").read_text()
        self.assertIn("--bridge /app/bin/whatsapp-bridge &", entrypoint)
        self.assertNotIn("\n/app/bin/whatsapp-bridge &", entrypoint)
        self.assertLess(
            entrypoint.index("/app/bin/backup_store.py"),
            entrypoint.index("bridge_pid=$!"),
        )
        self.assertLess(
            entrypoint.index("backup_deadline="), entrypoint.index("start_timeout=")
        )
        self.assertIn("WHATSAPP_ALLOW_FRESH_STORE:-false", entrypoint)
        self.assertIn("backup_args+=(--require-existing-databases)", entrypoint)
        self.assertIn(
            "COPY deploy/backup_store.py /app/bin/backup_store.py",
            (ROOT / "Dockerfile").read_text(),
        )


if __name__ == "__main__":
    unittest.main()
