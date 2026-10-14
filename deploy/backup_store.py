"""Fail-closed, immutable SQLite snapshots before the bridge can migrate storage."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Bump for another incompatible/additive storage transition, not every restart.
BACKUP_TRANSITION = "message-identity-provenance-v1"
DATABASE_NAMES = ("messages.db", "whatsapp.db")
MINIMUM_HEADROOM = 64 * 1024 * 1024
BACKUP_TIMEOUT_SECONDS = 600
MANIFEST_VERSION = 1


class BackupFailure(Exception):
    """Only this fixed, nonsecret category is safe to put in startup logs."""


def _fail(category: str) -> None:
    raise BackupFailure(category)


def _private_directory(path: Path, *, create: bool = False) -> None:
    if create:
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        _fail("unsafe_directory")
    if stat.S_IMODE(info.st_mode) & 0o077:
        _fail("directory_not_private")


def _private_regular_file(path: Path) -> os.stat_result:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
        _fail("unsafe_backup_file")
    if stat.S_IMODE(info.st_mode) & 0o077:
        _fail("backup_file_not_private")
    return info


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _create_file(path: Path, contents: bytes = b"") -> None:
    descriptor = os.open(
        path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "wb") as output:
        output.write(contents)
        output.flush()
        os.fsync(output.fileno())


@contextlib.contextmanager
def store_lock(store: Path):
    """The same open file description stays locked across exec of the bridge."""
    _private_directory(store, create=True)
    descriptor = os.open(
        store / ".bridge-start.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600
    )
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) & 0o077
        ):
            _fail("unsafe_writer_lock")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _fail("writer_lock_busy")
        yield descriptor
    finally:
        os.close(descriptor)


def reject_visible_legacy_bridge() -> None:
    # /proc cannot prove another container is absent. Railway's single-mounted-
    # deployment contract also excludes workers that do not share the store lock.
    for process in Path("/proc").glob("[0-9]*/exe"):
        if process.parent.name == str(os.getpid()):
            continue
        try:
            executable = os.readlink(process).removesuffix(" (deleted)")
        except OSError:
            continue
        if Path(executable).name == "whatsapp-bridge":
            _fail("another_bridge_visible")


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    connection.execute("PRAGMA query_only=ON")
    return connection


def _source_details(path: Path) -> dict:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        _fail("unsafe_source_file")
    with contextlib.closing(_readonly(path)) as connection:
        if connection.execute("PRAGMA quick_check").fetchmany(2) != [("ok",)]:
            _fail("source_integrity_failed")
        pages = connection.execute("PRAGMA page_count").fetchone()[0]
        page_size = connection.execute("PRAGMA page_size").fetchone()[0]
        migration_complete = _retention_migration_complete(connection)
        expected_tables = {
            "messages.db": ("messages", "chats"),
            "whatsapp.db": ("whatsmeow_device",),
        }[path.name]
        base_schema_present = all(
            connection.execute(
                "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", (name,)
            ).fetchone()
            for name in expected_tables
        )
    sidecar_bytes = 0
    for suffix in ("-wal", "-journal"):
        sidecar = path.with_name(path.name + suffix)
        if sidecar.exists() or sidecar.is_symlink():
            sidecar_info = sidecar.lstat()
            if not stat.S_ISREG(sidecar_info.st_mode):
                _fail("unsafe_source_sidecar")
            sidecar_bytes += sidecar_info.st_size
    return {
        "device": info.st_dev,
        "inode": info.st_ino,
        "estimated_bytes": max(info.st_size, pages * page_size) + sidecar_bytes,
        "migration_complete": migration_complete,
        "page_count": pages,
        "base_schema_present": base_schema_present,
    }


def _retention_migration_complete(connection: sqlite3.Connection) -> bool:
    expected = {
        ("table", "message_metadata_versions", "message_metadata_versions"),
        ("table", "message_row_versions", "message_row_versions"),
        ("trigger", "message_metadata_retain_insert", "message_metadata"),
        ("trigger", "message_metadata_retain_before_update", "message_metadata"),
        ("trigger", "message_metadata_retain_after_update", "message_metadata"),
        ("trigger", "message_rows_retain_insert", "messages"),
        ("trigger", "message_rows_retain_before_update", "messages"),
        ("trigger", "message_rows_retain_after_update", "messages"),
    }
    existing = set(connection.execute("SELECT type,name,tbl_name FROM sqlite_schema"))
    return expected.issubset(existing)


def _verify_database(connection: sqlite3.Connection) -> dict:
    if connection.execute("PRAGMA integrity_check").fetchmany(2) != [("ok",)]:
        _fail("database_integrity_failed")
    if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
        _fail("database_foreign_key_failed")
    counts = {}
    for table in ("messages", "chats", "message_metadata"):
        exists = connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if exists:
            counts[table] = connection.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
    return {
        "integrity_check": "ok",
        "foreign_key_check": "ok",
        "user_version": connection.execute("PRAGMA user_version").fetchone()[0],
        "counts": counts,
    }


def _sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _require_space(directory: Path, required: int) -> None:
    if shutil.disk_usage(directory).free < required:
        _fail("insufficient_disk_space")


def _reuse_backup(directory: Path, source: Path, details: dict) -> dict:
    _private_directory(directory)
    manifest_path = directory / "manifest.json"
    snapshot = directory / "snapshot.sqlite3"
    if not manifest_path.exists() or not snapshot.exists():
        _fail("incomplete_backup_requires_review")
    manifest_info = _private_regular_file(manifest_path)
    snapshot_info = _private_regular_file(snapshot)
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = snapshot.with_name(snapshot.name + suffix)
        if sidecar.exists() or sidecar.is_symlink():
            _fail("backup_has_sidecars")
    if manifest_info.st_size > 64 * 1024:
        _fail("backup_manifest_invalid")
    try:
        manifest = json.loads(manifest_path.read_text())
    except (ValueError, UnicodeError):
        _fail("backup_manifest_invalid")
    if (
        not isinstance(manifest, dict)
        or manifest.get("version") != MANIFEST_VERSION
        or manifest.get("transition") != BACKUP_TRANSITION
        or manifest.get("database") != source.name
        or manifest.get("complete") is not True
        or manifest.get("size_bytes") != snapshot_info.st_size
        or manifest.get("sha256") != _sha256(snapshot)
    ):
        _fail("backup_manifest_mismatch")
    # The verified persistent-volume deployment contract binds this directory.
    # Device numbers may change on container remount; normal SQLite writes keep
    # the inode. A replaced/restored file still requires explicit operator review.
    source_identity = manifest.get("source_identity")
    if (
        not isinstance(source_identity, dict)
        or source_identity.get("inode") != details["inode"]
    ):
        _fail("source_identity_changed_requires_review")
    with contextlib.closing(_readonly(snapshot)) as connection:
        if connection.execute("PRAGMA journal_mode").fetchone()[0] != "delete":
            _fail("backup_not_standalone")
        checks = _verify_database(connection)
    if manifest.get("verification") != checks:
        _fail("backup_verification_mismatch")
    return manifest


def _create_backup(directory: Path, source: Path, details: dict, reserve: int) -> dict:
    # A fixed exclusive directory also marks an interrupted attempt. Never
    # delete, overwrite, or automatically multiply incomplete backup attempts.
    directory.mkdir(mode=0o700)
    _sync_directory(directory.parent)
    snapshot = directory / "snapshot.sqlite3"
    _create_file(snapshot)
    started = time.monotonic()

    def progress(status: int, remaining: int, total: int) -> None:
        if time.monotonic() - started > BACKUP_TIMEOUT_SECONDS:
            _fail("backup_timeout")
        _require_space(directory, reserve)

    with (
        contextlib.closing(_readonly(source)) as original,
        contextlib.closing(
            sqlite3.connect(snapshot.as_uri() + "?mode=rw", uri=True)
        ) as destination,
    ):
        original.backup(destination, pages=256, progress=progress, sleep=0.05)
        # Finalize a standalone file even when the live database uses WAL.
        destination.execute("PRAGMA journal_mode=DELETE")
        checks = _verify_database(destination)
    current = source.lstat()
    if current.st_dev != details["device"] or current.st_ino != details["inode"]:
        _fail("source_changed_during_backup")
    with snapshot.open("rb") as completed:
        os.fsync(completed.fileno())
    manifest = {
        "version": MANIFEST_VERSION,
        "transition": BACKUP_TRANSITION,
        "database": source.name,
        "complete": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_identity": {"device": details["device"], "inode": details["inode"]},
        "size_bytes": snapshot.stat().st_size,
        "sha256": _sha256(snapshot),
        "verification": checks,
        "snapshot_scope": "one_database; databases are not an atomic combined snapshot",
    }
    _create_file(
        directory / "manifest.json",
        (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode(),
    )
    _sync_directory(directory)
    return manifest


def _log_manifest(status: str, manifest: dict) -> None:
    counts = manifest["verification"]["counts"]
    aggregate = " ".join(f"{table}={count}" for table, count in sorted(counts.items()))
    print(
        f"STARTUP_BACKUP status={status} database={manifest['database']} "
        f"transition={BACKUP_TRANSITION} bytes={manifest['size_bytes']} {aggregate}".rstrip(),
        flush=True,
    )


def prepare_backups(store: Path, *, require_existing: bool = False) -> list[dict]:
    """Caller must hold store_lock until the bridge stops, not just this function."""
    if require_existing and any(not (store / name).exists() for name in DATABASE_NAMES):
        _fail("required_source_database_missing")
    root = store / ".startup-backups"
    prior_transitions = []
    if root.exists() or root.is_symlink():
        _private_directory(root)
        for prior in root.iterdir():
            _private_directory(prior)
            prior_transitions.append(prior)
    sources = []
    for name in DATABASE_NAMES:
        source = store / name
        if source.exists() or source.is_symlink():
            details = _source_details(source)
            if require_existing and (
                details["page_count"] == 0 or not details["base_schema_present"]
            ):
                _fail("required_source_database_uninitialized")
            sources.append((source, details))
        elif any(
            (prior / name).exists() or (prior / name).is_symlink()
            for prior in prior_transitions
        ):
            _fail("previously_backed_source_missing")
    if not sources:
        print("STARTUP_BACKUP status=no_existing_databases", flush=True)
        return []
    _private_directory(root, create=True)
    _sync_directory(store)
    transition = root / BACKUP_TRANSITION
    _private_directory(transition, create=True)
    _sync_directory(root)
    completed = []
    pending = []
    for source, details in sources:
        directory = transition / source.name
        if directory.exists() or directory.is_symlink():
            manifest = _reuse_backup(directory, source, details)
            completed.append(manifest)
            _log_manifest("reused", manifest)
        else:
            pending.append((source, details))
    # Reserve additive archive data and its transaction journal only while this
    # exact transition is pending. Completed migrations must not multiply their
    # now-larger archive's space requirement on every restart.
    reserve = MINIMUM_HEADROOM + 2 * sum(
        info["estimated_bytes"]
        for source, info in sources
        if source.name == "messages.db" and not info["migration_complete"]
    )
    pending_bytes = sum(info["estimated_bytes"] for _, info in pending)
    _require_space(store, reserve + pending_bytes)
    for source, details in pending:
        pending_bytes -= details["estimated_bytes"]
        manifest = _create_backup(
            transition / source.name, source, details, reserve + pending_bytes
        )
        completed.append(manifest)
        _log_manifest("created", manifest)
    present = {name for name in DATABASE_NAMES if (store / name).exists()}
    if present != {source.name for source, _ in sources}:
        _fail("source_set_changed_during_backup")
    remaining_migration_bytes = 0
    for source, original in sources:
        current = _source_details(source)
        if require_existing and (
            current["page_count"] == 0 or not current["base_schema_present"]
        ):
            _fail("required_source_database_uninitialized")
        if any(current[field] != original[field] for field in ("device", "inode")):
            _fail("source_changed_during_backup")
        if source.name == "messages.db" and not current["migration_complete"]:
            remaining_migration_bytes += current["estimated_bytes"]
    _require_space(
        store, max(reserve, MINIMUM_HEADROOM + 2 * remaining_migration_bytes)
    )
    return completed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store-dir", required=True)
    parser.add_argument(
        "--bridge", help="Exec the bridge only after all snapshots verify"
    )
    parser.add_argument(
        "--ready-file", help="Private, empty supervisor readiness marker"
    )
    parser.add_argument("--require-existing-databases", action="store_true")
    args = parser.parse_args(argv)
    store = Path(args.store_dir).absolute()
    try:
        with store_lock(store) as descriptor:
            reject_visible_legacy_bridge()
            prepare_backups(store, require_existing=args.require_existing_databases)
            if args.bridge:
                os.environ["WHATSAPP_STORE_DIR"] = str(store)
                os.set_inheritable(descriptor, True)
                if args.ready_file:
                    ready = Path(args.ready_file)
                    if _private_regular_file(ready).st_size != 0:
                        _fail("invalid_readiness_marker")
                    ready_descriptor = os.open(ready, os.O_WRONLY | os.O_NOFOLLOW)
                    with os.fdopen(ready_descriptor, "wb") as output:
                        output.write(b"verified\n")
                        output.flush()
                print("STARTUP_BACKUP status=verified starting_bridge=true", flush=True)
                os.execv(args.bridge, [args.bridge])
        return 0
    except BackupFailure as error:
        category = str(error)
    except sqlite3.Error:
        category = "sqlite_backup_or_validation_failed"
    except OSError:
        category = "backup_filesystem_or_exec_failed"
    print(
        f"STARTUP_BACKUP status=blocked category={category}",
        file=sys.stderr,
        flush=True,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
