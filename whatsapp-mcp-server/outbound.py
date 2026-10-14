"""Persist a text-send attempt before network I/O; never retry uncertain attempts."""

import hashlib
import json
import os
import re
import sqlite3
from contextlib import closing

REQUEST_ID = re.compile(r"[A-Za-z0-9_-]{8,128}\Z")
RECIPIENT = re.compile(
    r"(?:[0-9]{5,20}|[0-9]{5,20}@(s\.whatsapp\.net|lid)|[0-9-]{5,40}@g\.us)\Z"
)


def result(status, recipient=None, message_id=None, archive_status=None):
    messages = {
        "sent": "WhatsApp bridge accepted the send; this does not confirm delivery or reading.",
        "unknown": "Send outcome is uncertain. This request will not be sent again. Check the conversation before starting a new send.",
        "conflict": "This request ID belongs to different recipient/text. Reuse it only for the same send.",
    }
    value = {"success": status == "sent", "status": status, "message": messages[status]}
    if status == "sent" and message_id:
        value["message_id"] = message_id
        value["chat_jid"] = (
            recipient if "@" in recipient else recipient + "@s.whatsapp.net"
        )
        value["archive_status"] = archive_status
    return value


def prepare_journal(conn):
    # Serialize additive upgrades with reservations from simultaneous callers.
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS sends (
            client TEXT NOT NULL, request_hash TEXT NOT NULL, payload_hash TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('unknown','sent')),
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            message_id TEXT, archive_status TEXT,
            PRIMARY KEY(client, request_hash))""")
        columns = {row[1] for row in conn.execute("PRAGMA table_info(sends)")}
        for column in ("message_id", "archive_status"):
            if column not in columns:
                conn.execute(f"ALTER TABLE sends ADD COLUMN {column} TEXT")
        conn.commit()
    except sqlite3.Error:
        conn.rollback()
        raise


def send_text_once(path, client, recipient, message, request_id, send):
    if (
        not isinstance(recipient, str)
        or not RECIPIENT.fullmatch(recipient)
        or not isinstance(message, str)
        or not message.strip()
        or len(message) > 65536
        or not isinstance(request_id, str)
        or not REQUEST_ID.fullmatch(request_id)
    ):
        return {
            "success": False,
            "status": "rejected",
            "message": "Provide a valid recipient, nonempty text (up to 65,536 characters), and an 8–128 character request ID using letters, digits, underscore or hyphen.",
        }
    digest = hashlib.sha256(
        json.dumps([recipient, message], ensure_ascii=False).encode()
    ).hexdigest()
    request_hash = hashlib.sha256(request_id.encode()).hexdigest()
    try:
        # The volume directory must already exist. No private content or raw request
        # IDs are stored; the payload digest detects conflicting reuse.
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        with closing(sqlite3.connect(path, timeout=10)) as conn:
            conn.execute("PRAGMA synchronous=FULL")
            prepare_journal(conn)
            with conn:
                inserted = conn.execute(
                    "INSERT OR IGNORE INTO sends(client,request_hash,payload_hash,status) VALUES(?,?,?,'unknown')",
                    (client, request_hash, digest),
                ).rowcount
                row = conn.execute(
                    "SELECT payload_hash,status,message_id,archive_status FROM sends WHERE client=? AND request_hash=?",
                    (client, request_hash),
                ).fetchone()
            if not inserted:
                if row[0] != digest:
                    return result("conflict")
                return result(row[1], recipient, row[2], row[3])
            # The unknown reservation is committed before the one permitted call.
            # A crash or lost reply must not turn into a duplicate after restart.
            try:
                success, details = send(recipient, message)
            except Exception:
                return result("unknown")
            if not success:
                return result("unknown")
            message_id = None
            archive_status = None
            if isinstance(details, dict):
                candidate = details.get("message_id")
                if isinstance(candidate, str) and re.fullmatch(
                    r"[A-Za-z0-9_-]{1,128}", candidate
                ):
                    message_id = candidate
                    archive_status = details.get("archive_status")
                    if archive_status not in {"saved", "pending", "failed"}:
                        archive_status = "unknown"
            with conn:
                conn.execute(
                    "UPDATE sends SET status='sent',message_id=?,archive_status=? WHERE client=? AND request_hash=?",
                    (message_id, archive_status, client, request_hash),
                )
            return result("sent", recipient, message_id, archive_status)
    except (OSError, sqlite3.Error):
        return {
            "success": False,
            "status": "unknown",
            "message": "Send tracking storage is unavailable. Do not retry with a new request ID; restore storage and check the conversation first.",
        }
