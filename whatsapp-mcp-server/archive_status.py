"""Read stored coverage evidence without starting or resuming synchronization."""

import json
import os
import re
from collections import Counter
from pathlib import Path

from identity import identities
from ingestion_status import operational_date
from logical_messages import prepare_logical_messages
from message_metadata import metadata_coverage

HISTORY_NOTICE = (
    "Search covers stored messages only. Missing results do not prove a conversation "
    "never happened. Phone completion reports do not establish lifetime completeness or absence of internal gaps."
)


def backfill_recovery_status(value):
    if not isinstance(value, dict):
        return None
    status = value.get("status")
    allowed = {
        "running",
        "waiting_for_phone",
        "waiting_for_connection",
        "waiting_for_retry",
        "awaiting_source_data",
        "blocked_storage",
        "worker_failed",
    }
    result = {
        "mode": "continuous",
        "status": status
        if isinstance(status, str) and status in allowed
        else "unknown",
        "history_complete": None,
        "next_attempt_at": operational_date(value.get("next_attempt_at")),
        "retry_limit_without_progress": 3,
    }
    for key in ("automatic_resumes", "phone_pauses"):
        count = value.get(key)
        if type(count) is int and 0 <= count <= 2**63 - 1:
            result[key] = count
    return result


def requested_backfill_window(report):
    days = report.get("requested_days")
    cutoff = operational_date(report.get("target_after"))
    if type(days) is not int or not 1 <= days <= 3650 or cutoff is None:
        return None
    return {
        "days": days,
        "after": cutoff,
        "coverage_complete": None,
        "notice": "Requested window is a recovery target, not proof that every message was imported.",
    }


def conversation_outcome(records):
    leaders = {record.get("jid") for record in records if not record.get("linked_to")}
    records = [record for record in records if record.get("linked_to") not in leaders]
    outcomes = {str(record.get("status", "unknown")) for record in records}
    for outcome in (
        "pending",
        "phone_reports_no_older_messages",
        "older_messages_not_accessible",
        "no_backward_progress",
    ):
        if outcome in outcomes:
            return outcome
    if len(outcomes) == 1:
        return outcomes.pop()
    return "mixed_unconfirmed"


def linked_report_records(report):
    identities.refresh()
    groups = {}
    for record in report["chats"]:
        jid = record.get("jid")
        if not isinstance(jid, str):
            continue
        aliases = tuple(alias for alias in identities.aliases(jid) if "@" in alias)
        groups.setdefault(aliases, []).append(record)
    return list(groups.values())


def read_backfill_report(database_path):
    run_id = os.environ.get("WHATSAPP_HISTORY_BACKFILL_RUN_ID", "").strip()
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", run_id):
        return None, "not_configured"
    path = Path(database_path).resolve().parent / "backfill" / run_id / "report.json"
    try:
        with path.open("rb") as report_file:
            raw = report_file.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            return None, "report_too_large"
        report = json.loads(raw)
        if not isinstance(report, dict) or not isinstance(report.get("chats"), list):
            return None, "report_invalid"
        if any(not isinstance(chat, dict) for chat in report["chats"]):
            return None, "report_invalid"
        return report, "available"
    except FileNotFoundError:
        return None, "report_unavailable"
    except (OSError, ValueError):
        return None, "report_unreadable"


def archive_coverage(conn, database_path, chat_aliases=None, identity_index=None):
    projection_exists = conn.execute(
        "SELECT 1 FROM sqlite_temp_master WHERE name='logical_messages'"
    ).fetchone()
    if not projection_exists:
        prepare_logical_messages(conn, identity_index=identity_index or identities)
    report, report_status = read_backfill_report(database_path)
    where, params = "", []
    if chat_aliases:
        where = (
            " WHERE logical_id IN (SELECT source.logical_id "
            "FROM logical_message_sources source JOIN main.messages raw "
            "ON raw.rowid=source.source_rowid WHERE raw.chat_jid IN ("
            + ",".join("?" for _ in chat_aliases)
            + "))"
        )
        params = chat_aliases
    row = conn.execute(
        "SELECT COUNT(*), MIN(julianday(timestamp)), MAX(julianday(timestamp)) "
        "FROM logical_messages" + where,
        params,
    ).fetchone()
    raw_source_rows = conn.execute(
        "SELECT COUNT(*) FROM logical_message_sources "
        "WHERE logical_id IN (SELECT logical_id FROM logical_messages" + where + ")",
        params,
    ).fetchone()[0]
    dates = []
    for value in row[1:3]:
        dates.append(
            conn.execute(
                "SELECT strftime('%Y-%m-%dT%H:%M:%fZ',?)", (value,)
            ).fetchone()[0]
            if value is not None
            else None
        )
    result = {
        "scope": "chat_aliases" if chat_aliases else "archive",
        "stored_messages": row[0],
        "raw_source_rows": raw_source_rows,
        "duplicate_alias_rows": raw_source_rows - row[0],
        "oldest_stored_at": dates[0],
        "newest_stored_at": dates[1],
        "history_complete": None,
        "notice": HISTORY_NOTICE,
        "backfill_report_status": report_status,
        "metadata_coverage": metadata_coverage(conn, chat_aliases, identity_index),
    }
    if chat_aliases:
        # This compatibility field counts conservative message identities, not bare IDs.
        result["stored_unique_message_ids"] = row[0]
        result["preferred_request_jid"] = None
        table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='history_request_preferences'"
        ).fetchone()
        if table_exists:
            preference = conn.execute(
                f"SELECT request_jid FROM history_request_preferences WHERE chat_jid IN ({','.join('?' for _ in chat_aliases)}) ORDER BY julianday(observed_at) DESC LIMIT 1",
                chat_aliases,
            ).fetchone()
            if preference and preference[0] in chat_aliases:
                result["preferred_request_jid"] = preference[0]
    if report is None:
        return result
    result["backfill"] = {
        "status": report.get("status"),
        "updated_at": report.get("updated_at"),
        "chat_record_outcomes": dict(
            Counter(str(chat.get("status", "unknown")) for chat in report["chats"])
        ),
        "logical_conversation_outcomes": dict(
            Counter(
                conversation_outcome(records)
                for records in linked_report_records(report)
            )
        ),
    }
    window = requested_backfill_window(report)
    if window is not None:
        result["backfill"]["requested_window"] = window
    recovery = backfill_recovery_status(report.get("recovery"))
    if recovery is not None:
        result["backfill"]["recovery"] = recovery
    if report.get("capture_version") == 1:
        result["backfill"]["capture_version"] = 1
        result["backfill"]["metadata_replay_outcomes"] = dict(
            Counter(
                str(chat["metadata_replay"].get("status", "unknown"))
                for chat in report["chats"]
                if not chat.get("linked_to")
                and isinstance(chat.get("metadata_replay"), dict)
            )
        )
    if chat_aliases:
        by_jid = {chat.get("jid"): chat for chat in report["chats"]}
        stored_jids = {
            row[0]
            for row in conn.execute(
                f"SELECT jid FROM chats WHERE jid IN ({','.join('?' for _ in chat_aliases)})",
                chat_aliases,
            )
        }
        result["chat_records"] = [
            {
                "chat_jid": jid,
                "history_status": by_jid.get(jid, {}).get("status", "unknown"),
                "requests": by_jid.get(jid, {}).get("requests", 0),
            }
            for jid in chat_aliases
            if jid in stored_jids
        ]
        records = [
            record for record in report["chats"] if record.get("jid") in chat_aliases
        ]
        result["conversation_history_status"] = (
            conversation_outcome(records) if records else "unknown"
        )
    return result
