"""Validate operational metadata; never forward queued payloads or raw errors."""

import math
from datetime import datetime

COUNTS = (
    "starts",
    "restart_count",
    "unclean_restart_count",
    "received_batches",
    "received_entries",
    "committed_batches",
    "committed_message_entries",
    "pending_batches",
    "pending_entries",
    "failed_batches",
    "queue_bytes",
    "queue_limit_bytes",
    "batch_limit_bytes",
    "failed_write_attempts",
    "recovered_batches",
    "unpersisted_events",
    "journal_failures",
    "disconnect_count",
    "reconnect_count",
)
DATES = (
    "observation_started_at",
    "process_started_at",
    "last_shutdown_at",
    "last_received_at",
    "last_committed_at",
    "oldest_pending_at",
    "last_write_failure_at",
    "last_connected_at",
    "last_disconnected_at",
)
ERRORS = {
    "",
    "database_locked",
    "disk_full",
    "storage_io_error",
    "database_read_only",
    "database_unavailable",
    "database_corrupt",
    "constraint_failed",
    "database_error",
    "write_timeout",
    "queue_capacity_limit",
    "batch_size_limit",
    "invalid_payload",
    "invalid_history_payload",
    "invalid_payload_or_storage_error",
}
HISTORY_DOWNLOAD_COUNTS = (
    "pending_notifications",
    "awaiting_storage",
    "pending_cleanup",
    "retrying_notifications",
    "failed_attempts",
    "received_notifications",
)
HISTORY_DOWNLOAD_DATES = (
    "last_received_at",
    "last_downloaded_at",
    "last_committed_at",
)
HISTORY_DOWNLOAD_ERRORS = ERRORS | {
    "download_failed",
    "receipt_failed",
    "cleanup_failed",
}


def operational_date(value):
    if isinstance(value, str) and len(value) <= 64:
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is not None:
                return parsed.isoformat()
        except ValueError:
            pass
    return None


def history_download_status(value):
    if not isinstance(value, dict):
        return None
    result = {}
    for key in HISTORY_DOWNLOAD_COUNTS:
        count = value.get(key)
        if type(count) is int and 0 <= count <= 2**63 - 1:
            result[key] = count
    for key in HISTORY_DOWNLOAD_DATES:
        result[key] = operational_date(value.get(key))
    category = value.get("last_error")
    if isinstance(category, str) and category in HISTORY_DOWNLOAD_ERRORS:
        result["last_error"] = category
    return result


def initial_history_status(value):
    if not isinstance(value, dict):
        return None
    result = {}
    phase = value.get("phase")
    if phase in ("receiving", "waiting_for_storage", "backfill_ready"):
        result["phase"] = phase
    for key in ("last_received_at", "last_committed_at"):
        result[key] = operational_date(value.get(key))
    progress = value.get("transfer_progress")
    if type(progress) is int and 0 <= progress <= 100:
        result["transfer_progress"] = progress
    transfer_type = value.get("transfer_type")
    if transfer_type in ("INITIAL_BOOTSTRAP", "RECENT", "FULL"):
        result["transfer_type"] = transfer_type
    result["history_complete"] = None
    result["notice"] = (
        "Transfer progress describes history payloads supplied by WhatsApp, "
        "not complete account history. A quiet transfer or ready backfill "
        "does not prove that all messages were delivered."
    )
    return result


def ingestion_status(value):
    if not isinstance(value, dict):
        return None
    result = {}
    if value.get("status") in ("healthy", "buffering", "degraded"):
        result["status"] = value["status"]
    if isinstance(value.get("diagnostics_available"), bool):
        result["diagnostics_available"] = value["diagnostics_available"]
    for key in COUNTS:
        count = value.get(key)
        if type(count) is int and 0 <= count <= 2**63 - 1:
            result[key] = count
    for key in DATES:
        result[key] = operational_date(value.get(key))
    age = value.get("oldest_pending_age_seconds")
    if type(age) in (int, float) and math.isfinite(age) and age >= 0:
        result["oldest_pending_age_seconds"] = age
    for key in ("last_write_error", "last_journal_error"):
        category = value.get(key)
        if isinstance(category, str) and category in ERRORS:
            result[key] = category
    events = value.get("recent_connection_events")
    result["recent_connection_events"] = []
    if isinstance(events, list):
        for event in events[-100:]:
            if not isinstance(event, dict):
                continue
            at = operational_date(event.get("at"))
            state = event.get("state")
            reason = event.get("reason")
            if (
                at
                and state in ("connected", "disconnected", "logged_out")
                and reason
                in (
                    "connection_established",
                    "connection_lost",
                    "reauthentication_required",
                    "session_replaced",
                    "client_outdated",
                    "permanent_connection_failure",
                    "planned_shutdown",
                )
            ):
                result["recent_connection_events"].append(
                    {"at": at, "state": state, "reason": reason}
                )
    result["coverage_notice"] = (
        "Counters cover events observed since the ingestion journal was created, "
        "not unique messages or complete WhatsApp delivery. A healthy queue does "
        "not prove full history."
    )
    history_downloads = history_download_status(value.get("history_downloads"))
    if history_downloads is not None:
        result["history_downloads"] = history_downloads
    initial_history = initial_history_status(value.get("initial_history"))
    if initial_history is not None:
        result["initial_history"] = initial_history
    return result
