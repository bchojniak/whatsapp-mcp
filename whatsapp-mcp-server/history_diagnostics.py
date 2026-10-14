"""Expose bounded history-stage evidence without private protocol references."""

import re

from ingestion_status import ERRORS, operational_date

RESULTS = {
    "planned",
    "waiting",
    "progress_observed",
    "no_backward_progress_observed",
    "phone_reports_no_older_messages",
    "older_messages_not_accessible",
    "response_timeout",
    "response_pending_storage",
    "request_failed",
    "interrupted",
    "invalid_anchor",
    "diagnostics_unavailable",
    "empty_response_unconfirmed",
    "unmatched_response_unconfirmed",
    "storage_failed",
}
END_FLAGS = {
    "unspecified",
    "COMPLETE_BUT_MORE_MESSAGES_REMAIN_ON_PRIMARY",
    "COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY",
    "COMPLETE_ON_DEMAND_SYNC_BUT_MORE_MSG_REMAIN_ON_PRIMARY",
    "COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS",
}


def counts(source, target, keys):
    for key in keys:
        value = source.get(key)
        if type(value) is int and 0 <= value <= 2**63 - 1:
            target[key] = value


def dates(source, target, keys):
    for key in keys:
        target[key] = operational_date(source.get(key))


def enum_field(source, target, key, allowed):
    value = source.get(key)
    if isinstance(value, str) and value in allowed:
        target[key] = value


def diagnostic_label(value):
    return isinstance(value, str) and value in {
        "control",
        "failed-1",
        "failed-2",
        "failed-3",
    }


def bounded_records(value, limit):
    if not isinstance(value, list):
        return []
    return [item for item in value[-limit:] if isinstance(item, dict)]


def history_diagnostics(value):
    if not isinstance(value, dict):
        return None
    result = {}
    run_id = value.get("run_id")
    if isinstance(run_id, str) and re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", run_id):
        result["run_id"] = run_id
    enum_field(
        value,
        result,
        "status",
        {
            "prepared",
            "running",
            "finished",
            "interrupted",
            "preparation_failed",
            "control_unresponsive",
            "response_pending_storage",
            "diagnostics_unavailable",
            "storage_failed",
            "verification_failed",
        },
    )
    if type(value.get("diagnostics_available")) is bool:
        result["diagnostics_available"] = value["diagnostics_available"]
    dates(value, result, ("updated_at",))
    counts(
        value,
        result,
        (
            "request_budget",
            "requests_started",
            "decoded_responses",
            "protocol_observations",
            "stored_response_batches",
            "inserted_rows",
            "duplicate_entries",
            "dropped_observations",
        ),
    )
    result["correlation"] = "chat_and_time_only"
    enum_field(
        value,
        result,
        "experiment",
        {"anchor_comparison", "alias_comparison", "oldest_boundary"},
    )
    result["attempts"] = []
    for attempt in bounded_records(value.get("attempts"), 8):
        label = attempt.get("label")
        if not diagnostic_label(label):
            continue
        item = {"label": label}
        counts(
            attempt,
            item,
            (
                "number",
                "alias_count",
                "correlated_protocol_observations",
                "new_older_message_ids",
            ),
        )
        dates(
            attempt, item, ("anchor_at", "started_at", "acknowledged_at", "finished_at")
        )
        enum_field(
            attempt,
            item,
            "variant",
            {
                "control_start",
                "control_end",
                "failed_boundary",
                "recent_anchor",
                "alternate_alias",
                "oldest_boundary",
            },
        )
        enum_field(
            attempt, item, "chat_type", {"phone_number", "lid", "group", "unknown"}
        )
        enum_field(attempt, item, "result", RESULTS)
        enum_field(
            attempt,
            item,
            "anchor_validation",
            {
                "stored_exact",
                "stored_alias",
                "provider_checkpoint_only",
                "invalid_anchor",
                "stored_anchor_mismatch",
                "archive_unavailable",
            },
        )
        if type(attempt.get("anchor_from_me")) is bool:
            item["anchor_from_me"] = attempt["anchor_from_me"]
        for key in ("coverage_before", "coverage_after"):
            coverage = attempt.get(key)
            if isinstance(coverage, dict):
                item[key] = {}
                counts(coverage, item[key], ("stored_rows", "unique_message_ids"))
                dates(coverage, item[key], ("oldest_stored_at", "newest_stored_at"))
        result["attempts"].append(item)
    result["responses"] = []
    for response in bounded_records(value.get("responses"), 16):
        item = {}
        counts(
            response,
            item,
            (
                "number",
                "active_attempt",
                "inserted_rows",
                "duplicate_entries",
                "storage_commits",
                "storage_failures",
            ),
        )
        dates(response, item, ("received_at", "stored_at"))
        enum_field(response, item, "storage_error", ERRORS)
        item["conversations"] = []
        for conversation in bounded_records(response.get("conversations"), 32):
            record = {}
            labels = conversation.get("labels")
            record["labels"] = []
            if isinstance(labels, list):
                record["labels"] = [
                    label for label in labels[:4] if diagnostic_label(label)
                ]
            counts(conversation, record, ("entries",))
            dates(conversation, record, ("oldest_at", "newest_at"))
            enum_field(conversation, record, "match", {"none", "exact", "alias"})
            enum_field(conversation, record, "end_flag", END_FLAGS)
            item["conversations"].append(record)
        result["responses"].append(item)
    result["protocol_details"] = []
    for observation in bounded_records(value.get("protocol_details"), 16):
        kind = observation.get("kind")
        if not isinstance(kind, str) or kind not in {
            "history_notification",
            "peer_response",
        }:
            continue
        item = {"kind": observation["kind"]}
        dates(observation, item, ("at",))
        for key in ("original_id_present", "session_id_present", "stanza_id_present"):
            if type(observation.get(key)) is bool:
                item[key] = observation[key]
        matching = observation.get("matching_attempts")
        item["matching_attempts"] = []
        if isinstance(matching, list):
            item["matching_attempts"] = [
                number
                for number in matching[:8]
                if type(number) is int and 1 <= number <= 8
            ]
        result["protocol_details"].append(item)
    result["notice"] = (
        "Decoded history replies lack a request ID. Chat/time matches are observations, "
        "and late replies may belong to earlier attempts. Protocol ID matches are shown separately. "
        "Rows and duplicate entries are physical storage counts. Completion flags describe a phone boundary, "
        "not lifetime completeness. Reports cover a bounded sample, not all chats."
    )
    return result
