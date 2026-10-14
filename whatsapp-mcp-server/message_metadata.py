"""Expose bounded source metadata without download keys or original payloads."""

import json

from logical_messages import prepare_logical_messages
from security import truncate_untrusted_text

MEDIA_FIELDS = {
    "subtype",
    "mime_type",
    "duration_seconds",
    "width",
    "height",
    "voice_note",
    "animated",
    "gif",
    "view_once",
    "lottie",
    "original_filename",
    "title",
    "page_count",
}
CONTEXT_FIELDS = {
    "reply_to_message_id",
    "reply_sender_id",
    "reply_chat_jid",
    "reaction_to_message_id",
    "forwarded",
    "forwarding_score",
    "expiration_seconds",
}
DETAIL_FIELDS = {
    "latitude",
    "longitude",
    "accuracy_meters",
    "name",
    "address",
    "display_name",
    "vcard",
    "selectable_options_count",
}


def source_metadata(conn, message_id, chat_jid, prefer_exact=False):
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_metadata'"
    ).fetchone()
    if not exists:
        return None
    projection_exists = conn.execute(
        "SELECT 1 FROM sqlite_temp_master WHERE name='logical_messages'"
    ).fetchone()
    if prefer_exact or not projection_exists:
        # A selected physical media source must never borrow another alias's
        # MIME type or duration, even when its own metadata was not captured.
        row = conn.execute(
            "SELECT metadata_json FROM message_metadata WHERE message_id=? AND chat_jid=?",
            (message_id, chat_jid),
        ).fetchone()
    else:
        # A metadata-only replay may enrich a non-representative alias. Resolve
        # within the verified logical identity, never by the bare message ID.
        row = conn.execute(
            "SELECT metadata.metadata_json FROM logical_message_sources source "
            "JOIN logical_messages logical ON logical.logical_id=source.logical_id "
            "JOIN main.messages raw ON raw.rowid=source.source_rowid "
            "JOIN message_metadata metadata ON metadata.message_id=raw.id "
            "AND metadata.chat_jid=raw.chat_jid "
            "WHERE source.logical_id IN ("
            "SELECT original.logical_id FROM logical_message_sources original "
            "JOIN main.messages requested ON requested.rowid=original.source_rowid "
            "WHERE requested.id=? AND requested.chat_jid=?) "
            "ORDER BY (source.source_rowid=logical.rowid) DESC, "
            "julianday(metadata.observed_at) DESC, source.source_rowid LIMIT 1",
            (message_id, chat_jid),
        ).fetchone()
    if not row:
        return None
    try:
        value = json.loads(row[0])
    except (ValueError, TypeError):
        return None
    return value if isinstance(value, dict) else None


def safe_fields(value, allowed):
    if not isinstance(value, dict):
        return {}
    result = {}
    for name in allowed:
        item = value.get(name)
        if isinstance(item, str):
            result[name] = truncate_untrusted_text(item, 512)
        elif type(item) in {bool, int, float}:
            result[name] = item
    return result


def public_message_metadata(conn, message_id, chat_jid):
    value = source_metadata(conn, message_id, chat_jid)
    if value is None:
        return {"source_metadata": "not_captured"}
    result = {"source_metadata": "captured"}
    kind = value.get("message_type")
    if isinstance(kind, str):
        result["message_type"] = truncate_untrusted_text(kind, 64)
    context = safe_fields(value.get("context"), CONTEXT_FIELDS)
    if context:
        result["message_context"] = context
    details = safe_fields(value.get("details"), DETAIL_FIELDS)
    original = value.get("details")
    if isinstance(original, dict) and isinstance(original.get("options"), list):
        details["options"] = [
            truncate_untrusted_text(option, 512)
            for option in original["options"][:50]
            if isinstance(option, str)
        ]
    if details:
        result["message_details"] = details
    return result


def public_media_metadata(conn, message_id, chat_jid):
    # Retrieval can explicitly choose a richer physical media source. Its media
    # fields must stay paired with that source's download reference.
    value = source_metadata(conn, message_id, chat_jid, prefer_exact=True)
    if value is None:
        return {"source_metadata": "not_captured"}
    return {
        "source_metadata": "captured",
        **safe_fields(value.get("media"), MEDIA_FIELDS),
    }


def metadata_coverage(conn, chat_aliases=None, identity_index=None):
    projection_exists = conn.execute(
        "SELECT 1 FROM sqlite_temp_master WHERE name='logical_messages'"
    ).fetchone()
    if not projection_exists:
        prepare_logical_messages(conn, identity_index=identity_index)
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_metadata'"
    ).fetchone()
    condition, params = "", []
    if chat_aliases:
        condition = (
            " WHERE logical.logical_id IN (SELECT source.logical_id "
            "FROM logical_message_sources source JOIN main.messages raw "
            "ON raw.rowid=source.source_rowid WHERE raw.chat_jid IN ("
            + ",".join("?" for _ in chat_aliases)
            + "))"
        )
        params = chat_aliases
    rows = conn.execute(
        "SELECT COALESCE(media_type,''), COUNT(*) FROM logical_messages logical"
        + condition
        + " GROUP BY media_type",
        params,
    ).fetchall()
    raw_source_rows = conn.execute(
        "SELECT COUNT(*) FROM logical_message_sources "
        "WHERE logical_id IN (SELECT logical.logical_id "
        "FROM logical_messages logical" + condition + ")",
        params,
    ).fetchone()[0]
    media_types = {kind: count for kind, count in rows if kind}
    captured, captured_media, message_types = 0, 0, {}
    if exists:
        # Rank source records once so each logical message contributes exactly
        # one captured type, even when both aliases retain source metadata.
        metadata_rows = conn.execute(
            "WITH ranked_metadata AS ("
            "SELECT source.logical_id, metadata.message_type, "
            "ROW_NUMBER() OVER (PARTITION BY source.logical_id ORDER BY "
            "(source.source_rowid=logical.rowid) DESC, "
            "julianday(metadata.observed_at) DESC, source.source_rowid) AS preference "
            "FROM logical_message_sources source "
            "JOIN logical_messages logical ON logical.logical_id=source.logical_id "
            "JOIN main.messages raw ON raw.rowid=source.source_rowid "
            "JOIN message_metadata metadata ON metadata.message_id=raw.id "
            "AND metadata.chat_jid=raw.chat_jid) "
            "SELECT ranked_metadata.message_type, "
            "COALESCE(logical.media_type,'')!='', COUNT(*) "
            "FROM logical_messages logical JOIN ranked_metadata "
            "ON ranked_metadata.logical_id=logical.logical_id "
            "AND ranked_metadata.preference=1"
            + condition
            + " GROUP BY ranked_metadata.message_type, COALESCE(logical.media_type,'')!=''",
            params,
        ).fetchall()
        for kind, is_media, count in metadata_rows:
            captured += count
            if is_media:
                captured_media += count
            message_types[kind] = message_types.get(kind, 0) + count
    total = sum(count for _, count in rows)
    return {
        "policy": "metadata_with_on_demand_downloads",
        "phone_history_complete": None,
        "counting_unit": "logical_messages",
        "stored_message_rows": total,
        "raw_source_rows": raw_source_rows,
        "duplicate_alias_rows": raw_source_rows - total,
        "rows_with_source_metadata": captured,
        "rows_without_source_metadata": total - captured,
        "stored_media_rows": sum(media_types.values()),
        "media_rows_with_source_metadata": captured_media,
        "media_rows_without_source_metadata": sum(media_types.values())
        - captured_media,
        "stored_media_types": media_types,
        "captured_message_types": message_types,
        "notice": "Counts describe logical stored messages; verified alias copies count once. raw_source_rows retains the physical source count. Missing source metadata is a capture gap; these counts do not measure phone-history completeness or file availability.",
    }
