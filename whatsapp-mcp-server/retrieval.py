"""Bounded archive queries, exact context, and resumable search pages."""

import base64
import hashlib
import json
import math
import re
from datetime import datetime

from logical_messages import prepare_logical_messages

MESSAGE_COLUMNS = (
    "messages.timestamp, messages.sender, chats.name, messages.content, "
    "messages.is_from_me, messages.chat_jid, messages.id, messages.media_type"
)
MESSAGE_JOIN = (
    "FROM logical_messages messages LEFT JOIN chats ON messages.chat_jid=chats.jid"
)
TIME_KEY = "julianday(messages.timestamp)"


def date_boundary(name, value):
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(
            f"Invalid date format for '{name}': {value}. Please use ISO-8601 format."
        ) from None


def full_text_query(value):
    if len(value) > 256:
        raise ValueError(
            "Full-text query must be at most 256 characters; use a few keywords."
        )
    words = re.findall(r"\w+", value, flags=re.UNICODE)
    if not words or len(words) > 16:
        raise ValueError(
            "Full-text query needs 1–16 words. Operators are not required."
        )
    return " AND ".join('"' + word + '"' for word in words)


def search_revision(conn, ranked):
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name='archive_search_state'"
    ).fetchone()
    if not exists:
        return None
    column = "revision" if ranked else "content_revision"
    return conn.execute(
        f"SELECT {column} FROM archive_search_state WHERE id=1"
    ).fetchone()[0]


def metadata_revision(conn, snapshot):
    """Current metadata fingerprint; retained distinct versions are not a change log."""
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name='message_metadata' AND type='table'"
    ).fetchone():
        return None
    digest = hashlib.sha256()
    for row in conn.execute(
        "SELECT messages.rowid,metadata.message_type,metadata.metadata_json,metadata.observed_at "
        "FROM messages JOIN message_metadata metadata ON metadata.message_id=messages.id "
        "AND metadata.chat_jid=messages.chat_jid WHERE messages.rowid<=? ORDER BY messages.rowid",
        (snapshot,),
    ):
        digest.update(json.dumps(row, separators=(",", ":")).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def encode_cursor(state):
    raw = json.dumps(state, separators=(",", ":"), allow_nan=False).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(value, fingerprint):
    try:
        if not isinstance(value, str) or len(value) > 2048:
            raise ValueError()
        raw = base64.b64decode(
            value + "=" * (-len(value) % 4), altchars=b"-_", validate=True
        )
        state = json.loads(raw)
        if state["v"] != 1 or state["filter"] != fingerprint:
            raise ValueError()
        if type(state["snapshot"]) is not int or state["snapshot"] < 0:
            raise ValueError()
        for name in ("time", "rank"):
            number = state[name]
            if (
                isinstance(number, bool)
                or not isinstance(number, (int, float))
                or not math.isfinite(number)
            ):
                raise ValueError()
        for name in ("id", "chat"):
            if not isinstance(state[name], str) or len(state[name]) > 256:
                raise ValueError()
        if state["revision"] is not None and type(state["revision"]) is not int:
            raise ValueError()
        metadata = state["metadata_revision"]
        if metadata is not None and (
            not isinstance(metadata, str) or not re.fullmatch(r"[0-9a-f]{64}", metadata)
        ):
            raise ValueError()
        return state
    except (ValueError, KeyError, TypeError, UnicodeError):
        raise ValueError(
            "Invalid cursor or changed search filters. Restart the search without cursor."
        ) from None


def query_messages(
    conn,
    *,
    after,
    before,
    sender_aliases,
    chat_aliases,
    query,
    query_mode,
    media_type,
    limit,
    page,
    cursor=None,
    identity_index=None,
):
    identity_revision = prepare_logical_messages(conn, identity_index)
    filters, params = [], []
    for name, value, comparison in (("after", after, ">"), ("before", before, "<")):
        if value:
            filters.append(f"{TIME_KEY} {comparison} julianday(?)")
            params.append(date_boundary(name, value).isoformat())
    for column, aliases in (("sender", sender_aliases), ("chat_jid", chat_aliases)):
        if aliases:
            filters.append(f"messages.{column} IN ({','.join('?' for _ in aliases)})")
            params.extend(aliases)
    if media_type == "any":
        filters.append("COALESCE(messages.media_type,'') != ''")
    elif media_type in {"gif", "voice_note", "video_note"}:
        metadata_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_metadata'"
        ).fetchone()
        if metadata_exists:
            filters.append(
                "EXISTS (SELECT 1 FROM message_metadata WHERE message_id=messages.id "
                "AND message_metadata.chat_jid=messages.chat_jid "
                "AND json_extract(metadata_json,'$.media.subtype')=?)"
            )
            params.append(media_type)
        else:
            filters.append("0")
    elif media_type:
        filters.append("messages.media_type=?")
        params.append(media_type)
    ranked = bool(query and query_mode == "full_text")
    join = "FROM messages"
    rank = "0.0"
    if ranked:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='messages_fts'"
        ).fetchone():
            raise ValueError(
                "Full-text search index is unavailable. Use query_mode='substring' and check get_sync_status."
            )
        join += " JOIN messages_fts ON messages_fts.rowid=messages.rowid"
        rank = "bm25(messages_fts)"
        filters.append("messages_fts MATCH ?")
        params.append(full_text_query(query))
    elif query:
        filters.append("LOWER(messages.content) LIKE LOWER(?)")
        params.append(f"%{query}%")
    fingerprint = hashlib.sha256(
        json.dumps(
            [
                after,
                before,
                sender_aliases,
                chat_aliases,
                query,
                query_mode,
                media_type,
                identity_revision,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    revision = search_revision(conn, ranked)
    snapshot = conn.execute("SELECT COALESCE(MAX(rowid),0) FROM messages").fetchone()[0]
    state = None
    if cursor:
        state = decode_cursor(cursor, fingerprint)
        if state["revision"] != revision:
            raise ValueError(
                "Archive changed while paginating. Restart the search without cursor."
            )
        snapshot = state["snapshot"]
        prepare_logical_messages(conn, identity_index, snapshot=snapshot)
    metadata_state = metadata_revision(conn, snapshot)
    if state and state["metadata_revision"] != metadata_state:
        raise ValueError(
            "Archive changed while paginating. Restart the search without cursor."
        )
    raw_filters = [*filters, "messages.rowid<=?"]
    raw_params = [*params, snapshot]
    filters, params = [], []
    if state:
        boundary = f"({TIME_KEY}, messages.id, messages.chat_jid) < (?, ?, ?)"
        boundary_params = [state["time"], state["id"], state["chat"]]
        if ranked:
            # BM25 is ascending: lower scores rank first.
            boundary = (
                f"(matched.match_rank > ? OR (matched.match_rank = ? AND {boundary}))"
            )
            boundary_params = [state["rank"], state["rank"], *boundary_params]
        filters.append(boundary)
        params.extend(boundary_params)
    ordering = f"{TIME_KEY} DESC, messages.id DESC, messages.chat_jid DESC"
    if ranked:
        ordering = "matched.match_rank ASC, " + ordering
    # Materialization evaluates FTS bm25 in its supported query context.
    where = " WHERE " + " AND ".join(filters) if filters else ""
    sql = f"""
        WITH raw_matches AS MATERIALIZED (
            SELECT messages.rowid AS source_rowid, {rank} AS match_rank
            {join} WHERE {" AND ".join(raw_filters)}
        ), matched AS (
            SELECT sources.representative_rowid, MIN(raw_matches.match_rank) AS match_rank
            FROM raw_matches JOIN logical_message_sources sources
                ON sources.source_rowid=raw_matches.source_rowid
            GROUP BY sources.representative_rowid
        )
        SELECT {MESSAGE_COLUMNS}, {TIME_KEY}, matched.match_rank
        {MESSAGE_JOIN} JOIN matched ON matched.representative_rowid=messages.rowid
        {where} ORDER BY {ordering} LIMIT ? OFFSET ?
    """
    rows = conn.execute(sql, [*raw_params, *params, limit + 1, page * limit]).fetchall()
    template = {
        "v": 1,
        "filter": fingerprint,
        "snapshot": snapshot,
        "revision": revision,
        "metadata_revision": metadata_state,
    }
    return rows[:limit], len(rows) > limit, template


def next_cursor(template, row):
    return encode_cursor(
        {**template, "time": row[8], "rank": row[9], "chat": row[5], "id": row[6]}
    )


def context_rows(conn, message_id, chat_jid, before, after, identity_index=None):
    if not conn.execute(
        "SELECT 1 FROM sqlite_temp_master WHERE name='logical_messages'"
    ).fetchone():
        prepare_logical_messages(conn, identity_index)
    params = [message_id]
    where = "raw.id=?"
    if chat_jid:
        where += " AND raw.chat_jid=?"
        params.append(chat_jid)
    targets = conn.execute(
        f"""SELECT {MESSAGE_COLUMNS}, {TIME_KEY}, messages.canonical_chat_jid
        {MESSAGE_JOIN} WHERE messages.rowid IN (
            SELECT sources.representative_rowid FROM messages raw
            JOIN logical_message_sources sources ON sources.source_rowid=raw.rowid
            WHERE {where}
        ) LIMIT 2""",
        params,
    ).fetchall()
    if not targets:
        raise ValueError(f"Message with ID {message_id} not found")
    if len(targets) > 1:
        raise ValueError(
            "Message ID occurs in multiple chats or conflicting source identities. "
            "Pass the exact chat_jid returned with the message."
        )
    target = targets[0]
    adjacent = []
    for count, comparison, order in ((before, "<", "DESC"), (after, ">", "ASC")):
        sql = f"""SELECT {MESSAGE_COLUMNS} {MESSAGE_JOIN}
        WHERE messages.canonical_chat_jid=?
        AND ({TIME_KEY}, messages.id, messages.chat_jid) {comparison} (?, ?, ?)
        ORDER BY {TIME_KEY} {order}, messages.id {order}, messages.chat_jid {order} LIMIT ?"""
        adjacent.append(
            conn.execute(
                sql, (target[9], target[8], target[6], target[5], count)
            ).fetchall()
        )
    return target[:8], adjacent[0], adjacent[1]
