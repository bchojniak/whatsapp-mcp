"""Connection-local logical identity; the bridge's raw archive stays untouched.

The protocol ID alone is not a key. A match additionally needs an unambiguous
protocol-map chat and author, direction, and exact timestamp. Content is kept
outside that key so alias copies and edits retain inspectable source variants.
"""

import hashlib
import json
import re
from datetime import datetime, timezone

from identity import identities, verified_lid_pairs
from security import require_bounded_int, truncate_untrusted_text

IDENTITY_POLICY = "verified_aliases_exact_id_author_direction_time_v1"
_INSTANT = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?"
    r"(Z|[+-]\d{2}:\d{2})?$"
)


def _digest(value):
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _alias_snapshot(identity_index):
    identity_index.refresh()
    evidence = list(identity_index.lid_to_phone.items())
    evidence.extend((lid, phone) for phone, lid in identity_index.phone_to_lid.items())
    pairs = verified_lid_pairs(evidence)
    return dict(pairs), {phone: lid for lid, phone in pairs}, _digest(pairs)


def _canonical_address(value, lid_to_phone, phone_to_lid):
    if not isinstance(value, str):
        return value
    if value.endswith("@lid"):
        phone = lid_to_phone.get(value[:-4])
        return phone + "@s.whatsapp.net" if phone else value
    if "@" in value:
        return value
    lid_phone = lid_to_phone.get(value)
    is_phone = value in phone_to_lid
    if lid_phone and is_phone and lid_phone != value:
        return None
    if lid_phone:
        return lid_phone + "@s.whatsapp.net"
    if is_phone:
        return value + "@s.whatsapp.net"
    return value


def _instant(value):
    """Normalize offsets without rounding WhatsApp's subsecond precision."""
    if not isinstance(value, str):
        return None
    match = _INSTANT.fullmatch(value)
    if not match:
        return None
    date, clock, fraction, offset = match.groups()
    try:
        stamp = datetime.fromisoformat(date + "T" + clock + (offset or "+00:00"))
        stamp = stamp.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None
    return stamp.isoformat(timespec="seconds"), (fraction or "").rstrip("0")


def _message_identity(rowid, message_id, chat, sender, direction, timestamp):
    instant = _instant(timestamp)
    if (
        not message_id
        or not chat
        or not sender
        or direction not in (0, 1)
        or instant is None
    ):
        return "wa1:" + _digest(
            ["unresolved", rowid, message_id, chat, sender, direction, timestamp]
        )
    return "wa1:" + _digest([message_id, chat, sender, direction, instant])


def _prepared(conn):
    return conn.execute(
        "SELECT 1 FROM sqlite_temp_master WHERE name='logical_messages'"
    ).fetchone()


def prepare_logical_messages(conn, identity_index=None, snapshot=None):
    """Build indexed TEMP membership in SQL, returning the alias revision hash.

    Only small protocol-map dictionaries enter Python memory. Archive rows stay
    in SQLite; representative selection and membership grouping happen before
    any caller applies LIMIT. A numeric snapshot excludes later raw arrivals.
    Calling again with another snapshot explicitly rebuilds membership.
    """
    if identity_index is None:
        identity_index = identities
    lid_to_phone, phone_to_lid, revision = _alias_snapshot(identity_index)
    if snapshot is not None:
        if type(snapshot) is not int or snapshot < 0:
            raise ValueError("Message identity snapshot must be a nonnegative integer")
    main_data_version = conn.execute("PRAGMA main.data_version").fetchone()[0]
    cache_exists = conn.execute(
        "SELECT 1 FROM sqlite_temp_master WHERE name='_logical_message_state'"
    ).fetchone()
    connection_changes = conn.execute("SELECT total_changes()").fetchone()[0]
    cache_key = (revision, snapshot, main_data_version, connection_changes)
    if cache_exists and _prepared(conn):
        state = conn.execute("SELECT state FROM _logical_message_state").fetchone()
        if state and tuple(json.loads(state[0])) == cache_key:
            return revision

    def canonical(value):
        return _canonical_address(value, lid_to_phone, phone_to_lid)

    conn.create_function("_wa_canonical_address", 1, canonical, deterministic=True)
    conn.create_function(
        "_wa_message_identity", 6, _message_identity, deterministic=True
    )
    conn.create_function(
        "_wa_content_variant", 3, lambda *values: _digest(values), deterministic=True
    )
    for name in ("logical_messages", "logical_message_sources"):
        conn.execute(f"DROP VIEW IF EXISTS temp.{name}")
    for name in (
        "_logical_message_rows",
        "_logical_message_groups",
        "_logical_message_state",
    ):
        conn.execute(f"DROP TABLE IF EXISTS temp.{name}")
    conn.execute(
        "CREATE TEMP TABLE _logical_message_rows ("
        "source_rowid INTEGER PRIMARY KEY, logical_id TEXT NOT NULL, "
        "canonical_chat_jid TEXT, canonical_sender TEXT, content_variant_id TEXT NOT NULL)"
    )
    columns = [row[1] for row in conn.execute("PRAGMA main.table_info(messages)")]
    filename = "filename" if "filename" in columns else "NULL"
    condition = " WHERE rowid<=?" if snapshot is not None else ""
    params = (snapshot,) if snapshot is not None else ()
    conn.execute(
        "INSERT INTO _logical_message_rows "
        "SELECT rowid, _wa_message_identity(rowid,id,_wa_canonical_address(chat_jid),"
        "_wa_canonical_address(sender),is_from_me,timestamp), "
        "_wa_canonical_address(chat_jid),_wa_canonical_address(sender),"
        f"_wa_content_variant(content,media_type,{filename}) FROM main.messages"
        + condition,
        params,
    )
    conn.execute(
        "CREATE INDEX _logical_rows_identity ON _logical_message_rows(logical_id,source_rowid)"
    )
    conn.execute(
        "CREATE TEMP TABLE _logical_message_groups (logical_id TEXT PRIMARY KEY, "
        "representative_rowid INTEGER NOT NULL UNIQUE, source_row_count INTEGER NOT NULL, "
        "content_variant_count INTEGER NOT NULL)"
    )
    conn.execute(
        "INSERT INTO _logical_message_groups "
        "SELECT logical_id,MIN(source_rowid),COUNT(*),COUNT(DISTINCT content_variant_id) "
        "FROM _logical_message_rows GROUP BY logical_id"
    )
    conn.execute(
        "CREATE TEMP VIEW logical_message_sources AS "
        "SELECT source.source_rowid,groups.representative_rowid,source.logical_id,"
        "source.canonical_chat_jid,source.canonical_sender,source.content_variant_id "
        "FROM _logical_message_rows source JOIN _logical_message_groups groups "
        "ON groups.logical_id=source.logical_id"
    )
    projection = []
    for column in columns:
        quoted = '"' + column.replace('"', '""') + '"'
        expression = f"messages.{quoted}"
        if column in {"content", "media_type", "filename", "file_length"}:
            present = (
                f"raw.{quoted}>0" if column == "file_length" else f"raw.{quoted}!=''"
            )
            missing = (
                f"messages.{quoted} IS NULL OR messages.{quoted}<=0"
                if column == "file_length"
                else f"messages.{quoted} IS NULL OR messages.{quoted}=''"
            )
            expression = (
                f"CASE WHEN {missing} THEN COALESCE((SELECT raw.{quoted} "
                "FROM _logical_message_rows member JOIN main.messages raw "
                "ON raw.rowid=member.source_rowid "
                f"WHERE member.logical_id=groups.logical_id AND {present} "
                f"ORDER BY member.source_rowid LIMIT 1),messages.{quoted}) "
                f"ELSE messages.{quoted} END"
            )
        projection.append(f"{expression} AS {quoted}")
    conn.execute(
        "CREATE TEMP VIEW logical_messages AS "
        "SELECT messages.rowid AS rowid," + ",".join(projection) + ",groups.logical_id,"
        "source.canonical_chat_jid,source.canonical_sender,groups.source_row_count,"
        "groups.content_variant_count FROM _logical_message_groups groups "
        "JOIN main.messages ON messages.rowid=groups.representative_rowid "
        "JOIN _logical_message_rows source ON source.source_rowid=groups.representative_rowid"
    )
    conn.execute("CREATE TEMP TABLE _logical_message_state (state TEXT NOT NULL)")
    connection_changes_before_state_insert = conn.execute(
        "SELECT total_changes()"
    ).fetchone()[0]
    state_marker_rows_to_insert = 1
    connection_changes_after_state_insert = (
        connection_changes_before_state_insert + state_marker_rows_to_insert
    )
    cache_key = (
        revision,
        snapshot,
        main_data_version,
        connection_changes_after_state_insert,
    )
    conn.execute(
        "INSERT INTO _logical_message_state VALUES(?)", (json.dumps(cache_key),)
    )
    return revision


def logical_key(conn, message_id, chat_jid):
    """Resolve a raw locator without discarding an existing retrieval snapshot."""
    if not _prepared(conn):
        prepare_logical_messages(conn)
    rows = conn.execute(
        "SELECT DISTINCT source.logical_id FROM logical_message_sources source "
        "JOIN main.messages raw ON raw.rowid=source.source_rowid "
        "WHERE raw.id IS ? AND raw.chat_jid IS ? LIMIT 2",
        (message_id, chat_jid),
    ).fetchall()
    if len(rows) > 1:
        raise ValueError("Raw message locator has conflicting logical identities")
    return rows[0][0] if rows else None


def _safe_string(value, maximum=512):
    return truncate_untrusted_text(str(value), maximum) if value is not None else None


def _identifier_fields(serialized, fields, nested=None):
    if not isinstance(serialized, str) or len(serialized) > 131_072:
        return {}
    try:
        value = json.loads(serialized)
    except (ValueError, TypeError):
        return {}
    if not isinstance(value, dict):
        return {}
    if nested is not None:
        value = value.get(nested)
        if not isinstance(value, dict):
            return {}
    return {
        target: _safe_string(value[source])
        for source, target in fields.items()
        if isinstance(value.get(source), str) and value[source]
    }


def _source_versions(conn, logical_id, limit, page):
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_metadata_versions'"
    ).fetchone()
    if not exists:
        return {
            "retained_source_version_count": 0,
            "source_versions": [],
            "source_versions_has_more": False,
        }
    membership = (
        " FROM message_metadata_versions version JOIN ("
        "SELECT DISTINCT raw.id,raw.chat_jid FROM logical_message_sources source "
        "JOIN main.messages raw ON raw.rowid=source.source_rowid "
        "WHERE source.logical_id=?) member "
        "ON version.message_id=member.id AND version.chat_jid=member.chat_jid"
    )
    count = conn.execute("SELECT COUNT(*)" + membership, (logical_id,)).fetchone()[0]
    rows = conn.execute(
        "SELECT version.version_id,version.message_id,version.chat_jid,"
        "version.message_type,version.source_kind,version.observed_at,"
        "version.provenance_json,version.metadata_json"
        + membership
        + " ORDER BY version_id LIMIT ? OFFSET ?",
        (logical_id, limit, page * limit),
    ).fetchall()
    return {
        "retained_source_version_count": count,
        "source_versions_scope": "raw_locator_history_including_edits_or_key_reuse",
        "source_versions": [
            {
                "version_id": row[0],
                "message_id": _safe_string(row[1]),
                "chat_jid": _safe_string(row[2]),
                "message_type": _safe_string(row[3], 64),
                "source_kind": _safe_string(row[4], 64),
                "observed_at": _safe_string(row[5], 96),
                "provider_identifiers": _identifier_fields(
                    row[6],
                    {
                        "ID": "message_id",
                        "Chat": "chat_jid",
                        "Sender": "sender_jid",
                        "SenderAlt": "sender_alt_jid",
                        "RecipientAlt": "recipient_alt_jid",
                        "conversation_jid": "conversation_jid",
                        "message_id": "message_id",
                        "remote_jid": "remote_jid",
                        "participant_jid": "participant_jid",
                    },
                ),
                "context_identifiers": _identifier_fields(
                    row[7],
                    {
                        name: name
                        for name in (
                            "reply_to_message_id",
                            "reply_sender_id",
                            "reply_chat_jid",
                            "reaction_to_message_id",
                        )
                    },
                    nested="context",
                ),
            }
            for row in rows
        ],
        "source_versions_has_more": (page + 1) * limit < count,
    }


def _historical_rows(conn, logical_id, limit, page):
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_row_versions'"
    ).fetchone()
    if not exists:
        return {
            "historical_row_version_count": 0,
            "historical_row_versions": [],
            "historical_row_versions_has_more": False,
            "first_retained_row_snapshot_at": None,
            "latest_retained_row_change_at": None,
        }
    membership = (
        " FROM message_row_versions version JOIN ("
        "SELECT DISTINCT raw.id,raw.chat_jid FROM logical_message_sources source "
        "JOIN main.messages raw ON raw.rowid=source.source_rowid "
        "WHERE source.logical_id=?) member "
        "ON version.message_id IS member.id AND version.chat_jid IS member.chat_jid"
    )
    count, first_snapshot, latest_change = conn.execute(
        "SELECT COUNT(*),MIN(version.captured_at),MAX(version.captured_at)"
        + membership,
        (logical_id,),
    ).fetchone()
    rows = conn.execute(
        "SELECT version.version_id,version.message_rowid,version.message_id,"
        "version.chat_jid,version.sender,version.timestamp,version.is_from_me,"
        "version.content,version.media_type,version.captured_at,"
        "_wa_message_identity(version.message_rowid,version.message_id,"
        "_wa_canonical_address(version.chat_jid),_wa_canonical_address(version.sender),"
        "version.is_from_me,version.timestamp),"
        "_wa_content_variant(version.content,version.media_type,version.filename)"
        + membership
        + " ORDER BY version.version_id LIMIT ? OFFSET ?",
        (logical_id, limit, page * limit),
    ).fetchall()
    return {
        "historical_row_version_count": count,
        "first_retained_row_snapshot_at": _safe_string(first_snapshot, 96),
        "latest_retained_row_change_at": _safe_string(latest_change, 96),
        "historical_row_versions_scope": "raw_locator_history_including_edits_or_key_reuse",
        "historical_row_versions": [
            {
                "version_id": row[0],
                "source_rowid": row[1],
                "message_id": _safe_string(row[2]),
                "chat_jid": _safe_string(row[3]),
                "sender": _safe_string(row[4]),
                "timestamp": _safe_string(row[5], 96),
                "is_from_me": bool(row[6]) if row[6] is not None else None,
                "content_preview": _safe_string(row[7], 256),
                "media_type": _safe_string(row[8], 64),
                "captured_at": _safe_string(row[9], 96),
                "inferred_logical_id": row[10],
                "matches_current_logical_identity": row[10] == logical_id,
                "content_variant_id": row[11],
            }
            for row in rows
        ],
        "historical_row_versions_has_more": (page + 1) * limit < count,
    }


def logical_identity(conn, message_id, chat_jid, limit=50, page=0):
    """Return paginated, allowlisted source locators and content-variant evidence."""
    require_bounded_int("limit", limit, 1, 50)
    require_bounded_int("page", page, 0, 2**31 - 1)
    key = logical_key(conn, message_id, chat_jid)
    if key is None:
        return None
    representative = conn.execute(
        "SELECT canonical_chat_jid,canonical_sender,source_row_count,content_variant_count "
        "FROM logical_messages WHERE logical_id=?",
        (key,),
    ).fetchone()
    rows = conn.execute(
        "SELECT raw.rowid,raw.id,raw.chat_jid,raw.sender,raw.timestamp,raw.is_from_me,"
        "source.content_variant_id,raw.content,raw.media_type "
        "FROM logical_message_sources source JOIN main.messages raw "
        "ON raw.rowid=source.source_rowid WHERE source.logical_id=? "
        "ORDER BY raw.rowid LIMIT ? OFFSET ?",
        (key, limit, page * limit),
    ).fetchall()
    source_identifiers = []
    for row in rows:
        source_identifiers.append(
            {
                "source_rowid": row[0],
                "message_id": _safe_string(row[1]),
                "chat_jid": _safe_string(row[2]),
                "sender": _safe_string(row[3]),
                "timestamp": _safe_string(row[4], 96),
                "is_from_me": bool(row[5]) if row[5] is not None else None,
                "content_variant_id": row[6],
                "content_preview": _safe_string(row[7], 256),
                "media_type": _safe_string(row[8], 64),
            }
        )
    source_has_more = (page + 1) * limit < representative[2]
    versions = _source_versions(conn, key, limit, page)
    historical_rows = _historical_rows(conn, key, limit, page)
    has_more = (
        source_has_more
        or versions["source_versions_has_more"]
        or historical_rows["historical_row_versions_has_more"]
    )
    collision_count = conn.execute(
        "SELECT COUNT(DISTINCT source.logical_id) FROM logical_message_sources source "
        "JOIN main.messages raw ON raw.rowid=source.source_rowid "
        "WHERE raw.id IS ? AND source.canonical_chat_jid IS ?",
        (message_id, representative[0]),
    ).fetchone()[0]
    return {
        "logical_id": key,
        "identity_policy": IDENTITY_POLICY,
        "observation_times": {
            "first_retrieved_at": None,
            "last_seen_at": None,
            "repeat_sightings_recorded": False,
            "notice": (
                "Message timestamps are sent times. Source observed_at records a retained "
                "capture or legacy observation; row captured_at records a snapshot. "
                "Neither proves first-ever retrieval, and identical repeat sightings are not tracked."
            ),
        },
        "canonical_chat_jid": _safe_string(representative[0]),
        "canonical_sender": _safe_string(representative[1]),
        "source_row_count": representative[2],
        "source_identifiers_total": representative[2],
        "source_identifiers": source_identifiers,
        "source_identifiers_has_more": source_has_more,
        "source_identifiers_truncated": len(rows) < representative[2],
        "source_identifiers_page": page,
        "content_variant_count": representative[3],
        "other_logical_messages_with_same_id": max(0, collision_count - 1),
        "page": page,
        "limit": limit,
        "has_more": has_more,
        "next_page": page + 1 if has_more else None,
        **versions,
        **historical_rows,
    }
