import json
import logging
import mimetypes
import os.path
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import audio
import requests
from archive_status import archive_coverage
from history_diagnostics import history_diagnostics
from identity import fold, identities
from ingestion_status import ingestion_status
from logical_messages import logical_identity, prepare_logical_messages
from message_metadata import public_media_metadata, public_message_metadata
from retrieval import context_rows, next_cursor, query_messages
from security import MAX_MESSAGE_CHARS, MAX_TEXT_RESULT_CHARS, truncate_untrusted_text

MESSAGES_DB_PATH = os.environ.get(
    "WHATSAPP_MESSAGES_DB_PATH",
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "whatsapp-bridge",
        "store",
        "messages.db",
    ),
)
WHATSAPP_API_BASE_URL = os.environ.get(
    "WHATSAPP_API_BASE_URL", "http://localhost:8080/api"
).rstrip("/")
identities.path = os.environ.get(
    "WHATSAPP_SESSION_DB_PATH", str(Path(MESSAGES_DB_PATH).with_name("whatsapp.db"))
)


def open_messages_db():
    return sqlite3.connect(
        Path(MESSAGES_DB_PATH).resolve().as_uri() + "?mode=ro", uri=True
    )


def database_error(error):
    logging.getLogger(__name__).error(
        "WhatsApp database query failed: %s", type(error).__name__
    )
    raise RuntimeError(
        "WhatsApp database query failed; check sync diagnostics and server logs."
    ) from error


@contextmanager
def messages_connection():
    """Close read connections and translate SQLite failures at one boundary."""
    connection = None
    try:
        connection = open_messages_db()
        yield connection
    except sqlite3.Error as error:
        database_error(error)
    finally:
        if connection is not None:
            connection.close()


def resolve_chat(chat):
    if chat and not chat.is_group:
        chat.name = identities.resolve(chat.jid, chat.name)["name"]
    return chat


@dataclass
class Message:
    timestamp: datetime
    sender: str
    content: str
    is_from_me: bool
    chat_jid: str
    id: str
    chat_name: Optional[str] = None
    media_type: Optional[str] = None
    metadata: Optional[dict] = None


@dataclass
class Chat:
    jid: str
    name: Optional[str]
    last_message_time: Optional[datetime]
    last_message: Optional[str] = None
    last_sender: Optional[str] = None
    last_is_from_me: Optional[bool] = None

    @property
    def is_group(self) -> bool:
        """Determine if chat is a group based on JID pattern."""
        return self.jid.endswith("@g.us")


@dataclass
class Contact:
    phone_number: Optional[str]
    name: Optional[str]
    jid: str


@dataclass
class MessageContext:
    message: Message
    before: List[Message]
    after: List[Message]


def _message_from_row(row, conn=None) -> Message:
    (
        timestamp,
        sender,
        chat_name,
        content,
        is_from_me,
        chat_jid,
        message_id,
        media_type,
    ) = row
    message = Message(
        timestamp=datetime.fromisoformat(timestamp),
        sender=sender,
        chat_name=chat_name,
        content=content,
        is_from_me=is_from_me,
        chat_jid=chat_jid,
        id=message_id,
        media_type=media_type,
    )
    if conn is not None:
        message.metadata = public_message_metadata(conn, message.id, message.chat_jid)
        message.metadata["identity"] = logical_identity(
            conn, message.id, message.chat_jid, limit=3
        )
        if message.media_type:
            media_source = message_media_source(conn, message.id, message.chat_jid)
            if media_source:
                source_id, source_jid = media_source[:2]
                message.metadata["media"] = {
                    **public_media_metadata(conn, source_id, source_jid),
                    "source_reference": {
                        "message_id": source_id,
                        "chat_jid": source_jid,
                    },
                }
    return message


def _chat_from_row(row) -> Chat:
    jid, name, last_message_time, last_message, last_sender, last_is_from_me = row
    return resolve_chat(
        Chat(
            jid=jid,
            name=name,
            last_message_time=datetime.fromisoformat(last_message_time)
            if last_message_time
            else None,
            last_message=last_message,
            last_sender=last_sender,
            last_is_from_me=last_is_from_me,
        )
    )


def get_sender_name(sender_jid: str) -> str:
    with messages_connection() as conn:
        cursor = conn.cursor()

        identity = identities.resolve(sender_jid)
        if identity["name"]:
            return identity["name"]
        aliases = identity["aliases"]
        placeholders = ",".join("?" for _ in aliases)
        cursor.execute(
            f"SELECT name FROM chats WHERE jid IN ({placeholders}) "
            "AND name IS NOT NULL AND name != ? ORDER BY jid LIMIT 1",
            (*aliases, ""),
        )
        result = cursor.fetchone()
        return result[0] if result else sender_jid


def format_message(message: Message, show_chat_info: bool = True) -> str:
    """Format a single message while bounding all untrusted text fields."""
    output = ""
    chat_name = truncate_untrusted_text(
        identities.resolve(message.chat_jid, message.chat_name)["name"] or "", 256
    )

    if show_chat_info and chat_name:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] Chat: {chat_name} "
    else:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] "

    content_prefix = ""
    if hasattr(message, "media_type") and message.media_type:
        media_type = truncate_untrusted_text(message.media_type, 64)
        message_id = truncate_untrusted_text(message.id, 256)
        chat_jid = truncate_untrusted_text(message.chat_jid, 256)
        content_prefix = (
            f"[{media_type} - Message ID: {message_id} - Chat JID: {chat_jid}] "
        )

    try:
        sender_name = (
            get_sender_name(message.sender) if not message.is_from_me else "Me"
        )
        sender_name = truncate_untrusted_text(sender_name, 256)
        content = truncate_untrusted_text(message.content or "")
        output += f"From: {sender_name}: {content_prefix}{content}\n"
    except Exception as e:
        raise RuntimeError("Could not format WhatsApp message") from e
    return output


def format_messages_list(messages: List[Message], show_chat_info: bool = True) -> str:
    output = ""
    if not messages:
        return "No messages to display."

    for message in messages:
        formatted = format_message(message, show_chat_info)
        if len(output) + len(formatted) > MAX_TEXT_RESULT_CHARS:
            output += "[remaining messages truncated by WhatsApp MCP security limit]"
            break
        output += formatted
    return output


def message_media_source(conn, message_id, chat_jid):
    """Choose a physical media variant, preferring complete retrieval references."""
    columns = {row[1] for row in conn.execute("PRAGMA main.table_info(messages)")}
    filename = "filename" if "filename" in columns else "NULL"
    length = "file_length" if "file_length" in columns else "NULL"
    own = conn.execute(
        f"SELECT id, chat_jid, media_type, {filename}, {length} FROM messages "
        "WHERE id=? AND chat_jid=? AND COALESCE(media_type,'')!=''",
        (message_id, chat_jid),
    ).fetchone()
    if not {"sender", "timestamp", "is_from_me", "content"} <= columns:
        return own
    if not conn.execute(
        "SELECT 1 FROM sqlite_temp_master WHERE name='logical_messages'"
    ).fetchone():
        prepare_logical_messages(conn, identities)
    filename = "media.filename" if "filename" in columns else "NULL"
    length = "media.file_length" if "file_length" in columns else "NULL"
    readiness = "0"
    if {"url", "media_key", "file_sha256", "file_enc_sha256", "file_length"} <= columns:
        path_available = "COALESCE(media.url,'')!=''"
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='message_metadata'"
        ).fetchone():
            path_available += (
                " OR EXISTS (SELECT 1 FROM message_metadata metadata "
                "WHERE metadata.message_id=media.id AND metadata.chat_jid=media.chat_jid "
                "AND CASE WHEN json_valid(metadata.metadata_json) THEN "
                "COALESCE(json_extract(metadata.metadata_json,'$.media.direct_path'),'')!='' ELSE 0 END)"
            )
        readiness = (
            f"(({path_available}) AND length(media.media_key)>0 "
            "AND length(media.file_sha256)>0 AND length(media.file_enc_sha256)>0 "
            "AND media.file_length>0)"
        )
    return conn.execute(
        f"SELECT media.id,media.chat_jid,media.media_type,{filename},{length} "
        "FROM messages target JOIN logical_message_sources target_source ON target_source.source_rowid=target.rowid "
        "JOIN logical_message_sources sources ON sources.logical_id=target_source.logical_id "
        "JOIN messages media ON media.rowid=sources.source_rowid "
        "WHERE target.id=? AND target.chat_jid=? AND COALESCE(media.media_type,'')!='' "
        f"ORDER BY COALESCE({readiness},0) DESC, (media.rowid=target.rowid) DESC, media.rowid LIMIT 1",
        (message_id, chat_jid),
    ).fetchone()


def _message_record(conn, message, sender_names):
    sender = "Me"
    if not message.is_from_me:
        if message.sender not in sender_names:
            sender_names[message.sender] = get_sender_name(message.sender)
        sender = sender_names[message.sender]
    record = {
        "message_id": message.id,
        "chat_jid": message.chat_jid,
        "chat_name": truncate_untrusted_text(
            identities.resolve(message.chat_jid, message.chat_name)["name"] or "", 256
        ),
        "timestamp": message.timestamp.isoformat(),
        "sender_name": truncate_untrusted_text(sender, 256),
        "sender_id": message.sender,
        "is_from_me": bool(message.is_from_me),
        "content": truncate_untrusted_text(message.content or ""),
        "content_truncated": len(message.content or "") > MAX_MESSAGE_CHARS,
    }
    record.update(public_message_metadata(conn, message.id, message.chat_jid))
    record["identity"] = logical_identity(conn, message.id, message.chat_jid, limit=3)
    media_source = message_media_source(conn, message.id, message.chat_jid)
    if media_source:
        source_id, source_jid, media_type, filename, length = media_source
        mime_type = mimetypes.guess_type(filename or "")[0]
        record["media"] = {
            "type": media_type,
            "filename": truncate_untrusted_text(filename, 512) if filename else None,
            "byte_length": length,
            "mime_type": mime_type,
            "availability": "not_checked",
            "inline_limit_bytes": 8 * 1024 * 1024,
            "source_reference": {"message_id": source_id, "chat_jid": source_jid},
            **public_media_metadata(conn, source_id, source_jid),
        }
    return record


def _structured_messages(
    conn,
    rows,
    has_more,
    template,
    database_path,
    chat_aliases,
    include_context,
    context_before,
    context_after,
):
    matches, context_records = [], []
    seen, sender_names = set(), {}
    returned_rows = []
    remaining = 16_000
    truncated = False
    for row in rows:
        message = _message_from_row(row[:8])
        record = _message_record(conn, message, sender_names)
        cost = len(json.dumps(record, ensure_ascii=False))
        if cost > remaining and matches:
            has_more = True
            truncated = True
            break
        remaining -= cost
        matches.append(record)
        returned_rows.append(row)
        seen.add((message.chat_jid, message.id))
    if include_context:
        for row in returned_rows:
            _, before_rows, after_rows = context_rows(
                conn,
                row[6],
                row[5],
                context_before,
                context_after,
                identity_index=identities,
            )
            for context_row in [*reversed(before_rows), *after_rows]:
                message = _message_from_row(context_row)
                key = (message.chat_jid, message.id)
                if key in seen:
                    continue
                seen.add(key)
                record = _message_record(conn, message, sender_names)
                cost = len(json.dumps(record, ensure_ascii=False))
                if cost > remaining or len(context_records) >= 50:
                    truncated = True
                    continue
                remaining -= cost
                context_records.append(record)
    return {
        "matches": matches,
        "context": context_records,
        "has_more": has_more,
        "next_cursor": next_cursor(template, returned_rows[-1])
        if has_more and returned_rows
        else None,
        "truncated": truncated,
        "coverage": archive_coverage(
            conn, database_path, chat_aliases, identity_index=identities
        ),
    }


def list_messages(
    after: Optional[str] = None,
    before: Optional[str] = None,
    sender_phone_number: Optional[str] = None,
    chat_jid: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1,
    response_format: str = "text",
    query_mode: str = "substring",
    cursor: Optional[str] = None,
    media_type: Optional[str] = None,
):
    if response_format not in {"text", "structured"}:
        raise ValueError("response_format must be 'text' or 'structured'")
    if query_mode not in {"substring", "full_text"}:
        raise ValueError("query_mode must be 'substring' or 'full_text'")
    if media_type not in {
        None,
        "any",
        "image",
        "audio",
        "video",
        "document",
        "sticker",
        "sticker_pack",
        "gif",
        "voice_note",
        "video_note",
    }:
        raise ValueError(
            "media_type must be 'any', 'image', 'audio', 'video', 'document', 'sticker', 'sticker_pack', 'gif', 'voice_note', or 'video_note'"
        )
    if cursor and (page != 0 or response_format != "structured"):
        raise ValueError("Use cursor with response_format='structured' and page=0")
    sender_aliases = (
        identities.aliases(sender_phone_number) if sender_phone_number else None
    )
    chat_aliases = identities.aliases(chat_jid) if chat_jid else None
    with messages_connection() as conn:
        conn.execute("BEGIN")
        rows, has_more, template = query_messages(
            conn,
            after=after,
            before=before,
            sender_aliases=sender_aliases,
            chat_aliases=chat_aliases,
            query=query,
            query_mode=query_mode,
            media_type=media_type,
            limit=limit,
            page=page,
            cursor=cursor,
            identity_index=identities,
        )
        if response_format == "structured":
            return _structured_messages(
                conn,
                rows,
                has_more,
                template,
                MESSAGES_DB_PATH,
                chat_aliases,
                include_context,
                context_before,
                context_after,
            )
        messages, seen = [], set()
        for row in rows:
            selected = [row[:8]]
            if include_context:
                target, before_rows, after_rows = context_rows(
                    conn,
                    row[6],
                    row[5],
                    context_before,
                    context_after,
                    identity_index=identities,
                )
                selected = [*before_rows, target, *after_rows]
            for message_row in selected:
                key = (message_row[5], message_row[6])
                if key not in seen:
                    seen.add(key)
                    messages.append(_message_from_row(message_row))
        return format_messages_list(messages, show_chat_info=True)


def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5,
    chat_jid: Optional[str] = None,
) -> MessageContext:
    with messages_connection() as conn:
        conn.execute("BEGIN")
        target, before_rows, after_rows = context_rows(
            conn, message_id, chat_jid, before, after, identity_index=identities
        )
        return MessageContext(
            message=_message_from_row(target, conn),
            before=[_message_from_row(row, conn) for row in before_rows],
            after=[_message_from_row(row, conn) for row in after_rows],
        )


def get_message_identity(
    message_id: str, chat_jid: str, limit: int = 50, page: int = 0
):
    """Enumerate safe original source identifiers without exposing payloads or keys."""
    with messages_connection() as conn:
        conn.execute("BEGIN")
        prepare_logical_messages(conn, identities)
        return logical_identity(conn, message_id, chat_jid, limit=limit, page=page)


def _prepare_logical_chats(conn):
    prepare_logical_messages(conn, identities)
    conn.execute("DROP TABLE IF EXISTS temp._logical_chats")
    conn.execute("DROP TABLE IF EXISTS temp._logical_chat_members")
    conn.execute(
        "CREATE TEMP TABLE _logical_chat_members AS "
        "SELECT rowid AS source_rowid, jid, name, last_message_time, "
        "COALESCE(_wa_canonical_address(jid),jid) AS canonical_chat_jid FROM chats"
    )
    conn.execute(
        "CREATE INDEX _logical_chat_members_family "
        "ON _logical_chat_members(canonical_chat_jid)"
    )
    conn.execute(
        "CREATE TEMP TABLE _logical_chats AS "
        "SELECT member.jid, member.name, "
        "(SELECT latest.last_message_time FROM _logical_chat_members latest "
        "WHERE latest.canonical_chat_jid=family.canonical_chat_jid "
        "ORDER BY julianday(latest.last_message_time) DESC, latest.source_rowid DESC "
        "LIMIT 1) AS last_message_time, "
        "family.canonical_chat_jid FROM _logical_chat_members member JOIN "
        "(SELECT canonical_chat_jid, MIN(source_rowid) AS representative_rowid "
        "FROM _logical_chat_members "
        "GROUP BY canonical_chat_jid) family "
        "ON member.source_rowid=family.representative_rowid"
    )


def _logical_chat_query(include_last_message=True):
    fields = "NULL, NULL, NULL"
    join = ""
    if include_last_message:
        fields = "m.content, m.sender, m.is_from_me"
        join = (
            " LEFT JOIN logical_messages m ON m.rowid = "
            "(SELECT rowid FROM logical_messages "
            "WHERE COALESCE(canonical_chat_jid,chat_jid)=c.canonical_chat_jid "
            "ORDER BY julianday(timestamp) DESC, rowid DESC LIMIT 1)"
        )
    return (
        f"SELECT c.jid, c.name, c.last_message_time, {fields} "
        "FROM _logical_chats c" + join
    )


def _contact_addresses(conn, jid):
    canonical = conn.execute("SELECT _wa_canonical_address(?)", (jid,)).fetchone()[0]
    addresses = {canonical or jid}
    if canonical == jid:
        if "@" not in jid:
            addresses.add(jid + "@s.whatsapp.net")
        elif jid.endswith(("@lid", "@s.whatsapp.net")):
            bare = jid.split("@")[0]
            bare_canonical = conn.execute(
                "SELECT _wa_canonical_address(?)", (bare,)
            ).fetchone()[0]
            if bare_canonical == bare:
                addresses.add(bare)
    return sorted(addresses)


def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active",
) -> List[Chat]:
    """Get chats matching the specified criteria."""
    with messages_connection() as conn:
        conn.execute("BEGIN")
        _prepare_logical_chats(conn)
        sql = _logical_chat_query(include_last_message)
        params = []
        if query:
            conn.create_function(
                "wa_fold", 1, lambda value: fold(value or ""), deterministic=True
            )
            names = dict(identities.names)
            for lid, phone in identities.lid_to_phone.items():
                names.setdefault(lid + "@lid", "")
                names.setdefault(phone + "@s.whatsapp.net", "")
            aliases = set()
            for jid, name in names.items():
                if fold(query) in fold((name or "") + " " + jid):
                    aliases.update(identities.aliases(jid))
            clause = "(wa_fold(member.name) LIKE wa_fold(?) OR member.jid LIKE ?"
            params.extend([f"%{query}%", f"%{query}%"])
            if aliases:
                clause += " OR member.jid IN (" + ",".join("?" for _ in aliases) + ")"
                params.extend(sorted(aliases))
            sql += (
                " WHERE EXISTS (SELECT 1 FROM _logical_chat_members member "
                "WHERE member.canonical_chat_jid=c.canonical_chat_jid AND "
                + clause
                + "))"
            )
        order_by = (
            "julianday(c.last_message_time) DESC"
            if sort_by == "last_active"
            else "c.name"
        )
        sql += f" ORDER BY {order_by}, c.jid LIMIT ? OFFSET ?"
        params.extend([limit, page * limit])
        return [_chat_from_row(row) for row in conn.execute(sql, params)]


def search_contacts(query: str) -> List[Contact]:
    """Accent-insensitive name/phone lookup including mapped LID contacts."""
    with messages_connection() as conn:
        identities.refresh()
        names = dict(identities.names)
        for jid, name in conn.execute(
            "SELECT jid, name FROM chats WHERE jid NOT LIKE '%@g.us'"
        ):
            names.setdefault(jid, name)
        result = []
        seen = set()
        needle = fold(query)
        for jid in sorted(names):
            identity = identities.resolve(jid, names[jid])
            searchable = " ".join(
                [identity["name"] or "", jid, identity["phone_number"] or ""]
            )
            key = identity["phone_number"] or jid
            if needle not in fold(searchable) or key in seen:
                continue
            seen.add(key)
            result.append(Contact(identity["phone_number"], identity["name"], jid))
            if len(result) == 50:
                break
        return result


def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> List[Chat]:
    """Get all chats involving the contact.

    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    with messages_connection() as conn:
        conn.execute("BEGIN")
        _prepare_logical_chats(conn)
        addresses = _contact_addresses(conn, jid)
        placeholders = ",".join("?" for _ in addresses)
        sql = _logical_chat_query() + (
            f" WHERE c.canonical_chat_jid IN ({placeholders}) OR EXISTS "
            "(SELECT 1 FROM logical_messages participant "
            "WHERE COALESCE(participant.canonical_chat_jid,participant.chat_jid)="
            "c.canonical_chat_jid AND "
            f"COALESCE(participant.canonical_sender,participant.sender) IN ({placeholders}))"
            " ORDER BY julianday(c.last_message_time) DESC, c.jid LIMIT ? OFFSET ?"
        )
        rows = conn.execute(sql, (*addresses, *addresses, limit, page * limit))
        return [_chat_from_row(row) for row in rows]


def get_last_interaction(jid: str) -> str:
    """Get most recent message involving the contact."""
    with messages_connection() as conn:
        conn.execute("BEGIN")
        _prepare_logical_chats(conn)
        addresses = _contact_addresses(conn, jid)
        placeholders = ",".join("?" for _ in addresses)
        row = conn.execute(
            "SELECT m.timestamp, m.sender, c.name, m.content, m.is_from_me, "
            "m.chat_jid, m.id, m.media_type FROM logical_messages m "
            "JOIN _logical_chats c ON "
            "COALESCE(m.canonical_chat_jid,m.chat_jid)=c.canonical_chat_jid "
            f"WHERE COALESCE(m.canonical_sender,m.sender) IN ({placeholders}) "
            f"OR c.canonical_chat_jid IN ({placeholders}) "
            "ORDER BY julianday(m.timestamp) DESC, m.rowid DESC LIMIT 1",
            (*addresses, *addresses),
        ).fetchone()
        if not row:
            return None
        message = _message_from_row(row)
        if message.media_type:
            source = message_media_source(conn, message.id, message.chat_jid)
            if source:
                message.id, message.chat_jid, message.media_type = source[:3]
        return format_message(message)


def get_chat(chat_jid: str, include_last_message: bool = True) -> Optional[Chat]:
    """Get chat metadata by JID."""
    with messages_connection() as conn:
        conn.execute("BEGIN")
        _prepare_logical_chats(conn)
        row = conn.execute(
            _logical_chat_query(include_last_message)
            + " WHERE c.canonical_chat_jid=COALESCE(_wa_canonical_address(?),?)",
            (chat_jid, chat_jid),
        ).fetchone()
        return _chat_from_row(row) if row else None


def get_direct_chat_by_contact(sender_phone_number: str) -> Optional[Chat]:
    if "@" not in sender_phone_number:
        chat = get_chat(sender_phone_number + "@s.whatsapp.net")
        if chat:
            return chat
        identities.refresh()
        if (
            sender_phone_number in identities.lid_to_phone
            and sender_phone_number not in identities.phone_to_lid
        ):
            return get_chat(sender_phone_number + "@lid")
        return None
    if sender_phone_number.endswith(("@lid", "@s.whatsapp.net")):
        return get_chat(sender_phone_number)
    return None


def _send_response(response) -> Tuple[bool, str]:
    if response.status_code != 200:
        return False, f"Error: HTTP {response.status_code} - {response.text}"
    result = response.json()
    return result.get("success", False), result.get("message", "Unknown response")


def _post_send(payload) -> Tuple[bool, str]:
    """Keep bridge transport and response errors consistent across send tools."""
    try:
        response = requests.post(
            f"{WHATSAPP_API_BASE_URL}/send", json=payload, timeout=30
        )
        return _send_response(response)
    except requests.RequestException as error:
        return False, f"Request error: {error}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as error:
        return False, f"Unexpected error: {error}"


def send_message(recipient: str, message: str) -> Tuple[bool, str]:
    try:
        if not recipient:
            return False, "Recipient must be provided"
        return _post_send({"recipient": recipient, "message": message})
    except Exception as error:
        return False, f"Unexpected error: {error}"


def send_text_message(recipient: str, message: str):
    """Return bounded send references without forwarding raw bridge errors."""
    try:
        response = requests.post(
            f"{WHATSAPP_API_BASE_URL}/send",
            json={"recipient": recipient, "message": message},
            timeout=30,
        )
        if response.status_code != 200:
            return False, {}
        data = response.json()
        if data.get("success") is not True:
            return False, {}
        return True, {
            "message_id": data.get("message_id"),
            "archive_status": data.get("archive_status"),
        }
    except (requests.RequestException, ValueError, TypeError, AttributeError):
        return False, {}


def _send_media(recipient, media_path, *, voice_note=False) -> Tuple[bool, str]:
    try:
        if not recipient:
            return False, "Recipient must be provided"
        if not media_path:
            return False, "Media path must be provided"
        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"
        if voice_note and not media_path.endswith(".ogg"):
            try:
                media_path = audio.convert_to_opus_ogg_temp(media_path)
            except Exception as error:
                return (
                    False,
                    f"Error converting file to opus ogg. You likely need to install ffmpeg: {error}",
                )
        return _post_send({"recipient": recipient, "media_path": media_path})
    except Exception as error:
        return False, f"Unexpected error: {error}"


def send_file(recipient: str, media_path: str) -> Tuple[bool, str]:
    return _send_media(recipient, media_path)


def send_audio_message(recipient: str, media_path: str) -> Tuple[bool, str]:
    return _send_media(recipient, media_path, voice_note=True)


def download_media(
    message_id: str, chat_jid: str
) -> tuple[Optional[str], Optional[str]]:
    """Download media from a message.

    Returns (local_path, error). error is None on success.
    """
    try:
        url = f"{WHATSAPP_API_BASE_URL}/download"
        payload = {"message_id": message_id, "chat_jid": chat_jid}
        response = requests.post(url, json=payload, timeout=30)
        if response.status_code == 200:
            result = response.json()
            if result.get("success", False):
                path = result.get("path")
                logging.getLogger(__name__).info("WhatsApp media download succeeded")
                return path, None
            return None, result.get("message", "Unknown download error")
        return None, f"HTTP {response.status_code} - {response.text}"
    except requests.RequestException as e:
        return None, f"Request error: {e}"
    except json.JSONDecodeError:
        return None, f"Error parsing response: {response.text}"
    except Exception as e:
        return None, f"Unexpected error: {e}"


def get_sync_status(chat_jid: Optional[str] = None):
    """Health metadata only. Last message time is not a sync heartbeat."""
    status = {
        "bridge_api_reachable": False,
        "connected": None,
        "reauthentication_required": None,
        "database_readable": False,
        "message_count": None,
        "raw_message_count": None,
        "raw_chat_count": None,
        "chat_count": None,
        "last_message_at": None,
        "last_successful_sync_at": None,
        "ingestion": None,
    }
    try:
        response = requests.get(f"{WHATSAPP_API_BASE_URL}/health", timeout=5)
        status["bridge_api_reachable"] = response.status_code == 200
        if response.status_code == 200:
            health = response.json()
            if isinstance(health, dict):
                for target, keys in [
                    ("connected", ["connected", "is_connected"]),
                    (
                        "reauthentication_required",
                        ["reauthentication_required", "requires_qr"],
                    ),
                ]:
                    for key in keys:
                        if isinstance(health.get(key), bool):
                            status[target] = health[key]
                            break
                for key in ("last_event_at", "last_successful_sync_at"):
                    value = health.get(key)
                    if isinstance(value, str):
                        try:
                            status[key] = datetime.fromisoformat(value).isoformat()
                        except ValueError:
                            pass
                if health.get("observation_scope") in ("current_process", "persistent"):
                    status["observation_scope"] = health["observation_scope"]
                status["ingestion"] = ingestion_status(health.get("ingestion"))
                if isinstance(health.get("history_diagnostics"), dict):
                    status["history_diagnostics"] = history_diagnostics(
                        health["history_diagnostics"]
                    )
    except (requests.RequestException, ValueError):
        pass
    conn = None
    try:
        conn = open_messages_db()
        conn.execute("BEGIN")
        _prepare_logical_chats(conn)
        count, latest = conn.execute(
            "SELECT COUNT(*), MAX(timestamp) FROM logical_messages"
        ).fetchone()
        status.update(
            database_readable=True,
            message_count=count,
            last_message_at=latest,
            chat_count=conn.execute("SELECT COUNT(*) FROM _logical_chats").fetchone()[
                0
            ],
            raw_chat_count=conn.execute("SELECT COUNT(*) FROM chats").fetchone()[0],
            raw_message_count=conn.execute("SELECT COUNT(*) FROM messages").fetchone()[
                0
            ],
        )
        status["archive"] = archive_coverage(
            conn,
            MESSAGES_DB_PATH,
            identities.aliases(chat_jid) if chat_jid else None,
            identity_index=identities,
        )
        status["full_text_search_available"] = bool(
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE name='messages_fts'"
            ).fetchone()
        )
    except sqlite3.Error:
        status["archive_status_available"] = False
        status["database_error"] = (
            "Database is unavailable or its schema is incompatible."
        )
    finally:
        if conn is not None:
            conn.close()
    identities.refresh()
    status["identity_resolution"] = identities.status
    status["sync_heartbeat_available"] = False
    return status
