"""Resolve contact metadata without changing the bridge's session database."""

import os
import sqlite3
import time
import unicodedata
from pathlib import Path


def fold(value):
    return "".join(
        c
        for c in unicodedata.normalize("NFKD", value.casefold())
        if not unicodedata.combining(c)
    )


def verified_lid_pairs(rows):
    """Keep only unambiguous protocol-map pairs, never display-name evidence."""
    phones_by_lid, lids_by_phone = {}, {}
    for lid, phone in rows:
        normalized = []
        for value, suffix in ((lid, "@lid"), (phone, "@s.whatsapp.net")):
            value = str(value)
            if value.endswith(suffix):
                value = value[: -len(suffix)]
            if not value or not value.isascii() or not value.isdecimal():
                break
            normalized.append(value)
        if len(normalized) != 2:
            continue
        lid, phone = normalized
        phones_by_lid.setdefault(lid, set()).add(phone)
        lids_by_phone.setdefault(phone, set()).add(lid)
    return tuple(
        sorted(
            (lid, phone)
            for lid, phones in phones_by_lid.items()
            for phone in phones
            if len(phones) == 1 and len(lids_by_phone[phone]) == 1
        )
    )


class IdentityIndex:
    def __init__(self, path=None):
        self.path = path
        self.loaded_at = 0
        self.names = {}
        self.lid_to_phone = {}
        self.phone_to_lid = {}
        self.status = "unavailable"

    def refresh(self):
        path = self.path or os.environ.get("WHATSAPP_SESSION_DB_PATH")
        if not path:
            message_path = os.environ.get("WHATSAPP_MESSAGES_DB_PATH")
            if message_path:
                path = str(Path(message_path).with_name("whatsapp.db"))
        if time.monotonic() - self.loaded_at < 15:
            return
        self.loaded_at = time.monotonic()
        self.names, self.lid_to_phone, self.phone_to_lid = {}, {}, {}
        self.status = "unavailable"
        if not path or not Path(path).is_file():
            return
        conn = None
        try:
            conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if "whatsmeow_contacts" in tables:
                accounts = [
                    r[0]
                    for r in conn.execute(
                        "SELECT DISTINCT our_jid FROM whatsmeow_contacts"
                    )
                ]
                account = os.environ.get("WHATSAPP_ACCOUNT_JID")
                if not account and len(accounts) > 1:
                    self.status = "account_selection_required"
                    return
                if not account:
                    account = accounts[0] if accounts else ""
                for jid, first, full, push, business in conn.execute(
                    "SELECT their_jid, first_name, full_name, push_name, business_name "
                    "FROM whatsmeow_contacts WHERE our_jid=?",
                    (account,),
                ):
                    self.names[jid] = full or first or push or business or ""
            if "whatsmeow_lid_map" in tables:
                pairs = verified_lid_pairs(
                    conn.execute("SELECT lid, pn FROM whatsmeow_lid_map")
                )
                for lid, phone in pairs:
                    self.lid_to_phone[lid] = phone
                    self.phone_to_lid[phone] = lid
            self.status = "ready"
        except sqlite3.Error:
            self.names, self.lid_to_phone, self.phone_to_lid = {}, {}, {}
            self.status = "schema_or_database_error"
        finally:
            if conn is not None:
                conn.close()

    def aliases(self, value):
        self.refresh()
        if value.endswith("@g.us"):
            return [value]
        if "@" in value and not value.endswith(("@lid", "@s.whatsapp.net")):
            return [value]
        bare = value.split("@")[0]
        if value.endswith("@lid"):
            lid, phone = bare, self.lid_to_phone.get(bare)
        elif "@" not in value:
            # Bare senders can be either phone numbers or LIDs.
            if (
                bare in self.lid_to_phone
                and bare in self.phone_to_lid
                and self.lid_to_phone[bare] != bare
            ):
                return [value]
            lid = bare if bare in self.lid_to_phone else self.phone_to_lid.get(bare)
            phone = self.lid_to_phone.get(bare, bare)
        else:
            phone, lid = bare, self.phone_to_lid.get(bare)
        result = {value, bare}
        if phone:
            result.update((phone, phone + "@s.whatsapp.net"))
        if lid:
            result.update((lid, lid + "@lid"))
        return sorted(result)

    def resolve(self, jid, fallback=None):
        aliases = self.aliases(jid)
        name = next((self.names[a] for a in aliases if self.names.get(a)), None)
        bare = jid.split("@")[0]
        if jid.endswith("@lid"):
            phone = self.lid_to_phone.get(bare)
        elif "@" in jid and not jid.endswith("@s.whatsapp.net"):
            phone = None
        elif (
            "@" not in jid
            and bare in self.lid_to_phone
            and bare in self.phone_to_lid
            and self.lid_to_phone[bare] != bare
        ):
            phone = None
        else:
            phone = bare
        return {"name": name or fallback, "phone_number": phone, "aliases": aliases}


identities = IdentityIndex()
