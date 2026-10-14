#!/usr/bin/env python3
"""Issue/rotate/revoke keys without printing credentials or uploading anything."""

import argparse
import hashlib
import json
import os
import secrets
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
from access_control import _CLIENT_ID, ALL_TOOLS, READ_TOOLS, AccessPolicy


def private_write(path, value):
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(value)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def manage(directory, action, client, tools=None):
    if not _CLIENT_ID.fullmatch(client):
        raise ValueError(
            "Client ID must be lowercase letters, digits, underscores or hyphens"
        )
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    registry = directory / "clients.json"
    config = json.loads(registry.read_text()) if registry.exists() else {}
    if action in {"grant-send", "revoke-send"}:
        if client not in config or not config[client].get("enabled", True):
            raise ValueError("Client must exist and be enabled")
        allowed = set(config[client].get("tools", READ_TOOLS))
        if action == "grant-send":
            allowed.add("send_message")
        else:
            allowed.discard("send_message")
        config[client]["tools"] = sorted(allowed)
    elif action == "revoke":
        if client not in config:
            raise ValueError("Client does not exist")
        config[client]["enabled"] = False
    else:
        if action == "issue" and client in config:
            raise ValueError("Client already exists; use rotate")
        if action == "rotate" and client not in config:
            raise ValueError("Client does not exist; use issue")
        allowed = tools
        if allowed is None:
            allowed = config.get(client, {}).get("tools", sorted(READ_TOOLS))
        key = secrets.token_urlsafe(32)
        config[client] = {
            "key_sha256": hashlib.sha256(key.encode()).hexdigest(),
            "enabled": True,
            "tools": allowed,
        }
    raw = json.dumps(config, sort_keys=True, separators=(",", ":"))
    AccessPolicy.from_json(raw)
    key_path = directory / f"{client}.key"
    if action in {"issue", "rotate"}:
        private_write(key_path, key + "\n")
    private_write(registry, raw + "\n")
    if action == "revoke":
        key_path.unlink(missing_ok=True)
    return registry


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=["issue", "rotate", "revoke", "grant-send", "revoke-send"]
    )
    parser.add_argument("client")
    parser.add_argument(
        "--directory",
        type=Path,
        default=Path(__file__).resolve().parents[1] / ".access",
    )
    parser.add_argument("--tools", nargs="+", choices=sorted(ALL_TOOLS))
    args = parser.parse_args()
    try:
        registry = manage(args.directory, args.action, args.client, args.tools)
    except (ValueError, RuntimeError):
        parser.exit(1, "Invalid policy or action; credential files were not updated.\n")
    print(
        f"Updated {registry}. Apply this policy to Railway and redeploy to activate it."
    )
    if args.action in {"issue", "rotate"}:
        print(f"Private key file: {args.directory / (args.client + '.key')}")


if __name__ == "__main__":
    main()
