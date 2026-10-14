#!/usr/bin/env python3
"""Prepare private worker configuration; never upload or print secrets."""

import argparse
import json
import os
import re
from pathlib import Path

from manage_access import READ_TOOLS, AccessPolicy, manage, private_write


def prepare(
    directory, tunnel_id, api_key, days, allow_send_text=False, *, no_tunnel=False
):
    if no_tunnel and (tunnel_id or api_key):
        raise ValueError("Tunnel credentials cannot be combined with no-tunnel mode")
    if not no_tunnel and not re.fullmatch(
        r"tunnel_[a-zA-Z0-9_-]{8,100}", tunnel_id or ""
    ):
        raise ValueError("Use the tunnel ID from your own OpenAI account")
    if type(days) is not int or not 0 <= days <= 3650:
        raise ValueError(
            "History days must be from 0 to 3650; zero means all available history"
        )
    api_key = (api_key or "").strip()
    if not no_tunnel and (
        len(api_key) < 32 or "$" in api_key or any(char.isspace() for char in api_key)
    ):
        raise ValueError("Supply a runtime OpenAI credential through a private file")
    client_name = "local-client" if no_tunnel else "openai-tunnel"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    target = directory / "railway-variables.json"
    if target.exists():
        existing = json.loads(target.read_text())
        expected = {
            "WHATSAPP_HISTORY_BACKFILL_DAYS": str(days),
            "WHATSAPP_MCP_READ_ONLY": "false" if allow_send_text else "true",
        }
        if no_tunnel:
            expected["WHATSAPP_MCP_TUNNEL_ENABLED"] = "false"
            if any(
                existing.get(name)
                for name in (
                    "CONTROL_PLANE_TUNNEL_ID",
                    "CONTROL_PLANE_API_KEY",
                    "WHATSAPP_MCP_TUNNEL_KEY",
                )
            ):
                raise ValueError("Existing export has tunnel credentials")
        else:
            expected.update(
                CONTROL_PLANE_TUNNEL_ID=tunnel_id, CONTROL_PLANE_API_KEY=api_key
            )
            if existing.get("WHATSAPP_MCP_TUNNEL_ENABLED") == "false":
                raise ValueError("Existing export disables the tunnel")
        if any(existing.get(name) != value for name, value in expected.items()):
            raise ValueError(
                "Prepared inputs differ; existing credentials were not replaced"
            )
        policy = AccessPolicy.from_json(existing.get("WHATSAPP_MCP_CLIENTS", ""))
        if no_tunnel:
            client = policy.authenticate(
                (directory / f"{client_name}.key").read_text().strip()
            )
            if client is None:
                raise ValueError("Local key does not match the prepared policy")
        else:
            client = policy.tunnel_client(existing.get("WHATSAPP_MCP_TUNNEL_KEY", ""))
        if allow_send_text and "send_message" not in client.tools:
            raise ValueError("Text sending requires an explicit client grant")
        target.chmod(0o600)
        write_worker_env(directory, existing)
        return target
    registry = directory / "clients.json"
    key_path = directory / f"{client_name}.key"
    if not registry.exists() and not key_path.exists():
        tools = sorted(READ_TOOLS | {"send_message"}) if allow_send_text else None
        manage(directory, "issue", client_name, tools)
    policy = AccessPolicy.from_json(registry.read_text())
    key = key_path.read_text().strip()
    client = policy.authenticate(key) if no_tunnel else policy.tunnel_client(key)
    if client is None:
        raise ValueError("Client key does not match the policy")
    if allow_send_text and "send_message" not in client.tools:
        raise ValueError(
            "Grant send_message to this client before preparing write access"
        )
    run_id = "initial-all" if days == 0 else f"initial-recent-{days}d"
    variables = {
        "WHATSAPP_MCP_CLIENTS": registry.read_text().strip(),
        "WHATSAPP_MCP_READ_ONLY": "false" if allow_send_text else "true",
        "WHATSAPP_ALLOW_LOG_QR": "false",
        "WHATSAPP_ALLOW_FRESH_STORE": "true",
        "WHATSAPP_PAIRING_MODE": "true",
        "WHATSAPP_BRIDGE_START_TIMEOUT": "900",
        "WHATSAPP_HISTORY_BACKFILL_DAYS": str(days),
        "WHATSAPP_HISTORY_BACKFILL_RUN_ID": run_id,
        "WHATSAPP_HISTORY_BACKFILL_CONTINUOUS": "true",
        "WHATSAPP_HISTORY_BACKFILL_START_DELAY_SECONDS": "90",
        "WHATSAPP_HISTORY_INITIAL_SYNC_FULL": "true",
        "WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS": "600",
    }
    variables["WHATSAPP_MCP_TUNNEL_ENABLED"] = "false" if no_tunnel else "true"
    if not no_tunnel:
        variables.update(
            CONTROL_PLANE_TUNNEL_ID=tunnel_id,
            CONTROL_PLANE_API_KEY=api_key,
            WHATSAPP_MCP_TUNNEL_KEY=key,
        )
    private_write(target, json.dumps(variables, indent=2) + "\n")
    write_worker_env(directory, variables)
    return target


def write_worker_env(directory, variables):
    # Values are generated identifiers, JSON, and keys with no newlines or dollars.
    if any(
        "\n" in str(value) or "\r" in str(value) or "$" in str(value)
        for value in variables.values()
    ):
        raise ValueError("Worker env values cannot contain interpolation or newlines")
    private_write(
        directory / "worker.env",
        "".join(f"{name}={value}\n" for name, value in variables.items()),
    )


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tunnel-id")
    parser.add_argument(
        "--no-tunnel",
        action="store_true",
        help="Prepare a local/Tailscale client without OpenAI credentials",
    )
    parser.add_argument("--api-key-file", type=Path)
    parser.add_argument(
        "--history-days",
        type=int,
        required=True,
        help="Days to recover; 0 requests all available history",
    )
    parser.add_argument(
        "--allow-send-text",
        action="store_true",
        help="Explicitly enable and grant text sending",
    )
    parser.add_argument(
        "--directory",
        type=Path,
        default=Path(__file__).resolve().parents[1] / ".access",
    )
    args = parser.parse_args()
    try:
        api_key = None
        if args.no_tunnel:
            if args.api_key_file is not None or args.tunnel_id is not None:
                raise ValueError("Choose one connection mode")
        else:
            if args.api_key_file is None or args.tunnel_id is None:
                raise ValueError("Tunnel ID and API key file are required")
            if args.api_key_file.stat().st_mode & 0o077:
                raise ValueError("API key file must be private (chmod 600)")
            api_key = args.api_key_file.read_text()
        target = prepare(
            args.directory,
            args.tunnel_id,
            api_key,
            args.history_days,
            args.allow_send_text,
            no_tunnel=args.no_tunnel,
        )
    except (OSError, RuntimeError, ValueError, KeyError):
        parser.exit(
            1,
            "Installation inputs are invalid or differ from the prepared file; existing credentials were not replaced.\n",
        )
    print(f"Private variables prepared: {target}")
    print(
        f"Worker env prepared: {target.parent / 'worker.env'}. Import privately; do not print or commit these files."
    )


if __name__ == "__main__":
    main()
