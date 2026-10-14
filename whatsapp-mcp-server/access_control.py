"""One authenticated HTTP access boundary for local, Tailscale and tunnel clients."""

import argparse
import hashlib
import hmac
import json
import logging
import os
import re
import time
from contextvars import ContextVar
from dataclasses import dataclass

KEY_HEADER = "X-WhatsApp-MCP-Key"
_KEY_HEADER_BYTES = KEY_HEADER.lower().encode()
READ_TOOLS = frozenset(
    {
        "search_contacts",
        "list_messages",
        "list_chats",
        "get_chat",
        "get_direct_chat_by_contact",
        "get_contact_chats",
        "get_last_interaction",
        "get_message_context",
        "get_message_identity",
        "download_media",
        "get_sync_status",
    }
)
# Support explicit ten-tool policies without granting get_message_identity.
LEGACY_READ_TOOLS = READ_TOOLS - {"get_message_identity"}
WRITE_TOOLS = frozenset({"send_message"})
ALL_TOOLS = READ_TOOLS | WRITE_TOOLS
CURRENT_CLIENT = ContextVar("whatsapp_mcp_client", default=None)
MAX_REQUEST_BYTES = 1024 * 1024
_CLIENT_ID = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_KEY = re.compile(r"[A-Za-z0-9_-]{43,128}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_METHODS = frozenset(
    {
        "initialize",
        "ping",
        "tools/list",
        "tools/call",
        "notifications/initialized",
        "notifications/cancelled",
        "resources/list",
        "resources/templates/list",
        "resources/read",
        "prompts/list",
        "prompts/get",
    }
)
audit = logging.getLogger("whatsapp_mcp.access")


@dataclass(frozen=True)
class Client:
    name: str
    digest: bytes
    enabled: bool
    tools: frozenset[str]


def _unique_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("Duplicate access-policy field")
        result[name] = value
    return result


class AccessPolicy:
    def __init__(self, clients):
        self.clients = tuple(clients)

    @classmethod
    def from_env(cls):
        return cls.from_json(os.environ.get("WHATSAPP_MCP_CLIENTS", ""))

    @classmethod
    def from_json(cls, raw):
        # Configuration errors must never reveal the input or fall back to no auth.
        try:
            config = json.loads(raw, object_pairs_hook=_unique_object)
            if not isinstance(config, dict) or not config:
                raise ValueError()
            clients = []
            digests = set()
            for name, entry in config.items():
                if not _CLIENT_ID.fullmatch(name) or not isinstance(entry, dict):
                    raise ValueError()
                if set(entry) - {"key_sha256", "enabled", "tools"}:
                    raise ValueError()
                digest = entry.get("key_sha256")
                enabled = entry.get("enabled", True)
                tools = entry.get("tools", sorted(READ_TOOLS))
                if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
                    raise ValueError()
                if type(enabled) is not bool or not isinstance(tools, list):
                    raise ValueError()
                if not all(
                    isinstance(tool, str) and tool in ALL_TOOLS for tool in tools
                ):
                    raise ValueError()
                if digest in digests:
                    raise ValueError()
                digests.add(digest)
                clients.append(
                    Client(name, bytes.fromhex(digest), enabled, frozenset(tools))
                )
            if not any(client.enabled for client in clients):
                raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise RuntimeError(
                "WHATSAPP_MCP_CLIENTS must contain a valid, nonempty client policy"
            ) from None
        return cls(clients)

    def authenticate(self, key):
        if not isinstance(key, str) or not _KEY.fullmatch(key):
            return None
        digest = hashlib.sha256(key.encode("ascii")).digest()
        matched = None
        # Compare fixed-size digests; identities are derived from keys, never headers.
        for client in self.clients:
            if hmac.compare_digest(digest, client.digest) and client.enabled:
                matched = client
        return matched

    def tunnel_client(self, key):
        """Require a complete supported read grant; write grants are explicit."""
        client = self.authenticate(key)
        if client is None or not LEGACY_READ_TOOLS <= client.tools:
            raise RuntimeError(
                "WHATSAPP_MCP_TUNNEL_KEY must match an enabled client with all read tools"
            )
        return client


def tunnel_enabled():
    value = os.environ.get("WHATSAPP_MCP_TUNNEL_ENABLED", "").strip().lower()
    if value in {"true", "1", "yes", "on"}:
        return True
    if value in {"false", "0", "no", "off"}:
        return False
    if value:
        raise RuntimeError("WHATSAPP_MCP_TUNNEL_ENABLED must be true or false")
    # Partial tunnel configuration must fail validation, not silently disable it.
    return any(
        os.environ.get(name)
        for name in (
            "CONTROL_PLANE_API_KEY",
            "CONTROL_PLANE_TUNNEL_ID",
            "OPENAI_TUNNEL_ID",
            "WHATSAPP_MCP_TUNNEL_KEY",
        )
    )


def check_tunnel_configuration():
    policy = AccessPolicy.from_env()
    enabled = tunnel_enabled()
    if enabled:
        policy.tunnel_client(os.environ.get("WHATSAPP_MCP_TUNNEL_KEY", ""))
    return enabled


async def _error(send, status):
    body = json.dumps(
        {"error": "Access denied" if status in {401, 403} else "Invalid request"}
    ).encode()
    headers = [(b"content-type", b"application/json"), (b"cache-control", b"no-store")]
    if status == 401:
        headers.append((b"www-authenticate", b'ApiKey realm="whatsapp-mcp"'))
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})


async def _read_body(receive, send):
    """Return a bounded body, or None when the request has already ended."""
    body = bytearray()
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return None
        if message["type"] != "http.request":
            await _error(send, 400)
            return None
        body.extend(message.get("body", b""))
        if len(body) > MAX_REQUEST_BYTES:
            await _error(send, 413)
            return None
        if not message.get("more_body", False):
            return bytes(body)


class ClientKeyMiddleware:
    """Reject unauthorized requests before they reach MCP or any data tool."""

    def __init__(self, app, policy):
        self.app = app
        self.policy = policy

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            return await self.app(scope, receive, send)
        if scope["type"] != "http":
            if scope["type"] == "websocket":
                await send({"type": "websocket.close", "code": 1008})
            return
        if scope["path"] == "/healthz" and scope["method"] == "GET":
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"cache-control", b"no-store"),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": b'{"ok":true}'})
            return
        headers = [
            value
            for name, value in scope.get("headers", [])
            if name.lower() == _KEY_HEADER_BYTES
        ]
        key = headers[0].decode("latin-1") if len(headers) == 1 else None
        client = self.policy.authenticate(key)
        if client is None:
            audit.info("client=unauthenticated status=401")
            return await _error(send, 401)
        method = "transport"
        tool = "-"
        buffered = []
        if scope["method"] == "POST":
            body = await _read_body(receive, send)
            if body is None:
                return
            buffered.append({"type": "http.request", "body": body, "more_body": False})
            try:
                request = json.loads(body, object_pairs_hook=_unique_object)
                if not isinstance(request, dict) or not isinstance(
                    request.get("method"), str
                ):
                    raise ValueError()
                rpc_method = request["method"]
                method = rpc_method if rpc_method in _METHODS else "other"
                if rpc_method == "tools/call":
                    params = request.get("params")
                    tool_name = params.get("name") if isinstance(params, dict) else None
                    if not isinstance(tool_name, str) or tool_name not in client.tools:
                        audit.info(
                            "client=%s method=tools/call status=403", client.name
                        )
                        return await _error(send, 403)
                    tool = tool_name
            except (ValueError, TypeError):
                return await _error(send, 400)

        async def replay():
            if buffered:
                return buffered.pop(0)
            return await receive()

        started = time.monotonic()

        async def audited_send(message):
            if message["type"] == "http.response.start":
                audit.info(
                    "client=%s method=%s tool=%s status=%s elapsed_ms=%d",
                    client.name,
                    method,
                    tool,
                    message["status"],
                    int((time.monotonic() - started) * 1000),
                )
            await send(message)

        # Credentials are checked only here; they are not needed in the tool layer.
        clean_scope = dict(scope)
        clean_scope["headers"] = [
            (name, value)
            for name, value in scope.get("headers", [])
            if name.lower() != _KEY_HEADER_BYTES
        ]
        token = CURRENT_CLIENT.set(client)
        try:
            return await self.app(clean_scope, replay, audited_send)
        finally:
            CURRENT_CLIENT.reset(token)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--print-tunnel-enabled", action="store_true")
    args = parser.parse_args()
    enabled = check_tunnel_configuration()
    if args.print_tunnel_enabled:
        print("true" if enabled else "false")
