import hashlib
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
from access_control import (
    KEY_HEADER,
    LEGACY_READ_TOOLS,
    MAX_REQUEST_BYTES,
    READ_TOOLS,
    AccessPolicy,
    ClientKeyMiddleware,
    check_tunnel_configuration,
    tunnel_enabled,
)

KEY = "a" * 43
OTHER = "b" * 43
REVOKED = "c" * 43


def registry():
    return json.dumps(
        {
            name: {"key_sha256": hashlib.sha256(key.encode()).hexdigest(), **extra}
            for name, key, extra in [
                ("openai-tunnel", KEY, {}),
                ("pi-agent", OTHER, {"tools": ["list_chats"]}),
                ("revoked", REVOKED, {"enabled": False}),
            ]
        }
    )


class AccessTests(unittest.IsolatedAsyncioTestCase):
    async def call(
        self,
        key=None,
        method="POST",
        body=None,
        path="/mcp",
        extra_headers=(),
        chunks=None,
    ):
        self.downstream = []

        async def downstream(scope, receive, send):
            self.downstream.append(scope)
            if method == "POST":
                self.forwarded = await receive()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"passed"})

        messages = chunks or [
            {"type": "http.request", "body": body or b'{"method":"tools/list"}'}
        ]

        async def receive():
            return messages.pop(0) if messages else {"type": "http.disconnect"}

        sent = []

        async def send(message):
            sent.append(message)

        headers = list(extra_headers)
        if key is not None:
            headers.append((KEY_HEADER.lower().encode(), key.encode()))
        scope = {
            "type": "http",
            "method": method,
            "path": path,
            "query_string": ("key=" + KEY).encode(),
            "headers": headers,
        }
        await ClientKeyMiddleware(downstream, AccessPolicy.from_json(registry()))(
            scope, receive, send
        )
        return sent[0]["status"], sent[-1].get("body")

    async def test_missing_wrong_revoked_and_query_keys_never_reach_tools(self):
        for method in ["GET", "POST", "DELETE"]:
            for key in [None, "invalid", REVOKED]:
                self.assertEqual((await self.call(key, method))[0], 401)
                self.assertEqual(self.downstream, [])

    async def test_duplicate_header_rejected(self):
        self.assertEqual(
            (await self.call(KEY, extra_headers=[(KEY_HEADER.encode(), KEY.encode())]))[
                0
            ],
            401,
        )
        self.assertEqual(self.downstream, [])

    async def test_valid_clients_and_secret_removed_from_scope(self):
        for key in [KEY, OTHER]:
            self.assertEqual((await self.call(key))[0], 200)
            self.assertEqual(self.downstream[0]["headers"], [])

    async def test_tool_allowlist_enforced_before_execution(self):
        for tool, status in [
            ("list_chats", 200),
            ("list_messages", 403),
            ("send_message", 403),
        ]:
            payload = json.dumps(
                {"method": "tools/call", "params": {"name": tool}}
            ).encode()
            self.assertEqual((await self.call(OTHER, body=payload))[0], status)
            if status == 403:
                self.assertEqual(self.downstream, [])

    async def test_malformed_batch_duplicate_and_oversized_requests_rejected(self):
        for payload, status in [
            (b"{", 400),
            (b"[]", 400),
            (b'{"method":"tools/list","method":"tools/call"}', 400),
            (b"x" * (MAX_REQUEST_BYTES + 1), 413),
        ]:
            self.assertEqual((await self.call(KEY, body=payload))[0], status)
            self.assertEqual(self.downstream, [])

    async def test_chunked_body_replayed(self):
        payload = b'{"method":"tools/list"}'
        chunks = [
            {"type": "http.request", "body": payload[:10], "more_body": True},
            {"type": "http.request", "body": payload[10:]},
        ]
        self.assertEqual((await self.call(KEY, chunks=chunks))[0], 200)
        self.assertEqual(self.forwarded["body"], payload)

    async def test_health_reveals_no_whatsapp_information(self):
        self.assertEqual(
            await self.call(path="/healthz", method="GET"), (200, b'{"ok":true}')
        )
        self.assertEqual(self.downstream, [])
        self.assertEqual((await self.call(path="/healthz", method="POST"))[0], 401)

    async def test_audit_identity_without_secrets_or_arguments(self):
        private = "private message, contact, query and media path"
        body = json.dumps(
            {
                "method": "tools/call",
                "params": {"name": "list_chats", "arguments": {"query": private}},
            }
        ).encode()
        with self.assertLogs("whatsapp_mcp.access", level="INFO") as logs:
            await self.call(OTHER, body=body)
        rendered = " ".join(logs.output)
        self.assertIn("client=pi-agent", rendered)
        self.assertIn("tool=list_chats", rendered)
        self.assertNotIn(OTHER, rendered)
        self.assertNotIn(private, rendered)


class PolicyTests(unittest.TestCase):
    def test_missing_invalid_and_duplicate_policy_fail_closed(self):
        digest = hashlib.sha256(KEY.encode()).hexdigest()
        for raw in [
            "",
            "null",
            "{}",
            '{"a":{"key_sha256":"secret"}}',
            json.dumps({"a": {"key_sha256": digest, "tools": ["send_file"]}}),
            json.dumps({"a": {"key_sha256": digest, "enabled": False}}),
            '{"a":{"key_sha256":"' + digest + '"},"a":{}}',
            json.dumps({"a": {"key_sha256": digest}, "b": {"key_sha256": digest}}),
        ]:
            with self.assertRaises(RuntimeError) as error:
                AccessPolicy.from_json(raw)
            self.assertNotIn("secret", str(error.exception))

    def test_explicit_legacy_tunnel_allowlist_keeps_its_permissions(self):
        raw = json.dumps(
            {
                "legacy": {
                    "key_sha256": hashlib.sha256(KEY.encode()).hexdigest(),
                    "tools": sorted(LEGACY_READ_TOOLS),
                }
            }
        )
        with patch.dict(
            "os.environ", {"WHATSAPP_MCP_CLIENTS": raw, "WHATSAPP_MCP_TUNNEL_KEY": KEY}
        ):
            check_tunnel_configuration()
        client = AccessPolicy.from_json(raw).authenticate(KEY)
        self.assertNotIn("get_message_identity", client.tools)
        self.assertEqual(client.tools, LEGACY_READ_TOOLS)

    def test_write_permission_is_explicit_and_tunnel_accepts_it(self):
        raw = json.dumps(
            {
                "writer": {
                    "key_sha256": hashlib.sha256(KEY.encode()).hexdigest(),
                    "tools": sorted(READ_TOOLS | {"send_message"}),
                }
            }
        )
        client = AccessPolicy.from_json(raw).tunnel_client(KEY)
        self.assertIn("send_message", client.tools)
        self.assertNotIn(
            "send_message", AccessPolicy.from_json(registry()).authenticate(KEY).tools
        )

    def test_tunnel_requires_enabled_full_read_client(self):
        for key, valid in [(KEY, True), (OTHER, False), (REVOKED, False), ("", False)]:
            with patch.dict(
                "os.environ",
                {
                    "WHATSAPP_MCP_CLIENTS": registry(),
                    "WHATSAPP_MCP_TUNNEL_KEY": key,
                    "WHATSAPP_MCP_TUNNEL_ENABLED": "true",
                },
            ):
                if valid:
                    check_tunnel_configuration()
                else:
                    with self.assertRaises(RuntimeError):
                        check_tunnel_configuration()

    def test_no_tunnel_still_requires_a_valid_application_policy(self):
        with patch.dict("os.environ", {"WHATSAPP_MCP_CLIENTS": registry()}, clear=True):
            self.assertFalse(check_tunnel_configuration())
        with patch.dict(
            "os.environ", {"WHATSAPP_MCP_TUNNEL_ENABLED": "false"}, clear=True
        ):
            with self.assertRaises(RuntimeError):
                check_tunnel_configuration()

    def test_partial_tunnel_config_and_invalid_modes_fail_closed(self):
        for name in (
            "CONTROL_PLANE_API_KEY",
            "CONTROL_PLANE_TUNNEL_ID",
            "OPENAI_TUNNEL_ID",
        ):
            with patch.dict(
                "os.environ",
                {"WHATSAPP_MCP_CLIENTS": registry(), name: "synthetic"},
                clear=True,
            ):
                self.assertTrue(tunnel_enabled())
                with self.assertRaises(RuntimeError):
                    check_tunnel_configuration()
        with patch.dict(
            "os.environ", {"WHATSAPP_MCP_TUNNEL_ENABLED": "typo"}, clear=True
        ):
            with self.assertRaises(RuntimeError):
                tunnel_enabled()

    def test_explicit_disable_ignores_unused_tunnel_credentials(self):
        with patch.dict(
            "os.environ",
            {
                "WHATSAPP_MCP_CLIENTS": registry(),
                "WHATSAPP_MCP_TUNNEL_ENABLED": "false",
                "CONTROL_PLANE_API_KEY": "unused",
            },
            clear=True,
        ):
            self.assertFalse(check_tunnel_configuration())


if __name__ == "__main__":
    unittest.main()
