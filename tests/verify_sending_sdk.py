"""Exercise explicit send grants and duplicate protection through real MCP HTTP."""

import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from starlette.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))

from access_control import CURRENT_CLIENT, READ_TOOLS

keys = {"writer": "w" * 43, "reader": "r" * 43, "revoked": "v" * 43}
policy = json.dumps(
    {
        name: {
            "key_sha256": hashlib.sha256(key.encode()).hexdigest(),
            "enabled": name != "revoked",
            "tools": sorted(READ_TOOLS | {"send_message"})
            if name == "writer"
            else sorted(READ_TOOLS),
        }
        for name, key in keys.items()
    }
)

with (
    tempfile.TemporaryDirectory() as root,
    patch.dict(
        os.environ,
        {
            "WHATSAPP_MCP_READ_ONLY": "false",
            "WHATSAPP_MCP_CLIENTS": policy,
            "WHATSAPP_MESSAGES_DB_PATH": str(Path(root) / "messages.db"),
        },
    ),
):
    import main

    assert (
        main.send_message("5511999999999", "Synthetic text", "sdk_request_123")[
            "status"
        ]
        == "rejected"
    )
    transport = Mock(
        return_value=(
            True,
            {"message_id": "sdk_message_123", "archive_status": "saved"},
        )
    )
    with (
        patch.object(main, "whatsapp_send_message", transport),
        TestClient(main.create_http_app(), base_url="http://127.0.0.1:8000") as client,
    ):

        def rpc(method, params, who="writer"):
            return client.post(
                "/mcp",
                headers={
                    "Accept": "application/json, text/event-stream",
                    "X-WhatsApp-MCP-Key": keys[who],
                },
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            )

        init = {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "send-fixture", "version": "1"},
        }
        initialized = rpc("initialize", init)
        assert initialized.status_code == 200
        assert (
            "read-only WhatsApp search service"
            not in initialized.json()["result"]["instructions"]
        )
        tools = rpc("tools/list", {}).json()["result"]["tools"]
        by_name = {tool["name"]: tool for tool in tools}
        assert set(by_name) == READ_TOOLS | {"send_message"}
        assert "idempotency_key" in by_name["send_message"]["inputSchema"]["required"]
        assert by_name["send_message"]["annotations"]["readOnlyHint"] is False
        args = {
            "recipient": "5511999999999",
            "message": "Synthetic text",
            "idempotency_key": "sdk_request_123",
        }
        call = {"name": "send_message", "arguments": args}
        assert rpc("tools/call", call, "reader").status_code == 403
        assert rpc("tools/call", call, "revoked").status_code == 401
        transport.assert_not_called()
        for _ in range(2):
            response = rpc("tools/call", call)
            assert response.status_code == 200, response.text
            result = response.json()["result"]
            assert result["structuredContent"]["result"]["status"] == "sent", result
            assert (
                result["structuredContent"]["result"]["message_id"] == "sdk_message_123"
            )
            assert result["structuredContent"]["result"]["archive_status"] == "saved"
        transport.assert_called_once_with(args["recipient"], args["message"])
        assert CURRENT_CLIENT.get() is None
        transport.side_effect = TimeoutError("synthetic timeout")
        call["arguments"]["idempotency_key"] = "sdk_timeout_123"
        for _ in range(2):
            assert (
                rpc("tools/call", call).json()["result"]["structuredContent"]["result"][
                    "status"
                ]
                == "unknown"
            )
        assert transport.call_count == 2
        call["arguments"]["message"] = "Different text"
        assert (
            rpc("tools/call", call).json()["result"]["structuredContent"]["result"][
                "status"
            ]
            == "conflict"
        )
        with patch.object(main, "READ_ONLY", True):
            call["arguments"]["idempotency_key"] = "readonly_attempt"
            assert (
                rpc("tools/call", call).json()["result"]["structuredContent"]["result"][
                    "status"
                ]
                == "rejected"
            )
        assert transport.call_count == 2

print(
    "Real MCP HTTP: explicit text grants, denied readers/revoked clients, durable retries and read-only guard passed."
)
