"""Separate process: exercise authentication and permissions with the real MCP SDK."""

import hashlib
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

from starlette.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
from access_control import KEY_HEADER

key = "a" * 43
restricted = "b" * 43
registry = json.dumps(
    {
        "openai-tunnel": {"key_sha256": hashlib.sha256(key.encode()).hexdigest()},
        "pi-agent": {
            "key_sha256": hashlib.sha256(restricted.encode()).hexdigest(),
            "tools": ["list_chats"],
        },
    }
)
headers = {"Accept": "application/json, text/event-stream", KEY_HEADER: key}
with patch.dict(
    os.environ, {"WHATSAPP_MCP_CLIENTS": registry, "WHATSAPP_MCP_READ_ONLY": "true"}
):
    import main

    with TestClient(main.create_http_app(), base_url="http://127.0.0.1:8000") as client:

        def rpc(method, params=None, auth=headers):
            payload = {"jsonrpc": "2.0", "id": 1, "method": method}
            if params is not None:
                payload["params"] = params
            return client.post("/mcp", headers=auth, json=payload)

        assert rpc("tools/list", auth={"Accept": headers["Accept"]}).status_code == 401
        initialized = rpc(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "fixture", "version": "1"},
            },
        )
        assert initialized.status_code == 200, initialized.text
        response = rpc("tools/list")
        assert response.status_code == 200, response.text
        names = {tool["name"] for tool in response.json()["result"]["tools"]}
        assert len(names) == 11 and "download_media" in names
        assert not {"send_message", "send_file", "send_audio_message"} & names
        with patch.object(
            main, "whatsapp_list_chats", return_value=[{"fixture": True}]
        ) as read:
            allowed = rpc(
                "tools/call",
                {"name": "list_chats", "arguments": {}},
                {**headers, KEY_HEADER: restricted},
            )
            assert allowed.status_code == 200, allowed.text
            assert "fixture" in str(allowed.json()), allowed.text
            assert read.call_count == 1
            denied = rpc(
                "tools/call",
                {"name": "list_messages", "arguments": {}},
                {**headers, KEY_HEADER: restricted},
            )
            assert denied.status_code == 403
            assert read.call_count == 1
        revoked = json.loads(registry)
        revoked["pi-agent"]["enabled"] = False
        with patch.dict(os.environ, {"WHATSAPP_MCP_CLIENTS": json.dumps(revoked)}):
            restarted = TestClient(
                main.create_http_app(), base_url="http://127.0.0.1:8000"
            )
            assert (
                restarted.post(
                    "/mcp",
                    headers={**headers, KEY_HEADER: restricted},
                    json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                ).status_code
                == 401
            )
print(
    "Real MCP HTTP: authenticated discovery/read, scoped rejection and revocation passed."
)
