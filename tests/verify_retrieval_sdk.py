"""Exercise the agent workflow through authenticated, real MCP HTTP."""

import hashlib
import json
import os
from unittest.mock import patch

from starlette.testclient import TestClient
from test_retrieval import RetrievalTests

fixture = RetrievalTests()
fixture.setUp()
fixture.add_phone_copy()
key = "r" * 43
policy = json.dumps(
    {"fixture": {"key_sha256": hashlib.sha256(key.encode()).hexdigest()}}
)
try:
    with patch.dict(
        os.environ, {"WHATSAPP_MCP_READ_ONLY": "true", "WHATSAPP_MCP_CLIENTS": policy}
    ):
        import main

        with TestClient(
            main.create_http_app(), base_url="http://127.0.0.1:8000"
        ) as client:
            headers = {
                "Accept": "application/json, text/event-stream",
                "X-WhatsApp-MCP-Key": key,
            }

            def rpc(method, params):
                response = client.post(
                    "/mcp",
                    headers=headers,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": method,
                        "params": params,
                    },
                )
                assert response.status_code == 200
                result = response.json()["result"]
                assert not result.get("isError"), result
                return result

            rpc(
                "initialize",
                {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "retrieval-fixture", "version": "1"},
                },
            )
            tools = {tool["name"]: tool for tool in rpc("tools/list", {})["tools"]}
            assert len(tools) == 11
            properties = tools["list_messages"]["inputSchema"]["properties"]
            assert properties["response_format"]["enum"] == ["text", "structured"]
            assert properties["query_mode"]["enum"] == ["substring", "full_text"]
            assert (
                "chat_jid" in tools["get_message_context"]["inputSchema"]["properties"]
            )
            assert (
                "metadata_only" in tools["download_media"]["inputSchema"]["properties"]
            )
            health = {
                "connected": True,
                "observation_scope": "persistent",
                "history_diagnostics": {
                    "status": "finished",
                    "requests_started": 8,
                    "decoded_responses": 2,
                    "request_id": "private protocol id",
                },
                "ingestion": {
                    "status": "degraded",
                    "diagnostics_available": True,
                    "pending_batches": 2,
                    "failed_batches": 1,
                    "restart_count": 3,
                    "payload": "private fixture content",
                },
            }
            import types

            import requests

            with patch.object(
                requests,
                "get",
                return_value=types.SimpleNamespace(
                    status_code=200, json=lambda: health
                ),
            ):
                sync = rpc("tools/call", {"name": "get_sync_status", "arguments": {}})[
                    "structuredContent"
                ]["result"]
            assert sync["connected"] is True
            assert sync["observation_scope"] == "persistent"
            assert sync["ingestion"]["failed_batches"] == 1
            assert "payload" not in sync["ingestion"]
            assert sync["history_diagnostics"]["requests_started"] == 8
            assert "request_id" not in sync["history_diagnostics"]
            search = rpc(
                "tools/call",
                {
                    "name": "list_messages",
                    "arguments": {
                        "query": "joao orcamento",
                        "query_mode": "full_text",
                        "response_format": "structured",
                        "include_context": False,
                    },
                },
            )
            data = search["structuredContent"]["result"]["data"]
            assert len(data["matches"]) == 1
            match = data["matches"][0]
            identity_result = rpc(
                "tools/call",
                {
                    "name": "get_message_identity",
                    "arguments": {
                        "message_id": match["message_id"],
                        "chat_jid": match["chat_jid"],
                    },
                },
            )["structuredContent"]["result"]["data"]
            assert identity_result["source_identifiers_total"] == 2
            assert len(identity_result["source_identifiers"]) == 2
            assert data["coverage"]["stored_messages"] == 3
            assert data["coverage"]["raw_source_rows"] == 4
            context = rpc(
                "tools/call",
                {
                    "name": "get_message_context",
                    "arguments": {
                        "message_id": match["message_id"],
                        "chat_jid": match["chat_jid"],
                    },
                },
            )
            assert (
                context["structuredContent"]["result"]["data"]["message"]["chat_jid"]
                == match["chat_jid"]
            )
            first = rpc(
                "tools/call",
                {
                    "name": "list_messages",
                    "arguments": {
                        "limit": 1,
                        "response_format": "structured",
                        "include_context": False,
                    },
                },
            )["structuredContent"]["result"]["data"]
            second = rpc(
                "tools/call",
                {
                    "name": "list_messages",
                    "arguments": {
                        "limit": 1,
                        "response_format": "structured",
                        "include_context": False,
                        "cursor": first["next_cursor"],
                    },
                },
            )["structuredContent"]["result"]["data"]
            assert (
                first["matches"][0]["message_id"] != second["matches"][0]["message_id"]
            )
    print(
        "Real MCP HTTP: structured full-text search → exact context, schema discovery, cursor continuation and persistent ingestion diagnostics passed."
    )
finally:
    fixture.doCleanups()
