import importlib
import os
import sys
import types
import unittest
from unittest.mock import patch

import test_regressions


class FakeMCP:
    def __init__(self, *args, **kwargs):
        self.tools = {}

    def tool(self, **kwargs):
        def register(fn):
            self.tools[fn.__name__] = (fn, kwargs)
            return fn

        return register


def load_server():
    # Exercise module registration and tool code without requiring network installs.
    modules = {
        name: types.ModuleType(name)
        for name in [
            "mcp",
            "mcp.server",
            "mcp.server.fastmcp",
            "mcp.server.transport_security",
            "mcp.types",
        ]
    }
    modules["mcp.server.fastmcp"].FastMCP = FakeMCP
    modules[
        "mcp.server.transport_security"
    ].TransportSecuritySettings = types.SimpleNamespace
    for name in [
        "ToolAnnotations",
        "CallToolResult",
        "TextContent",
        "ImageContent",
        "EmbeddedResource",
        "BlobResourceContents",
    ]:
        setattr(modules["mcp.types"], name, types.SimpleNamespace)
    with (
        patch.dict(sys.modules, modules),
        patch.dict(os.environ, {"WHATSAPP_MCP_READ_ONLY": "true"}),
    ):
        sys.modules.pop("main", None)
        return importlib.import_module("main")


class MediaTests(unittest.TestCase):
    def setUp(self):
        test_regressions.RegressionTests.setUp(self)
        with test_regressions.sqlite3.connect(self.root / "messages.db") as conn:
            conn.execute("ALTER TABLE messages ADD COLUMN filename TEXT")
            conn.execute("ALTER TABLE messages ADD COLUMN file_length INTEGER")

    def test_readonly_registration_includes_media_and_excludes_sends(self):
        main = load_server()
        self.assertIn("download_media", main.mcp.tools)
        self.assertIn("get_sync_status", main.mcp.tools)
        self.assertNotIn("send_message", main.mcp.tools)

    def test_image_is_returned_as_mcp_image_block(self):
        main = load_server()
        image = self.root / "image.png"
        image.write_bytes(b"fake image bytes")
        with (
            patch.dict(os.environ, {"WHATSAPP_MEDIA_ROOT": str(self.root)}),
            patch.object(
                main, "whatsapp_download_media", return_value=(str(image), None)
            ),
        ):
            result = main.download_media("a", "999@lid")
        self.assertEqual(result.content[1].type, "image")
        self.assertEqual(result.content[1].mimeType, "image/png")
        self.assertNotIn("file_path", result.structuredContent)

    def test_document_is_returned_as_embedded_resource(self):
        main = load_server()
        doc = self.root / "document.pdf"
        doc.write_bytes(b"%PDF-test")
        with (
            patch.dict(os.environ, {"WHATSAPP_MEDIA_ROOT": str(self.root)}),
            patch.object(
                main, "whatsapp_download_media", return_value=(str(doc), None)
            ),
        ):
            result = main.download_media("a", "999@lid")
        self.assertEqual(result.content[1].type, "resource")
        self.assertEqual(result.content[1].resource.mimeType, "application/pdf")

    def test_wrong_chat_is_rejected_before_bridge_request(self):
        main = load_server()
        with patch.object(main, "whatsapp_download_media") as download:
            with self.assertRaises(ValueError):
                main.download_media("a", "group@g.us")
            download.assert_not_called()

    def test_path_escape_is_rejected(self):
        main = load_server()
        with (
            patch.dict(os.environ, {"WHATSAPP_MEDIA_ROOT": str(self.root)}),
            patch.object(
                main, "whatsapp_download_media", return_value=("/etc/passwd", None)
            ),
        ):
            with self.assertRaises(RuntimeError):
                main.download_media("a", "999@lid")

    def test_large_media_is_rejected(self):
        main = load_server()
        large = self.root / "large.bin"
        with large.open("wb") as file:
            file.truncate(8 * 1024 * 1024 + 1)
        with (
            patch.dict(os.environ, {"WHATSAPP_MEDIA_ROOT": str(self.root)}),
            patch.object(
                main, "whatsapp_download_media", return_value=(str(large), None)
            ),
        ):
            with self.assertRaises(ValueError):
                main.download_media("a", "999@lid")

    def test_metadata_does_not_download_or_expose_paths(self):
        main = load_server()
        with patch.object(main, "whatsapp_download_media") as download:
            result = main.download_media("a", "999@lid", metadata_only=True)
        download.assert_not_called()
        self.assertEqual(result.structuredContent["media_type"], "image")
        self.assertNotIn("path", result.structuredContent)

    def test_known_oversized_media_is_rejected_before_download(self):
        main = load_server()
        with test_regressions.sqlite3.connect(self.root / "messages.db") as conn:
            conn.execute(
                "UPDATE messages SET file_length=? WHERE id='a'", (8 * 1024 * 1024 + 1,)
            )
        with patch.object(main, "whatsapp_download_media") as download:
            with self.assertRaisesRegex(ValueError, "metadata_only"):
                main.download_media("a", "999@lid")
        download.assert_not_called()
