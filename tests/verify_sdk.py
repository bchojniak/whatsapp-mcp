"""Run separately from stub-based regression tests against the real MCP SDK."""

import asyncio
import base64
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))


async def verify():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        db = root / "messages.db"
        with sqlite3.connect(db) as conn:
            conn.executescript(
                "CREATE TABLE messages(id TEXT, chat_jid TEXT, media_type TEXT,filename TEXT,file_length INTEGER);"
                "INSERT INTO messages VALUES('fixture','fixture@lid','image',NULL,NULL);"
            )
        with patch.dict(
            os.environ,
            {
                "WHATSAPP_MCP_READ_ONLY": "true",
                "WHATSAPP_MESSAGES_DB_PATH": str(db),
                "WHATSAPP_MEDIA_ROOT": str(root),
            },
        ):
            import main

            names = {tool.name for tool in await main.mcp.list_tools()}
            assert len(names) == 11, names
            assert {"download_media", "get_sync_status"} <= names
            assert not {"send_message", "send_file", "send_audio_message"} & names
            for name, raw, expected in [
                ("fixture.png", b"fixture image", "image"),
                ("fixture.pdf", b"%PDF-fixture", "resource"),
            ]:
                path = root / name
                path.write_bytes(raw)
                with patch.object(
                    main, "whatsapp_download_media", return_value=(str(path), None)
                ):
                    result = main.download_media("fixture", "fixture@lid")
                assert result.content[1].type == expected
                assert "file_path" not in result.structuredContent
                serialized = result.model_dump_json()
                assert base64.b64encode(raw).decode() in serialized
    print(
        "Real MCP SDK: 11 read tools; all 3 send tools absent; image/resource serialization passed."
    )


asyncio.run(verify())
