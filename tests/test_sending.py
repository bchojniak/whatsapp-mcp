"""Keep optional send tools' payloads, conversion and errors stable after cleanup."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
import whatsapp as wa


class SendingTests(unittest.TestCase):
    def test_text_transport_retains_only_bounded_reference_fields(self):
        response = Mock(status_code=200)
        response.json.return_value = {
            "success": True,
            "message_id": "fixture_123",
            "archive_status": "saved",
            "message": "private reply",
            "chat_jid": "private",
        }
        with patch.object(wa.requests, "post", return_value=response):
            self.assertEqual(
                wa.send_text_message("12345", "text"),
                (True, {"message_id": "fixture_123", "archive_status": "saved"}),
            )
        response.status_code = 500
        with patch.object(wa.requests, "post", return_value=response):
            self.assertEqual(wa.send_text_message("12345", "text"), (False, {}))

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.file = str(Path(self.directory.name) / "voice.ogg")
        Path(self.file).write_bytes(b"synthetic media")
        self.calls = [
            (
                wa.send_message,
                ("recipient", "message"),
                {"recipient": "recipient", "message": "message"},
            ),
            (
                wa.send_file,
                ("recipient", self.file),
                {"recipient": "recipient", "media_path": self.file},
            ),
            (
                wa.send_audio_message,
                ("recipient", self.file),
                {"recipient": "recipient", "media_path": self.file},
            ),
        ]

    def test_each_tool_keeps_payload_timeout_and_response_contract(self):
        for send, args, payload in self.calls:
            response = Mock(status_code=200)
            response.json.return_value = {"success": True, "message": "sent"}
            with (
                self.subTest(tool=send.__name__),
                patch.object(wa.requests, "post", return_value=response) as post,
            ):
                self.assertEqual(send(*args), (True, "sent"))
                self.assertEqual(
                    post.call_args.kwargs, {"json": payload, "timeout": 30}
                )

    def test_transport_http_and_json_errors_keep_their_messages(self):
        for send, args, _ in self.calls:
            with self.subTest(tool=send.__name__):
                with patch.object(
                    wa.requests,
                    "post",
                    side_effect=wa.requests.RequestException("offline"),
                ):
                    self.assertEqual(send(*args), (False, "Request error: offline"))
                response = Mock(status_code=500, text="bridge failed")
                with patch.object(wa.requests, "post", return_value=response):
                    self.assertEqual(
                        send(*args), (False, "Error: HTTP 500 - bridge failed")
                    )
                response = Mock(status_code=200, text="invalid")
                response.json.side_effect = json.JSONDecodeError("bad", "invalid", 0)
                with patch.object(wa.requests, "post", return_value=response):
                    self.assertEqual(
                        send(*args), (False, "Error parsing response: invalid")
                    )

    def test_voice_conversion_is_only_used_when_needed(self):
        source = str(Path(self.directory.name) / "voice.wav")
        Path(source).write_bytes(b"synthetic audio")
        response = Mock(status_code=200)
        response.json.return_value = {"success": True, "message": "sent"}
        with (
            patch.object(
                wa.audio, "convert_to_opus_ogg_temp", return_value=self.file
            ) as convert,
            patch.object(wa.requests, "post", return_value=response) as post,
        ):
            wa.send_file("recipient", source)
            convert.assert_not_called()
            wa.send_audio_message("recipient", source)
            convert.assert_called_once_with(source)
            self.assertEqual(post.call_args.kwargs["json"]["media_path"], self.file)
        with (
            patch.object(
                wa.audio, "convert_to_opus_ogg_temp", side_effect=RuntimeError("failed")
            ),
            patch.object(wa.requests, "post") as post,
        ):
            self.assertIn(
                "You likely need to install ffmpeg",
                wa.send_audio_message("recipient", source)[1],
            )
            post.assert_not_called()

    def test_missing_inputs_never_call_the_bridge(self):
        for send in (wa.send_message, wa.send_file, wa.send_audio_message):
            with patch.object(wa.requests, "post") as post:
                self.assertEqual(
                    send("", "unused"), (False, "Recipient must be provided")
                )
                post.assert_not_called()
        for send in (wa.send_file, wa.send_audio_message):
            self.assertEqual(
                send("recipient", ""), (False, "Media path must be provided")
            )
            self.assertEqual(
                send("recipient", "/missing/file"),
                (False, "Media file not found: /missing/file"),
            )


if __name__ == "__main__":
    unittest.main()
