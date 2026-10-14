import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "whatsapp-mcp-server"))
import audio


class AudioTests(unittest.TestCase):
    def test_failed_conversion_removes_temporary_output(self):
        paths = []

        def fail(input_file, output_file, bitrate, sample_rate):
            paths.append(Path(output_file))
            self.assertTrue(paths[-1].is_file())
            raise RuntimeError("conversion failed")

        with patch.object(audio, "convert_to_opus_ogg", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "conversion failed"):
                audio.convert_to_opus_ogg_temp("fixture.wav")
        self.assertEqual(len(paths), 1)
        self.assertFalse(paths[0].exists())

    def test_conversion_keeps_command_options_and_output_path(self):
        with tempfile.TemporaryDirectory() as directory:
            input_file = Path(directory) / "voice.wav"
            input_file.write_bytes(b"fixture")
            with patch.object(audio.subprocess, "run") as run:
                result = audio.convert_to_opus_ogg(str(input_file))
            self.assertEqual(result, str(input_file.with_suffix(".ogg")))
            command = run.call_args.args[0]
            self.assertEqual(command[-1], result)
            self.assertEqual(command[command.index("-c:a") + 1], "libopus")
            self.assertEqual(command[command.index("-ar") + 1], "24000")
            self.assertTrue(run.call_args.kwargs["check"])
