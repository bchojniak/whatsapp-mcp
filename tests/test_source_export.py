import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "deploy"))
from export_source import export_source


class SourceExportTests(unittest.TestCase):
    def test_export_excludes_history_untracked_secrets_and_author_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            root = directory / "repo"
            root.mkdir()

            def git(*args):
                return subprocess.check_output(
                    ["git", *args], cwd=root, stderr=subprocess.DEVNULL
                )

            git("init")
            git("config", "user.name", "Synthetic Author")
            git("config", "user.email", "history@example.test")
            (root / "README.md").write_text("private historical text")
            git("add", ".")
            git("commit", "-m", "old content")
            (root / "README.md").write_text("current source")
            git("add", ".")
            git("commit", "-m", "current content")
            (root / ".access").mkdir()
            (root / ".access/client.key").write_text("untracked private fixture")
            output = directory / "source.tar.gz"
            export_source(output, root=root)
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            with tarfile.open(output) as archive:
                self.assertEqual(archive.getnames(), ["whatsapp-mcp/README.md"])
                self.assertEqual(
                    archive.extractfile(archive.getmembers()[0]).read(),
                    b"current source",
                )
                self.assertEqual(archive.pax_headers, {})
                self.assertEqual(archive.getmembers()[0].uname, "")
            with self.assertRaises(FileExistsError):
                export_source(output, root=root)
            with self.assertRaises(ValueError):
                export_source(root / "nested.tar.gz", root=root)
            git("add", "-f", ".access/client.key")
            git("commit", "-m", "synthetic forbidden artifact")
            with self.assertRaises(ValueError):
                export_source(directory / "unsafe.tar.gz", root=root)
            self.assertFalse((directory / "unsafe.tar.gz").exists())
