import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "deploy"))
from manage_access import manage


class ManagementTests(unittest.TestCase):
    def test_issue_rotate_revoke_preserves_other_clients_and_private_permissions(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / ".access"
            path = manage(directory, "issue", "openai-tunnel")
            manage(directory, "issue", "pi-agent", ["list_chats"])
            original = (directory / "pi-agent.key").read_text().strip()
            initial = json.loads(path.read_text())
            self.assertEqual(
                initial["pi-agent"]["key_sha256"],
                hashlib.sha256(original.encode()).hexdigest(),
            )
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual((directory / "pi-agent.key").stat().st_mode & 0o777, 0o600)
            manage(directory, "rotate", "pi-agent")
            rotated = (directory / "pi-agent.key").read_text().strip()
            self.assertNotEqual(original, rotated)
            current = json.loads(path.read_text())
            self.assertEqual(current["openai-tunnel"], initial["openai-tunnel"])
            self.assertEqual(current["pi-agent"]["tools"], ["list_chats"])
            manage(directory, "revoke", "pi-agent")
            self.assertFalse(json.loads(path.read_text())["pi-agent"]["enabled"])
            self.assertFalse((directory / "pi-agent.key").exists())
            with self.assertRaises(RuntimeError):
                manage(directory, "revoke", "openai-tunnel")
            self.assertTrue(json.loads(path.read_text())["openai-tunnel"]["enabled"])

    def test_send_grant_and_revocation_preserve_keys_and_other_grants(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            registry = manage(directory, "issue", "agent", ["list_chats"])
            key = (directory / "agent.key").read_bytes()
            manage(directory, "grant-send", "agent")
            self.assertEqual(
                json.loads(registry.read_text())["agent"]["tools"],
                ["list_chats", "send_message"],
            )
            manage(directory, "revoke-send", "agent")
            self.assertEqual(
                json.loads(registry.read_text())["agent"]["tools"], ["list_chats"]
            )
            self.assertEqual((directory / "agent.key").read_bytes(), key)

    def test_invalid_action_does_not_replace_key(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            registry = manage(directory, "issue", "pi-agent")
            original = registry.read_text()
            for client in ["../escape", "UPPER", "pi-agent"]:
                with self.assertRaises(ValueError):
                    manage(directory, "issue", client)
            self.assertEqual(registry.read_text(), original)
