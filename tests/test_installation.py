"""Offline onboarding checks use synthetic keys and mock Railway transport."""

import http.client
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "deploy"))
sys.path.insert(0, str(ROOT / "whatsapp-mcp-server"))
from archive_status import requested_backfill_window
from manage_access import manage
from pair_account import (
    docker_command,
    make_handler,
    railway_command,
    remote_read,
    ssh_command,
)
from prepare_install import prepare


class InstallationTests(unittest.TestCase):
    def test_existing_valid_tunnel_identity_is_not_renamed_or_restricted(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            manage(directory, "issue", "custom-tunnel")
            original = (directory / "clients.json").read_bytes()
            key = (directory / "custom-tunnel.key").read_bytes()
            (directory / "openai-tunnel.key").write_bytes(key)
            path = prepare(directory, "tunnel_synthetic_owner_test", "x" * 40, 7)
            self.assertEqual((directory / "clients.json").read_bytes(), original)
            self.assertEqual(
                json.loads(path.read_text())["WHATSAPP_MCP_TUNNEL_KEY"],
                key.decode().strip(),
            )

    def test_local_cli_needs_no_openai_files_and_never_prints_its_key(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / ".access"
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "deploy/prepare_install.py"),
                    "--no-tunnel",
                    "--history-days",
                    "7",
                    "--directory",
                    str(directory),
                ],
                capture_output=True,
                text=True,
                timeout=15,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            key = (directory / "local-client.key").read_text().strip()
            self.assertNotIn(key, result.stdout + result.stderr)
            self.assertNotIn(
                "CONTROL_PLANE_API_KEY",
                json.loads((directory / "railway-variables.json").read_text()),
            )

    def test_no_tunnel_prepares_a_private_local_key_and_preserves_it(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            path = prepare(
                directory, None, None, 30, no_tunnel=True, allow_send_text=True
            )
            original = path.read_bytes()
            variables = json.loads(original)
            self.assertEqual(variables["WHATSAPP_MCP_TUNNEL_ENABLED"], "false")
            self.assertNotIn("CONTROL_PLANE_API_KEY", variables)
            self.assertNotIn("WHATSAPP_MCP_TUNNEL_KEY", variables)
            registry = json.loads(variables["WHATSAPP_MCP_CLIENTS"])
            self.assertIn("send_message", registry["local-client"]["tools"])
            key = (directory / "local-client.key").read_bytes()
            self.assertEqual((directory / "worker.env").stat().st_mode & 0o777, 0o600)
            prepare(directory, None, None, 30, no_tunnel=True, allow_send_text=True)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual((directory / "local-client.key").read_bytes(), key)
            with self.assertRaises(ValueError):
                prepare(directory, "tunnel_synthetic_owner_test", "x" * 40, 30)
            self.assertEqual(path.read_bytes(), original)

    def test_local_install_rejects_mixed_connection_inputs(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "unused"
            with self.assertRaises(ValueError):
                prepare(
                    directory,
                    "tunnel_synthetic_owner_test",
                    "x" * 40,
                    7,
                    no_tunnel=True,
                )
            self.assertFalse(directory.exists())
        self.assertEqual(
            docker_command("whatsapp-mcp"),
            ["docker", "compose", "exec", "-T", "whatsapp-mcp", "sh", "-c"],
        )
        with self.assertRaises(ValueError):
            docker_command("service;other")

    def test_private_new_owner_inputs_and_rerun_preserve_credentials(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / ".access"
            path = prepare(directory, "tunnel_synthetic_owner_test", "x" * 40, 30)
            variables = json.loads(path.read_text())
            self.assertEqual(variables["WHATSAPP_HISTORY_BACKFILL_DAYS"], "30")
            self.assertEqual(variables["WHATSAPP_ALLOW_LOG_QR"], "false")
            self.assertEqual(variables["WHATSAPP_PAIRING_MODE"], "true")
            self.assertEqual(variables["WHATSAPP_MCP_READ_ONLY"], "true")
            self.assertEqual(variables["WHATSAPP_HISTORY_INITIAL_SYNC_FULL"], "true")
            self.assertEqual(
                variables["WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS"], "600"
            )
            self.assertEqual(
                variables["WHATSAPP_HISTORY_BACKFILL_START_DELAY_SECONDS"], "90"
            )
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
            original = path.read_bytes()
            key = (directory / "openai-tunnel.key").read_bytes()
            self.assertEqual(
                prepare(directory, "tunnel_synthetic_owner_test", "x" * 40, 30), path
            )
            for tunnel, days in [
                ("tunnel_synthetic_owner_test", 30),
                ("tunnel_other_owner_test", 7),
            ]:
                with self.assertRaises(ValueError):
                    prepare(directory, tunnel, "y" * 40, days)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual((directory / "openai-tunnel.key").read_bytes(), key)

    def test_text_send_opt_in_and_rerun_preserve_explicit_permission(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            path = prepare(
                directory,
                "tunnel_synthetic_owner_test",
                "x" * 40,
                7,
                allow_send_text=True,
            )
            variables = json.loads(path.read_text())
            self.assertEqual(variables["WHATSAPP_MCP_READ_ONLY"], "false")
            registry = json.loads(variables["WHATSAPP_MCP_CLIENTS"])
            self.assertIn("send_message", registry["openai-tunnel"]["tools"])
            original = path.read_bytes()
            prepare(
                directory,
                "tunnel_synthetic_owner_test",
                "x" * 40,
                7,
                allow_send_text=True,
            )
            with self.assertRaises(ValueError):
                prepare(directory, "tunnel_synthetic_owner_test", "x" * 40, 7)
            self.assertEqual(path.read_bytes(), original)

    def test_rerun_keeps_previously_prepared_profile_unchanged(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            path = prepare(directory, "tunnel_synthetic_owner_test", "x" * 40, 7)
            variables = json.loads(path.read_text())
            variables.pop("WHATSAPP_HISTORY_INITIAL_SYNC_FULL")
            variables.pop("WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS")
            path.write_text(json.dumps(variables, indent=2) + "\n")
            previous = path.read_bytes()
            key = (directory / "openai-tunnel.key").read_bytes()
            prepare(directory, "tunnel_synthetic_owner_test", "x" * 40, 7)
            self.assertEqual(path.read_bytes(), previous)
            self.assertEqual((directory / "openai-tunnel.key").read_bytes(), key)

    def test_history_period_is_explicit_and_supports_unbounded_recovery(self):
        for days in (0, 7, 90, 3650):
            with self.subTest(days=days), tempfile.TemporaryDirectory() as root:
                path = prepare(
                    Path(root), "tunnel_synthetic_owner_test", "x" * 40, days
                )
                variables = json.loads(path.read_text())
                self.assertEqual(variables["WHATSAPP_HISTORY_BACKFILL_DAYS"], str(days))
                self.assertEqual(
                    variables["WHATSAPP_HISTORY_INITIAL_SYNC_FULL"], "true"
                )
                self.assertEqual(
                    variables["WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS"], "600"
                )
                if days == 0:
                    self.assertEqual(
                        variables["WHATSAPP_HISTORY_BACKFILL_RUN_ID"], "initial-all"
                    )

    def test_cli_installs_without_test_import_paths_and_never_prints_keys(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            api_key = root / "runtime.key"
            api_key.write_text("x" * 40)
            api_key.chmod(0o600)
            command = [
                sys.executable,
                str(ROOT / "deploy" / "prepare_install.py"),
                "--tunnel-id",
                "tunnel_synthetic_owner_test",
                "--api-key-file",
                str(api_key),
                "--history-days",
                "0",
                "--directory",
                str(root / ".access"),
            ]
            environment = dict(os.environ)
            environment.pop("PYTHONPATH", None)
            for _ in range(2):
                result = subprocess.run(
                    command, env=environment, capture_output=True, text=True, timeout=15
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn(api_key.read_text(), result.stdout + result.stderr)
            variables = json.loads(
                (root / ".access" / "railway-variables.json").read_text()
            )
            self.assertEqual(variables["WHATSAPP_HISTORY_BACKFILL_DAYS"], "0")

    def test_invalid_install_inputs_do_not_create_keys(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "unused"
            for tunnel, key, days in [
                ("invalid", "x" * 40, 30),
                ("tunnel_synthetic_test", "short", 30),
                ("tunnel_synthetic_test", "x" * 40, -1),
            ]:
                with self.assertRaises(ValueError):
                    prepare(directory, tunnel, key, days)
            self.assertFalse(directory.exists())

    def test_pairing_transport_never_returns_raw_tokens(self):
        command = ssh_command("instance-test@ssh.railway.com", Path("/private/key"))
        self.assertIn("StrictHostKeyChecking=yes", command)
        with patch(
            "pair_account.subprocess.run",
            return_value=SimpleNamespace(
                returncode=0, stdout=b'{"status":"connected","token":"private"}'
            ),
        ) as run:
            self.assertEqual(
                remote_read(command, "/status"), b'{"status": "connected"}'
            )
            self.assertNotIn("private", remote_read(command, "/status").decode())
            self.assertIn("capture_output", run.call_args.kwargs)
        with patch(
            "pair_account.subprocess.run",
            return_value=SimpleNamespace(returncode=0, stdout=b"not-png"),
        ):
            with self.assertRaises(RuntimeError):
                remote_read(command, "/qr")
        with self.assertRaises(ValueError):
            remote_read(command, "/arbitrary")
        with self.assertRaises(ValueError):
            ssh_command("-oProxyCommand=bad", Path("/private/key"))
        self.assertEqual(railway_command("project", "production", "service")[-1], "--")
        with self.assertRaises(ValueError):
            railway_command(None, "production", "service")

    def test_browser_proxy_rejects_foreign_hosts_and_origins(self):
        calls = []

        def read(route):
            calls.append(route)
            return b'{"status":"scan_qr"}'

        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(read, 0))
        port = server.server_address[1]
        server.RequestHandlerClass = make_handler(read, port)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for headers, expected in [
                ({"Host": "foreign.example"}, 403),
                ({"Origin": "https://foreign.example"}, 403),
                ({}, 200),
            ]:
                conn = http.client.HTTPConnection("127.0.0.1", port)
                conn.request("GET", "/status", headers=headers)
                response = conn.getresponse()
                self.assertEqual(response.status, expected)
                response.read()
                if expected == 200:
                    self.assertEqual(response.getheader("Cache-Control"), "no-store")
                conn.close()
            self.assertEqual(calls, ["/status"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_history_target_remains_unconfirmed(self):
        window = requested_backfill_window(
            {"requested_days": 30, "target_after": "2026-09-12T10:00:00Z"}
        )
        self.assertEqual(window["days"], 30)
        self.assertIsNone(window["coverage_complete"])
        self.assertIsNone(
            requested_backfill_window(
                {"requested_days": True, "target_after": "private"}
            )
        )


if __name__ == "__main__":
    unittest.main()
