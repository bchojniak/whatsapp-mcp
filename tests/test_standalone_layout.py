"""Deployment sources must resolve from the standalone repository root."""

import json
import shlex
import unittest
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]


class StandaloneLayoutTests(unittest.TestCase):
    def test_railway_uses_root_dockerfile(self):
        configuration = json.loads((ROOT / "railway.json").read_text())
        self.assertEqual(configuration["build"]["builder"], "DOCKERFILE")
        self.assertEqual(configuration["build"]["dockerfilePath"], "Dockerfile")
        self.assertTrue((ROOT / configuration["build"]["dockerfilePath"]).is_file())

    def test_docker_local_copy_inputs_exist_inside_context(self):
        sources = []
        for line in (ROOT / "Dockerfile").read_text().splitlines():
            if not line.startswith("COPY ") or "--from=" in line:
                continue
            sources.extend(shlex.split(line)[1:-1])
        self.assertTrue(sources)
        for source in sources:
            with self.subTest(source=source):
                self.assertFalse(Path(source).is_absolute())
                self.assertNotIn("..", Path(source).parts)
                matches = list(ROOT.glob(source))
                self.assertTrue(matches, f"Missing build input: {source}")
                for match in matches:
                    self.assertTrue(match.is_file())
                    self.assertTrue(match.resolve().is_relative_to(ROOT))

    def test_python_readme_and_operator_script_resolve_locally(self):
        project_root = ROOT / "whatsapp-mcp-server"
        project = tomllib.loads((project_root / "pyproject.toml").read_text())
        readme = (project_root / project["project"]["readme"]).resolve()
        self.assertEqual(readme, ROOT / "README.md")
        self.assertTrue(readme.is_file())
        self.assertTrue((ROOT / "deploy" / "manage_access.py").is_file())
        self.assertTrue((project_root / "access_control.py").is_file())

    def test_runtime_and_secret_files_are_excluded(self):
        git_patterns = set((ROOT / ".gitignore").read_text().splitlines())
        docker_patterns = set((ROOT / ".dockerignore").read_text().splitlines())
        for git_pattern, docker_pattern in (
            (".env", "**/.env"),
            (".env.*", "**/.env.*"),
            (".venv/", "**/.venv"),
            ("store/", "**/store"),
            ("data/", "**/data"),
            ("attachments/", "**/attachments"),
            (".access/", "**/.access"),
            ("*.db", "**/*.db"),
            ("*.sqlite", "**/*.sqlite"),
            ("*.sqlite3", "**/*.sqlite3"),
            ("*.key", "**/*.key"),
            ("*.pem", "**/*.pem"),
        ):
            with self.subTest(pattern=git_pattern):
                self.assertIn(git_pattern, git_patterns)
                self.assertIn(docker_pattern, docker_patterns)


if __name__ == "__main__":
    unittest.main()
