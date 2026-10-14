#!/usr/bin/env python3
"""Export tracked source without private repository history or runtime files."""

import argparse
import io
import os
import subprocess
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PRIVATE_PARTS = {
    ".git",
    ".access",
    ".venv",
    "__pycache__",
    "store",
    "data",
    "attachments",
}


def export_source(output, ref="HEAD", root=ROOT):
    output = output.resolve()
    root = root.resolve()
    if output.is_relative_to(root):
        raise ValueError("Write the source archive outside the repository")
    commit = subprocess.check_output(
        ["git", "rev-parse", "--verify", "--end-of-options", ref + "^{commit}"],
        cwd=root,
        text=True,
    ).strip()
    source = subprocess.check_output(
        ["git", "archive", "--format=tar", commit], cwd=root
    )
    with tarfile.open(fileobj=io.BytesIO(source)) as archive:
        members = archive.getmembers()
        for member in members:
            path = Path(member.name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or set(path.parts) & PRIVATE_PARTS
            ):
                raise ValueError("Source contains a private or unsafe path")
            if path.name.startswith(".env") or path.suffix in {
                ".db",
                ".sqlite",
                ".sqlite3",
                ".key",
                ".pem",
                ".log",
            }:
                raise ValueError("Source contains a private artifact")
            if not member.isfile() and not member.isdir():
                raise ValueError("Source export requires regular files and directories")
        fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with (
                os.fdopen(fd, "wb") as stream,
                tarfile.open(fileobj=stream, mode="w:gz") as target,
            ):
                for member in members:
                    clean = tarfile.TarInfo("whatsapp-mcp/" + member.name)
                    clean.mode, clean.size, clean.type = (
                        member.mode,
                        member.size,
                        member.type,
                    )
                    content = archive.extractfile(member) if member.isfile() else None
                    try:
                        target.addfile(clean, content)
                    finally:
                        if content is not None:
                            content.close()
        except BaseException:
            output.unlink(missing_ok=True)
            raise
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ref", default="HEAD")
    args = parser.parse_args()
    try:
        result = export_source(args.output, args.ref)
    except (OSError, ValueError, subprocess.CalledProcessError):
        parser.exit(
            1,
            "Source export failed; check the ref, source paths and output destination.\n",
        )
    print(f"Source archive prepared: {result}")


if __name__ == "__main__":
    main()
