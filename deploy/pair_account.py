#!/usr/bin/env python3
"""Relay private pairing over SSH or Docker Compose without exposing its token."""

import argparse
import json
import os
import re
import shlex
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REMOTE = """import pathlib,sys,urllib.request
token=pathlib.Path('/data/.pairing-token').read_text().strip()
request=urllib.request.Request('http://127.0.0.1:8082'+sys.argv[1],headers={'Authorization':'Bearer '+token})
try:
    with urllib.request.urlopen(request,timeout=5) as response:
        data=response.read(1048577)
        if len(data)>1048576: raise ValueError()
        sys.stdout.buffer.write(data)
except Exception:
    sys.exit(1)
"""

PAGE = """<!doctype html><html lang="pt-BR"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Conectar WhatsApp</title><style>
body{font:18px system-ui;max-width:560px;margin:48px auto;padding:24px;color:#172222;background:#f6f8f7}
img{display:block;width:320px;max-width:100%;image-rendering:pixelated;border:16px solid white}
</style><h1>Conecte seu WhatsApp</h1>
<p>No celular, abra Configurações → Dispositivos conectados → Conectar dispositivo.</p>
<p id="state">Aguardando conexão…</p><img id="qr" alt="QR para vincular seu WhatsApp" hidden>
<p>Esta página é privada e temporária. Depois de conectar, o agente continuará a instalação.</p>
<script>
async function update(){try{const r=await fetch('/status');if(!r.ok)throw Error();const s=await r.json();
const texts={connected:'WhatsApp conectado. Pode voltar ao agente.',scan_qr:'Escaneie o QR abaixo.',
waiting_for_qr:'Preparando o QR…',expired:'QR expirou. Peça ao agente para reiniciar o serviço ainda não conectado.'};
document.querySelector('#state').textContent=texts[s.status]||'Aguardando conexão…';
const image=document.querySelector('#qr');image.hidden=s.status!=='scan_qr';
if(!image.hidden)image.src='/qr?t='+Date.now();else image.removeAttribute('src');
}catch{document.querySelector('#state').textContent='Aguardando o serviço privado. O agente pode conferir a conexão.'}}
update();setInterval(update,10000);
</script></html>""".encode()


def ssh_command(target, identity):
    if not re.fullmatch(r"[a-zA-Z0-9_.-]+@ssh\.railway\.com", target):
        raise ValueError("Use the exact service-instance target from Railway SSH")
    return [
        "ssh",
        "-T",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "ConnectTimeout=8",
        "-i",
        str(identity),
        target,
    ]


def remote_read(command, route):
    if route not in {"/qr", "/status"}:
        raise ValueError("Unsupported pairing route")
    remote = "python -c " + shlex.quote(REMOTE) + " " + shlex.quote(route)
    result = subprocess.run(command + [remote], capture_output=True, timeout=20)
    if result.returncode:
        raise RuntimeError(
            "Private pairing unavailable; check worker access and pairing mode"
        )
    if len(result.stdout) > 1024 * 1024:
        raise RuntimeError("Pairing response too large")
    if route == "/qr" and not result.stdout.startswith(b"\x89PNG\r\n\x1a\n"):
        raise RuntimeError("Private pairing did not return a PNG")
    if route == "/status":
        state = json.loads(result.stdout).get("status")
        if state not in {"waiting_for_qr", "scan_qr", "connected", "expired", "closed"}:
            raise RuntimeError("Invalid pairing state")
        return json.dumps({"status": state}).encode()
    return result.stdout


def railway_command(project, environment, service, identity=None):
    if not all(
        re.fullmatch(r"[a-zA-Z0-9_-]+", value or "")
        for value in (project, environment, service)
    ):
        raise ValueError("Supply explicit Railway project, environment and service IDs")
    command = [
        "railway",
        "ssh",
        "--project",
        project,
        "--environment",
        environment,
        "--service",
        service,
    ]
    if identity is not None:
        command.extend(["--identity-file", str(identity)])
    return command + ["--"]


def docker_command(service):
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", service or ""):
        raise ValueError("Invalid Compose service name")
    return ["docker", "compose", "exec", "-T", service, "sh", "-c"]


def make_handler(read, port):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            if self.headers.get("Host") != f"127.0.0.1:{port}" or self.headers.get(
                "Origin"
            ):
                self.send_error(403)
                return
            try:
                if self.path == "/":
                    body, mime = PAGE, "text/html; charset=utf-8"
                elif self.path == "/status":
                    body, mime = read("/status"), "application/json"
                elif re.fullmatch(r"/qr\?t=\d+", self.path):
                    body, mime = read("/qr"), "image/png"
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                for name, value in [
                    ("Content-Type", mime),
                    ("Cache-Control", "no-store"),
                    ("X-Frame-Options", "DENY"),
                    ("X-Content-Type-Options", "nosniff"),
                    ("Referrer-Policy", "no-referrer"),
                ]:
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(body)
            except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired):
                self.send_error(503, "Private pairing unavailable")

    return Handler


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["serve", "qr", "status"])
    parser.add_argument("--ssh-target")
    parser.add_argument(
        "--compose-service", help="Read pairing privately through docker compose exec"
    )
    parser.add_argument("--identity-file", type=Path)
    parser.add_argument("--project-id")
    parser.add_argument("--environment-id")
    parser.add_argument("--service-id")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--port", type=int, default=3214)
    args = parser.parse_args()
    try:
        if args.compose_service:
            if any(
                [
                    args.ssh_target,
                    args.identity_file,
                    args.project_id,
                    args.environment_id,
                    args.service_id,
                ]
            ):
                raise ValueError("Compose pairing cannot use SSH arguments")
            command = docker_command(args.compose_service)
        elif args.ssh_target:
            if args.identity_file is None or any(
                [args.project_id, args.environment_id, args.service_id]
            ):
                raise ValueError("Direct SSH needs an identity file and no CLI IDs")
            command = ssh_command(args.ssh_target, args.identity_file)
        else:
            command = railway_command(
                args.project_id,
                args.environment_id,
                args.service_id,
                args.identity_file,
            )
        if not 1024 <= args.port <= 65535:
            raise ValueError("Invalid port")
        if args.action == "status":
            print(remote_read(command, "/status").decode())
        elif args.action == "qr":
            if args.output is None:
                raise ValueError("QR output path is required")
            body = remote_read(command, "/qr")
            with args.output.open("xb") as stream:
                stream.write(body)
            args.output.chmod(0o600)
            print(f"Private QR image saved: {args.output}")
        else:
            handler = make_handler(lambda route: remote_read(command, route), args.port)
            print(f"Private pairing page: http://127.0.0.1:{args.port}", flush=True)
            with ThreadingHTTPServer(("127.0.0.1", args.port), handler) as server:
                server.serve_forever()
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired):
        parser.exit(
            1,
            "Private pairing unavailable. Check worker access, pairing mode, and output file.\n",
        )


if __name__ == "__main__":
    main()
