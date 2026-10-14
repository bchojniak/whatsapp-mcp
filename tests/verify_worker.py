"""Exercise the real worker/MCP in network-isolated Docker with a synthetic bridge.

The backup gate and WhatsApp network process are test fixtures. Their independent
storage behavior is covered by the backup and Go suites; this checks supervision,
connection-mode selection, and the real authenticated HTTP server.
"""

import hashlib
import json
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

IMAGE = sys.argv[1] if len(sys.argv) > 1 else "whatsapp-mcp:ci"
KEY = "s" * 43
POLICY = json.dumps(
    {"fixture": {"key_sha256": hashlib.sha256(KEY.encode()).hexdigest()}}
)
BACKUP = """import os,sys,pathlib
args=sys.argv
pathlib.Path(args[args.index('--ready-file')+1]).write_text('verified')
bridge=args[args.index('--bridge')+1]
os.execv(bridge,[bridge])
"""
BRIDGE = """#!/usr/local/bin/python
from http.server import BaseHTTPRequestHandler,HTTPServer
class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def do_GET(self):
        self.send_response(200);self.end_headers();self.wfile.write(b'{"ok":true,"connected":true}')
HTTPServer(('127.0.0.1',8080),Handler).serve_forever()
"""
TUNNEL = """#!/usr/local/bin/python
import pathlib,time
pathlib.Path('/tmp/fixture-tunnel-started').touch()
while True: time.sleep(1)
"""
PROBE = """import json,urllib.request,urllib.error,pathlib,sys
url='http://127.0.0.1:8000/mcp'
try:
    urllib.request.urlopen(urllib.request.Request(url,data=b'{}'))
    raise AssertionError('anonymous access accepted')
except urllib.error.HTTPError as error:
    assert error.code==401
def rpc(method,params):
    request=urllib.request.Request(url,data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode(),headers={'X-WhatsApp-MCP-Key':'s'*43,'Content-Type':'application/json','Accept':'application/json, text/event-stream'})
    with urllib.request.urlopen(request) as response: return json.load(response)['result']
rpc('initialize',{'protocolVersion':'2025-06-18','capabilities':{},'clientInfo':{'name':'worker-fixture','version':'1'}})
tools=rpc('tools/list',{})['tools']
assert len(tools)==11 and not any(t['name']=='send_message' for t in tools)
assert pathlib.Path('/tmp/fixture-tunnel-started').exists()==(sys.argv[1]=='tunnel')
print('Real worker: authenticated MCP discovery, anonymous denial, and '+sys.argv[1]+' startup passed')
"""


def docker(*args, check=True, timeout=35):
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=check, timeout=timeout
    )


with tempfile.TemporaryDirectory() as directory:
    directory = Path(directory)
    fixtures = {
        "backup_store.py": BACKUP,
        "whatsapp-bridge": BRIDGE,
        "tunnel-client": TUNNEL,
    }
    mounts = []
    for name, contents in fixtures.items():
        path = directory / name
        path.write_text(contents)
        path.chmod(0o755)
        target = (
            "/usr/local/bin/tunnel-client"
            if name == "tunnel-client"
            else "/app/bin/" + name
        )
        mounts += ["--mount", f"type=bind,src={path},dst={target},readonly"]
    for mode in ("direct", "tunnel"):
        name = "whatsapp-mcp-fixture-" + uuid.uuid4().hex[:12]
        env = [
            "-e",
            "WHATSAPP_MCP_CLIENTS=" + POLICY,
            "-e",
            "WHATSAPP_MCP_HOST=0.0.0.0",
        ]
        if mode == "tunnel":
            for key, value in {
                "CONTROL_PLANE_API_KEY": "x" * 40,
                "CONTROL_PLANE_TUNNEL_ID": "tunnel_synthetic_test",
                "WHATSAPP_MCP_TUNNEL_KEY": KEY,
            }.items():
                env += ["-e", key + "=" + value]
        try:
            docker(
                "run",
                "--detach",
                "--network=none",
                "--name",
                name,
                *mounts,
                *env,
                IMAGE,
            )
            for _ in range(30):
                ready = docker(
                    "exec",
                    name,
                    "python",
                    "-c",
                    "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/healthz',timeout=1)",
                    check=False,
                )
                if ready.returncode == 0:
                    break
                if (
                    docker(
                        "inspect", name, "--format", "{{.State.Running}}"
                    ).stdout.strip()
                    != "true"
                ):
                    raise RuntimeError("Synthetic worker exited before readiness")
                time.sleep(0.25)
            else:
                raise RuntimeError("Synthetic worker did not become ready")
            if mode == "tunnel":
                # Readiness precedes starting the tunnel in the entrypoint.
                docker(
                    "exec",
                    name,
                    "python",
                    "-c",
                    "import pathlib,time\nfor _ in range(40):\n if pathlib.Path('/tmp/fixture-tunnel-started').exists(): break\n time.sleep(.1)",
                )
            print(docker("exec", name, "python", "-c", PROBE, mode).stdout.strip())
        finally:
            docker("rm", "--force", name, check=False)
    failed = docker(
        "run",
        "--rm",
        "--network=none",
        "-e",
        "WHATSAPP_MCP_TUNNEL_ENABLED=false",
        "-e",
        "WHATSAPP_MCP_CLIENTS={}",
        IMAGE,
        check=False,
    )
    assert failed.returncode != 0
    print("Worker with tunnel disabled still rejects an invalid application policy")
