# Local Docker setup

Run the WhatsApp bridge and authenticated MCP server on a computer with Docker
Compose and Python 3.11 or later. This route does not need Railway, an OpenAI
account, or a tunnel. It uses a named Docker volume for the archive and session.

## Prepare access and history

From the repository root, choose a history period and prepare private access:

```sh
python3 deploy/prepare_install.py --no-tunnel --history-days 30
docker compose up --build -d
```

Use `0` instead of `30` to request all available history. This is a recovery
target, not a coverage guarantee. Add `--allow-send-text` to the preparation
command only when text sending is wanted. Each installation uses its own
`.access` directory, account, and runtime volume.

The helper writes `.access/clients.json`, `.access/local-client.key`, and worker
configuration. Keep these private. Compose reads `.access/worker.env` and exposes
the authenticated MCP endpoint only at `http://127.0.0.1:8000/mcp` on the host.
The bridge, pairing endpoint, and databases are not published as host ports.
Do not change the port binding to a public interface to obtain remote access.

## Link the phone

The first startup waits for a private QR scan. In another terminal, from the same
repository and Compose project, run:

```sh
python3 deploy/pair_account.py serve --compose-service whatsapp-mcp
```

Open the printed localhost URL on that computer. On the phone, open
**WhatsApp → Settings → Linked devices → Link a device** and scan the QR.
The helper reads the protected pairing endpoint through `docker compose exec`;
it does not reveal its private token or publish the pairing service.

Check the status with:

```sh
python3 deploy/pair_account.py status --compose-service whatsapp-mcp
```

After connection is confirmed, remove `WHATSAPP_ALLOW_FRESH_STORE` and
`WHATSAPP_PAIRING_MODE` from `.access/worker.env` and
`.access/railway-variables.json`. Restart with the saved volume:

```sh
docker compose up -d --force-recreate
```

Stop the pairing helper. Startup checks backups before reconnecting; this can
take time. Keep the volume and saved session intact while waiting.

## Connect the MCP client

Configure Streamable HTTP at `http://127.0.0.1:8000/mcp`. Set the
`X-WhatsApp-MCP-Key` header privately to the contents of
`.access/local-client.key`. Use a client that supports custom HTTP headers;
the server does not provide a stdio transport. Do not put the key in a URL or
commit client settings containing it.

Verify `get_sync_status`, `list_chats`, and a known-message search through the
intended client. Unauthenticated requests are rejected. If sending was enabled,
verify the explicit grant and follow [send outcome handling](SENDING.md).

Keep the host running for access and live ingestion. `docker compose down` stops
the worker while retaining the named volume. **`docker compose down -v` deletes
the volume**, including the paired session and archived messages. Maintain
consistent backups outside that volume.

For remote private access, configure [Tailscale or an optional OpenAI tunnel](../CLIENT_AUTH.md).
Providing tunnel credentials enables its validation automatically unless
`WHATSAPP_MCP_TUNNEL_ENABLED` is explicitly `false`. Existing tunnel deployments
do not need to change their keys or re-pair WhatsApp.
