# Private Railway worker deployment

For a new account, start with [installation](docs/INSTALLATION.md).
To transfer a saved session and archive, use the data-transfer instructions below.

This image runs the Go WhatsApp bridge and an authenticated Python Streamable
HTTP MCP server. On Railway, keep both listeners on loopback and connect through
an OpenAI Secure MCP Tunnel, Tailscale, or both. Do not add a public Railway
domain or TCP proxy. For a local worker without either service, use
[Docker Compose](docs/LOCAL_DOCKER.md).

## Configuration

Create a private Railway service from this repository. Use the repository root
as its Root Directory (leave the field unset or use `/`), the Dockerfile builder,
and `Dockerfile` at that root. The root-level `railway.json` supplies the builder
and restart settings. Do not select an enclosing workspace or a nested source
folder as the build context.

Attach a persistent volume at `/data` and set `WHATSAPP_MCP_CLIENTS` to a valid
client-key policy. For an OpenAI tunnel, also set:

- `OPENAI_TUNNEL_ID`: the tunnel ID from OpenAI Platform Tunnels. `CONTROL_PLANE_TUNNEL_ID` is also accepted.
- `CONTROL_PLANE_API_KEY`: a runtime API key whose principal has Tunnels Read + Use. Do not use an admin key for the worker.
- `WHATSAPP_MCP_TUNNEL_KEY`: the enabled tunnel client's key; follow [client access setup](CLIENT_AUTH.md).

Tunnel mode is detected from its credentials unless `WHATSAPP_MCP_TUNNEL_ENABLED`
explicitly selects `true` or `false`. For Tailscale-only access, prepare with
`--no-tunnel`, set `WHATSAPP_MCP_TUNNEL_ENABLED=false`, and configure Tailscale.
The application policy is still required. Partial tunnel credentials fail startup.

When uploading a saved database snapshot, set `WHATSAPP_MIGRATION_MODE=true` and deploy. This keeps the container idle without starting WhatsApp or requiring tunnel credentials, while Railway's authenticated file tooling transfers the snapshot into `/data`. Remove the variable before starting the worker.

The image supplies these safe deployment defaults:

- `WHATSAPP_STORE_DIR=/data`
- `WHATSAPP_MESSAGES_DB_PATH=/data/messages.db`
- `WHATSAPP_API_LISTEN_ADDR=127.0.0.1:8080`
- `WHATSAPP_API_BASE_URL=http://127.0.0.1:8080/api`
- `WHATSAPP_MCP_READ_ONLY=true`
- `WHATSAPP_ALLOW_LOG_QR=false`

Read-only mode omits `send_message`, `send_file`, and `send_audio_message` from MCP tool discovery and removes the bridge's internal `/api/send` route. Media retrieval remains available through `download_media` and the loopback `/api/download` route. Optional [text sending](docs/SENDING.md) requires an explicit client grant and read-only mode disabled. Query results are capped at 50 records, pagination at page 100, context windows at 10 messages per side, individual message text at 4,000 characters, and combined text results at 20,000 characters.

The entrypoint tightens `/data` directories to mode `0700`, files to `0600`, transfers ownership to the dedicated `whatsapp` user, and drops root before starting any worker process. Message contents, contact names, chat identifiers, media URLs, and filenames must never be written to application logs.

`WHATSAPP_BRIDGE_START_TIMEOUT` controls the supervisor's default 300-second API startup window, after the backup gate finishes. `WHATSAPP_BACKUP_START_TIMEOUT` separately defaults to 1500 seconds for both SQLite snapshots and verification; each online copy is bounded to 600 seconds. Any platform health-check startup timeout must cover the backup phase as well as worker startup. QR codes are not logged by default. If the saved WhatsApp session is lost, restore and inspect the private session backup before choosing to pair again.

### Startup backups and storage checks

Normal startup requires both `messages.db` and `whatsapp.db` under `WHATSAPP_STORE_DIR` (default `/data`). A missing or wrong volume fails closed before the bridge starts. Only a deliberate brand-new installation should set `WHATSAPP_ALLOW_FRESH_STORE=true`; remove it once both databases exist. Previously backed-up databases that later disappear always block startup, even with that opt-in.

Before the bridge can mutate storage, `deploy/backup_store.py` uses SQLite's online backup API, including committed WAL data, to create private snapshots under `/data/.startup-backups/message-identity-provenance-v1/`. Each database has its own immutable `snapshot.sqlite3` and `manifest.json`, with SHA-256, integrity/foreign-key checks, source identity, timestamp, and aggregate message/chat/metadata counts. The two snapshots are separate points in time, not a cross-database atomic snapshot. Logs contain only safe counts and status categories.

These snapshots share the `/data` volume. They protect against migration or code-rollback mistakes, not loss of that volume; off-volume disaster recovery needs a separately authorized backup.

Restarts verify and reuse completed snapshots. Device numbers are informational across volume remounts; an unexpected source inode change requires operator review of a restore/replacement. An incomplete, corrupt, or mismatched snapshot is preserved and blocks startup rather than being overwritten or multiplied by retries. Investigate it privately; never delete the source, reset pairing, or discard retained backup evidence to bypass the gate. An operator can preserve an interrupted attempt under a separate private recovery location before explicitly retrying.

Disk preflight budgets pending snapshots, existing WAL/journal size, additive row/metadata retention and its transaction journal, plus 64 MiB minimum headroom. Once the expected retention tables and six triggers exist, ordinary reuse needs runtime headroom rather than another full migration budget. No backup cleanup or retention deletion runs automatically.

The gate holds an exclusive volume lock through the bridge's lifetime and detects other bridge executables visible in its process namespace. Workers in another container cannot be conclusively detected through `/proc`; use one replica and Railway's stop-before-replacement contract for services with an attached volume. Do not run a second paired writer against this store.

## Transfer a saved session and archive

Never copy live SQLite files. Stop the local bridge first, confirm no bridge process still has either database open, then make private backups of both `store/messages.db` and `store/whatsapp.db`. Include the ingestion journal, `outbound.db` when present, and downloaded-media subdirectories under `store/`. Use consistent SQLite online backups for each database. Transfer that snapshot through a one-time private operator channel directly into the Railway volume at `/data`; never add it to Git, a Docker build context, variables, or logs. Verify the two database files and media directories exist under `/data` before starting the worker.

Run only one WhatsApp bridge for this session. Keep the Railway service stopped
during transfer, start it after the volume is complete, and do not restart the
local bridge after the transfer. For a new account, start with an empty `/data`, enable
`WHATSAPP_ALLOW_FRESH_STORE=true` and `WHATSAPP_PAIRING_MODE=true`, and follow the
private SSH pairing flow in [installation](docs/INSTALLATION.md).
Remove both flags after verifying the saved session. Keep `WHATSAPP_ALLOW_LOG_QR`
disabled in the new installation flow.

## Verification

After startup, check readiness logs and make authenticated MCP calls to verify:

1. The bridge connected and its API became ready.
2. The selected private route is ready; if the tunnel is enabled, `tunnel-client` started without exposing secrets.
3. The MCP client discovers the expected tools through that route.
4. A read-only query such as `list_chats` succeeds with a permitted key; eleven read tools including media retrieval and identity inspection appear. In read-only mode no send tool appears; if text sending is enabled, verify its explicit client grant.
5. A Tailscale MCP request without a key returns `401`; the local readiness check `/healthz` returns only `{"ok":true}`.

When enabled, the tunnel client's admin and health endpoints stay on container loopback at `127.0.0.1:8081`; there is no inbound Railway endpoint. Connector availability depends on this worker remaining active.

## Recovery and moving the worker

Stop the active worker before starting another bridge with the same session.
If returning to a local worker, transfer a consistent snapshot of the current
archive, session and media so messages received on Railway are preserved.
Restoring an earlier snapshot cannot recover later ingestion; reconcile that data
before replacing a newer archive. Keep the Railway volume intact until recovery
is verified. Never run the Railway and local bridges concurrently.

## Optional Tailscale access

Optional. Set `TS_AUTHKEY` on the Railway service (reusable, non-ephemeral auth key). Do not use Funnel and do not add a Railway domain.

The worker starts `tailscaled` in userspace networking and `tailscale serve` against `http://127.0.0.1:8000`. The MCP stays on loopback. Tunnel clients use the OpenAI tunnel; Tailscale clients use `https://whatsapp-mcp.<tailnet>.ts.net/mcp` and supply their dedicated `X-WhatsApp-MCP-Key` header. Both paths pass through the same server authentication check.

`TS_HOSTNAME` defaults to `whatsapp-mcp`. The MCP allows that MagicDNS Host header so Tailscale serve can proxy `/mcp`.

