# Installation

The worker runs the WhatsApp bridge and authenticated MCP server. For local
installation without Railway or OpenAI credentials, follow [Docker setup](LOCAL_DOCKER.md).
The steps below cover Railway using a private OpenAI tunnel or Tailscale route.
Configure the corresponding route in the MCP client's own settings.

## Prerequisites

- Access to this repository and a local environment with Python and `uv`.
- A Railway account with an always-on service, persistent volumes, and CLI/SSH
  access. Verify available resources and billing for that account.
- For the OpenAI route: a tunnel and a private runtime credential with Tunnels
  Read + Use. Creating or managing a tunnel requires Tunnels Read + Manage.
  For Tailscale-only access: a tailnet and a private authentication key instead.
- A WhatsApp account and access to its phone to link a device.
- An MCP client that supports the configured private connection route. Tailscale
  clients need Streamable HTTP with a custom authentication header.

Each installation requires its own session, archive, keys, and volume. Run
commands from the repository root. Do not reuse another installation's storage
or credentials.

## Prepare the configuration

Install the Python dependencies:

```sh
cd whatsapp-mcp-server
uv sync --frozen --no-dev
cd ..
```

Choose the history period before generating configuration. `--history-days` is
required and accepts `0` through `3650`; `0` requests all available history.
The period guides the registration-time history request and subsequent backfill,
but WhatsApp may supply less history or leave gaps.

For the OpenAI route, create a tunnel in [OpenAI Platform Tunnels](https://platform.openai.com/settings/organization/tunnels)
and store its runtime credential in a private file with mode `0600`, outside
source control. Then run:

```sh
whatsapp-mcp-server/.venv/bin/python deploy/prepare_install.py \
  --tunnel-id tunnel_YOUR_TUNNEL_ID \
  --api-key-file /secure/openai-runtime.key \
  --history-days HISTORY_DAYS
```

For Tailscale-only access, use this instead, then configure `TS_AUTHKEY` and
`TS_TAILNET` privately as described in [Railway deployment](../RAILWAY.md):

```sh
whatsapp-mcp-server/.venv/bin/python deploy/prepare_install.py \
  --no-tunnel --history-days HISTORY_DAYS
```

Add `--allow-send-text` only when sending is wanted. It sets
`WHATSAPP_MCP_READ_ONLY=false` and explicitly grants `send_message` to the
generated client (`openai-tunnel` or `local-client`). Without that option, the installation permits reads only.
See [text sending](SENDING.md).

The helper writes `.access/railway-variables.json` and `.access/worker.env`. The private directory and its
files are excluded from Git and Docker builds. Import the variables through a
private editor or authenticated API; do not print values, paste them into chats,
or pass secrets in command arguments.

Repeating the command with identical inputs preserves the prepared export and
keys. Different inputs are rejected rather than replacing credentials. Use a
separate checkout or access directory for another installation.

## Deploy the worker

Create a new Railway project, environment, service, and persistent volume mounted
at `/data`. Use one replica with sleeping/serverless disabled. Allow space for the
archive, media, ingestion journal, and backup snapshots; monitor actual usage.

Use the repository root as the build context, the Dockerfile builder, and
`Dockerfile`. Import the prepared variables before starting the worker. Connect
the chosen repository/branch or upload from the root:

```sh
railway up --project PROJECT_ID --environment ENVIRONMENT_ID --service SERVICE_ID
```

Do not add a public domain, TCP proxy, or Tailscale Funnel. Both the MCP server
and the temporary pairing endpoint stay on loopback. Deployment details and
startup backup requirements are in [Railway deployment](../RAILWAY.md).

The generated configuration enables `WHATSAPP_ALLOW_FRESH_STORE` and
`WHATSAPP_PAIRING_MODE` for the new account. It requests expanded initial history
before pairing. The MCP server starts after linking WhatsApp; a worker waiting
for its first QR scan is not ready for tool calls.

## Link WhatsApp privately

Authenticate the Railway CLI for the deployment account and prepare SSH access.
A dedicated public SSH key can be registered with:

```sh
railway ssh keys add --key PUBLIC_KEY_PATH --name whatsapp-setup
```

Keep the private key protected and host verification enabled. Start the private
pairing helper using the new service's IDs:

```sh
python3 deploy/pair_account.py serve \
  --project-id PROJECT_ID --environment-id ENVIRONMENT_ID --service-id SERVICE_ID
```

Open the helper's printed localhost URL on the same computer. On the phone, use
**WhatsApp → Settings → Linked devices → Link a device** and scan the QR.

To obtain a private image instead:

```sh
python3 deploy/pair_account.py qr \
  --project-id PROJECT_ID --environment-id ENVIRONMENT_ID --service-id SERVICE_ID \
  --output /tmp/whatsapp-pairing.png
```

Keep the image private, refresh it if expired, and remove it after pairing. Never
commit QR images, session databases, or credentials. The helper also accepts
`--ssh-target INSTANCE@ssh.railway.com --identity-file PRIVATE_KEY_PATH` instead
of the project, environment, and service IDs. Instance and service IDs differ.

Check `pair_account.py status` with the same connection arguments for `connected`.
Do not delete session databases or force another pairing to fix an SSH problem.

## Finish setup and connect an MCP client

After the account connects and both `messages.db` and `whatsapp.db` exist in
`/data`, remove `WHATSAPP_ALLOW_FRESH_STORE` and `WHATSAPP_PAIRING_MODE` from the
service configuration and both prepared local exports. Redeploy, verify reconnection
using the saved session, and stop the pairing helper. Keep the startup backup
gate enabled.

For the tunnel route, configure the MCP client to use the corresponding OpenAI
tunnel. The worker supplies the application key through private headers.
For Tailscale, configure Streamable HTTP at
`https://whatsapp-mcp.<tailnet>.ts.net/mcp` with the client's own
`X-WhatsApp-MCP-Key` header. See [client authentication](../CLIENT_AUTH.md) for
issuing keys, supported grants, and connection requirements. Do not put keys
in URLs. The HTTP server does not provide a stdio transport.

## Verify the installation

Use the intended MCP client to verify:

1. `get_sync_status` reports a reachable bridge, readable database, connected
   account, and no reauthentication requirement. Inspect pending ingestion and
   failures separately from history completeness.
2. `list_chats`, a search for a known message, and `get_message_context` succeed.
   Receive a new message and confirm it appears through MCP.
3. The configured history period appears in `archive.backfill.requested_window`.
   Compare known messages in recent individual and group conversations against
   the phone, and record any gaps.
4. Unauthenticated MCP calls are rejected. Read-only mode disables sending;
   clients without an explicit write grant remain readers when sending is enabled.
5. If sending was enabled and a real test is authorized, resolve the recipient
   from authoritative account/contact identity, use one stable request ID, and
   inspect the result and WhatsApp conversation. A `sent` result means bridge
   acceptance, not a delivery or read receipt. Follow [send outcome handling](SENDING.md).
6. The saved session, archive, and configured history target survive the setup
   redeployment without resetting pairing.

Startup can spend time checking backups before the bridge and MCP server start.
A successful Railway deployment is not sufficient evidence of application
readiness. Likewise, initial transfer progress, an empty queue, or a finished
backfill does not establish complete history.

For history settings and recovery, see [initial synchronization](INITIAL_HISTORY_SYNC.md),
[history coverage](../DATA_COMPLETENESS.md), and the
[technical reference](TECHNICAL_REFERENCE.md). Keep consistent backups outside
the runtime volume; startup snapshots on that volume do not protect against its
loss. Never run two bridge instances against the same linked session.
