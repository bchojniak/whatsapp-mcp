# Text sending

The `send_message` tool sends text through the linked WhatsApp account
to an individual or a group. Reading remains the default. Sending requires both
the service setting and an explicit grant for the client making the call.
File and voice-note sending are not exposed through the authenticated MCP.

## Send requests and outcomes

Resolve the intended recipient before calling `send_message`. The tool accepts
country-code phone digits or an exact direct/group WhatsApp JID, nonempty text
up to 65,536 characters, and a unique request ID (`idempotency_key`). Request IDs
contain 8–128 ASCII letters, digits, underscores, or hyphens.
The server checks the client’s permission, records the attempt on persistent
storage, then asks the WhatsApp bridge to send it.

- `sent`: the bridge accepted the send. This does not confirm recipient delivery
  or reading.
- `unknown`: the message may have been sent, or the attempt may have stopped
  before sending. Check the conversation before starting another send.
- `conflict`: the same request ID was supplied with different text or recipient.
- `rejected`: sending is not permitted or the input is invalid.

Reuse the same request ID, recipient and text when retrying or checking a send.
The server returns its recorded outcome without sending again, including after
a restart. A new ID represents a deliberately new message. This protection is
scoped to the authenticated client and requires retaining `outbound.db`.
When available, `message_id` and `chat_jid` identify the accepted outgoing message;
repeated calls return the same stored reference.
An uncertain attempt is never automatically retried, so preventing duplicates
can mean leaving a message unsent. It is not an exactly-once delivery guarantee.

## Outgoing archive visibility

Accepted text messages are written to the archive with the provider's message ID,
authoritative account identity, and an `outgoing` source label. This records a
local send observation, not a delivery or read receipt. `archive_status` reports
what happened during the attempt:

- `saved`: the outgoing message was committed and can be queried by its reference.
- `pending`: the archive write was durably queued after an immediate write failure.
  Recovery retries storage only, without sending another message.
- `failed`: WhatsApp accepted the send but local archival and queueing failed.
  Inspect storage diagnostics and the WhatsApp application; do not resend to fix it.
- `unknown`: the bridge returned a message reference without a recognized archive result.
  Older attempts or bridges can omit the reference and archive fields entirely.

The send journal stores the initial archive status, not live recovery progress.
Read the message reference to check a pending write later. A process failure
between WhatsApp acceptance and local persistence can leave an uncertain attempt;
no archive mechanism establishes delivery or complete history.

Confirmation is an agent behavior, not an approval screen enforced by this server.
The tool description directs the agent to follow the user's sending instructions.

## Enable it for a new installation

Add `--allow-send-text` to the `deploy/prepare_install.py` command in
[the installation guide](INSTALLATION.md). The prepared configuration sets
`WHATSAPP_MCP_READ_ONLY=false` and grants `send_message` to the generated client
(`openai-tunnel`, or `local-client` with `--no-tunnel`).
Without that flag, the installer prepares read-only access.

## Enable it for an installed client

Use the private access directory belonging to that installation. Replace
`openai-tunnel` with the actual client name, such as `local-client`:

```sh
whatsapp-mcp-server/.venv/bin/python deploy/manage_access.py grant-send openai-tunnel
```

This updates `clients.json` without changing the key or other grants. Apply its
contents privately to `WHATSAPP_MCP_CLIENTS`, set `WHATSAPP_MCP_READ_ONLY=false`,
and restart the service. Do not print the registry or credentials in a chat.
Refresh the connector's tool discovery if it has cached the read-only list.
No new WhatsApp pairing or backfill run is required.

Setting `WHATSAPP_MCP_READ_ONLY=true` disables sending for every client. To remove
only one client's permission, use `revoke-send CLIENT`, apply the updated policy
and restart. Clients without an explicit send grant retain read-only access even
when sending is enabled for others. Omitting `tools` grants only read tools.

## Storage and verification

`outbound.db` lives beside `messages.db` on the persistent volume. It stores client
names, hashes of request IDs and recipient/text, status, creation time, and
returned message IDs with their initial archive status. Schema upgrades add these
reference fields without dropping existing attempts. It does not store message text, recipient addresses, keys or raw bridge errors.
Records are not automatically pruned. Keep this file in consistent online backups
and worker transfers. Deleting it or restoring an older snapshot loses duplicate
protection for attempts absent from that snapshot.

Verify discovery, reader denial, write permission and retry behavior with the
synthetic checks in `tests/verify_sending_sdk.py`. An actual WhatsApp send is a
separate check using a recipient and text authorized by the user. Automated tests
mock the bridge and never send WhatsApp messages.
