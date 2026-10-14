# WhatsApp MCP

A Model Context Protocol (MCP) server for searching WhatsApp conversations,
reading message context, retrieving attachments, and optionally sending text.
It connects to a linked WhatsApp account and maintains a searchable archive.
Reading is enabled by default. Sending requires an explicit client permission
and service setting.

## How it works

A Go bridge built on Whatsmeow connects as a WhatsApp linked device. It saves
messages and contact identities in SQLite. A Python MCP server exposes tools
through authenticated Streamable HTTP.

The Docker worker can serve a local MCP client, use Tailscale, or connect through
an optional OpenAI Secure MCP Tunnel. Local Docker Compose access is bound to
`127.0.0.1:8000` on the host. The bridge and pairing endpoint remain internal;
every MCP route checks client keys and tool permissions.
See [local Docker setup](docs/LOCAL_DOCKER.md), [installation](docs/INSTALLATION.md),
[Railway deployment](RAILWAY.md), and [client authentication](CLIENT_AUTH.md).

## Tools

| Tool | Purpose |
| --- | --- |
| `search_contacts` | Find contacts by name or phone number. |
| `list_chats` | List and filter conversations. |
| `get_chat` | Read conversation metadata. |
| `get_direct_chat_by_contact` | Find a direct conversation by phone number. |
| `get_contact_chats` | Find conversations involving a contact. |
| `get_last_interaction` | Read the latest stored interaction with a contact. |
| `list_messages` | Search messages and attachments by text, conversation, sender, date, or media type. |
| `get_message_context` | Read messages around a specific message. |
| `get_message_identity` | Inspect retained source identifiers and known aliases. |
| `download_media` | Retrieve available images, audio, videos, and documents. |
| `get_sync_status` | Inspect connection, archive, ingestion, and history recovery status. |
| `send_message` | Send text with explicit permission and a persistent request ID. |

`send_message` is available when read-only mode is disabled; each caller still
needs an explicit grant. File and voice-note sending are not exposed through
MCP. See [text sending](docs/SENDING.md) for setup and outcome handling.

Search supports substring matching and ranked full-text matching. It searches
the stored archive, rather than requesting a new import for every query.
Structured results include message references, context, pagination, and coverage
information. See the [technical reference](docs/TECHNICAL_REFERENCE.md) for tool
parameters and retrieval behavior.

## History and synchronization

- **Initial synchronization:** WhatsApp supplies available history when a new
  device is linked.
- **Backfill:** the bridge requests older messages using stored anchors and
  persistent per-conversation checkpoints.
- **Live synchronization:** the connected bridge saves new messages while
  history recovery continues independently.

The setup helper requires a history period: a positive number of days or `0`
for all available history. Positive backfill targets are fixed when the run is
created; restarts resume saved progress. Changing the target requires a new run
ID. Previously imported messages are retained.

WhatsApp controls which messages it delivers. A finished transfer, quiet queue,
or completed backfill does not establish complete history. Compare important
missing messages against the phone. See [initial synchronization](docs/INITIAL_HISTORY_SYNC.md)
and [history coverage](DATA_COMPLETENESS.md).

## Storage and availability

The archive and linked-device session live on the server's persistent storage.
Media is downloaded on demand. Missing or expired files may be unavailable;
automatic transcription is not included.

Stored messages remain searchable during a WhatsApp disconnect while the MCP
server runs. Stopping the worker pauses access and new-message ingestion.
Restarting with the same storage preserves the archive and session; recovery of
messages missed during an outage is not guaranteed.

Each installation uses its own account, keys, and storage. When a client invokes
a tool, the returned messages or media are available to that client and its AI
provider. Client grants cover tools across the stored archive, rather than
individual conversations. Keep credentials and database backups private.

## Documentation

- [Local Docker setup](docs/LOCAL_DOCKER.md): run without Railway or OpenAI credentials.
- [Installation](docs/INSTALLATION.md): prerequisites, configuration, pairing, and verification.
- [Railway deployment](RAILWAY.md): worker settings, persistent storage, backups, and recovery.
- [Client authentication](CLIENT_AUTH.md): keys, permissions, and private connection routes.
- [Text sending](docs/SENDING.md): activation, request IDs, and uncertain outcomes.
- [Initial synchronization](docs/INITIAL_HISTORY_SYNC.md) and [history coverage](DATA_COMPLETENESS.md).
- [Message identities](MESSAGE_IDENTITY.md): aliases, retained sources, and identity limitations.
- [Technical reference](docs/TECHNICAL_REFERENCE.md): retrieval, ingestion, diagnostics, and configuration.
- [Contributing guidance](AGENTS.md): repository conventions and verification commands.

## License and upstream projects

This project derives from [Luke Harries's WhatsApp MCP](https://github.com/lharries/whatsapp-mcp)
and retains its [MIT license](LICENSE). The Go bridge uses
[Whatsmeow](https://github.com/tulir/whatsmeow), licensed under MPL-2.0.
Dependency licenses remain applicable to their respective components.

This is a third-party integration with WhatsApp's linked-device protocol, not
the official WhatsApp Business Platform API. It is not affiliated with or
endorsed by WhatsApp or Meta. Protocol changes can affect compatibility.

Report vulnerabilities privately using the [security policy](SECURITY.md).
See [publication preparation](docs/PUBLISHING.md) for exporting source separately
from private repository history and runtime data.
