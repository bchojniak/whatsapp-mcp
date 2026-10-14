# Technical reference

Detailed retrieval, ingestion, identity, diagnostic and recovery contracts.
For setup, see the [installation guide](INSTALLATION.md).

## Capabilities and invariants

- Local, Tailscale, and optional OpenAI tunnel clients use explicit client keys.
  Credentials are hashed, tool permissions are per client, and audit records
  omit private content. See [client access setup](../CLIENT_AUTH.md).
- Optional previews in `list_chats` and `get_chat` select one latest message
  deterministically when timestamps tie.
- Database errors are explicit. Message storage opens read-only so a missing
  path cannot silently create a new empty database.
- Exact phone/LID aliases resolve through whatsmeow contact metadata. Contact
  search supports accents. Never present an unmapped LID as a phone number or mix
  names from multiple accounts without explicit account selection.
- Media retrieval is independent of write tools. Requested media matches its
  stored message/chat, returned file paths are constrained, and inline data is
  capped at 8 MiB. Images return as MCP image blocks; other files return as resources.
- `get_sync_status` reports bridge reachability, explicit connection/QR flags when
  available, message/chat counts, latest stored message, and identity-store status.
  A latest-message timestamp is not a heartbeat. Missing sync/connection information
  is reported as unknown rather than invented.
- Bridge HTTP calls have timeouts; sending is disabled by default.

## Tests

Run `python -m unittest discover -s tests -v` from the repository root. Tests use real
temporary SQLite databases and mock external HTTP. Media tests additionally stub
the MCP SDK when it is not installed; these are not a production transport test.

## Agent retrieval

The server exposes eleven read tools and optional, explicitly granted
[text sending](SENDING.md). `get_message_identity` enumerates original
source identifiers; each client policy controls which tools can execute. For agent
workflows, call `list_messages(response_format="structured")`: results separate
matches from deduplicated context, include exact `message_id`/`chat_jid` references,
and expose `has_more`, `next_cursor`, text truncation and stored-history coverage.
Copy both references into `get_message_context` or `download_media`. Context lookup
without a chat rejects ambiguous IDs. Equal timestamps use a stable ID ordering;
this does not assert an original send order within the same second.

Use `query_mode="full_text"` for ranked, accent-insensitive word and filename
search. All supplied words must match, in any order; this is not semantic search
or transcription/OCR. The defaults are `query_mode="substring"` and a text response.
Chat search also uses enriched, accent-insensitive contact names; chat filters expand exact known phone/LID aliases without rewriting
stored identities. Physical alias rows are retained rather than silently merged.

Structured pagination uses a snapshot and exact continuation key. Keep filters
unchanged, pass the returned cursor with `page=0`, and restart on an actionable
archive-change error. Chronological pages tolerate newly inserted messages;
ranked pages restart if the corpus changes, because relevance scores can change.
Changes to existing rows also invalidate pagination. Message date filters compare
actual instants, including timezone offsets, with exclusive boundaries.

`media_type="any"` or a specific supported type finds attachments. Structured hits
include file names and known sizes. `download_media(metadata_only=true)` returns
details without fetching bytes; known files over the 8 MiB inline limit are rejected
before download. Availability remains unconfirmed until retrieval succeeds.

`get_sync_status(chat_jid=...)` exposes stored date ranges and saved per-chat
backfill outcomes. An empty result, missing anchor, timeout or phone-reported end
does not establish complete lifetime history. The bridge health response
reports actual connection/pairing flags. Ingestion observations persist across
restarts; a stored message timestamp still is not a heartbeat.

Startup initializes transactional, idempotent query indexes and an external-content FTS5
index with insert/update/delete triggers. Archive rows are indexed once;
later events update the index in the same SQLite transaction. Production builds
enable `sqlite_fts5`; verify with `go test -tags sqlite_fts5 ./...`. Plain Go builds
without this tag can initialize a fresh basic archive but do not provide FTS5.
Once the archive has FTS5 triggers, keep using an FTS5-capable bridge image: rolling
back to an older binary without that SQLite capability requires first removing the
derived FTS triggers/index using a compatible operator process. Do not roll back
the message or WhatsApp session database to an older snapshot.

Live/history replays use explicit upserts, preserving row identity, existing media
filenames and metadata when a replay lacks those values. Delayed messages cannot
move a chat's last-message time backward.

## Reliable ingestion and diagnostics

The optional initial-history profile and backfill handoff are described in
[initial history sync](INITIAL_HISTORY_SYNC.md). Initial history notifications
are durably admitted before download and replayed sequentially through the
archive writer. Pending downloads or unresolved storage work block the
adaptive handoff even after its maximum wait; a timer never establishes coverage.

Incoming live messages and provider history batches first commit to a separate
`/data/ingestion.db` journal on the persistent volume. A single writer saves each
batch's chats and messages atomically to `messages.db`, then removes its pending
payload. A crash between those commits replays the batch using existing message
keys, preserving original media metadata. On-demand history inserts missing rows
without overwriting stored records. Backfill responses reach the checkpoint worker
only after archive and queue completion.

Archive lock, I/O, full-disk, read-only and write-timeout failures retry in queue
order with delays increasing from 1 to 30 seconds. Retries continue while the
failure is temporary; permanent errors retain a failed batch without blocking
subsequent batches. Failed batches require operator investigation and repair.
Shutdown stops the writer and leaves pending work for the next startup. The bridge
reports handler success to whatsmeow only after durable admission; library retry
behavior depends on event type and is not a promise of provider redelivery.

`get_sync_status` includes an `ingestion` object with pending and failed batch counts,
queue age/bytes, accepted and committed entry counts, write failures and recovery,
unpersisted-event evidence, connection transitions, and clean/unclean restart
counts. `observation_scope="persistent"` and received/committed timestamps cover
this journal's lifetime. Connection history retains the latest 100 transitions;
counters persist. Entry counts include history entries and replays, not unique
messages. A successful empty batch does not advance the committed-message time.

Ingestion is `healthy` with no unresolved work, `buffering` while recent work is
pending, and `degraded` for a queue at least 60 seconds old, failed batches,
unpersisted events, or unavailable diagnostics. Silence alone is not a failure.
These are ingestion states, separate from the actual WhatsApp connection flag and
from history completeness. Diagnostics contain no message bodies, media keys,
chat identifiers or raw database error text. Client authentication and tool
permissions apply to these diagnostic reads.

The pending payload limit is 256 MiB / 100,000 batches, with a 64 MiB limit per
encoded batch. Admission failures are explicit and counted; accepted work is not
evicted. Journal files use private permissions and SQLite WAL with synchronous
FULL. Completed payloads are logically deleted, not forensically erased from
SQLite pages. Include the journal and its WAL in consistent backup/recovery plans.
The journal shares the persistent volume: volume loss or inability to write it
cannot be recovered by this queue. During a storage outage diagnostics may exist
only in memory/logs until persistence recovers; a crash during that interval can
lose those observations. This is not a guarantee of complete WhatsApp delivery.

Deployment and rollback require an FTS5-capable build and a compatible ingestion
journal reader. Preserve `ingestion.db`; drain or recover its pending batches
before switching to a build that cannot replay them.

## Code layout

The Go bridge is a single package with separate files for startup (`main.go`),
storage (`store.go`), message synchronization (`sync.go`), media (`media.go`), and
the internal HTTP API (`api.go`). Build or run the whole package, using
`go build .` or `go run .` inside `whatsapp-bridge`.

Python tool definitions live in `main.py`; query logic and shared database
connection/row handling live in `whatsapp.py`. `identity.py` resolves contacts,
`access_control.py` authenticates clients, and `security.py` bounds tool results.
See [coding guidance](../AGENTS.md) for conventions and verification commands.

## Configuration

- `WHATSAPP_MESSAGES_DB_PATH`: bridge message database path.
- `WHATSAPP_SESSION_DB_PATH`: optional; defaults to `whatsapp.db` beside the message
  database. Only contact and LID metadata is queried, in read-only mode.
- `WHATSAPP_ACCOUNT_JID`: required for identity enrichment if the contact store
  contains multiple accounts.
- `WHATSAPP_MEDIA_ROOT`: directory containing downloaded media; defaults to `/data`.
- `WHATSAPP_MCP_READ_ONLY`: defaults to true; media retrieval is still available.
- Tunnel, Tailscale, transport and bridge settings are described in
  [Railway deployment](../RAILWAY.md) and [client access](../CLIENT_AUTH.md).
- `WHATSAPP_HISTORY_DIAGNOSTIC_RUN_ID`: optional, bounded diagnostic experiment.
  It reads a **finished** report selected by `WHATSAPP_HISTORY_BACKFILL_RUN_ID`
  without modifying its checkpoints. It selects three distinct timed-out chats,
  resolving known phone/LID aliases, and compares each saved failed boundary with
  a recent stored anchor. A previously responsive chat is checked at the start
  and end. At most eight peer requests of 50 messages are sent, one at a time,
  with 15 seconds between requests, a 30-second send timeout and a 60-second
  response/storage wait. If the control fails, storage remains pending or
  diagnostics cannot be persisted, the run stops. It does not start a broad sweep.
  Protocol notification observations, decoded replies, date ranges, completion
  flags and actual committed insert/duplicate counts remain in a private,
  fsynced report at `/data/history-diagnostics/<run-id>/report.json`. No message
  bodies or media secrets are copied. Health and `get_sync_status` expose bounded
  labels, dates and counts, without raw chat IDs, anchors or protocol IDs.
  Decoded `HistorySync` events do not contain request IDs: replies are correlated
  by known chat aliases and arrival time, so a late reply may belong to an earlier
  attempt. Any protocol-ID matches are recorded separately. Counts are physical
  rows/entries, not unique logical messages across aliases. The first 128 decoded
  responses and 128 protocol observations are retained, including late arrivals
  after completion; overflow is counted. Interrupted or completed runs do not
  repeat automatically, and another experiment requires a new explicit run ID.
  The ingestion queue preserves the diagnostic response reference across replay;
  commits/replays are counted separately. A crash between archive commit and
  trace persistence can undercount inserts, so diagnostics are evidence rather
  than a transactional audit of all delivery. Completed files remain on the
  Railway volume. Removing this variable disables tracing without deleting them.
- `WHATSAPP_HISTORY_DIAGNOSTIC_PREVIOUS_RUN_ID`: optional comparison with a
  completed diagnostic run. With a **new** diagnostic run ID, this selects up to
  three acknowledged, timed-out boundary attempts from that earlier experiment,
  uses the same verified anchor and a different known chat alias, and finishes
  with a control check. It sends at most four requests. The prior closing control
  must have responded within 20 minutes, and alias-comparison runs cannot chain.
  Original reports stay intact. This helps distinguish an unresponsive LID/phone
  chat address from unavailable message history without guessing identities or
  starting another broad retry. `chat_type` and `experiment` expose the comparison
  in diagnostics without revealing addresses.
- `WHATSAPP_HISTORY_BACKFILL_RUN_ID`: optional, explicit one-off history sweep.
  Each unique ID runs once, with a SQLite snapshot and private checkpoints under
  `/data/backfill/<run-id>/`. Completed or blocked runs do not repeat on restart.
  It requests 50 messages at a time from the primary phone, walks backward from
  each known chat's newest stored message, and inserts missing messages without
  overwriting existing rows or downloaded media metadata. This covers gaps within
  the stored date range as well as older history. Chats without a stored message
  cannot be requested through this protocol. The report distinguishes an explicit
  phone response that no older messages remain from access restrictions, failed
  requests, timeouts and batches that make no progress; these are not treated as
  proof of complete history. After three consecutive request failures/timeouts,
  it checks a previously responsive chat before stopping the sweep. Explicit empty
  or unmatched phone responses remain unconfirmed and do not look like timeouts.
  A new `WHATSAPP_HISTORY_BACKFILL_RESUME_ID` selects targeted retries from saved
  cursors: failed, timed-out, interrupted and unmatched requests, along with
  ongoing backward progress. It discovers newly available message anchors and
  prioritizes chats never requested before. Confirmed results, empty and
  non-progressing replies, access restrictions, counters and request limits stay
  intact. A chat without a message anchor is recorded but not requested. A saved
  pending entry without a recorded request result is deferred as unconfirmed.
  Checkpoints retain each request's result, and aggregate logs show queue reasons.
  Each resume ID is saved before requests and consumed once; ordinary restarts
  do not repeat a finished or blocked sweep. The one-off worker has a 24-hour
  runtime and 10,000-request limit per chat. Media files are
  still downloaded on demand. Aggregate `BACKFILL` logs omit chat IDs and content.
- `WHATSAPP_HISTORY_BACKFILL_DAYS`: optional, `0` or absent means no cutoff.
  Positive values freeze a target relative to the run's start time. They also
  supply the requested days at fresh pairing when the full-sync profile is enabled.
  A different target requires a new run ID; received older data is retained.
- `WHATSAPP_HISTORY_INITIAL_SYNC_FULL`: optional, defaults to `false`. On a fresh
  pairing, requests full sync and, for positive backfill days, sets the requested
  full-sync days. Existing sessions and upstream capabilities/quotas are preserved.
- `WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS`: optional, defaults to `0`
  (fixed startup delay). A positive value enables the adaptive initial-sync wait
  before backfill, capped at that duration unless downloads or storage remain
  unresolved. New installations use `600`.
- `WHATSAPP_HISTORY_BACKFILL_START_DELAY_SECONDS`: optional, defaults to `0`.
  With adaptive waiting enabled this is the initial-history quiet period;
  otherwise it remains the fixed startup delay. New installations use `90`.
- `WHATSAPP_HISTORY_BACKFILL_CONTINUOUS`: optional, defaults to `false`. With a
  backfill run ID and no diagnostic run, enables a persistent recovery worker
  without the one-off 24-hour deadline. It keeps the same snapshot and cursors,
  resumes after disconnects and phone pauses, and waits five minutes before
  retrying temporary failures. Repeated phone-wide pauses back off to ten,
  twenty, then thirty minutes. Waiting times survive restarts. A chat receives
  up to three automatic retries without backward progress; progress resets that
  budget. Exhausted retries stay `retry_limit_reached_unconfirmed`. Empty or
  non-progressing responses, missing anchors, unsupported formats and access
  restrictions require another data source rather than repeated requests.
  When no work is eligible, it checks hourly for new message anchors, or every
  minute in a bounded recent-history run while initial sync may still be arriving. Storage or
  checkpoint failures stop recovery. `get_sync_status` exposes the recovery
  state and next attempt time without private identifiers, and continues to
  report `history_complete: null`. This mode retrieves supported message and
  media records; it does not create a complete media-file archive.
- `WHATSAPP_MCP_CLIENTS`: required JSON registry of named client key digests.
- `WHATSAPP_MCP_TUNNEL_KEY`: enabled client key required when the OpenAI tunnel is enabled.
- `WHATSAPP_MCP_TUNNEL_ENABLED`: explicit `true`/`false`; unset detects tunnel configuration. Every mode still requires a valid client policy.
- The server entrypoint requires Streamable HTTP; see
  [client access](../CLIENT_AUTH.md).

## Build and runtime

The root Dockerfile builds the Go package with `sqlite_fts5`, installs the locked
Python dependencies and starts the private worker. It copies all runtime Go files.
Keep the build context at the repository root; generated data and credentials
are excluded. `railway.json` supplies the build and restart settings.

The bridge omits `/api/send` in read-only mode and keeps media retrieval available.
The MCP HTTP boundary checks client keys on local, Tailscale, and optional OpenAI tunnel paths.
Health observations are not a periodic heartbeat or proof of complete history.

## History recovery with linked identifiers

History requests use only conversation identifiers linked by WhatsApp's session
store. A verified diagnostic reply records its working request identifier in
`messages.db` (`history_request_preferences`). The bridge checks the current
mapping before reusing that preference. Unknown identifiers keep their existing
request address; the bridge does not guess a phone number.

Backfill scheduling groups linked records and accepts responses under either
known identifier. Each group has one active checkpoint. Original physical
records remain available for inspection. Resumes use the older stored boundary
or an already recorded older provider checkpoint. Ordinary new sweeps retain
their sweep from the newest anchor to check gaps inside the stored date range.

`get_sync_status` and chat coverage report logical message counts, physical source
counts and conversation outcomes. Chat coverage shows duplicate alias rows and
the current verified request preference. A successful request or a phone
completion flag does not establish lifetime completeness.

For a bounded recovery trial, set a new `WHATSAPP_HISTORY_DIAGNOSTIC_RUN_ID`,
`WHATSAPP_HISTORY_DIAGNOSTIC_MODE=oldest_boundary` and
`WHATSAPP_HISTORY_DIAGNOSTIC_PREVIOUS_RUN_ID` to a finished identifier comparison.
The source report must contain a matching phone protocol notification and a
successfully stored reply for each selected request. The trial uses a fresh
control request, the oldest stored message across each of at most three linked
conversations, and a closing control request: at most five requests in total.
It reports before/after coverage and previously absent message IDs older than
the requested boundary. New chat activity and copies under another known
identifier do not count as older history recovery. Counts describe archive
changes during each request window; decoded history replies still lack request
IDs. Uncertain sends are not repeated after a restart. Reusing the run ID does
not start another trial. Original broad-backfill reports remain unchanged.

## Logical message identity and retained sources

Verified phone-number/LID copies appear once in search, context, chat previews,
and coverage, while distinct IDs and conflicting author/direction/timestamp
identities remain separate. Original rows and observed payload/row versions stay
preserved. Structured results expose identity/source locators and variant counts.
Use `get_message_identity` pages for all retained identifiers and safe history;
`media.source_reference` identifies the actual selected media variant.

Logical IDs depend on verified mapping evidence; original message/chat references
remain the durable external locators. See [MESSAGE_IDENTITY.md](../MESSAGE_IDENTITY.md)
for identity rules, limits, backup, staging, and rollback. This does not recover
missing phone history or content overwritten before retention was installed.
