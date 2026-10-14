# Logical message identity and retained sources

The read layer shows one logical message for verified copies across identifiers,
while preserving physical archive rows and their retained source versions.
Downloaded files and original message/chat references stay available. Logical
deduplication does not establish complete phone history.

## Identity policy

The read layer creates indexed, connection-local SQLite TEMP membership tables
and views. `logical_messages` exposes one representative per logical identity;
`logical_message_sources` retains every physical row locator in that identity.
They work with a read-only archive connection and disappear when it closes.

Rows can share an identity only when all of the following agree:

- A nonempty WhatsApp message ID
- The canonical conversation, using only an unambiguous protocol LID/phone map
- The canonical sender, using the same verified mapping
- Message direction
- The exact timestamp instant, including fractional precision

Equivalent timestamp offsets describe the same instant. There is no time-window
or content-similarity matching. Missing identity evidence, conflicting mappings,
different IDs, different conversations, different senders, opposite directions,
and different timestamps remain separate. Groups are never linked to direct
chats. Contact names and similar text are not evidence of message identity.

Content is deliberately outside the identity key. Two aliases of the same
message may contain different text or media fields. Those source rows remain
available, their content variants are counted, and the earliest physical row is
the stable representative. Only missing representative text, media type, filename,
or file length can be filled from an existing source row; nonempty conflicting
values are not silently replaced.

Search, pagination, context, previews, and coverage use this logical projection.
Filtering can find a matching alias source while still returning its logical
message once. Structured identity information exposes bounded source locators,
content-variant evidence, and retained-version references. It never exposes raw
protobufs, media keys, signed download paths, or provenance JSON.

Identity hashes are policy- and mapping-dependent, not permanent protocol IDs.
Raw `(message_id, chat_jid)` references identify physical sources. An ambiguous raw
message ID still requires a conversation, and conflicting identities within an
alias family remain distinct. Consumers must not deduplicate on message ID alone.

## What the counts mean

Archive and chat `stored_messages` count logical messages. The chat field
`stored_unique_message_ids` also counts conservative logical identities, including
distinct collisions and rows lacking usable IDs. It does not mean
`COUNT(DISTINCT id)`.

- `raw_source_rows`: physical rows backing the selected logical messages
- `duplicate_alias_rows`: `raw_source_rows - stored_messages`
- `metadata_coverage.counting_unit`: `logical_messages`
- Metadata fields ending in `_rows`: logical-message counts

Media categories use the same visible logical message fields. A message has
captured source metadata when at least one of its associated physical rows has a
`message_metadata` record. Captured message types count each logical message only
once. Public message metadata and type counts prefer the representative's metadata,
then the newest observed alias metadata, with source row order as the deterministic
tie-break. Public media metadata uses only the explicitly selected physical media
source so its subtype, duration, and MIME type stay paired with its download
reference. When that exact physical source has no captured metadata, media metadata
reports `not_captured`; it never borrows another alias's MIME type or duration.
Retained versions are provenance evidence, not extra visible messages or extra
metadata-covered messages.

The projection preserves a retrieval request's prepared snapshot. Later arrivals
do not change an already prepared page's membership. Changes to verified identity
mapping or an incompatible archive update can invalidate continuation cursors;
restart the search rather than assuming a stale cursor remains meaningful.

`history_complete` and `phone_history_complete` remain unknown. A deduplicated
count, healthy write queue, completed replay, or phone end-of-history flag cannot
establish lifetime completeness, absence of gaps, or media file availability.
Older rows without source metadata remain explicit capture gaps. See
[DATA_COMPLETENESS.md](DATA_COMPLETENESS.md) for the phone-comparison requirement.

## Additive source retention

Bridge initialization creates `message_metadata_versions`, `message_row_versions`
and their indexes through an idempotent, transactional schema migration. The metadata
index uses `(message_id, chat_jid, version_id)`. Each retained metadata-version
record includes source metadata, the captured protobuf and format, observation time,
source kind, and source provenance. Schema initialization preserves message primary
keys and rowids and copies stored protobuf blobs byte-for-byte. Event captures
deterministically serialize WhatsMeow's decoded protobuf, including unknown fields;
they are not claimed to be original encrypted network frames.

The migration takes a SQLite writer reservation before checking schema state,
snapshots current `message_metadata` records as baseline evidence, and installs
six compatibility triggers atomically:

- `message_metadata_retain_insert`
- `message_metadata_retain_before_update`
- `message_metadata_retain_after_update`
- `message_rows_retain_insert`
- `message_rows_retain_before_update`
- `message_rows_retain_after_update`

`message_row_versions` snapshots every normalized `messages` column, the physical
`message_rowid`, and a `captured_at` snapshot time, including rows without
source metadata. NULL and empty values remain distinguishable. Changed text,
sender, timestamp, direction, media references, and filenames are retained even
when a raw `(id, chat_jid)` is reused. The current logical projection still reads
current physical rows; retained old rows are evidence, not additional current
visible messages.

Retention triggers apply to any writer using the base message and metadata tables.
Bridge ingestion records live-event identifiers or history synchronization
provenance in the same transaction as message storage. Per-entry history keys
retain message ID, remote JID, and participant JID; remote and conversation JIDs
can differ. Queued events retain their original observation time when retried.
Metadata `observed_at` is the recorded event capture time when available, otherwise
that retained version's archive observation. Baseline snapshots preserve the
metadata projection's recorded observation, which can describe a later replay
rather than the message's first retrieval. Row `captured_at`
records snapshot time. A distinct combination
of raw message locator, source payload, metadata, type, format, source kind, or
provenance is retained; replay differing only in observation time does not append
another copy. This is append-only behavior of normal write paths, not tamper-proof
storage.

Versions are scoped to a raw locator's history, which can include edits or key
reuse. Their presence does not prove that all versions represent one unchanged
logical event. Payloads overwritten before retention began cannot be reconstructed
from a later snapshot. Session data and retained payloads remain private archive
material and must not be added to test fixtures or published.

## Staging validation

Validate identity and retention with generated fixtures:

```sh
python -m unittest discover -s tests -v
python -m unittest discover -s whatsapp-mcp-server/tests -v
(cd whatsapp-bridge && go test ./... && go vet ./...)
git diff --check
```

With the project's environment installed, also run the access and SDK
checks documented in [AGENTS.md](AGENTS.md). Validate the Docker build if the
runtime source list or layout changes.

For staging with an archive snapshot:

1. Make a consistent SQLite online backup to an access-restricted destination.
   Use Python's `sqlite3.Connection.backup`, SQLite's `.backup`, or an equivalent
   SQLite backup API. Do not copy a live `messages.db` file while ignoring its WAL;
   do not copy database/WAL/SHM files independently and assume they form a snapshot.
2. Back up the identity/session database separately if mapping evidence is needed.
   Two independent database backups are not one cross-database atomic snapshot;
   record their times and account for mapping changes between them. Never start a
   second paired WhatsApp worker from a staging copy of session credentials.
3. Record raw message count, metadata count, raw primary-key/rowid mapping, and
   existing content/filename checksums in private staging evidence. Run
   `PRAGMA integrity_check` and `PRAGMA foreign_key_check` on the backup. Investigate
   existing failures before attributing them to a migration.
4. Exercise initialization twice on an isolated staging database. Verify unchanged
   original rowids/keys/content/filenames, preserved current source bytes, no lost
   metadata versions, and no duplicate versions on an identical replay. Test
   updates through bridge ingestion and direct writes to the base metadata table.
5. Compare global, chat-scoped, media, full-text, paginated, and context results
   against fixture expectations. Confirm aliases appear once, repeated text under
   different IDs remains separate, conflicting sender/direction/time evidence is
   not collapsed, and all raw source references can still be enumerated.
6. Check that logical counts reconcile with raw source counts, metadata gaps remain
   visible, and public responses contain no raw payloads or download secrets.
7. Exercise rollback in staging while leaving additive storage in place. Verify
   an older reader/writer still works and retained versions survive subsequent
   writes. Measure TEMP projection time and storage against the actual archive
   size before choosing a production maintenance window.

The online-backup step can be performed without raw file copying. For example,
with absolute, operator-supplied paths and a new destination filename:

```python
import sqlite3
from pathlib import Path

source = Path("/absolute/source/messages.db")
destination = Path("/restricted/backup/messages-snapshot.db")
if destination.exists():
    raise FileExistsError(destination)
destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
destination.touch(mode=0o600, exist_ok=False)
with sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True) as reader:
    with sqlite3.connect(destination) as backup:
        reader.backup(backup, pages=256, sleep=0.1)
        assert backup.execute("PRAGMA integrity_check").fetchone() == ("ok",)
```

Use a trusted private directory, verify its actual permissions, and protect backup
files like the live database. A successful backup is a point-in-time recovery
artifact; it does not include ingestion that happens after its snapshot.

## Deployment and live-ingest safety

Before deployment, record the build/configuration and take consistent online
backups. Preserve the ingestion journal, media directory, saved history cursors
and session. Run one paired writer against the archive.

Schema initialization and retention triggers commit atomically. Writes serialize
under SQLite locking, while readers use the base schema and their connection-local
identity projection. Budget for initialization on a large archive: the baseline
copy of message rows and metadata, retained versions, and transaction journal/WAL
need free disk space. Monitor lock errors, queue backlog and failed transactions.

After startup, verify representative selection, logical versus physical counts,
pagination, source metadata, client authorization and reader error handling. Check
that live/queued ingestion commits and saved history checkpoints advance normally.
A failed schema initialization or queued event must be investigated without
truncating queues, deleting alias rows or moving cursors past uncommitted data.

Use SQLite online backups rather than copying live files or force-killing a writer.
Long reader snapshots can delay rollback-journal writers or retain WAL pages; keep
reader connections short-lived and monitor database/WAL growth.

## Rollback without data loss

TEMP logical views are connection-local and require no persistent schema undo
when switching readers. Keep both version tables, their indexes and all six
retention triggers when rolling back the bridge binary; the triggers work with
base message and metadata inserts and updates. Choose an FTS5-capable build that
can replay the ingestion journal, or drain/recover pending work before changing
builds. Do not drop source retention storage during an application rollback.

Do not restore an earlier backup over a database that has received newer live
messages. That would discard post-backup ingestion. Preserve the current database
and its journal, diagnose from an online copy, and plan any repair/reconciliation
separately. Keep backups until the retention, reader behavior, and live-ingest
checks pass. Source retention is not automatically pruned during recovery.

## Known limits

- Verified mappings can be absent, ambiguous, stale, or later corrected; the safe
  outcome is separate logical messages until evidence supports a link
- Exact stored timestamps can disagree across source records, intentionally leaving
  duplicates unresolved; no fuzzy matching is attempted
- A protocol ID reused with otherwise identical identity evidence cannot be
  distinguished from an alias copy using these stored fields alone
- The `messages` primary key permits one current row per raw `(id, chat_jid)`;
  version tables preserve observed changes, but the current projection does not
  show overwritten versions as separate current rows or recover content lost
  before retention began
- Content-variant counts describe current physical message rows; metadata-version
  counts describe retained raw-locator history, and the two are not interchangeable
- The per-connection projection scans stored identity fields and consumes TEMP
  storage; validate cost on large archives and keep reader connections short-lived
- Logical deduplication does not fill historical gaps, retrieve missing media, or
  establish completeness against the primary phone

## Sent time is not first retrieval time

The message `timestamp` is the provider's sent time. Retained source `observed_at`
values describe recorded observations of distinct versions. They do not establish
when the message first entered this archive before retention instrumentation.
Baseline snapshots must not be treated as a historical first-seen date.

`first_retained_row_snapshot_at` and `latest_retained_row_change_at` bound retained
row snapshots since instrumentation, including the initial migration snapshot;
they are not first retrieval or last sighting. Identical repeated sightings are
not appended, so `observation_times.first_retrieved_at` and `last_seen_at` remain
explicitly unknown. Retained-version timestamps are preserved on replay.

