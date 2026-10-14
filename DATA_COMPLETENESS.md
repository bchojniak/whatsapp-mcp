# Archive completeness

The target is all individual and group chats still visible on the primary phone,
including archived chats, at a recorded comparison date. There is no fixed
starting date. Channels and Status updates are outside this target.

Messages, captions, replies and other visible message types must be retained with
their chat, sender and timestamp. Media completeness means retaining every media
message and the metadata WhatsApp supplies. Original files remain downloadable on
demand; this project does not automatically archive all media binaries.

## Capture and access

The installation flow enables expanded history at fresh pairing, using the selected
backfill period as a protocol preference. This does not guarantee that period's
delivery or filter out older messages. Restarting a saved session does not repeat
the registration-time request. See
[initial-history setup and references](docs/INITIAL_HISTORY_SYNC.md).

Initial history notifications are retained before download so admitted pending
work can resume sequentially after a restart. Adaptive backfill startup observes
initial sync activity and waits for pending download/storage work to finish.
Its maximum wait bounds silence detection, not unfinished work. Per-phase
protocol progress, including 100%, is not account-wide completeness.

The ingestion journal persists incoming events before acknowledgement. A message
and its source metadata commit in the same archive transaction before a history
cursor advances. `message_metadata` retains the original protobuf and its format,
including wrappers, unknown fields, quoted context and supplied media attributes.
Message replays preserve stored IDs and downloaded filenames.

Images, videos, audio, documents, stickers and sticker packs are indexed as media.
GIFs and video notes remain videos; voice notes remain audio. Their subtype is
available in structured results and can be used as a `list_messages` filter.
Wrapped and business messages can contain media too. The complete source payload
retains nested attachments; `download_media` retrieves the first attachment.
Message types without text or a primary file receive an explicit type marker and
retain their original payload instead of being discarded.

Structured `list_messages`, `get_message_context` and `download_media` with
`metadata_only=true` expose bounded metadata. They never return download keys,
signed paths or original source payloads. Metadata reads do not download files.
Requested file contents have an 8 MiB inline return limit.

## Recovery

Continuous recovery uses the paired worker and durable report. Temporary
chat failures have three retries without progress; phone-wide failures wait with
backoff. Never-requested chats retain priority. The worker has no 24-hour stop.

A saved report whose `capture_version` is below the supported version can receive
a separate metadata replay cursor. It revisits only the range already requested,
while preserving the primary history cursor, counters, snapshot and start time.
Replay waits until the active primary pass has finished and has its own retry
budget and checkpoint.
`captured_requested_range` describes that replay range, not lifetime completeness.

## Verification

`get_sync_status` reports stored media categories, captured message types and rows
with or without original source metadata. These are logical-message counts after verified alias resolution;
`raw_source_rows` and `duplicate_alias_rows` report retained physical copies. Older rows without source metadata remain explicit
capture gaps; they are not silently marked complete.

Lifetime completeness requires comparison against a reliable phone-derived
reference, such as a sufficiently complete export or accessible backup. Neither
a healthy write queue nor a phone's end-of-history flag establishes this.
Exports can have limits; their coverage must itself be checked. If the phone shows
messages older than the archive's boundary, that is a known gap.

`WHATSAPP_HISTORY_BACKFILL_DAYS` selects the requested period, from `1` to `3650`
days; `0` or an absent value means no application cutoff. For a positive value,
the report freezes `target_after` relative to the run's start time.
`requested_window_reached` means the request cursor reached that date, not that every message in the window is
present. Phone completion, no anchor and failed requests remain separate outcomes.
Received batches can include older messages; the window stops further requests
without deleting data WhatsApp already supplied. A different target requires a
new run ID; restarting the same run retains its saved cutoff.

File retrieval success is separate from metadata completeness. An expired link,
server download failure or missing original file does not remove its message
record. A server failure alone does not prove the file is unavailable on the phone.

Additive row and metadata version tables preserve observed edits, replay variants,
media-reference refreshes, and original identifiers. They cannot reconstruct
data overwritten before source retention began. See
[identity/rollback guidance](MESSAGE_IDENTITY.md).

