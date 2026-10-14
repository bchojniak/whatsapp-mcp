package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// The version archive is additive: the metadata table holds the current
// projection, and neither physical messages nor downloaded filenames are moved.
// Exact byte comparisons avoid hash collisions and do not index large payloads.
// SQLite serializes these INSERT ... SELECT statements with all other writers.
const messageMetadataVersionsSchema = `
	CREATE TABLE IF NOT EXISTS message_metadata_versions (
		version_id INTEGER PRIMARY KEY,
		message_id TEXT NOT NULL,
		chat_jid TEXT NOT NULL,
		message_type TEXT NOT NULL,
		metadata_json TEXT NOT NULL,
		source_format TEXT NOT NULL,
		source_payload BLOB NOT NULL,
		source_kind TEXT NOT NULL,
		provenance_json TEXT NOT NULL,
		observed_at TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS message_metadata_versions_message
		ON message_metadata_versions(message_id, chat_jid, version_id);
`

// Retention triggers cover writers that update metadata without recording versions.
// Provenance-aware writers insert their version first in the same transaction,
// so these fallback triggers do not invent a second, unknown source for them.
const messageMetadataRetentionTriggers = `
	CREATE TRIGGER IF NOT EXISTS message_metadata_retain_insert
	AFTER INSERT ON message_metadata BEGIN
		INSERT INTO message_metadata_versions
			(message_id, chat_jid, message_type, metadata_json, source_format,
			 source_payload, source_kind, provenance_json, observed_at)
		SELECT new.message_id, new.chat_jid, new.message_type, new.metadata_json,
			new.source_format, new.source_payload, 'legacy_metadata', '{}', new.observed_at
		WHERE NOT EXISTS (SELECT 1 FROM message_metadata_versions v
			WHERE v.message_id=new.message_id AND v.chat_jid=new.chat_jid
			AND v.message_type=new.message_type AND v.metadata_json=new.metadata_json
			AND v.source_format=new.source_format AND v.source_payload=new.source_payload);
	END;
	CREATE TRIGGER IF NOT EXISTS message_metadata_retain_before_update
	BEFORE UPDATE ON message_metadata BEGIN
		INSERT INTO message_metadata_versions
			(message_id, chat_jid, message_type, metadata_json, source_format,
			 source_payload, source_kind, provenance_json, observed_at)
		SELECT old.message_id, old.chat_jid, old.message_type, old.metadata_json,
			old.source_format, old.source_payload, 'legacy_metadata', '{}', old.observed_at
		WHERE NOT EXISTS (SELECT 1 FROM message_metadata_versions v
			WHERE v.message_id=old.message_id AND v.chat_jid=old.chat_jid
			AND v.message_type=old.message_type AND v.metadata_json=old.metadata_json
			AND v.source_format=old.source_format AND v.source_payload=old.source_payload);
	END;
	CREATE TRIGGER IF NOT EXISTS message_metadata_retain_after_update
	AFTER UPDATE ON message_metadata BEGIN
		INSERT INTO message_metadata_versions
			(message_id, chat_jid, message_type, metadata_json, source_format,
			 source_payload, source_kind, provenance_json, observed_at)
		SELECT new.message_id, new.chat_jid, new.message_type, new.metadata_json,
			new.source_format, new.source_payload, 'legacy_metadata', '{}', new.observed_at
		WHERE NOT EXISTS (SELECT 1 FROM message_metadata_versions v
			WHERE v.message_id=new.message_id AND v.chat_jid=new.chat_jid
			AND v.message_type=new.message_type AND v.metadata_json=new.metadata_json
			AND v.source_format=new.source_format AND v.source_payload=new.source_payload);
	END;
`

const backfillMessageMetadataVersionsSQL = `
	INSERT INTO message_metadata_versions
		(message_id, chat_jid, message_type, metadata_json, source_format,
		 source_payload, source_kind, provenance_json, observed_at)
	SELECT m.message_id, m.chat_jid, m.message_type, m.metadata_json,
		m.source_format, m.source_payload, 'legacy_metadata', '{}', m.observed_at
	FROM message_metadata m
	WHERE NOT EXISTS (SELECT 1 FROM message_metadata_versions v
		WHERE v.message_id=m.message_id AND v.chat_jid=m.chat_jid
		AND v.message_type=m.message_type AND v.metadata_json=m.metadata_json
		AND v.source_format=m.source_format AND v.source_payload=m.source_payload)
`

func migrateMessageMetadataVersions(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Take the writer reservation before any schema reads. On repeat startup,
	// IF NOT EXISTS can be a read-only no-op; upgrading that read transaction
	// while another ingestion writer waits would otherwise fail with BUSY.
	if _, err := tx.Exec(`UPDATE message_metadata SET observed_at=observed_at WHERE 0`); err != nil {
		return err
	}
	// Install triggers and copy unversioned values in one write transaction
	// so concurrent metadata writers cannot replace an unretained source.
	for _, statement := range []string{messageMetadataVersionsSchema, messageMetadataRetentionTriggers, backfillMessageMetadataVersionsSQL} {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	if err := migrateMessageRowVersions(tx); err != nil {
		return err
	}
	return tx.Commit()
}

const insertMessageMetadataVersionSQL = `
	INSERT INTO message_metadata_versions
		(message_id, chat_jid, message_type, metadata_json, source_format,
		 source_payload, source_kind, provenance_json, observed_at)
	SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
	WHERE NOT EXISTS (SELECT 1 FROM message_metadata_versions
		WHERE message_id=? AND chat_jid=? AND message_type=? AND metadata_json=?
		AND source_format=? AND source_payload=? AND source_kind=? AND provenance_json=?)
`

func storeIncomingMetadataVersion(ctx context.Context, tx *sql.Tx, message *incomingMessage, observedAt time.Time) error {
	sourceKind := message.SourceKind
	if sourceKind == "" {
		sourceKind = "legacy_metadata"
	}
	provenance := string(message.Provenance)
	if provenance == "" {
		provenance = "{}"
	}
	_, err := tx.ExecContext(ctx, insertMessageMetadataVersionSQL,
		message.ID, message.ChatJID, message.MessageType, string(message.Metadata),
		message.SourceFormat, message.SourcePayload, sourceKind, provenance, observedAt,
		message.ID, message.ChatJID, message.MessageType, string(message.Metadata),
		message.SourceFormat, message.SourcePayload, sourceKind, provenance)
	if err != nil {
		return fmt.Errorf("retain message source version: %w", err)
	}
	return nil
}
