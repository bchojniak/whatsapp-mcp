package main

import (
	"database/sql"
	"fmt"
	"strings"
)

// Keep nulls, all retrieval references, and the original SQLite locator. This
// also protects messages stored without a provider protobuf.
const messageRowVersionsSchema = `
	CREATE TABLE IF NOT EXISTS message_row_versions (
		version_id INTEGER PRIMARY KEY,
		message_rowid INTEGER NOT NULL,
		message_id TEXT,
		chat_jid TEXT,
		sender TEXT,
		content TEXT,
		timestamp TIMESTAMP,
		is_from_me BOOLEAN,
		media_type TEXT,
		filename TEXT,
		url TEXT,
		media_key BLOB,
		file_sha256 BLOB,
		file_enc_sha256 BLOB,
		file_length INTEGER,
		captured_at TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS message_row_versions_message
		ON message_row_versions(message_id, chat_jid, version_id);
	CREATE INDEX IF NOT EXISTS message_row_versions_rowid
		ON message_row_versions(message_rowid, version_id);
`

var messageRowSnapshotFields = []struct {
	snapshot string
	original string
}{
	{"message_rowid", "rowid"}, {"message_id", "id"}, {"chat_jid", "chat_jid"},
	{"sender", "sender"}, {"content", "content"}, {"timestamp", "timestamp"},
	{"is_from_me", "is_from_me"}, {"media_type", "media_type"}, {"filename", "filename"},
	{"url", "url"}, {"media_key", "media_key"}, {"file_sha256", "file_sha256"},
	{"file_enc_sha256", "file_enc_sha256"}, {"file_length", "file_length"},
}

// alias and from are fixed SQL fragments supplied only by the migration below.
// Sharing the field list keeps trigger and backfill snapshots byte-for-byte
// equivalent, including the distinction between SQL NULL and empty values.
func messageRowSnapshotSQL(alias, from string) string {
	columns := make([]string, 0, len(messageRowSnapshotFields)+1)
	values := make([]string, 0, len(messageRowSnapshotFields)+1)
	comparisons := make([]string, 0, len(messageRowSnapshotFields))
	for _, field := range messageRowSnapshotFields {
		columns = append(columns, field.snapshot)
		values = append(values, alias+"."+field.original)
		comparisons = append(comparisons, "v."+field.snapshot+" IS "+alias+"."+field.original)
	}
	columns = append(columns, "captured_at")
	values = append(values, "strftime('%Y-%m-%dT%H:%M:%fZ','now')")
	return fmt.Sprintf(`INSERT INTO message_row_versions (%s)
		SELECT %s %s WHERE NOT EXISTS (SELECT 1 FROM message_row_versions v WHERE %s);`,
		strings.Join(columns, ","), strings.Join(values, ","), from, strings.Join(comparisons, " AND "))
}

func migrateMessageRowVersions(tx *sql.Tx) error {
	if _, err := tx.Exec(messageRowVersionsSchema); err != nil {
		return err
	}
	for _, trigger := range []struct {
		name, timing, alias string
	}{
		{"message_rows_retain_insert", "AFTER INSERT", "new"},
		{"message_rows_retain_before_update", "BEFORE UPDATE", "old"},
		{"message_rows_retain_after_update", "AFTER UPDATE", "new"},
	} {
		statement := fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS %s %s ON messages BEGIN %s END;",
			trigger.name, trigger.timing, messageRowSnapshotSQL(trigger.alias, ""))
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	_, err := tx.Exec(messageRowSnapshotSQL("m", "FROM messages m"))
	return err
}
