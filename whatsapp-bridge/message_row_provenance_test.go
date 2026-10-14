package main

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"
)

func assertRowVersionCount(t *testing.T, store *MessageStore, want int) {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM message_row_versions`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("retained %d raw row versions, want %d", count, want)
	}
}

func TestRawRowVersionsRetainLocatorCollisionsWithoutSourceMetadata(t *testing.T) {
	store, _ := ingestionFixture(t)
	payload := incomingFixture("colliding-id")
	payload.Message.Filename = "original.pdf"
	payload.Message.URL = "https://example.test/original"
	payload.Message.MediaKey = []byte{0, 1, 255}
	if _, _, err := store.applyIngestion(context.Background(), nil, payload); err != nil {
		t.Fatal(err)
	}
	var originalRowID int64
	if err := store.db.QueryRow(`SELECT rowid FROM messages`).Scan(&originalRowID); err != nil {
		t.Fatal(err)
	}
	payload.Message.Sender = "another-sender"
	payload.Message.Timestamp = time.Unix(200, 0)
	payload.Message.Content = "different content under the same physical key"
	payload.Message.FromMe = true
	payload.Message.URL = "https://example.test/replaced"
	payload.Message.MediaKey = []byte{2, 3}
	for replay := 0; replay < 2; replay++ {
		if _, _, err := store.applyIngestion(context.Background(), nil, payload); err != nil {
			t.Fatal(err)
		}
	}
	assertRowVersionCount(t, store, 2)
	assertProvenanceCount(t, store, 0)
	var sender, content, filename, url string
	var key []byte
	var timestamp time.Time
	var fromMe bool
	var retainedRowID int64
	if err := store.db.QueryRow(`SELECT message_rowid,sender,content,timestamp,is_from_me,filename,url,media_key
		FROM message_row_versions ORDER BY version_id LIMIT 1`).Scan(&retainedRowID, &sender, &content, &timestamp, &fromMe, &filename, &url, &key); err != nil {
		t.Fatal(err)
	}
	if retainedRowID != originalRowID || sender != "100" || content != "synthetic message" || !timestamp.Equal(time.Unix(100, 0)) || fromMe || filename != "original.pdf" || url != "https://example.test/original" || !bytes.Equal(key, []byte{0, 1, 255}) {
		t.Fatal("original normalized raw fields were overwritten")
	}
	if err := store.StoreMediaInfo("colliding-id", "100@s.whatsapp.net", "https://example.test/refreshed", []byte{4}, []byte{5}, []byte{6}, 7); err != nil {
		t.Fatal(err)
	}
	assertRowVersionCount(t, store, 3)
}

func TestRawRowMigrationPreservesNullsAndEmptyValuesExactly(t *testing.T) {
	store, _ := ingestionFixture(t)
	if err := store.StoreChat("100@s.whatsapp.net", "Synthetic", time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`
		DROP TRIGGER message_rows_retain_insert;
		DROP TRIGGER message_rows_retain_before_update;
		DROP TRIGGER message_rows_retain_after_update;
		DROP TABLE message_row_versions;
		INSERT INTO messages(id,chat_jid) VALUES('legacy-null','100@s.whatsapp.net');
	`); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := migrateMessageMetadataVersions(store.db); err != nil {
			t.Fatal(err)
		}
	}
	assertRowVersionCount(t, store, 1)
	var filename sql.NullString
	var key []byte
	if err := store.db.QueryRow(`SELECT filename,media_key FROM message_row_versions`).Scan(&filename, &key); err != nil {
		t.Fatal(err)
	}
	if filename.Valid || key != nil {
		t.Fatal("legacy NULLs were normalized or lost")
	}
	for _, statement := range []string{
		`UPDATE messages SET filename=''`,
		`UPDATE messages SET media_key=X''`,
		`UPDATE messages SET content='legacy-writer-edit'`,
		`UPDATE messages SET content='legacy-writer-edit'`,
	} {
		if _, err := store.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	assertRowVersionCount(t, store, 4)
	if err := migrateMessageMetadataVersions(store.db); err != nil {
		t.Fatal(err)
	}
	assertRowVersionCount(t, store, 4)
}

func TestRawRowRetentionFailureRollsBackIngestion(t *testing.T) {
	store, _ := ingestionFixture(t)
	if _, err := store.db.Exec(`CREATE TRIGGER fail_raw_row_version BEFORE INSERT ON message_row_versions BEGIN SELECT RAISE(ABORT,'synthetic raw row retention failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.applyIngestion(context.Background(), nil, provenanceFixture("failure")); err == nil {
		t.Fatal("raw row retention failure was hidden")
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial message committed: count=%d error=%v", count, err)
	}
	assertRowVersionCount(t, store, 0)
	assertProvenanceCount(t, store, 0)
}
