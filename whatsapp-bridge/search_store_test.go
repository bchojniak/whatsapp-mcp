package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSearchMigrationIndexesExistingRowsAndSurvivesRestart(t *testing.T) {
	store := backfillFixture(t)
	var enabled bool
	if err := store.db.QueryRow(`SELECT sqlite_compileoption_used('ENABLE_FTS5')`).Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if !enabled {
		t.Skip("production FTS verification requires -tags sqlite_fts5")
	}
	// Create an archive without derived search indexes.
	if _, err := store.db.Exec(`DROP TRIGGER messages_fts_insert; DROP TRIGGER messages_fts_update; DROP TRIGGER messages_fts_delete; DROP TABLE messages_fts; DELETE FROM archive_migrations;`); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreChat("100@s.whatsapp.net", "Contact", time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreMessage("a", "100@s.whatsapp.net", "100", "João orçamento", time.Unix(100, 0), false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := migrateSearchStore(store.db); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'joao AND orcamento'`).Scan(&count); err != nil || count != 1 {
			t.Fatalf("index returned %d, error %v", count, err)
		}
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'original'`).Scan(&count); err != nil || count != 2 {
			t.Fatalf("existing archive rows missing from index: %d %v", count, err)
		}
	}
}

func TestReplayPreservesMessageRowIdentityMediaAndChatRecency(t *testing.T) {
	store := backfillFixture(t)
	jid := "100@s.whatsapp.net"
	if err := store.StoreChat(jid, "Contact", time.Unix(400, 0)); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreMessage("a", jid, "100", "caption", time.Unix(100, 0), false, "image", "original.jpg", "url", []byte{1}, []byte{2}, []byte{3}, 123); err != nil {
		t.Fatal(err)
	}
	var oldRow int
	if err := store.db.QueryRow(`SELECT rowid FROM messages WHERE id='a'`).Scan(&oldRow); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreChat(jid, "Contact", time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreMessage("a", jid, "100", "edited caption", time.Unix(100, 0), false, "image", "generated.jpg", "", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	var row, length int
	var filename, url, body string
	if err := store.db.QueryRow(`SELECT rowid,filename,url,file_length,content FROM messages WHERE id='a'`).Scan(&row, &filename, &url, &length, &body); err != nil {
		t.Fatal(err)
	}
	if row != oldRow || filename != "original.jpg" || url != "url" || length != 123 || body != "edited caption" {
		t.Fatalf("replay lost identity or metadata: %d %s %s %d %s", row, filename, url, length, body)
	}
	var latest time.Time
	if err := store.db.QueryRow(`SELECT last_message_time FROM chats WHERE jid=?`, jid).Scan(&latest); err != nil || !latest.Equal(time.Unix(400, 0)) {
		t.Fatalf("chat recency regressed: %v %v", latest, err)
	}
}

func TestHealthReportsObservedStorageWithoutInventingConnection(t *testing.T) {
	store := backfillFixture(t)
	store.syncObservation.record(true)
	handler := newRESTHandler(nil, store, true)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	var body map[string]interface{}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["connected"] != nil || body["last_successful_sync_at"] == nil || body["observation_scope"] != "current_process" {
		t.Fatalf("unexpected health: %v", body)
	}
}
