package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func ingestionFixture(t *testing.T) (*MessageStore, *ingestionJournal) {
	t.Helper()
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	journal, err := newIngestionJournal(store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { journal.close(); store.Close() })
	return store, journal
}

func incomingFixture(id string) ingestionPayload {
	return ingestionPayload{Kind: "message", Message: &incomingMessage{ID: id, ChatJID: "100@s.whatsapp.net", Sender: "100", Content: "synthetic message", Timestamp: time.Unix(100, 0)}}
}

func ingestionHealth(journal *ingestionJournal) map[string]interface{} {
	return journal.snapshot()["ingestion"].(map[string]interface{})
}

func TestIngestionQueueSurvivesUncleanRestart(t *testing.T) {
	store, journal := ingestionFixture(t)
	if err := journal.enqueue(incomingFixture("a")); err != nil {
		t.Fatal(err)
	}
	if err := journal.db.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := newIngestionJournal(store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	if !restarted.processOne(context.Background()) {
		t.Fatal("persisted batch was not processed")
	}
	health := ingestionHealth(restarted)
	if health["pending_batches"] != int64(0) || health["unclean_restart_count"] != int64(1) || health["received_batches"] != int64(1) || health["committed_message_entries"] != int64(1) {
		t.Fatalf("unexpected recovery: %v", health)
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("count %d error %v", count, err)
	}
}

func TestIngestionReplayAfterArchiveCommitPreservesMediaAndIdentity(t *testing.T) {
	store, journal := ingestionFixture(t)
	payload := incomingFixture("a")
	payload.Message.MediaType, payload.Message.Filename = "image", "original.jpg"
	payload.Message.URL, payload.Message.MediaKey, payload.Message.FileLength = "https://example.test/media", []byte{1, 2}, 100
	if err := journal.enqueue(payload); err != nil {
		t.Fatal(err)
	}
	batch, err := journal.nextBatch()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.applyIngestion(context.Background(), nil, payload); err != nil {
		t.Fatal(err)
	}
	var rowID int64
	store.db.QueryRow(`SELECT rowid FROM messages WHERE id='a'`).Scan(&rowID)
	journal.db.Close()
	restarted, err := newIngestionJournal(store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	if !restarted.processOne(context.Background()) {
		t.Fatal("replay did not complete")
	}
	var count int
	var replayRow int64
	var filename string
	if err := store.db.QueryRow(`SELECT COUNT(*),rowid,filename FROM messages WHERE id='a'`).Scan(&count, &replayRow, &filename); err != nil {
		t.Fatal(err)
	}
	if count != 1 || replayRow != rowID || filename != "original.jpg" || batch == nil {
		t.Fatal("replay duplicated a message or lost media identity")
	}
}

func TestIngestionAtomicFailureRetainsBatchAndDoesNotBlockNewMessages(t *testing.T) {
	store, journal := ingestionFixture(t)
	if _, err := store.db.Exec(`CREATE TRIGGER reject_message BEFORE INSERT ON messages WHEN new.id='bad' BEGIN SELECT RAISE(FAIL,'synthetic constraint failure'); END`); err != nil {
		t.Fatal(err)
	}
	journal.enqueue(incomingFixture("bad"))
	if !journal.processOne(context.Background()) {
		t.Fatal("permanent failure should allow the next batch")
	}
	var count int
	store.db.QueryRow(`SELECT COUNT(*) FROM chats`).Scan(&count)
	if count != 0 {
		t.Fatal("chat survived a failed message transaction")
	}
	journal.enqueue(incomingFixture("good"))
	journal.processOne(context.Background())
	health := ingestionHealth(journal)
	if health["status"] != "degraded" || health["failed_batches"] != int64(1) || health["failed_write_attempts"] != int64(1) || health["committed_message_entries"] != int64(1) || health["last_write_error"] != "constraint_failed" {
		t.Fatalf("failed write was hidden: %v", health)
	}
	journal.close()
	restarted, err := newIngestionJournal(store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	if ingestionHealth(restarted)["failed_batches"] != int64(1) {
		t.Fatal("restart erased failed batch")
	}
}

func TestIngestionRetriesRealArchiveLockInOrder(t *testing.T) {
	store, journal := ingestionFixture(t)
	locker, err := sql.Open("sqlite3", "file:"+filepath.Join(store.storeDir, "messages.db")+"?_foreign_keys=on")
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Close()
	if _, err := locker.Exec(`BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	journal.enqueue(incomingFixture("first"))
	journal.enqueue(incomingFixture("second"))
	if journal.processOne(context.Background()) {
		t.Fatal("locked write unexpectedly completed")
	}
	health := ingestionHealth(journal)
	if health["pending_batches"] != int64(2) || health["failed_write_attempts"] != int64(1) || health["last_write_error"] != "database_locked" {
		t.Fatalf("lock was not retained: %v", health)
	}
	if journal.processOne(context.Background()) {
		t.Fatal("new event bypassed the retrying event")
	}
	if _, err := locker.Exec(`ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	journal.db.Exec(`UPDATE ingestion_queue SET retry_at='2000-01-01T00:00:00Z'`)
	if !journal.processOne(context.Background()) || !journal.processOne(context.Background()) {
		t.Fatal("queue did not recover")
	}
	health = ingestionHealth(journal)
	if health["status"] != "healthy" || health["recovered_batches"] != int64(1) || health["pending_batches"] != int64(0) {
		t.Fatalf("bad recovery status: %v", health)
	}
}

func TestHistoryQueueNotifiesOnlyAfterAtomicCommit(t *testing.T) {
	store, journal := ingestionFixture(t)
	history := &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{ID: proto.String("100@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{historyMessage("a", "synthetic", 100), historyMessage("b", "synthetic", 99)}}}}
	data, err := proto.MarshalOptions{AllowPartial: true}.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	journal.onHistory = func(event *events.HistorySync, err error) {
		var count int
		store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count)
		if err != nil || count != 2 || event.Data.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND {
			t.Fatal("history callback before full commit")
		}
	}
	journal.enqueue(ingestionPayload{Kind: "history", History: data})
	if !journal.processOne(context.Background()) {
		t.Fatal("history batch was not processed")
	}
	if store.historyMessagesAdded.Load() != 2 {
		t.Fatal("history inserted count changed")
	}
	journal.enqueue(ingestionPayload{Kind: "history", History: data})
	journal.processOne(context.Background())
	if store.historyMessagesAdded.Load() != 2 {
		t.Fatal("history replay counted duplicates as new")
	}
}

func TestRegularHistoryBatchRollsBackAllMessages(t *testing.T) {
	store, journal := ingestionFixture(t)
	store.db.Exec(`CREATE TRIGGER reject_message BEFORE INSERT ON messages WHEN new.id='bad' BEGIN SELECT RAISE(FAIL,'synthetic'); END`)
	history := &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{{ID: proto.String("100@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{historyMessage("good", "synthetic", 100), historyMessage("bad", "synthetic", 99)}}}}
	if _, err := store.applyHistory(context.Background(), nil, history, ""); err == nil {
		t.Fatal("expected history write failure")
	}
	var count int
	store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count)
	if count != 0 {
		t.Fatal("partially committed history")
	}
	if ingestionHealth(journal)["committed_batches"] != int64(0) {
		t.Fatal("invented journal commit")
	}
}

func TestJournalFailureIsExplicitAndPersistsWhenStorageRecovers(t *testing.T) {
	store, journal := ingestionFixture(t)
	journal.db.Exec(`PRAGMA query_only=ON`)
	if err := journal.enqueue(incomingFixture("a")); err == nil {
		t.Fatal("expected journal write failure")
	}
	health := ingestionHealth(journal)
	if health["status"] != "degraded" || health["diagnostics_available"] != false || health["unpersisted_events"] != int64(1) {
		t.Fatalf("journal failure hidden: %v", health)
	}
	journal.db.Exec(`PRAGMA query_only=OFF`)
	journal.recordConnection("connected", "connection_established")
	journal.close()
	restarted, err := newIngestionJournal(store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	if ingestionHealth(restarted)["unpersisted_events"] != int64(1) {
		t.Fatal("recovery erased unpersisted-event evidence")
	}
}

func TestIngestionDiagnosticsPersistAndQuietAccountIsHealthy(t *testing.T) {
	store, journal := ingestionFixture(t)
	journal.recordConnection("connected", "connection_established")
	journal.recordConnection("disconnected", "connection_lost")
	journal.recordConnection("connected", "connection_established")
	journal.enqueue(incomingFixture("a"))
	journal.processOne(context.Background())
	firstObserved := journal.state.StartedAt
	journal.close()
	restarted, err := newIngestionJournal(store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	restarted.recordConnection("connected", "connection_established")
	health := ingestionHealth(restarted)
	if health["status"] != "healthy" || health["disconnect_count"] != int64(1) || health["reconnect_count"] != int64(2) || health["unclean_restart_count"] != int64(0) || health["last_committed_at"] == nil || !restarted.state.StartedAt.Equal(firstObserved) {
		t.Fatalf("diagnostics lost on restart: %v", health)
	}
}

func TestAgeingQueueIsDegradedWithoutInventingMessageFailures(t *testing.T) {
	_, journal := ingestionFixture(t)
	journal.enqueue(incomingFixture("a"))
	journal.db.Exec(`UPDATE ingestion_queue SET received_at='2000-01-01T00:00:00Z'`)
	health := ingestionHealth(journal)
	if health["status"] != "degraded" || health["failed_write_attempts"] != int64(0) {
		t.Fatal("ageing queue status is misleading")
	}
}

func TestQueueCapacityRefusalIsDurableAndContainsNoPrivateDiagnostics(t *testing.T) {
	_, journal := ingestionFixture(t)
	if _, err := journal.db.Exec(`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000) INSERT INTO ingestion_queue(payload,received_at,entries,retry_at,status) SELECT '{}','2000-01-01T00:00:00Z',0,'2000-01-01T00:00:00Z','failed' FROM n`); err != nil {
		t.Fatal(err)
	}
	payload := incomingFixture("private-message-id")
	payload.Message.Content = "private synthetic content"
	if err := journal.enqueue(payload); err == nil {
		t.Fatal("queue capacity was ignored")
	}
	health := ingestionHealth(journal)
	encoded, err := json.Marshal(health)
	if err != nil {
		t.Fatal(err)
	}
	if health["unpersisted_events"] != int64(1) || strings.Contains(string(encoded), "private-message") || strings.Contains(string(encoded), "synthetic content") {
		t.Fatal("refusal is missing or leaks private data")
	}
	stat, err := os.Stat(filepath.Join(journal.store.storeDir, "ingestion.db"))
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatal("journal permissions are not private")
	}
}

func TestBackgroundWriterHandlesConcurrentEventsAndSnapshots(t *testing.T) {
	store, journal := ingestionFixture(t)
	journal.start()
	var callers sync.WaitGroup
	for i := 0; i < 10; i++ {
		callers.Add(1)
		go func() { defer callers.Done(); journal.enqueue(incomingFixture("same-id")); journal.snapshot() }()
	}
	callers.Wait()
	journal.close()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count); err != nil || count > 1 {
		t.Fatal("concurrent duplicate writes")
	}
	if journal.state.ReceivedBatches != 10 {
		t.Fatal("lost concurrent admission")
	}
}

func TestStorageRetryCategories(t *testing.T) {
	for _, code := range []sqlite3.ErrNo{sqlite3.ErrBusy, sqlite3.ErrLocked, sqlite3.ErrFull, sqlite3.ErrIoErr, sqlite3.ErrReadonly, sqlite3.ErrCantOpen} {
		if !retryableStorageError(sqlite3.Error{Code: code}) {
			t.Fatalf("temporary storage error not retryable: %v", code)
		}
	}
	for _, code := range []sqlite3.ErrNo{sqlite3.ErrConstraint, sqlite3.ErrCorrupt, sqlite3.ErrNotADB} {
		if retryableStorageError(sqlite3.Error{Code: code}) {
			t.Fatalf("permanent error was retried: %v", code)
		}
	}
}
