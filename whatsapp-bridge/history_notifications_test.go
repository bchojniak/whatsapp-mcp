package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type fakeHistoryDownload struct {
	connected                           bool
	history                             *waHistorySync.HistorySync
	downloadErr, receiptErr, cleanupErr error
	downloads, receipts, cleanups       int
	synchronous                         bool
	started, release                    chan struct{}
}

func (f *fakeHistoryDownload) IsConnected() bool { return f.connected }
func (f *fakeHistoryDownload) DownloadHistorySync(ctx context.Context, _ *waE2E.HistorySyncNotification, synchronous bool) (*waHistorySync.HistorySync, error) {
	f.downloads++
	f.synchronous = synchronous
	if f.started != nil {
		close(f.started)
		select {
		case <-f.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return f.history, f.downloadErr
}
func (f *fakeHistoryDownload) SendProtocolMessageReceipt(_ context.Context, _ types.MessageID, _ types.ReceiptType) error {
	f.receipts++
	return f.receiptErr
}
func (f *fakeHistoryDownload) DeleteMedia(_ context.Context, _ whatsmeow.MediaType, _ string, _ []byte, _ string) error {
	f.cleanups++
	return f.cleanupErr
}

func historyDownloadFixture() *fakeHistoryDownload {
	return &fakeHistoryDownload{connected: true, history: &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_RECENT.Enum(), Progress: proto.Uint32(100), Conversations: []*waHistorySync.Conversation{{ID: proto.String("100@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{historyMessage("history-a", "synthetic history", 100)}}}}}
}

func historyNotificationFixture() *waE2E.HistorySyncNotification {
	return &waE2E.HistorySyncNotification{SyncType: waE2E.HistorySyncType_RECENT.Enum(), DirectPath: proto.String("/synthetic-history"), FileEncSHA256: []byte("synthetic digest")}
}

func forceHistoryRetry(t *testing.T, journal *ingestionJournal) {
	t.Helper()
	if _, err := journal.db.Exec(`UPDATE history_notifications SET retry_at='2000-01-01T00:00:00Z'`); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryNotificationSurvivesRestartAndCleansOnlyAfterArchiveCommit(t *testing.T) {
	store, journal := ingestionFixture(t)
	notice := historyNotificationFixture()
	if err := journal.enqueueHistoryNotification("synthetic-notice", notice); err != nil {
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
	fake := historyDownloadFixture()
	restarted.historyClient = fake
	if !restarted.processHistoryNotification(context.Background()) {
		t.Fatal("saved notification was not downloaded")
	}
	if fake.receipts != 1 || fake.cleanups != 0 || !fake.synchronous {
		t.Fatal("download ran outside durable receipt/storage boundary")
	}
	if restarted.processHistoryNotification(context.Background()) {
		t.Fatal("remote cleanup preceded archive commit")
	}
	if !restarted.processOne(context.Background()) {
		t.Fatal("downloaded history was not committed")
	}
	var messages int
	if err = store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&messages); err != nil || messages != 1 {
		t.Fatalf("messages=%d err=%v", messages, err)
	}
	if !restarted.processHistoryNotification(context.Background()) || fake.cleanups != 1 {
		t.Fatal("committed history was not cleaned")
	}
	if err = restarted.enqueueHistoryNotification("redelivered-notice", notice); err != nil {
		t.Fatal(err)
	}
	if restarted.processHistoryNotification(context.Background()) || fake.downloads != 1 {
		t.Fatal("completed redelivery was downloaded again")
	}
	var retained int
	restarted.db.QueryRow(`SELECT length(payload)+length(message_id) FROM history_notifications`).Scan(&retained)
	if retained != 0 {
		t.Fatal("completed notification retained remote credentials")
	}
}

func TestHistoryDownloadRetryPersistsBackoffAndDoesNotBlockLiveIngestion(t *testing.T) {
	store, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	fake.downloadErr = errors.New("synthetic network failure")
	journal.historyClient = fake
	journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture())
	if journal.processHistoryNotification(context.Background()) {
		t.Fatal("failed download succeeded")
	}
	if journal.processHistoryNotification(context.Background()) || fake.downloads != 1 {
		t.Fatal("retry ignored stored backoff")
	}
	journal.enqueue(incomingFixture("live"))
	if !journal.processOne(context.Background()) {
		t.Fatal("download backoff blocked live message storage")
	}
	journal.db.Close()
	restarted, err := newIngestionJournal(store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.close()
	restarted.historyClient = fake
	if restarted.processHistoryNotification(context.Background()) || fake.downloads != 1 {
		t.Fatal("restart erased backoff")
	}
	fake.downloadErr = nil
	forceHistoryRetry(t, restarted)
	if !restarted.processHistoryNotification(context.Background()) || fake.receipts != 1 {
		t.Fatal("retry failed or repeated persisted receipt")
	}
	health := ingestionHealth(restarted)["history_downloads"].(map[string]interface{})
	if health["failed_attempts"] != int64(1) || health["last_error"] != "" {
		t.Fatalf("lost failure/recovery evidence: %v", health)
	}
}

func TestHistoryArchiveFailureNeverDeletesRemoteSourceOrBypassesReadiness(t *testing.T) {
	store, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	journal.historyClient = fake
	store.db.Exec(`CREATE TRIGGER reject_history BEFORE INSERT ON messages BEGIN SELECT RAISE(FAIL,'synthetic failure'); END`)
	journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture())
	journal.processHistoryNotification(context.Background())
	journal.processOne(context.Background())
	if journal.processHistoryNotification(context.Background()) || fake.cleanups != 0 {
		t.Fatal("failed archive lost remote source")
	}
	_, pending, err := journal.initialHistorySnapshot()
	if err != nil || !pending {
		t.Fatal("failed archive was treated as drained")
	}
}

func TestHistoryDownloadAndNotificationTransitionIsAtomic(t *testing.T) {
	_, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	journal.historyClient = fake
	journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture())
	journal.db.Exec(`CREATE TRIGGER reject_link BEFORE UPDATE ON history_notifications WHEN new.status='awaiting_storage' BEGIN SELECT RAISE(FAIL,'synthetic failure'); END`)
	journal.processHistoryNotification(context.Background())
	var count int
	journal.db.QueryRow(`SELECT COUNT(*) FROM ingestion_queue`).Scan(&count)
	if count != 0 {
		t.Fatal("archive batch survived failed notification-link transaction")
	}
	journal.db.Exec(`DROP TRIGGER reject_link`)
	forceHistoryRetry(t, journal)
	if !journal.processHistoryNotification(context.Background()) {
		t.Fatal("notification could not recover")
	}
	journal.db.QueryRow(`SELECT COUNT(*) FROM ingestion_queue`).Scan(&count)
	if count != 1 {
		t.Fatal("transition duplicated batch")
	}
}

func TestHistoryCleanupRetryDoesNotRedownloadAndAcceptsAlreadyGoneSource(t *testing.T) {
	_, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	journal.historyClient = fake
	journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture())
	journal.processHistoryNotification(context.Background())
	journal.processOne(context.Background())
	fake.cleanupErr = errors.New("synthetic cleanup failure")
	if journal.processHistoryNotification(context.Background()) {
		t.Fatal("failed cleanup succeeded")
	}
	forceHistoryRetry(t, journal)
	fake.cleanupErr = errors.New("media delete failed with status code 404")
	if !journal.processHistoryNotification(context.Background()) || fake.downloads != 1 {
		t.Fatal("cleanup recovery redownloaded or failed")
	}
}

func TestHistoryReceiptFollowsPersistenceAndWaitsForConnection(t *testing.T) {
	_, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	fake.connected = false
	journal.historyClient = fake
	journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture())
	if journal.processHistoryNotification(context.Background()) || fake.receipts != 0 {
		t.Fatal("disconnected worker sent receipt")
	}
	fake.connected = true
	fake.receiptErr = errors.New("synthetic receipt failure")
	journal.processHistoryNotification(context.Background())
	if fake.downloads != 0 {
		t.Fatal("download preceded durable receipt retry")
	}
	forceHistoryRetry(t, journal)
	fake.receiptErr = nil
	if !journal.processHistoryNotification(context.Background()) {
		t.Fatal("receipt retry did not recover")
	}
}

func TestInitialHistorySnapshotIgnoresLiveAndOnDemandTiming(t *testing.T) {
	_, journal := ingestionFixture(t)
	notice := historyNotificationFixture()
	journal.enqueueHistoryNotification("synthetic-notice", notice)
	first, _, err := journal.initialHistorySnapshot()
	if err != nil || first.IsZero() {
		t.Fatal("initial notice did not update clock")
	}
	journal.enqueue(incomingFixture("live"))
	journal.processOne(context.Background())
	other := historyNotificationFixture()
	other.SyncType = waE2E.HistorySyncType_ON_DEMAND.Enum()
	journal.enqueueHistoryNotification("synthetic-ondemand", other)
	last, _, err := journal.initialHistorySnapshot()
	if err != nil || !last.Equal(first) {
		t.Fatal("live or on-demand event reset initial quiet clock")
	}
	health := ingestionHealth(journal)
	raw, err := json.Marshal(health)
	data := string(raw)
	if err != nil || strings.Contains(data, "synthetic-notice") || strings.Contains(data, "synthetic digest") || strings.Contains(data, "/synthetic-history") {
		t.Fatal("status exposed raw notification credentials")
	}
}

func TestHistoryNoticeOnlyAcceptsOwnProtocolNotifications(t *testing.T) {
	notice := historyNotificationFixture()
	event := &events.Message{Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{HistorySyncNotification: notice}}}
	if historyNotice(event) != nil {
		t.Fatal("foreign message accepted as device history")
	}
	event.Info.IsFromMe = true
	if historyNotice(event) != notice {
		t.Fatal("own notification missed")
	}
	if historyNotice(nil) != nil {
		t.Fatal("nil event accepted")
	}
}

func TestInitialHistoryProgressDoesNotClaimCoverage(t *testing.T) {
	_, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	journal.historyClient = fake
	journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture())
	journal.processHistoryNotification(context.Background())
	journal.processOne(context.Background())
	initial := ingestionHealth(journal)["initial_history"].(map[string]interface{})
	if initial["history_complete"] != nil || initial["transfer_progress"] == nil {
		t.Fatal("transfer progress became completeness evidence")
	}
	last, pending, err := journal.initialHistorySnapshot()
	if err != nil || pending || time.Since(last) > time.Minute {
		t.Fatal("committed initial history still blocked by remote cleanup")
	}
}

func TestInlineHistoryNoticeIsNotQueuedTwiceAndNeedsNoRemoteCleanup(t *testing.T) {
	_, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	journal.historyClient = fake
	notice := historyNotificationFixture()
	notice.DirectPath = nil
	notice.InitialHistBootstrapInlinePayload = []byte("synthetic inline history")
	event := &events.Message{Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{HistorySyncNotification: notice}}}
	event.Info.IsFromMe = true
	event.Info.ID = "synthetic-notice"
	if err := journal.enqueueIncomingMessage(event, nil); err != nil {
		t.Fatal(err)
	}
	var count int
	journal.db.QueryRow(`SELECT COUNT(*) FROM ingestion_queue`).Scan(&count)
	if count != 0 {
		t.Fatal("protocol notice was also enqueued as a chat message")
	}
	if !journal.processHistoryNotification(context.Background()) {
		t.Fatal("inline notice failed")
	}
	var metadata []byte
	journal.db.QueryRow(`SELECT payload FROM history_notifications`).Scan(&metadata)
	decoded := &waE2E.HistorySyncNotification{}
	if err := proto.Unmarshal(metadata, decoded); err != nil || decoded.InitialHistBootstrapInlinePayload != nil {
		t.Fatal("inline history retained twice after durable conversion")
	}
	journal.processOne(context.Background())
	if !journal.processHistoryNotification(context.Background()) || fake.cleanups != 0 {
		t.Fatal("inline history tried to delete a nonexistent remote object")
	}
}

func TestHistoryConversionReusesReservedSlotAtQueueLimit(t *testing.T) {
	_, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	journal.historyClient = fake
	journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture())
	_, err := journal.db.Exec(`WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<?)
		INSERT INTO history_notifications(id,message_id,payload,received_at,retry_at)
		SELECT printf('synthetic-%d',n),'synthetic',x'','9999-01-01T00:00:00Z','9999-01-01T00:00:00Z' FROM ids`, maxIngestionBatches-1)
	if err != nil {
		t.Fatal(err)
	}
	if !journal.processHistoryNotification(context.Background()) {
		t.Fatal("full queue prevented converting an already admitted notification")
	}
	var count int
	journal.db.QueryRow(`SELECT COUNT(*) FROM ingestion_queue`).Scan(&count)
	if count != 1 {
		t.Fatal("reserved notification slot was not converted")
	}
}

func TestHistoryAdmissionReservesParsedBatchHeadroom(t *testing.T) {
	_, journal := ingestionFixture(t)
	_, err := journal.db.Exec(`INSERT INTO ingestion_queue(payload,received_at,entries,retry_at) VALUES(zeroblob(?),'2000-01-01T00:00:00Z',0,'2000-01-01T00:00:00Z')`, maxIngestionBytes-maxIngestionBatchBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture()); err == nil {
		t.Fatal("notification consumed the reserved parsed-batch headroom")
	}
}

func TestSlowHistoryDownloadDoesNotBlockLiveArchiveAndShutdown(t *testing.T) {
	store, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	fake.started, fake.release = make(chan struct{}), make(chan struct{})
	journal.historyClient = fake
	journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture())
	journal.start()
	select {
	case <-fake.started:
	case <-time.After(2 * time.Second):
		t.Fatal("downloader did not start")
	}
	journal.enqueue(incomingFixture("live-during-download"))
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		store.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE id='live-during-download'`).Scan(&count)
		if count == 1 {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("slow download blocked live archive commits")
		}
	}
	closed := make(chan struct{})
	go func() { journal.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("download ignored shutdown cancellation")
	}
}

func TestDownloadedHistorySurvivesBothStorageAndCleanupRestarts(t *testing.T) {
	store, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	journal.historyClient = fake
	journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture())
	journal.processHistoryNotification(context.Background())
	journal.close()
	restarted, err := newIngestionJournal(store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	restarted.historyClient = fake
	if restarted.processHistoryNotification(context.Background()) || fake.downloads != 1 || fake.cleanups != 0 {
		t.Fatal("uncommitted history was redownloaded or deleted after restart")
	}
	if !restarted.processOne(context.Background()) {
		t.Fatal("downloaded batch did not survive restart")
	}
	restarted.close()
	cleanup, err := newIngestionJournal(store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup.close()
	cleanup.historyClient = fake
	if !cleanup.processHistoryNotification(context.Background()) || fake.downloads != 1 || fake.cleanups != 1 {
		t.Fatal("archive-committed notification did not resume cleanup")
	}
}

func TestLargerInitialHistoryBatchIsStoredWithoutLosingEntries(t *testing.T) {
	store, journal := ingestionFixture(t)
	fake := historyDownloadFixture()
	fake.history.Conversations[0].Messages = nil
	for i := 2000; i > 0; i-- {
		fake.history.Conversations[0].Messages = append(fake.history.Conversations[0].Messages, historyMessage(fmt.Sprintf("synthetic-history-%d", i), "synthetic large history batch", uint64(i)))
	}
	journal.historyClient = fake
	journal.enqueueHistoryNotification("synthetic-notice", historyNotificationFixture())
	if !journal.processHistoryNotification(context.Background()) || !journal.processOne(context.Background()) {
		t.Fatal("large initial batch failed to commit")
	}
	var count int
	store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count)
	if count != 2000 {
		t.Fatalf("large history batch stored %d of 2000 entries", count)
	}
}
