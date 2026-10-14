package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type historyDownloadClient interface {
	IsConnected() bool
	DownloadHistorySync(context.Context, *waE2E.HistorySyncNotification, bool) (*waHistorySync.HistorySync, error)
	SendProtocolMessageReceipt(context.Context, types.MessageID, types.ReceiptType) error
	DeleteMedia(context.Context, whatsmeow.MediaType, string, []byte, string) error
}

type historyTransferState struct {
	Received         int64      `json:"received_notifications"`
	FailedAttempts   int64      `json:"failed_attempts"`
	LastError        string     `json:"last_error,omitempty"`
	LastReceived     *time.Time `json:"last_received_at"`
	LastDownloaded   *time.Time `json:"last_downloaded_at"`
	LastCommitted    *time.Time `json:"last_committed_at"`
	InitialReceived  *time.Time `json:"initial_received_at"`
	InitialCommitted *time.Time `json:"initial_committed_at"`
	TransferType     string     `json:"transfer_type,omitempty"`
	TransferProgress *uint32    `json:"transfer_progress,omitempty"`
	Phase            string     `json:"phase,omitempty"`
}

type historyNotification struct {
	id        string
	messageID string
	data      []byte
	status    string
	attempts  int
	receipted bool
}

func historyNotice(event *events.Message) *waE2E.HistorySyncNotification {
	if event == nil || !event.Info.IsFromMe {
		return nil
	}
	return event.Message.GetProtocolMessage().GetHistorySyncNotification()
}

func (journal *ingestionJournal) enqueueIncomingMessage(event *events.Message, diagnostics *historyDiagnostics) error {
	if diagnostics != nil {
		diagnostics.recordProtocol(event)
	}
	journal.store.syncObservation.record(false)
	if notice := historyNotice(event); notice != nil {
		return journal.enqueueHistoryNotification(event.Info.ID, notice)
	}
	payload, err := captureMessage(event)
	if err != nil {
		journal.rejectEvent("invalid_message_payload")
		return err
	}
	return journal.enqueue(payload)
}

func (journal *ingestionJournal) enqueueHistoryNotification(messageID types.MessageID, notice *waE2E.HistorySyncNotification) error {
	if notice == nil || messageID == "" {
		return errors.New("invalid history notification")
	}
	data, err := proto.MarshalOptions{AllowPartial: true, Deterministic: true}.Marshal(notice)
	if err != nil || len(data) > maxIngestionBatchBytes {
		journal.rejectEvent("invalid_history_payload")
		return errors.New("invalid history notification")
	}
	digest := sha256.Sum256(data)
	id := hex.EncodeToString(digest[:])
	journal.mu.Lock()
	defer journal.mu.Unlock()
	tx, err := journal.db.Begin()
	if err != nil {
		journal.rejectLocked(err)
		return err
	}
	defer tx.Rollback()
	var exists int
	err = tx.QueryRow(`SELECT COUNT(*) FROM history_notifications WHERE id=?`, id).Scan(&exists)
	if err == nil && exists > 0 {
		return nil
	}
	if err == nil {
		err = checkHistoryQueueCapacity(tx, len(data)+maxIngestionBatchBytes, nil)
	}
	now := time.Now().UTC()
	if err == nil {
		_, err = tx.Exec(`INSERT INTO history_notifications(id,message_id,payload,received_at,retry_at) VALUES(?,?,?,?,?)`, id, messageID, data, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	}
	next := journal.state
	next.LastReceived = &now
	next.History.Received++
	next.History.LastReceived = &now
	if notice.SyncType != nil && isInitialHistorySync(waHistorySync.HistorySync_HistorySyncType(notice.GetSyncType())) {
		next.History.InitialReceived = &now
		next.History.TransferType = notice.GetSyncType().String()
		next.History.TransferProgress = nil
		if notice.Progress != nil && notice.GetProgress() <= 100 {
			progress := notice.GetProgress()
			next.History.TransferProgress = &progress
		}
		if next.History.Phase != "backfill_ready" {
			next.History.Phase = "receiving"
		}
	}
	if err == nil {
		err = writeJournalState(tx, next)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		tx.Rollback()
		journal.rejectLocked(err)
		return err
	}
	journal.state = next
	journal.observeJournalError(nil)
	select {
	case journal.wake <- struct{}{}:
	default:
	}
	return nil
}

func checkHistoryQueueCapacity(tx *sql.Tx, incoming int, replacing *historyNotification) error {
	var count, size int64
	err := tx.QueryRow(`SELECT (SELECT COUNT(*) FROM ingestion_queue)+(SELECT COUNT(*) FROM history_notifications WHERE status='pending'), (SELECT COALESCE(SUM(length(payload)),0) FROM ingestion_queue)+(SELECT COALESCE(SUM(length(payload)),0) FROM history_notifications)`).Scan(&count, &size)
	if replacing != nil {
		count--
		size -= int64(len(replacing.data))
	}
	if err == nil && (count >= maxIngestionBatches || size+int64(incoming) > maxIngestionBytes) {
		return errors.New("queue_capacity_limit")
	}
	return err
}

func (journal *ingestionJournal) processHistoryNotification(ctx context.Context) bool {
	if journal.historyClient == nil || !journal.historyClient.IsConnected() {
		return false
	}
	journal.mu.Lock()
	notice := &historyNotification{}
	err := journal.db.QueryRow(`SELECT id,message_id,payload,status,attempts,receipted FROM history_notifications n
		WHERE julianday(retry_at)<=julianday(?) AND (status='pending' OR (status='awaiting_storage' AND NOT EXISTS(SELECT 1 FROM ingestion_queue q WHERE q.id=n.batch_id)))
		ORDER BY received_at,id LIMIT 1`, time.Now().UTC().Format(time.RFC3339Nano)).Scan(&notice.id, &notice.messageID, &notice.data, &notice.status, &notice.attempts, &notice.receipted)
	journal.mu.Unlock()
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		return false
	}
	decoded := &waE2E.HistorySyncNotification{}
	if err = (proto.UnmarshalOptions{AllowPartial: true}).Unmarshal(notice.data, decoded); err != nil {
		journal.retryHistoryNotification(notice, "invalid_history_payload")
		return false
	}
	requestCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if notice.status == "awaiting_storage" {
		if decoded.GetDirectPath() != "" || decoded.GetEncHandle() != "" {
			err = journal.historyClient.DeleteMedia(requestCtx, whatsmeow.MediaHistory, decoded.GetDirectPath(), decoded.GetFileEncSHA256(), decoded.GetEncHandle())
			if !historyCleanupComplete(err) {
				if ctx.Err() == nil {
					journal.retryHistoryNotification(notice, "cleanup_failed")
				}
				return false
			}
		}
		journal.mu.Lock()
		_, err = journal.db.Exec(`UPDATE history_notifications SET status='done',payload=x'',message_id='',attempts=0,last_error='' WHERE id=?`, notice.id)
		journal.observeJournalError(err)
		journal.mu.Unlock()
		return err == nil
	}
	if !notice.receipted {
		if err = journal.historyClient.SendProtocolMessageReceipt(requestCtx, notice.messageID, types.ReceiptTypeHistorySync); err != nil {
			if ctx.Err() == nil {
				journal.retryHistoryNotification(notice, "receipt_failed")
			}
			return false
		}
		journal.mu.Lock()
		_, err = journal.db.Exec(`UPDATE history_notifications SET receipted=1 WHERE id=?`, notice.id)
		journal.observeJournalError(err)
		journal.mu.Unlock()
		if err != nil {
			return false
		}
	}
	history, err := journal.historyClient.DownloadHistorySync(requestCtx, decoded, true)
	if ctx.Err() != nil {
		return false
	}
	if err != nil {
		journal.retryHistoryNotification(notice, "download_failed")
		return false
	}
	payload, err := captureHistory(journal.client, &events.HistorySync{Data: history})
	if err == nil && journal.store.historyDiagnostics != nil {
		payload.HistoryTraceID = journal.store.historyDiagnostics.recordReceived(history)
	}
	if err == nil {
		err = journal.queueDownloadedHistory(notice, payload, history)
	}
	if err != nil {
		journal.retryHistoryNotification(notice, storageErrorCategory(err))
		return false
	}
	return true
}

func (journal *ingestionJournal) queueDownloadedHistory(notice *historyNotification, payload ingestionPayload, history *waHistorySync.HistorySync) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(data) > maxIngestionBatchBytes {
		return errors.New("batch_size_limit")
	}
	entries := payload.entryCount()
	metadata := &waE2E.HistorySyncNotification{}
	if err = (proto.UnmarshalOptions{AllowPartial: true}).Unmarshal(notice.data, metadata); err != nil {
		return err
	}
	metadata.InitialHistBootstrapInlinePayload = nil
	cleanupData, err := proto.MarshalOptions{AllowPartial: true, Deterministic: true}.Marshal(metadata)
	if err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	tx, err := journal.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = checkHistoryQueueCapacity(tx, len(data)+len(cleanupData), notice); err != nil {
		return err
	}
	now := time.Now().UTC()
	result, err := tx.Exec(`INSERT INTO ingestion_queue(payload,received_at,entries,retry_at) VALUES(?,?,?,?)`, data, now.Format(time.RFC3339Nano), entries, now.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	batchID, err := result.LastInsertId()
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE history_notifications SET status='awaiting_storage',batch_id=?,payload=?,attempts=0,last_error='' WHERE id=?`, batchID, cleanupData, notice.id)
	next := journal.state
	next.LastReceived = &now
	next.ReceivedBatches++
	next.ReceivedEntries += int64(entries)
	next.History.LastDownloaded = &now
	next.History.LastError = ""
	if isInitialHistorySync(history.GetSyncType()) {
		next.History.InitialReceived = &now
		next.History.TransferType = history.GetSyncType().String()
		next.History.TransferProgress = nil
		if history.Progress != nil && history.GetProgress() <= 100 {
			progress := history.GetProgress()
			next.History.TransferProgress = &progress
		}
		if next.History.Phase != "backfill_ready" {
			next.History.Phase = "waiting_for_storage"
		}
	}
	if err == nil {
		err = writeJournalState(tx, next)
	}
	if err == nil {
		err = tx.Commit()
	}
	journal.observeJournalError(err)
	if err == nil {
		journal.state = next
	}
	return err
}

func (journal *ingestionJournal) retryHistoryNotification(notice *historyNotification, category string) {
	delay := 5 * time.Second
	for i := 0; i < notice.attempts && delay < time.Minute; i++ {
		delay *= 2
	}
	if delay > time.Minute {
		delay = time.Minute
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	tx, err := journal.db.Begin()
	if err != nil {
		journal.observeJournalError(err)
		return
	}
	defer tx.Rollback()
	next := journal.state
	next.History.FailedAttempts++
	next.History.LastError = category
	_, err = tx.Exec(`UPDATE history_notifications SET attempts=attempts+1,retry_at=?,last_error=? WHERE id=?`, time.Now().UTC().Add(delay).Format(time.RFC3339Nano), category, notice.id)
	if err == nil {
		err = writeJournalState(tx, next)
	}
	if err == nil {
		err = tx.Commit()
	}
	journal.observeJournalError(err)
	if err == nil {
		journal.state = next
	}
}

func (journal *ingestionJournal) initialHistorySnapshot() (time.Time, bool, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	var pending int
	err := journal.db.QueryRow(`SELECT (SELECT COUNT(*) FROM ingestion_queue)+(SELECT COUNT(*) FROM history_notifications n WHERE status='pending')`).Scan(&pending)
	last := time.Time{}
	for _, at := range []*time.Time{journal.state.History.InitialReceived, journal.state.History.InitialCommitted} {
		if at != nil && at.After(last) {
			last = *at
		}
	}
	return last, pending > 0, err
}

func (journal *ingestionJournal) recordInitialHistoryPhase(phase string) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	journal.state.History.Phase = phase
	journal.saveState()
}

func (journal *ingestionJournal) historySnapshotLocked() (map[string]interface{}, map[string]interface{}, error) {
	var pending, awaiting, cleanup, retrying int64
	err := journal.db.QueryRow(`SELECT COALESCE(SUM(status='pending'),0),COALESCE(SUM(status='awaiting_storage' AND EXISTS(SELECT 1 FROM ingestion_queue q WHERE q.id=n.batch_id)),0),COALESCE(SUM(status='awaiting_storage' AND NOT EXISTS(SELECT 1 FROM ingestion_queue q WHERE q.id=n.batch_id)),0),COALESCE(SUM(status!='done' AND attempts>0),0) FROM history_notifications n`).Scan(&pending, &awaiting, &cleanup, &retrying)
	h := journal.state.History
	downloads := map[string]interface{}{"pending_notifications": pending, "awaiting_storage": awaiting, "pending_cleanup": cleanup, "retrying_notifications": retrying, "failed_attempts": h.FailedAttempts, "received_notifications": h.Received, "last_error": h.LastError, "last_received_at": h.LastReceived, "last_downloaded_at": h.LastDownloaded, "last_committed_at": h.LastCommitted}
	initial := map[string]interface{}{"phase": h.Phase, "last_received_at": h.InitialReceived, "last_committed_at": h.InitialCommitted, "transfer_type": h.TransferType, "transfer_progress": h.TransferProgress, "history_complete": nil, "notice": "Transfer progress describes reported batches, not complete phone history."}
	if err != nil {
		for _, key := range []string{"pending_notifications", "awaiting_storage", "pending_cleanup", "retrying_notifications"} {
			downloads[key] = nil
		}
	}
	return downloads, initial, err
}

func historyCleanupComplete(err error) bool {
	if err == nil {
		return true
	}
	return strings.TrimSpace(err.Error()) == "media delete failed with status code 404" || strings.TrimSpace(err.Error()) == "media delete failed with status code 410"
}
