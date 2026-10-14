package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
)

const maxIngestionBytes = 256 << 20
const maxIngestionBatchBytes = 64 << 20
const maxIngestionBatches = 100000

type connectionObservation struct {
	At     time.Time `json:"at"`
	State  string    `json:"state"`
	Reason string    `json:"reason"`
}

type ingestionState struct {
	ProcessStartedAt  time.Time               `json:"process_started_at"`
	StartedAt         time.Time               `json:"started_at"`
	Starts            int64                   `json:"starts"`
	UncleanRestarts   int64                   `json:"unclean_restarts"`
	CleanShutdown     bool                    `json:"clean_shutdown"`
	LastShutdown      *time.Time              `json:"last_shutdown_at"`
	LastReceived      *time.Time              `json:"last_received_at"`
	LastCommitted     *time.Time              `json:"last_committed_at"`
	ReceivedBatches   int64                   `json:"received_batches"`
	ReceivedEntries   int64                   `json:"received_entries"`
	CommittedBatches  int64                   `json:"committed_batches"`
	CommittedEntries  int64                   `json:"committed_message_entries"`
	FailedWrites      int64                   `json:"failed_write_attempts"`
	RecoveredBatches  int64                   `json:"recovered_batches"`
	UnpersistedEvents int64                   `json:"unpersisted_events"`
	JournalFailures   int64                   `json:"journal_failures"`
	LastWriteFailure  *time.Time              `json:"last_write_failure_at"`
	LastJournalError  string                  `json:"last_journal_error,omitempty"`
	LastWriteError    string                  `json:"last_write_error,omitempty"`
	Disconnects       int64                   `json:"disconnects"`
	Reconnects        int64                   `json:"reconnects"`
	LastConnected     *time.Time              `json:"last_connected_at"`
	LastDisconnected  *time.Time              `json:"last_disconnected_at"`
	ConnectionEvents  []connectionObservation `json:"recent_connection_events"`
	History           historyTransferState    `json:"history"`
}

type queuedIngestion struct {
	id       int64
	data     []byte
	attempts int
	retryAt  time.Time
}

// The journal is independent of messages.db. Enqueue commits before an event
// handler returns; archive commit precedes journal deletion. A crash between
// these commits replays the batch using the archive's existing message keys.
type ingestionJournal struct {
	mu               sync.Mutex
	db               *sql.DB
	store            *MessageStore
	client           *whatsmeow.Client
	historyClient    historyDownloadClient
	state            ingestionState
	available        bool
	connectionState  string
	connectionReason string
	onHistory        func(*events.HistorySync, error)
	wake             chan struct{}
	done             chan struct{}
	cancel           context.CancelFunc
}

func newIngestionJournal(store *MessageStore, client *whatsmeow.Client, onHistory func(*events.HistorySync, error)) (*ingestionJournal, error) {
	path := filepath.ToSlash(filepath.Join(store.storeDir, "ingestion.db"))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = file.Chmod(0600); err != nil {
		file.Close()
		return nil, err
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_busy_timeout=1000&_journal_mode=WAL&_synchronous=FULL", path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	journal := &ingestionJournal{db: db, store: store, client: client, onHistory: onHistory, wake: make(chan struct{}, 1), available: true}
	if client != nil {
		journal.historyClient = client
	}
	if _, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS ingestion_state(id INTEGER PRIMARY KEY CHECK(id=1),data BLOB NOT NULL);
		CREATE TABLE IF NOT EXISTS ingestion_queue(
			id INTEGER PRIMARY KEY AUTOINCREMENT, payload BLOB NOT NULL, received_at TEXT NOT NULL,
			entries INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
			retry_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', last_error TEXT NOT NULL DEFAULT '');
		CREATE INDEX IF NOT EXISTS ingestion_queue_status ON ingestion_queue(status,id);
		CREATE TABLE IF NOT EXISTS history_notifications(
			id TEXT PRIMARY KEY, message_id TEXT NOT NULL, payload BLOB NOT NULL,received_at TEXT NOT NULL,
			retry_at TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',batch_id INTEGER,
			receipted INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '');
	`); err != nil {
		db.Close()
		return nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		db.Close()
		return nil, err
	}
	var data []byte
	err = db.QueryRow(`SELECT data FROM ingestion_state WHERE id=1`).Scan(&data)
	if err == nil {
		err = json.Unmarshal(data, &journal.state)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		db.Close()
		return nil, err
	}
	if journal.state.Starts > 0 && !journal.state.CleanShutdown {
		journal.state.UncleanRestarts++
	}
	if journal.state.Starts == 0 {
		journal.state.StartedAt = time.Now().UTC()
	}
	journal.state.Starts++
	journal.state.ProcessStartedAt = time.Now().UTC()
	journal.state.CleanShutdown = false
	if err = journal.saveState(); err != nil {
		db.Close()
		return nil, err
	}
	store.ingestion = journal
	return journal, nil
}

func (journal *ingestionJournal) saveState() error {
	data, err := json.Marshal(journal.state)
	if err == nil {
		_, err = journal.db.Exec(`INSERT INTO ingestion_state(id,data) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data`, data)
	}
	journal.observeJournalError(err)
	return err
}

func (journal *ingestionJournal) observeJournalError(err error) {
	journal.available = err == nil
	if err != nil {
		journal.state.JournalFailures++
		journal.state.LastJournalError = storageErrorCategory(err)
		fmt.Printf("INGESTION journal_failure category=%s\n", journal.state.LastJournalError)
	}
}

func (journal *ingestionJournal) rejectEvent(category string) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	journal.state.UnpersistedEvents++
	journal.state.LastJournalError = category
	journal.saveState()
	fmt.Printf("INGESTION event_not_persisted category=%s\n", category)
}

func (journal *ingestionJournal) enqueue(payload ingestionPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		journal.rejectEvent("invalid_payload")
		return err
	}
	if len(data) > maxIngestionBatchBytes {
		journal.rejectEvent("batch_size_limit")
		return errors.New("ingestion batch size limit")
	}
	entries := payload.entryCount()
	journal.mu.Lock()
	defer journal.mu.Unlock()
	now := time.Now().UTC()
	tx, err := journal.db.Begin()
	if err != nil {
		journal.rejectLocked(err)
		return err
	}
	defer tx.Rollback()
	err = checkHistoryQueueCapacity(tx, len(data), nil)
	if err == nil {
		_, err = tx.Exec(`INSERT INTO ingestion_queue(payload,received_at,entries,retry_at) VALUES(?,?,?,?)`, data, now.Format(time.RFC3339Nano), entries, now.Format(time.RFC3339Nano))
	}
	next := journal.state
	next.LastReceived = &now
	next.ReceivedBatches++
	next.ReceivedEntries += int64(entries)
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

func (journal *ingestionJournal) rejectLocked(err error) {
	journal.state.UnpersistedEvents++
	journal.observeJournalError(err)
	journal.saveState()
}

func writeJournalState(tx *sql.Tx, state ingestionState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE ingestion_state SET data=? WHERE id=1`, data)
	return err
}

func (journal *ingestionJournal) recordConnection(state, reason string) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	now := time.Now().UTC()
	previous := journal.connectionState
	if previous == state && journal.connectionReason == reason {
		return
	}
	journal.connectionState = state
	journal.connectionReason = reason
	if state == "connected" {
		if journal.state.LastConnected != nil {
			journal.state.Reconnects++
		}
		journal.state.LastConnected = &now
	} else {
		if previous == "connected" || journal.state.LastDisconnected == nil {
			journal.state.Disconnects++
		}
		journal.state.LastDisconnected = &now
	}
	journal.state.ConnectionEvents = append(journal.state.ConnectionEvents, connectionObservation{At: now, State: state, Reason: reason})
	if len(journal.state.ConnectionEvents) > 100 {
		journal.state.ConnectionEvents = journal.state.ConnectionEvents[len(journal.state.ConnectionEvents)-100:]
	}
	journal.saveState()
}

func (journal *ingestionJournal) start() {
	ctx, cancel := context.WithCancel(context.Background())
	journal.cancel = cancel
	journal.done = make(chan struct{})
	var workers sync.WaitGroup
	// One sequential downloader and one archive writer. Slow network downloads
	// must not stop live messages from being committed from the durable queue.
	for _, process := range []func(context.Context) bool{journal.processOne, journal.processHistoryNotification} {
		workers.Add(1)
		go func(process func(context.Context) bool) {
			defer workers.Done()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				for ctx.Err() == nil && process(ctx) {
				}
				select {
				case <-ctx.Done():
					return
				case <-journal.wake:
				case <-ticker.C:
				}
			}
		}(process)
	}
	go func() { workers.Wait(); close(journal.done) }()
}

func (journal *ingestionJournal) close() error {
	if journal.cancel != nil {
		journal.cancel()
		<-journal.done
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	now := time.Now().UTC()
	journal.state.CleanShutdown = true
	journal.state.LastShutdown = &now
	err := journal.saveState()
	closeErr := journal.db.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (journal *ingestionJournal) nextBatch() (*queuedIngestion, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	batch := &queuedIngestion{}
	var retry string
	err := journal.db.QueryRow(`SELECT id,payload,attempts,retry_at FROM ingestion_queue WHERE status='pending' ORDER BY id LIMIT 1`).Scan(&batch.id, &batch.data, &batch.attempts, &retry)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		journal.observeJournalError(err)
		return nil, err
	}
	batch.retryAt, err = time.Parse(time.RFC3339Nano, retry)
	if err != nil {
		return nil, err
	}
	return batch, nil
}

func (journal *ingestionJournal) processOne(ctx context.Context) bool {
	batch, err := journal.nextBatch()
	if err != nil || batch == nil || time.Now().Before(batch.retryAt) {
		return false
	}
	payload, err := decodeIngestion(batch.data)
	var history *events.HistorySync
	stored := 0
	if err == nil {
		history, stored, err = journal.store.applyIngestion(ctx, journal.client, payload)
	}
	if ctx.Err() != nil {
		return false
	}
	if err != nil {
		retry := retryableStorageError(err)
		if journal.store.historyDiagnostics != nil && payload.HistoryTraceID != "" {
			journal.store.historyDiagnostics.recordStorageError(payload.HistoryTraceID, storageErrorCategory(err), !retry)
		}
		if journal.failBatch(batch, err, retry) != nil {
			return false
		}
		if !retry && history != nil && journal.onHistory != nil {
			journal.onHistory(history, err)
		}
		return !retry
	}
	if journal.completeBatch(batch, stored, history) != nil {
		return false
	}
	journal.store.syncObservation.record(true)
	if history != nil && journal.onHistory != nil {
		journal.onHistory(history, nil)
	}
	return true
}

func (journal *ingestionJournal) failBatch(batch *queuedIngestion, failure error, retry bool) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	now := time.Now().UTC()
	category := storageErrorCategory(failure)
	status := "failed"
	delay := time.Second
	if retry {
		status = "pending"
		for i := 0; i < batch.attempts && delay < 30*time.Second; i++ {
			delay *= 2
		}
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
	}
	tx, err := journal.db.Begin()
	if err != nil {
		journal.observeJournalError(err)
		return err
	}
	defer tx.Rollback()
	next := journal.state
	next.FailedWrites++
	next.LastWriteFailure = &now
	next.LastWriteError = category
	_, err = tx.Exec(`UPDATE ingestion_queue SET status=?,attempts=attempts+1,retry_at=?,last_error=? WHERE id=?`, status, now.Add(delay).Format(time.RFC3339Nano), category, batch.id)
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
	fmt.Printf("INGESTION archive_write_failed category=%s retry=%t\n", category, retry)
	return err
}

func (journal *ingestionJournal) completeBatch(batch *queuedIngestion, stored int, history *events.HistorySync) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	tx, err := journal.db.Begin()
	if err != nil {
		journal.observeJournalError(err)
		return err
	}
	defer tx.Rollback()
	next := journal.state
	now := time.Now().UTC()
	next.CommittedBatches++
	next.CommittedEntries += int64(stored)
	if stored > 0 {
		next.LastCommitted = &now
	}
	if batch.attempts > 0 {
		next.RecoveredBatches++
	}
	if history != nil {
		next.History.LastCommitted = &now
		if isInitialHistorySync(history.Data.GetSyncType()) {
			next.History.InitialCommitted = &now
		}
	}
	_, err = tx.Exec(`DELETE FROM ingestion_queue WHERE id=?`, batch.id)
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

func storageErrorCategory(err error) string {
	var sqliteError sqlite3.Error
	if errors.As(err, &sqliteError) {
		switch sqliteError.Code {
		case sqlite3.ErrBusy, sqlite3.ErrLocked:
			return "database_locked"
		case sqlite3.ErrFull:
			return "disk_full"
		case sqlite3.ErrInterrupt:
			return "write_timeout"
		case sqlite3.ErrIoErr:
			return "storage_io_error"
		case sqlite3.ErrReadonly:
			return "database_read_only"
		case sqlite3.ErrCantOpen:
			return "database_unavailable"
		case sqlite3.ErrCorrupt, sqlite3.ErrNotADB:
			return "database_corrupt"
		case sqlite3.ErrConstraint:
			return "constraint_failed"
		default:
			return "database_error"
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "write_timeout"
	}
	if err.Error() == "queue_capacity_limit" {
		return "queue_capacity_limit"
	}
	if err.Error() == "batch_size_limit" {
		return "batch_size_limit"
	}
	return "invalid_payload_or_storage_error"
}

func retryableStorageError(err error) bool {
	switch storageErrorCategory(err) {
	case "database_locked", "disk_full", "storage_io_error", "database_read_only", "database_unavailable", "write_timeout":
		return true
	default:
		return false
	}
}

func (journal *ingestionJournal) snapshot() map[string]interface{} {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	var pending, entries, failed, bytes int64
	var oldest sql.NullString
	err := journal.db.QueryRow(`SELECT COALESCE(SUM(status='pending'),0),COALESCE(SUM(CASE WHEN status='pending' THEN entries ELSE 0 END),0),COALESCE(SUM(status='failed'),0),COALESCE(SUM(length(payload)),0),MIN(CASE WHEN status='pending' THEN received_at END) FROM ingestion_queue`).Scan(&pending, &entries, &failed, &bytes, &oldest)
	status := "healthy"
	age := float64(0)
	var oldestAt *time.Time
	if oldest.Valid {
		parsed, parseErr := time.Parse(time.RFC3339Nano, oldest.String)
		if parseErr == nil {
			oldestAt = &parsed
			age = time.Since(parsed).Seconds()
			if age < 0 {
				age = 0
			}
		}
	}
	downloads, initial, historyErr := journal.historySnapshotLocked()
	if pending > 0 || downloads["pending_notifications"] != int64(0) {
		status = "buffering"
	}
	if historyErr != nil || downloads["retrying_notifications"] != int64(0) {
		status = "degraded"
	}
	if failed > 0 || journal.state.UnpersistedEvents > 0 || !journal.available || err != nil || age >= 60 {
		status = "degraded"
	}
	ingestion := map[string]interface{}{
		"status": status, "diagnostics_available": err == nil && historyErr == nil && journal.available,
		"process_started_at": journal.state.ProcessStartedAt, "observation_started_at": journal.state.StartedAt, "starts": journal.state.Starts,
		"restart_count": journal.state.Starts - 1, "unclean_restart_count": journal.state.UncleanRestarts,
		"last_shutdown_at": journal.state.LastShutdown, "last_received_at": journal.state.LastReceived,
		"last_committed_at": journal.state.LastCommitted,
		"received_batches":  journal.state.ReceivedBatches, "received_entries": journal.state.ReceivedEntries,
		"committed_batches": journal.state.CommittedBatches, "committed_message_entries": journal.state.CommittedEntries,
		"pending_batches": pending, "pending_entries": entries, "failed_batches": failed,
		"queue_bytes": bytes, "queue_limit_bytes": maxIngestionBytes, "batch_limit_bytes": maxIngestionBatchBytes,
		"oldest_pending_at": oldestAt, "oldest_pending_age_seconds": age,
		"failed_write_attempts": journal.state.FailedWrites, "recovered_batches": journal.state.RecoveredBatches,
		"unpersisted_events": journal.state.UnpersistedEvents, "journal_failures": journal.state.JournalFailures,
		"last_write_failure_at": journal.state.LastWriteFailure, "last_write_error": journal.state.LastWriteError,
		"last_journal_error": journal.state.LastJournalError,
		"disconnect_count":   journal.state.Disconnects, "reconnect_count": journal.state.Reconnects,
		"last_connected_at": journal.state.LastConnected, "last_disconnected_at": journal.state.LastDisconnected,
		"recent_connection_events": append([]connectionObservation{}, journal.state.ConnectionEvents...),
		"history_downloads":        downloads, "initial_history": initial,
		"coverage_notice": "Counters cover events observed since this journal was created, not unique messages or complete WhatsApp delivery. A healthy queue does not prove full history.",
	}
	if err != nil {
		for _, key := range []string{"pending_batches", "pending_entries", "failed_batches", "queue_bytes", "oldest_pending_age_seconds"} {
			ingestion[key] = nil
		}
	}
	return map[string]interface{}{"observation_scope": "persistent", "last_event_at": journal.state.LastReceived, "last_successful_sync_at": journal.state.LastCommitted, "ingestion": ingestion}
}
