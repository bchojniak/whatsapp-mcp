package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// These migrations add derived indexes; original message rows and keys stay intact.
func migrateSearchStore(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		CREATE INDEX IF NOT EXISTS messages_chat_time_idx ON messages(chat_jid, julianday(timestamp), id);
		CREATE INDEX IF NOT EXISTS messages_sender_time_idx ON messages(sender, julianday(timestamp), id);
		CREATE INDEX IF NOT EXISTS messages_time_idx ON messages(julianday(timestamp), id, chat_jid);
		CREATE TABLE IF NOT EXISTS archive_migrations (name TEXT PRIMARY KEY);
		CREATE TABLE IF NOT EXISTS archive_search_state (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL, content_revision INTEGER NOT NULL);
		INSERT INTO archive_search_state VALUES(1, 0, 0) ON CONFLICT(id) DO NOTHING;
	`); err != nil {
		return err
	}
	var enabled bool
	if err := tx.QueryRow(`SELECT sqlite_compileoption_used('ENABLE_FTS5')`).Scan(&enabled); err != nil {
		return err
	}
	// Plain Go builds remain usable; production builds enable sqlite_fts5 explicitly.
	if enabled {
		if _, err := tx.Exec(`
			CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(content, filename, content='messages', content_rowid='rowid', tokenize='unicode61 remove_diacritics 2');
			CREATE TRIGGER IF NOT EXISTS messages_fts_insert AFTER INSERT ON messages BEGIN
				INSERT INTO messages_fts(rowid, content, filename) VALUES(new.rowid, new.content, new.filename);
				UPDATE archive_search_state SET revision=revision+1 WHERE id=1;
			END;
			CREATE TRIGGER IF NOT EXISTS messages_fts_delete AFTER DELETE ON messages BEGIN
				INSERT INTO messages_fts(messages_fts, rowid, content, filename) VALUES('delete', old.rowid, old.content, old.filename);
				UPDATE archive_search_state SET revision=revision+1, content_revision=content_revision+1 WHERE id=1;
			END;
			CREATE TRIGGER IF NOT EXISTS messages_fts_update AFTER UPDATE ON messages BEGIN
				INSERT INTO messages_fts(messages_fts, rowid, content, filename) VALUES('delete', old.rowid, old.content, old.filename);
				INSERT INTO messages_fts(rowid, content, filename) VALUES(new.rowid, new.content, new.filename);
				UPDATE archive_search_state SET revision=revision+1, content_revision=content_revision+1 WHERE id=1;
			END;
		`); err != nil {
			return err
		}
		var migrated bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM archive_migrations WHERE name='messages_fts_v1')`).Scan(&migrated); err != nil {
			return err
		}
		if !migrated {
			if _, err := tx.Exec(`INSERT INTO messages_fts(messages_fts) VALUES('rebuild'); INSERT INTO archive_migrations VALUES('messages_fts_v1');`); err != nil {
				return fmt.Errorf("message search index migration: %w", err)
			}
		}
	}
	return tx.Commit()
}

type syncObservation struct {
	mu         sync.Mutex
	lastEvent  *time.Time
	lastStored *time.Time
}

func (observation *syncObservation) record(stored bool) {
	now := time.Now().UTC()
	observation.mu.Lock()
	defer observation.mu.Unlock()
	observation.lastEvent = &now
	if stored {
		observation.lastStored = &now
	}
}

func (observation *syncObservation) snapshot() map[string]interface{} {
	observation.mu.Lock()
	defer observation.mu.Unlock()
	return map[string]interface{}{
		"last_event_at":           observation.lastEvent,
		"last_successful_sync_at": observation.lastStored,
		"observation_scope":       "current_process",
	}
}
