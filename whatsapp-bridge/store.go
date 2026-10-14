package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

type MessageStore struct {
	db                   *sql.DB
	storeDir             string
	historyMessagesAdded atomic.Int64
	syncObservation      syncObservation
	ingestion            *ingestionJournal
	historyDiagnostics   *historyDiagnostics
}

func NewMessageStore(storeDir string) (*MessageStore, error) {
	if err := os.MkdirAll(storeDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}
	if err := os.Chmod(storeDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to secure store directory: %v", err)
	}

	dbPath := filepath.ToSlash(filepath.Join(storeDir, "messages.db"))
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_foreign_keys=on&_busy_timeout=1000", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
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
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);

		CREATE TABLE IF NOT EXISTS history_request_preferences (
			chat_jid TEXT PRIMARY KEY,
			request_jid TEXT NOT NULL,
			observed_at TIMESTAMP NOT NULL
		);

		CREATE TABLE IF NOT EXISTS message_metadata (
			message_id TEXT NOT NULL,
			chat_jid TEXT NOT NULL,
			message_type TEXT NOT NULL,
			metadata_json TEXT NOT NULL,
			source_format TEXT NOT NULL,
			source_payload BLOB NOT NULL,
			observed_at TIMESTAMP NOT NULL,
			PRIMARY KEY (message_id, chat_jid),
			FOREIGN KEY (message_id, chat_jid) REFERENCES messages(id, chat_jid)
		);
		CREATE INDEX IF NOT EXISTS message_metadata_type ON message_metadata(message_type);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}
	if err := os.Chmod(dbPath, 0600); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to secure message database: %v", err)
	}
	if err := migrateSearchStore(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize archive indexes: %w", err)
	}
	if err := migrateMessageMetadataVersions(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize source provenance: %w", err)
	}

	return &MessageStore{db: db, storeDir: storeDir}, nil
}

func (store *MessageStore) Close() error {
	return store.db.Close()
}

const storeChatSQL = `INSERT INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)
		ON CONFLICT(jid) DO UPDATE SET
		name = CASE WHEN excluded.name != '' THEN excluded.name ELSE chats.name END,
		last_message_time = CASE WHEN chats.last_message_time IS NULL OR julianday(excluded.last_message_time) > julianday(chats.last_message_time)
		THEN excluded.last_message_time ELSE chats.last_message_time END`

const storeMessageSQL = `INSERT INTO messages
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id, chat_jid) DO UPDATE SET
		sender=excluded.sender, content=excluded.content, timestamp=excluded.timestamp,
		is_from_me=excluded.is_from_me, media_type=excluded.media_type,
		filename=CASE WHEN messages.filename IS NULL OR messages.filename='' THEN excluded.filename ELSE messages.filename END,
		url=COALESCE(NULLIF(excluded.url,''), messages.url),
		media_key=COALESCE(excluded.media_key, messages.media_key),
		file_sha256=COALESCE(excluded.file_sha256, messages.file_sha256),
		file_enc_sha256=COALESCE(excluded.file_enc_sha256, messages.file_enc_sha256),
		file_length=CASE WHEN excluded.file_length>0 THEN excluded.file_length ELSE messages.file_length END`

func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		storeChatSQL,
		jid, name, lastMessageTime,
	)
	return err
}

func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	if content == "" && mediaType == "" {
		return nil
	}

	_, err := store.db.Exec(
		storeMessageSQL,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	)
	return err
}

func (store *MessageStore) GetNewestMessage(chatJID string) (id, sender string, ts time.Time, isFromMe bool, err error) {
	err = store.db.QueryRow(
		`SELECT id, sender, timestamp, is_from_me FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT 1`,
		chatJID,
	).Scan(&id, &sender, &ts, &isFromMe)
	return
}

func (store *MessageStore) GetOldestMessage(chatJID string) (id, sender string, ts time.Time, isFromMe bool, err error) {
	err = store.db.QueryRow(
		`SELECT id, sender, timestamp, is_from_me FROM messages WHERE chat_jid = ? AND id != '' ORDER BY timestamp ASC, id ASC LIMIT 1`,
		chatJID,
	).Scan(&id, &sender, &ts, &isFromMe)
	return
}

func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		err := rows.Scan(&msg.Sender, &msg.Content, &msg.Time, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64

	err := store.db.QueryRow(
		"SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)

	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}
