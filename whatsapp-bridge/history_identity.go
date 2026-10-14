package main

import (
	"database/sql"
	"errors"
	"strings"
	"time"
)

func aliasQuery(aliases []string) (string, []interface{}, error) {
	if len(aliases) == 0 || len(aliases) > 2 {
		return "", nil, errors.New("invalid known alias set")
	}
	params := make([]interface{}, len(aliases))
	for index, alias := range aliases {
		if alias == "" {
			return "", nil, errors.New("empty known alias")
		}
		params[index] = alias
	}
	return strings.TrimSuffix(strings.Repeat("?,", len(aliases)), ","), params, nil
}

func (store *MessageStore) rememberHistoryIdentifier(aliases []string, identifier string) error {
	if !containsIdentifier(aliases, identifier) {
		return errors.New("request identifier must be a known alias")
	}
	if _, _, err := aliasQuery(aliases); err != nil {
		return err
	}
	tx, err := store.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	for _, alias := range aliases {
		_, err = tx.Exec(`INSERT INTO history_request_preferences(chat_jid,request_jid,observed_at) VALUES(?,?,?)
			ON CONFLICT(chat_jid) DO UPDATE SET request_jid=excluded.request_jid,observed_at=excluded.observed_at`, alias, identifier, now)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (store *MessageStore) preferredHistoryIdentifier(aliases []string, fallback string) (string, error) {
	marks, params, err := aliasQuery(aliases)
	if err != nil || !containsIdentifier(aliases, fallback) {
		return "", errors.New("invalid request alias set")
	}
	var preferred string
	err = store.db.QueryRow(`SELECT request_jid FROM history_request_preferences WHERE chat_jid IN (`+marks+`) ORDER BY julianday(observed_at) DESC LIMIT 1`, params...).Scan(&preferred)
	if errors.Is(err, sql.ErrNoRows) {
		return fallback, nil
	}
	if err != nil {
		return "", err
	}
	if !containsIdentifier(aliases, preferred) {
		return fallback, nil
	}
	return preferred, nil
}

func (store *MessageStore) oldestHistoryAnchor(aliases []string) (historyCursor, error) {
	var cursor historyCursor
	marks, params, err := aliasQuery(aliases)
	if err != nil {
		return cursor, err
	}
	err = store.db.QueryRow(`SELECT id,sender,timestamp,is_from_me FROM messages WHERE chat_jid IN (`+marks+`)
		AND id<>'' AND length(id)<=256 AND julianday(timestamp)>julianday('1970-01-01')
		ORDER BY julianday(timestamp),rowid LIMIT 1`, params...).Scan(&cursor.ID, &cursor.Sender, &cursor.Timestamp, &cursor.FromMe)
	return cursor, err
}

type historyCoverage struct {
	Rows        int        `json:"stored_rows"`
	Messages    int        `json:"unique_message_ids"`
	Oldest      *time.Time `json:"oldest_stored_at"`
	Newest      *time.Time `json:"newest_stored_at"`
	SnapshotRow int64      `json:"snapshot_row_id"`
}

func (store *MessageStore) historyCoverage(aliases []string) (*historyCoverage, error) {
	marks, params, err := aliasQuery(aliases)
	if err != nil {
		return nil, err
	}
	tx, err := store.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	coverage := &historyCoverage{}
	err = tx.QueryRow(`SELECT COALESCE(MAX(rowid),0) FROM messages`).Scan(&coverage.SnapshotRow)
	if err != nil {
		return nil, err
	}
	var oldest, newest sql.NullString
	err = tx.QueryRow(`SELECT COUNT(*),COUNT(DISTINCT NULLIF(id,'')),strftime('%Y-%m-%dT%H:%M:%fZ',MIN(julianday(timestamp))),strftime('%Y-%m-%dT%H:%M:%fZ',MAX(julianday(timestamp))) FROM messages WHERE chat_jid IN (`+marks+`)`, params...).Scan(&coverage.Rows, &coverage.Messages, &oldest, &newest)
	if err != nil {
		return nil, err
	}
	for index, value := range []sql.NullString{oldest, newest} {
		if !value.Valid {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, value.String)
		if err != nil {
			return nil, err
		}
		if index == 0 {
			coverage.Oldest = &parsed
		} else {
			coverage.Newest = &parsed
		}
	}
	return coverage, tx.Commit()
}

func (store *MessageStore) newOlderHistoryMessages(aliases []string, before *historyCoverage, anchor time.Time) (int, error) {
	marks, params, err := aliasQuery(aliases)
	if err != nil || before == nil {
		return 0, errors.New("invalid coverage boundary")
	}
	params = append(params, before.SnapshotRow, anchor)
	for _, alias := range aliases {
		params = append(params, alias)
	}
	params = append(params, before.SnapshotRow)
	var count int
	err = store.db.QueryRow(`SELECT COUNT(DISTINCT m.id) FROM messages m WHERE m.chat_jid IN (`+marks+`) AND m.rowid>?
		AND julianday(m.timestamp)<julianday(?) AND m.id<>'' AND NOT EXISTS
		(SELECT 1 FROM messages old WHERE old.id=m.id AND old.chat_jid IN (`+marks+`) AND old.rowid<=?)`, params...).Scan(&count)
	return count, err
}

func responsiveHistoryResult(result string) bool {
	switch result {
	case "progress_observed", "no_backward_progress_observed", "phone_reports_no_older_messages", "older_messages_not_accessible", "no_backward_progress", "progress":
		return true
	default:
		return false
	}
}

func consolidateBackfillChats(store *MessageStore, report *backfillReport, aliasesFor func(string) ([]string, error), resume bool) error {
	leaders := make(map[string]*backfillChat)
	for _, chat := range report.Chats {
		aliases, err := aliasesFor(chat.JID)
		if err != nil {
			return err
		}
		var leader *backfillChat
		for _, alias := range aliases {
			if leaders[alias] != nil {
				leader = leaders[alias]
				break
			}
		}
		if leader != nil {
			chat.LinkedTo = leader.JID
			if chat.Cursor.ID != "" {
				useCursor := leader.Cursor.ID == ""
				if resume && chat.Cursor.Timestamp.Before(leader.Cursor.Timestamp) {
					useCursor = true
				}
				if !resume && chat.Cursor.Timestamp.After(leader.Cursor.Timestamp) {
					useCursor = true
				}
				if useCursor {
					leader.Cursor = chat.Cursor
				}
			}
			if chat.Status == "pending" {
				leader.Status = "pending"
			}
			continue
		}
		chat.LinkedTo = ""
		chat.Aliases = aliases
		fallback := chat.JID
		for _, record := range report.Chats {
			if containsIdentifier(aliases, record.JID) && record.Received > 0 && responsiveHistoryResult(record.Status) {
				fallback = record.JID
				break
			}
		}
		chat.RequestJID, err = store.preferredHistoryIdentifier(aliases, fallback)
		if err != nil {
			return err
		}
		if resume && report.Recovery == nil {
			oldest, err := store.oldestHistoryAnchor(aliases)
			if err == nil && (chat.Cursor.ID == "" || oldest.Timestamp.Before(chat.Cursor.Timestamp)) {
				chat.Cursor = oldest
			} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		for _, alias := range aliases {
			leaders[alias] = chat
		}
	}
	return nil
}
