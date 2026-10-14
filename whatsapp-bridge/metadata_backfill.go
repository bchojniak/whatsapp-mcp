package main

import (
	"context"
	"database/sql"
	"errors"
)

const archiveCaptureVersion = 1

type metadataReplayState struct {
	Cursor           historyCursor `json:"cursor"`
	Until            historyCursor `json:"until"`
	Status           string        `json:"status"`
	LastResult       string        `json:"last_result,omitempty"`
	Pages            int           `json:"requests"`
	AutomaticRetries int           `json:"automatic_retries"`
}

// For runs below archiveCaptureVersion, replay only previously requested ranges.
// Preserve history cursors and counters; unrequested chats use normal capture.
func initializeMetadataReplay(store *MessageStore, report *backfillReport) error {
	if report.CaptureVersion >= archiveCaptureVersion {
		return nil
	}
	for _, chat := range report.Chats {
		if chat.LinkedTo != "" || chat.Pages == 0 || chat.Status == "unsupported_chat" || chat.Status == "no_anchor" {
			continue
		}
		aliases := chat.Aliases
		if len(aliases) == 0 {
			aliases = []string{chat.JID}
		}
		marks, params, err := aliasQuery(aliases)
		if err != nil {
			return err
		}
		var cursor historyCursor
		err = store.db.QueryRow(`SELECT id,sender,timestamp,is_from_me FROM messages WHERE chat_jid IN (`+marks+`)
			AND id<>'' AND julianday(timestamp) IS NOT NULL ORDER BY julianday(timestamp) DESC,rowid DESC LIMIT 1`, params...).Scan(&cursor.ID, &cursor.Sender, &cursor.Timestamp, &cursor.FromMe)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		chat.MetadataReplay = &metadataReplayState{Cursor: cursor, Until: chat.Cursor, Status: "pending"}
	}
	report.CaptureVersion = archiveCaptureVersion
	return nil
}

func eligibleMetadataReplay(replay *metadataReplayState) bool {
	if replay == nil {
		return false
	}
	if replay.Status == "pending" && !retryableBackfillResult(replay.LastResult) {
		return true
	}
	result := replay.Status
	if result == "pending" {
		result = replay.LastResult
	}
	if !retryableBackfillResult(result) {
		return false
	}
	if replay.AutomaticRetries >= automaticBackfillRetryLimit {
		replay.LastResult = result
		replay.Status = "retry_limit_reached_unconfirmed"
		return false
	}
	return true
}

func resumeMetadataReplays(report *backfillReport) bool {
	pending := false
	for _, chat := range report.Chats {
		if chat.LinkedTo == "" && eligibleMetadataReplay(chat.MetadataReplay) {
			chat.MetadataReplay.Status = "pending"
			pending = true
		}
	}
	return pending
}

func backfillMetadataPage(ctx context.Context, client historyRequestClient, mailbox *backfillMailbox, chat *backfillChat) string {
	replay := chat.MetadataReplay
	job := &backfillChat{JID: chat.JID, Aliases: chat.Aliases, RequestJID: chat.RequestJID,
		Cursor: replay.Cursor, Status: "pending", Pages: replay.Pages}
	if retryableBackfillResult(replay.LastResult) {
		replay.AutomaticRetries++
	}
	previous := job.Cursor
	backfillPage(ctx, client, mailbox, job)
	replay.Cursor, replay.Pages, replay.Status = job.Cursor, job.Pages, job.Status
	if replay.Cursor.ID != previous.ID || !replay.Cursor.Timestamp.Equal(previous.Timestamp) {
		replay.AutomaticRetries = 0
		if replay.Cursor.Timestamp.Before(replay.Until.Timestamp) || replay.Cursor.ID == replay.Until.ID {
			replay.Status = "captured_requested_range"
		}
	}
	replay.LastResult = replay.Status
	return replay.Status
}
