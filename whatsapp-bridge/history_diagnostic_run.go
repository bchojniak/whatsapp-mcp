package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

func knownHistoryAliases(client *whatsmeow.Client, identifier string) ([]string, error) {
	jid, err := types.ParseJID(identifier)
	if err != nil {
		return nil, err
	}
	aliases := []string{identifier}
	if client == nil || client.Store == nil || client.Store.LIDs == nil {
		return aliases, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var alias types.JID
	switch jid.Server {
	case types.DefaultUserServer:
		alias, err = client.Store.LIDs.GetLIDForPN(ctx, jid)
	case types.HiddenUserServer:
		alias, err = client.Store.LIDs.GetPNForLID(ctx, jid)
	}
	if err != nil {
		return nil, err
	}
	if !alias.IsEmpty() && !containsIdentifier(aliases, alias.String()) {
		aliases = append(aliases, alias.String())
	}
	return aliases, nil
}

func newestDiagnosticAnchor(store *MessageStore, chat string) (historyCursor, error) {
	var cursor historyCursor
	err := store.db.QueryRow(`SELECT id,sender,timestamp,is_from_me FROM messages WHERE chat_jid=? AND id<>'' AND julianday(timestamp) IS NOT NULL ORDER BY julianday(timestamp) DESC,rowid DESC LIMIT 1`, chat).Scan(&cursor.ID, &cursor.Sender, &cursor.Timestamp, &cursor.FromMe)
	return cursor, err
}

func validateDiagnosticAnchor(store *MessageStore, attempt *diagnosticAttempt) string {
	if attempt.Anchor.ID == "" || len(attempt.Anchor.ID) > 256 || attempt.Anchor.Timestamp.Unix() <= 0 {
		return "invalid_anchor"
	}
	jid, err := types.ParseJID(attempt.JID)
	if err != nil || (jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer && jid.Server != types.GroupServer) {
		return "invalid_anchor"
	}
	for _, alias := range attempt.Aliases {
		var timestamp time.Time
		var fromMe bool
		err := store.db.QueryRow(`SELECT timestamp,is_from_me FROM messages WHERE id=? AND chat_jid=?`, attempt.Anchor.ID, alias).Scan(&timestamp, &fromMe)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return "archive_unavailable"
		}
		if timestamp.Unix() != attempt.Anchor.Timestamp.Unix() || fromMe != attempt.Anchor.FromMe {
			return "stored_anchor_mismatch"
		}
		if alias == attempt.JID {
			return "stored_exact"
		}
		return "stored_alias"
	}
	if attempt.Variant == "failed_boundary" {
		return "provider_checkpoint_only"
	}
	return "invalid_anchor"
}

func selectDiagnosticAttempts(store *MessageStore, report *backfillReport, aliasesFor func(string) ([]string, error)) ([]*diagnosticAttempt, error) {
	if report.Status != "finished" {
		return nil, errors.New("source backfill must be finished")
	}
	var failed []*backfillChat
	for _, chat := range report.Chats {
		if chat.Status == "response_timeout" {
			failed = append(failed, chat)
		}
	}
	sort.SliceStable(failed, func(i, j int) bool { return failed[i].Cursor.Timestamp.Before(failed[j].Cursor.Timestamp) })
	var attempts []*diagnosticAttempt
	var selected []string
	var control *diagnosticAttempt
	for _, chat := range report.Chats {
		if chat.Received == 0 || chat.Status == "response_timeout" {
			continue
		}
		anchor, err := newestDiagnosticAnchor(store, chat.JID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		aliases, err := aliasesFor(chat.JID)
		if err != nil {
			return nil, err
		}
		candidate := &diagnosticAttempt{Label: "control", Variant: "control_start", JID: chat.JID, Aliases: aliases, Anchor: anchor, Result: "planned"}
		candidate.AnchorValidation = validateDiagnosticAnchor(store, candidate)
		if candidate.AnchorValidation != "stored_exact" && candidate.AnchorValidation != "stored_alias" {
			continue
		}
		control = candidate
		attempts = append(attempts, control)
		selected = append(selected, aliases...)
		break
	}
	if control == nil {
		return nil, errors.New("no verified responsive control chat")
	}
	labels := 0
	for _, chat := range failed {
		if labels == 3 {
			break
		}
		aliases, err := aliasesFor(chat.JID)
		if err != nil {
			return nil, err
		}
		duplicate := false
		for _, alias := range aliases {
			if containsIdentifier(selected, alias) {
				duplicate = true
			}
		}
		if duplicate {
			continue
		}
		recent, err := newestDiagnosticAnchor(store, chat.JID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		labels++
		label := fmt.Sprintf("failed-%d", labels)
		boundary := &diagnosticAttempt{Label: label, Variant: "failed_boundary", JID: chat.JID, Aliases: aliases, Anchor: chat.Cursor, Result: "planned"}
		boundary.AnchorValidation = validateDiagnosticAnchor(store, boundary)
		latest := &diagnosticAttempt{Label: label, Variant: "recent_anchor", JID: chat.JID, Aliases: aliases, Anchor: recent, Result: "planned"}
		latest.AnchorValidation = validateDiagnosticAnchor(store, latest)
		attempts = append(attempts, boundary, latest)
		selected = append(selected, aliases...)
	}
	if labels == 0 {
		return nil, errors.New("no requestable failed chats")
	}
	closing := *control
	closing.Variant = "control_end"
	attempts = append(attempts, &closing)
	for index, attempt := range attempts {
		attempt.Number = index + 1
	}
	return attempts, nil
}

func (trace *historyDiagnostics) prepare(store *MessageStore, aliasesFor func(string) ([]string, error)) error {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if trace.report.Status != "prepared" {
		return nil
	}
	if trace.report.PreviousRunID != "" {
		data, err := os.ReadFile(filepath.Join(store.storeDir, "history-diagnostics", trace.report.PreviousRunID, "report.json"))
		if err != nil {
			return err
		}
		if len(data) > 2<<20 {
			return errors.New("prior diagnostic report size limit")
		}
		var previous diagnosticReport
		if err = json.Unmarshal(data, &previous); err != nil {
			return err
		}
		if previous.RunID != trace.report.PreviousRunID || previous.SourceRunID != trace.report.SourceRunID {
			return errors.New("incompatible prior diagnostic report")
		}
		var attempts []*diagnosticAttempt
		if trace.report.Mode == "oldest_boundary" {
			attempts, err = selectRecoveryAttempts(store, &previous, aliasesFor)
		} else if previous.PreviousRunID == "" {
			attempts, err = selectAliasDiagnosticAttempts(store, &previous, aliasesFor)
		} else {
			return errors.New("alias comparisons cannot be chained")
		}
		if err != nil {
			return err
		}
		trace.report.Attempts = attempts
		trace.report.Status = "running"
		return trace.saveLocked()
	}
	data, err := os.ReadFile(filepath.Join(store.storeDir, "backfill", trace.report.SourceRunID, "report.json"))
	if err != nil {
		return err
	}
	var source backfillReport
	if err = json.Unmarshal(data, &source); err != nil {
		return err
	}
	if source.RunID != trace.report.SourceRunID {
		return errors.New("source report ID mismatch")
	}
	attempts, err := selectDiagnosticAttempts(store, &source, aliasesFor)
	if err != nil {
		return err
	}
	trace.report.Attempts = attempts
	trace.report.Status = "running"
	return trace.saveLocked()
}

func (trace *historyDiagnostics) observedResultLocked(attempt *diagnosticAttempt) string {
	if attempt.Started == nil {
		return ""
	}
	for _, response := range trace.report.Responses {
		if response.Received.Before(*attempt.Started) {
			continue
		}
		for _, conversation := range response.Conversations {
			if !diagnosticConversationMatches(attempt, conversation) {
				continue
			}
			if response.PermanentStorageFailure {
				return "storage_failed"
			}
			if response.Stored == nil {
				continue
			}
			switch conversation.EndFlag {
			case "COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY":
				return "phone_reports_no_older_messages"
			case "COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS":
				return "older_messages_not_accessible"
			}
			if conversation.Oldest != nil && conversation.Oldest.Before(attempt.Anchor.Timestamp) {
				return "progress_observed"
			}
			return "no_backward_progress_observed"
		}
	}
	return ""
}

func (trace *historyDiagnostics) finishAttempt(attempt *diagnosticAttempt, result string) {
	trace.mu.Lock()
	attempt.Result = result
	now := time.Now().UTC()
	attempt.Finished = &now
	trace.active = 0
	trace.saveLocked()
	trace.mu.Unlock()
	trace.logSnapshot()
}

func (trace *historyDiagnostics) request(ctx context.Context, client historyRequestClient, attempt *diagnosticAttempt, wait time.Duration) string {
	if attempt.AnchorValidation != "stored_exact" && attempt.AnchorValidation != "stored_alias" && attempt.AnchorValidation != "provider_checkpoint_only" {
		return "invalid_anchor"
	}
	jid, err := types.ParseJID(attempt.JID)
	if err != nil {
		return "invalid_anchor"
	}
	trace.mu.Lock()
	if !trace.available {
		trace.mu.Unlock()
		return "diagnostics_unavailable"
	}
	started := time.Now().UTC()
	attempt.Started = &started
	attempt.Result = "waiting"
	trace.active = attempt.Number
	err = trace.saveLocked()
	trace.mu.Unlock()
	if err != nil {
		return "diagnostics_unavailable"
	}
	trace.logSnapshot()
	requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	ack, err := client.SendPeerMessage(requestCtx, client.BuildHistorySyncRequest(attempt.Anchor.messageInfo(jid), 50))
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return "interrupted"
		}
		return "request_failed"
	}
	trace.mu.Lock()
	acknowledged := time.Now().UTC()
	attempt.Acknowledged = &acknowledged
	attempt.RequestID = ack.ID
	err = trace.saveLocked()
	trace.mu.Unlock()
	if err != nil {
		return "diagnostics_unavailable"
	}
	trace.logSnapshot()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		trace.mu.Lock()
		result := trace.observedResultLocked(attempt)
		available := trace.available
		trace.mu.Unlock()
		if !available {
			return "diagnostics_unavailable"
		}
		if result != "" {
			return result
		}
		select {
		case <-trace.wake:
		case <-ctx.Done():
			return "interrupted"
		case <-timer.C:
			trace.mu.Lock()
			defer trace.mu.Unlock()
			if result := trace.observedResultLocked(attempt); result != "" {
				return result
			}
			unmatched, empty := false, false
			for _, response := range trace.report.Responses {
				if response.Received.Before(*attempt.Started) {
					continue
				}
				if len(response.Conversations) == 0 {
					empty = true
				} else {
					unmatched = true
				}
				for _, conversation := range response.Conversations {
					if diagnosticConversationMatches(attempt, conversation) {
						return "response_pending_storage"
					}
				}
			}
			if unmatched {
				return "unmatched_response_unconfirmed"
			}
			if empty {
				return "empty_response_unconfirmed"
			}
			return "response_timeout"
		}
	}
}

func waitDiagnosticSpacing(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (trace *historyDiagnostics) finishRun(status string) {
	trace.mu.Lock()
	trace.report.Status = status
	trace.active = 0
	trace.saveLocked()
	trace.mu.Unlock()
	trace.logSnapshot()
}

func runHistoryDiagnostic(ctx context.Context, client *whatsmeow.Client, store *MessageStore, trace *historyDiagnostics) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	if err := trace.prepare(store, func(jid string) ([]string, error) { return knownHistoryAliases(client, jid) }); err != nil {
		fmt.Printf("HISTORY_DIAGNOSTIC preparation_failed category=%s\n", storageErrorCategory(err))
		trace.finishRun("preparation_failed")
		return
	}
	trace.mu.Lock()
	status := trace.report.Status
	attempts := append([]*diagnosticAttempt(nil), trace.report.Attempts...)
	trace.mu.Unlock()
	trace.logSnapshot()
	if status != "running" {
		return
	}
	for index, attempt := range attempts {
		if !client.IsConnected() || ctx.Err() != nil {
			trace.finishRun("interrupted")
			return
		}
		if index > 0 && !waitDiagnosticSpacing(ctx, 15*time.Second) {
			trace.finishRun("interrupted")
			return
		}
		if attempt.Variant == "oldest_boundary" {
			coverage, err := store.historyCoverage(attempt.Aliases)
			if err != nil {
				trace.finishRun("verification_failed")
				return
			}
			trace.mu.Lock()
			attempt.CoverageBefore = coverage
			trace.mu.Unlock()
		}
		result := trace.request(ctx, client, attempt, 60*time.Second)
		if attempt.CoverageBefore != nil {
			coverage, err := store.historyCoverage(attempt.Aliases)
			if err != nil {
				trace.finishAttempt(attempt, result)
				trace.finishRun("verification_failed")
				return
			}
			added, err := store.newOlderHistoryMessages(attempt.Aliases, attempt.CoverageBefore, attempt.Anchor.Timestamp)
			if err != nil {
				trace.finishAttempt(attempt, result)
				trace.finishRun("verification_failed")
				return
			}
			trace.mu.Lock()
			attempt.CoverageAfter = coverage
			attempt.NewOlderMessages = added
			trace.mu.Unlock()
		}
		trace.finishAttempt(attempt, result)
		if result == "diagnostics_unavailable" || result == "interrupted" || result == "response_pending_storage" || result == "storage_failed" {
			trace.finishRun(result)
			return
		}
		if attempt.Label == "control" && (result == "response_timeout" || result == "request_failed" || result == "empty_response_unconfirmed" || result == "unmatched_response_unconfirmed") {
			trace.finishRun("control_unresponsive")
			return
		}
	}
	if !waitDiagnosticSpacing(ctx, 30*time.Second) {
		trace.finishRun("interrupted")
		return
	}
	trace.finishRun("finished")
}
