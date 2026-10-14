package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"syscall"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func storeOnDemandHistory(client *whatsmeow.Client, store *MessageStore, history *waHistorySync.HistorySync) error {
	ownUser := ""
	if client != nil && client.Store != nil && client.Store.ID != nil {
		ownUser = client.Store.ID.User
	}
	_, err := store.applyHistory(context.Background(), client, history, ownUser)
	return err
}

type historyRequestClient interface {
	BuildHistorySyncRequest(*types.MessageInfo, int) *waE2E.Message
	SendPeerMessage(context.Context, *waE2E.Message) (whatsmeow.SendResponse, error)
}

type historyCursor struct {
	ID        string    `json:"id"`
	Sender    string    `json:"sender"`
	Timestamp time.Time `json:"timestamp"`
	FromMe    bool      `json:"from_me"`
}

func (cursor historyCursor) messageInfo(chat types.JID) *types.MessageInfo {
	sender := types.NewJID(cursor.Sender, types.DefaultUserServer)
	if parsed, err := types.ParseJID(cursor.Sender); err == nil && parsed.Server != "" {
		sender = parsed
	}
	return &types.MessageInfo{MessageSource: types.MessageSource{
		Chat: chat, Sender: sender, IsFromMe: cursor.FromMe, IsGroup: chat.Server == types.GroupServer,
	}, ID: cursor.ID, Timestamp: cursor.Timestamp}
}

type backfillChat struct {
	Aliases          []string             `json:"aliases,omitempty"`
	RequestJID       string               `json:"request_jid,omitempty"`
	LinkedTo         string               `json:"linked_to,omitempty"`
	JID              string               `json:"jid"`
	Status           string               `json:"status"`
	Cursor           historyCursor        `json:"cursor"`
	Pages            int                  `json:"requests"`
	Received         int                  `json:"received_messages"`
	LastResult       string               `json:"last_result,omitempty"`
	AutomaticRetries int                  `json:"automatic_retries,omitempty"`
	MetadataReplay   *metadataReplayState `json:"metadata_replay,omitempty"`
}

type archiveSummary struct {
	Messages int        `json:"messages"`
	Chats    int        `json:"chats"`
	Oldest   *time.Time `json:"oldest"`
}

func (store *MessageStore) archiveSummary() (archiveSummary, error) {
	var summary archiveSummary
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&summary.Messages); err != nil {
		return summary, err
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM chats`).Scan(&summary.Chats); err != nil {
		return summary, err
	}
	var oldest time.Time
	err := store.db.QueryRow(`SELECT timestamp FROM messages ORDER BY timestamp ASC LIMIT 1`).Scan(&oldest)
	if errors.Is(err, sql.ErrNoRows) {
		return summary, nil
	}
	if err == nil {
		summary.Oldest = &oldest
	}
	return summary, err
}

type backfillReport struct {
	RequestedDays  int                    `json:"requested_days,omitempty"`
	TargetAfter    *time.Time             `json:"target_after,omitempty"`
	RunID          string                 `json:"run_id"`
	Status         string                 `json:"status"`
	Started        time.Time              `json:"started_at"`
	Updated        time.Time              `json:"updated_at"`
	Before         archiveSummary         `json:"before"`
	After          archiveSummary         `json:"after"`
	Chats          []*backfillChat        `json:"chats"`
	MessagesAdded  int64                  `json:"messages_added"`
	ResumeID       string                 `json:"resume_id,omitempty"`
	ProbeRequests  int                    `json:"probe_requests"`
	RetryPolicy    string                 `json:"retry_policy,omitempty"`
	Recovery       *backfillRecoveryState `json:"recovery,omitempty"`
	CaptureVersion int                    `json:"capture_version,omitempty"`
}

type backfillReply struct {
	conversation  *waHistorySync.Conversation
	conversations int
}

type backfillMailbox struct {
	mu            sync.Mutex
	chat          string
	aliases       []string
	responses     chan backfillReply
	storageFailed bool
}

func (mailbox *backfillMailbox) selectChat(chat string) {
	mailbox.selectConversation(chat, []string{chat})
}

func (mailbox *backfillMailbox) selectConversation(chat string, aliases []string) {
	mailbox.mu.Lock()
	defer mailbox.mu.Unlock()
	mailbox.chat = chat
	mailbox.aliases = append([]string(nil), aliases...)
	mailbox.storageFailed = false
	for {
		select {
		case <-mailbox.responses:
		default:
			return
		}
	}
}

func (mailbox *backfillMailbox) handle(event interface{}, storageError error) {
	history, ok := event.(*events.HistorySync)
	if !ok || history.Data == nil || history.Data.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND {
		return
	}
	mailbox.mu.Lock()
	defer mailbox.mu.Unlock()
	if storageError != nil {
		mailbox.storageFailed = true
	}
	matched, messages := 0, 0
	for _, conversation := range history.Data.GetConversations() {
		messages += len(conversation.GetMessages())
		if mailbox.chat == "" || (!containsIdentifier(mailbox.aliases, conversation.GetID()) && !containsIdentifier(mailbox.aliases, conversation.GetOldJID()) && !containsIdentifier(mailbox.aliases, conversation.GetNewJID())) {
			continue
		}
		matched++
		select {
		case mailbox.responses <- backfillReply{conversation: conversation, conversations: len(history.Data.GetConversations())}:
		default:
			fmt.Println("BACKFILL response mailbox full; this request remains unconfirmed")
		}
	}
	if matched == 0 && mailbox.chat != "" {
		select {
		case mailbox.responses <- backfillReply{conversations: len(history.Data.GetConversations())}:
		default:
		}
	}
	fmt.Printf("HISTORY_RESPONSE conversations=%d matched=%d messages=%d\n", len(history.Data.GetConversations()), matched, messages)
}

func saveBackfillReport(path string, report *backfillReport) error {
	report.Updated = time.Now().UTC()
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path+".tmp", data, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

func prepareBackfill(store *MessageStore, runID string) (*backfillReport, string, error) {
	days, err := configuredBackfillDays()
	if err != nil {
		return nil, "", err
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`).MatchString(runID) {
		return nil, "", errors.New("invalid backfill run ID")
	}
	dir := filepath.Join(store.storeDir, "backfill", runID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, "", err
	}
	path := filepath.Join(dir, "report.json")
	data, err := os.ReadFile(path)
	if err == nil {
		var report backfillReport
		if err := json.Unmarshal(data, &report); err != nil {
			return nil, path, err
		}
		if err := validateBackfillWindow(&report, days); err != nil {
			return nil, path, err
		}
		return &report, path, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, path, err
	}
	info, err := os.Stat(filepath.Join(store.storeDir, "messages.db"))
	if err != nil {
		return nil, path, err
	}
	var disk syscall.Statfs_t
	if err := syscall.Statfs(store.storeDir, &disk); err != nil {
		return nil, path, err
	}
	if disk.Bavail*uint64(disk.Bsize) < uint64(info.Size())*3+100*1024*1024 {
		return nil, path, errors.New("insufficient free space for a database snapshot and backfill")
	}
	backup := filepath.Join(dir, "messages.before.db")
	if _, err := os.Stat(backup); errors.Is(err, os.ErrNotExist) {
		if _, err := store.db.Exec("VACUUM INTO ?", backup); err != nil {
			return nil, path, fmt.Errorf("database snapshot: %w", err)
		}
		if err := os.Chmod(backup, 0600); err != nil {
			return nil, path, err
		}
	} else if err != nil {
		return nil, path, err
	}
	before, err := store.archiveSummary()
	if err != nil {
		return nil, path, err
	}
	rows, err := store.db.Query(`SELECT chat_jid FROM messages GROUP BY chat_jid ORDER BY MAX(timestamp) DESC`)
	if err != nil {
		return nil, path, err
	}
	var chats []*backfillChat
	for rows.Next() {
		var jid string
		if err := rows.Scan(&jid); err != nil {
			rows.Close()
			return nil, path, err
		}
		chats = append(chats, &backfillChat{JID: jid, Status: "pending"})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, path, err
	}
	for _, chat := range chats {
		jid, err := types.ParseJID(chat.JID)
		if err != nil || (jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer && jid.Server != types.GroupServer) {
			chat.Status = "unsupported_chat"
			continue
		}
		chat.Cursor.ID, chat.Cursor.Sender, chat.Cursor.Timestamp, chat.Cursor.FromMe, err = store.GetNewestMessage(chat.JID)
		if err != nil || chat.Cursor.ID == "" {
			chat.Status = "no_anchor"
		}
	}
	report := &backfillReport{RunID: runID, Status: "running", Started: time.Now().UTC(), Before: before, After: before, Chats: chats}
	if days > 0 {
		cutoff := report.Started.Add(-time.Duration(days) * 24 * time.Hour)
		report.RequestedDays = days
		report.TargetAfter = &cutoff
	}
	applyBackfillWindow(report)
	return report, path, saveBackfillReport(path, report)
}

func refreshBackfillChats(store *MessageStore, report *backfillReport) error {
	rows, err := store.db.Query(`SELECT jid FROM chats UNION SELECT chat_jid FROM messages ORDER BY 1`)
	if err != nil {
		return err
	}
	var identifiers []string
	for rows.Next() {
		var jid string
		if err := rows.Scan(&jid); err != nil {
			rows.Close()
			return err
		}
		identifiers = append(identifiers, jid)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	known := make(map[string]*backfillChat, len(report.Chats))
	for _, chat := range report.Chats {
		known[chat.JID] = chat
		for _, alias := range chat.Aliases {
			known[alias] = chat
		}
	}
	for _, identifier := range identifiers {
		chat := known[identifier]
		if chat != nil && chat.Status != "no_anchor" {
			continue
		}
		candidate := &backfillChat{JID: identifier, Status: "pending"}
		candidate.Cursor.ID, candidate.Cursor.Sender, candidate.Cursor.Timestamp, candidate.Cursor.FromMe, err = store.GetNewestMessage(identifier)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && candidate.Cursor.ID == "") {
			candidate.Status = "no_anchor"
		} else if err != nil {
			return err
		} else {
			jid, parseError := types.ParseJID(identifier)
			if parseError != nil || (jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer && jid.Server != types.GroupServer) {
				candidate.Status = "unsupported_chat"
			}
		}
		if chat == nil {
			report.Chats = append(report.Chats, candidate)
		} else if candidate.Status != "no_anchor" {
			candidate.JID = chat.JID
			candidate.Aliases = chat.Aliases
			candidate.RequestJID = chat.RequestJID
			*chat = *candidate
		}
	}
	applyBackfillWindow(report)
	return nil
}

func applyBackfillResponse(chat *backfillChat, conversation *waHistorySync.Conversation) {
	chat.Received += len(conversation.GetMessages())
	previous := chat.Cursor
	for _, entry := range conversation.GetMessages() {
		message := entry.GetMessage()
		key := message.GetKey()
		if key.GetID() == "" || message.GetMessageTimestamp() == 0 {
			continue
		}
		timestamp := time.Unix(int64(message.GetMessageTimestamp()), 0).UTC()
		if timestamp.Before(chat.Cursor.Timestamp) || timestamp.Equal(chat.Cursor.Timestamp) {
			chat.Cursor = historyCursor{ID: key.GetID(), Sender: key.GetParticipant(), Timestamp: timestamp, FromMe: key.GetFromMe()}
		}
	}
	if conversation.EndOfHistoryTransferType != nil {
		switch conversation.GetEndOfHistoryTransferType() {
		case waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY:
			chat.Status = "phone_reports_no_older_messages"
			return
		case waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS:
			chat.Status = "older_messages_not_accessible"
			return
		}
	}
	if chat.Cursor.ID == previous.ID && chat.Cursor.Timestamp.Equal(previous.Timestamp) {
		chat.Status = "no_backward_progress"
	}
}

func backfillPage(ctx context.Context, client historyRequestClient, mailbox *backfillMailbox, chat *backfillChat) {
	defer func() {
		chat.LastResult = chat.Status
		if chat.Status == "pending" {
			chat.LastResult = "progress"
		}
	}()
	if chat.Pages >= 10000 {
		chat.Status = "request_limit"
		return
	}
	requestJID := chat.RequestJID
	if requestJID == "" {
		requestJID = chat.JID
	}
	jid, err := types.ParseJID(requestJID)
	if err != nil {
		chat.Status = "invalid_chat"
		return
	}
	aliases := chat.Aliases
	if len(aliases) == 0 {
		aliases = []string{chat.JID}
	}
	mailbox.selectConversation(requestJID, aliases)
	defer mailbox.selectChat("")
	requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	_, err = client.SendPeerMessage(requestCtx, client.BuildHistorySyncRequest(chat.Cursor.messageInfo(jid), 50))
	cancel()
	chat.Pages++
	if err != nil {
		chat.Status = "request_failed"
		fmt.Printf("BACKFILL request failed: %v\n", err)
		return
	}
	timer := time.NewTimer(60 * time.Second)
	defer timer.Stop()
	select {
	case reply := <-mailbox.responses:
		mailbox.mu.Lock()
		storageFailed := mailbox.storageFailed
		mailbox.mu.Unlock()
		if storageFailed {
			chat.Status = "storage_failed"
			return
		}
		if reply.conversation == nil {
			chat.Status = "empty_response_unconfirmed"
			if reply.conversations > 0 {
				chat.Status = "unmatched_response_unconfirmed"
			}
			return
		}
		applyBackfillResponse(chat, reply.conversation)
	case <-timer.C:
		chat.Status = "response_timeout"
	case <-ctx.Done():
		chat.Status = "interrupted"
	}
}

func logBackfillReport(report *backfillReport) {
	counts := make(map[string]int)
	queue := make(map[string]int)
	requests, received := 0, 0
	for _, chat := range report.Chats {
		counts[chat.Status]++
		requests += chat.Pages
		received += chat.Received
		if chat.Status == "pending" && chat.LinkedTo == "" {
			switch {
			case chat.Pages == 0:
				queue["never_requested"]++
			case chat.LastResult == "progress":
				queue["continuing_history"]++
			default:
				queue["retry_failed_requests"]++
			}
		}
	}
	data, _ := json.Marshal(map[string]interface{}{
		"run_id": report.RunID, "status": report.Status, "chats": counts,
		"requests": requests, "received_messages": received,
		"before": report.Before, "after": report.After,
		"net_new_stored_messages": report.After.Messages - report.Before.Messages,
		"backfilled_messages":     report.MessagesAdded,
		"probe_requests":          report.ProbeRequests,
		"retry_policy":            report.RetryPolicy,
		"queue":                   queue,
		"requested_days":          report.RequestedDays,
		"target_after":            report.TargetAfter,
	})
	fmt.Printf("BACKFILL %s\n", data)
}

func resumeBackfill(report *backfillReport, resumeID string) bool {
	if resumeID == "" || resumeID == report.ResumeID {
		return false
	}
	switch report.Status {
	case "running", "phone_unresponsive", "interrupted", "finished":
	default:
		return false
	}
	hasPendingChats := false
	if resumeMetadataReplays(report) {
		hasPendingChats = true
	}
	for _, chat := range report.Chats {
		if chat.LinkedTo != "" {
			continue
		}
		if chat.Status == "pending" {
			switch {
			case chat.Pages == 0 || chat.LastResult == "progress":
				hasPendingChats = true
				continue
			case chat.LastResult == "":
				chat.Status = "deferred_previous_result_unknown"
				continue
			default:
				chat.Status = chat.LastResult
			}
		}
		switch chat.Status {
		case "response_timeout", "request_failed", "interrupted", "unmatched_response_unconfirmed":
			chat.LastResult = chat.Status
			chat.Status = "pending"
			hasPendingChats = true
		}
	}
	report.ResumeID = resumeID
	report.RetryPolicy = "targeted_v1"
	if !hasPendingChats {
		report.Status = "finished"
		return false
	}
	report.Status = "running"
	sort.SliceStable(report.Chats, func(i, j int) bool {
		leftUnrequested := report.Chats[i].Status == "pending" && report.Chats[i].Pages == 0
		rightUnrequested := report.Chats[j].Status == "pending" && report.Chats[j].Pages == 0
		return leftUnrequested && !rightUnrequested
	})
	return true
}

func probePrimaryPhone(ctx context.Context, client historyRequestClient, store *MessageStore, mailbox *backfillMailbox, report *backfillReport) bool {
	for _, chat := range report.Chats {
		if chat.Received == 0 {
			continue
		}
		probe := &backfillChat{JID: chat.JID, Aliases: chat.Aliases, RequestJID: chat.RequestJID, Status: "pending"}
		var err error
		probe.Cursor.ID, probe.Cursor.Sender, probe.Cursor.Timestamp, probe.Cursor.FromMe, err = store.GetNewestMessage(chat.JID)
		if err != nil || probe.Cursor.ID == "" {
			continue
		}
		backfillPage(ctx, client, mailbox, probe)
		report.ProbeRequests += probe.Pages
		report.MessagesAdded += store.historyMessagesAdded.Swap(0)
		if probe.Status == "storage_failed" || probe.Status == "interrupted" {
			report.Status = probe.Status
		}
		responsive := probe.Status != "response_timeout" && probe.Status != "request_failed" && probe.Status != "interrupted" && probe.Status != "storage_failed"
		fmt.Printf("PRIMARY_PHONE_PROBE responsive=%t\n", responsive)
		return responsive
	}
	return false
}

func runOneOffBackfill(ctx context.Context, client *whatsmeow.Client, store *MessageStore, mailbox *backfillMailbox, runID, resumeID string) {
	report, path, err := prepareBackfill(store, runID)
	if err != nil {
		fmt.Printf("BACKFILL preparation failed: %v\n", err)
		return
	}
	newResumeMarker := resumeID != "" && resumeID != report.ResumeID
	if newResumeMarker {
		if err := refreshBackfillChats(store, report); err != nil {
			fmt.Printf("BACKFILL chat discovery failed: %v\n", err)
			return
		}
	}
	hadPreviousRequests := false
	for _, chat := range report.Chats {
		if chat.Pages > 0 {
			hadPreviousRequests = true
			break
		}
	}
	if report.Status == "running" || newResumeMarker {
		if err := consolidateBackfillChats(store, report, func(jid string) ([]string, error) { return knownHistoryAliases(client, jid) }, newResumeMarker && hadPreviousRequests); err != nil {
			fmt.Printf("BACKFILL identity_selection_failed category=%s\n", storageErrorCategory(err))
			return
		}
	}
	resumed := resumeBackfill(report, resumeID)
	applyBackfillWindow(report)
	if newResumeMarker {
		if err := saveBackfillReport(path, report); err != nil {
			fmt.Printf("BACKFILL selection checkpoint failed: %v\n", err)
			return
		}
	}
	if report.Status != "running" {
		logBackfillReport(report)
		return
	}
	logBackfillReport(report)
	store.historyMessagesAdded.Store(0)
	if resumed && hadPreviousRequests && !probePrimaryPhone(ctx, client, store, mailbox, report) {
		if report.Status == "running" {
			report.Status = "phone_unresponsive"
		}
		if err := saveBackfillReport(path, report); err != nil {
			fmt.Printf("BACKFILL resume checkpoint failed: %v\n", err)
		}
		logBackfillReport(report)
		return
	}
	consecutiveFailures := 0
	for report.Status == "running" {
		pending := false
		primaryPending := false
		for _, chat := range report.Chats {
			if chat.LinkedTo == "" && chat.Status == "pending" {
				primaryPending = true
				break
			}
		}
		for _, chat := range report.Chats {
			replayPending := !primaryPending && chat.MetadataReplay != nil && chat.MetadataReplay.Status == "pending"
			if (chat.Status != "pending" && !replayPending) || chat.LinkedTo != "" {
				continue
			}
			pending = true
			if ctx.Err() != nil || !client.IsConnected() {
				report.Status = "interrupted"
				break
			}
			result := chat.Status
			if replayPending {
				result = backfillMetadataPage(ctx, client, mailbox, chat)
			} else {
				backfillPageWithRecovery(ctx, client, mailbox, chat, report.Recovery != nil)
				applyBackfillWindow(report)
				result = chat.Status
			}
			report.MessagesAdded += store.historyMessagesAdded.Swap(0)
			if result == "storage_failed" {
				report.Status = "storage_failed"
			}
			if result == "response_timeout" || result == "request_failed" {
				consecutiveFailures++
			} else {
				consecutiveFailures = 0
			}
			if consecutiveFailures >= 3 {
				if probePrimaryPhone(ctx, client, store, mailbox, report) {
					consecutiveFailures = 0
				} else {
					if report.Status == "running" {
						report.Status = "phone_unresponsive"
					}
				}
			}
			report.After, err = store.archiveSummary()
			if err != nil {
				fmt.Printf("BACKFILL archive verification failed: %v\n", err)
				report.Status = "verification_failed"
			}
			if err := saveBackfillReport(path, report); err != nil {
				fmt.Printf("BACKFILL checkpoint failed: %v\n", err)
				return
			}
			logBackfillReport(report)
			if report.Status != "running" {
				break
			}
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
			}
		}
		if !pending {
			report.Status = "finished"
		}
	}
	if err := saveBackfillReport(path, report); err != nil {
		fmt.Printf("BACKFILL final checkpoint failed: %v\n", err)
	}
	logBackfillReport(report)
}
