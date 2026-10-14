package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const maxDiagnosticAttempts = 8
const maxDiagnosticResponses = 128

type diagnosticAttempt struct {
	CoverageBefore   *historyCoverage `json:"coverage_before,omitempty"`
	CoverageAfter    *historyCoverage `json:"coverage_after,omitempty"`
	NewOlderMessages int              `json:"new_older_message_ids"`
	Number           int              `json:"number"`
	Label            string           `json:"label"`
	Variant          string           `json:"variant"`
	JID              string           `json:"jid"`
	Aliases          []string         `json:"aliases"`
	Anchor           historyCursor    `json:"anchor"`
	AnchorValidation string           `json:"anchor_validation"`
	Result           string           `json:"result"`
	Started          *time.Time       `json:"started_at"`
	Acknowledged     *time.Time       `json:"acknowledged_at"`
	Finished         *time.Time       `json:"finished_at"`
	RequestID        string           `json:"request_id,omitempty"`
}

type diagnosticConversation struct {
	JID     string     `json:"jid"`
	OldJID  string     `json:"old_jid,omitempty"`
	NewJID  string     `json:"new_jid,omitempty"`
	Labels  []string   `json:"labels"`
	Match   string     `json:"match"`
	Entries int        `json:"entries"`
	Oldest  *time.Time `json:"oldest"`
	Newest  *time.Time `json:"newest"`
	EndFlag string     `json:"end_flag"`
}

type diagnosticResponse struct {
	ID                      string                   `json:"id"`
	Received                time.Time                `json:"received_at"`
	ActiveAttempt           int                      `json:"active_attempt"`
	Conversations           []diagnosticConversation `json:"conversations"`
	Stored                  *time.Time               `json:"stored_at"`
	Eligible                int                      `json:"eligible_entries"`
	Added                   int                      `json:"inserted_rows"`
	Duplicates              int                      `json:"duplicate_entries"`
	StorageCommits          int                      `json:"storage_commits"`
	StorageFailures         int                      `json:"storage_failures"`
	StorageError            string                   `json:"storage_error,omitempty"`
	PermanentStorageFailure bool                     `json:"permanent_storage_failure"`
}

type diagnosticNotification struct {
	At         time.Time `json:"at"`
	OriginalID string    `json:"original_message_id,omitempty"`
	SessionID  string    `json:"peer_request_session_id,omitempty"`
	StanzaID   string    `json:"stanza_id,omitempty"`
	Kind       string    `json:"kind"`
}

type diagnosticReport struct {
	Mode                string                   `json:"mode,omitempty"`
	RunID               string                   `json:"run_id"`
	SourceRunID         string                   `json:"source_run_id"`
	PreviousRunID       string                   `json:"previous_run_id,omitempty"`
	Status              string                   `json:"status"`
	Started             time.Time                `json:"started_at"`
	Updated             time.Time                `json:"updated_at"`
	Attempts            []*diagnosticAttempt     `json:"attempts"`
	Responses           []*diagnosticResponse    `json:"responses"`
	Notifications       []diagnosticNotification `json:"notifications"`
	DroppedObservations int                      `json:"dropped_observations"`
}

// Raw chat/anchor references stay in this private file. Health and Railway logs
// expose only labels, stages, dates and counts. Message bodies and media secrets
// are never copied into diagnostic reports.
type historyDiagnostics struct {
	mu        sync.Mutex
	path      string
	report    diagnosticReport
	active    int
	available bool
	wake      chan struct{}
}

var historyRunIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)

func openHistoryDiagnostics(store *MessageStore, runID, sourceRunID, previousRunID string, modes ...string) (*historyDiagnostics, error) {
	mode := ""
	if len(modes) > 0 {
		mode = modes[0]
	}
	if mode != "" && mode != "oldest_boundary" {
		return nil, errors.New("invalid diagnostic mode")
	}
	if mode == "oldest_boundary" && previousRunID == "" {
		return nil, errors.New("boundary recovery requires prior identifier evidence")
	}
	if !historyRunIDPattern.MatchString(runID) || !historyRunIDPattern.MatchString(sourceRunID) {
		return nil, errors.New("invalid diagnostic or source run ID")
	}
	if previousRunID != "" && (!historyRunIDPattern.MatchString(previousRunID) || previousRunID == runID) {
		return nil, errors.New("invalid prior diagnostic run ID")
	}
	dir := filepath.Join(store.storeDir, "history-diagnostics", runID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	trace := &historyDiagnostics{path: filepath.Join(dir, "report.json"), wake: make(chan struct{}, 1), available: true}
	data, err := os.ReadFile(trace.path)
	if err == nil {
		if len(data) > 2<<20 {
			return nil, errors.New("diagnostic report size limit")
		}
		if err = json.Unmarshal(data, &trace.report); err != nil {
			return nil, err
		}
		if trace.report.RunID != runID || trace.report.SourceRunID != sourceRunID || trace.report.PreviousRunID != previousRunID || trace.report.Mode != mode || len(trace.report.Attempts) > maxDiagnosticAttempts || len(trace.report.Responses) > maxDiagnosticResponses || len(trace.report.Notifications) > maxDiagnosticResponses {
			return nil, errors.New("incompatible diagnostic report")
		}
		// An uncertain send is never repeated automatically after a restart.
		if trace.report.Status == "running" {
			trace.report.Status = "interrupted"
			for _, attempt := range trace.report.Attempts {
				if attempt.Result == "waiting" {
					attempt.Result = "interrupted"
				}
			}
		}
	} else if errors.Is(err, os.ErrNotExist) {
		trace.report = diagnosticReport{RunID: runID, SourceRunID: sourceRunID, PreviousRunID: previousRunID, Mode: mode, Status: "prepared", Started: time.Now().UTC()}
	} else {
		return nil, err
	}
	if err = trace.saveLocked(); err != nil {
		return nil, err
	}
	store.historyDiagnostics = trace
	return trace, nil
}

func (trace *historyDiagnostics) saveLocked() error {
	trace.report.Updated = time.Now().UTC()
	data, err := json.MarshalIndent(trace.report, "", "  ")
	if err == nil {
		var file *os.File
		file, err = os.OpenFile(trace.path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err == nil {
			err = file.Chmod(0600)
			if err == nil {
				_, err = file.Write(data)
			}
			if err == nil {
				err = file.Sync()
			}
			closeErr := file.Close()
			if err == nil {
				err = closeErr
			}
		}
		if err == nil {
			err = os.Rename(trace.path+".tmp", trace.path)
		}
		if err == nil {
			var directory *os.File
			directory, err = os.Open(filepath.Dir(trace.path))
			if err == nil {
				err = directory.Sync()
				directory.Close()
			}
		}
	}
	trace.available = err == nil
	if err != nil {
		fmt.Printf("HISTORY_DIAGNOSTIC persistence_failed category=%s\n", storageErrorCategory(err))
	}
	select {
	case trace.wake <- struct{}{}:
	default:
	}
	return err
}

func containsIdentifier(identifiers []string, identifier string) bool {
	if identifier == "" {
		return false
	}
	for _, known := range identifiers {
		if identifier == known {
			return true
		}
	}
	return false
}

func diagnosticConversationMatches(attempt *diagnosticAttempt, conversation diagnosticConversation) bool {
	return containsIdentifier(attempt.Aliases, conversation.JID) || containsIdentifier(attempt.Aliases, conversation.OldJID) || containsIdentifier(attempt.Aliases, conversation.NewJID)
}

func (trace *historyDiagnostics) recordReceived(history *waHistorySync.HistorySync) string {
	if history == nil || history.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND {
		return ""
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if len(trace.report.Responses) >= maxDiagnosticResponses {
		trace.report.DroppedObservations++
		trace.saveLocked()
		return ""
	}
	response := &diagnosticResponse{ID: fmt.Sprintf("%s:%d", trace.report.RunID, len(trace.report.Responses)+1), Received: time.Now().UTC(), ActiveAttempt: trace.active}
	for _, conversation := range history.GetConversations() {
		if conversation == nil {
			continue
		}
		if len(response.Conversations) >= 32 {
			trace.report.DroppedObservations++
			continue
		}
		evidence := diagnosticConversation{JID: conversation.GetID(), OldJID: conversation.GetOldJID(), NewJID: conversation.GetNewJID(), Entries: len(conversation.GetMessages()), Match: "none", EndFlag: "unspecified"}
		if conversation.EndOfHistoryTransferType != nil {
			evidence.EndFlag = conversation.GetEndOfHistoryTransferType().String()
		}
		for _, entry := range conversation.GetMessages() {
			message := entry.GetMessage()
			if message.GetKey().GetID() == "" || message.GetMessageTimestamp() == 0 {
				continue
			}
			timestamp := time.Unix(int64(message.GetMessageTimestamp()), 0).UTC()
			if evidence.Oldest == nil || timestamp.Before(*evidence.Oldest) {
				value := timestamp
				evidence.Oldest = &value
			}
			if evidence.Newest == nil || timestamp.After(*evidence.Newest) {
				value := timestamp
				evidence.Newest = &value
			}
		}
		for _, attempt := range trace.report.Attempts {
			if !diagnosticConversationMatches(attempt, evidence) {
				continue
			}
			if !containsIdentifier(evidence.Labels, attempt.Label) {
				evidence.Labels = append(evidence.Labels, attempt.Label)
			}
			if evidence.Match != "exact" {
				evidence.Match = "alias"
				if evidence.JID == attempt.JID || evidence.OldJID == attempt.JID || evidence.NewJID == attempt.JID {
					evidence.Match = "exact"
				}
			}
		}
		response.Conversations = append(response.Conversations, evidence)
	}
	trace.report.Responses = append(trace.report.Responses, response)
	trace.saveLocked()
	return response.ID
}

func (trace *historyDiagnostics) responseLocked(id string) *diagnosticResponse {
	for _, response := range trace.report.Responses {
		if response.ID == id {
			return response
		}
	}
	return nil
}

func (trace *historyDiagnostics) recordStored(id string, eligible, added int) {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	response := trace.responseLocked(id)
	if response == nil {
		return
	}
	now := time.Now().UTC()
	response.Stored = &now
	response.StorageCommits++
	response.Eligible += eligible
	response.Added += added
	response.Duplicates += eligible - added
	response.StorageError = ""
	response.PermanentStorageFailure = false
	trace.saveLocked()
}

func (trace *historyDiagnostics) recordStorageError(id, category string, permanent bool) {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	response := trace.responseLocked(id)
	if response == nil {
		return
	}
	response.StorageFailures++
	response.StorageError = category
	response.PermanentStorageFailure = permanent
	trace.saveLocked()
}

func (trace *historyDiagnostics) recordProtocol(event *events.Message) {
	if !event.Info.IsFromMe {
		return
	}
	protocol := event.Message.GetProtocolMessage()
	notification := protocol.GetHistorySyncNotification()
	peer := protocol.GetPeerDataOperationRequestResponseMessage()
	observation := diagnosticNotification{At: time.Now().UTC()}
	if notification != nil && notification.GetSyncType() == waE2E.HistorySyncType_ON_DEMAND {
		observation.Kind = "history_notification"
		observation.OriginalID = notification.GetOriginalMessageID()
		observation.SessionID = notification.GetPeerDataRequestSessionID()
	} else if peer != nil && peer.GetPeerDataOperationRequestType() == waE2E.PeerDataOperationRequestType_HISTORY_SYNC_ON_DEMAND {
		observation.Kind = "peer_response"
		observation.StanzaID = peer.GetStanzaID()
	} else {
		return
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if len(trace.report.Notifications) < maxDiagnosticResponses {
		trace.report.Notifications = append(trace.report.Notifications, observation)
	} else {
		trace.report.DroppedObservations++
	}
	trace.saveLocked()
}

func (trace *historyDiagnostics) snapshotLocked() map[string]interface{} {
	result := map[string]interface{}{"run_id": trace.report.RunID, "status": trace.report.Status, "updated_at": trace.report.Updated, "diagnostics_available": trace.available, "request_budget": maxDiagnosticAttempts, "decoded_responses": len(trace.report.Responses), "protocol_observations": len(trace.report.Notifications), "dropped_observations": trace.report.DroppedObservations, "correlation": "chat_and_time_only"}
	result["experiment"] = "anchor_comparison"
	if trace.report.PreviousRunID != "" {
		result["experiment"] = "alias_comparison"
		result["request_budget"] = 4
	}
	if trace.report.Mode == "oldest_boundary" {
		result["experiment"] = "oldest_boundary"
		result["request_budget"] = 5
	}
	attempts := make([]map[string]interface{}, 0, len(trace.report.Attempts))
	requests, inserted, duplicates, stored := 0, 0, 0, 0
	for _, attempt := range trace.report.Attempts {
		if attempt.Started != nil {
			requests++
		}
		notifications := 0
		for _, notification := range trace.report.Notifications {
			if attempt.RequestID != "" && (notification.OriginalID == attempt.RequestID || notification.SessionID == attempt.RequestID || notification.StanzaID == attempt.RequestID) {
				notifications++
			}
		}
		attempts = append(attempts, map[string]interface{}{"number": attempt.Number, "label": attempt.Label, "variant": attempt.Variant, "chat_type": historyChatType(attempt.JID), "result": attempt.Result, "anchor_validation": attempt.AnchorValidation, "anchor_at": attempt.Anchor.Timestamp, "anchor_from_me": attempt.Anchor.FromMe, "alias_count": len(attempt.Aliases), "started_at": attempt.Started, "acknowledged_at": attempt.Acknowledged, "finished_at": attempt.Finished, "correlated_protocol_observations": notifications})
		item := attempts[len(attempts)-1]
		item["new_older_message_ids"] = attempt.NewOlderMessages
		for key, coverage := range map[string]*historyCoverage{"coverage_before": attempt.CoverageBefore, "coverage_after": attempt.CoverageAfter} {
			if coverage != nil {
				item[key] = map[string]interface{}{"stored_rows": coverage.Rows, "unique_message_ids": coverage.Messages, "oldest_stored_at": coverage.Oldest, "newest_stored_at": coverage.Newest}
			}
		}
	}
	responses := make([]map[string]interface{}, 0)
	for index, response := range trace.report.Responses {
		inserted += response.Added
		duplicates += response.Duplicates
		if response.Stored != nil {
			stored++
		}
		if index < len(trace.report.Responses)-16 {
			continue
		}
		conversations := make([]map[string]interface{}, 0, len(response.Conversations))
		for _, conversation := range response.Conversations {
			conversations = append(conversations, map[string]interface{}{"labels": conversation.Labels, "match": conversation.Match, "entries": conversation.Entries, "oldest_at": conversation.Oldest, "newest_at": conversation.Newest, "end_flag": conversation.EndFlag})
		}
		responses = append(responses, map[string]interface{}{"number": index + 1, "received_at": response.Received, "active_attempt": response.ActiveAttempt, "stored_at": response.Stored, "inserted_rows": response.Added, "duplicate_entries": response.Duplicates, "storage_commits": response.StorageCommits, "storage_failures": response.StorageFailures, "storage_error": response.StorageError, "conversations": conversations})
	}
	result["requests_started"] = requests
	result["stored_response_batches"] = stored
	result["inserted_rows"] = inserted
	result["duplicate_entries"] = duplicates
	result["attempts"] = attempts
	result["responses"] = responses
	protocol := make([]map[string]interface{}, 0)
	for index, notification := range trace.report.Notifications {
		if index < len(trace.report.Notifications)-16 {
			continue
		}
		matched := make([]int, 0)
		for _, attempt := range trace.report.Attempts {
			if attempt.RequestID != "" && (attempt.RequestID == notification.OriginalID || attempt.RequestID == notification.SessionID || attempt.RequestID == notification.StanzaID) {
				matched = append(matched, attempt.Number)
			}
		}
		protocol = append(protocol, map[string]interface{}{"at": notification.At, "kind": notification.Kind, "matching_attempts": matched, "original_id_present": notification.OriginalID != "", "session_id_present": notification.SessionID != "", "stanza_id_present": notification.StanzaID != ""})
	}
	result["protocol_details"] = protocol
	return result
}

func historyChatType(identifier string) string {
	jid, err := types.ParseJID(identifier)
	if err != nil {
		return "unknown"
	}
	switch jid.Server {
	case types.DefaultUserServer:
		return "phone_number"
	case types.HiddenUserServer:
		return "lid"
	case types.GroupServer:
		return "group"
	default:
		return "unknown"
	}
}

func (trace *historyDiagnostics) snapshot() map[string]interface{} {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return trace.snapshotLocked()
}

func (trace *historyDiagnostics) logSnapshot() {
	data, err := json.Marshal(trace.snapshot())
	if err == nil {
		fmt.Printf("HISTORY_DIAGNOSTIC %s\n", data)
	}
}
