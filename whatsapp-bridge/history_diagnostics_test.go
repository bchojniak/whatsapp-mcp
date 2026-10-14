package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func diagnosticFixture(t *testing.T, store *MessageStore) (*historyDiagnostics, *diagnosticAttempt) {
	t.Helper()
	trace, err := openHistoryDiagnostics(store, "fixture", "source", "")
	if err != nil {
		t.Fatal(err)
	}
	attempt := &diagnosticAttempt{Number: 1, Label: "failed-1", Variant: "failed_boundary", JID: "100@s.whatsapp.net", Aliases: []string{"100@s.whatsapp.net", "200@lid"}, Anchor: historyCursor{ID: "anchor", Sender: "private-sender", Timestamp: time.Unix(100, 0)}, AnchorValidation: "provider_checkpoint_only", Result: "planned"}
	trace.report.Attempts = []*diagnosticAttempt{attempt}
	return trace, attempt
}

func diagnosticHistory(chat string, messages ...*waHistorySync.HistorySyncMsg) *waHistorySync.HistorySync {
	return &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{ID: proto.String(chat), Messages: messages}}}
}

type diagnosticClient struct {
	requested *waE2E.Message
	respond   func()
}

func (client *diagnosticClient) BuildHistorySyncRequest(info *types.MessageInfo, count int) *waE2E.Message {
	return (&whatsmeow.Client{}).BuildHistorySyncRequest(info, count)
}

func (client *diagnosticClient) SendPeerMessage(ctx context.Context, request *waE2E.Message) (whatsmeow.SendResponse, error) {
	client.requested = request
	if client.respond != nil {
		client.respond()
	}
	return whatsmeow.SendResponse{ID: "request-fixture"}, nil
}

func TestDiagnosticWireRequestAndCommittedEvidence(t *testing.T) {
	store, journal := ingestionFixture(t)
	trace, attempt := diagnosticFixture(t, store)
	attempt.Anchor.FromMe = true
	client := &diagnosticClient{respond: func() {
		history := diagnosticHistory("200@lid", historyMessage("older", "synthetic-private-content", 50))
		id := trace.recordReceived(history)
		payload, err := captureHistory(nil, &events.HistorySync{Data: history})
		if err != nil {
			t.Fatal(err)
		}
		payload.HistoryTraceID = id
		if err = journal.enqueue(payload); err != nil {
			t.Fatal(err)
		}
		if !journal.processOne(context.Background()) {
			t.Fatal("not committed")
		}
	}}
	result := trace.request(context.Background(), client, attempt, time.Second)
	request := client.requested.GetProtocolMessage().GetPeerDataOperationRequestMessage().GetHistorySyncOnDemandRequest()
	if request.GetChatJID() != attempt.JID || request.GetOldestMsgID() != "anchor" || request.GetOldestMsgTimestampMS() != 100 || request.GetOnDemandMsgCount() != 50 || !request.GetOldestMsgFromMe() {
		t.Fatalf("wrong request: %v", request)
	}
	if result != "progress_observed" || attempt.Acknowledged == nil || len(trace.report.Responses) != 1 || trace.report.Responses[0].Added != 1 || trace.report.Responses[0].Conversations[0].Match != "alias" {
		t.Fatalf("unexpected diagnostic evidence: %s %+v", result, trace.report)
	}
	data, err := json.Marshal(trace.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"100@s.whatsapp.net", "200@lid", "private-sender", "request-fixture", "synthetic-private-content"} {
		if strings.Contains(string(data), private) {
			t.Fatalf("private field leaked: %s", private)
		}
	}
}

func TestDiagnosticRequiresStorageAndIgnoresUnmatchedReply(t *testing.T) {
	store := backfillFixture(t)
	trace, attempt := diagnosticFixture(t, store)
	client := &diagnosticClient{respond: func() {
		id := trace.recordReceived(diagnosticHistory("unrelated@s.whatsapp.net", historyMessage("x", "", 50)))
		trace.recordStored(id, 0, 0)
		trace.recordReceived(diagnosticHistory(attempt.JID, historyMessage("older", "synthetic", 50)))
	}}
	if result := trace.request(context.Background(), client, attempt, time.Millisecond); result != "response_pending_storage" {
		t.Fatalf("uncommitted response advanced: %s", result)
	}
	if len(trace.report.Responses) != 2 || trace.report.Responses[1].Stored != nil {
		t.Fatal("receipt invented successful storage")
	}
}

func TestDiagnosticTimeoutLateReplyAndRestartDoNotRepeat(t *testing.T) {
	store := backfillFixture(t)
	trace, attempt := diagnosticFixture(t, store)
	trace.report.Status = "running"
	if result := trace.request(context.Background(), &diagnosticClient{}, attempt, time.Millisecond); result != "response_timeout" {
		t.Fatalf("unexpected result: %s", result)
	}
	trace.finishAttempt(attempt, "response_timeout")
	id := trace.recordReceived(diagnosticHistory(attempt.JID, historyMessage("late", "synthetic", 50)))
	trace.recordStored(id, 1, 1)
	if attempt.Result != "response_timeout" || trace.report.Responses[0].ActiveAttempt != 0 {
		t.Fatal("late response rewrote timeout")
	}
	reopened, err := openHistoryDiagnostics(store, "fixture", "source", "")
	if err != nil {
		t.Fatal(err)
	}
	if reopened.report.Status != "interrupted" || reopened.report.Attempts[0].Result != "response_timeout" || reopened.report.Responses[0].Stored == nil {
		t.Fatal("restart discarded evidence or queued uncertain requests")
	}
	if err = reopened.prepare(store, nil); err != nil {
		t.Fatal(err)
	}
	if reopened.report.Status != "interrupted" {
		t.Fatal("interrupted run automatically repeated")
	}
	info, err := os.Stat(reopened.path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("report permissions", err)
	}
	info, err = os.Stat(filepath.Dir(reopened.path))
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal("directory permissions", err)
	}
}

func TestDiagnosticLogFailurePreventsRequest(t *testing.T) {
	store := backfillFixture(t)
	trace, attempt := diagnosticFixture(t, store)
	trace.path = filepath.Join(store.storeDir, "nonexistent", "report.json")
	client := &diagnosticClient{}
	if result := trace.request(context.Background(), client, attempt, time.Millisecond); result != "diagnostics_unavailable" || client.requested != nil {
		t.Fatal("request sent without durable attempt log")
	}
}

func TestDiagnosticQueueRecordsActualDuplicatesAndFailure(t *testing.T) {
	store, journal := ingestionFixture(t)
	trace, _ := diagnosticFixture(t, store)
	if _, err := store.db.Exec(`CREATE TRIGGER fail_fixture BEFORE INSERT ON messages WHEN NEW.id='bad' BEGIN SELECT RAISE(ABORT,'synthetic'); END`); err != nil {
		t.Fatal(err)
	}
	history := diagnosticHistory("100@s.whatsapp.net", historyMessage("older", "synthetic", 50), historyMessage("bad", "synthetic", 40))
	id := trace.recordReceived(history)
	payload, err := captureHistory(nil, &events.HistorySync{Data: history})
	if err != nil {
		t.Fatal(err)
	}
	payload.HistoryTraceID = id
	if err = journal.enqueue(payload); err != nil {
		t.Fatal(err)
	}
	journal.processOne(context.Background())
	if trace.report.Responses[0].Stored != nil || trace.report.Responses[0].StorageError != "constraint_failed" {
		t.Fatal("failed transaction reported saved")
	}
	history = diagnosticHistory("100@s.whatsapp.net", historyMessage("older", "synthetic", 50), historyMessage("older", "synthetic", 50), nil)
	id = trace.recordReceived(history)
	payload, err = captureHistory(nil, &events.HistorySync{Data: history})
	if err != nil {
		t.Fatal(err)
	}
	payload.HistoryTraceID = id
	if err = journal.enqueue(payload); err != nil {
		t.Fatal(err)
	}
	if !journal.processOne(context.Background()) {
		t.Fatal("good batch not processed")
	}
	response := trace.report.Responses[1]
	if response.Stored == nil || response.Added != 1 || response.Duplicates != 1 || response.Eligible != 2 {
		t.Fatalf("wrong committed counts: %+v", response)
	}
}

func TestDiagnosticProtocolIDMatchSurvivesAckOrdering(t *testing.T) {
	store := backfillFixture(t)
	trace, attempt := diagnosticFixture(t, store)
	trace.recordProtocol(&events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{IsFromMe: true}}, Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{HistorySyncNotification: &waE2E.HistorySyncNotification{SyncType: waE2E.HistorySyncType_ON_DEMAND.Enum(), OriginalMessageID: proto.String("request-fixture")}}}})
	attempt.RequestID = "request-fixture"
	data, err := json.Marshal(trace.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"correlated_protocol_observations":1`) || strings.Contains(string(data), "request-fixture") {
		t.Fatalf("protocol match missing or ID leaked: %s", data)
	}
}

func TestDiagnosticPermanentFailureIsDistinctFromPhoneTimeout(t *testing.T) {
	store := backfillFixture(t)
	trace, attempt := diagnosticFixture(t, store)
	client := &diagnosticClient{respond: func() {
		id := trace.recordReceived(diagnosticHistory(attempt.JID, historyMessage("older", "synthetic", 50)))
		trace.recordStorageError(id, "constraint_failed", true)
	}}
	if result := trace.request(context.Background(), client, attempt, time.Second); result != "storage_failed" {
		t.Fatal("storage failure blamed on phone", result)
	}
}

func TestDiagnosticPendingReferenceSurvivesRestart(t *testing.T) {
	store, journal := ingestionFixture(t)
	trace, _ := diagnosticFixture(t, store)
	trace.report.Status = "running"
	history := diagnosticHistory("100@s.whatsapp.net", historyMessage("queued", "synthetic", 50))
	id := trace.recordReceived(history)
	payload, err := captureHistory(nil, &events.HistorySync{Data: history})
	if err != nil {
		t.Fatal(err)
	}
	payload.HistoryTraceID = id
	if err = journal.enqueue(payload); err != nil {
		t.Fatal(err)
	}
	reopened, err := openHistoryDiagnostics(store, "fixture", "source", "")
	if err != nil {
		t.Fatal(err)
	}
	if !journal.processOne(context.Background()) {
		t.Fatal("queued response not committed")
	}
	if reopened.report.Status != "interrupted" || reopened.report.Responses[0].Stored == nil || reopened.report.Responses[0].Added != 1 {
		t.Fatal("diagnostic response lost across restart")
	}
}

func TestDiagnosticSelectionBudgetDeduplicatesAliasesAndPreservesSource(t *testing.T) {
	store := backfillFixture(t)
	report := &backfillReport{Status: "finished", Chats: []*backfillChat{{JID: "100@s.whatsapp.net", Received: 50, Status: "no_backward_progress"}}}
	for index, jid := range []string{"200@s.whatsapp.net", "200@lid", "300@s.whatsapp.net", "400@g.us", "500@s.whatsapp.net"} {
		if err := store.StoreChat(jid, "fixture", time.Unix(300, 0)); err != nil {
			t.Fatal(err)
		}
		if err := store.StoreMessage("new", jid, "sender", "synthetic", time.Unix(300, 0), false, "", "", "", nil, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
		report.Chats = append(report.Chats, &backfillChat{JID: jid, Status: "response_timeout", Cursor: historyCursor{ID: "boundary", Timestamp: time.Unix(int64(50+index), 0)}})
	}
	before, _ := json.Marshal(report)
	attempts, err := selectDiagnosticAttempts(store, report, func(jid string) ([]string, error) {
		if jid == "200@s.whatsapp.net" || jid == "200@lid" {
			return []string{jid, "200@s.whatsapp.net", "200@lid"}, nil
		}
		return []string{jid}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 8 || attempts[0].Variant != "control_start" || attempts[7].Variant != "control_end" || attempts[3].JID == "200@lid" {
		t.Fatalf("bad sample: %+v", attempts)
	}
	after, _ := json.Marshal(report)
	if string(before) != string(after) {
		t.Fatal("diagnostic selection changed original backfill checkpoint")
	}
}

func TestDiagnosticValidatesAnchorTimeAndFromMe(t *testing.T) {
	store := backfillFixture(t)
	_, attempt := diagnosticFixture(t, store)
	attempt.Anchor.ID = "oldest"
	if validation := validateDiagnosticAnchor(store, attempt); validation != "stored_exact" {
		t.Fatal(validation)
	}
	attempt.Anchor.FromMe = true
	if validation := validateDiagnosticAnchor(store, attempt); validation != "stored_anchor_mismatch" {
		t.Fatal(validation)
	}
	attempt.Anchor.FromMe = false
	attempt.Anchor.Timestamp = time.Unix(101, 0)
	if validation := validateDiagnosticAnchor(store, attempt); validation != "stored_anchor_mismatch" {
		t.Fatal(validation)
	}
}

func TestDiagnosticConcurrentObservationsRemainBounded(t *testing.T) {
	store := backfillFixture(t)
	trace, _ := diagnosticFixture(t, store)
	var workers sync.WaitGroup
	for index := 0; index < 12; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			id := trace.recordReceived(diagnosticHistory("100@s.whatsapp.net"))
			trace.recordStored(id, 0, 0)
			trace.snapshot()
		}()
	}
	workers.Wait()
	if len(trace.report.Responses) != 12 {
		t.Fatal("lost concurrent observations")
	}
	trace.mu.Lock()
	for len(trace.report.Responses) < maxDiagnosticResponses {
		trace.report.Responses = append(trace.report.Responses, &diagnosticResponse{})
	}
	trace.mu.Unlock()
	if trace.recordReceived(diagnosticHistory("100@s.whatsapp.net")) != "" || trace.report.DroppedObservations != 1 || len(trace.snapshot()["responses"].([]map[string]interface{})) != 16 {
		t.Fatal("unbounded diagnostic observations")
	}
}

func TestAliasDiagnosticChangesOnlyKnownWireAddress(t *testing.T) {
	store := backfillFixture(t)
	now := time.Now().UTC()
	previous := &diagnosticReport{Status: "finished", Attempts: []*diagnosticAttempt{{Label: "control", Variant: "control_end", JID: "100@s.whatsapp.net", Aliases: []string{"100@s.whatsapp.net"}, Finished: &now, Result: "progress_observed"}}}
	for index, jid := range []string{"300@s.whatsapp.net", "400@s.whatsapp.net", "500@s.whatsapp.net"} {
		if err := store.StoreChat(jid, "fixture", time.Unix(100, 0)); err != nil {
			t.Fatal(err)
		}
		if err := store.StoreMessage("boundary", jid, "sender", "synthetic", time.Unix(100, 0), false, "", "", "", nil, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
		source := &diagnosticAttempt{Label: fmt.Sprintf("failed-%d", index+1), Variant: "failed_boundary", JID: fmt.Sprintf("%d@lid", index+1), Aliases: []string{fmt.Sprintf("%d@lid", index+1), jid}, Anchor: historyCursor{ID: "boundary", Timestamp: time.Unix(100, 0)}, Result: "response_timeout", Acknowledged: &now}
		previous.Attempts = append(previous.Attempts, source)
	}
	before, _ := json.Marshal(previous)
	attempts, err := selectAliasDiagnosticAttempts(store, previous, func(jid string) ([]string, error) {
		for _, source := range previous.Attempts {
			if source.JID == jid {
				return source.Aliases, nil
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 4 || attempts[3].Variant != "control_end" {
		t.Fatalf("wrong comparison budget: %d", len(attempts))
	}
	for index, attempt := range attempts[:3] {
		source := previous.Attempts[index+1]
		if attempt.Anchor != source.Anchor || attempt.JID == source.JID || attempt.Variant != "alternate_alias" || attempt.AnchorValidation != "stored_exact" || attempt.Started != nil || attempt.RequestID != "" {
			t.Fatal("comparison changed anchor or retained send state")
		}
		originalJID, _ := types.ParseJID(source.JID)
		aliasJID, _ := types.ParseJID(attempt.JID)
		client := &whatsmeow.Client{}
		original := client.BuildHistorySyncRequest(source.Anchor.messageInfo(originalJID), 50)
		alternate := client.BuildHistorySyncRequest(attempt.Anchor.messageInfo(aliasJID), 50)
		original.GetProtocolMessage().GetPeerDataOperationRequestMessage().GetHistorySyncOnDemandRequest().ChatJID = proto.String(attempt.JID)
		if !proto.Equal(original, alternate) {
			t.Fatal("wire request changed more than the chat address")
		}
	}
	after, _ := json.Marshal(previous)
	if string(before) != string(after) {
		t.Fatal("comparison changed prior report")
	}
	old := now.Add(-21 * time.Minute)
	previous.Attempts[0].Finished = &old
	if _, err = selectAliasDiagnosticAttempts(store, previous, nil); err == nil {
		t.Fatal("stale control accepted")
	}
}

func TestAliasDiagnosticRejectsUnverifiedAliasOrAnchor(t *testing.T) {
	store := backfillFixture(t)
	now := time.Now().UTC()
	previous := &diagnosticReport{Status: "finished", Attempts: []*diagnosticAttempt{
		{Label: "control", Variant: "control_end", JID: "100@s.whatsapp.net", Aliases: []string{"100@s.whatsapp.net"}, Finished: &now, Result: "progress_observed"},
		{Label: "failed-1", Variant: "failed_boundary", JID: "200@lid", Aliases: []string{"200@lid"}, Anchor: historyCursor{ID: "oldest", Timestamp: time.Unix(100, 0)}, Result: "response_timeout", Acknowledged: &now},
	}}
	resolver := func(jid string) ([]string, error) { return []string{jid, "100@s.whatsapp.net"}, nil }
	if _, err := selectAliasDiagnosticAttempts(store, previous, resolver); err == nil {
		t.Fatal("alias missing from prior experiment accepted")
	}
	previous.Attempts[1].Aliases = append(previous.Attempts[1].Aliases, "100@s.whatsapp.net")
	previous.Attempts[1].Anchor.ID = "not-stored"
	if _, err := selectAliasDiagnosticAttempts(store, previous, resolver); err == nil {
		t.Fatal("unverified anchor accepted")
	}
}
