package main

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
)

func addRecoveryMessage(t *testing.T, store *MessageStore, chat, id string, timestamp int64) {
	t.Helper()
	if err := store.StoreChat(chat, "synthetic", time.Unix(timestamp, 0)); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreMessage(id, chat, "synthetic", "synthetic", time.Unix(timestamp, 0), false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryPreferencePersistsAndRequiresCurrentMapping(t *testing.T) {
	store := backfillFixture(t)
	aliases := []string{"100@s.whatsapp.net", "200@lid"}
	if err := store.rememberHistoryIdentifier(aliases, aliases[0]); err != nil {
		t.Fatal(err)
	}
	identifier, err := store.preferredHistoryIdentifier(aliases, aliases[1])
	if err != nil || identifier != aliases[0] {
		t.Fatalf("verified preference ignored: %s %v", identifier, err)
	}
	reopened, err := NewMessageStore(store.storeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	identifier, err = reopened.preferredHistoryIdentifier(aliases, aliases[1])
	if err != nil || identifier != aliases[0] {
		t.Fatal("preference did not survive reopening")
	}
	identifier, err = store.preferredHistoryIdentifier([]string{aliases[1]}, aliases[1])
	if err != nil || identifier != aliases[1] {
		t.Fatal("stale mapping was followed")
	}
	if err := store.rememberHistoryIdentifier(aliases, "unrelated@s.whatsapp.net"); err == nil {
		t.Fatal("unverified preference accepted")
	}
}

func TestHistoryCoverageExcludesAliasCopiesAndNewActivity(t *testing.T) {
	store := backfillFixture(t)
	aliases := []string{"100@s.whatsapp.net", "200@lid"}
	addRecoveryMessage(t, store, aliases[1], "lid-only", 200)
	before, err := store.historyCoverage(aliases)
	if err != nil || before.Rows != 3 || before.Messages != 3 || before.Oldest.Unix() != 100 {
		t.Fatalf("incorrect baseline: %+v %v", before, err)
	}
	addRecoveryMessage(t, store, aliases[1], "oldest", 100)
	addRecoveryMessage(t, store, aliases[0], "recovered", 50)
	addRecoveryMessage(t, store, aliases[1], "recovered", 50)
	addRecoveryMessage(t, store, aliases[0], "live", 400)
	addRecoveryMessage(t, store, "unrelated@s.whatsapp.net", "unrelated", 25)
	count, err := store.newOlderHistoryMessages(aliases, before, time.Unix(100, 0))
	if err != nil || count != 1 {
		t.Fatalf("copies or activity counted as recovery: %d %v", count, err)
	}
	after, err := store.historyCoverage(aliases)
	if err != nil || after.Rows != 7 || after.Messages != 5 || after.Oldest.Unix() != 50 {
		t.Fatalf("incorrect final coverage: %+v %v", after, err)
	}
	anchor, err := store.oldestHistoryAnchor(aliases)
	if err != nil || anchor.ID != "recovered" || anchor.Timestamp.Unix() != 50 {
		t.Fatalf("incorrect logical boundary: %+v %v", anchor, err)
	}
}

func verifiedRecoverySource(t *testing.T, store *MessageStore) *diagnosticReport {
	t.Helper()
	addRecoveryMessage(t, store, "200@lid", "recent-only", 500)
	addRecoveryMessage(t, store, "control@g.us", "control-anchor", 600)
	started, finished := time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour)
	anchor, _ := newestDiagnosticAnchor(store, "200@lid")
	controlAnchor, _ := newestDiagnosticAnchor(store, "control@g.us")
	failed := &diagnosticAttempt{Number: 1, Label: "failed-1", Variant: "alternate_alias", JID: "100@s.whatsapp.net", Aliases: []string{"100@s.whatsapp.net", "200@lid"}, Anchor: anchor, Started: &started, Finished: &finished, RequestID: "reply-1", Result: "progress_observed"}
	control := &diagnosticAttempt{Number: 2, Label: "control", Variant: "control_end", JID: "control@g.us", Aliases: []string{"control@g.us"}, Anchor: controlAnchor, Started: &started, Finished: &finished, RequestID: "reply-2", Result: "no_backward_progress_observed"}
	report := &diagnosticReport{Status: "finished", Attempts: []*diagnosticAttempt{failed, control}}
	for _, attempt := range report.Attempts {
		report.Notifications = append(report.Notifications, diagnosticNotification{SessionID: attempt.RequestID})
		report.Responses = append(report.Responses, &diagnosticResponse{ActiveAttempt: attempt.Number, Received: started.Add(time.Minute), Stored: &finished, Conversations: []diagnosticConversation{{JID: attempt.JID}}})
	}
	return report
}

func TestRecoveryUsesOldestLinkedAnchorAndFreshControls(t *testing.T) {
	store := backfillFixture(t)
	source := verifiedRecoverySource(t, store)
	aliasesFor := func(chat string) ([]string, error) {
		if chat == "control@g.us" {
			return []string{chat}, nil
		}
		return []string{"100@s.whatsapp.net", "200@lid"}, nil
	}
	attempts, err := selectRecoveryAttempts(store, source, aliasesFor)
	if err != nil || len(attempts) != 3 || attempts[0].Variant != "control_start" || attempts[2].Variant != "control_end" {
		t.Fatalf("controls missing: %+v %v", attempts, err)
	}
	if attempts[1].Anchor.ID != "oldest" || attempts[1].JID != "100@s.whatsapp.net" || attempts[1].Anchor.Timestamp.Unix() != 100 {
		t.Fatalf("physical or recent boundary used: %+v", attempts[1])
	}
	source.Notifications = nil
	if _, err := selectRecoveryAttempts(store, source, aliasesFor); err == nil {
		t.Fatal("unverified reply accepted")
	}
	source = verifiedRecoverySource(t, store)
	if _, err := selectRecoveryAttempts(store, source, func(chat string) ([]string, error) { return []string{chat}, nil }); err == nil {
		t.Fatal("changed alias mapping accepted")
	}
}

func TestConsolidatedBackfillSendsOnceThroughWorkingIdentifier(t *testing.T) {
	store := backfillFixture(t)
	addRecoveryMessage(t, store, "200@lid", "lid-anchor", 500)
	report := &backfillReport{Chats: []*backfillChat{
		{JID: "200@lid", Status: "pending", Cursor: historyCursor{ID: "lid-anchor", Timestamp: time.Unix(500, 0)}},
		{JID: "100@s.whatsapp.net", Status: "no_backward_progress", Received: 50},
	}}
	aliases := []string{"100@s.whatsapp.net", "200@lid"}
	if err := consolidateBackfillChats(store, report, func(string) ([]string, error) { return aliases, nil }, true); err != nil {
		t.Fatal(err)
	}
	leader := report.Chats[0]
	if leader.RequestJID != aliases[0] || leader.Cursor.ID != "oldest" || report.Chats[1].LinkedTo != leader.JID {
		t.Fatalf("conversation not consolidated: %+v %+v", leader, report.Chats[1])
	}
	mailbox := &backfillMailbox{responses: make(chan backfillReply, 16), storageFailed: true}
	history := diagnosticHistory("200@lid", historyMessage("older", "synthetic", 50))
	client := &fakeHistoryClient{client: &whatsmeow.Client{}, mailbox: mailbox, response: &events.HistorySync{Data: history}}
	backfillPage(context.Background(), client, mailbox, leader)
	request := client.requested.GetProtocolMessage().GetPeerDataOperationRequestMessage().GetHistorySyncOnDemandRequest()
	if request.GetChatJID() != aliases[0] || request.GetOldestMsgID() != "oldest" || leader.Cursor.ID != "older" || leader.Status != "pending" {
		t.Fatalf("alias reply not accepted or wrong wire request: %+v %v", leader, request)
	}
}

func TestLinkedCheckpointDoesNotQueueASecondConversationOnResume(t *testing.T) {
	store := backfillFixture(t)
	report := &backfillReport{Status: "finished", Chats: []*backfillChat{
		{JID: "100@s.whatsapp.net", Status: "no_backward_progress"},
		{JID: "200@lid", Status: "response_timeout", LinkedTo: "100@s.whatsapp.net"},
	}}
	if resumeBackfill(report, "next") || report.Status != "finished" || report.Chats[1].Status != "response_timeout" {
		t.Fatalf("linked physical failure scheduled independently: %+v", report)
	}
	if err := consolidateBackfillChats(store, report, func(chat string) ([]string, error) { return []string{chat}, nil }, true); err != nil {
		t.Fatal(err)
	}
	independent := report.Chats[1]
	if !resumeBackfill(report, "after-mapping-changed") || independent.LinkedTo != "" || independent.Status != "pending" {
		t.Fatal("stale grouping suppressed an independent conversation")
	}
}

func TestBoundaryRecoveryIsNotRepeatedAfterRestart(t *testing.T) {
	store := backfillFixture(t)
	trace, err := openHistoryDiagnostics(store, "boundary", "source", "previous", "oldest_boundary")
	if err != nil {
		t.Fatal(err)
	}
	trace.report.Status = "running"
	trace.report.Attempts = []*diagnosticAttempt{{Result: "waiting"}}
	if err := trace.saveLocked(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openHistoryDiagnostics(store, "boundary", "source", "previous", "oldest_boundary")
	if err != nil || reopened.report.Status != "interrupted" || reopened.report.Attempts[0].Result != "interrupted" {
		t.Fatalf("uncertain request repeated: %+v %v", reopened, err)
	}
	if _, err := openHistoryDiagnostics(store, "boundary", "source", "previous"); err == nil {
		t.Fatal("mode changed for consumed run")
	}
}
