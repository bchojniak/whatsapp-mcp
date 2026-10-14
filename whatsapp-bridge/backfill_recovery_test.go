package main

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types/events"
)

func TestContinuousBackfillKeepsActiveSweepAndSavedCursor(t *testing.T) {
	report := &backfillReport{Status: "running", ResumeID: "consumed", MessagesAdded: 10,
		Chats: []*backfillChat{{Status: "pending", Pages: 20, LastResult: "progress", Cursor: historyCursor{ID: "provider-only-sticker"}}}}
	marker, run := planBackfillRecovery(report, true, time.Now().UTC(), "consumed")
	if !run || marker != "consumed" || report.Recovery.AutomaticResumes != 0 || report.Recovery.NextAttemptAt != nil {
		t.Fatal("active sweep was restarted or delayed")
	}
	if report.Chats[0].Cursor.ID != "provider-only-sticker" || report.Chats[0].Pages != 20 || report.MessagesAdded != 10 {
		t.Fatal("continuous mode reset existing progress")
	}
}

func TestContinuousBackfillResumeDoesNotSkipInternalHistory(t *testing.T) {
	store := backfillFixture(t)
	chat := &backfillChat{JID: "100@s.whatsapp.net", Status: "pending", LastResult: "progress", Pages: 1,
		Cursor: historyCursor{ID: "provider-only-sticker", Timestamp: time.Unix(250, 0)}}
	report := &backfillReport{Recovery: &backfillRecoveryState{}, Chats: []*backfillChat{chat}}
	if err := consolidateBackfillChats(store, report, func(jid string) ([]string, error) { return []string{jid}, nil }, true); err != nil {
		t.Fatal(err)
	}
	if chat.Cursor.ID != "provider-only-sticker" || chat.Cursor.Timestamp.Unix() != 250 {
		t.Fatal("continuous resume skipped unvisited pages by jumping to the oldest stored message")
	}
}

func TestContinuousBackfillKeepsPendingRetriesAfterInterruption(t *testing.T) {
	now := time.Now().UTC()
	chat := &backfillChat{Status: "pending", LastResult: "response_timeout", Pages: 2, AutomaticRetries: 1}
	report := &backfillReport{Status: "interrupted", Chats: []*backfillChat{chat}}
	if _, run := planBackfillRecovery(report, true, now, ""); run || report.Recovery.Status != "waiting_for_retry" {
		t.Fatal("interrupted queued retry was discarded")
	}
	marker, run := planBackfillRecovery(report, true, now.Add(5*time.Minute), "")
	if !run || !resumeBackfill(report, marker) || chat.Status != "pending" {
		t.Fatal("interrupted queued retry did not resume")
	}
}

func TestContinuousBackfillWaitSurvivesRestartAndResumesOnlyEligibleChats(t *testing.T) {
	store := backfillFixture(t)
	report, path, err := prepareBackfill(store, "continuous-test")
	if err != nil {
		t.Fatal(err)
	}
	report.Status = "phone_unresponsive"
	report.Chats[0].Status = "response_timeout"
	report.Chats[0].LastResult = "response_timeout"
	report.Chats[0].Cursor.ID = "saved-provider-cursor"
	report.Chats[0].Pages = 4
	report.Chats = append(report.Chats, &backfillChat{Status: "phone_reports_no_older_messages", Pages: 9})
	now := time.Now().UTC()
	if _, run := planBackfillRecovery(report, true, now, ""); run {
		t.Fatal("phone pause retried without waiting")
	}
	deadline := now.Add(5 * time.Minute)
	if report.Recovery.Status != "waiting_for_phone" || !report.Recovery.NextAttemptAt.Equal(deadline) {
		t.Fatal("phone retry was not scheduled")
	}
	if err := saveBackfillReport(path, report); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := prepareBackfill(store, "continuous-test")
	if err != nil {
		t.Fatal(err)
	}
	if _, run := planBackfillRecovery(loaded, true, now.Add(2*time.Minute), ""); run || !loaded.Recovery.NextAttemptAt.Equal(deadline) {
		t.Fatal("restart skipped or extended the saved wait")
	}
	marker, run := planBackfillRecovery(loaded, true, deadline, "")
	if !run || marker != "automatic-1" || !resumeBackfill(loaded, marker) {
		t.Fatal("saved retry did not resume")
	}
	if loaded.Chats[0].Cursor.ID != "saved-provider-cursor" || loaded.Chats[0].Pages != 4 || loaded.Chats[1].Status != "phone_reports_no_older_messages" {
		t.Fatal("retry lost progress or reopened a confirmed chat")
	}
}

func TestContinuousBackfillDoesNotTreatBlockedHistoryAsComplete(t *testing.T) {
	report := &backfillReport{Status: "finished", Chats: []*backfillChat{
		{Status: "response_timeout", AutomaticRetries: 3, Cursor: historyCursor{ID: "saved"}},
		{Status: "no_anchor"}, {Status: "unsupported_chat"},
		{Status: "older_messages_not_accessible"}, {Status: "no_backward_progress"},
		{Status: "empty_response_unconfirmed"}, {Status: "phone_reports_no_older_messages"},
	}}
	now := time.Now().UTC()
	if _, run := planBackfillRecovery(report, true, now, ""); run {
		t.Fatal("blocked history was blindly retried")
	}
	if report.Recovery.Status != "awaiting_source_data" || !report.Recovery.NextAttemptAt.Equal(now.Add(time.Hour)) {
		t.Fatal("blocked history was not retained for source recovery")
	}
	if report.Chats[0].Status != "retry_limit_reached_unconfirmed" || report.Chats[0].Cursor.ID != "saved" || report.Chats[0].LastResult != "response_timeout" {
		t.Fatal("retry budget erased failure evidence or cursor")
	}
}

func TestContinuousBackfillStopsOnStorageFailureAndBacksOffPhonePauses(t *testing.T) {
	now := time.Now().UTC()
	for _, status := range []string{"storage_failed", "verification_failed"} {
		next := now.Add(time.Minute)
		report := &backfillReport{Status: status, Recovery: &backfillRecoveryState{NextAttemptAt: &next}, Chats: []*backfillChat{{Status: "response_timeout"}}}
		if _, run := planBackfillRecovery(report, true, now, ""); run || report.Recovery.Status != "blocked_storage" || report.Recovery.NextAttemptAt != nil {
			t.Fatalf("unsafe recovery of %s", status)
		}
	}
	for pauses, expected := range map[int]time.Duration{0: 5 * time.Minute, 1: 10 * time.Minute, 2: 20 * time.Minute, 3: 30 * time.Minute, 100: 30 * time.Minute} {
		report := &backfillReport{Status: "phone_unresponsive", Recovery: &backfillRecoveryState{PhonePauses: pauses}, Chats: []*backfillChat{{Status: "pending"}}}
		if _, run := planBackfillRecovery(report, true, now, ""); run || !report.Recovery.NextAttemptAt.Equal(now.Add(expected)) {
			t.Fatalf("incorrect backoff after %d pauses", pauses)
		}
		if report.Chats[0].AutomaticRetries != 0 {
			t.Fatal("waiting for the phone spent a chat retry")
		}
	}
}

func TestContinuousBackfillProgressResetsRetryBudget(t *testing.T) {
	mailbox := &backfillMailbox{responses: make(chan backfillReply, 1)}
	client := &fakeHistoryClient{client: &whatsmeow.Client{}, mailbox: mailbox}
	chat := &backfillChat{JID: "100@s.whatsapp.net", Status: "pending", LastResult: "response_timeout", AutomaticRetries: 2,
		Cursor: historyCursor{ID: "anchor", Timestamp: time.Unix(100, 0)}}
	backfillPageWithRecovery(context.Background(), client, mailbox, chat, true)
	if chat.AutomaticRetries != 0 || chat.LastResult != "progress" || chat.Cursor.ID != "older" {
		t.Fatal("successful recovery did not reset the retry budget")
	}
	client.response = &events.HistorySync{Data: &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum()}}
	chat.Status, chat.LastResult, chat.AutomaticRetries = "pending", "response_timeout", 2
	backfillPageWithRecovery(context.Background(), client, mailbox, chat, true)
	if chat.AutomaticRetries != 3 || chat.Status != "empty_response_unconfirmed" {
		t.Fatal("retry without progress was not counted")
	}
}

func TestBackfillAliasDiscoveryPreservesConfirmedConversation(t *testing.T) {
	store := backfillFixture(t)
	report, _, err := prepareBackfill(store, "alias-discovery")
	if err != nil {
		t.Fatal(err)
	}
	leader := report.Chats[0]
	leader.Aliases = []string{leader.JID, "888@lid"}
	leader.Status = "phone_reports_no_older_messages"
	leader.Pages = 5
	if err := store.StoreChat("888@lid", "Known alias", time.Unix(300, 0)); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreMessage("newest", "888@lid", "888", "alias copy", time.Unix(300, 0), false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := refreshBackfillChats(store, report); err != nil {
		t.Fatal(err)
	}
	if len(report.Chats) != 1 || leader.Status != "phone_reports_no_older_messages" || leader.Pages != 5 {
		t.Fatal("discovering an existing alias reopened a confirmed conversation")
	}
}

func TestBackfillNewAliasAnchorPreservesVerifiedRequestAddress(t *testing.T) {
	store := backfillFixture(t)
	leader := &backfillChat{JID: "200@s.whatsapp.net", RequestJID: "200@s.whatsapp.net", Status: "no_anchor", Aliases: []string{"200@s.whatsapp.net", "999@lid"}}
	report := &backfillReport{Chats: []*backfillChat{leader}}
	if err := store.StoreChat("999@lid", "Known alias", time.Unix(400, 0)); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreMessage("first", "999@lid", "999", "new anchor", time.Unix(400, 0), false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := refreshBackfillChats(store, report); err != nil {
		t.Fatal(err)
	}
	if leader.Status != "pending" || leader.Cursor.ID != "first" || leader.JID != "200@s.whatsapp.net" || leader.RequestJID != "200@s.whatsapp.net" || len(leader.Aliases) != 2 {
		t.Fatal("new anchor lost the verified conversation address")
	}
}

func TestBackfillRecoveryWaitCancelsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitForBackfillRecovery(ctx, time.Now().Add(time.Hour)) {
		t.Fatal("shutdown did not cancel a scheduled wait")
	}
}
