package main

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
)

func TestMetadataReplayUpgradePreservesHistoryProgressAndRunsOnce(t *testing.T) {
	store := backfillFixture(t)
	chat := &backfillChat{JID: "100@s.whatsapp.net", Status: "pending", Pages: 4,
		Cursor: historyCursor{ID: "old-provider-cursor", Timestamp: time.Unix(50, 0)}}
	unrequested := &backfillChat{JID: "100@s.whatsapp.net", Status: "pending"}
	report := &backfillReport{Chats: []*backfillChat{chat, unrequested}, MessagesAdded: 10}
	if err := initializeMetadataReplay(store, report); err != nil {
		t.Fatal(err)
	}
	if chat.Cursor.ID != "old-provider-cursor" || chat.Pages != 4 || report.MessagesAdded != 10 {
		t.Fatal("upgrade reset history progress")
	}
	if chat.MetadataReplay == nil || chat.MetadataReplay.Until.ID != chat.Cursor.ID || unrequested.MetadataReplay != nil {
		t.Fatal("wrong replay selection")
	}
	chat.MetadataReplay.Status = "captured_requested_range"
	if err := initializeMetadataReplay(store, report); err != nil || chat.MetadataReplay.Status != "captured_requested_range" {
		t.Fatal("upgrade repeated a completed replay")
	}
}

func TestMetadataReplayStopsAtPreviousBoundaryAndKeepsPrimaryCursor(t *testing.T) {
	mailbox := &backfillMailbox{responses: make(chan backfillReply, 1)}
	client := &fakeHistoryClient{client: &whatsmeow.Client{}, mailbox: mailbox}
	chat := &backfillChat{JID: "100@s.whatsapp.net", Status: "phone_reports_no_older_messages", Pages: 8,
		Cursor:         historyCursor{ID: "primary", Timestamp: time.Unix(20, 0)},
		MetadataReplay: &metadataReplayState{Status: "pending", Cursor: historyCursor{ID: "newest", Timestamp: time.Unix(100, 0)}, Until: historyCursor{ID: "older", Timestamp: time.Unix(50, 0)}}}
	if result := backfillMetadataPage(context.Background(), client, mailbox, chat); result != "captured_requested_range" {
		t.Fatalf("result %s", result)
	}
	if chat.Cursor.ID != "primary" || chat.Pages != 8 || chat.Status != "phone_reports_no_older_messages" {
		t.Fatal("replay changed the primary checkpoint")
	}
}

func TestMetadataReplayRetryBudgetAndCheckpointSurviveRestart(t *testing.T) {
	store := backfillFixture(t)
	report, path, err := prepareBackfill(store, "metadata-retry")
	if err != nil {
		t.Fatal(err)
	}
	chat := report.Chats[0]
	chat.Status = "phone_reports_no_older_messages"
	chat.MetadataReplay = &metadataReplayState{Status: "response_timeout", AutomaticRetries: 2, Cursor: historyCursor{ID: "saved"}}
	report.Status = "finished"
	if !eligibleBackfillRecovery(report) || !resumeBackfill(report, "metadata-retry-1") || chat.MetadataReplay.Status != "pending" {
		t.Fatal("eligible metadata replay did not resume")
	}
	if err := saveBackfillReport(path, report); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := prepareBackfill(store, report.RunID)
	if err != nil {
		t.Fatal(err)
	}
	replay := loaded.Chats[0].MetadataReplay
	if replay.Cursor.ID != "saved" || replay.AutomaticRetries != 2 {
		t.Fatal("restart erased replay progress")
	}
	replay.Status, replay.AutomaticRetries = "response_timeout", 3
	if eligibleMetadataReplay(replay) || replay.Status != "retry_limit_reached_unconfirmed" {
		t.Fatal("metadata replay retried forever")
	}
}
