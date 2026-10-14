package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func backfillFixture(t *testing.T) *MessageStore {
	t.Helper()
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.StoreChat("100@s.whatsapp.net", "Existing contact", time.Unix(300, 0)); err != nil {
		t.Fatal(err)
	}
	for id, timestamp := range map[string]int64{"oldest": 100, "newest": 300} {
		if err := store.StoreMessage(id, "100@s.whatsapp.net", "100", "original", time.Unix(timestamp, 0), false,
			"image", "already-downloaded.jpg", "original-url", []byte("original-key"), nil, nil, 123); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func TestOnDemandPreservesExistingDataAndFillsGaps(t *testing.T) {
	store := backfillFixture(t)
	conversation := &waHistorySync.Conversation{ID: proto.String("100@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
		nil,
		historyMessage("oldest", "overwrite attempted", 100),
		historyMessage("gap", "missing middle message", 200),
		historyMessage("older", "older message", 50),
	}}
	history := &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{conversation}}
	for i := 0; i < 2; i++ {
		if err := storeOnDemandHistory(nil, store, history); err != nil {
			t.Fatal(err)
		}
	}
	var content, filename, url string
	var key []byte
	if err := store.db.QueryRow(`SELECT content, filename, url, media_key FROM messages WHERE id='oldest'`).Scan(&content, &filename, &url, &key); err != nil {
		t.Fatal(err)
	}
	if content != "original" || filename != "already-downloaded.jpg" || url != "original-url" || string(key) != "original-key" {
		t.Fatalf("existing message overwritten: %q %q %q %q", content, filename, url, key)
	}
	var name string
	var latest time.Time
	if err := store.db.QueryRow(`SELECT name, last_message_time FROM chats`).Scan(&name, &latest); err != nil {
		t.Fatal(err)
	}
	if name != "Existing contact" || latest.Unix() != 300 {
		t.Fatalf("chat metadata changed: %q %v", name, latest)
	}
	summary, err := store.archiveSummary()
	if err != nil || summary.Messages != 4 || summary.Oldest.Unix() != 50 {
		t.Fatalf("history not stored: %+v %v", summary, err)
	}
}

func TestOnDemandStorageFailureRollsBackWholeBatch(t *testing.T) {
	store := backfillFixture(t)
	if _, err := store.db.Exec(`CREATE TRIGGER reject_test BEFORE INSERT ON messages WHEN NEW.id='fail' BEGIN SELECT RAISE(ABORT, 'test failure'); END`); err != nil {
		t.Fatal(err)
	}
	history := &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{{
		ID: proto.String("100@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{historyMessage("new", "first", 50), historyMessage("fail", "fail", 40)},
	}}}
	if err := storeOnDemandHistory(nil, store, history); err == nil {
		t.Fatal("storage failure was ignored")
	}
	summary, err := store.archiveSummary()
	if err != nil || summary.Messages != 2 {
		t.Fatalf("part of failed response was committed: %+v %v", summary, err)
	}
}

func TestBackfillCursorAndExplicitCompletion(t *testing.T) {
	for _, tc := range []struct {
		flag   *waHistorySync.Conversation_EndOfHistoryTransferType
		status string
	}{
		{nil, "pending"},
		{waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY.Enum(), "phone_reports_no_older_messages"},
		{waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS.Enum(), "older_messages_not_accessible"},
		{waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_BUT_MORE_MSG_REMAIN_ON_PRIMARY.Enum(), "pending"},
	} {
		chat := &backfillChat{Status: "pending", Cursor: historyCursor{ID: "anchor", Timestamp: time.Unix(100, 0)}}
		applyBackfillResponse(chat, &waHistorySync.Conversation{EndOfHistoryTransferType: tc.flag, Messages: []*waHistorySync.HistorySyncMsg{
			historyMessage("older", "", 50), nil, historyMessage("newer", "", 150),
		}})
		if chat.Status != tc.status || chat.Cursor.ID != "older" || chat.Cursor.Timestamp.Unix() != 50 {
			t.Fatalf("bad response result: %+v", chat)
		}
	}
	chat := &backfillChat{Status: "pending", Cursor: historyCursor{ID: "anchor", Timestamp: time.Unix(100, 0)}}
	applyBackfillResponse(chat, &waHistorySync.Conversation{})
	if chat.Status != "no_backward_progress" {
		t.Fatalf("empty response claimed completion: %s", chat.Status)
	}
}

func TestBackfillSnapshotAndOneOffCheckpoint(t *testing.T) {
	store := backfillFixture(t)
	report, path, err := prepareBackfill(store, "test-run")
	if err != nil {
		t.Fatal(err)
	}
	if report.Before.Messages != 2 || len(report.Chats) != 1 || report.Chats[0].Cursor.ID != "newest" {
		t.Fatalf("wrong starting coverage or anchor: %+v", report)
	}
	for _, path := range []string{path, filepath.Join(filepath.Dir(path), "messages.before.db")} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("private backfill artifact permissions: %v %v", info, err)
		}
	}
	report.Status = "finished"
	if err := saveBackfillReport(path, report); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := prepareBackfill(store, "test-run")
	if err != nil || loaded.Status != "finished" {
		t.Fatalf("completed run was recreated: %+v %v", loaded, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint backfillReport
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, _, err := prepareBackfill(store, "../escape"); err == nil {
		t.Fatal("invalid report path was accepted")
	}
}

type fakeHistoryClient struct {
	client     *whatsmeow.Client
	mailbox    *backfillMailbox
	storeError error
	requested  *waE2E.Message
	response   *events.HistorySync
}

func (client *fakeHistoryClient) BuildHistorySyncRequest(info *types.MessageInfo, count int) *waE2E.Message {
	return client.client.BuildHistorySyncRequest(info, count)
}

func (client *fakeHistoryClient) SendPeerMessage(ctx context.Context, message *waE2E.Message) (whatsmeow.SendResponse, error) {
	client.requested = message
	response := client.response
	if response == nil {
		response = &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{
				ID: proto.String("100@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{historyMessage("older", "returned", 50)},
			}},
		}}
	}
	client.mailbox.handle(response, client.storeError)
	return whatsmeow.SendResponse{}, nil
}

func TestBackfillUsesPeerRequestAndAdvancesOnlyAfterStorage(t *testing.T) {
	for _, storageError := range []error{nil, errors.New("disk full")} {
		mailbox := &backfillMailbox{responses: make(chan backfillReply, 16)}
		client := &fakeHistoryClient{client: &whatsmeow.Client{}, mailbox: mailbox, storeError: storageError}
		chat := &backfillChat{JID: "100@s.whatsapp.net", Status: "pending", Cursor: historyCursor{ID: "anchor", Timestamp: time.Unix(100, 0)}}
		backfillPage(context.Background(), client, mailbox, chat)
		request := client.requested.GetProtocolMessage().GetPeerDataOperationRequestMessage().GetHistorySyncOnDemandRequest()
		if request.GetChatJID() != chat.JID || request.GetOldestMsgID() != "anchor" || request.GetOnDemandMsgCount() != 50 || request.GetOldestMsgTimestampMS() != 100 {
			t.Fatalf("incorrect request: %+v", request)
		}
		if storageError == nil && (chat.Cursor.ID != "older" || chat.LastResult != "progress") {
			t.Fatal("stored response did not advance cursor")
		}
		if storageError != nil && (chat.Status != "storage_failed" || chat.Cursor.ID != "anchor" || chat.LastResult != "storage_failed") {
			t.Fatal("failed storage advanced cursor")
		}
	}
}

func TestEmptyAndUnmatchedResponsesRemainUnconfirmed(t *testing.T) {
	for _, tc := range []struct {
		conversations []*waHistorySync.Conversation
		status        string
	}{
		{nil, "empty_response_unconfirmed"},
		{[]*waHistorySync.Conversation{{ID: proto.String("unrelated@s.whatsapp.net")}}, "unmatched_response_unconfirmed"},
	} {
		mailbox := &backfillMailbox{responses: make(chan backfillReply, 1)}
		client := &fakeHistoryClient{client: &whatsmeow.Client{}, mailbox: mailbox, response: &events.HistorySync{Data: &waHistorySync.HistorySync{
			SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: tc.conversations,
		}}}
		chat := &backfillChat{JID: "100@s.whatsapp.net", Status: "pending", Cursor: historyCursor{ID: "anchor", Timestamp: time.Unix(100, 0)}}
		backfillPage(context.Background(), client, mailbox, chat)
		if chat.Status != tc.status || chat.Cursor.ID != "anchor" || chat.LastResult != tc.status {
			t.Fatalf("response incorrectly confirmed history: %+v", chat)
		}
	}
	mailbox := &backfillMailbox{responses: make(chan backfillReply, 1)}
	mailbox.handle(&events.HistorySync{Data: &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{ID: proto.String("100@s.whatsapp.net")}},
	}}, nil)
	if len(mailbox.responses) != 0 {
		t.Fatal("idle mailbox accepted a response")
	}
}

func TestPhoneProbePreservesStorageFailureClassification(t *testing.T) {
	store := backfillFixture(t)
	mailbox := &backfillMailbox{responses: make(chan backfillReply, 1)}
	client := &fakeHistoryClient{client: &whatsmeow.Client{}, mailbox: mailbox, storeError: errors.New("disk full")}
	report := &backfillReport{Status: "running", Chats: []*backfillChat{{JID: "100@s.whatsapp.net", Received: 1}}}
	if probePrimaryPhone(context.Background(), client, store, mailbox, report) || report.Status != "storage_failed" {
		t.Fatal("database failure was mistaken for phone unavailability")
	}
}

func TestBackfillResumeMarkerIsConsumedOnce(t *testing.T) {
	report := &backfillReport{Status: "phone_unresponsive", MessagesAdded: 112, Chats: []*backfillChat{
		{Status: "phone_reports_no_older_messages"},
		{Status: "response_timeout", Cursor: historyCursor{ID: "saved-cursor"}, Pages: 1},
	}}
	if resumeBackfill(report, "") {
		t.Fatal("blocked run resumed without an explicit marker")
	}
	if !resumeBackfill(report, "retry-1") || report.Status != "running" || report.Chats[1].Status != "pending" || report.Chats[1].Cursor.ID != "saved-cursor" || report.MessagesAdded != 112 {
		t.Fatal("resume failed to preserve progress")
	}
	if report.Chats[0].Status != "phone_reports_no_older_messages" {
		t.Fatal("completed chat was restarted")
	}
	report.Status = "phone_unresponsive"
	if resumeBackfill(report, "retry-1") {
		t.Fatal("consumed marker repeated after restart")
	}
	report.Status = "finished"
	report.Chats[1].Status = "phone_reports_no_older_messages"
	if resumeBackfill(report, "retry-2") {
		t.Fatal("fully confirmed sweep restarted")
	}
}

func TestBackfillResumeRetriesFailedRequestsFromSavedCursors(t *testing.T) {
	for _, runStatus := range []string{"running", "finished", "phone_unresponsive", "interrupted"} {
		t.Run(runStatus, func(t *testing.T) {
			cursor := historyCursor{ID: "saved-cursor", Sender: "100", Timestamp: time.Unix(100, 0), FromMe: true}
			report := &backfillReport{
				Status: runStatus, ResumeID: "previous-retry", MessagesAdded: 68874,
				Before: archiveSummary{Messages: 74612}, After: archiveSummary{Messages: 143489},
			}
			for _, status := range []string{
				"pending", "response_timeout", "request_failed", "interrupted",
				"unmatched_response_unconfirmed",
			} {
				report.Chats = append(report.Chats, &backfillChat{Status: status, LastResult: "progress", Cursor: cursor, Pages: 4, Received: 150})
			}
			if !resumeBackfill(report, "next-retry") || report.Status != "running" || report.ResumeID != "next-retry" {
				t.Fatal("explicit retry did not resume unconfirmed chats")
			}
			for _, chat := range report.Chats {
				if chat.Status != "pending" || chat.Cursor != cursor || chat.Pages != 4 || chat.Received != 150 {
					t.Fatalf("resume changed saved progress: %+v", chat)
				}
			}
			if report.MessagesAdded != 68874 || report.Before.Messages != 74612 || report.After.Messages != 143489 {
				t.Fatal("resume reset archive totals")
			}
			report.Status = "finished"
			if resumeBackfill(report, "next-retry") {
				t.Fatal("consumed retry marker restarted a finished sweep")
			}
		})
	}
}

func TestBackfillResumePreservesTerminalChatsAndFailedRuns(t *testing.T) {
	for _, status := range []string{
		"phone_reports_no_older_messages", "older_messages_not_accessible", "unsupported_chat",
		"no_anchor", "invalid_chat", "request_limit", "storage_failed", "no_backward_progress",
		"empty_response_unconfirmed", "deferred_previous_result_unknown",
	} {
		t.Run(status, func(t *testing.T) {
			chat := &backfillChat{Status: status, Cursor: historyCursor{ID: "preserved"}, Pages: 10000}
			report := &backfillReport{Status: "finished", ResumeID: "previous", Chats: []*backfillChat{
				chat, {Status: "response_timeout"},
			}}
			if !resumeBackfill(report, "next") || chat.Status != status || chat.Cursor.ID != "preserved" || chat.Pages != 10000 {
				t.Fatal("resume changed a terminal chat")
			}
		})
	}
	for _, status := range []string{"storage_failed", "verification_failed"} {
		report := &backfillReport{Status: status, ResumeID: "previous", Chats: []*backfillChat{{Status: "response_timeout"}}}
		if resumeBackfill(report, "next") || report.Status != status || report.ResumeID != "previous" || report.Chats[0].Status != "response_timeout" {
			t.Fatalf("resume bypassed run status %s", status)
		}
	}
}

func TestTargetedResumePrioritizesUnrequestedAndDefersUnknownLegacyResults(t *testing.T) {
	legacy := &backfillChat{JID: "legacy", Status: "pending", Pages: 3, Cursor: historyCursor{ID: "legacy-cursor"}}
	failed := &backfillChat{JID: "failed", Status: "response_timeout", Pages: 2, Cursor: historyCursor{ID: "failed-cursor"}}
	progress := &backfillChat{JID: "progress", Status: "pending", LastResult: "progress", Pages: 4, Cursor: historyCursor{ID: "progress-cursor"}}
	unrequested := &backfillChat{JID: "new", Status: "pending", Cursor: historyCursor{ID: "new-cursor"}}
	answered := &backfillChat{JID: "answered", Status: "pending", LastResult: "no_backward_progress", Pages: 2}
	report := &backfillReport{Status: "running", ResumeID: "old", Chats: []*backfillChat{legacy, failed, progress, unrequested, answered}}
	if !resumeBackfill(report, "targeted") || report.Chats[0] != unrequested || report.RetryPolicy != "targeted_v1" {
		t.Fatal("targeted retry did not prioritize a never-requested chat")
	}
	if legacy.Status != "deferred_previous_result_unknown" || legacy.Pages != 3 || legacy.Cursor.ID != "legacy-cursor" {
		t.Fatal("unknown legacy result was retried or its cursor lost")
	}
	if failed.Status != "pending" || failed.LastResult != "response_timeout" || failed.Cursor.ID != "failed-cursor" {
		t.Fatal("failed request lost its retry reason or saved position")
	}
	if progress.Status != "pending" || progress.Cursor.ID != "progress-cursor" || answered.Status != "no_backward_progress" {
		t.Fatal("targeted retry lost ongoing progress or repeated an answered request")
	}
}

func TestTargetedResumeWithNoEligibleChatsConsumesMarker(t *testing.T) {
	report := &backfillReport{Status: "finished", Chats: []*backfillChat{
		{Status: "no_backward_progress", Pages: 1}, {Status: "empty_response_unconfirmed", Pages: 1},
	}}
	if resumeBackfill(report, "targeted") || report.Status != "finished" || report.ResumeID != "targeted" {
		t.Fatal("a retry without eligible chats repeated requests or left its marker unconsumed")
	}
	if resumeBackfill(report, "targeted") {
		t.Fatal("consumed marker restarted a finished sweep")
	}
}

func TestBackfillRefreshFindsNewAnchorsAndPreservesExistingProgress(t *testing.T) {
	store := backfillFixture(t)
	report, _, err := prepareBackfill(store, "refresh-test")
	if err != nil {
		t.Fatal(err)
	}
	existing := report.Chats[0]
	existing.Status = "phone_reports_no_older_messages"
	existing.Pages = 2
	if err := store.StoreChat("200@s.whatsapp.net", "No stored message", time.Unix(400, 0)); err != nil {
		t.Fatal(err)
	}
	if err := refreshBackfillChats(store, report); err != nil {
		t.Fatal(err)
	}
	if len(report.Chats) != 2 || report.Chats[1].Status != "no_anchor" || report.Chats[1].Pages != 0 {
		t.Fatal("chat without a message was omitted or given a fabricated cursor")
	}
	if err := store.StoreMessage("first", "200@s.whatsapp.net", "200", "newly available anchor", time.Unix(400, 0), false,
		"", "", "", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := refreshBackfillChats(store, report); err != nil {
			t.Fatal(err)
		}
	}
	if len(report.Chats) != 2 || report.Chats[1].Status != "pending" || report.Chats[1].Cursor.ID != "first" || report.Chats[1].Pages != 0 {
		t.Fatal("newly anchored chat was missed or added twice")
	}
	if existing.Status != "phone_reports_no_older_messages" || existing.Cursor.ID != "newest" || existing.Pages != 2 || report.Before.Messages != 2 {
		t.Fatal("chat discovery changed existing progress or the original baseline")
	}
	report.Status = "finished"
	if !resumeBackfill(report, "targeted") || report.Chats[0].JID != "200@s.whatsapp.net" {
		t.Fatal("newly anchored chat was not requested first")
	}
}

func TestBackfillSameSecondCursorUsesMessageOrder(t *testing.T) {
	chat := &backfillChat{Status: "pending", Cursor: historyCursor{ID: "a", Timestamp: time.Unix(100, 0)}}
	applyBackfillResponse(chat, &waHistorySync.Conversation{Messages: []*waHistorySync.HistorySyncMsg{
		historyMessage("z", "", 100), historyMessage("b", "", 100), historyMessage("y", "", 100),
	}})
	if chat.Cursor.ID != "y" || chat.Status != "pending" {
		t.Fatalf("same-second history failed to progress: %+v", chat)
	}
}
