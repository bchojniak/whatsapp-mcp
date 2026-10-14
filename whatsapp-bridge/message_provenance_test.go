package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func provenanceFixture(id string) ingestionPayload {
	payload := incomingFixture(id)
	message := payload.Message
	message.MessageType = "text"
	message.Metadata = []byte(`{"message_type":"text"}`)
	message.SourceFormat = "waE2E.Message"
	message.SourcePayload = []byte{10, 3, 'o', 'l', 'd'}
	message.SourceKind = "message"
	message.Provenance = []byte(`{"observed_chat":"100@s.whatsapp.net"}`)
	message.ObservedAt = time.Unix(200, 0).UTC()
	return payload
}

func assertProvenanceCount(t *testing.T, store *MessageStore, want int) {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM message_metadata_versions`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("retained %d source versions, want %d", count, want)
	}
}

func TestSourceVersionsPreserveOriginalAndEveryDistinctObservation(t *testing.T) {
	store, _ := ingestionFixture(t)
	payload := provenanceFixture("same-id")
	payload.Message.MediaType = "document"
	payload.Message.Filename = "downloaded-original.pdf"
	original := append([]byte(nil), payload.Message.SourcePayload...)
	apply := func() {
		t.Helper()
		if _, _, err := store.applyIngestion(context.Background(), nil, payload); err != nil {
			t.Fatal(err)
		}
	}
	apply()
	var rowID int64
	if err := store.db.QueryRow(`SELECT rowid FROM messages`).Scan(&rowID); err != nil {
		t.Fatal(err)
	}
	// Even later observations of the same bytes are retry-idempotent.
	payload.Message.ObservedAt = time.Unix(300, 0).UTC()
	apply()
	assertProvenanceCount(t, store, 1)
	payload.Message.SourcePayload = []byte{10, 3, 'n', 'e', 'w'}
	payload.Message.Filename = "provider-renamed.pdf"
	apply()
	payload.Message.Metadata = []byte(`{"message_type":"text","details":{"revision":2}}`)
	apply()
	payload.Message.MessageType = "edited"
	apply()
	payload.Message.SourceFormat = "future.Message"
	apply()
	payload.Message.SourceKind = "history"
	apply()
	payload.Message.Provenance = []byte(`{"observed_chat":"100@s.whatsapp.net","trace":"another"}`)
	apply()
	apply()
	assertProvenanceCount(t, store, 7)
	var first []byte
	var firstObserved time.Time
	if err := store.db.QueryRow(`SELECT source_payload,observed_at FROM message_metadata_versions ORDER BY version_id LIMIT 1`).Scan(&first, &firstObserved); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, original) || !firstObserved.Equal(time.Unix(200, 0)) {
		t.Fatal("the original payload or first observation was replaced")
	}
	var current []byte
	var filename string
	var replayRowID int64
	if err := store.db.QueryRow(`SELECT m.rowid,m.filename,md.source_payload FROM messages m JOIN message_metadata md ON md.message_id=m.id AND md.chat_jid=m.chat_jid`).Scan(&replayRowID, &filename, &current); err != nil {
		t.Fatal(err)
	}
	if replayRowID != rowID || filename != "downloaded-original.pdf" || !bytes.Equal(current, payload.Message.SourcePayload) {
		t.Fatal("provenance changed row identity, downloaded filename, or current metadata")
	}
}

func TestSourceVersionMigrationBackfillsAndProtectsLegacyWriters(t *testing.T) {
	store, _ := ingestionFixture(t)
	if _, _, err := store.applyIngestion(context.Background(), nil, incomingFixture("legacy")); err != nil {
		t.Fatal(err)
	}
	// Create a metadata source without a retained version.
	if _, err := store.db.Exec(`
		DROP TRIGGER message_metadata_retain_insert;
		DROP TRIGGER message_metadata_retain_before_update;
		DROP TRIGGER message_metadata_retain_after_update;
		DROP TABLE message_metadata_versions;
		INSERT INTO message_metadata VALUES('legacy','100@s.whatsapp.net','text','{"old":true}','original',X'0001FF','2020-01-01T00:00:00Z');
	`); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := migrateMessageMetadataVersions(store.db); err != nil {
			t.Fatal(err)
		}
	}
	assertProvenanceCount(t, store, 1)
	var source, provenance string
	var raw []byte
	if err := store.db.QueryRow(`SELECT source_kind,provenance_json,source_payload FROM message_metadata_versions`).Scan(&source, &provenance, &raw); err != nil {
		t.Fatal(err)
	}
	if source != "legacy_metadata" || provenance != "{}" || !bytes.Equal(raw, []byte{0, 1, 255}) {
		t.Fatal("legacy migration changed source bytes or invented provenance")
	}
	// A writer can upsert the metadata table directly. Triggers retain
	// both payload-only and metadata-only changes, including empty payloads.
	for _, statement := range []string{
		`UPDATE message_metadata SET source_payload=X'0002FF'`,
		`UPDATE message_metadata SET metadata_json='{"changed":true}'`,
		`UPDATE message_metadata SET source_payload=X''`,
		`UPDATE message_metadata SET observed_at='2025-01-01T00:00:00Z'`,
	} {
		if _, err := store.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := migrateMessageMetadataVersions(store.db); err != nil {
		t.Fatal(err)
	}
	assertProvenanceCount(t, store, 4)
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("migration changed physical messages: count=%d error=%v", count, err)
	}
}

func TestSourceVersionFailureRollsBackWholeMessage(t *testing.T) {
	store, _ := ingestionFixture(t)
	if _, err := store.db.Exec(`CREATE TRIGGER fail_source_version BEFORE INSERT ON message_metadata_versions BEGIN SELECT RAISE(FAIL,'synthetic source retention failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.applyIngestion(context.Background(), nil, provenanceFixture("failure")); err == nil {
		t.Fatal("source retention failure was hidden")
	}
	for _, table := range []string{"messages", "message_metadata", "message_metadata_versions"} {
		var count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("partial commit in %s: count=%d error=%v", table, count, err)
		}
	}
}

func TestSourceVersionsRemainScopedToPhysicalChatAndOldQueuesReplay(t *testing.T) {
	store, _ := ingestionFixture(t)
	payload := provenanceFixture("same-id")
	payload.Message.SourceKind = ""
	payload.Message.Provenance = nil
	payload.Message.ObservedAt = time.Time{}
	for _, chat := range []string{"100@s.whatsapp.net", "999@lid", "200@s.whatsapp.net"} {
		payload.Message.ChatJID = chat
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeIngestion(encoded)
		if err != nil {
			t.Fatal(err)
		}
		for replay := 0; replay < 2; replay++ {
			if _, _, err := store.applyIngestion(context.Background(), nil, decoded); err != nil {
				t.Fatal(err)
			}
		}
	}
	assertProvenanceCount(t, store, 3)
}

func TestCapturedSourcePreservesUnknownFieldsAndEventIdentifiers(t *testing.T) {
	content := &waE2E.Message{Conversation: proto.String("synthetic")}
	content.ProtoReflect().SetUnknown([]byte{0xf8, 0x7f, 0x01})
	event := &events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat: types.NewJID("100", types.DefaultUserServer), Sender: types.NewJID("200", types.DefaultUserServer),
			SenderAlt: types.NewJID("999", "lid"), RecipientAlt: types.NewJID("888", "lid"),
		},
		ID: "source", Timestamp: time.Unix(100, 0),
	}, RawMessage: content}
	payload, err := captureMessage(event)
	if err != nil {
		t.Fatal(err)
	}
	var restored waE2E.Message
	if err := proto.Unmarshal(payload.Message.SourcePayload, &restored); err != nil || !proto.Equal(content, &restored) {
		t.Fatalf("source including unknown fields was not retained: %v", err)
	}
	wantProvenance, err := json.Marshal(event.Info)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload.Message.Provenance, wantProvenance) || payload.Message.SourceKind != "message" || payload.Message.ObservedAt.IsZero() {
		t.Fatal("live event identifiers or observation time were lost")
	}
	var identifiers map[string]interface{}
	if err := json.Unmarshal(payload.Message.Provenance, &identifiers); err != nil {
		t.Fatal(err)
	}
	if identifiers["SenderAlt"] != "999@lid" || identifiers["RecipientAlt"] != "888@lid" {
		t.Fatal("alternate provider identifiers were lost")
	}
}

func TestHistorySourceVersionsRetainReplayProvenanceAndMediaFilename(t *testing.T) {
	store, _ := ingestionFixture(t)
	entry := historyMessage("historical", "", 100)
	entry.Message.Message = &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileName: proto.String("original.pdf"), URL: proto.String("https://example.test/old")}}
	history := &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{ID: proto.String("100@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{entry}}}}
	apply := func() {
		t.Helper()
		if _, err := store.applyHistoryWithTrace(context.Background(), nil, history, "", "history-trace"); err != nil {
			t.Fatal(err)
		}
	}
	apply()
	if _, err := store.db.Exec(`UPDATE messages SET filename='downloaded.pdf'`); err != nil {
		t.Fatal(err)
	}
	entry.Message.Message.DocumentMessage.URL = proto.String("https://example.test/fresh")
	entry.Message.Message.DocumentMessage.FileName = proto.String("renamed.pdf")
	apply()
	apply()
	assertProvenanceCount(t, store, 2)
	var filename, source, provenance string
	if err := store.db.QueryRow(`SELECT filename FROM messages`).Scan(&filename); err != nil {
		t.Fatal(err)
	}
	if filename != "downloaded.pdf" || store.historyMessagesAdded.Load() != 1 {
		t.Fatal("history replay replaced filename or recounted an existing message")
	}
	if err := store.db.QueryRow(`SELECT source_kind,provenance_json FROM message_metadata_versions ORDER BY version_id LIMIT 1`).Scan(&source, &provenance); err != nil {
		t.Fatal(err)
	}
	var details map[string]interface{}
	if err := json.Unmarshal([]byte(provenance), &details); err != nil {
		t.Fatal(err)
	}
	if source != "on_demand_history" || details["history_trace_id"] != "history-trace" || details["conversation_jid"] != "100@s.whatsapp.net" {
		t.Fatal("historical source provenance was lost")
	}
}

func TestSourceVersionMigrationAndConcurrentIngestionAreIdempotent(t *testing.T) {
	store, _ := ingestionFixture(t)
	const workers = 4
	const versions = 6
	var group sync.WaitGroup
	errors := make(chan error, workers+1)
	start := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for version := 0; version < versions; version++ {
				payload := provenanceFixture("concurrent")
				payload.Message.SourcePayload = []byte(fmt.Sprintf("source-%d", version))
				if _, _, err := store.applyIngestion(context.Background(), nil, payload); err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	group.Add(1)
	go func() {
		defer group.Done()
		<-start
		for attempt := 0; attempt < versions; attempt++ {
			if err := migrateMessageMetadataVersions(store.db); err != nil {
				errors <- err
				return
			}
		}
	}()
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	assertProvenanceCount(t, store, versions)
	assertRowVersionCount(t, store, 1)
}

func TestHistoryProvenanceRetainsOriginalMessageKeyIdentifiers(t *testing.T) {
	store, _ := ingestionFixture(t)
	entries := []*waHistorySync.HistorySyncMsg{
		historyMessage("first-key", "first message", 100),
		historyMessage("second-key", "second message", 99),
	}
	remoteJIDs := []string{"999@lid", "888@lid"}
	participants := []string{"200@s.whatsapp.net", "300@s.whatsapp.net"}
	wantPayloads := make([][]byte, len(entries))
	for index, entry := range entries {
		entry.Message.Key.RemoteJID = proto.String(remoteJIDs[index])
		entry.Message.Key.Participant = proto.String(participants[index])
		entry.Message.Key.ProtoReflect().SetUnknown([]byte{0xf8, 0x7f, 0x01})
		var err error
		wantPayloads[index], err = (proto.MarshalOptions{AllowPartial: true, Deterministic: true}).Marshal(entry.Message)
		if err != nil {
			t.Fatal(err)
		}
	}
	history := &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(),
		Conversations: []*waHistorySync.Conversation{{
			ID: proto.String("100@s.whatsapp.net"), Messages: entries,
		}},
	}
	for replay := 0; replay < 2; replay++ {
		if _, err := store.applyHistoryWithTrace(context.Background(), nil, history, "", "identifier-trace"); err != nil {
			t.Fatal(err)
		}
	}
	assertProvenanceCount(t, store, 2)
	for index, entry := range entries {
		var provenance string
		var raw []byte
		if err := store.db.QueryRow(`SELECT provenance_json,source_payload FROM message_metadata_versions WHERE message_id=?`, entry.Message.Key.GetID()).Scan(&provenance, &raw); err != nil {
			t.Fatal(err)
		}
		var identifiers map[string]interface{}
		if err := json.Unmarshal([]byte(provenance), &identifiers); err != nil {
			t.Fatal(err)
		}
		if identifiers["message_id"] != entry.Message.Key.GetID() || identifiers["remote_jid"] != remoteJIDs[index] || identifiers["participant_jid"] != participants[index] || identifiers["conversation_jid"] != "100@s.whatsapp.net" || identifiers["history_trace_id"] != "identifier-trace" {
			t.Fatalf("original history key identifiers were lost or mixed between entries: %s", provenance)
		}
		if !bytes.Equal(raw, wantPayloads[index]) {
			t.Fatal("extracting key identifiers changed the captured payload")
		}
	}
}
