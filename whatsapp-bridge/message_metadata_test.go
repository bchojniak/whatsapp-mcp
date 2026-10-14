package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestArchiveCapturesEverySupportedMediaVariant(t *testing.T) {
	fixtures := []struct {
		name, kind, subtype string
		message             *waE2E.Message
	}{
		{"image", "image", "", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/png"), Width: proto.Uint32(640)}}},
		{"video", "video", "", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{Seconds: proto.Uint32(12)}}},
		{"gif", "video", "gif", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{GifPlayback: proto.Bool(true)}}},
		{"video note", "video", "video_note", &waE2E.Message{PtvMessage: &waE2E.VideoMessage{Seconds: proto.Uint32(12)}}},
		{"audio", "audio", "", &waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(false)}}},
		{"voice note", "audio", "voice_note", &waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(true)}}},
		{"document", "document", "", &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileName: proto.String("report.pdf")}}},
		{"sticker", "sticker", "", &waE2E.Message{StickerMessage: &waE2E.StickerMessage{IsAnimated: proto.Bool(true), DirectPath: proto.String("/media/sticker")}}},
		{"sticker pack", "sticker_pack", "", &waE2E.Message{StickerPackMessage: &waE2E.StickerPackMessage{}}},
		{"wrapped document", "document", "", &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{Caption: proto.String("caption")}}}}},
		{"ephemeral image", "image", "", &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}}}},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			store, journal := ingestionFixture(t)
			event := &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: types.NewJID("100", types.DefaultUserServer), Sender: types.NewJID("200", types.DefaultUserServer)}, ID: "synthetic", Timestamp: time.Unix(100, 0)}, RawMessage: fixture.message}
			payload, err := captureMessage(event)
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.enqueue(payload); err != nil {
				t.Fatal(err)
			}
			if !journal.processOne(context.Background()) {
				t.Fatal("event did not commit")
			}
			var kind, metadataJSON string
			var raw []byte
			if err := store.db.QueryRow(`SELECT messages.media_type, metadata_json, source_payload FROM messages JOIN message_metadata ON message_id=messages.id AND message_metadata.chat_jid=messages.chat_jid`).Scan(&kind, &metadataJSON, &raw); err != nil {
				t.Fatal(err)
			}
			if kind != fixture.kind {
				t.Fatalf("media type %s", kind)
			}
			var metadata messageMetadata
			if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
				t.Fatal(err)
			}
			if fixture.subtype != "" && metadata.Media["subtype"] != fixture.subtype {
				t.Fatalf("subtype %v", metadata.Media)
			}
			restored := &waE2E.Message{}
			if err := proto.Unmarshal(raw, restored); err != nil || !proto.Equal(restored, fixture.message) {
				t.Fatal("original payload changed")
			}
		})
	}
}

func TestSourceMetadataCommitsAtomicallyAndReplaysWithoutDuplicates(t *testing.T) {
	store, _ := ingestionFixture(t)
	entry := historyMessage("source", "", 100)
	entry.Message.Message = &waE2E.Message{StickerMessage: &waE2E.StickerMessage{FileLength: proto.Uint64(25), DirectPath: proto.String("/fresh")}}
	history := &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{ID: proto.String("100@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{entry}}}}
	if _, err := store.db.Exec(`CREATE TRIGGER fail_metadata BEFORE INSERT ON message_metadata BEGIN SELECT RAISE(FAIL,'synthetic metadata failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.applyHistory(context.Background(), nil, history, ""); err == nil {
		t.Fatal("metadata failure hidden")
	}
	var count int
	store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count)
	if count != 0 {
		t.Fatal("message committed without source metadata")
	}
	assertProvenanceCount(t, store, 0)
	store.db.Exec(`DROP TRIGGER fail_metadata`)
	for replay := 0; replay < 2; replay++ {
		if _, err := store.applyHistory(context.Background(), nil, history, ""); err != nil {
			t.Fatal(err)
		}
	}
	store.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count)
	if count != 1 || store.historyMessagesAdded.Load() != 1 {
		t.Fatal("replay counted as a new message")
	}
	assertProvenanceCount(t, store, 1)
	var raw []byte
	store.db.QueryRow(`SELECT source_payload FROM message_metadata`).Scan(&raw)
	if len(raw) == 0 {
		t.Fatal("history payload missing")
	}
}

func TestNonTextMessagesAndReplyReferencesAreRetained(t *testing.T) {
	store, _ := ingestionFixture(t)
	entries := []*waHistorySync.HistorySyncMsg{historyMessage("location", "", 100), historyMessage("poll", "", 99), historyMessage("reaction", "", 98), historyMessage("reply", "", 97)}
	entries[0].Message.Message = &waE2E.Message{LocationMessage: &waE2E.LocationMessage{DegreesLatitude: proto.Float64(1), DegreesLongitude: proto.Float64(2)}}
	entries[1].Message.Message = &waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{Name: proto.String("Where?")}}
	entries[2].Message.Message = &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("👍"), Key: entries[1].Message.Key}}
	entries[3].Message.Message = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("Here"), ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("location")}}}
	history := &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{ID: proto.String("100@s.whatsapp.net"), Messages: entries}}}
	if count, err := store.applyHistory(context.Background(), nil, history, ""); err != nil || count != 4 {
		t.Fatalf("stored %d error %v", count, err)
	}
	var raw string
	if err := store.db.QueryRow(`SELECT metadata_json FROM message_metadata WHERE message_id='reply'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var metadata messageMetadata
	json.Unmarshal([]byte(raw), &metadata)
	if metadata.Context["reply_to_message_id"] != "location" {
		t.Fatal("reply reference lost")
	}
}

func TestArchiveExcludesTransportMessagesButRetainsVisibleProtocolChanges(t *testing.T) {
	for _, kind := range []waE2E.ProtocolMessage_Type{waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION, waE2E.ProtocolMessage_PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE, waE2E.ProtocolMessage_REVOKE, waE2E.ProtocolMessage_MESSAGE_EDIT, waE2E.ProtocolMessage_EPHEMERAL_SETTING} {
		event := &events.Message{Info: types.MessageInfo{ID: "synthetic"}, Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: kind.Enum(), Key: &waCommon.MessageKey{ID: proto.String("original")}}}}
		payload, err := captureMessage(event)
		if err != nil {
			t.Fatal(err)
		}
		visible := kind == waE2E.ProtocolMessage_REVOKE || kind == waE2E.ProtocolMessage_MESSAGE_EDIT || kind == waE2E.ProtocolMessage_EPHEMERAL_SETTING
		if (payload.entryCount() == 1) != visible {
			t.Fatalf("incorrect visibility for %v", kind)
		}
	}
}
