package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

func historyMessage(id, text string, timestamp uint64) *waProto.HistorySyncMsg {
	return &waProto.HistorySyncMsg{Message: &waProto.WebMessageInfo{
		Key:              &waProto.MessageKey{ID: proto.String(id)},
		Message:          &waProto.Message{Conversation: proto.String(text)},
		MessageTimestamp: proto.Uint64(timestamp),
	}}
}

func TestHistorySyncPreservesSendersMediaAndSkippedMessages(t *testing.T) {
	messageStore, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer messageStore.Close()
	chatJID := "100@g.us"
	if err := messageStore.StoreChat(chatJID, "Cached group name", time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	ownJID := types.NewJID("self", types.DefaultUserServer)
	client := &whatsmeow.Client{Store: &store.Device{ID: &ownJID}}
	participant := historyMessage("participant", "from participant", 100)
	participant.Message.Key.Participant = proto.String("other@s.whatsapp.net")
	own := historyMessage("own", "from me", 99)
	own.Message.Key.FromMe = proto.Bool(true)
	noKey := historyMessage("unused", "without key", 98)
	noKey.Message.Key = nil
	media := historyMessage("media", "", 97)
	media.Message.Message = &waProto.Message{ImageMessage: &waProto.ImageMessage{
		URL: proto.String("https://example.test/image"),
	}}
	conversation := &waProto.Conversation{
		ID: proto.String(chatJID),
		Messages: []*waProto.HistorySyncMsg{
			participant, own, noKey, media, nil, {},
			historyMessage("empty", "", 96),
			historyMessage("no-time", "skip", 0),
		},
	}
	handleHistorySync(client, messageStore, &events.HistorySync{
		Data: &waProto.HistorySync{Conversations: []*waProto.Conversation{conversation}},
	}, waLog.Noop)

	rows, err := messageStore.db.Query("SELECT id, sender, is_from_me, media_type FROM messages")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type storedMessage struct {
		sender    string
		fromMe    bool
		mediaType string
	}
	want := map[string]storedMessage{
		"participant": {"other@s.whatsapp.net", false, ""},
		"own":         {"self", true, ""},
		"":            {"100", false, ""},
		"media":       {"100", false, "image"},
	}
	for rows.Next() {
		var id string
		var got storedMessage
		if err := rows.Scan(&id, &got.sender, &got.fromMe, &got.mediaType); err != nil {
			t.Fatal(err)
		}
		if expected, ok := want[id]; !ok || got != expected {
			t.Fatalf("unexpected message %q: %+v", id, got)
		}
		delete(want, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(want) != 0 {
		t.Fatalf("missing messages: %+v", want)
	}
}

func TestHistorySyncSkipsConversationWithInvalidLatestMessage(t *testing.T) {
	for _, messages := range [][]*waProto.HistorySyncMsg{
		nil,
		{nil, historyMessage("valid", "must skip", 100)},
		{{}, historyMessage("valid", "must skip", 100)},
		{historyMessage("zero", "must skip", 0), historyMessage("valid", "must skip", 100)},
	} {
		messageStore, err := NewMessageStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { messageStore.Close() })
		chatJID := "100@g.us"
		if err := messageStore.StoreChat(chatJID, "Cached group name", time.Unix(1, 0)); err != nil {
			t.Fatal(err)
		}
		handleHistorySync(nil, messageStore, &events.HistorySync{
			Data: &waProto.HistorySync{Conversations: []*waProto.Conversation{
				{ID: proto.String(chatJID), Messages: messages},
			}},
		}, waLog.Noop)
		var count int
		if err := messageStore.db.QueryRow("SELECT COUNT(*) FROM messages").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("stored %d messages for an invalid latest message", count)
		}
	}
}
