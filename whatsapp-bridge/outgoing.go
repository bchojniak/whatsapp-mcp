package main

import (
	"context"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func sendTextAndArchive(client *whatsmeow.Client, store *MessageStore, recipient, text string) SendMessageResponse {
	if client == nil || !client.IsConnected() || client.Store == nil || client.Store.ID == nil {
		return SendMessageResponse{Message: "WhatsApp is not connected"}
	}
	jid := types.NewJID(recipient, types.DefaultUserServer)
	if strings.Contains(recipient, "@") {
		var err error
		jid, err = types.ParseJID(recipient)
		if err != nil {
			return SendMessageResponse{Message: "Invalid recipient"}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return dispatchTextAndArchive(ctx, store, client.Store.ID.ToNonAD(), jid.ToNonAD(), text,
		func(ctx context.Context, to types.JID, message *waE2E.Message) (whatsmeow.SendResponse, error) {
			return client.SendMessage(ctx, to, message)
		})
}

func dispatchTextAndArchive(ctx context.Context, store *MessageStore, own, recipient types.JID, text string,
	send func(context.Context, types.JID, *waE2E.Message) (whatsmeow.SendResponse, error)) SendMessageResponse {
	if store == nil || own.IsEmpty() || recipient.IsEmpty() || strings.TrimSpace(text) == "" {
		return SendMessageResponse{Message: "Account, archive, recipient and text are required"}
	}
	message := &waE2E.Message{Conversation: proto.String(text)}
	ack, err := send(ctx, recipient, message)
	if err != nil || ack.ID == "" {
		return SendMessageResponse{Message: "Send outcome is uncertain"}
	}
	response := SendMessageResponse{Success: true, Message: "WhatsApp accepted the send",
		MessageID: ack.ID, ChatJID: recipient.String(), ArchiveStatus: "failed"}
	timestamp := ack.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	payload, err := captureMessage(&events.Message{
		Info: types.MessageInfo{MessageSource: types.MessageSource{
			Chat: recipient, Sender: own, IsFromMe: true, IsGroup: recipient.Server == types.GroupServer,
		}, ID: ack.ID, Timestamp: timestamp}, Message: message,
	})
	if err != nil {
		return response
	}
	// This is a local outgoing observation, not a received provider event or receipt.
	payload.Message.SourceKind = "outgoing"
	archiveCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err = store.applyIngestion(archiveCtx, nil, payload); err == nil {
		response.ArchiveStatus = "saved"
	} else if store.ingestion != nil && store.ingestion.enqueue(payload) == nil {
		// Recovery replays only the archive write; it never dispatches another send.
		response.ArchiveStatus = "pending"
	}
	return response
}
