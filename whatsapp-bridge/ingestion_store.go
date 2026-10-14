package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type incomingMessage struct {
	ID, ChatJID, Sender, Content string
	Timestamp                    time.Time
	FromMe                       bool
	MediaType, Filename, URL     string
	MediaKey, SHA256, EncSHA256  []byte
	FileLength                   uint64
	MessageType, SourceFormat    string
	Metadata, SourcePayload      []byte
	SourceKind                   string    `json:",omitempty"`
	Provenance                   []byte    `json:",omitempty"`
	ObservedAt                   time.Time `json:",omitempty"`
}

// History keeps the provider's protobuf, including unsupported entries and end
// flags, so replay cannot change the backfill's evidence or cursor semantics.
type ingestionPayload struct {
	Kind           string
	Message        *incomingMessage `json:",omitempty"`
	History        []byte           `json:",omitempty"`
	OwnUser        string           `json:",omitempty"`
	HistoryTraceID string           `json:",omitempty"`
	ObservedAt     time.Time        `json:",omitempty"`
}

func captureMessage(event *events.Message) (ingestionPayload, error) {
	content := event.RawMessage
	if content == nil {
		content = event.Message
	}
	kind, filename, url, key, sha, encSHA, length := extractMediaInfo(content)
	message := &incomingMessage{
		ID: event.Info.ID, ChatJID: event.Info.Chat.String(), Sender: event.Info.Sender.User,
		Content: extractTextContent(content), Timestamp: event.Info.Timestamp, FromMe: event.Info.IsFromMe,
		MediaType: kind, Filename: filename, URL: url, MediaKey: key, SHA256: sha, EncSHA256: encSHA, FileLength: length,
		SourceKind: "message", ObservedAt: time.Now().UTC(),
	}
	provenance, err := json.Marshal(event.Info)
	if err != nil {
		return ingestionPayload{}, err
	}
	message.Provenance = provenance
	var source proto.Message = content
	format := "waE2E.Message"
	if event.SourceWebMsg != nil {
		source, format = event.SourceWebMsg, "waWeb.WebMessageInfo"
	}
	err = captureArchivePayload(message, source, format, content)
	return ingestionPayload{Kind: "message", Message: message}, err
}

func captureHistory(client *whatsmeow.Client, event *events.HistorySync) (ingestionPayload, error) {
	if event.Data == nil {
		return ingestionPayload{}, errors.New("missing history data")
	}
	data, err := proto.MarshalOptions{AllowPartial: true, Deterministic: true}.Marshal(event.Data)
	payload := ingestionPayload{Kind: "history", History: data, ObservedAt: time.Now().UTC()}
	if client != nil && client.Store != nil && client.Store.ID != nil {
		payload.OwnUser = client.Store.ID.User
	}
	return payload, err
}

func (payload ingestionPayload) entryCount() int {
	if payload.Message != nil {
		if payload.Message.Content != "" || payload.Message.MediaType != "" {
			return 1
		}
		return 0
	}
	var history waHistorySync.HistorySync
	if (proto.UnmarshalOptions{AllowPartial: true}).Unmarshal(payload.History, &history) != nil {
		return 0
	}
	count := 0
	for _, conversation := range history.GetConversations() {
		count += len(conversation.GetMessages())
	}
	return count
}

func (store *MessageStore) applyIngestion(ctx context.Context, client *whatsmeow.Client, payload ingestionPayload) (*events.HistorySync, int, error) {
	switch payload.Kind {
	case "message":
		if payload.Message == nil {
			return nil, 0, errors.New("invalid queued message")
		}
		// For queue entries without provenance fields, retain the payload
		// and record only the source kind established by the queue.
		incoming := *payload.Message
		message := &incoming
		if message.SourceKind == "" {
			message.SourceKind = "message"
		}
		jid, err := types.ParseJID(message.ChatJID)
		if err != nil {
			return nil, 0, err
		}
		name := getChatName(ctx, client, store, jid, message.ChatJID, nil, message.Sender, waLog.Noop)
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, 0, err
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, storeChatSQL, message.ChatJID, name, message.Timestamp); err != nil {
			return nil, 0, err
		}
		count := 0
		if message.Content != "" || message.MediaType != "" {
			if err = insertIncomingMessage(ctx, tx, storeMessageSQL, message); err != nil {
				return nil, 0, err
			}
			count = 1
		}
		return nil, count, tx.Commit()
	case "history":
		history := &waHistorySync.HistorySync{}
		if err := (proto.UnmarshalOptions{AllowPartial: true}).Unmarshal(payload.History, history); err != nil {
			return nil, 0, err
		}
		count, err := store.applyHistoryWithProvenance(ctx, client, history, payload.OwnUser, payload.HistoryTraceID, payload.ObservedAt)
		return &events.HistorySync{Data: history}, count, err
	default:
		return nil, 0, errors.New("unknown queued event type")
	}
}

func insertIncomingMessage(ctx context.Context, tx *sql.Tx, statement string, message *incomingMessage) error {
	_, err := tx.ExecContext(ctx, statement, message.ID, message.ChatJID, message.Sender, message.Content,
		message.Timestamp, message.FromMe, message.MediaType, message.Filename, message.URL,
		message.MediaKey, message.SHA256, message.EncSHA256, message.FileLength)
	if err != nil {
		return err
	}
	return storeIncomingMetadata(ctx, tx, message)
}

func (store *MessageStore) applyHistory(ctx context.Context, client *whatsmeow.Client, history *waHistorySync.HistorySync, ownUser string) (int, error) {
	return store.applyHistoryWithTrace(ctx, client, history, ownUser, "")
}

func (store *MessageStore) applyHistoryWithTrace(ctx context.Context, client *whatsmeow.Client, history *waHistorySync.HistorySync, ownUser, traceID string) (int, error) {
	return store.applyHistoryWithProvenance(ctx, client, history, ownUser, traceID, time.Time{})
}

func (store *MessageStore) applyHistoryWithProvenance(ctx context.Context, client *whatsmeow.Client, history *waHistorySync.HistorySync, ownUser, traceID string, observedAt time.Time) (int, error) {
	onDemand := history.GetSyncType() == waHistorySync.HistorySync_ON_DEMAND
	sourceKind := "history"
	if onDemand {
		sourceKind = "on_demand_history"
	}
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	// Resolve names before opening a write transaction. Optional contact/group
	// enrichment must not hold the archive's write lock while using the network.
	names := make(map[string]string)
	namingCtx, cancelNames := context.WithTimeout(ctx, 20*time.Second)
	defer cancelNames()
	if !onDemand {
		for _, conversation := range history.GetConversations() {
			jid, err := types.ParseJID(conversation.GetID())
			if err == nil && conversation.ID != nil {
				names[conversation.GetID()] = getChatName(namingCtx, client, store, jid, conversation.GetID(), conversation, "", waLog.Noop)
			}
		}
	}
	ctx, cancelWrite := context.WithTimeout(ctx, 15*time.Second)
	defer cancelWrite()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stored, added := 0, int64(0)
	for _, conversation := range history.GetConversations() {
		jid, err := types.ParseJID(conversation.GetID())
		if err != nil || jid.Server == "" {
			if onDemand {
				return 0, errors.New("invalid chat identifier in on-demand history")
			}
			continue
		}
		if !onDemand {
			messages := conversation.GetMessages()
			if conversation.ID == nil || len(messages) == 0 || messages[0].GetMessage().GetMessageTimestamp() == 0 {
				continue
			}
			if _, err := tx.ExecContext(ctx, storeChatSQL, conversation.GetID(), names[conversation.GetID()], time.Unix(int64(messages[0].GetMessage().GetMessageTimestamp()), 0)); err != nil {
				return 0, err
			}
		}
		for _, entry := range conversation.GetMessages() {
			message := entry.GetMessage()
			key := message.GetKey()
			if message.GetMessageTimestamp() == 0 || (onDemand && key.GetID() == "") {
				continue
			}
			provenance, err := json.Marshal(struct {
				SyncType        string `json:"sync_type"`
				ConversationJID string `json:"conversation_jid"`
				HistoryTraceID  string `json:"history_trace_id,omitempty"`
				MessageID       string `json:"message_id"`
				RemoteJID       string `json:"remote_jid,omitempty"`
				ParticipantJID  string `json:"participant_jid,omitempty"`
			}{
				SyncType: history.GetSyncType().String(), ConversationJID: conversation.GetID(),
				HistoryTraceID: traceID, MessageID: key.GetID(),
				RemoteJID: key.GetRemoteJID(), ParticipantJID: key.GetParticipant(),
			})
			if err != nil {
				return 0, err
			}
			mediaType, filename, url, mediaKey, sha, encSHA, length := extractMediaInfo(message.GetMessage())
			content := extractTextContent(message.GetMessage())
			sender := key.GetParticipant()
			if sender == "" {
				sender = jid.User
			}
			if key.GetFromMe() {
				if ownUser == "" {
					return 0, errors.New("missing own device identity while storing history")
				}
				sender = ownUser
			}
			timestamp := time.Unix(int64(message.GetMessageTimestamp()), 0)
			incoming := &incomingMessage{ID: key.GetID(), ChatJID: conversation.GetID(), Sender: sender,
				Content: content, Timestamp: timestamp, FromMe: key.GetFromMe(), MediaType: mediaType,
				Filename: filename, URL: url, MediaKey: mediaKey, SHA256: sha, EncSHA256: encSHA, FileLength: length,
				SourceKind: sourceKind, Provenance: provenance, ObservedAt: observedAt}
			if err := captureArchivePayload(incoming, message, "waWeb.WebMessageInfo", message.GetMessage()); err != nil {
				return 0, err
			}
			if incoming.Content == "" && incoming.MediaType == "" {
				continue
			}
			statement := storeMessageSQL
			if onDemand {
				name := conversation.GetDisplayName()
				if name == "" {
					name = conversation.GetName()
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO chats(jid,name,last_message_time) VALUES(?,?,?) ON CONFLICT(jid) DO NOTHING`, conversation.GetID(), name, timestamp); err != nil {
					return 0, err
				}
				statement = `INSERT INTO messages(id,chat_jid,sender,content,timestamp,is_from_me,media_type,filename,url,media_key,file_sha256,file_enc_sha256,file_length) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id,chat_jid) DO NOTHING`
			}
			result, err := tx.ExecContext(ctx, statement, key.GetID(), conversation.GetID(), sender, incoming.Content, timestamp, key.GetFromMe(), mediaType, filename, url, mediaKey, sha, encSHA, length)
			if err != nil {
				return 0, err
			}
			if err := storeIncomingMetadata(ctx, tx, incoming); err != nil {
				return 0, err
			}
			if onDemand && mediaType != "" {
				// Refresh expired retrieval references without replacing the existing
				// message, timestamp, or downloaded filename, and without counting an insertion.
				if _, err := tx.ExecContext(ctx, `UPDATE messages SET
					media_type=CASE WHEN COALESCE(media_type,'')='' THEN ? ELSE media_type END,
					filename=CASE WHEN COALESCE(filename,'')='' THEN ? ELSE filename END,
					url=COALESCE(NULLIF(?,''),url), media_key=COALESCE(?,media_key),
					file_sha256=COALESCE(?,file_sha256), file_enc_sha256=COALESCE(?,file_enc_sha256),
					file_length=CASE WHEN ?>0 THEN ? ELSE file_length END WHERE id=? AND chat_jid=?`,
					mediaType, filename, url, mediaKey, sha, encSHA, length, length, key.GetID(), conversation.GetID()); err != nil {
					return 0, err
				}
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return 0, err
			}
			stored++
			if onDemand {
				added += rows
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	if onDemand {
		store.historyMessagesAdded.Add(added)
		if store.historyDiagnostics != nil && traceID != "" {
			store.historyDiagnostics.recordStored(traceID, stored, int(added))
		}
	}
	return stored, nil
}

func decodeIngestion(data []byte) (ingestionPayload, error) {
	var payload ingestionPayload
	err := json.Unmarshal(data, &payload)
	return payload, err
}
