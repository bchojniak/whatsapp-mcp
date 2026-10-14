package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type messageMetadata struct {
	MessageType string                 `json:"message_type"`
	Media       map[string]interface{} `json:"media,omitempty"`
	Context     map[string]interface{} `json:"context,omitempty"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

// Preserve wrappers and unknown fields in SourcePayload; unwrap only for indexing.
func unwrapArchiveMessage(message *waE2E.Message) *waE2E.Message {
	for depth := 0; message != nil && depth < 16; depth++ {
		if next := message.GetDeviceSentMessage().GetMessage(); next != nil {
			message = next
			continue
		}
		var next *waE2E.Message
		reflection := message.ProtoReflect()
		fields := reflection.Descriptor().Fields()
		for index := 0; index < fields.Len(); index++ {
			field := fields.Get(index)
			if field.Kind() != protoreflect.MessageKind || field.IsList() || !reflection.Has(field) {
				continue
			}
			if wrapper, ok := reflection.Get(field).Message().Interface().(*waE2E.FutureProofMessage); ok {
				next = wrapper.GetMessage()
				break
			}
		}
		if next == nil {
			return message
		}
		message = next
	}
	return message
}

func archiveMessageBody(message *waE2E.Message) (string, proto.Message) {
	message = unwrapArchiveMessage(message)
	if message == nil {
		return "system", nil
	}
	reflection := message.ProtoReflect()
	fields := reflection.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if !reflection.Has(field) || field.Name() == "messageContextInfo" || field.Name() == "senderKeyDistributionMessage" {
			continue
		}
		if field.Name() == "conversation" {
			return "text", message
		}
		if field.Kind() == protoreflect.MessageKind && !field.IsList() {
			kind := string(field.Name())
			if kind == "extendedTextMessage" {
				kind = "text"
			}
			return kind, reflection.Get(field).Message().Interface()
		}
	}
	return "unknown", message
}

// Business templates can nest ordinary attachments. Their full payload retains
// all attachments; the one-file download tool selects the first one.
func findArchiveMedia(message proto.Message, depth int) (string, proto.Message) {
	if message == nil || depth > 16 {
		return "", nil
	}
	if _, ok := message.(whatsmeow.DownloadableMessage); ok {
		switch string(message.ProtoReflect().Descriptor().Name()) {
		case "ImageMessage":
			return "image", message
		case "VideoMessage":
			return "video", message
		case "AudioMessage":
			return "audio", message
		case "DocumentMessage":
			return "document", message
		case "StickerMessage":
			return "sticker", message
		case "StickerPackMessage":
			return "sticker_pack", message
		}
	}
	reflection := message.ProtoReflect()
	fields := reflection.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		// A quoted message is context, not a new attachment in this message.
		if field.Kind() != protoreflect.MessageKind || field.Name() == "contextInfo" || field.Name() == "quotedMessage" || !reflection.Has(field) {
			continue
		}
		if field.IsList() {
			list := reflection.Get(field).List()
			for item := 0; item < list.Len(); item++ {
				if kind, media := findArchiveMedia(list.Get(item).Message().Interface(), depth+1); media != nil {
					return kind, media
				}
			}
		} else if kind, media := findArchiveMedia(reflection.Get(field).Message().Interface(), depth+1); media != nil {
			return kind, media
		}
	}
	return "", nil
}

func copyMetadataFields(message proto.Message, names map[string]string) map[string]interface{} {
	result := make(map[string]interface{})
	if message == nil {
		return result
	}
	reflection := message.ProtoReflect()
	for original, name := range names {
		field := reflection.Descriptor().Fields().ByName(protoreflect.Name(original))
		if field == nil || !reflection.Has(field) || field.IsList() || field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.BytesKind {
			continue
		}
		result[name] = reflection.Get(field).Interface()
	}
	return result
}

func describeArchiveMessage(message *waE2E.Message) messageMetadata {
	kind, body := archiveMessageBody(message)
	result := messageMetadata{MessageType: kind}
	mediaType, media := findArchiveMedia(unwrapArchiveMessage(message), 0)
	if media != nil {
		result.Media = copyMetadataFields(media, map[string]string{
			"mimetype": "mime_type", "fileLength": "byte_length", "seconds": "duration_seconds",
			"width": "width", "height": "height", "PTT": "voice_note", "isAnimated": "animated",
			"gifPlayback": "gif", "viewOnce": "view_once", "isLottie": "lottie",
			"directPath": "direct_path", "fileName": "original_filename", "title": "title",
			"pageCount": "page_count", "mediaKeyTimestamp": "media_key_timestamp",
		})
		result.Media["type"] = mediaType
		switch {
		case kind == "ptvMessage":
			result.Media["subtype"] = "video_note"
		case result.Media["voice_note"] == true:
			result.Media["subtype"] = "voice_note"
		case result.Media["gif"] == true:
			result.Media["subtype"] = "gif"
		case result.Media["lottie"] == true:
			result.Media["subtype"] = "lottie_sticker"
		}
	}
	if body != nil {
		result.Details = copyMetadataFields(body, map[string]string{
			"degreesLatitude": "latitude", "degreesLongitude": "longitude", "accuracyInMeters": "accuracy_meters",
			"name": "name", "address": "address", "displayName": "display_name", "vcard": "vcard",
			"selectableOptionsCount": "selectable_options_count",
		})
		reflection := body.ProtoReflect()
		field := reflection.Descriptor().Fields().ByName("contextInfo")
		if field != nil && reflection.Has(field) {
			result.Context = copyMetadataFields(reflection.Get(field).Message().Interface(), map[string]string{
				"stanzaID": "reply_to_message_id", "participant": "reply_sender_id",
				"remoteJID": "reply_chat_jid", "isForwarded": "forwarded", "forwardingScore": "forwarding_score",
				"expiration": "expiration_seconds",
			})
		}
	}
	if poll := unwrapArchiveMessage(message).GetPollCreationMessage(); poll != nil {
		var options []string
		for _, option := range poll.GetOptions() {
			options = append(options, option.GetOptionName())
		}
		result.Details["options"] = options
	}
	if reaction := unwrapArchiveMessage(message).GetReactionMessage(); reaction != nil {
		if result.Context == nil {
			result.Context = make(map[string]interface{})
		}
		result.Context["reaction_to_message_id"] = reaction.GetKey().GetID()
	}
	return result
}

func captureArchivePayload(message *incomingMessage, source proto.Message, format string, content *waE2E.Message) error {
	payload, err := (proto.MarshalOptions{AllowPartial: true, Deterministic: true}).Marshal(source)
	if err != nil {
		return err
	}
	metadata := describeArchiveMessage(content)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	message.SourcePayload, message.SourceFormat = payload, format
	message.Metadata, message.MessageType = encoded, metadata.MessageType
	if archiveTransportMessage(content) {
		// Peer history responses and key exchange are transport events, not
		// phone-visible chat messages or valid anchors for the user's history.
		message.Content, message.MediaType = "", ""
		return nil
	}
	if message.Content == "" {
		_, body := archiveMessageBody(content)
		message.Content = readableStructuredContent(body, 0)
		if message.Content == "" && message.MediaType == "" && metadata.MessageType != "text" {
			message.Content = "[" + metadata.MessageType + "]"
		}
	}
	return nil
}

func archiveTransportMessage(message *waE2E.Message) bool {
	message = unwrapArchiveMessage(message)
	if message == nil {
		return false // History stubs retain their WebMessageInfo source.
	}
	if protocol := message.GetProtocolMessage(); protocol != nil {
		switch protocol.GetType() {
		case waE2E.ProtocolMessage_EPHEMERAL_SYNC_RESPONSE,
			waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION,
			waE2E.ProtocolMessage_APP_STATE_SYNC_KEY_SHARE,
			waE2E.ProtocolMessage_APP_STATE_SYNC_KEY_REQUEST,
			waE2E.ProtocolMessage_MSG_FANOUT_BACKFILL_REQUEST,
			waE2E.ProtocolMessage_INITIAL_SECURITY_NOTIFICATION_SETTING_SYNC,
			waE2E.ProtocolMessage_APP_STATE_FATAL_EXCEPTION_NOTIFICATION,
			waE2E.ProtocolMessage_PEER_DATA_OPERATION_REQUEST_MESSAGE,
			waE2E.ProtocolMessage_PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE,
			waE2E.ProtocolMessage_CLOUD_API_THREAD_CONTROL_NOTIFICATION,
			waE2E.ProtocolMessage_LID_MIGRATION_MAPPING_SYNC:
			return true
		case waE2E.ProtocolMessage_REVOKE, waE2E.ProtocolMessage_MESSAGE_EDIT:
			return protocol.GetKey().GetID() == ""
		default:
			return false // Keep visible settings and future protocol message types.
		}
	}
	kind, _ := archiveMessageBody(message)
	return kind == "unknown" && len(message.ProtoReflect().GetUnknown()) == 0
}

func readableStructuredContent(message proto.Message, depth int) string {
	if message == nil || depth > 6 {
		return ""
	}
	fields := message.ProtoReflect().Descriptor().Fields()
	var parts []string
	for index := 0; index < fields.Len() && len(parts) < 32; index++ {
		field := fields.Get(index)
		reflection := message.ProtoReflect()
		if !reflection.Has(field) || field.IsList() || field.Name() == "contextInfo" {
			continue
		}
		if field.Kind() == protoreflect.StringKind {
			switch field.Name() {
			case "conversation", "text", "caption", "title", "description", "name", "displayName", "optionName", "selectedDisplayText":
				parts = append(parts, reflection.Get(field).String())
			}
		} else if field.Kind() == protoreflect.MessageKind {
			if text := readableStructuredContent(reflection.Get(field).Message().Interface(), depth+1); text != "" {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, "\n")
}

func storeIncomingMetadata(ctx context.Context, tx *sql.Tx, message *incomingMessage) error {
	if len(message.SourcePayload) == 0 {
		return nil // Queued events without a source payload cannot supply metadata.
	}
	observedAt := message.ObservedAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	if err := storeIncomingMetadataVersion(ctx, tx, message, observedAt); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO message_metadata(message_id,chat_jid,message_type,metadata_json,source_format,source_payload,observed_at)
		VALUES(?,?,?,?,?,?,?) ON CONFLICT(message_id,chat_jid) DO UPDATE SET
		message_type=excluded.message_type, metadata_json=excluded.metadata_json,
		source_format=excluded.source_format, source_payload=excluded.source_payload, observed_at=excluded.observed_at`,
		message.ID, message.ChatJID, message.MessageType, string(message.Metadata), message.SourceFormat, message.SourcePayload, observedAt)
	if err != nil {
		return fmt.Errorf("store message source metadata: %w", err)
	}
	return nil
}
