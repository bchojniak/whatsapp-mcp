package main

import (
	"context"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	return getChatName(context.Background(), client, messageStore, jid, chatJID, conversation, sender, logger)
}

func getChatName(ctx context.Context, client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	var existingName string
	err := messageStore.db.QueryRowContext(ctx, "SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		logger.Infof("Using existing WhatsApp chat name")
		return existingName
	}

	var name string

	if jid.Server == "g.us" {
		logger.Infof("Resolving WhatsApp group name")

		if data, ok := conversation.(*waHistorySync.Conversation); ok {
			name = data.GetDisplayName()
			if name == "" {
				name = data.GetName()
			}
		}

		if name == "" {
			var groupInfo *types.GroupInfo
			var err error
			if client != nil && ctx.Err() == nil {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				groupInfo, err = client.GetGroupInfo(ctx, jid)
				cancel()
			}
			if groupInfo == nil {
				err = fmt.Errorf("group info unavailable")
			}
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Infof("Resolved WhatsApp group name")
	} else {
		logger.Infof("Resolving WhatsApp contact name")

		var contact types.ContactInfo
		var err error
		if client != nil && client.Store != nil && client.Store.Contacts != nil {
			contact, err = client.Store.Contacts.GetContact(ctx, jid)
		}
		if err == nil && contact.FullName != "" {
			name = contact.FullName
		} else if sender != "" {
			name = sender
		} else {
			name = jid.User
		}

		logger.Infof("Resolved WhatsApp contact name")
	}

	return name
}

func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) error {
	ownUser := ""
	if client != nil && client.Store != nil && client.Store.ID != nil {
		ownUser = client.Store.ID.User
	}
	_, err := messageStore.applyHistory(context.Background(), client, historySync.Data, ownUser)
	return err
}

func requestHistorySyncForChat(client *whatsmeow.Client, messageStore *MessageStore, chatJID string) {
	defer func() {
		if rec := recover(); rec != nil {
			fmt.Printf("history sync recovered from panic: %v\n", rec)
		}
	}()
	if client == nil || messageStore == nil {
		fmt.Println("history sync: client or store missing")
		return
	}
	if !client.IsConnected() || client.Store == nil || client.Store.ID == nil {
		fmt.Println("history sync: not connected")
		return
	}
	aliases, err := knownHistoryAliases(client, chatJID)
	if err != nil {
		fmt.Printf("history sync: identity lookup failed category=%s\n", storageErrorCategory(err))
		return
	}
	cursor, err := messageStore.oldestHistoryAnchor(aliases)
	if err != nil || cursor.ID == "" {
		fmt.Printf("history sync: no stored anchor category=%s\n", storageErrorCategory(err))
		return
	}
	requestJID, err := messageStore.preferredHistoryIdentifier(aliases, chatJID)
	if err != nil {
		fmt.Printf("history sync: identifier selection failed category=%s\n", storageErrorCategory(err))
		return
	}
	chat, err := types.ParseJID(requestJID)
	if err != nil {
		fmt.Printf("history sync: bad chat jid: %v\n", err)
		return
	}
	historyMsg := client.BuildHistorySyncRequest(cursor.messageInfo(chat), 50)
	if historyMsg == nil {
		fmt.Println("history sync: BuildHistorySyncRequest returned nil")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = client.SendPeerMessage(ctx, historyMsg)
	if err != nil {
		fmt.Printf("history sync request failed: %v\n", err)
		return
	}
	fmt.Println("History sync request acknowledged; waiting for the phone's response")
}
