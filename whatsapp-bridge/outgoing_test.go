package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

func TestOutgoingTextIsSearchableWithProviderIdentity(t *testing.T) {
	for _, server := range []string{types.DefaultUserServer, types.HiddenUserServer, types.GroupServer} {
		t.Run(server, func(t *testing.T) {
			store, journal := ingestionFixture(t)
			own, to := types.NewJID("101", types.DefaultUserServer), types.NewJID("202", server)
			dispatched := 0
			send := func(ctx context.Context, recipient types.JID, message *waE2E.Message) (whatsmeow.SendResponse, error) {
				dispatched++
				if recipient != to || message.GetConversation() != "synthetic outgoing text" {
					t.Fatal("wrong dispatch")
				}
				return whatsmeow.SendResponse{ID: "outgoing-fixture", Timestamp: time.Unix(123, 0)}, nil
			}
			result := dispatchTextAndArchive(context.Background(), store, own, to, "synthetic outgoing text", send)
			if !result.Success || result.MessageID != "outgoing-fixture" || result.ChatJID != to.String() || result.ArchiveStatus != "saved" || dispatched != 1 {
				t.Fatalf("unexpected result: %+v", result)
			}
			var fromMe bool
			var text, source string
			if err := store.db.QueryRow(`SELECT content,is_from_me FROM messages WHERE id=? AND chat_jid=?`, result.MessageID, to.String()).Scan(&text, &fromMe); err != nil || !fromMe || text != "synthetic outgoing text" {
				t.Fatalf("outgoing message absent: %v", err)
			}
			if err := store.db.QueryRow(`SELECT source_kind FROM message_metadata_versions WHERE message_id=?`, result.MessageID).Scan(&source); err != nil || source != "outgoing" {
				t.Fatalf("wrong provenance %q: %v", source, err)
			}
			if journal.processOne(context.Background()) {
				t.Fatal("successful immediate archive should not leave a queued duplicate")
			}
			var hasFTS bool
			if err := store.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='messages_fts')`).Scan(&hasFTS); err != nil {
				t.Fatal(err)
			}
			if hasFTS {
				var hits int
				if err := store.db.QueryRow(`SELECT count(*) FROM messages_fts WHERE messages_fts MATCH '"synthetic outgoing"'`).Scan(&hits); err != nil || hits != 1 {
					t.Fatalf("sent text was not indexed for search: %d, %v", hits, err)
				}
			}
		})
	}
}

func TestOutgoingArchiveFailureRecoversWithoutSendingAgain(t *testing.T) {
	store, journal := ingestionFixture(t)
	_, err := store.db.Exec(`CREATE TRIGGER reject_outgoing BEFORE INSERT ON messages BEGIN SELECT RAISE(FAIL,'synthetic failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	sends := 0
	result := dispatchTextAndArchive(context.Background(), store, types.NewJID("101", types.DefaultUserServer), types.NewJID("202", types.DefaultUserServer), "queued text",
		func(context.Context, types.JID, *waE2E.Message) (whatsmeow.SendResponse, error) {
			sends++
			return whatsmeow.SendResponse{ID: "queued-fixture", Timestamp: time.Unix(124, 0)}, nil
		})
	if !result.Success || result.ArchiveStatus != "pending" || result.MessageID != "queued-fixture" {
		t.Fatalf("unexpected result %+v", result)
	}
	if _, err = store.db.Exec(`DROP TRIGGER reject_outgoing`); err != nil {
		t.Fatal(err)
	}
	if !journal.processOne(context.Background()) || sends != 1 {
		t.Fatal("archive recovery failed or repeated send")
	}
	var count int
	if err = store.db.QueryRow(`SELECT count(*) FROM messages WHERE id='queued-fixture'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("archive count %d: %v", count, err)
	}
}

func TestFailedSendDoesNotInventAnArchivedMessage(t *testing.T) {
	store, _ := ingestionFixture(t)
	result := dispatchTextAndArchive(context.Background(), store, types.NewJID("101", types.DefaultUserServer), types.NewJID("202", types.DefaultUserServer), "unsent text",
		func(context.Context, types.JID, *waE2E.Message) (whatsmeow.SendResponse, error) {
			return whatsmeow.SendResponse{}, errors.New("synthetic failure")
		})
	var count int
	if err := store.db.QueryRow(`SELECT count(*) FROM messages`).Scan(&count); err != nil || count != 0 || result.Success || result.MessageID != "" {
		t.Fatalf("failed send invented a message: %+v, %v", result, err)
	}
}

func TestAcceptedSendReportsUnrecoverableArchiveFailure(t *testing.T) {
	store, journal := ingestionFixture(t)
	store.db.Close()
	journal.db.Close()
	result := dispatchTextAndArchive(context.Background(), store, types.NewJID("101", types.DefaultUserServer), types.NewJID("202", types.DefaultUserServer), "accepted text",
		func(context.Context, types.JID, *waE2E.Message) (whatsmeow.SendResponse, error) {
			return whatsmeow.SendResponse{ID: "archive-failed-fixture"}, nil
		})
	if !result.Success || result.MessageID != "archive-failed-fixture" || result.ArchiveStatus != "failed" {
		t.Fatalf("archive failure disguised an accepted send: %+v", result)
	}
}
