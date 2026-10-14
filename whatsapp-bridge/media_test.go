package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
)

type recordingMediaDownloadClient struct {
	t     *testing.T
	paths []string
	data  []byte
	err   error
}

func (client *recordingMediaDownloadClient) Download(_ context.Context, message whatsmeow.DownloadableMessage) ([]byte, error) {
	client.t.Helper()
	client.paths = append(client.paths, message.GetDirectPath())
	length, hasLength := message.(interface{ GetFileLength() uint64 })
	if !bytes.Equal(message.GetMediaKey(), []byte("synthetic-key")) ||
		!bytes.Equal(message.GetFileSHA256(), []byte("synthetic-hash")) ||
		!bytes.Equal(message.GetFileEncSHA256(), []byte("synthetic-encrypted-hash")) ||
		!hasLength || length.GetFileLength() != 15 || whatsmeow.GetMediaType(message) != whatsmeow.MediaImage {
		client.t.Fatal("download changed the stored crypto inputs or media type")
	}
	return client.data, client.err
}

func mediaDownloadFixture(t *testing.T, completeReference bool) *MessageStore {
	t.Helper()
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.StoreChat("100@s.whatsapp.net", "synthetic", time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	var key, hash, encryptedHash []byte
	var length uint64
	if completeReference {
		key, hash, encryptedHash = []byte("synthetic-key"), []byte("synthetic-hash"), []byte("synthetic-encrypted-hash")
		length = 15
	}
	if err := store.StoreMessage("media-fixture", "100@s.whatsapp.net", "100", "synthetic", time.Unix(100, 0), false,
		"image", "fixture.jpg", "https://mmg.whatsapp.net/v/fixture.enc?ccb=11&oh=synthetic%2Bhash&oe=123&ccb=12", key, hash, encryptedHash, length); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestDownloadMediaPreservesSignedReferenceAndCachesPrivateFile(t *testing.T) {
	store := mediaDownloadFixture(t, true)
	client := &recordingMediaDownloadClient{t: t, data: []byte("synthetic media")}
	ok, kind, filename, path, err := downloadMedia(client, store, "media-fixture", "100@s.whatsapp.net")
	if err != nil || !ok || kind != "image" || filename != "fixture.jpg" {
		t.Fatalf("download result: %v, %v, %q, %q", ok, err, kind, filename)
	}
	if len(client.paths) != 1 || client.paths[0] != "/v/fixture.enc?ccb=11&oh=synthetic%2Bhash&oe=123&ccb=12" {
		t.Fatalf("signed reference changed: %q", client.paths)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "synthetic media" {
		t.Fatalf("saved payload: %q, %v", data, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("downloaded file must be private: %v, %v", info, err)
	}
	client.err = errors.New("network unavailable")
	ok, _, _, cachedPath, err := downloadMedia(client, store, "media-fixture", "100@s.whatsapp.net")
	if err != nil || !ok || cachedPath != path || len(client.paths) != 1 {
		t.Fatalf("cached file triggered another download: %v, %v, %q", ok, err, client.paths)
	}
}

func TestDownloadMediaDoesNotRetryWithAlteredSignedPath(t *testing.T) {
	store := mediaDownloadFixture(t, true)
	client := &recordingMediaDownloadClient{t: t, err: errors.New("synthetic SDK failure")}
	ok, _, _, _, err := downloadMedia(client, store, "media-fixture", "100@s.whatsapp.net")
	if ok || err == nil || !strings.Contains(err.Error(), "synthetic SDK failure") {
		t.Fatalf("SDK failure was not returned: %v, %v", ok, err)
	}
	if len(client.paths) != 1 || client.paths[0] != "/v/fixture.enc?ccb=11&oh=synthetic%2Bhash&oe=123&ccb=12" {
		t.Fatalf("failure caused a modified signed request: %q", client.paths)
	}
	path := filepath.Join(store.storeDir, "100@s.whatsapp.net", "media-fixture.jpg")
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed download created a file: %v", err)
	}
}

func TestDownloadMediaUsesCachedFileWithoutCompleteReference(t *testing.T) {
	store := mediaDownloadFixture(t, false)
	directory := filepath.Join(store.storeDir, "100@s.whatsapp.net")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "media-fixture.jpg")
	if err := os.WriteFile(path, []byte("existing cached media"), 0600); err != nil {
		t.Fatal(err)
	}
	client := &recordingMediaDownloadClient{t: t, err: errors.New("should not download")}
	ok, kind, filename, cachedPath, err := downloadMedia(client, store, "media-fixture", "100@s.whatsapp.net")
	if err != nil || !ok || kind != "image" || filename != "fixture.jpg" || cachedPath != path || len(client.paths) != 0 {
		t.Fatalf("existing cached source was not used: %v, %v, %q, %q", ok, err, cachedPath, client.paths)
	}
}
