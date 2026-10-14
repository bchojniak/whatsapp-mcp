package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestReadOnlyHandlerDoesNotExposeSendEndpoint(t *testing.T) {
	handler := newRESTHandler(nil, nil, true)

	for _, path := range []string{"/api/send"} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != http.StatusNotFound {
			t.Fatalf("%s returned %d, want 404 in read-only mode", path, res.Code)
		}
	}
}

func TestReadOnlyHandlerKeepsHealthEndpoint(t *testing.T) {
	handler := newRESTHandler(nil, nil, true)
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("health returned %d, want 200", res.Code)
	}
}

func TestMessageStoreUsesPrivatePermissions(t *testing.T) {
	dir := t.TempDir() + "/store"
	store, err := NewMessageStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for path, want := range map[string]os.FileMode{
		dir:                  0700,
		dir + "/messages.db": 0600,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Fatalf("%s permissions are %o, want %o", path, got, want)
		}
	}
}

func TestReadOnlyHandlerKeepsDownloadValidation(t *testing.T) {
	handler := newRESTHandler(nil, nil, true)
	for _, tc := range []struct {
		method, body string
		want         int
	}{
		{http.MethodGet, "", http.StatusMethodNotAllowed},
		{http.MethodPost, "{", http.StatusBadRequest},
		{http.MethodPost, "{}", http.StatusBadRequest},
	} {
		req := httptest.NewRequest(tc.method, "/api/download", strings.NewReader(tc.body))
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != tc.want {
			t.Fatalf("download returned %d, want %d", res.Code, tc.want)
		}
	}
}
