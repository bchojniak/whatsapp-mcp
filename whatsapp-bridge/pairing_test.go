package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrivatePairingBoundaryAndQRRemoval(t *testing.T) {
	connected := false
	server := &pairingServer{token: "synthetic-test-token", connected: func() bool { return connected }}
	server.setState("scan_qr", "synthetic-qr-payload")
	for _, tc := range []struct {
		path, host, auth, origin, method string
		want                             int
	}{
		{"/qr", "127.0.0.1:8082", "", "", "GET", 401},
		{"/qr", "127.0.0.1:8082", server.token, "", "GET", 401},
		{"/qr", "public.example.com", "Bearer " + server.token, "", "GET", 403},
		{"/qr", "127.0.0.1:8082", "Bearer " + server.token, "https://example.com", "GET", 403},
		{"/qr?token=x", "127.0.0.1:8082", "Bearer " + server.token, "", "GET", 405},
		{"/qr", "127.0.0.1:8082", "Bearer " + server.token, "", "POST", 405},
		{"/qr", "127.0.0.1:8082", "Bearer " + server.token, "", "GET", 200},
	} {
		request := httptest.NewRequest(tc.method, "http://127.0.0.1:8082"+tc.path, nil)
		request.Host = tc.host
		request.Header.Set("Authorization", tc.auth)
		request.Header.Set("Origin", tc.origin)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != tc.want {
			t.Fatalf("%+v returned %d", tc, response.Code)
		}
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("pairing content could be cached")
		}
		if response.Code == 200 && !strings.HasPrefix(response.Body.String(), "\x89PNG\r\n\x1a\n") {
			t.Fatal("QR was not delivered as PNG")
		}
	}
	connected = true
	request := httptest.NewRequest("GET", "http://127.0.0.1:8082/qr", nil)
	request.Header.Set("Authorization", "Bearer "+server.token)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatal("a paired account still exposed a QR")
	}
	request.URL.Path = "/status"
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if !strings.Contains(response.Body.String(), "connected") || strings.Contains(response.Body.String(), server.token) {
		t.Fatal("status leaked private state or did not report connection")
	}
}

func TestPairingTokenIsPrivateAndRemovedOnShutdown(t *testing.T) {
	root := t.TempDir()
	server, err := startPairingServer(root, func() bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.close)
	path := filepath.Join(root, ".pairing-token")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("pairing token was not private")
	}
	server.close()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("pairing token survived clean shutdown")
	}
}
