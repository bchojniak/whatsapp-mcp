package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mdp/qrterminal"
	"go.mau.fi/whatsmeow"
	"rsc.io/qr"
)

type pairingServer struct {
	mu        sync.Mutex
	code      string
	state     string
	token     string
	tokenPath string
	connected func() bool
	http      *http.Server
}

func startPairingServer(storeDir string, connected func() bool) (*pairingServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:8082")
	if err != nil {
		return nil, err
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		listener.Close()
		return nil, err
	}
	server := &pairingServer{token: hex.EncodeToString(key), state: "waiting_for_qr", connected: connected,
		tokenPath: filepath.Join(storeDir, ".pairing-token")}
	if info, err := os.Lstat(server.tokenPath); err == nil && !info.Mode().IsRegular() {
		listener.Close()
		return nil, errors.New("pairing token path must be a regular file")
	} else if err != nil && !os.IsNotExist(err) {
		listener.Close()
		return nil, err
	}
	if err = os.WriteFile(server.tokenPath, []byte(server.token), 0600); err != nil {
		listener.Close()
		return nil, err
	}
	if err = os.Chmod(server.tokenPath, 0600); err != nil {
		listener.Close()
		os.Remove(server.tokenPath)
		return nil, err
	}
	server.http = &http.Server{Handler: server, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second}
	go func() {
		_ = server.http.Serve(listener)
	}()
	return server, nil
}

func (server *pairingServer) setState(state, code string) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.state, server.code = state, code
}

func (server *pairingServer) close() {
	server.setState("closed", "")
	_ = server.http.Close()
	_ = os.Remove(server.tokenPath)
}

func (server *pairingServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	if request.Host != "127.0.0.1:8082" || request.Header.Get("Origin") != "" {
		http.Error(writer, "Forbidden", http.StatusForbidden)
		return
	}
	provided := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	if !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") || server.token == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(server.token)) != 1 {
		http.Error(writer, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if request.Method != http.MethodGet || request.URL.RawQuery != "" {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	server.mu.Lock()
	state, code := server.state, server.code
	server.mu.Unlock()
	if server.connected() {
		state, code = "connected", ""
	}
	switch request.URL.Path {
	case "/status":
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]string{"status": state})
	case "/qr":
		if state != "scan_qr" || code == "" {
			http.Error(writer, "QR not available", http.StatusConflict)
			return
		}
		image, err := qr.Encode(code, qr.M)
		if err != nil {
			http.Error(writer, "QR unavailable", http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("Content-Type", "image/png")
		_, _ = writer.Write(image.PNG())
	default:
		http.NotFound(writer, request)
	}
}

func connectWhatsApp(client *whatsmeow.Client, pairing *pairingServer, allowLogQR bool) error {
	if client.Store.ID != nil {
		return client.Connect()
	}
	if pairing == nil && !allowLogQR {
		return errors.New("pairing required; enable WHATSAPP_PAIRING_MODE for private QR access")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	codes, err := client.GetQRChannel(ctx)
	if err != nil {
		return errors.New("could not start WhatsApp pairing")
	}
	if err := client.Connect(); err != nil {
		return err
	}
	for event := range codes {
		switch event.Event {
		case "code":
			if pairing != nil {
				pairing.setState("scan_qr", event.Code)
			} else {
				fmt.Println("\nScan this QR code with your WhatsApp app:")
				qrterminal.GenerateHalfBlock(event.Code, qrterminal.L, os.Stdout)
			}
		case "success":
			if pairing != nil {
				pairing.setState("connected", "")
			}
			fmt.Println("\nSuccessfully connected and authenticated!")
			return nil
		default:
			if pairing != nil {
				pairing.setState("expired", "")
			}
		}
	}
	return errors.New("pairing expired or failed; restart the unpaired worker to request a fresh QR")
}
