package main

import (
	"encoding/json"
	"fmt"
	"go.mau.fi/whatsmeow"
	"net/http"
	"time"
)

type SendMessageResponse struct {
	Success       bool   `json:"success"`
	Message       string `json:"message"`
	MessageID     string `json:"message_id,omitempty"`
	ChatJID       string `json:"chat_jid,omitempty"`
	ArchiveStatus string `json:"archive_status,omitempty"`
}

type SendMessageRequest struct {
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
	MediaPath string `json:"media_path,omitempty"`
}

type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

type DownloadMediaResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Build the internal API handler. Sending routes do not exist in read-only mode.
func newRESTHandler(client *whatsmeow.Client, messageStore *MessageStore, readOnly bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		health := map[string]interface{}{"ok": true, "connected": nil, "reauthentication_required": nil}
		if client != nil {
			health["connected"] = client.IsConnected()
			health["reauthentication_required"] = client.Store == nil || client.Store.ID == nil
		}
		if messageStore != nil {
			if messageStore.historyDiagnostics != nil {
				health["history_diagnostics"] = messageStore.historyDiagnostics.snapshot()
			}
			observations := messageStore.syncObservation.snapshot()
			if messageStore.ingestion != nil {
				observations = messageStore.ingestion.snapshot()
			}
			for key, value := range observations {
				health[key] = value
			}
		}
		json.NewEncoder(w).Encode(health)
	})
	mux.HandleFunc("/api/sync-history", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ChatJID string `json:"chat_jid"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.ChatJID == "" {
			http.Error(w, "chat_jid is required", http.StatusBadRequest)
			return
		}
		go requestHistorySyncForChat(client, messageStore, req.ChatJID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "requested", "chat_jid": req.ChatJID})
	})

	if !readOnly {
		// These endpoints are intentionally absent in read-only deployments.
		mux.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}

			var req SendMessageRequest
			r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "Invalid request format", http.StatusBadRequest)
				return
			}

			if req.Recipient == "" {
				http.Error(w, "Recipient is required", http.StatusBadRequest)
				return
			}

			if req.Message == "" && req.MediaPath == "" {
				http.Error(w, "Message or media path is required", http.StatusBadRequest)
				return
			}

			var response SendMessageResponse
			if req.MediaPath == "" {
				response = sendTextAndArchive(client, messageStore, req.Recipient, req.Message)
			} else {
				success, message := sendWhatsAppMessage(client, req.Recipient, req.Message, req.MediaPath)
				response = SendMessageResponse{Success: success, Message: message}
			}
			w.Header().Set("Content-Type", "application/json")

			if !response.Success {
				w.WriteHeader(http.StatusInternalServerError)
			}

			json.NewEncoder(w).Encode(response)
		})

	} else {
		fmt.Println("WhatsApp bridge read-only mode enabled; send endpoint is disabled")
	}

	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req DownloadMediaRequest
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}

		success, mediaType, filename, path, err := downloadMedia(client, messageStore, req.MessageID, req.ChatJID)

		w.Header().Set("Content-Type", "application/json")

		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to download media: %s", errMsg),
			})
			return
		}

		json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:  true,
			Message:  fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename,
			Path:     path,
		})
	})
	return mux
}

// Start a loopback-only REST API server for the local MCP process.
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, listenAddr string, readOnly bool) <-chan error {
	handler := newRESTHandler(client, messageStore, readOnly)

	fmt.Printf("Starting REST API server on %s...\n", listenAddr)

	serverErrors := make(chan error, 1)
	server := &http.Server{
		Addr:              listenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			serverErrors <- err
		}
	}()
	return serverErrors
}
