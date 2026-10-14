package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func envBool(name string, defaultValue bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if value == "" {
		return defaultValue
	}
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func main() {
	logger := waLog.Stdout("Client", "INFO", true)
	logger.Infof("Starting WhatsApp client...")

	storeDir := os.Getenv("WHATSAPP_STORE_DIR")
	if storeDir == "" {
		storeDir = "store"
	}
	storeDir = filepath.Clean(storeDir)
	apiListenAddr := os.Getenv("WHATSAPP_API_LISTEN_ADDR")
	if apiListenAddr == "" {
		apiListenAddr = "127.0.0.1:8080"
	}
	readOnly := envBool("WHATSAPP_MCP_READ_ONLY", true)
	allowLogQR := envBool("WHATSAPP_ALLOW_LOG_QR", false)
	privatePairing := envBool("WHATSAPP_PAIRING_MODE", false)
	if _, err := configuredBackfillDays(); err != nil {
		logger.Errorf("Invalid backfill window: %v", err)
		return
	}
	initialHistoryMaximum, err := configuredInitialHistoryWait()
	if err != nil {
		logger.Errorf("Invalid initial history wait: %v", err)
		return
	}
	backfillDelay, err := configuredBackfillDelay()
	if err != nil {
		logger.Errorf("Invalid initial history delay: %v", err)
		return
	}

	dbLog := waLog.Stdout("Database", "INFO", true)

	if err := os.MkdirAll(storeDir, 0700); err != nil {
		logger.Errorf("Failed to create store directory: %v", err)
		return
	}
	if err := os.Chmod(storeDir, 0700); err != nil {
		logger.Errorf("Failed to secure store directory: %v", err)
		return
	}

	sessionDBPath := filepath.ToSlash(filepath.Join(storeDir, "whatsapp.db"))
	container, err := sqlstore.New(context.Background(), "sqlite3", fmt.Sprintf("file:%s?_foreign_keys=on", sessionDBPath), dbLog)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return
	}
	if err := os.Chmod(sessionDBPath, 0600); err != nil && !os.IsNotExist(err) {
		logger.Errorf("Failed to secure WhatsApp session database: %v", err)
		return
	}

	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			return
		}
	}

	if err := configureInitialHistory(deviceStore.ID == nil); err != nil {
		logger.Errorf("Invalid initial history profile: %v", err)
		return
	}
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		return
	}

	client.ManualHistorySyncDownload = true
	client.DisableManualHistorySyncReceipt = true
	client.SynchronousAck = true
	client.EnableDecryptedEventBuffer = true

	messageStore, err := NewMessageStore(storeDir)
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		return
	}
	defer messageStore.Close()
	backfillRunID := strings.TrimSpace(os.Getenv("WHATSAPP_HISTORY_BACKFILL_RUN_ID"))
	backfillResumeID := strings.TrimSpace(os.Getenv("WHATSAPP_HISTORY_BACKFILL_RESUME_ID"))
	continuousBackfill := envBool("WHATSAPP_HISTORY_BACKFILL_CONTINUOUS", false)
	diagnosticRunID := strings.TrimSpace(os.Getenv("WHATSAPP_HISTORY_DIAGNOSTIC_RUN_ID"))
	previousDiagnosticRunID := strings.TrimSpace(os.Getenv("WHATSAPP_HISTORY_DIAGNOSTIC_PREVIOUS_RUN_ID"))
	diagnosticMode := strings.TrimSpace(os.Getenv("WHATSAPP_HISTORY_DIAGNOSTIC_MODE"))
	backfillMailbox := &backfillMailbox{responses: make(chan backfillReply, 16)}
	var diagnostics *historyDiagnostics
	if diagnosticRunID != "" {
		diagnostics, err = openHistoryDiagnostics(messageStore, diagnosticRunID, backfillRunID, previousDiagnosticRunID, diagnosticMode)
		if err != nil {
			logger.Errorf("Failed to initialize history diagnostics: %s", storageErrorCategory(err))
			return
		}
	}

	journal, err := newIngestionJournal(messageStore, client, func(history *events.HistorySync, storageError error) {
		if backfillRunID != "" {
			backfillMailbox.handle(history, storageError)
		}
	})
	if err != nil {
		logger.Errorf("Failed to initialize ingestion journal: %s", storageErrorCategory(err))
		return
	}
	journal.start()
	defer journal.close()

	client.AddEventHandlerWithSuccessStatus(func(evt interface{}) bool {
		switch v := evt.(type) {
		case *events.Message:
			return journal.enqueueIncomingMessage(v, diagnostics) == nil

		case *events.HistorySync:
			messageStore.syncObservation.record(false)
			payload, err := captureHistory(client, v)
			if err != nil {
				journal.rejectEvent("invalid_history_payload")
				return false
			}
			if diagnostics != nil {
				payload.HistoryTraceID = diagnostics.recordReceived(v.Data)
			}
			return journal.enqueue(payload) == nil
		case *events.Connected:
			journal.recordConnection("connected", "connection_established")
			logger.Infof("Connected to WhatsApp")
		case *events.Disconnected:
			journal.recordConnection("disconnected", "connection_lost")
		case *events.StreamReplaced:
			journal.recordConnection("disconnected", "session_replaced")
		case *events.ClientOutdated:
			journal.recordConnection("disconnected", "client_outdated")
		case *events.LoggedOut:
			journal.recordConnection("logged_out", "reauthentication_required")
			logger.Warnf("Device logged out, please scan QR code to log in again")
		case events.PermanentDisconnect:
			journal.recordConnection("disconnected", "permanent_connection_failure")
		}
		return true
	})

	var pairing *pairingServer
	if privatePairing {
		pairing, err = startPairingServer(storeDir, client.IsConnected)
		if err != nil {
			logger.Errorf("Private pairing listener could not start")
			return
		}
		defer pairing.close()
		logger.Infof("Private pairing is available through authenticated Railway SSH")
	}

	if err := connectWhatsApp(client, pairing, allowLogQR); err != nil {
		logger.Errorf("WhatsApp connection failed: %v", err)
		return
	}

	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		logger.Errorf("Failed to establish stable connection")
		return
	}

	fmt.Println("\n✓ Connected to WhatsApp! Type 'help' for commands.")

	serverErrors := startRESTServer(client, messageStore, apiListenAddr, readOnly)
	var backfillCtx context.Context
	var cancelBackfill context.CancelFunc
	if continuousBackfill && diagnostics == nil && backfillRunID != "" {
		backfillCtx, cancelBackfill = context.WithCancel(context.Background())
	} else {
		backfillCtx, cancelBackfill = context.WithTimeout(context.Background(), 24*time.Hour)
	}
	backfillDone := make(chan struct{})
	if diagnostics != nil {
		go func() {
			defer close(backfillDone)
			runHistoryDiagnostic(backfillCtx, client, messageStore, diagnostics)
		}()
	} else if backfillRunID != "" {
		go func() {
			defer close(backfillDone)
			if initialHistoryMaximum > 0 {
				journal.recordInitialHistoryPhase("receiving")
			}
			if !waitForInitialHistoryReady(backfillCtx, backfillDelay, initialHistoryMaximum, journal.initialHistorySnapshot) {
				return
			}
			journal.recordInitialHistoryPhase("backfill_ready")
			if continuousBackfill {
				runContinuousBackfill(backfillCtx, client, messageStore, backfillMailbox, backfillRunID, backfillResumeID)
			} else {
				runOneOffBackfill(backfillCtx, client, messageStore, backfillMailbox, backfillRunID, backfillResumeID)
			}
		}()
	} else {
		close(backfillDone)
	}
	defer func() {
		cancelBackfill()
		<-backfillDone
	}()

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("REST server is running. Press Ctrl+C to disconnect and exit.")

	select {
	case <-exitChan:
	case err := <-serverErrors:
		logger.Errorf("REST API server failed: %v", err)
	}

	fmt.Println("Disconnecting...")
	cancelBackfill()
	journal.recordConnection("disconnected", "planned_shutdown")
	client.Disconnect()
}
