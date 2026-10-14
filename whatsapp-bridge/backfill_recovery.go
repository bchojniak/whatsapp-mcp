package main

import (
	"context"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow"
)

const automaticBackfillRetryLimit = 3

type backfillRecoveryState struct {
	Status           string     `json:"status"`
	AutomaticResumes int        `json:"automatic_resumes"`
	PhonePauses      int        `json:"phone_pauses"`
	NextAttemptAt    *time.Time `json:"next_attempt_at,omitempty"`
}

func retryableBackfillResult(result string) bool {
	switch result {
	case "response_timeout", "request_failed", "interrupted", "unmatched_response_unconfirmed":
		return true
	default:
		return false
	}
}

func backfillPageWithRecovery(ctx context.Context, client historyRequestClient, mailbox *backfillMailbox, chat *backfillChat, continuous bool) {
	if continuous && retryableBackfillResult(chat.LastResult) {
		chat.AutomaticRetries++
	}
	backfillPage(ctx, client, mailbox, chat)
	if continuous && chat.LastResult == "progress" {
		chat.AutomaticRetries = 0
	}
}

// Retries are bounded only while a chat fails to make progress. A phone-wide
// pause can wait indefinitely without spending the remaining chats' budgets.
func eligibleBackfillRecovery(report *backfillReport) bool {
	eligible := false
	for _, chat := range report.Chats {
		if chat.LinkedTo != "" {
			continue
		}
		if eligibleMetadataReplay(chat.MetadataReplay) {
			eligible = true
		}
		result := chat.Status
		if result == "pending" {
			if chat.Pages == 0 || chat.LastResult == "progress" {
				eligible = true
				continue
			}
			result = chat.LastResult
		}
		if !retryableBackfillResult(result) {
			continue
		}
		if chat.AutomaticRetries >= automaticBackfillRetryLimit {
			chat.LastResult = result
			chat.Status = "retry_limit_reached_unconfirmed"
			continue
		}
		eligible = true
	}
	return eligible
}

func backfillPhoneRetryDelay(pauses int) time.Duration {
	switch {
	case pauses <= 1:
		return 5 * time.Minute
	case pauses == 2:
		return 10 * time.Minute
	case pauses == 3:
		return 20 * time.Minute
	default:
		return 30 * time.Minute
	}
}

func waitForBackfillRecovery(ctx context.Context, next time.Time) bool {
	timer := time.NewTimer(time.Until(next))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func planBackfillRecovery(report *backfillReport, connected bool, now time.Time, initialResumeID string) (string, bool) {
	if report.Recovery == nil {
		report.Recovery = &backfillRecoveryState{}
	}
	recovery := report.Recovery
	switch report.Status {
	case "storage_failed", "verification_failed":
		recovery.Status = "blocked_storage"
		recovery.NextAttemptAt = nil
		return "", false
	case "running", "phone_unresponsive", "interrupted", "finished":
	default:
		recovery.Status = "worker_failed"
		recovery.NextAttemptAt = nil
		return "", false
	}
	if recovery.NextAttemptAt != nil && now.Before(*recovery.NextAttemptAt) {
		return "", false
	}
	if !connected {
		recovery.Status = "waiting_for_connection"
		next := now.Add(5 * time.Minute)
		recovery.NextAttemptAt = &next
		return "", false
	}
	resumeID := initialResumeID
	if report.Status != "running" {
		if !eligibleBackfillRecovery(report) {
			recovery.Status = "awaiting_source_data"
			delay := time.Hour
			if report.RequestedDays > 0 {
				// New devices can still be receiving their initial chat inventory.
				// This polls local data only; it sends no extra phone requests.
				delay = time.Minute
			}
			next := now.Add(delay)
			recovery.NextAttemptAt = &next
			return "", false
		}
		if recovery.NextAttemptAt == nil {
			delay := 5 * time.Minute
			recovery.Status = "waiting_for_retry"
			if report.Status == "phone_unresponsive" {
				recovery.Status = "waiting_for_phone"
				recovery.PhonePauses++
				delay = backfillPhoneRetryDelay(recovery.PhonePauses)
			} else {
				recovery.PhonePauses = 0
			}
			next := now.Add(delay)
			recovery.NextAttemptAt = &next
			return "", false
		}
		recovery.AutomaticResumes++
		resumeID = fmt.Sprintf("automatic-%d", recovery.AutomaticResumes)
	}
	recovery.Status = "running"
	recovery.NextAttemptAt = nil
	return resumeID, true
}

// The report remains the sole cursor checkpoint. Waiting times and retry
// budgets live there too, so restarting does not erase progress or cause bursts.
func runContinuousBackfill(ctx context.Context, client *whatsmeow.Client, store *MessageStore, mailbox *backfillMailbox, runID, initialResumeID string) {
	for ctx.Err() == nil {
		report, path, err := prepareBackfill(store, runID)
		if err != nil {
			fmt.Printf("BACKFILL_RECOVERY preparation_failed category=%s\n", storageErrorCategory(err))
			return
		}
		if err := initializeMetadataReplay(store, report); err != nil {
			fmt.Printf("BACKFILL_RECOVERY metadata_selection_failed category=%s\n", storageErrorCategory(err))
			return
		}
		if report.Recovery != nil && report.Recovery.NextAttemptAt != nil {
			if !waitForBackfillRecovery(ctx, *report.Recovery.NextAttemptAt) {
				return
			}
		}
		switch report.Status {
		case "phone_unresponsive", "interrupted", "finished":
			if err := refreshBackfillChats(store, report); err != nil {
				fmt.Printf("BACKFILL_RECOVERY discovery_failed category=%s\n", storageErrorCategory(err))
				return
			}
		}
		resumeID, run := planBackfillRecovery(report, client.IsConnected(), time.Now().UTC(), initialResumeID)
		if err := saveBackfillReport(path, report); err != nil {
			fmt.Println("BACKFILL_RECOVERY checkpoint_failed")
			return
		}
		if !run {
			if report.Recovery.NextAttemptAt == nil {
				return
			}
			continue
		}
		runOneOffBackfill(ctx, client, store, mailbox, runID, resumeID)
		if ctx.Err() != nil {
			return
		}
		checkpoint, _, err := prepareBackfill(store, runID)
		if err != nil {
			fmt.Println("BACKFILL_RECOVERY checkpoint_failed")
			return
		}
		if checkpoint.Status == "running" {
			checkpoint.Recovery.Status = "worker_failed"
			checkpoint.Recovery.NextAttemptAt = nil
			if err := saveBackfillReport(path, checkpoint); err != nil {
				fmt.Println("BACKFILL_RECOVERY checkpoint_failed")
			}
			return
		}
	}
}
