package main

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

func configuredBackfillDays() (int, error) {
	raw := strings.TrimSpace(os.Getenv("WHATSAPP_HISTORY_BACKFILL_DAYS"))
	if raw == "" {
		return 0, nil
	}
	days, err := strconv.Atoi(raw)
	if err != nil || days < 0 || days > 3650 {
		return 0, errors.New("WHATSAPP_HISTORY_BACKFILL_DAYS must be an integer from 0 to 3650")
	}
	return days, nil
}

func applyBackfillWindow(report *backfillReport) {
	if report.TargetAfter == nil {
		return
	}
	for _, chat := range report.Chats {
		if chat.LinkedTo == "" && chat.Status == "pending" && chat.Cursor.ID != "" &&
			!chat.Cursor.Timestamp.After(*report.TargetAfter) {
			chat.Status = "requested_window_reached"
			chat.LastResult = chat.Status
		}
	}
}

func validateBackfillWindow(report *backfillReport, days int) error {
	if report.RequestedDays < 0 || report.RequestedDays > 3650 ||
		(report.RequestedDays == 0) != (report.TargetAfter == nil) {
		return errors.New("invalid saved backfill window")
	}
	if days != 0 && days != report.RequestedDays {
		return errors.New("backfill window differs from saved run; choose a new run ID")
	}
	if report.TargetAfter != nil {
		expected := report.Started.Add(-time.Duration(report.RequestedDays) * 24 * time.Hour)
		if !report.TargetAfter.Equal(expected) {
			return errors.New("saved backfill cutoff does not match run start")
		}
	}
	return nil
}

func configuredBackfillDelay() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv("WHATSAPP_HISTORY_BACKFILL_START_DELAY_SECONDS"))
	if raw == "" {
		return 0, nil
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 0 || seconds > 600 {
		return 0, errors.New("WHATSAPP_HISTORY_BACKFILL_START_DELAY_SECONDS must be from 0 to 600")
	}
	return time.Duration(seconds) * time.Second, nil
}

func waitForInitialHistory(ctx context.Context, delay time.Duration) bool {
	if delay == 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
