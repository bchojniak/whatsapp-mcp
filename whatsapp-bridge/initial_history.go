package main

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

func configureInitialHistory(fresh bool) error {
	raw := strings.TrimSpace(os.Getenv("WHATSAPP_HISTORY_INITIAL_SYNC_FULL"))
	if raw == "" {
		return nil
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		return errors.New("WHATSAPP_HISTORY_INITIAL_SYNC_FULL must be a boolean")
	}
	if !enabled || !fresh {
		return nil
	}
	days, err := configuredBackfillDays()
	if err != nil {
		return err
	}
	store.DeviceProps = initialHistoryProfile(store.DeviceProps, days)
	return nil
}

func initialHistoryProfile(current *waCompanionReg.DeviceProps, days int) *waCompanionReg.DeviceProps {
	profile := proto.Clone(current).(*waCompanionReg.DeviceProps)
	profile.RequireFullSync = proto.Bool(true)
	if profile.HistorySyncConfig == nil {
		profile.HistorySyncConfig = &waCompanionReg.DeviceProps_HistorySyncConfig{}
	}
	profile.HistorySyncConfig.FullSyncDaysLimit = nil
	if days > 0 {
		profile.HistorySyncConfig.FullSyncDaysLimit = proto.Uint32(uint32(days))
	}
	return profile
}

func configuredInitialHistoryWait() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv("WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS"))
	if raw == "" {
		return 0, nil
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 0 || seconds > 3600 {
		return 0, errors.New("WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS must be from 0 to 3600")
	}
	return time.Duration(seconds) * time.Second, nil
}

func isInitialHistorySync(syncType waHistorySync.HistorySync_HistorySyncType) bool {
	switch syncType {
	case waHistorySync.HistorySync_INITIAL_BOOTSTRAP, waHistorySync.HistorySync_RECENT, waHistorySync.HistorySync_FULL:
		return true
	default:
		return false
	}
}

func initialHistoryReady(started, now, last time.Time, quiet, maximum time.Duration, pending bool, snapshotErr error) bool {
	if pending || snapshotErr != nil {
		return false
	}
	if now.Sub(started) >= maximum {
		return true
	}
	if last.Before(started) {
		last = started
	}
	return now.Sub(last) >= quiet
}

func waitForInitialHistoryReady(ctx context.Context, quiet, maximum time.Duration, snapshot func() (time.Time, bool, error)) bool {
	if maximum == 0 {
		return waitForInitialHistory(ctx, quiet)
	}
	started := time.Now()
	interval := 250 * time.Millisecond
	for _, duration := range []time.Duration{quiet, maximum} {
		if duration > 0 && duration < interval {
			interval = duration
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return false
		}
		last, pending, err := snapshot()
		if initialHistoryReady(started, time.Now(), last, quiet, maximum, pending, err) {
			return true
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return false
		}
	}
}
