package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

func TestInitialHistoryProfilePreservesUpstreamCapabilities(t *testing.T) {
	original := proto.Clone(store.DeviceProps).(*waCompanionReg.DeviceProps)
	for _, days := range []int{0, 7, 90, 3650} {
		profile := initialHistoryProfile(original, days)
		if !profile.GetRequireFullSync() || profile == original || profile.HistorySyncConfig == original.HistorySyncConfig {
			t.Fatal("profile did not request full sync on a deep copy")
		}
		if days == 0 && profile.HistorySyncConfig.FullSyncDaysLimit != nil {
			t.Fatal("unbounded history was assigned an invented day limit")
		}
		if days > 0 && profile.HistorySyncConfig.GetFullSyncDaysLimit() != uint32(days) {
			t.Fatal("initial sync does not share the backfill period")
		}
		profile.RequireFullSync = original.RequireFullSync
		profile.HistorySyncConfig.FullSyncDaysLimit = original.HistorySyncConfig.FullSyncDaysLimit
		if !proto.Equal(profile, original) {
			t.Fatal("pairing change replaced upstream quota, identity, or capabilities")
		}
	}
}

func TestInitialHistoryConfigurationOnlyChangesFreshOptedInDevices(t *testing.T) {
	original := store.DeviceProps
	t.Cleanup(func() { store.DeviceProps = original })
	t.Setenv("WHATSAPP_HISTORY_BACKFILL_DAYS", "60")
	for _, enabled := range []string{"", "false"} {
		t.Setenv("WHATSAPP_HISTORY_INITIAL_SYNC_FULL", enabled)
		if err := configureInitialHistory(true); err != nil || store.DeviceProps != original {
			t.Fatal("legacy configuration changed device properties")
		}
	}
	t.Setenv("WHATSAPP_HISTORY_INITIAL_SYNC_FULL", "true")
	if err := configureInitialHistory(false); err != nil || store.DeviceProps != original {
		t.Fatal("saved session was treated as a new pairing")
	}
	if err := configureInitialHistory(true); err != nil || !store.DeviceProps.GetRequireFullSync() || store.DeviceProps.HistorySyncConfig.GetFullSyncDaysLimit() != 60 {
		t.Fatal("fresh opted-in device did not receive the requested profile")
	}
	t.Setenv("WHATSAPP_HISTORY_INITIAL_SYNC_FULL", "maybe")
	if err := configureInitialHistory(false); err == nil {
		t.Fatal("invalid sync flag was accepted")
	}
}

func TestInitialHistoryWaitConfiguration(t *testing.T) {
	for _, value := range []string{"", "0", "600", "3600"} {
		t.Setenv("WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS", value)
		if _, err := configuredInitialHistoryWait(); err != nil {
			t.Fatalf("valid wait rejected: %q", value)
		}
	}
	for _, value := range []string{"-1", "3601", "bad", "1.5"} {
		t.Setenv("WHATSAPP_HISTORY_INITIAL_SYNC_WAIT_MAX_SECONDS", value)
		if _, err := configuredInitialHistoryWait(); err == nil {
			t.Fatalf("invalid wait accepted: %q", value)
		}
	}
}

func TestInitialHistoryReadinessUsesHistoryActivityAndSafeTimeout(t *testing.T) {
	start := time.Unix(1000, 0)
	now := start.Add(100 * time.Second)
	quiet, maximum := 90*time.Second, 600*time.Second
	if !initialHistoryReady(start, now, time.Time{}, quiet, maximum, false, nil) {
		t.Fatal("no history arriving prevented fallback to backfill")
	}
	if initialHistoryReady(start, now, start.Add(20*time.Second), quiet, maximum, false, nil) {
		t.Fatal("quiet period was measured from startup instead of the latest history activity")
	}
	late := start.Add(700 * time.Second)
	if !initialHistoryReady(start, late, late, quiet, maximum, false, nil) {
		t.Fatal("maximum wait did not allow supplementing continued history arrivals")
	}
	for _, failure := range []struct {
		pending bool
		err     error
	}{{true, nil}, {false, errors.New("synthetic storage failure")}} {
		if initialHistoryReady(start, late, start, quiet, maximum, failure.pending, failure.err) {
			t.Fatal("timeout bypassed unfinished or unreadable durable ingestion")
		}
	}
}

func TestInitialHistoryWaitHonorsCancellationAndLegacyFixedDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitForInitialHistoryReady(ctx, time.Minute, time.Minute, func() (time.Time, bool, error) { return time.Time{}, false, nil }) {
		t.Fatal("adaptive wait ignored shutdown")
	}
	if !waitForInitialHistoryReady(context.Background(), 0, 0, func() (time.Time, bool, error) {
		t.Fatal("legacy delay inspected ingestion")
		return time.Time{}, true, nil
	}) {
		t.Fatal("legacy zero delay changed")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if waitForInitialHistoryReady(ctx, 0, time.Millisecond, func() (time.Time, bool, error) { return time.Time{}, true, nil }) {
		t.Fatal("maximum or zero quiet delay bypassed pending ingestion")
	}
}

func TestInitialHistoryWaitContinuesAfterPendingIngestionDrains(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	checks := 0
	if !waitForInitialHistoryReady(ctx, 0, time.Millisecond, func() (time.Time, bool, error) {
		checks++
		return time.Now(), checks < 4, nil
	}) || checks < 4 {
		t.Fatal("wait did not recheck ingestion until the durable queue drained")
	}
}

func TestInitialHistoryTypesExcludeLiveAndOnDemandActivity(t *testing.T) {
	for _, kind := range []waHistorySync.HistorySync_HistorySyncType{waHistorySync.HistorySync_INITIAL_BOOTSTRAP, waHistorySync.HistorySync_RECENT, waHistorySync.HistorySync_FULL} {
		if !isInitialHistorySync(kind) {
			t.Fatal("initial history stage was not observed")
		}
	}
	for _, kind := range []waHistorySync.HistorySync_HistorySyncType{waHistorySync.HistorySync_ON_DEMAND, waHistorySync.HistorySync_PUSH_NAME, waHistorySync.HistorySync_HistorySyncType(99)} {
		if isInitialHistorySync(kind) {
			t.Fatal("unrelated event extended the initial history quiet period")
		}
	}
}
