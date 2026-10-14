package main

import (
	"context"
	"testing"
	"time"
)

func TestWindowStopsRequestsAtCutoffWithoutClaimingFullHistory(t *testing.T) {
	cutoff := time.Unix(200, 0)
	report := &backfillReport{TargetAfter: &cutoff, Chats: []*backfillChat{
		{Status: "pending", Cursor: historyCursor{ID: "older", Timestamp: time.Unix(100, 0)}},
		{Status: "pending", Cursor: historyCursor{ID: "boundary", Timestamp: cutoff}},
		{Status: "pending", Cursor: historyCursor{ID: "recent", Timestamp: time.Unix(300, 0)}},
		{Status: "storage_failed", Cursor: historyCursor{ID: "failed", Timestamp: time.Unix(100, 0)}},
		{Status: "no_anchor"},
	}}
	applyBackfillWindow(report)
	if report.Chats[0].Status != "requested_window_reached" || report.Chats[1].Status != "requested_window_reached" {
		t.Fatal("window did not stop at the saved cutoff")
	}
	if report.Chats[2].Status != "pending" || report.Chats[3].Status != "storage_failed" || report.Chats[4].Status != "no_anchor" {
		t.Fatal("window overwrote unresolved evidence")
	}
}

func TestWindowCutoffSurvivesRestartAndRejectsChangedTarget(t *testing.T) {
	t.Setenv("WHATSAPP_HISTORY_BACKFILL_DAYS", "30")
	store := backfillFixture(t)
	report, _, err := prepareBackfill(store, "recent")
	if err != nil {
		t.Fatal(err)
	}
	if report.TargetAfter == nil || report.RequestedDays != 30 {
		t.Fatal("window not recorded")
	}
	loaded, _, err := prepareBackfill(store, "recent")
	if err != nil || !loaded.TargetAfter.Equal(*report.TargetAfter) {
		t.Fatal("restart slid the cutoff")
	}
	t.Setenv("WHATSAPP_HISTORY_BACKFILL_DAYS", "7")
	if _, _, err := prepareBackfill(store, "recent"); err == nil {
		t.Fatal("existing run was silently relabeled")
	}
	t.Setenv("WHATSAPP_HISTORY_BACKFILL_DAYS", "")
	if loaded, _, err := prepareBackfill(store, "recent"); err != nil || loaded.TargetAfter == nil {
		t.Fatal("removing config expanded an existing bounded run")
	}
}

func TestLegacyRunCannotBeRelabeledAsBounded(t *testing.T) {
	t.Setenv("WHATSAPP_HISTORY_BACKFILL_DAYS", "")
	store := backfillFixture(t)
	if _, _, err := prepareBackfill(store, "legacy"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WHATSAPP_HISTORY_BACKFILL_DAYS", "30")
	if _, _, err := prepareBackfill(store, "legacy"); err == nil {
		t.Fatal("legacy progress was reused under a different target")
	}
}

func TestWindowConfigurationAndCancelledInitialWait(t *testing.T) {
	for _, value := range []string{"-1", "3651", "bad", "1.5"} {
		t.Setenv("WHATSAPP_HISTORY_BACKFILL_DAYS", value)
		if _, err := configuredBackfillDays(); err == nil {
			t.Fatalf("invalid days accepted: %s", value)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitForInitialHistory(ctx, time.Minute) || waitForInitialHistory(ctx, 0) {
		t.Fatal("initial wait ignored shutdown")
	}
	for _, value := range []string{"-1", "601", "bad"} {
		t.Setenv("WHATSAPP_HISTORY_BACKFILL_START_DELAY_SECONDS", value)
		if _, err := configuredBackfillDelay(); err == nil {
			t.Fatal("invalid startup delay accepted")
		}
	}
}
