package main

import "errors"

func verifiedDiagnosticReply(report *diagnosticReport, attempt *diagnosticAttempt) bool {
	if attempt.Started == nil || attempt.Finished == nil || attempt.RequestID == "" || !responsiveHistoryResult(attempt.Result) {
		return false
	}
	notified := false
	for _, notification := range report.Notifications {
		if notification.SessionID == attempt.RequestID || notification.OriginalID == attempt.RequestID || notification.StanzaID == attempt.RequestID {
			notified = true
		}
	}
	if !notified {
		return false
	}
	for _, response := range report.Responses {
		if response.Stored == nil || response.PermanentStorageFailure || response.ActiveAttempt != attempt.Number || response.Received.Before(*attempt.Started) || response.Received.After(*attempt.Finished) {
			continue
		}
		for _, conversation := range response.Conversations {
			if diagnosticConversationMatches(attempt, conversation) {
				return true
			}
		}
	}
	return false
}

func selectRecoveryAttempts(store *MessageStore, previous *diagnosticReport, aliasesFor func(string) ([]string, error)) ([]*diagnosticAttempt, error) {
	if previous.Status != "finished" || len(previous.Attempts) > maxDiagnosticAttempts || previous.Mode != "" {
		return nil, errors.New("finished identifier experiment required")
	}
	var control *diagnosticAttempt
	var recovery []*diagnosticAttempt
	var selected []string
	for _, source := range previous.Attempts {
		if !verifiedDiagnosticReply(previous, source) {
			continue
		}
		if source.Label == "control" {
			control = source
			continue
		}
		if source.Variant != "alternate_alias" || len(recovery) == 3 {
			continue
		}
		aliases, err := aliasesFor(source.JID)
		if err != nil {
			return nil, err
		}
		if !containsIdentifier(aliases, source.JID) || len(aliases) != len(source.Aliases) {
			return nil, errors.New("identifier evidence no longer matches known aliases")
		}
		for _, alias := range aliases {
			if !containsIdentifier(source.Aliases, alias) || containsIdentifier(selected, alias) {
				return nil, errors.New("unverified or duplicate conversation")
			}
		}
		anchor, err := store.oldestHistoryAnchor(aliases)
		if err != nil {
			return nil, err
		}
		if err = store.rememberHistoryIdentifier(aliases, source.JID); err != nil {
			return nil, err
		}
		identifier, err := store.preferredHistoryIdentifier(aliases, source.JID)
		if err != nil {
			return nil, err
		}
		attempt := &diagnosticAttempt{Label: source.Label, Variant: "oldest_boundary", JID: identifier, Aliases: aliases, Anchor: anchor, Result: "planned"}
		attempt.AnchorValidation = validateDiagnosticAnchor(store, attempt)
		if attempt.AnchorValidation != "stored_exact" && attempt.AnchorValidation != "stored_alias" {
			return nil, errors.New("invalid oldest stored anchor")
		}
		recovery = append(recovery, attempt)
		selected = append(selected, aliases...)
	}
	if control == nil || len(recovery) == 0 {
		return nil, errors.New("verified control and recovery conversations required")
	}
	aliases, err := aliasesFor(control.JID)
	if err != nil {
		return nil, err
	}
	anchor, err := newestDiagnosticAnchor(store, control.JID)
	if err != nil {
		return nil, err
	}
	start := &diagnosticAttempt{Label: "control", Variant: "control_start", JID: control.JID, Aliases: aliases, Anchor: anchor, Result: "planned"}
	start.AnchorValidation = validateDiagnosticAnchor(store, start)
	if start.AnchorValidation != "stored_exact" && start.AnchorValidation != "stored_alias" {
		return nil, errors.New("invalid control anchor")
	}
	end := *start
	end.Variant = "control_end"
	attempts := append([]*diagnosticAttempt{start}, recovery...)
	attempts = append(attempts, &end)
	for index, attempt := range attempts {
		attempt.Number = index + 1
	}
	return attempts, nil
}
