package main

import (
	"errors"
	"time"
)

func selectAliasDiagnosticAttempts(store *MessageStore, previous *diagnosticReport, aliasesFor func(string) ([]string, error)) ([]*diagnosticAttempt, error) {
	if previous.Status != "finished" || len(previous.Attempts) > maxDiagnosticAttempts {
		return nil, errors.New("prior experiment must be finished")
	}
	var control *diagnosticAttempt
	for _, attempt := range previous.Attempts {
		if attempt.Variant == "control_end" {
			control = attempt
		}
	}
	if control == nil || control.Finished == nil || control.Finished.Before(time.Now().Add(-20*time.Minute)) || (control.Result != "progress_observed" && control.Result != "no_backward_progress_observed" && control.Result != "phone_reports_no_older_messages" && control.Result != "older_messages_not_accessible") {
		return nil, errors.New("recent responsive closing control required")
	}
	var attempts []*diagnosticAttempt
	for _, source := range previous.Attempts {
		if len(attempts) == 3 {
			break
		}
		if source.Variant != "failed_boundary" || source.Result != "response_timeout" || source.Acknowledged == nil {
			continue
		}
		aliases, err := aliasesFor(source.JID)
		if err != nil {
			return nil, err
		}
		for _, alias := range aliases {
			if alias == source.JID || !containsIdentifier(source.Aliases, alias) {
				continue
			}
			candidate := &diagnosticAttempt{Number: len(attempts) + 1, Label: source.Label, Variant: "alternate_alias", JID: alias, Aliases: aliases, Anchor: source.Anchor, Result: "planned"}
			candidate.AnchorValidation = validateDiagnosticAnchor(store, candidate)
			if candidate.AnchorValidation != "stored_exact" && candidate.AnchorValidation != "stored_alias" {
				continue
			}
			attempts = append(attempts, candidate)
			break
		}
	}
	if len(attempts) == 0 {
		return nil, errors.New("no verified alternate aliases")
	}
	anchor, err := newestDiagnosticAnchor(store, control.JID)
	if err != nil {
		return nil, err
	}
	closing := &diagnosticAttempt{Number: len(attempts) + 1, Label: "control", Variant: "control_end", JID: control.JID, Aliases: control.Aliases, Anchor: anchor, Result: "planned"}
	closing.AnchorValidation = validateDiagnosticAnchor(store, closing)
	if closing.AnchorValidation != "stored_exact" && closing.AnchorValidation != "stored_alias" {
		return nil, errors.New("invalid closing control anchor")
	}
	return append(attempts, closing), nil
}
