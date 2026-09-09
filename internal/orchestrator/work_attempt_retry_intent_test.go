package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	"github.com/digitaldrywood/detent/internal/providercapacity"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestDurableRecoveryConvergesThroughPublicAPI(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"request", "intent recorded", "tracker applied", "retry queued"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := t.Context()
			dbPath := filepath.Join(t.TempDir(), "recovery.db")
			db := openCompletionDeferralStoreWithoutCleanup(t, dbPath)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			now := time.Now().UTC()
			issue := recoveryTestIssue()
			issue.State, issue.Title = "Blocked", "Correct invalid effort"
			issue.Description = "```detent-agent\nschema: 1\neffort: low\n```"
			issue.AssignedToWorker = true
			tracker := memory.New(memory.Config{Issues: []connector.Issue{issue}, Stateful: true})
			runner := newClaimBlockingRunner()
			newHost := func() *Orchestrator {
				host, err := New(Config{PollInterval: time.Hour, MaxConcurrentAgents: 1,
					Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"Todo", "In Progress"},
					ObservedStates: []string{"Blocked"}, TerminalStates: []string{"Done"}},
					Dependencies{Connector: tracker, Runner: runner, WorkAttempts: db, WorkflowMetrics: db})
				if err != nil {
					t.Fatal(err)
				}
				return host
			}
			host := newHost()
			var attemptID int64
			for index := range 6 {
				at := now.Add(time.Duration(index-10) * time.Minute)
				attemptID = startRecoveryWorkAttempt(t, ctx, db, issue, store.WorkAttemptStatusActive, "", at)
				if err := db.CompleteWorkAttempt(ctx, store.WorkAttemptCompletion{AttemptID: attemptID, CompletedAt: at.Add(time.Second),
					Status: store.WorkAttemptStatusTerminal, TerminalState: store.WorkAttemptTerminalFailure,
					ErrorClass: "runner_error", ErrorMessage: "agent override rejected: effort: explicit effort is unsupported by the selected model"}); err != nil {
					t.Fatal(err)
				}
			}
			parked := cloneIssue(issue)
			parked.State = "In Progress"
			host.recordLaneTransition(ctx, parked, "Blocked", now.Add(-time.Minute), repeatedFailureCircuitBreakerCause,
				workflowLaneMetadata{BlockedRecovery: &workflowLaneBlockedRecoveryMetadata{Owner: "human", Cause: repeatedFailureCircuitBreakerCause, CauseFingerprint: "invalid-effort-generation"}})
			request := WorkAttemptRecoveryRequest{AttemptID: attemptID, Action: WorkAttemptRecoveryRetryFresh}
			if boundary != "request" {
				receipt, err := host.workAttemptRecoveryReceipt(ctx, "detent", attemptID, now)
				if err != nil {
					t.Fatal(err)
				}
				intent, err := host.prepareWorkAttemptRetryIntent(ctx, issue, request, receipt, now)
				if err != nil {
					t.Fatal(err)
				}
				if boundary == "retry queued" {
					state := newState(host.cfg)
					response, err := host.applyWorkAttemptRetryIntent(ctx, &state, issue, intent, receipt, now)
					if err != nil || response.Status != "queued" {
						t.Fatalf("queue before restart: %#v, %v", response, err)
					}
				}
				if boundary == "tracker applied" {
					if err := tracker.UpdateIssueState(ctx, issue.ID, "Todo"); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db = openCompletionDeferralStoreWithoutCleanup(t, dbPath)
				host = newHost()
			}
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- host.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("Run: %v", err)
				}
			})
			var requests sync.WaitGroup
			for range 4 {
				requests.Go(func() {
					response, err := host.RecoverWorkAttempt(ctx, request)
					if err != nil || response.Status != "queued" && response.Status != "running" {
						t.Errorf("recovery: status=%s blockers=%v err=%v", response.Status, response.Blockers, err)
					}
				})
			}
			requests.Wait()
			select {
			case <-runner.started:
			case <-ctx.Done():
				state, _ := host.State(context.Background())
				t.Fatalf("recovery did not dispatch: blocked=%v retry=%v decisions=%v", state.Blocked, state.Retry, state.SchedulerDecisions)
			}
			select {
			case extra := <-runner.started:
				t.Fatalf("duplicate worker: %#v", extra)
			default:
			}
			active, err := db.ListActiveWorkAttempts(ctx, store.WorkAttemptQuery{ProjectID: "detent"})
			if err != nil || len(active) != 1 {
				t.Fatalf("active attempts = %d, %v", len(active), err)
			}
			receipt, err := host.WorkAttemptReceipt(ctx, "detent", request.AttemptID)
			if err != nil || receipt.Status != "running" || receipt.CurrentAttemptID != active[0].ID || receipt.AuditEventID != 0 {
				t.Fatalf("current receipt = %#v, %v", receipt, err)
			}
		})
	}
}

func TestDurableRecoveryPreservesCurrentPredicates(t *testing.T) {
	t.Parallel()
	for _, condition := range []string{"dependency", "budget", "project outage", "stale tracker", "new invalid override", "multiple causes"} {
		t.Run(condition, func(t *testing.T) {
			db := openWorkAttemptRecoveryStore(t, t.Context())
			host := newWorkAttemptRecoveryOrchestrator(t, db, nil)
			issue := recoveryTestIssue()
			now := time.Now().UTC()
			issue.UpdatedAt = &now
			attemptID := startRecoveryWorkAttempt(t, t.Context(), db, issue, store.WorkAttemptStatusTerminal, store.WorkAttemptTerminalFailure, now.Add(-time.Hour))
			receipt, err := host.workAttemptRecoveryReceipt(t.Context(), "detent", attemptID, now)
			if err != nil {
				t.Fatal(err)
			}
			intent, err := host.prepareWorkAttemptRetryIntent(t.Context(), issue, WorkAttemptRecoveryRequest{AttemptID: attemptID, Action: WorkAttemptRecoveryRetryFresh}, receipt, now)
			if err != nil {
				t.Fatal(err)
			}
			state := newState(host.cfg)
			switch condition {
			case "dependency":
				issue.BlockedBy = []connector.BlockedRef{{Identifier: "digitaldrywood/detent#1", State: "Todo"}}
			case "budget":
				state.BudgetRefusals[issue.ID] = BudgetRefusal{Issue: issue, Code: "per_issue_max_usd"}
			case "project outage":
				state.FailureBreaker.Class = "backend_startup_timeout"
				state.FailureBreaker.ResumeAt = now.Add(time.Hour)
			case "stale tracker":
				old := now.Add(-time.Hour)
				issue.UpdatedAt = &old
			case "new invalid override":
				issue.Description = "```detent-agent\nschema: 2\n```"
			case "multiple causes":
				issue.BlockedBy = []connector.BlockedRef{{Identifier: "digitaldrywood/detent#1", State: "Todo"}}
				state.BudgetRefusals[issue.ID] = BudgetRefusal{Issue: issue, Code: "per_issue_max_usd"}
				state.FailureBreaker.Class = "backend_startup_timeout"
				state.FailureBreaker.ResumeAt = now.Add(time.Hour)
			}
			for range 2 {
				response, err := host.applyWorkAttemptRetryIntent(t.Context(), &state, issue, intent, receipt, now)
				if err != nil || response.Status != "blocked" || len(response.Blockers) == 0 {
					t.Fatalf("%s recovery: %#v, %v", condition, response, err)
				}
				if condition == "multiple causes" && len(response.Blockers) < 3 {
					t.Fatalf("response omitted current blockers: %v", response.Blockers)
				}
				if _, queued := state.Retry[issue.ID]; queued {
					t.Fatal("queued worker despite an independent predicate")
				}
				host.dispatchReadyIssues(t.Context(), &state, []connector.Issue{issue}, now)
				if len(state.Running) != 0 {
					t.Fatal("scheduler dispatched despite an independent predicate")
				}
			}
		})
	}
}

type failingRecoveryJournal struct {
	store.Store
}

func (s failingRecoveryJournal) RecordWorkflowPhaseEvent(ctx context.Context, event store.WorkflowPhaseEvent) (int64, error) {
	if event.PhaseName == workAttemptRetryIntentPhase {
		return 0, errors.New("recovery journal unavailable")
	}
	return s.Store.RecordWorkflowPhaseEvent(ctx, event)
}

func TestRecoveryJournalFailureLeavesHoldsIntact(t *testing.T) {
	t.Parallel()
	db := openWorkAttemptRecoveryStore(t, t.Context())
	host := newWorkAttemptRecoveryOrchestrator(t, db, nil)
	host.workflowMetrics = failingRecoveryJournal{Store: db}
	issue := recoveryTestIssue()
	now := time.Now().UTC()
	attemptID := startRecoveryWorkAttempt(t, t.Context(), db, issue, store.WorkAttemptStatusTerminal, store.WorkAttemptTerminalFailure, now.Add(-time.Hour))
	state := newState(host.cfg)
	state.Blocked[issue.ID] = Blocked{Issue: issue, Source: BlockedSourceProjectStatus, Reason: "existing hold"}
	_, err := host.handleWorkAttemptRecovery(t.Context(), &state, WorkAttemptRecoveryRequest{AttemptID: attemptID, Action: WorkAttemptRecoveryRetryFresh}, now)
	if err == nil || !strings.Contains(err.Error(), "journal unavailable") {
		t.Fatalf("recovery error = %v", err)
	}
	if len(state.Retry) != 0 || state.Blocked[issue.ID].Reason != "existing hold" {
		t.Fatal("failed durable write mutated recovery state")
	}
}

func TestRecoveryIntentCannotAcknowledgeNewParkGeneration(t *testing.T) {
	t.Parallel()
	for _, cause := range []string{repeatedFailureCircuitBreakerCause, "operator_investigation"} {
		t.Run(cause, func(t *testing.T) {
			db := openWorkAttemptRecoveryStore(t, t.Context())
			host := newWorkAttemptRecoveryOrchestrator(t, db, nil)
			issue := recoveryTestIssue()
			now := time.Now().UTC().Truncate(time.Second)
			oldPark := workflowLaneBlockedRecoveryMetadata{Owner: "human", Cause: repeatedFailureCircuitBreakerCause, CauseFingerprint: "same-fingerprint"}
			intent := workAttemptRetryIntent{Schema: 1, IssueID: issue.ID, Request: WorkAttemptRecoveryRequest{AttemptID: 1, Action: WorkAttemptRecoveryRetryFresh}, Configuration: true, Park: &oldPark, ParkedAt: now.Add(-time.Hour), RecordedAt: now}
			data, err := json.Marshal(intent)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.RecordWorkflowPhaseEvent(t.Context(), store.WorkflowPhaseEvent{ProjectID: "detent", IssueID: issue.ID, PhaseType: store.WorkflowPhaseTypeRecovery, PhaseName: workAttemptRetryIntentPhase, Status: "queued", StartedAt: now, FinishedAt: now, MetadataJSON: string(data)}); err != nil {
				t.Fatal(err)
			}
			newPark := oldPark
			newPark.Cause = cause
			host.recordLaneTransition(t.Context(), issue, "Blocked", now.Add(time.Minute), cause, workflowLaneMetadata{BlockedRecovery: &newPark})
			issue.State = "Todo"
			state := newState(host.cfg)
			host.retainUnacknowledgedRecoveryParks(t.Context(), &state, []connector.Issue{issue})
			if blocked, held := state.Blocked[issue.ID]; !held || blocked.Reason != cause {
				t.Fatalf("new park was acknowledged by old intent: %#v", blocked)
			}
		})
	}
}

func TestRecoveryIntentDoesNotReplayAfterNewAttempt(t *testing.T) {
	t.Parallel()
	db := openWorkAttemptRecoveryStore(t, t.Context())
	host := newWorkAttemptRecoveryOrchestrator(t, db, nil)
	issue := recoveryTestIssue()
	now := time.Now().UTC()
	oldID := startRecoveryWorkAttempt(t, t.Context(), db, issue, store.WorkAttemptStatusTerminal, store.WorkAttemptTerminalFailure, now.Add(-time.Hour))
	state := newState(host.cfg)
	request := WorkAttemptRecoveryRequest{AttemptID: oldID, Action: WorkAttemptRecoveryRetryFresh}
	if _, err := host.handleWorkAttemptRecovery(t.Context(), &state, request, now); err != nil {
		t.Fatal(err)
	}
	newID := startRecoveryWorkAttempt(t, t.Context(), db, issue, store.WorkAttemptStatusTerminal, store.WorkAttemptTerminalFailure, now.Add(time.Minute))
	state = newState(host.cfg)
	host.restoreWorkAttemptRetryIntents(t.Context(), &state, []connector.Issue{issue}, now.Add(time.Hour))
	if len(state.Retry) != 0 {
		t.Fatal("old recovery intent replayed after newer attempt")
	}
	receipt, err := host.workAttemptRecoveryReceipt(t.Context(), "detent", oldID, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	response := host.describeWorkAttemptRecovery(t.Context(), &state, receipt)
	if response.Status != "superseded" || response.CurrentAttemptID != newID || response.Queued {
		t.Fatalf("receipt misreports finished recovery: %#v", response)
	}
}

func TestRecoveryReceiptReportsCurrentState(t *testing.T) {
	t.Parallel()
	for _, status := range []string{"queued", "blocked", "running"} {
		t.Run(status, func(t *testing.T) {
			db := openWorkAttemptRecoveryStore(t, t.Context())
			host := newWorkAttemptRecoveryOrchestrator(t, db, nil)
			issue := recoveryTestIssue()
			now := time.Now().UTC()
			attemptID := startRecoveryWorkAttempt(t, t.Context(), db, issue, store.WorkAttemptStatusTerminal, store.WorkAttemptTerminalFailure, now.Add(-time.Hour))
			state := newState(host.cfg)
			request := WorkAttemptRecoveryRequest{AttemptID: attemptID, Action: WorkAttemptRecoveryRetryFresh}
			if _, err := host.handleWorkAttemptRecovery(t.Context(), &state, request, now); err != nil {
				t.Fatal(err)
			}
			switch status {
			case "blocked":
				state.Blocked[issue.ID] = Blocked{Issue: issue, Source: BlockedSourceProjectStatus, Reason: "operator_recovery_pending", RecoveryRemedy: "dependency remains open"}
				state.BudgetRefusals[issue.ID] = BudgetRefusal{Issue: issue, Code: "per_issue_max_usd"}
				state.FailureBreaker.Class = "backend_startup_timeout"
				state.FailureBreaker.ResumeAt = now.Add(time.Hour)
			case "running":
				state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: attemptID + 1}
			}
			request.Action = WorkAttemptRecoveryInspect
			response, err := host.handleWorkAttemptRecovery(t.Context(), &state, request, now.Add(time.Minute))
			if err != nil || response.Status != status || response.NextAction == "" {
				t.Fatalf("receipt = %#v, %v", response, err)
			}
			if status == "blocked" && (len(response.Blockers) != 3 || !strings.Contains(response.Message, "dependency remains open")) {
				t.Fatalf("receipt omitted blockers: %#v", response)
			}
			if status == "running" && (response.CurrentAttemptID != attemptID+1 || response.Queued) {
				t.Fatalf("running receipt = %#v", response)
			}
		})
	}
}

type recoveryCapacityResolver struct {
	err error
}

func (r recoveryCapacityResolver) DispatchCapacity(context.Context, runpkg.RunRequest) (providercapacity.Requirement, error) {
	return providercapacity.Requirement{}, r.err
}

func TestRecoveryRevalidatesConfigurationBeforeRecordingIntent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		code WorkAttemptRecoveryErrorCode
	}{
		{name: "invalid effort", err: &runpkg.IssueConfigurationError{Field: "effort", Reason: "explicit effort is unsupported by the selected model"}, code: WorkAttemptRecoveryUnsupportedState},
		{name: "catalog outage", err: errors.New("catalog unavailable"), code: WorkAttemptRecoveryUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openWorkAttemptRecoveryStore(t, t.Context())
			host := newWorkAttemptRecoveryOrchestrator(t, db, nil)
			host.providerCapacity = recoveryCapacityResolver{err: tc.err}
			issue := recoveryTestIssue()
			now := time.Now().UTC()
			attemptID := startRecoveryWorkAttempt(t, t.Context(), db, issue, store.WorkAttemptStatusTerminal, store.WorkAttemptTerminalFailure, now.Add(-time.Hour))
			state := newState(host.cfg)
			request := WorkAttemptRecoveryRequest{AttemptID: attemptID, Action: WorkAttemptRecoveryRetryFresh}
			_, err := host.handleWorkAttemptRecovery(t.Context(), &state, request, now)
			var rejected *WorkAttemptRecoveryError
			if !errors.As(err, &rejected) || rejected.Code != tc.code {
				t.Fatalf("revalidation = %v, want %s", err, tc.code)
			}
			if _, found, err := host.latestWorkAttemptRetryIntent(t.Context(), issue); err != nil || found || len(state.Retry) != 0 {
				t.Fatalf("invalid recovery persisted intent: found=%v err=%v", found, err)
			}
			host.providerCapacity = recoveryCapacityResolver{}
			response, err := host.handleWorkAttemptRecovery(t.Context(), &state, request, now.Add(time.Minute))
			if err != nil || response.Status != "queued" {
				t.Fatalf("corrected recovery = %#v, %v", response, err)
			}
		})
	}
}

func TestConfigurationCompletionDoesNotRetryOrPoisonProject(t *testing.T) {
	t.Parallel()
	for _, form := range []string{"typed", "wrapped", "restored"} {
		t.Run(form, func(t *testing.T) {
			db := openWorkAttemptRecoveryStore(t, t.Context())
			host := newWorkAttemptRecoveryOrchestrator(t, db, nil)
			issue := recoveryTestIssue()
			now := time.Now().UTC()
			attemptID := startRecoveryWorkAttempt(t, t.Context(), db, issue, store.WorkAttemptStatusActive, "", now.Add(-time.Minute))
			state := newState(host.cfg)
			state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: attemptID, Attempt: 1, StartedAt: now.Add(-time.Minute)}
			var failure error = &runpkg.IssueConfigurationError{Field: "effort", Reason: "explicit effort is unsupported by the selected model"}
			switch form {
			case "wrapped":
				failure = fmt.Errorf("launch: %w", failure)
			case "restored":
				failure = errors.New(failure.Error())
			}
			host.handleRunResult(t.Context(), &state, runpkg.Completion{IssueID: issue.ID, Err: failure, CompletedAt: now})
			attempt, err := db.WorkAttempt(t.Context(), attemptID)
			if err != nil || attempt.ErrorClass != "issue_configuration" || len(state.Retry) != 0 || state.FailureBreaker.Active() {
				t.Fatalf("completion class=%s retry=%v breaker=%v err=%v", attempt.ErrorClass, state.Retry, state.FailureBreaker, err)
			}
			if state.Blocked[issue.ID].Reason != "issue_configuration" {
				t.Fatalf("missing actionable configuration hold: %v", state.Blocked)
			}
		})
	}
}

func TestInvalidConfigurationPreflightDoesNotSpendLaunchAttempts(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	db := openWorkAttemptRecoveryStore(t, ctx)
	issue := recoveryTestIssue()
	issue.Title = "Fix invalid override"
	issue.AssignedToWorker = true
	issue.Description = "```detent-agent\nschema: 2\n```"
	tracker := memory.New(memory.Config{Issues: []connector.Issue{issue}, Stateful: true})
	runner := newClaimBlockingRunner()
	host, err := New(Config{MaxConcurrentAgents: 1, Project: scheduler.ProjectCandidate{ID: "detent"}, ActiveStates: []string{"Todo", "In Progress"}}, Dependencies{Connector: tracker, Runner: runner, WorkAttempts: db, WorkflowMetrics: db})
	if err != nil {
		t.Fatal(err)
	}
	state := newState(host.cfg)
	now := time.Now().UTC()
	for range 6 {
		host.dispatchReadyIssues(ctx, &state, []connector.Issue{issue}, now)
	}
	if len(state.Running) != 0 || len(state.WorkAttempts) != 0 || state.FailureBreaker.Active() || state.Blocked[issue.ID].Reason != "issue_configuration" {
		t.Fatalf("invalid override consumed attempts: running=%v attempts=%v breaker=%v blocked=%v", state.Running, state.WorkAttempts, state.FailureBreaker, state.Blocked)
	}
	issue.Description = "```detent-agent\nschema: 1\neffort: low\n```"
	if err := tracker.UpdateIssueBody(ctx, issue.ID, issue.Description); err != nil {
		t.Fatal(err)
	}
	host.dispatchReadyIssues(ctx, &state, []connector.Issue{issue}, now.Add(time.Second))
	select {
	case <-runner.started:
	case <-time.After(5 * time.Second):
		t.Fatalf("corrected override did not dispatch: %v", state.Blocked)
	}
	cancel()
	select {
	case <-runner.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestConfigurationRecoveryOnlyClearsMatchingFailures(t *testing.T) {
	t.Parallel()
	for _, unrelated := range []bool{false, true} {
		t.Run(strconv.FormatBool(unrelated), func(t *testing.T) {
			now := time.Now().UTC()
			host := &Orchestrator{}
			state := newState(normalizeConfig(Config{FailureBreaker: FailureBreakerConfig{SameClassLimit: 2}}))
			state.FailureBreaker.Class = "runner_error"
			state.FailureBreaker.ResumeAt = now.Add(time.Hour)
			for range 6 {
				state.FailureBreaker.Failures["runner_error"] = append(state.FailureBreaker.Failures["runner_error"], ProjectFailure{IssueID: "bad", At: now, ErrorMessage: "agent override rejected: effort: explicit effort is unsupported by the selected model"})
			}
			if unrelated {
				for range 2 {
					state.FailureBreaker.Failures["runner_error"] = append(state.FailureBreaker.Failures["runner_error"], ProjectFailure{IssueID: "bad", At: now, ErrorMessage: "runner unavailable"})
				}
			}
			host.clearConfigurationFailureEvidence(&state, "bad", now)
			if state.FailureBreaker.Active() != unrelated {
				t.Fatalf("breaker active = %v", state.FailureBreaker.Active())
			}
			for _, failure := range state.FailureBreaker.Failures["runner_error"] {
				if strings.Contains(failure.ErrorMessage, "agent override rejected:") {
					t.Fatal("matching stale evidence retained")
				}
			}
		})
	}
}
