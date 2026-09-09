package orchestrator

import (
	"errors"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
)

type failureBreakerBackendError struct {
	body string
}

func (e failureBreakerBackendError) Error() string {
	return "backend failed"
}

func (e failureBreakerBackendError) BackendErrorBody() string {
	return e.body
}

func TestProjectAttemptFailureClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		err           error
		terminalState store.WorkAttemptTerminalState
		errorClass    string
		errorMessage  string
		want          string
	}{
		{
			name:          "token ceiling ignores changing counters",
			err:           errors.New("session token ceiling exceeded: total_tokens=16000001 ceiling_tokens=16000000"),
			terminalState: store.WorkAttemptTerminalFailure,
			want:          projectFailureClassSessionTokenCeiling,
		},
		{
			name:          "legacy deliverable command names the failing command",
			err:           errors.New("deliverable command failed (gh pr create): exit status 1"),
			terminalState: store.WorkAttemptTerminalFailure,
			want:          projectFailureClassDeliverableCommand + ":gh pr create",
		},
		{
			name: "structured deliverable command names the failing command",
			err: &runpkg.DeliverableCommandError{
				Operation: "codex_apps/github.create_pull_request",
				Arguments: `{"head":"detent/acme_widgets_18"}`,
				Status:    "failed",
				Message:   "HTTP 503: unavailable",
			},
			terminalState: store.WorkAttemptTerminalFailure,
			want:          projectFailureClassDeliverableCommand + ":codex_apps/github.create_pull_request",
		},
		{
			name: "published push later failure has distinct class",
			err: &runpkg.DeliverableCommandError{
				OperationClass: "post_push",
				Operation:      "post-push command",
				Status:         "failed",
			},
			terminalState: store.WorkAttemptTerminalFailure,
			errorClass:    workAttemptErrorPostPushCommand,
			want:          projectFailureClassDeliverableCommand + ":post-push command",
		},
		{
			name:          "backend body is hashed",
			err:           failureBreakerBackendError{body: `{"code":"overloaded"}`},
			terminalState: store.WorkAttemptTerminalFailure,
			want:          projectFailureClassBackendError + ":" + projectFailureHash(`{"code":"overloaded"}`),
		},
		{
			name:          "durable error class is retained",
			terminalState: store.WorkAttemptTerminalNoProgress,
			errorClass:    "spend_since_progress_circuit_breaker",
			want:          "spend_since_progress_circuit_breaker",
		},
		{
			name:          "startup timeout shares systemic startup class",
			terminalState: store.WorkAttemptTerminalTimedOut,
			errorClass:    backendcapacity.StartupTimeoutErrorClass,
			want:          backendcapacity.StartupFailureErrorClass,
		},
		{
			name:          "startup exit keeps systemic startup class",
			terminalState: store.WorkAttemptTerminalFailure,
			errorClass:    backendcapacity.StartupFailureErrorClass,
			want:          backendcapacity.StartupFailureErrorClass,
		},
		{
			name:          "no progress has stable class",
			terminalState: store.WorkAttemptTerminalNoProgress,
			want:          projectFailureClassNoProgress,
		},
		{
			name:          "final state is hashed",
			terminalState: store.WorkAttemptTerminalFailure,
			errorMessage:  "failed",
			want:          projectFailureClassRunnerFinalState + ":" + projectFailureHash("failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := projectAttemptFailureClass(tt.err, tt.terminalState, tt.errorClass, tt.errorMessage); got != tt.want {
				t.Fatalf("projectAttemptFailureClass() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestProjectFailureBreakerCapturesOperatorEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 21, 0, 0, 0, time.UTC)
	resetAt := now.Add(39 * time.Minute)
	issue := connector.Issue{
		ID:         "video-1",
		Identifier: "2026-07-10-detent-not-vibe-coding-short",
		URL:        "https://example.test/items/video-1",
		Title:      "Author beat visuals",
		State:      "In Progress",
	}
	scope := backendcapacity.Scope{BackendID: "claude-code", BackendKind: "claude_code", Provider: "anthropic"}
	state := newState(normalizeConfig(Config{FailureBreaker: FailureBreakerConfig{SameClassLimit: 1, Window: time.Hour, Cooldown: time.Hour}}))
	state.Running[issue.ID] = Running{
		Issue:           issue,
		CapacityScope:   scope,
		RuntimeIdentity: agentidentity.Configured("claude-code", "claude_code", "default", "code", "claude-opus", "anthropic", "high", "", now),
	}
	capacityErr := backendcapacity.NewError(scope, backendcapacity.Details{
		Type: backendcapacity.ErrorTypeUsageLimit, Kind: "usage_limit_exceeded", Reason: "provider usage limit reached", ResetAt: &resetAt,
	}, errors.New("You've hit your limit. Try again at 9:39 PM"))

	orch := &Orchestrator{}
	orch.recordProjectAttemptOutcome(&state, issue.ID, now, store.WorkAttemptTerminalFailure, capacityErr, backendcapacity.ErrorClass, capacityErr.Error())

	failures := state.FailureBreaker.Failures[backendcapacity.ErrorClass]
	if len(failures) != 1 {
		t.Fatalf("Failures = %#v, want one", state.FailureBreaker.Failures)
	}
	failure := failures[0]
	if failure.Identifier != issue.Identifier || failure.IssueURL != issue.URL || failure.Title != issue.Title {
		t.Fatalf("issue evidence = %#v", failure)
	}
	if failure.Cause != "provider usage limit reached" || failure.ErrorMessage != "You've hit your limit. Try again at 9:39 PM" {
		t.Fatalf("cause/error = %q/%q", failure.Cause, failure.ErrorMessage)
	}
	if failure.BackendID != scope.BackendID || failure.BackendKind != scope.BackendKind || failure.Provider != scope.Provider {
		t.Fatalf("backend evidence = %#v", failure)
	}
}

func TestProjectFailureBreakerMixedSuccessStreamDoesNotTrip(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
	orch := &Orchestrator{}
	state := State{FailureBreaker: newProjectFailureBreaker(FailureBreakerConfig{
		SameClassLimit: 3,
		Window:         time.Hour,
		Cooldown:       time.Minute,
	})}

	for index := range 6 {
		at := base.Add(time.Duration(index) * time.Minute)
		orch.recordProjectAttemptOutcome(&state, "issue", at, store.WorkAttemptTerminalFailure, errors.New("same error"), workAttemptErrorRunner, "same error")
		orch.recordProjectAttemptOutcome(&state, "issue", at.Add(time.Second), store.WorkAttemptTerminalSuccess, nil, "", "")
	}

	if state.FailureBreaker.Active() {
		t.Fatalf("FailureBreaker = %#v, want inactive", state.FailureBreaker)
	}
	if len(state.FailureBreaker.Failures) != 0 {
		t.Fatalf("Failures = %#v, want cleared by successful yield", state.FailureBreaker.Failures)
	}
}

func TestProjectFailureBreakerPrunesFailuresOutsideWindow(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
	orch := &Orchestrator{}
	state := State{FailureBreaker: newProjectFailureBreaker(FailureBreakerConfig{
		SameClassLimit: 2,
		Window:         time.Minute,
		Cooldown:       time.Minute,
	})}

	orch.recordProjectAttemptOutcome(&state, "issue-1", base, store.WorkAttemptTerminalFailure, errors.New("same error"), workAttemptErrorRunner, "same error")
	orch.recordProjectAttemptOutcome(&state, "issue-2", base.Add(2*time.Minute), store.WorkAttemptTerminalFailure, errors.New("same error"), workAttemptErrorRunner, "same error")

	if state.FailureBreaker.Active() {
		t.Fatalf("FailureBreaker = %#v, want failures outside window not to trip", state.FailureBreaker)
	}
	for _, failures := range state.FailureBreaker.Failures {
		if len(failures) != 1 {
			t.Fatalf("Failures = %#v, want one failure inside rolling window", state.FailureBreaker.Failures)
		}
	}
}

func TestProjectFailureBreakerCanaryStateMachine(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
	config := FailureBreakerConfig{SameClassLimit: 2, Window: time.Hour, Cooldown: time.Minute}
	orch := &Orchestrator{cfg: Config{FailureBreaker: config}, now: func() time.Time { return base }}
	state := State{FailureBreaker: newProjectFailureBreaker(config)}

	orch.recordProjectAttemptOutcome(&state, "issue-1", base, store.WorkAttemptTerminalFailure, errors.New("same error"), workAttemptErrorRunner, "same error")
	orch.recordProjectAttemptOutcome(&state, "issue-2", base.Add(time.Second), store.WorkAttemptTerminalFailure, errors.New("same error"), workAttemptErrorRunner, "same error")
	if !state.FailureBreaker.Active() {
		t.Fatal("FailureBreaker.Active() = false, want true")
	}
	if projectFailureBreakerAllowsDispatch(&state, base.Add(30*time.Second)) {
		t.Fatal("projectFailureBreakerAllowsDispatch() = true during cooldown")
	}

	canaryAt := state.FailureBreaker.ResumeAt
	if !projectFailureBreakerAllowsDispatch(&state, canaryAt) {
		t.Fatal("projectFailureBreakerAllowsDispatch() = false when canary is due")
	}
	canary, allowed := tryReserveProjectFailureBreakerCanary(&state, "canary-1", canaryAt)
	if !canary || !allowed {
		t.Fatalf("tryReserveProjectFailureBreakerCanary() = (%t, %t), want (true, true)", canary, allowed)
	}
	if projectFailureBreakerAllowsDispatch(&state, canaryAt) {
		t.Fatal("second dispatch allowed while canary is running")
	}
	orch.reloadProjectFailureBreaker(&state, config, canaryAt)
	if projectFailureBreakerAllowsDispatch(&state, canaryAt) {
		t.Fatal("workflow reload allowed a second dispatch while canary is running")
	}

	orch.recordProjectAttemptOutcome(&state, "canary-1", canaryAt.Add(time.Second), store.WorkAttemptTerminalFailure, errors.New("same error"), workAttemptErrorRunner, "same error")
	if got := state.FailureBreaker.ResumeAt; got != canaryAt.Add(time.Second+config.Cooldown) {
		t.Fatalf("ResumeAt = %s, want full cooldown through %s", got, canaryAt.Add(time.Second+config.Cooldown))
	}
	if state.FailureBreaker.CanaryIssueID != "" {
		t.Fatalf("CanaryIssueID = %q, want cleared after retrip", state.FailureBreaker.CanaryIssueID)
	}

	reloadAt := canaryAt.Add(10 * time.Second)
	orch.reloadProjectFailureBreaker(&state, config, reloadAt)
	if !projectFailureBreakerAllowsDispatch(&state, reloadAt) {
		t.Fatal("workflow reload did not make one canary eligible")
	}
	canary, allowed = tryReserveProjectFailureBreakerCanary(&state, "canary-2", reloadAt)
	if !canary || !allowed {
		t.Fatalf("tryReserveProjectFailureBreakerCanary() after reload = (%t, %t), want (true, true)", canary, allowed)
	}
	orch.recordProjectAttemptOutcome(&state, "canary-2", reloadAt.Add(time.Second), store.WorkAttemptTerminalFailure, errors.New("different error"), workAttemptErrorRunner, "different error")
	if state.FailureBreaker.Active() {
		t.Fatalf("FailureBreaker = %#v, want different class to close it", state.FailureBreaker)
	}

	successAt := reloadAt.Add(2 * time.Second)
	orch.recordProjectAttemptOutcome(&state, "issue-3", successAt, store.WorkAttemptTerminalFailure, errors.New("same error"), workAttemptErrorRunner, "same error")
	orch.recordProjectAttemptOutcome(&state, "issue-4", successAt.Add(time.Second), store.WorkAttemptTerminalFailure, errors.New("same error"), workAttemptErrorRunner, "same error")
	if !state.FailureBreaker.Active() {
		t.Fatal("FailureBreaker.Active() = false before success canary")
	}
	orch.recordProjectAttemptOutcome(&state, "in-flight-success", successAt.Add(2*time.Second), store.WorkAttemptTerminalSuccess, nil, "", "")
	if !state.FailureBreaker.Active() {
		t.Fatal("FailureBreaker.Active() = false after non-canary success")
	}
	canaryAt = state.FailureBreaker.ResumeAt
	canary, allowed = tryReserveProjectFailureBreakerCanary(&state, "canary-success", canaryAt)
	if !canary || !allowed {
		t.Fatalf("tryReserveProjectFailureBreakerCanary() for success = (%t, %t), want (true, true)", canary, allowed)
	}
	orch.recordProjectAttemptOutcome(&state, "canary-success", canaryAt, store.WorkAttemptTerminalSuccess, nil, "", "")
	if state.FailureBreaker.Active() {
		t.Fatalf("FailureBreaker = %#v, want success to close it", state.FailureBreaker)
	}
}

func TestIssueConfigurationDoesNotTripProjectBreaker(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		err   error
		class string
	}{
		{name: "typed", err: &runpkg.IssueConfigurationError{Field: "effort", Reason: "normal is unsupported"}},
		{name: "restored", class: "issue_configuration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := normalizeConfig(Config{FailureBreaker: FailureBreakerConfig{SameClassLimit: 2, Window: time.Hour, Cooldown: time.Hour}})
			state := newState(cfg)
			orch := &Orchestrator{cfg: cfg}
			for range 6 {
				orch.recordProjectAttemptOutcome(&state, "invalid-issue", time.Now(), store.WorkAttemptTerminalFailure, tc.err, tc.class, "invalid effort normal")
			}
			if state.FailureBreaker.Active() || len(state.FailureBreaker.Failures) != 0 {
				t.Fatalf("issue validation poisoned project breaker: %#v", state.FailureBreaker)
			}
		})
	}
}
