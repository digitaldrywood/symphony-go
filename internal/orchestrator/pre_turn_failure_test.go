package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func TestPreTurnFailuresDrainInstance(t *testing.T) {
	t.Parallel()
	startup := backendcapacity.NewError(backendcapacity.Scope{}, backendcapacity.Details{Type: backendcapacity.ErrorTypeTransientOverload, Kind: backendcapacity.StartupTimeoutKind}, context.DeadlineExceeded)
	protocol := errors.New("codex turn/start: JSON-RPC -32600 invalid request")
	workspace := fmt.Errorf("%w: after_create exited 1", runpkg.ErrWorkspacePreparation)
	for _, tt := range []struct {
		name     string
		failures []error
	}{
		{"protocol", []error{protocol, protocol, protocol}},
		{"workspace", []error{workspace, workspace, workspace}},
		{"startup", []error{startup, startup, startup}},
		{"mixed", []error{protocol, startup, workspace}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
			cfg := normalizeConfig(Config{ActiveStates: []string{"Todo", "In Progress", "Rework"}, BlockedRecovery: BlockedRecoveryConfig{BreakerCooldown: time.Minute}})
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{}}
			o := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: &terminalRetryWorkAttemptStore{}}
			state := newState(cfg)
			for index, failure := range tt.failures {
				issue := terminalRetryTestIssue(strconv.Itoa(index))
				tracker.issues[issue.ID] = issue
				state.Running[issue.ID] = Running{Issue: issue, Attempt: 1, WorkAttemptID: int64(index + 1), DispatchSourceState: "Todo", DispatchTargetState: "In Progress", StartedAt: now.Add(-time.Second)}
				o.handleRunResult(t.Context(), &state, runpkg.Completion{IssueID: issue.ID, Err: failure, CompletedAt: now})
				if tracker.issues[issue.ID].State != "Todo" || len(state.Blocked) != 0 || len(tracker.comments) != 0 || len(state.Retry) != 0 || len(state.PriorAttempts) != 0 {
					t.Fatalf("pre-turn failure changed issue accounting: state=%s blocked=%v comments=%v retries=%v", tracker.issues[issue.ID].State, state.Blocked, tracker.comments, state.Retry)
				}
				if allowed := projectFailureBreakerAllowsDispatch(&state, now); allowed != (index < 2) {
					t.Fatalf("dispatch allowed after failure %d = %v", index+1, allowed)
				}
			}
			rows := projectFailureBreakerSnapshots(state)
			if len(rows) != 1 || rows[0].RepresentativeError == "" || rows[0].Count != 3 || !rows[0].InstanceDrained || rows[0].CooldownSeconds != 60 {
				t.Fatalf("breaker health evidence = %#v", rows)
			}
			if clearProjectFailureBreakerIssue(&state.FailureBreaker, "issue-0") {
				t.Fatal("moving an issue cleared instance evidence")
			}
			if !state.FailureBreaker.ResumeAt.Equal(now.Add(time.Minute)) || !projectFailureBreakerAllowsDispatch(&state, now.Add(time.Minute)) {
				t.Fatal("cooldown did not resume dispatch")
			}
			reserved, allowed := tryReserveProjectFailureBreakerCanary(&state, "canary", now.Add(time.Minute))
			if !reserved || !allowed || projectFailureBreakerAllowsDispatch(&state, now.Add(time.Minute)) {
				t.Fatal("cooldown did not admit exactly one canary")
			}
			if !clearProjectFailureBreakerIssue(&state.FailureBreaker, "canary") || !state.FailureBreaker.Active() {
				t.Fatal("moving canary did not release reservation while preserving drain")
			}
			reserved, allowed = tryReserveProjectFailureBreakerCanary(&state, "canary", now.Add(time.Minute))
			if !reserved || !allowed {
				t.Fatal("moved canary retained reservation")
			}
			state.Running["canary"] = Running{Issue: connector.Issue{ID: "canary"}}
			o.handleRunUpdate(&state, runUpdate{issueID: "canary", usage: runpkg.UsageUpdate{SessionID: "thread-only"}})
			if !state.FailureBreaker.Active() {
				t.Fatal("thread creation cleared pre-turn breaker")
			}
			o.handleRunUpdate(&state, runUpdate{issueID: "canary", usage: runpkg.UsageUpdate{TurnCount: 1}})
			if state.FailureBreaker.Active() {
				t.Fatal("first turn did not close canary")
			}

		})
	}
}

func TestPreTurnAttemptTaxonomy(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		class   string
		metrics string
		want    bool
	}{
		{"runner before turn", workAttemptErrorRunner, `{"turns":0}`, true},
		{"runner after turn", workAttemptErrorRunner, `{"turns":1}`, false},
		{"runner used tokens", workAttemptErrorRunner, `{"turns":0,"total_tokens":5}`, false},
		{"unknown historical turns", workAttemptErrorRunner, `{}`, false},
		{"missing historical turns", workAttemptErrorRunner, "", false},
		{"malformed metrics", workAttemptErrorRunner, `{`, false},
		{"workspace hook", workAttemptErrorWorkspace, "", true},
		{"startup timeout", backendcapacity.StartupTimeoutErrorClass, "", true},
		{"startup exit", backendcapacity.StartupFailureErrorClass, "", true},
		{"provider capacity", backendcapacity.ErrorClass, `{"turns":0}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := preTurnAttempt(telemetry.WorkAttempt{ErrorClass: tt.class, MetricsJSON: tt.metrics}); got != tt.want {
				t.Fatalf("pre-turn = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPreTurnRestoresDispatchLane(t *testing.T) {
	t.Parallel()
	for _, lane := range []string{"Todo", "Rework", "Merging"} {
		t.Run(lane, func(t *testing.T) {
			t.Parallel()
			issue := terminalRetryTestIssue("lane")
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{issue.ID: issue}}
			cfg := normalizeConfig(Config{ActiveStates: []string{"Todo", "Rework", "Merging", "In Progress"}})
			o := &Orchestrator{cfg: cfg, connector: tracker}
			state := newState(cfg)
			running := Running{Issue: issue, DispatchSourceState: lane, DispatchTargetState: "In Progress"}
			o.restorePreTurnIssue(t.Context(), &state, running, time.Now())
			if tracker.issues[issue.ID].State != lane || len(tracker.comments) != 0 {
				t.Fatalf("issue = %#v, comments = %v", tracker.issues[issue.ID], tracker.comments)
			}
		})
	}
}
