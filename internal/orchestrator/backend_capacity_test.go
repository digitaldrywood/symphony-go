package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/claudecode"
	"github.com/digitaldrywood/detent/internal/codex"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/connector/memory"
	"github.com/digitaldrywood/detent/internal/gate"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/workpad"
)

func TestCodexProviderOutagePausesAndRecoversWithoutParking(t *testing.T) {
	t.Parallel()

	for _, status := range []int{404, 500, 502, 503, 529, 599} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 9, 3, 19, 0, 0, 0, time.UTC)
			scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
			controller := backendCapacityTestController{scope: scope, hasStatus: true, status: runpkg.CapacityStatus{Available: true}}
			attempts := &terminalRetryWorkAttemptStore{}
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{}}
			cfg := normalizeConfig(Config{ActiveStates: []string{"Todo", "In Progress"}, FailureBreaker: FailureBreakerConfig{SameClassLimit: 2, Window: time.Hour, Cooldown: time.Hour}})
			orch := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts, capacityController: controller, capacityStatus: controller}
			state := newState(cfg)
			providerErr := &codex.TurnFailedError{Status: "failed", Body: fmt.Sprintf(`{"message":"unexpected status %d from responses endpoint"}`, status)}
			details, ok := codex.ClassifyCapacityError(providerErr, nil, now)
			if !ok || details.Type != backendcapacity.ErrorTypeProviderOutage {
				t.Fatalf("capacity classification = %#v, %v", details, ok)
			}
			for attempt := 1; attempt <= 30; attempt++ {
				issue := connector.Issue{ID: fmt.Sprintf("issue-%d", attempt%5), State: "In Progress"}
				tracker.issues[issue.ID] = issue
				state.Running[issue.ID] = Running{Issue: issue, Attempt: attempt, WorkAttemptID: int64(attempt), CapacityScope: scope, StartedAt: now.Add(-time.Second)}
				state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-time.Second)}
				orch.upsertWorkAttemptSnapshot(&state, telemetry.WorkAttempt{AttemptID: int64(attempt), IssueID: issue.ID, Status: string(store.WorkAttemptStatusActive), StartedAt: now.Add(-time.Second)})
				orch.handleRunResult(t.Context(), &state, runpkg.Completion{IssueID: issue.ID, Err: backendcapacity.NewError(scope, details, providerErr), CompletedAt: now})
			}
			if len(state.BackendOutages) != 1 || len(state.Retry) != 5 || len(state.Blocked) != 0 || state.FailureBreaker.Active() || len(state.FailureBreaker.Failures) != 0 || len(state.InstantFailures) != 0 || len(state.RepeatedFailures) != 0 {
				t.Fatalf("outage state = outages %#v retries %#v blocked %#v breaker %#v", state.BackendOutages, state.Retry, state.Blocked, state.FailureBreaker)
			}
			if len(attempts.completions) != 30 {
				t.Fatalf("completed attempts = %d, want 30", len(attempts.completions))
			}
			for _, attempt := range attempts.completions {
				if attempt.TerminalState != store.WorkAttemptTerminalCapacity || attempt.ErrorClass != backendcapacity.ErrorClass {
					t.Fatalf("completed attempt = %#v", attempt)
				}
			}
			request := runpkg.RunRequest{Issue: connector.Issue{ID: "probe", State: "In Progress"}}
			if _, _, paused := orch.backendCapacityDispatch(&state, request, now); !paused {
				t.Fatal("dispatch continued during provider outage")
			}
			outage := state.BackendOutages[scope.Key()]
			for probe := 1; probe <= 5; probe++ {
				probeAt := outage.NextProbeAt
				_, key, paused := orch.backendCapacityDispatch(&state, request, probeAt)
				if paused || key == "" {
					t.Fatal("due capacity probe did not become eligible")
				}
				orch.markBackendCapacityProbe(&state, key, request.Issue.ID, probeAt)
				if _, _, paused := orch.backendCapacityDispatch(&state, request, probeAt); !paused {
					t.Fatal("concurrent probe was allowed")
				}
				capacityErr, _ := backendcapacity.As(backendcapacity.NewError(scope, details, providerErr))
				outage = orch.registerBackendOutage(&state, capacityErr, probeAt, true)
				if want := probeAt.Add(backendCapacityProbeDelayForAttempt(probe)); !outage.NextProbeAt.Equal(want) {
					t.Fatalf("probe %d next time = %s, want %s", probe, outage.NextProbeAt, want)
				}
			}
			orch.recoverBackendCapacityFromStatus(&state, Running{CapacityScope: scope}, &telemetry.RateLimits{}, outage.LastObservedAt)
			if len(state.BackendOutages) != 1 {
				t.Fatal("quota status cleared an HTTP endpoint outage")
			}
			recoveredAt := outage.NextProbeAt
			orch.recoverBackendCapacity(&state, Running{CapacityScope: scope, CapacityProbe: true}, recoveredAt)
			if len(state.BackendOutages) != 0 || len(state.Blocked) != 0 || state.FailureBreaker.Active() {
				t.Fatal("successful probe left the provider or cards parked")
			}
			if _, _, paused := orch.backendCapacityDispatch(&state, request, recoveredAt); paused {
				t.Fatal("dispatch remained paused after recovery")
			}
			for _, retry := range state.Retry {
				if !retry.DueAt.Equal(recoveredAt) {
					t.Fatalf("capacity retry not released: %#v", retry)
				}
			}
		})
	}
}

func TestProductionClaudeSessionLimitRecordsOneCapacityOutage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 15, 20, 47, 49, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "claude-video", BackendKind: "claude_code", Provider: "anthropic"}
	cfg := normalizeConfig(Config{
		ActiveStates:   []string{"In Progress"},
		TerminalStates: []string{"Done"},
		FailureBreaker: FailureBreakerConfig{SameClassLimit: 5, Window: time.Hour, Cooldown: time.Hour},
	})
	orch := &Orchestrator{cfg: cfg}
	state := newState(cfg)
	message := "You've hit your session limit · resets 4:10pm (America/Chicago)"
	issue := connector.Issue{ID: "issue-capacity", State: "In Progress"}

	for attempt := 1; attempt <= 5; attempt++ {
		state.Running[issue.ID] = Running{Issue: issue, Attempt: attempt, StartedAt: now.Add(-time.Minute)}
		state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-time.Minute)}
		details, ok := claudecode.ClassifyCapacityError(errors.New(message), nil, now)
		if !ok {
			t.Fatalf("attempt %d did not classify production output as capacity", attempt)
		}
		capacityErr := backendcapacity.NewError(scope, details, errors.New(message))
		orch.handleRunResult(t.Context(), &state, runpkg.Completion{
			IssueID:     issue.ID,
			Request:     runpkg.RunRequest{Issue: issue, Attempt: attempt},
			Err:         capacityErr,
			CompletedAt: now,
		})
	}

	if len(state.BackendOutages) != 1 {
		t.Fatalf("BackendOutages = %#v, want one scoped outage", state.BackendOutages)
	}
	if state.FailureBreaker.Active() || len(state.FailureBreaker.Failures) != 0 {
		t.Fatalf("FailureBreaker = %#v, want no capacity strikes", state.FailureBreaker)
	}
	if len(state.InstantFailures) != 0 || len(state.RepeatedFailures) != 0 {
		t.Fatalf("issue failure breakers = instant %#v repeated %#v, want no capacity strikes", state.InstantFailures, state.RepeatedFailures)
	}
}

func TestSubscriptionStatusArmsOutageBeforeCompletion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 19, 16, 30, 43, 0, time.UTC)
	resetAt := time.Date(2026, 8, 20, 15, 27, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	controller := backendCapacityTestController{
		scope: scope,
		status: runpkg.CapacityStatus{
			Exhausted: true,
			Detail:    "live provider status reports an exhausted subscription window",
			Details: backendcapacity.Details{
				Type:    backendcapacity.ErrorTypeUsageLimit,
				Kind:    "subscription_window_exhausted",
				Reason:  "subscription window exhausted",
				ResetAt: &resetAt,
			},
		},
		hasStatus: true,
	}
	orch := &Orchestrator{capacityController: controller, capacityStatus: controller, now: func() time.Time { return now }}
	state := newState(normalizeConfig(Config{}))
	issue := connector.Issue{ID: "issue-live-exhaustion", State: "In Progress"}
	state.Running[issue.ID] = Running{Issue: issue, CapacityScope: scope, StartedAt: now.Add(-time.Minute)}

	orch.handleRunUpdate(&state, runUpdate{
		issueID: issue.ID,
		usage: runpkg.UsageUpdate{
			LastEventAt: now,
			RateLimits: &telemetry.RateLimits{
				ReachedType: "primary",
				Primary: &telemetry.RateLimitBucket{
					Status:  telemetry.RateLimitStatusExhausted,
					ResetAt: &resetAt,
				},
			},
		},
	})

	outage, ok := state.BackendOutages[scope.Key()]
	if !ok || outage.Kind != "subscription_window_exhausted" || outage.Reason != "subscription window exhausted" || !outage.ResetAt.Equal(resetAt) {
		t.Fatalf("BackendOutages = %#v, want named subscription outage reset at %s", state.BackendOutages, resetAt)
	}
	if _, _, paused := orch.backendCapacityDispatch(&state, runpkg.RunRequest{Issue: connector.Issue{ID: "issue-next"}}, now); !paused {
		t.Fatal("backendCapacityDispatch() paused = false after rejected subscription status")
	}
	message := backendCapacityStatusMessage(outage)
	if !strings.Contains(message, "backend codex: subscription window exhausted") || !strings.Contains(message, "resumes ~2026-08-20 15:27 UTC") {
		t.Fatalf("backendCapacityStatusMessage() = %q", message)
	}

	orch.capacityStatus = backendCapacityTestController{
		scope:     scope,
		status:    runpkg.CapacityStatus{Exhausted: true},
		hasStatus: true,
	}
	orch.recoverBackendCapacityFromStatus(&state, state.Running[issue.ID], &telemetry.RateLimits{}, now.Add(time.Minute))
	outage = state.BackendOutages[scope.Key()]
	if outage.Kind != "subscription_window_exhausted" || outage.Reason != "subscription window exhausted" {
		t.Fatalf("defaulted BackendOutage = %#v, want named subscription outage", outage)
	}
}

func TestSlippedSubscriptionAttemptsFinishAsCapacityBeforeSessionBrakes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 19, 20, 0, 0, 0, time.UTC)
	resetAt := now.Add(12 * time.Hour)
	scope := backendcapacity.Scope{BackendID: "claude-video", BackendKind: "claude_code", Provider: "anthropic"}
	attempts := &recordingWorkAttemptStore{}
	cfg := normalizeConfig(Config{ActiveStates: []string{"In Progress", "Rework"}, TerminalStates: []string{"Done"}})
	orch := &Orchestrator{cfg: cfg, workAttempts: attempts}
	state := newState(cfg)
	issue := connector.Issue{ID: "issue-slipped-capacity", State: "In Progress"}
	state.Running[issue.ID] = Running{
		Issue:         issue,
		Attempt:       3,
		WorkAttemptID: 42,
		CapacityScope: scope,
		StartedAt:     now.Add(-5 * time.Minute),
	}
	state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-5 * time.Minute)}
	ceilingErr := &runpkg.SessionTokenCeilingError{
		TotalTokens:   16_100_000,
		CeilingTokens: 16_000_000,
		Source:        runpkg.TokenCeilingSourceAbsolute,
	}

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID: issue.ID,
		Request: runpkg.RunRequest{Issue: issue, Attempt: 3, Mode: runpkg.RunModeImplement},
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateTokenCeilingExceeded,
			Tokens:     TokenTotals{TotalTokens: 16_100_000},
		},
		Err: backendcapacity.NewError(scope, backendcapacity.Details{
			Type:    backendcapacity.ErrorTypeUsageLimit,
			Kind:    "subscription_window_exhausted",
			Reason:  "subscription window exhausted",
			ResetAt: &resetAt,
		}, ceilingErr),
		CompletedAt: now,
	})

	if len(attempts.completions) != 1 || attempts.completions[0].TerminalState != store.WorkAttemptTerminalCapacity || attempts.completions[0].ErrorClass != backendcapacity.ErrorClass {
		t.Fatalf("work attempt completions = %#v, want backend capacity", attempts.completions)
	}
	if len(state.Blocked) != 0 || len(state.InstantFailures) != 0 || len(state.RepeatedFailures) != 0 || state.FailureBreaker.Active() {
		t.Fatalf("outage-era brakes = blocked %#v instant %#v repeated %#v project %#v", state.Blocked, state.InstantFailures, state.RepeatedFailures, state.FailureBreaker)
	}
	if retry, ok := state.Retry[issue.ID]; !ok || !retry.CapacityScope.Matches(scope) || retry.Attempt != 3 {
		t.Fatalf("Retry[%q] = %#v, want same-attempt capacity wait", issue.ID, retry)
	}
}

func TestBackendCapacityDispatchAllowsOneResetProbe(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 55, 0, 0, time.UTC)
	resetAt := now.Add(44 * time.Minute)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	controller := backendCapacityTestController{scope: scope}
	var logs bytes.Buffer
	orch := &Orchestrator{
		capacityController: controller,
		logger:             slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(normalizeConfig(Config{}))
	capacityErr, ok := backendcapacity.As(backendcapacity.NewError(
		scope,
		backendcapacity.Details{Kind: "usageLimitExceeded", Reason: "provider usage limit reached", Trigger: `{"error":{"type":"usageLimitExceeded"}}`, ResetAt: &resetAt},
		errors.New("usage limit reached"),
	))
	if !ok {
		t.Fatal("capacity error did not unwrap")
	}
	outage := orch.registerBackendOutage(&state, capacityErr, now, false)
	request := runpkg.RunRequest{Issue: connector.Issue{ID: "issue-1", State: "In Progress"}}

	if _, _, paused := orch.backendCapacityDispatch(&state, request, now); !paused {
		t.Fatal("backendCapacityDispatch() paused = false before reset")
	}
	probeAt := now.Add(backendCapacityProbeDelay)
	if !probeAt.Before(outage.ResumeAt) {
		t.Fatalf("probeAt = %s, want before provider resume %s", probeAt, outage.ResumeAt)
	}
	resolvedScope, probeKey, paused := orch.backendCapacityDispatch(&state, request, probeAt)
	if paused || probeKey == "" || !resolvedScope.Matches(scope) {
		t.Fatalf("backendCapacityDispatch() = scope %#v probe %q paused %v, want one early probe", resolvedScope, probeKey, paused)
	}
	orch.markBackendCapacityProbe(&state, probeKey, request.Issue.ID, probeAt)
	if !strings.Contains(logs.String(), "backend capacity probe started") || !stateEventExists(state, "backend_capacity_probe_started") {
		t.Fatalf("probe evidence missing: logs %q events %#v", logs.String(), state.RecentEvents)
	}
	if _, _, paused := orch.backendCapacityDispatch(&state, request, probeAt); !paused {
		t.Fatal("backendCapacityDispatch() paused = false while probe is running")
	}

	orch.recoverBackendCapacity(&state, Running{CapacityScope: scope, CapacityProbe: true}, probeAt.Add(time.Second))
	if len(state.BackendOutages) != 0 || len(state.BackendRecoveries) != 1 {
		t.Fatalf("capacity state after successful probe = outages %#v recoveries %#v", state.BackendOutages, state.BackendRecoveries)
	}
}

func TestHandleRunResultRetriesTransientOverloadWithoutBackendOutage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 12, 20, 34, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	cfg := normalizeConfig(Config{
		OverloadRetryDelay: 45 * time.Second,
		ActiveStates:       []string{"In Progress", "Merging"},
		TerminalStates:     []string{"Done"},
	})
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:                cfg,
		capacityController: backendCapacityTestController{scope: scope},
		logger:             slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	issue := connector.Issue{ID: "issue-merge-overload", Identifier: "digitaldrywood/detent#1281", State: "Merging"}
	state.Running[issue.ID] = Running{Issue: issue, Attempt: 7, WorkerHost: "worker-a", StartedAt: now.Add(-time.Minute)}
	state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-time.Minute)}
	overloadErr := backendcapacity.NewError(scope, backendcapacity.Details{
		Type:   backendcapacity.ErrorTypeTransientOverload,
		Kind:   "serverOverloaded",
		Reason: string(backendcapacity.ErrorTypeTransientOverload),
	}, errors.New("selected model is at capacity"))

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:      issue.ID,
		Request:      runpkg.RunRequest{Issue: issue, Attempt: 7},
		Err:          overloadErr,
		CompletedAt:  now,
		Retryable:    true,
		RetryAttempt: 7,
		RetryDelay:   45 * time.Second,
	})

	if len(state.BackendOutages) != 0 {
		t.Fatalf("BackendOutages = %#v, want none", state.BackendOutages)
	}
	retry, ok := state.Retry[issue.ID]
	if !ok || retry.Attempt != 7 || !retry.DueAt.Equal(now.Add(45*time.Second)) || retry.Error != "transient_overload" {
		t.Fatalf("Retry[%q] = %#v, want same-attempt transient retry after 45s", issue.ID, retry)
	}
	if _, _, paused := orch.backendCapacityDispatch(&state, runpkg.RunRequest{Issue: connector.Issue{ID: "other-issue"}}, now); paused {
		t.Fatal("transient overload paused dispatch for another issue")
	}
	if len(state.InstantFailures) != 0 || len(state.RepeatedFailures) != 0 {
		t.Fatalf("failure breakers = instant %#v repeated %#v, want no overload strikes", state.InstantFailures, state.RepeatedFailures)
	}
	if !strings.Contains(logs.String(), "level=INFO") || !strings.Contains(logs.String(), "reason=transient_overload") {
		t.Fatalf("logs = %q, want INFO transient_overload reason", logs.String())
	}
}

func TestHandleRunResultTracksStartupTimeoutAsCapacityAndBreakerSignal(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	cfg := normalizeConfig(Config{
		OverloadRetryDelay: 45 * time.Second,
		ActiveStates:       []string{"In Progress"},
		TerminalStates:     []string{"Done"},
		FailureBreaker: FailureBreakerConfig{
			SameClassLimit: 2,
			Window:         time.Hour,
			Cooldown:       5 * time.Minute,
		},
	})
	attempts := &recordingWorkAttemptStore{}
	orch := &Orchestrator{cfg: cfg, workAttempts: attempts}
	state := newState(cfg)
	issue := connector.Issue{ID: "issue-startup-timeout", State: "In Progress"}
	state.Running[issue.ID] = Running{Issue: issue, Attempt: 3, WorkAttemptID: 42, StartedAt: now.Add(-time.Minute)}
	state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-time.Minute)}
	timeoutErr := backendcapacity.NewError(scope, backendcapacity.Details{
		Type:   backendcapacity.ErrorTypeTransientOverload,
		Kind:   backendcapacity.StartupTimeoutKind,
		Reason: "backend startup handshake timed out",
		Startup: &backendcapacity.StartupEvidence{
			Stage:              "thread/start",
			ElapsedMS:          5001,
			DeadlineMS:         5000,
			WorkerHost:         "worker-a",
			ConcurrentStartups: 3,
			ActiveWorkers:      7,
			Process: backendcapacity.StartupProcessEvidence{
				Ready:        true,
				ExitObserved: true,
				ExitStatus:   "signal: killed",
			},
		},
	}, context.DeadlineExceeded)

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:      issue.ID,
		Err:          timeoutErr,
		CompletedAt:  now,
		RetryAttempt: 3,
		RetryDelay:   45 * time.Second,
	})

	if len(state.BackendOutages) != 0 {
		t.Fatalf("BackendOutages = %#v, want short retry without outage", state.BackendOutages)
	}
	if len(state.Retry) != 0 {
		t.Fatal("startup failure retained issue retry")
	}
	if len(state.FailureBreaker.Failures[backendcapacity.StartupTimeoutErrorClass]) != 1 || state.FailureBreaker.Active() {
		t.Fatalf("FailureBreaker = %#v, want one preserved systemic signal", state.FailureBreaker)
	}
	if len(attempts.completions) != 1 {
		t.Fatalf("work attempt completions = %#v, want one", attempts.completions)
	}
	completion := attempts.completions[0]
	if completion.TerminalState != store.WorkAttemptTerminalTimedOut || completion.ErrorClass != backendcapacity.StartupTimeoutErrorClass || completion.StatusMessage != "instance failed before first turn" {
		t.Fatalf("completion = %#v, want startup timeout telemetry", completion)
	}
	var metadata struct {
		BackendStartup backendcapacity.StartupEvidence `json:"backend_startup"`
	}
	if err := json.Unmarshal([]byte(completion.WorkerMetadataJSON), &metadata); err != nil {
		t.Fatalf("unmarshal worker metadata: %v", err)
	}
	if metadata.BackendStartup.Stage != "thread/start" || metadata.BackendStartup.ElapsedMS != 5001 || metadata.BackendStartup.DeadlineMS != 5000 || metadata.BackendStartup.ConcurrentStartups != 3 || metadata.BackendStartup.ActiveWorkers != 7 || !metadata.BackendStartup.Process.Ready || !metadata.BackendStartup.Process.ExitObserved {
		t.Fatalf("backend startup metadata = %#v, want durable stage, timing, host, and process evidence", metadata.BackendStartup)
	}
}

func TestHandleRunResultTracksStartupExitAsStableBreakerSignal(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 18, 18, 0, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	cfg := normalizeConfig(Config{
		OverloadRetryDelay: 45 * time.Second,
		TerminalStates:     []string{"Done"},
		FailureBreaker: FailureBreakerConfig{
			SameClassLimit: 2,
			Window:         time.Hour,
			Cooldown:       5 * time.Minute,
		},
	})
	attempts := &recordingWorkAttemptStore{}
	orch := &Orchestrator{cfg: cfg, workAttempts: attempts}
	state := newState(cfg)

	for index, stderr := range []string{"first issue stderr", "second issue stderr", "third issue stderr"} {
		issue := connector.Issue{ID: fmt.Sprintf("issue-startup-exit-%d", index+1), State: "In Progress"}
		state.Running[issue.ID] = Running{Issue: issue, Attempt: 1, WorkAttemptID: int64(42 + index), StartedAt: now.Add(-time.Minute)}
		state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-time.Minute)}
		exitErr := backendcapacity.NewError(scope, backendcapacity.Details{
			Type:   backendcapacity.ErrorTypeTransientOverload,
			Kind:   backendcapacity.StartupFailureKind,
			Reason: "backend startup handshake failed",
		}, fmt.Errorf("wait for initialize response: EOF: stderr: %s", stderr))

		orch.handleRunResult(t.Context(), &state, runpkg.Completion{
			IssueID:      issue.ID,
			Err:          exitErr,
			CompletedAt:  now.Add(time.Duration(index) * time.Second),
			RetryAttempt: 1,
			RetryDelay:   45 * time.Second,
		})
	}

	if !state.FailureBreaker.Active() || state.FailureBreaker.Class != backendcapacity.StartupFailureErrorClass || state.FailureBreaker.Count != 3 {
		t.Fatalf("FailureBreaker = %#v, want three normalized startup failures", state.FailureBreaker)
	}
	if len(attempts.completions) != 3 {
		t.Fatalf("work attempt completions = %#v, want three", attempts.completions)
	}
	for _, completion := range attempts.completions {
		if completion.ErrorClass != backendcapacity.StartupFailureErrorClass || !strings.Contains(completion.ErrorMessage, "stderr") {
			t.Fatalf("completion = %#v, want startup class with stderr diagnostics", completion)
		}
	}
}

func TestHandleRunResultTransientOverloadDefersFailureBreakerCanary(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 15, 20, 0, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	cfg := normalizeConfig(Config{OverloadRetryDelay: 45 * time.Second, TerminalStates: []string{"Done"}})
	orch := &Orchestrator{cfg: cfg}
	state := newState(cfg)
	issue := connector.Issue{ID: "issue-overload-canary", State: "In Progress"}
	state.Running[issue.ID] = Running{Issue: issue, Attempt: 2, StartedAt: now.Add(-time.Minute)}
	state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-time.Minute)}
	state.FailureBreaker.Class = "runner_error:systemic"
	state.FailureBreaker.CanaryIssueID = issue.ID
	state.FailureBreaker.ResumeAt = now
	overloadErr := backendcapacity.NewError(scope, backendcapacity.Details{
		Type:   backendcapacity.ErrorTypeTransientOverload,
		Kind:   "serverOverloaded",
		Reason: string(backendcapacity.ErrorTypeTransientOverload),
	}, errors.New("selected model is at capacity"))

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:      issue.ID,
		Err:          overloadErr,
		CompletedAt:  now,
		RetryAttempt: 2,
		RetryDelay:   45 * time.Second,
	})

	if !state.FailureBreaker.Active() || state.FailureBreaker.CanaryIssueID != "" || !state.FailureBreaker.ResumeAt.Equal(now.Add(45*time.Second)) {
		t.Fatalf("FailureBreaker = %#v, want active breaker with bounded canary backoff", state.FailureBreaker)
	}
	if projectFailureBreakerAllowsDispatch(&state, now.Add(44*time.Second)) {
		t.Fatal("failure breaker admitted another canary before transient backoff elapsed")
	}
	if !projectFailureBreakerAllowsDispatch(&state, now.Add(45*time.Second)) {
		t.Fatal("failure breaker did not admit a new canary after transient backoff")
	}
	if retry := state.Retry[issue.ID]; !retry.DueAt.Equal(now.Add(45*time.Second)) || retry.Error != backendcapacity.TransientOverloadErrorClass {
		t.Fatalf("Retry[%q] = %#v, want transient overload canary retry", issue.ID, retry)
	}
}

func TestRegisterBackendOutageRejectsTransientOverload(t *testing.T) {
	t.Parallel()

	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	overloadErr, ok := backendcapacity.As(backendcapacity.NewError(scope, backendcapacity.Details{
		Type: backendcapacity.ErrorTypeTransientOverload,
	}, errors.New("HTTP 529")))
	if !ok {
		t.Fatal("transient overload error did not unwrap")
	}
	state := newState(normalizeConfig(Config{}))
	orch := &Orchestrator{}
	if outage := orch.registerBackendOutage(&state, overloadErr, time.Now(), false); outage != (BackendOutage{}) || len(state.BackendOutages) != 0 {
		t.Fatalf("registerBackendOutage() = %#v, outages %#v, want no outage", outage, state.BackendOutages)
	}
}

func TestHandleRunResultTransientOverloadReleasesCapacityProbe(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 12, 21, 30, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	cfg := normalizeConfig(Config{OverloadRetryDelay: 45 * time.Second, TerminalStates: []string{"Done"}})
	orch := &Orchestrator{cfg: cfg}
	state := newState(cfg)
	issue := connector.Issue{ID: "issue-overloaded-probe", State: "In Progress"}
	resumeAt := now.Add(-time.Second)
	state.BackendOutages[scope.Key()] = BackendOutage{
		Scope:        scope,
		ResumeAt:     resumeAt,
		ProbeIssueID: issue.ID,
	}
	state.Running[issue.ID] = Running{
		Issue:         issue,
		Attempt:       3,
		StartedAt:     now.Add(-time.Minute),
		CapacityScope: scope,
		CapacityProbe: true,
	}
	overloadErr := backendcapacity.NewError(scope, backendcapacity.Details{
		Type:   backendcapacity.ErrorTypeTransientOverload,
		Kind:   "serverOverloaded",
		Reason: string(backendcapacity.ErrorTypeTransientOverload),
	}, errors.New("selected model is at capacity"))

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:      issue.ID,
		Err:          overloadErr,
		CompletedAt:  now,
		RetryAttempt: 3,
		RetryDelay:   45 * time.Second,
	})

	outage := state.BackendOutages[scope.Key()]
	if outage.ProbeIssueID != "" || !outage.ResumeAt.Equal(resumeAt) {
		t.Fatalf("outage = %#v, want released probe with unchanged resume time", outage)
	}
	if retry := state.Retry[issue.ID]; retry.Attempt != 3 || !retry.DueAt.Equal(now.Add(45*time.Second)) {
		t.Fatalf("Retry[%q] = %#v, want same-attempt overload retry", issue.ID, retry)
	}
}

func TestHandleOperatorStopCompletionDefersCapacityProbe(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 14, 22, 30, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	cfg := normalizeConfig(Config{StopRunTargetState: "Blocked"})
	issue := connector.Issue{ID: "issue-stopped-probe", Identifier: "digitaldrywood/detent#1311", State: "In Progress"}
	connector := &backendCapacityTestConnector{}
	orch := &Orchestrator{
		cfg:            cfg,
		connector:      connector,
		pendingStops:   map[string]*pendingStopRun{},
		completedStops: map[string]StopRunResult{},
		now:            func() time.Time { return now },
	}
	state := newState(cfg)
	state.BackendOutages[scope.Key()] = BackendOutage{
		Scope:         scope,
		ResumeAt:      now.Add(time.Hour),
		ProbeIssueID:  issue.ID,
		ProbeAttempts: 1,
	}
	state.DispatchRecoveries[dispatchRecoveryBackendCapacity] = DispatchRecovery{
		Kind:       dispatchRecoveryBackendCapacity,
		Status:     dispatchRecoveryStatusRamping,
		Limit:      1,
		Admissions: map[string]bool{issue.ID: false},
	}
	state.FailureBreaker.Class = "runner_error:systemic"
	state.FailureBreaker.CanaryIssueID = issue.ID
	state.FailureBreaker.ResumeAt = now
	running := Running{Issue: issue, Attempt: 1, CapacityScope: scope, CapacityProbe: true}
	state.Running[issue.ID] = running
	orch.pendingStops[issue.ID] = &pendingStopRun{
		result: StopRunResult{
			IssueID:     issue.ID,
			Identifier:  issue.Identifier,
			Attempt:     running.Attempt,
			Destination: "Blocked",
			Outcome:     "pending",
			RequestedAt: now.Add(-time.Second),
		},
		reapDone: true,
	}

	if handled := orch.handleOperatorStopCompletion(t.Context(), &state, runpkg.Completion{IssueID: issue.ID, CompletedAt: now}, running); !handled {
		t.Fatal("handleOperatorStopCompletion() handled = false")
	}

	outage := state.BackendOutages[scope.Key()]
	if outage.ProbeIssueID != "" || outage.NextProbeAt.IsZero() || outage.LastProbeResult != "failed" {
		t.Fatalf("outage = %#v, want stopped probe released and deferred", outage)
	}
	if len(connector.updates) != 1 || connector.updates[0] != (backendCapacityTestUpdate{issueID: issue.ID, state: "Blocked"}) {
		t.Fatalf("tracker updates = %#v, want Blocked transition", connector.updates)
	}
	if recovery := state.DispatchRecoveries[dispatchRecoveryBackendCapacity]; len(recovery.Admissions) != 0 {
		t.Fatalf("dispatch recovery = %#v, want operator-stopped admission released", recovery)
	}
	if !state.FailureBreaker.Active() || state.FailureBreaker.CanaryIssueID != "" {
		t.Fatalf("failure breaker = %#v, want active breaker with canary released", state.FailureBreaker)
	}
}

func TestBackendCapacityStatusProbeRecoversOutageEarly(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 2, 39, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	controller := backendCapacityTestController{
		scope:     scope,
		status:    runpkg.CapacityStatus{Available: true, Detail: "live provider status reports 20% capacity remaining"},
		hasStatus: true,
	}
	orch := &Orchestrator{capacityStatus: controller}
	state := newState(normalizeConfig(Config{}))
	state.BackendOutages[scope.Key()] = BackendOutage{
		Scope:       scope,
		DetectedAt:  now.Add(-time.Hour),
		ResumeAt:    now.Add(3 * time.Hour),
		NextProbeAt: now.Add(5 * time.Minute),
	}
	state.Running["issue-live-status"] = Running{
		Issue:         connector.Issue{ID: "issue-live-status"},
		CapacityScope: scope,
	}

	orch.handleRunUpdate(&state, runUpdate{
		issueID: "issue-live-status",
		usage: runpkg.UsageUpdate{
			LastEventAt: now,
			RateLimits: &telemetry.RateLimits{
				Primary: &telemetry.RateLimitBucket{Limit: 100, Remaining: 20},
			},
		},
	})

	if len(state.BackendOutages) != 0 || len(state.BackendRecoveries) != 1 {
		t.Fatalf("capacity state = outages %#v recoveries %#v, want live-status recovery", state.BackendOutages, state.BackendRecoveries)
	}
	recovery := state.BackendRecoveries[scope.Key()]
	if recovery.Outage.LastProbeResult != "status_available" || !recovery.RecoveredAt.Equal(now) {
		t.Fatalf("recovery = %#v", recovery)
	}
	if !stateEventExists(state, "backend_capacity_recovered") {
		t.Fatalf("events = %#v, want recovery event", state.RecentEvents)
	}
}

func TestBackendCapacityStatusProbeKeepsExhaustedOutage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 2, 39, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	controller := backendCapacityTestController{
		scope:     scope,
		status:    runpkg.CapacityStatus{Detail: "live provider status reports 0% capacity remaining"},
		hasStatus: true,
	}
	orch := &Orchestrator{capacityStatus: controller, now: func() time.Time { return now }}
	state := newState(normalizeConfig(Config{}))
	state.BackendOutages[scope.Key()] = BackendOutage{
		Scope:       scope,
		DetectedAt:  now.Add(-time.Hour),
		ResumeAt:    now.Add(3 * time.Hour),
		NextProbeAt: now.Add(5 * time.Minute),
	}
	running := Running{CapacityScope: scope}

	orch.recoverBackendCapacityFromStatus(&state, running, &telemetry.RateLimits{
		Primary: &telemetry.RateLimitBucket{Limit: 100},
	}, time.Time{})

	outage, ok := state.BackendOutages[scope.Key()]
	if !ok {
		t.Fatal("exhausted status cleared the outage")
	}
	if outage.LastProbeResult != "status_exhausted" || !outage.LastProbeAt.Equal(now) || outage.LastProbeDetail != controller.status.Detail {
		t.Fatalf("outage = %#v", outage)
	}
	orch.capacityStatus = backendCapacityTestController{}
	orch.recoverBackendCapacityFromStatus(&state, running, &telemetry.RateLimits{}, now)
	orch.capacityStatus = nil
	orch.recoverBackendCapacityFromStatus(&state, running, &telemetry.RateLimits{}, now)
}

func TestClearBackendCapacityClearsScopeAndReleasesRetries(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 2, 39, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	otherScope := backendcapacity.Scope{BackendID: "claude", BackendKind: "claude_code", Provider: "anthropic"}
	var logs bytes.Buffer
	orch := &Orchestrator{logger: slog.New(slog.NewTextHandler(&logs, nil))}
	state := newState(normalizeConfig(Config{}))
	state.BackendOutages[scope.Key()] = BackendOutage{Scope: scope, ResumeAt: now.Add(time.Hour)}
	state.BackendOutages[otherScope.Key()] = BackendOutage{Scope: otherScope, ResumeAt: now.Add(time.Hour)}
	state.Retry["issue-codex"] = Retry{
		Issue:         connector.Issue{ID: "issue-codex"},
		DueAt:         now.Add(time.Hour),
		CapacityScope: scope,
	}

	cleared := orch.clearBackendCapacity(&state, "codex", now)

	if len(cleared) != 1 || !cleared[0].Scope.Matches(scope) {
		t.Fatalf("cleared = %#v", cleared)
	}
	if _, ok := state.BackendOutages[scope.Key()]; ok {
		t.Fatal("codex outage remains after operator clear")
	}
	if _, ok := state.BackendOutages[otherScope.Key()]; !ok {
		t.Fatal("unrelated outage was cleared")
	}
	if !state.Retry["issue-codex"].DueAt.Equal(now) {
		t.Fatalf("retry due = %s, want %s", state.Retry["issue-codex"].DueAt, now)
	}
	if !strings.Contains(logs.String(), "operator cleared backend capacity outage") || !stateEventExists(state, "backend_capacity_operator_cleared") {
		t.Fatalf("operator evidence missing: logs %q events %#v", logs.String(), state.RecentEvents)
	}
}

func TestRequestBackendCapacityClearUsesBoundedQueue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 3, 19, 0, 0, 0, time.UTC)
	orch := &Orchestrator{
		capacityClearRequests: make(chan capacityClearRequest, 1),
		done:                  make(chan struct{}),
		now:                   func() time.Time { return now },
	}
	if err := orch.RequestBackendCapacityClear(t.Context(), " codex "); err != nil {
		t.Fatalf("RequestBackendCapacityClear() error = %v", err)
	}
	if err := orch.RequestBackendCapacityClear(t.Context(), "codex"); !errors.Is(err, ErrCapacityClearQueueFull) {
		t.Fatalf("RequestBackendCapacityClear() second error = %v, want %v", err, ErrCapacityClearQueueFull)
	}
	request := <-orch.capacityClearRequests
	if request.scope != "codex" || !request.at.Equal(now) || request.reply != nil {
		t.Fatalf("capacity clear request = %#v", request)
	}
	close(orch.done)
	if err := orch.RequestBackendCapacityClear(t.Context(), "codex"); !errors.Is(err, ErrStopped) {
		t.Fatalf("RequestBackendCapacityClear() stopped error = %v, want %v", err, ErrStopped)
	}
}

func TestRequestBackendCapacityClearLifecycle(t *testing.T) {
	t.Parallel()

	for _, scenario := range []string{"nil context", "canceled", "stopped", "shutdown during request"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()

			orch, err := New(Config{}, Dependencies{Connector: memory.New(memory.Config{}), Runner: FakeRunner{}})
			if err != nil {
				t.Fatal(err)
			}
			var ctx context.Context
			var wantErr error
			switch scenario {
			case "canceled":
				canceled, cancel := context.WithCancel(t.Context())
				cancel()
				ctx = canceled
				wantErr = context.Canceled
			case "stopped", "shutdown during request":
				runCtx, stop := context.WithCancel(t.Context())
				stop()
				if scenario == "stopped" {
					if err := orch.Run(runCtx); !errors.Is(err, context.Canceled) {
						t.Fatalf("Run() error = %v", err)
					}
				} else {
					orch.now = func() time.Time {
						if err := orch.Run(runCtx); !errors.Is(err, context.Canceled) {
							t.Errorf("Run() error = %v", err)
						}
						return time.Now()
					}
				}
				wantErr = ErrStopped
			}
			if err := orch.RequestBackendCapacityClear(ctx, "codex"); !errors.Is(err, wantErr) {
				t.Fatalf("RequestBackendCapacityClear() error = %v, want %v", err, wantErr)
			}
			if wantErr != nil && len(orch.capacityClearRequests) != 0 {
				t.Fatal("rejected request remained queued")
			}
		})
	}
}

type capacityClearBusyConnector struct {
	connector.Connector
	entered chan struct{}
	release chan struct{}
}

func (c *capacityClearBusyConnector) FetchCandidateIssues(ctx context.Context) ([]connector.Issue, error) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return nil, nil
	}
}

func TestRequestBackendCapacityClearWhileRefreshIsBusy(t *testing.T) {
	t.Parallel()

	tracker := &capacityClearBusyConnector{Connector: memory.New(memory.Config{}), entered: make(chan struct{}, 1), release: make(chan struct{})}
	orch, err := New(Config{PollInterval: time.Hour}, Dependencies{Connector: tracker, Runner: FakeRunner{}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- orch.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("orchestrator did not stop")
		}
	})
	select {
	case <-tracker.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not enter the tracker")
	}
	requestCtx, requestCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer requestCancel()
	if err := orch.RequestBackendCapacityClear(requestCtx, "codex"); err != nil {
		t.Fatalf("clear request blocked behind refresh: %v", err)
	}
	if err := orch.RequestBackendCapacityClear(requestCtx, "codex"); !errors.Is(err, ErrCapacityClearQueueFull) {
		t.Fatalf("second request = %v, want bounded queue rejection", err)
	}
	close(tracker.release)
	barrier := capacityClearRequest{scope: "codex", reply: make(chan capacityClearReply, 1)}
	select {
	case orch.capacityClearRequests <- barrier:
	case <-requestCtx.Done():
		t.Fatal("queued clear was not consumed after refresh")
	}
	select {
	case <-barrier.reply:
	case <-requestCtx.Done():
		t.Fatal("clear processing did not complete")
	}
}

func stateEventExists(state State, event string) bool {
	for _, candidate := range state.RecentEvents {
		if candidate.Event == event {
			return true
		}
	}
	return false
}

func countStateEvents(events []telemetry.ActivityEvent, event string) int {
	count := 0
	for _, candidate := range events {
		if candidate.Event == event {
			count++
		}
	}
	return count
}

func stateEventMessageContains(events []telemetry.ActivityEvent, event string, text string) bool {
	for _, candidate := range events {
		if candidate.Event == event && strings.Contains(candidate.Message, text) {
			return true
		}
	}
	return false
}

func TestHandleRunResultKeepsOutageWhenCapacityProbeFails(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 2, 39, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	cfg := normalizeConfig(Config{})
	orch := &Orchestrator{cfg: cfg}
	state := newState(cfg)
	state.BackendOutages[scope.Key()] = BackendOutage{
		Scope:        scope,
		DetectedAt:   now.Add(-44 * time.Minute),
		ResumeAt:     now,
		ProbeIssueID: "issue-capacity-probe",
	}
	state.Running["issue-capacity-probe"] = Running{
		Issue:         connector.Issue{ID: "issue-capacity-probe", State: "In Progress"},
		Attempt:       1,
		StartedAt:     now.Add(-time.Minute),
		CapacityScope: scope,
		CapacityProbe: true,
	}

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:     "issue-capacity-probe",
		Err:         errors.New("workspace setup failed"),
		CompletedAt: now,
	})

	if _, ok := state.BackendOutages[scope.Key()]; !ok {
		t.Fatal("capacity probe failure cleared the backend outage")
	}
	outage := state.BackendOutages[scope.Key()]
	if outage.ProbeIssueID != "" {
		t.Fatalf("ProbeIssueID = %q, want released probe", outage.ProbeIssueID)
	}
	if !outage.ResumeAt.Equal(now) {
		t.Fatalf("ResumeAt = %s, want preserved provider time %s", outage.ResumeAt, now)
	}
	if want := now.Add(backendCapacityProbeDelayForAttempt(1)); !outage.NextProbeAt.Equal(want) {
		t.Fatalf("NextProbeAt = %s, want %s", outage.NextProbeAt, want)
	}
	if len(state.BackendRecoveries) != 0 {
		t.Fatalf("backend recoveries = %#v, want none", state.BackendRecoveries)
	}
}

func TestBackendCapacityProbeFailureRefreshesProviderWindow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 2, 39, 0, 0, time.UTC)
	freshResetAt := now.Add(2 * time.Hour)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	controller := backendCapacityTestController{scope: scope}
	orch := &Orchestrator{capacityController: controller}
	state := newState(normalizeConfig(Config{}))
	state.BackendOutages[scope.Key()] = BackendOutage{
		Scope:           scope,
		DetectedAt:      now.Add(-time.Hour),
		ResumeAt:        now.Add(3 * time.Hour),
		ProbeAttempts:   1,
		ProbeIssueID:    "issue-canary",
		LastProbeAt:     now.Add(-time.Minute),
		LastProbeResult: "in_progress",
	}
	capacityErr, ok := backendcapacity.As(backendcapacity.NewError(
		scope,
		backendcapacity.Details{Kind: "usageLimitExceeded", ResetAt: &freshResetAt},
		errors.New("usage limit reached"),
	))
	if !ok {
		t.Fatal("capacity error did not unwrap")
	}

	outage := orch.registerBackendOutage(&state, capacityErr, now, true)

	if !outage.ResetAt.Equal(freshResetAt) || !outage.ResumeAt.Equal(freshResetAt.Add(backendCapacityResetJitter)) {
		t.Fatalf("fresh provider window = reset %s resume %s", outage.ResetAt, outage.ResumeAt)
	}
	if outage.LastProbeResult != "capacity_exhausted" || outage.ProbeIssueID != "" {
		t.Fatalf("probe result = %#v", outage)
	}
	if want := now.Add(backendCapacityProbeDelayForAttempt(1)); !outage.NextProbeAt.Equal(want) {
		t.Fatalf("NextProbeAt = %s, want %s", outage.NextProbeAt, want)
	}
	request := runpkg.RunRequest{Issue: connector.Issue{ID: "issue-other"}}
	if _, _, paused := orch.backendCapacityDispatch(&state, request, now.Add(backendCapacityProbeDelay)); !paused {
		t.Fatal("full-width dispatch resumed before the next bounded canary")
	}
}

func TestHandleRunResultRecoversOutageWhenCapacityProbeTurnStarts(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 2, 39, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	cfg := normalizeConfig(Config{})
	orch := &Orchestrator{cfg: cfg}
	state := newState(cfg)
	state.BackendOutages[scope.Key()] = BackendOutage{Scope: scope, ResumeAt: now, ProbeIssueID: "issue-started-probe"}
	state.Running["issue-started-probe"] = Running{
		Issue:         connector.Issue{ID: "issue-started-probe", State: "In Progress"},
		Attempt:       1,
		StartedAt:     now.Add(-time.Minute),
		CapacityScope: scope,
		CapacityProbe: true,
	}

	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:     "issue-started-probe",
		Result:      runpkg.RunResult{TurnStarted: true},
		Err:         errors.New("agent work failed"),
		CompletedAt: now,
	})

	if len(state.BackendOutages) != 0 || len(state.BackendRecoveries) != 1 {
		t.Fatalf("capacity state = outages %#v recoveries %#v, want recovered", state.BackendOutages, state.BackendRecoveries)
	}
}

func TestBackendCapacityDispatchLeavesLocalProviderUnaffected(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 55, 0, 0, time.UTC)
	state := newState(normalizeConfig(Config{}))
	state.BackendOutages["hosted"] = BackendOutage{
		Scope:    backendcapacity.Scope{BackendID: "codex-hosted", BackendKind: "codex", Provider: "openai"},
		ResumeAt: now.Add(time.Hour),
	}
	orch := &Orchestrator{capacityController: backendCapacityTestController{
		scope: backendcapacity.Scope{BackendID: "codex-local", BackendKind: "codex", Provider: "local_ollama"},
	}}

	_, _, paused := orch.backendCapacityDispatch(&state, runpkg.RunRequest{Issue: connector.Issue{ID: "local"}}, now)
	if paused {
		t.Fatal("local provider dispatch paused by hosted-provider outage")
	}
}

func TestBackendCapacityWithoutResetUsesLowFrequencyProbe(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 55, 0, 0, time.UTC)
	if got, want := backendCapacityResumeAt(time.Time{}, now), now.Add(backendCapacityProbeDelay); !got.Equal(want) {
		t.Fatalf("backendCapacityResumeAt() = %s, want %s", got, want)
	}
}

func TestBackendCapacityHelperBoundaries(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 55, 0, 0, time.UTC)
	if got := backendCapacityStatusMessage(BackendOutage{}); got != "backend agent backend at usage limit" {
		t.Fatalf("backendCapacityStatusMessage() = %q", got)
	}
	outage := BackendOutage{
		Scope:    backendcapacity.Scope{BackendKind: "codex"},
		Reason:   "subscription window exhausted",
		ResumeAt: now,
	}
	if got := backendCapacityStatusMessage(outage); !strings.Contains(got, "backend codex: subscription window exhausted") || !strings.Contains(got, "resumes ~2026-07-10 01:55 UTC") {
		t.Fatalf("backendCapacityStatusMessage() = %q", got)
	}

	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	resetAt := now.Add(time.Hour)
	capacityErr, ok := backendcapacity.As(backendcapacity.NewError(
		scope,
		backendcapacity.Details{Kind: "usageLimitExceeded", Reason: "provider usage limit reached", Trigger: `{"error":{"type":"usageLimitExceeded"}}`, ResetAt: &resetAt},
		errors.New("usage limit reached"),
	))
	if !ok {
		t.Fatal("capacity error did not unwrap")
	}
	orch := &Orchestrator{now: func() time.Time { return now }}
	state := State{
		Retry:   map[string]Retry{},
		Claimed: map[string]Claimed{},
	}
	registered := orch.registerBackendOutage(&state, capacityErr, time.Time{}, false)
	if !registered.DetectedAt.Equal(now) || state.BackendOutages[scope.Key()].Kind != "usageLimitExceeded" || state.BackendOutages[scope.Key()].Trigger == "" {
		t.Fatalf("registered outage = %#v", registered)
	}

	running := Running{Issue: connector.Issue{ID: "issue-1"}, Attempt: 2, WorkerHost: "worker"}
	orch.scheduleBackendCapacityRetry(&state, running, registered)
	if _, ok := state.Claimed[running.Issue.ID]; ok || state.Retry[running.Issue.ID].WorkerHost != "worker" {
		t.Fatalf("scheduled state = claims %#v retries %#v", state.Claimed, state.Retry)
	}
	if !state.Retry[running.Issue.ID].DueAt.Equal(registered.NextProbeAt) {
		t.Fatalf("retry due = %s, want early probe %s", state.Retry[running.Issue.ID].DueAt, registered.NextProbeAt)
	}
	orch.markBackendCapacityProbe(&state, "missing", running.Issue.ID, now)
	orch.recoverBackendCapacity(&state, Running{CapacityScope: backendcapacity.Scope{BackendID: "missing"}}, now)
	if _, _, paused := orch.validatorCapacityDispatch(nil, connector.Issue{}, now); paused {
		t.Fatal("nil validator capacity dispatch paused")
	}
	orch.publishValidatorCapacityEvent(t.Context(), validatorCapacityEvent{})
	orch.handleValidatorCapacityEvent(&state, validatorCapacityEvent{})

	var logs bytes.Buffer
	orch.logger = slog.New(slog.NewTextHandler(&logs, nil))
	orch.handleValidatorCapacityEvent(&state, validatorCapacityEvent{CapacityErr: capacityErr, CompletedAt: now})
	orch.recoverBackendCapacity(&state, Running{CapacityScope: scope, CapacityProbe: true}, now)
	state.BackendOutages[scope.Key()] = registered
	orch.deferBackendCapacityProbe(&state, Running{CapacityScope: scope, CapacityProbe: true}, time.Time{}, errors.New("probe failed"))
	if !strings.Contains(logs.String(), "backend capacity recovered") || !strings.Contains(logs.String(), "probe failed") || !strings.Contains(logs.String(), "capacity_trigger") || !strings.Contains(logs.String(), "usageLimitExceeded") {
		t.Fatalf("capacity logs = %q", logs.String())
	}

	recovery := BackendRecovery{Outage: registered, RecoveredAt: now}
	if key, _, found := matchingBackendRecovery(map[string]BackendRecovery{scope.Key(): recovery}, scope); !found || key != scope.Key() {
		t.Fatalf("matchingBackendRecovery() = %q, %t", key, found)
	}
	readerless := &Orchestrator{connector: &backendCapacityTestConnector{}}
	if _, _, _, ok := readerless.classifyBlockedCapacityIssue(t.Context(), &state, connector.Issue{ID: "blocked"}, now); ok {
		t.Fatal("readerless capacity issue classified")
	}

	recoveryState := newState(normalizeConfig(Config{}))
	recoveryIssue := connector.Issue{ID: "recover", State: "Blocked"}
	recoveryState.Blocked[recoveryIssue.ID] = Blocked{Issue: recoveryIssue}
	recoveryOrch := &Orchestrator{connector: &backendCapacityTestConnector{}}
	if !recoveryOrch.applyBackendCapacityBlockedRecovery(t.Context(), &recoveryState, recoveryIssue, "Todo", BackendOutage{}, recovery, now) {
		t.Fatal("applyBackendCapacityBlockedRecovery() = false")
	}
}

func TestValidatorCapacityPausesWithoutFailureBackoff(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 55, 0, 0, time.UTC)
	resetAt := now.Add(44 * time.Minute)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	validator := &backendCapacityTestValidator{
		requests: make(chan ValidatorRequest, 2),
		err: backendcapacity.NewError(
			scope,
			backendcapacity.Details{Kind: "usageLimitExceeded", ResetAt: &resetAt},
			errors.New("usage limit reached"),
		),
	}
	controller := backendCapacityTestController{scope: scope}
	orch := &Orchestrator{
		cfg:                     normalizeConfig(Config{}),
		validator:               validator,
		validatorCapacity:       controller,
		validatorRuns:           map[string]struct{}{},
		validatorResults:        map[string]validatorStageResult{},
		validatorFailures:       map[string]validatorStageFailure{},
		now:                     func() time.Time { return now },
		validatorCapacityEvents: make(chan validatorCapacityEvent, 1),
		done:                    make(chan struct{}),
	}
	state := newState(orch.cfg)
	issue := connector.Issue{
		ID:    "issue-validator-capacity",
		State: "In Progress",
		PullRequest: &connector.PullRequest{
			HeadSHA: "capacity-head",
		},
	}

	orch.startValidatorStage(t.Context(), &state, issue, now)
	select {
	case <-validator.requests:
	case <-time.After(time.Second):
		t.Fatal("validator did not run")
	}
	var event validatorCapacityEvent
	select {
	case event = <-orch.validatorCapacityEvents:
	case <-time.After(time.Second):
		t.Fatal("validator capacity event was not published")
	}
	orch.handleValidatorCapacityEvent(&state, event)
	if len(orch.validatorFailures) != 0 {
		t.Fatalf("validator failures = %#v, want none", orch.validatorFailures)
	}
	if len(state.BackendOutages) != 1 {
		t.Fatalf("backend outages = %#v, want one", state.BackendOutages)
	}

	orch.startValidatorStage(t.Context(), &state, issue, now.Add(time.Minute))
	select {
	case <-validator.requests:
		t.Fatal("validator ran while backend capacity was paused")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestValidatorTransientOverloadReleasesCapacityProbe(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 12, 21, 0, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	validator := &backendCapacityTestValidator{
		requests: make(chan ValidatorRequest, 1),
		err: backendcapacity.NewError(scope, backendcapacity.Details{
			Type:   backendcapacity.ErrorTypeTransientOverload,
			Kind:   "serverOverloaded",
			Reason: string(backendcapacity.ErrorTypeTransientOverload),
		}, errors.New("selected model is at capacity")),
	}
	cfg := normalizeConfig(Config{OverloadRetryDelay: 45 * time.Second})
	orch := &Orchestrator{
		cfg:                     cfg,
		validator:               validator,
		validatorCapacity:       backendCapacityTestController{scope: scope},
		validatorRuns:           map[string]struct{}{},
		validatorResults:        map[string]validatorStageResult{},
		validatorFailures:       map[string]validatorStageFailure{},
		now:                     func() time.Time { return now },
		validatorCapacityEvents: make(chan validatorCapacityEvent, 1),
		done:                    make(chan struct{}),
	}
	state := newState(cfg)
	issue := connector.Issue{
		ID:    "issue-validator-overload",
		State: "In Progress",
		PullRequest: &connector.PullRequest{
			HeadSHA: "overload-head",
		},
	}
	resumeAt := now
	state.BackendOutages[scope.Key()] = BackendOutage{
		Scope:    scope,
		ResumeAt: resumeAt,
	}

	orch.startValidatorStage(t.Context(), &state, issue, now)
	select {
	case <-validator.requests:
	case <-time.After(time.Second):
		t.Fatal("validator did not run")
	}
	identity := validatorStageIdentityForIssue(issue)
	deadline := time.NewTimer(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	t.Cleanup(func() { deadline.Stop() })
	t.Cleanup(ticker.Stop)
	var failure validatorStageFailure
	for failure.NextRetryAt.IsZero() {
		select {
		case <-ticker.C:
			orch.validatorMu.Lock()
			failure = orch.validatorFailures[identity.Key]
			orch.validatorMu.Unlock()
		case <-deadline.C:
			t.Fatal("validator overload retry was not scheduled")
		}
	}
	if failure.Attempt != 0 || !failure.NextRetryAt.Equal(now.Add(45*time.Second)) || failure.Error != "transient_overload" {
		t.Fatalf("validator failure = %#v, want same-attempt retry after 45s", failure)
	}
	var capacityEvent validatorCapacityEvent
	select {
	case capacityEvent = <-orch.validatorCapacityEvents:
	case <-time.After(time.Second):
		t.Fatal("validator overload probe release event was not published")
	}
	orch.handleValidatorCapacityEvent(&state, capacityEvent)
	outage := state.BackendOutages[scope.Key()]
	if outage.ProbeIssueID != "" || !outage.ResumeAt.Equal(resumeAt) {
		t.Fatalf("outage = %#v, want released probe with unchanged resume time", outage)
	}
}

func TestValidatorCapacityProbeFailureKeepsOutage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 2, 39, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	validator := &backendCapacityTestValidator{
		requests: make(chan ValidatorRequest, 1),
		err:      errors.New("workspace setup failed"),
	}
	controller := backendCapacityTestController{scope: scope}
	orch := &Orchestrator{
		cfg:                     normalizeConfig(Config{}),
		validator:               validator,
		validatorCapacity:       controller,
		validatorRuns:           map[string]struct{}{},
		validatorResults:        map[string]validatorStageResult{},
		validatorFailures:       map[string]validatorStageFailure{},
		now:                     func() time.Time { return now },
		validatorCapacityEvents: make(chan validatorCapacityEvent, 1),
		done:                    make(chan struct{}),
	}
	state := newState(orch.cfg)
	state.BackendOutages[scope.Key()] = BackendOutage{
		Scope:      scope,
		DetectedAt: now.Add(-44 * time.Minute),
		ResumeAt:   now,
	}
	issue := connector.Issue{
		ID:    "issue-validator-probe",
		State: "In Progress",
		PullRequest: &connector.PullRequest{
			HeadSHA: "capacity-probe-head",
		},
	}

	orch.startValidatorStage(t.Context(), &state, issue, now)
	select {
	case <-validator.requests:
	case <-time.After(time.Second):
		t.Fatal("validator did not run")
	}
	orch.validatorWG.Wait()
	select {
	case event := <-orch.validatorCapacityEvents:
		orch.handleValidatorCapacityEvent(&state, event)
	default:
	}

	if _, ok := state.BackendOutages[scope.Key()]; !ok {
		t.Fatal("validator capacity probe failure cleared the backend outage")
	}
	outage := state.BackendOutages[scope.Key()]
	if outage.ProbeIssueID != "" {
		t.Fatalf("ProbeIssueID = %q, want released probe", outage.ProbeIssueID)
	}
	if !outage.ResumeAt.Equal(now) {
		t.Fatalf("ResumeAt = %s, want preserved provider time %s", outage.ResumeAt, now)
	}
	if want := now.Add(backendCapacityProbeDelayForAttempt(1)); !outage.NextProbeAt.Equal(want) {
		t.Fatalf("NextProbeAt = %s, want %s", outage.NextProbeAt, want)
	}
	if len(orch.validatorFailures) != 1 {
		t.Fatalf("validator failures = %#v, want one", orch.validatorFailures)
	}
}

func TestRecoverBackendCapacityBlockedIssues(t *testing.T) {
	t.Parallel()

	parkedAt := time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC)
	scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	tests := []struct {
		name                  string
		sourceState           string
		currentEntryReason    string
		currentEntryAt        time.Time
		workpadSignal         *workpad.Signal
		recoveryNotifications int
		wantTransitions       int
		wantState             string
		wantSuppression       bool
		wantDiagnostic        string
		reinsertRecovery      bool
	}{
		{
			name:                  "capacity-only park recovers to captured lane",
			sourceState:           "In Progress",
			currentEntryReason:    "instant_fail_circuit_breaker",
			currentEntryAt:        parkedAt,
			recoveryNotifications: 1,
			wantTransitions:       1,
			wantState:             "In Progress",
		},
		{
			name:               "independent human Workpad blocker stays blocked",
			sourceState:        "Rework",
			currentEntryReason: "instant_fail_circuit_breaker",
			currentEntryAt:     parkedAt,
			workpadSignal: &workpad.Signal{
				Source: workpad.SourceStructured,
				Status: workpad.StatusBlocked,
				Blockers: []workpad.Blocker{{
					Ref:    "missing:browser-tooling:clerk-browser-state",
					Reason: "browser automation and authenticated Clerk state are unavailable",
					Owner:  workpad.BlockerOwnerHuman,
					Predicate: &workpad.Predicate{
						Type:        workpad.PredicateConfigFingerprint,
						Fingerprint: "missing:browser-tooling:clerk-browser-state",
					},
				}},
				HumanAction: "provide browser automation and authenticated Clerk state",
				RecordedAt:  timePointer(parkedAt.Add(time.Minute)),
			},
			recoveryNotifications: 2,
			wantTransitions:       0,
			wantSuppression:       true,
			wantDiagnostic:        "human action",
		},
		{
			name:                  "stale recovery after newer capacity Blocked entry is ignored",
			sourceState:           "Rework",
			currentEntryReason:    "instant_fail_circuit_breaker",
			currentEntryAt:        parkedAt.Add(6 * time.Minute),
			recoveryNotifications: 1,
			wantTransitions:       0,
			wantSuppression:       true,
			wantDiagnostic:        "recovery predates the current Blocked entry evidence",
		},
		{
			name:                  "repeated recovery notifications transition once",
			sourceState:           "Rework",
			currentEntryReason:    "instant_fail_circuit_breaker",
			currentEntryAt:        parkedAt,
			recoveryNotifications: 2,
			wantTransitions:       1,
			wantState:             "Rework",
			reinsertRecovery:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			legacyCommentAt := tt.currentEntryAt.Add(time.Second)
			stageUpdatedAt := tt.currentEntryAt
			issue := connector.Issue{
				ID:             "issue-capacity-blocked",
				Identifier:     "digitaldrywood/detent#1142",
				State:          "Blocked",
				StageUpdatedAt: &stageUpdatedAt,
				PullRequest:    &connector.PullRequest{Number: 1142, State: "open"},
				WorkpadSignal:  tt.workpadSignal,
				Comments: []connector.IssueComment{{
					Body: "Detent stopped retrying this worker after 5 consecutive instant failures with the same backend error. backend_error_body: " +
						`{"error":{"type":"usageLimitExceeded","resetAt":1783666800}}`,
					CreatedAt: &legacyCommentAt,
				}},
			}
			tracker := &backendCapacityTestConnector{}
			controller := backendCapacityTestController{scope: scope}
			metrics := &autoPromoteWorkflowMetricsRecorder{}
			orch := &Orchestrator{
				cfg:                normalizeConfig(Config{ActiveStates: []string{"Todo", "In Progress", "Rework"}}),
				connector:          tracker,
				capacityController: controller,
				workflowMetrics:    metrics,
			}
			state := newState(orch.cfg)
			state.Blocked[issue.ID] = Blocked{Issue: issue, BlockedAt: tt.currentEntryAt, Source: BlockedSourceProjectStatus}
			if tt.currentEntryAt.After(parkedAt) {
				if _, err := metrics.RecordWorkflowPhaseEvent(t.Context(), store.WorkflowPhaseEvent{
					ProjectID:         defaultWorkflowMetricsProjectID,
					IssueID:           issue.ID,
					Identifier:        issue.Identifier,
					PhaseType:         store.WorkflowPhaseTypeLane,
					PhaseName:         blockedStatusState,
					PreviousPhaseName: tt.sourceState,
					Reason:            "instant_fail_circuit_breaker",
					Status:            "entered",
					StartedAt:         parkedAt,
				}); err != nil {
					t.Fatalf("RecordWorkflowPhaseEvent(original park) error = %v", err)
				}
			}
			if _, err := metrics.RecordWorkflowPhaseEvent(t.Context(), store.WorkflowPhaseEvent{
				ProjectID:         defaultWorkflowMetricsProjectID,
				IssueID:           issue.ID,
				Identifier:        issue.Identifier,
				PhaseType:         store.WorkflowPhaseTypeLane,
				PhaseName:         blockedStatusState,
				PreviousPhaseName: tt.sourceState,
				Reason:            tt.currentEntryReason,
				Status:            "entered",
				StartedAt:         tt.currentEntryAt,
			}); err != nil {
				t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
			}

			recovery := BackendRecovery{
				Outage:      BackendOutage{Scope: scope, DetectedAt: parkedAt.Add(-time.Minute)},
				RecoveredAt: parkedAt.Add(5 * time.Minute),
			}
			state.BackendRecoveries[scope.Key()] = recovery
			for notification := range tt.recoveryNotifications {
				if notification > 0 && tt.reinsertRecovery {
					state.BackendRecoveries[scope.Key()] = recovery
				}
				orch.recoverBackendCapacityBlockedIssues(t.Context(), &state, []connector.Issue{issue}, recovery.RecoveredAt.Add(time.Duration(notification)*time.Minute))
			}

			if len(tracker.updates) != tt.wantTransitions {
				t.Fatalf("updates = %#v, want %d transition(s)", tracker.updates, tt.wantTransitions)
			}
			if tt.wantTransitions > 0 {
				if tracker.updates[0].state != tt.wantState {
					t.Fatalf("updates = %#v, want target %s", tracker.updates, tt.wantState)
				}
				if len(tracker.comments) != 1 || !strings.Contains(tracker.comments[0], "reason: backend_capacity_recovered") {
					t.Fatalf("comments = %#v, want one machine-classifiable recovery comment", tracker.comments)
				}
				if _, ok := state.Blocked[issue.ID]; ok {
					t.Fatalf("Blocked[%q] still present after recovery", issue.ID)
				}
			} else {
				if _, ok := state.Blocked[issue.ID]; !ok {
					t.Fatalf("Blocked[%q] missing after suppressed recovery", issue.ID)
				}
				if len(tracker.comments) != 0 {
					t.Fatalf("comments = %#v, want no recovery comment", tracker.comments)
				}
			}
			if got := countStateEvents(state.RecentEvents, "backend_capacity_blocked_recovery_suppressed"); tt.wantSuppression && got != 1 {
				t.Fatalf("suppression events = %d, want one bounded diagnostic", got)
			}
			if tt.wantDiagnostic != "" && !stateEventMessageContains(state.RecentEvents, "backend_capacity_blocked_recovery_suppressed", tt.wantDiagnostic) {
				t.Fatalf("suppression events = %#v, want diagnostic containing %q", state.RecentEvents, tt.wantDiagnostic)
			}
			if tt.wantSuppression {
				cloned := state.clone()
				clonedRecovery := cloned.BackendRecoveries[scope.Key()]
				clonedRecovery.SuppressedIssues[issue.ID] = "mutated clone"
				if state.BackendRecoveries[scope.Key()].SuppressedIssues[issue.ID] == "mutated clone" {
					t.Fatal("State.clone() shared backend recovery suppression memory")
				}
			}
		})
	}
}

func TestRecoverBackendCapacityOutageEraBreakerParks(t *testing.T) {
	t.Parallel()

	parkedAt := time.Date(2026, 8, 19, 22, 0, 0, 0, time.UTC)
	detectedAt := parkedAt.Add(-3 * time.Minute)
	recoveredAt := parkedAt.Add(5 * time.Minute)
	scope := backendcapacity.Scope{BackendID: "claude-video", BackendKind: "claude_code", Provider: "anthropic"}
	otherScope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
	otherProviderScope := backendcapacity.Scope{BackendID: scope.BackendID, BackendKind: scope.BackendKind, Provider: "openai"}
	withScope := func(attempt store.WorkAttempt, attemptScope backendcapacity.Scope) store.WorkAttempt {
		attempt.RuntimeIdentity.BackendID = attemptScope.BackendID
		attempt.RuntimeIdentity.BackendKind = attemptScope.BackendKind
		attempt.RuntimeIdentity.Provider.Value = attemptScope.Provider
		return attempt
	}
	artifactAttempt := func(completedAt time.Time, consecutive int, tripped bool) store.WorkAttempt {
		return withScope(store.WorkAttempt{
			Status:        store.WorkAttemptStatusTerminal,
			TerminalState: store.WorkAttemptTerminalSuccess,
			CompletedAt:   completedAt,
			WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
				artifactGateConvergenceMetadataKey: artifactGateConvergenceRecord{
					StatusField:          "Artifact Status",
					DispatchStatus:       "Pending",
					CompletionStatus:     "Pending",
					Unchanged:            true,
					ConsecutiveUnchanged: consecutive,
					Limit:                artifactGateConvergenceLimit,
					Tripped:              tripped,
				},
			}),
		}, scope)
	}
	mismatchedArtifact := artifactAttempt(parkedAt.Add(-time.Minute), 2, false)
	mismatchedArtifact.WorkerMetadataJSON = marshalWorkAttemptJSON(map[string]any{
		artifactGateConvergenceMetadataKey: artifactGateConvergenceRecord{
			StatusField:          "Artifact Status",
			DispatchStatus:       "Pending",
			CompletionStatus:     "Changed",
			Unchanged:            true,
			ConsecutiveUnchanged: 2,
			Limit:                artifactGateConvergenceLimit,
		},
	})
	tests := []struct {
		name            string
		reason          string
		attempts        []store.WorkAttempt
		historyErr      error
		wantTransition  bool
		wantSuppression string
	}{
		{
			name:   "token ceiling evidence entirely inside outage",
			reason: "token_ceiling_circuit_breaker",
			attempts: []store.WorkAttempt{withScope(store.WorkAttempt{
				Status:        store.WorkAttemptStatusTerminal,
				TerminalState: store.WorkAttemptTerminalFailure,
				ErrorClass:    "runner_error",
				ErrorMessage:  "session token ceiling exceeded: total_tokens=16100000 ceiling_tokens=16000000",
				CompletedAt:   parkedAt,
			}, scope)},
			wantTransition: true,
		},
		{
			name:   "token ceiling evidence outside outage stays parked",
			reason: "token_ceiling_circuit_breaker",
			attempts: []store.WorkAttempt{withScope(store.WorkAttempt{
				ErrorMessage: "session token ceiling exceeded",
				CompletedAt:  detectedAt.Add(-time.Minute),
			}, scope)},
			wantSuppression: "falls outside",
		},
		{
			name:   "token ceiling evidence from another backend stays parked",
			reason: "token_ceiling_circuit_breaker",
			attempts: []store.WorkAttempt{withScope(store.WorkAttempt{
				ErrorMessage: "session token ceiling exceeded",
				CompletedAt:  parkedAt,
			}, otherScope)},
			wantSuppression: "different backend capacity scope",
		},
		{
			name:   "token ceiling evidence from another provider stays parked",
			reason: "token_ceiling_circuit_breaker",
			attempts: []store.WorkAttempt{withScope(store.WorkAttempt{
				ErrorMessage: "session token ceiling exceeded",
				CompletedAt:  parkedAt,
			}, otherProviderScope)},
			wantSuppression: "different backend capacity scope",
		},
		{
			name:   "token ceiling evidence without runtime identity stays parked",
			reason: "token_ceiling_circuit_breaker",
			attempts: []store.WorkAttempt{{
				ErrorMessage: "session token ceiling exceeded",
				CompletedAt:  parkedAt,
			}},
			wantSuppression: "different backend capacity scope",
		},
		{
			name:   "non token ceiling evidence stays parked",
			reason: "token_ceiling_circuit_breaker",
			attempts: []store.WorkAttempt{withScope(store.WorkAttempt{
				ErrorMessage: "ordinary runner failure",
				CompletedAt:  parkedAt,
			}, scope)},
			wantSuppression: "not a token-ceiling",
		},
		{
			name:   "artifact convergence evidence entirely inside outage",
			reason: artifactGateConvergenceReason,
			attempts: []store.WorkAttempt{
				artifactAttempt(parkedAt, 3, true),
				artifactAttempt(parkedAt.Add(-time.Minute), 2, false),
				artifactAttempt(parkedAt.Add(-2*time.Minute), 1, false),
			},
			wantTransition: true,
		},
		{
			name:   "artifact convergence with pre-outage evidence stays parked",
			reason: artifactGateConvergenceReason,
			attempts: []store.WorkAttempt{
				artifactAttempt(parkedAt, 3, true),
				artifactAttempt(parkedAt.Add(-time.Minute), 2, false),
				artifactAttempt(detectedAt.Add(-time.Minute), 1, false),
			},
			wantSuppression: "falls outside",
		},
		{
			name:   "artifact convergence from another backend stays parked",
			reason: artifactGateConvergenceReason,
			attempts: []store.WorkAttempt{
				withScope(artifactAttempt(parkedAt, 3, true), otherScope),
				artifactAttempt(parkedAt.Add(-time.Minute), 2, false),
				artifactAttempt(parkedAt.Add(-2*time.Minute), 1, false),
			},
			wantSuppression: "different backend capacity scope",
		},
		{
			name:   "artifact convergence with older evidence from another backend stays parked",
			reason: artifactGateConvergenceReason,
			attempts: []store.WorkAttempt{
				artifactAttempt(parkedAt, 3, true),
				withScope(artifactAttempt(parkedAt.Add(-time.Minute), 2, false), otherScope),
				artifactAttempt(parkedAt.Add(-2*time.Minute), 1, false),
			},
			wantSuppression: "different backend capacity scope",
		},
		{
			name:            "artifact convergence without history stays parked",
			reason:          artifactGateConvergenceReason,
			wantSuppression: "history is unavailable",
		},
		{
			name:            "artifact convergence incomplete history stays parked",
			reason:          artifactGateConvergenceReason,
			attempts:        []store.WorkAttempt{artifactAttempt(parkedAt, 3, true)},
			wantSuppression: "evidence is incomplete",
		},
		{
			name:   "artifact convergence mismatched history stays parked",
			reason: artifactGateConvergenceReason,
			attempts: []store.WorkAttempt{
				artifactAttempt(parkedAt, 3, true),
				mismatchedArtifact,
				artifactAttempt(parkedAt.Add(-2*time.Minute), 1, false),
			},
			wantSuppression: "evidence is incomplete",
		},
		{
			name:            "attempt history lookup failure stays parked",
			reason:          artifactGateConvergenceReason,
			historyErr:      errors.New("history unavailable"),
			wantSuppression: "lookup failed",
		},
		{
			name:            "independent breaker stays parked",
			reason:          "operator_hold",
			wantSuppression: "independent cause",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stageUpdatedAt := parkedAt
			issue := connector.Issue{
				ID:             "issue-" + strings.ReplaceAll(tt.name, " ", "-"),
				Identifier:     "digitaldrywood/video#99",
				State:          blockedStatusState,
				StageUpdatedAt: &stageUpdatedAt,
				Comments:       []connector.IssueComment{{Body: "ordinary operator note"}},
				WorkpadSignal: &workpad.Signal{
					Source: workpad.SourceStructured,
					Status: workpad.StatusInProgress,
				},
			}
			tracker := &backendCapacityTestConnector{}
			metrics := &autoPromoteWorkflowMetricsRecorder{}
			attemptStore := &recordingWorkAttemptStore{history: tt.attempts, historyErr: tt.historyErr}
			orch := &Orchestrator{
				cfg:                normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "video"}, ActiveStates: []string{"In Progress", "Rework"}, TerminalStates: []string{"Done"}}),
				connector:          tracker,
				capacityController: backendCapacityTestController{scope: scope},
				workflowMetrics:    metrics,
				workAttempts:       attemptStore,
			}
			if _, err := metrics.RecordWorkflowPhaseEvent(t.Context(), store.WorkflowPhaseEvent{
				ProjectID:         "video",
				IssueID:           issue.ID,
				Identifier:        issue.Identifier,
				PhaseType:         store.WorkflowPhaseTypeLane,
				PhaseName:         blockedStatusState,
				PreviousPhaseName: "In Progress",
				Reason:            tt.reason,
				Status:            "entered",
				StartedAt:         parkedAt,
			}); err != nil {
				t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
			}
			state := newState(orch.cfg)
			state.Blocked[issue.ID] = Blocked{Issue: issue, BlockedAt: parkedAt, Source: BlockedSourceProjectStatus}
			state.BackendRecoveries[scope.Key()] = BackendRecovery{
				Outage: BackendOutage{
					Scope:      scope,
					DetectedAt: detectedAt,
					Reason:     "subscription window exhausted",
				},
				RecoveredAt: recoveredAt,
			}

			transitioned := orch.recoverBackendCapacityBlockedIssues(t.Context(), &state, []connector.Issue{issue}, recoveredAt)
			if tt.wantTransition {
				if _, ok := transitioned[issue.ID]; !ok || len(tracker.updates) != 1 || tracker.updates[0].state != "In Progress" {
					t.Fatalf("recovery = transitioned %#v updates %#v, want In Progress", transitioned, tracker.updates)
				}
				if _, blocked := state.Blocked[issue.ID]; blocked {
					t.Fatalf("Blocked[%q] remains after outage-era breaker recovery", issue.ID)
				}
				return
			}
			if len(transitioned) != 0 || len(tracker.updates) != 0 {
				t.Fatalf("recovery = transitioned %#v updates %#v, want sticky park", transitioned, tracker.updates)
			}
			recovery := state.BackendRecoveries[scope.Key()]
			if got := recovery.SuppressedIssues[issue.ID]; !strings.Contains(got, tt.wantSuppression) {
				t.Fatalf("suppression = %q, want %q", got, tt.wantSuppression)
			}
		})
	}
}

func TestBackendCapacityBreakerRecoveryRequiresDurableProvenance(t *testing.T) {
	t.Parallel()

	orch := &Orchestrator{}
	issue := connector.Issue{ID: "issue-no-history", State: blockedStatusState}
	if _, reason := orch.backendCapacityBreakerRecoveryTarget(t.Context(), issue, BackendRecovery{}); !strings.Contains(reason, "provenance is unavailable") {
		t.Fatalf("recovery target suppression = %q, want missing provenance", reason)
	}
	if ok, reason := orch.backendCapacityBreakerEvidenceWithinOutage(t.Context(), issue, "token_ceiling_circuit_breaker", BackendRecovery{}); ok || !strings.Contains(reason, "history is unavailable") {
		t.Fatalf("recovery evidence = (%v, %q), want missing history", ok, reason)
	}
}

func TestBackendCapacityBlockedRecoveryTargetBoundaries(t *testing.T) {
	t.Parallel()

	entryAt := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	commentAt := entryAt.Add(time.Second)
	staleCommentAt := entryAt.Add(-time.Minute)
	updatedCommentAt := entryAt.Add(2 * time.Second)
	tests := []struct {
		name           string
		entryPhase     string
		entryReason    string
		sourceState    string
		commentTime    string
		blockedBy      []connector.BlockedRef
		workpadSignal  *workpad.Signal
		terminalStates []string
		recoveredAt    time.Time
		wantTarget     string
		wantReason     string
	}{
		{
			name:        "missing durable entry",
			commentTime: "created",
			wantReason:  "provenance is unavailable",
		},
		{
			name:        "latest entry is not Blocked",
			entryPhase:  "Rework",
			entryReason: "backend_capacity_recovered",
			sourceState: "Blocked",
			commentTime: "created",
			wantReason:  "not the current Blocked entry",
		},
		{
			name:        "capacity evidence timestamp is missing",
			entryPhase:  "Blocked",
			entryReason: "instant_fail_circuit_breaker",
			sourceState: "Rework",
			commentTime: "missing",
			wantReason:  "has no timestamp",
		},
		{
			name:        "no progress park is never capacity recovered",
			entryPhase:  "Blocked",
			entryReason: noProgressLimitReason,
			sourceState: "Rework",
			commentTime: "created",
			wantReason:  "independent cause " + noProgressLimitReason,
		},
		{
			name:        "capacity evidence predates entry",
			entryPhase:  "Blocked",
			entryReason: "instant_fail_circuit_breaker",
			sourceState: "Rework",
			commentTime: "stale",
			wantReason:  "predates the current Blocked entry",
		},
		{
			name:        "recovery predates current capacity entry",
			entryPhase:  "Blocked",
			entryReason: "instant_fail_circuit_breaker",
			sourceState: "Rework",
			commentTime: "created",
			recoveredAt: entryAt.Add(-time.Second),
			wantReason:  "recovery predates the current Blocked entry evidence",
		},
		{
			name:        "dependency blocker",
			entryPhase:  "Blocked",
			entryReason: "instant_fail_circuit_breaker",
			sourceState: "Rework",
			commentTime: "created",
			blockedBy:   []connector.BlockedRef{{Identifier: "digitaldrywood/detent#1880"}},
			wantReason:  "dependency blockers",
		},
		{
			name:        "invalid structured Workpad",
			entryPhase:  "Blocked",
			entryReason: "instant_fail_circuit_breaker",
			sourceState: "Rework",
			commentTime: "created",
			workpadSignal: &workpad.Signal{
				Source:  workpad.SourceStructured,
				Invalid: &workpad.Invalid{Message: "invalid status block"},
			},
			wantReason: "status is invalid",
		},
		{
			name:        "independent typed Workpad blocker",
			entryPhase:  "Blocked",
			entryReason: "instant_fail_circuit_breaker",
			sourceState: "Rework",
			commentTime: "created",
			workpadSignal: &workpad.Signal{
				Source:   workpad.SourceStructured,
				Status:   workpad.StatusInProgress,
				Blockers: []workpad.Blocker{{Reason: "browser state unavailable"}},
			},
			wantReason: "independent typed blockers",
		},
		{
			name:        "independent structured blocked reason",
			entryPhase:  "Blocked",
			entryReason: "instant_fail_circuit_breaker",
			sourceState: "Rework",
			commentTime: "created",
			workpadSignal: &workpad.Signal{
				Source:     workpad.SourceStructured,
				Status:     workpad.StatusBlocked,
				ReasonCode: blockedRecoveryReasonMergeConflict,
			},
			wantReason: "independent blocked reason",
		},
		{
			name:           "captured terminal lane",
			entryPhase:     "Blocked",
			entryReason:    "instant_fail_circuit_breaker",
			sourceState:    "Done",
			commentTime:    "created",
			terminalStates: []string{"Done"},
			wantReason:     "no recoverable captured source lane",
		},
		{
			name:        "updated timestamp fallback",
			entryPhase:  "Blocked",
			entryReason: "repeated_failure_circuit_breaker",
			sourceState: "Todo",
			commentTime: "updated",
			wantTarget:  "Todo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := connector.Issue{
				ID:             "issue-boundary",
				Identifier:     "digitaldrywood/detent#1879",
				State:          "Blocked",
				StageUpdatedAt: timePointer(entryAt),
				BlockedBy:      tt.blockedBy,
				WorkpadSignal:  tt.workpadSignal,
			}
			comment := connector.IssueComment{}
			switch tt.commentTime {
			case "created":
				comment.CreatedAt = &commentAt
			case "stale":
				comment.CreatedAt = &staleCommentAt
			case "updated":
				comment.UpdatedAt = &updatedCommentAt
			}
			metrics := &autoPromoteWorkflowMetricsRecorder{}
			if tt.entryPhase != "" {
				if _, err := metrics.RecordWorkflowPhaseEvent(t.Context(), store.WorkflowPhaseEvent{
					ProjectID:         defaultWorkflowMetricsProjectID,
					IssueID:           issue.ID,
					Identifier:        issue.Identifier,
					PhaseType:         store.WorkflowPhaseTypeLane,
					PhaseName:         tt.entryPhase,
					PreviousPhaseName: tt.sourceState,
					Reason:            tt.entryReason,
					Status:            "entered",
					StartedAt:         entryAt,
				}); err != nil {
					t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
				}
			}
			orch := &Orchestrator{
				cfg:             normalizeConfig(Config{TerminalStates: tt.terminalStates}),
				workflowMetrics: metrics,
			}
			target, reason := orch.backendCapacityBlockedRecoveryTarget(t.Context(), issue, comment, tt.recoveredAt)
			if target != tt.wantTarget || !strings.Contains(reason, tt.wantReason) {
				t.Fatalf("backendCapacityBlockedRecoveryTarget() = %q, %q, want %q and reason containing %q", target, reason, tt.wantTarget, tt.wantReason)
			}
		})
	}
}

func TestLegacyFailureBreakerBackendErrorExtractsTypedEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "inline provider body",
			body: `Detent stopped retrying this worker after 5 consecutive instant failures with the same backend error. backend_error_body: {"error":{"type":"usageLimitExceeded"}}`,
			want: `{"error":{"type":"usageLimitExceeded"}}`,
		},
		{
			name: "fenced provider body",
			body: "Detent stopped retrying this worker after 5 consecutive instant failures with the same backend error.\n\n- backend_error_body:\n\n```json\n{\"status_code\":429}\n```\n\noperator guidance",
			want: `{"status_code":429}`,
		},
		{
			name: "narrative capacity phrase without provider body",
			body: "Detent stopped retrying this worker after 5 consecutive instant failures with the same backend error. The issue instructions mention quota exceeded.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err, ok := LegacyFailureBreakerBackendError(tt.body)
			if (tt.want != "") != ok {
				t.Fatalf("LegacyFailureBreakerBackendError() ok = %v, want %v", ok, tt.want != "")
			}
			if ok && err.Error() != tt.want {
				t.Fatalf("LegacyFailureBreakerBackendError() = %q, want %q", err, tt.want)
			}
			if ok {
				carrier := err.(interface {
					BackendErrorBody() string
					BackendErrorMessage() string
					BackendErrorStatus() string
				})
				if carrier.BackendErrorBody() != tt.want || carrier.BackendErrorMessage() != "" || carrier.BackendErrorStatus() != "" {
					t.Fatalf("backend error evidence = body %q message %q status %q", carrier.BackendErrorBody(), carrier.BackendErrorMessage(), carrier.BackendErrorStatus())
				}
			}
		})
	}
}

func TestRecoverBackendCapacityBlockedIssuesIgnoresUnrelatedQuotaComment(t *testing.T) {
	t.Parallel()

	issue := connector.Issue{
		ID:         "issue-unrelated-comment",
		Identifier: "digitaldrywood/detent#1143",
		State:      "Blocked",
		Comments: []connector.IssueComment{{
			Body: "A user mentioned usageLimitExceeded while documenting an unrelated blocker.",
		}},
	}
	tracker := &backendCapacityTestConnector{}
	orch := &Orchestrator{
		cfg:                normalizeConfig(Config{ActiveStates: []string{"Todo", "In Progress", "Rework"}}),
		connector:          tracker,
		capacityController: backendCapacityTestController{scope: backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}},
	}
	state := newState(orch.cfg)

	transitioned := orch.recoverBackendCapacityBlockedIssues(t.Context(), &state, []connector.Issue{issue}, time.Now())
	if len(transitioned) != 0 || len(tracker.updates) != 0 || len(state.BackendOutages) != 0 {
		t.Fatalf("unrelated quota comment triggered recovery: transitioned %#v updates %#v outages %#v", transitioned, tracker.updates, state.BackendOutages)
	}
}

type backendCapacityTestController struct {
	scope     backendcapacity.Scope
	status    runpkg.CapacityStatus
	hasStatus bool
}

func (c backendCapacityTestController) CapacityScope(runpkg.RunRequest) (backendcapacity.Scope, bool) {
	return c.scope, true
}

func (c backendCapacityTestController) ValidatorCapacityScope(runpkg.ValidatorRequest) (backendcapacity.Scope, bool) {
	return c.scope, true
}

func (c backendCapacityTestController) BackendCapacityStatus(
	backendcapacity.Scope,
	*telemetry.RateLimits,
) (runpkg.CapacityStatus, bool) {
	return c.status, c.hasStatus
}

func (c backendCapacityTestController) ClassifyCapacityError(
	_ runpkg.RunRequest,
	err error,
	limits *telemetry.RateLimits,
	now time.Time,
) (*backendcapacity.Error, bool) {
	var fallback *time.Time
	if limits != nil && limits.Primary != nil {
		fallback = limits.Primary.ResetAt
	}
	details, ok := backendcapacity.Classify(err.Error(), fallback, now, backendcapacity.Rules{Kinds: []string{"usageLimitExceeded"}})
	if !ok || !c.scope.Hosted() {
		return nil, false
	}
	capacityErr, ok := backendcapacity.As(backendcapacity.NewError(c.scope, details, err))
	return capacityErr, ok
}

type backendCapacityTestUpdate struct {
	issueID string
	state   string
}

type backendCapacityTestConnector struct {
	updates  []backendCapacityTestUpdate
	comments []string
}

func (c *backendCapacityTestConnector) Name() string {
	return "backend-capacity-test"
}

func (c *backendCapacityTestConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	return nil, nil
}

func (c *backendCapacityTestConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *backendCapacityTestConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (c *backendCapacityTestConnector) CreateComment(_ context.Context, _ string, body string) error {
	c.comments = append(c.comments, body)
	return nil
}

func (c *backendCapacityTestConnector) UpdateIssueState(_ context.Context, issueID string, state string) error {
	c.updates = append(c.updates, backendCapacityTestUpdate{issueID: issueID, state: state})
	return nil
}

func (c *backendCapacityTestConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *backendCapacityTestConnector) SetField(context.Context, string, string, string) error {
	return nil
}

type backendCapacityTestValidator struct {
	requests chan ValidatorRequest
	err      error
}

func (v *backendCapacityTestValidator) Validate(_ context.Context, request ValidatorRequest) (gate.ValidatorResult, error) {
	v.requests <- request
	return gate.ValidatorResult{}, v.err
}

func TestOperatorCapacityRecoveryModes(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		immediate bool
		filter    string
		wantRamp  bool
	}{
		{"default ramp", false, "codex", true},
		{"immediate backend", true, "codex", false},
		{"immediate provider", true, "openai", false},
		{"immediate pair", true, "codex/openai", false},
		{"unmatched scope", true, "missing", true},
		{"all scopes", true, "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
			cfg := normalizeConfig(Config{MaxConcurrentAgents: 10})
			orch := &Orchestrator{cfg: cfg}
			state := newState(cfg)
			scope := backendcapacity.Scope{BackendID: "codex", BackendKind: "codex", Provider: "openai"}
			orch.capacityController = backendCapacityTestController{scope: scope}
			var logs bytes.Buffer
			orch.logger = slog.New(slog.NewJSONHandler(&logs, nil))
			outage := BackendOutage{Scope: scope, ResumeAt: now.Add(time.Hour)}
			state.BackendOutages[scope.Key()] = outage
			state.Running["active"] = Running{Issue: connector.Issue{ID: "active"}, CapacityScope: scope}
			orch.activateBackendDispatchRecovery(&state, outage, now)
			for range 2 {
				orch.clearBackendCapacity(&state, tt.filter, now, tt.immediate)
				_, first, _ := tryReserveDispatchRecovery(&state, "one", now)
				_, second, _ := tryReserveDispatchRecovery(&state, "two", now)
				if !first || second == tt.wantRamp {
					t.Fatalf("admissions = %v, %v; want ramp %v", first, second, tt.wantRamp)
				}
				releaseDispatchRecoveryAdmission(&state, "one")
				releaseDispatchRecoveryAdmission(&state, "two")
			}
			if len(state.Running) != 1 || orch.cfg.MaxConcurrentAgents != 10 {
				t.Fatal("active workers or configured concurrency changed")
			}
			if !tt.wantRamp {
				orch.recoverBackendCapacity(&state, Running{CapacityScope: scope, CapacityProbe: true}, now)
				if reason := dispatchRecoveryBlockReason(&state, now); reason != "" {
					t.Fatalf("active probe reintroduced recovery: %s", reason)
				}
				if _, _, blocked := orch.backendCapacityDispatch(&state, runpkg.RunRequest{}, now); blocked {
					t.Fatal("cleared outage still blocks dispatch")
				}
				if !strings.Contains(logs.String(), `"recovery_applied":"immediate"`) {
					t.Fatalf("application evidence missing: %s", logs.String())
				}
				for i := range 10 {
					if _, allowed, reason := tryReserveDispatchRecovery(&state, strconv.Itoa(i), now); !allowed {
						t.Fatalf("admission %d refused: %s", i, reason)
					}
				}
			}
			renewed := orch.registerBackendOutage(&state, &backendcapacity.Error{Scope: scope, Details: backendcapacity.Details{Type: backendcapacity.ErrorTypeUsageLimit, Reason: "renewed quota limit"}}, now, false)
			if _, ok := state.BackendOutages[scope.Key()]; !ok || !renewed.ResumeAt.After(now) {
				t.Fatal("renewed outage did not pause capacity")
			}
			if _, _, blocked := orch.backendCapacityDispatch(&state, runpkg.RunRequest{}, now); !blocked {
				t.Fatal("renewed outage permits dispatch")
			}
			orch.completeBackendCapacityRecovery(&state, scope.Key(), renewed, now.Add(time.Hour), "live_status")
			_, first, _ := tryReserveDispatchRecovery(&state, "new-one", now.Add(time.Hour))
			_, second, _ := tryReserveDispatchRecovery(&state, "new-two", now.Add(time.Hour))
			if !first || second {
				t.Fatal("automatic recovery did not restore cautious ramp")
			}
		})
	}
}

func TestImmediateCapacityClearPreservesOtherHolds(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{dispatchRecoveryGitHubREST, dispatchRecoveryTrackerUnavailable, dispatchRecoveryProjectFailureBreaker, dispatchRecoveryBackendCapacity} {
		t.Run(kind, func(t *testing.T) {
			now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
			cfg := normalizeConfig(Config{MaxConcurrentAgents: 10})
			orch := &Orchestrator{cfg: cfg}
			state := newState(cfg)
			codex := BackendOutage{Scope: backendcapacity.Scope{BackendID: "codex", Provider: "openai"}}
			other := BackendOutage{Scope: backendcapacity.Scope{BackendID: "claude", Provider: "anthropic"}}
			state.BackendOutages[other.Scope.Key()] = other
			orch.activateBackendDispatchRecovery(&state, codex, now)
			if kind == dispatchRecoveryBackendCapacity {
				orch.activateBackendDispatchRecovery(&state, other, now)
			} else {
				orch.activateDispatchRecovery(&state, kind, "unrelated hold", now, "")
			}
			state.FailureBreaker.Class = "human hold"
			orch.clearBackendCapacity(&state, "codex", now, true)
			if len(state.DispatchRecoveries) != 1 || len(state.BackendOutages) != 1 || state.FailureBreaker.Class != "human hold" {
				t.Fatalf("unrelated holds changed: %#v", state.DispatchRecoveries)
			}
			_, first, _ := tryReserveDispatchRecovery(&state, "one", now)
			_, second, reason := tryReserveDispatchRecovery(&state, "two", now)
			if !first || second || reason != kind+"_recovery" {
				t.Fatalf("other recovery no longer limits admissions: %v %v %s", first, second, reason)
			}
		})
	}
}

func TestCapacityClearQueueRecoveryMode(t *testing.T) {
	for _, immediate := range []bool{false, true} {
		t.Run(strconv.FormatBool(immediate), func(t *testing.T) {
			orch := &Orchestrator{capacityClearRequests: make(chan capacityClearRequest, 1), done: make(chan struct{})}
			if err := orch.RequestBackendCapacityClear(t.Context(), " codex ", immediate); err != nil {
				t.Fatal(err)
			}
			request := <-orch.capacityClearRequests
			if request.scope != "codex" || request.immediate != immediate {
				t.Fatalf("request = %#v", request)
			}
		})
	}
}

func TestCapacityClearRespectsConfiguredDispatchSlots(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name         string
		immediate    bool
		active, want int
	}{
		{"cautious", false, 0, 1},
		{"full", true, 0, 10},
		{"active workers", true, 3, 7},
		{"already full", true, 10, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
			cfg := normalizeConfig(Config{MaxConcurrentAgents: 10, ActiveStates: []string{"Todo"}, TerminalStates: []string{"Done"}})
			orch := &Orchestrator{cfg: cfg}
			state := newState(cfg)
			scope := backendcapacity.Scope{BackendID: "codex", Provider: "openai"}
			state.BackendOutages[scope.Key()] = BackendOutage{Scope: scope}
			for i := range tt.active {
				id := fmt.Sprintf("active-%d", i)
				state.Running[id] = Running{Issue: dispatchTestIssue(id, "Todo")}
			}
			orch.clearBackendCapacity(&state, "codex", now, tt.immediate)
			issues := make([]connector.Issue, 12)
			for i := range issues {
				issues[i] = dispatchTestIssue(fmt.Sprintf("ready-%d", i), "Todo")
			}
			admitted := 0
			orch.dispatchPlanner().plan(&state, issues, now, dispatchPlanHooks{dispatch: func(action dispatchAction) bool {
				_, allowed, _ := tryReserveDispatchRecovery(&state, action.issue.ID, now)
				if allowed {
					admitted++
					state.Running[action.issue.ID] = Running{Issue: action.issue}
				}
				return allowed
			}})
			if admitted != tt.want {
				t.Fatalf("admitted = %d, want %d", admitted, tt.want)
			}
		})
	}
}
