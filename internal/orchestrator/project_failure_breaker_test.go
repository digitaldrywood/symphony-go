package orchestrator_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/orchestrator"
)

func TestRunPausesProjectAfterCorrelatedFailuresAcrossIssues(t *testing.T) {
	t.Parallel()

	issues := make([]connector.Issue, 0, 10)
	for number := 1; number <= 10; number++ {
		issues = append(issues, testIssue(fmt.Sprintf("issue-%d", number), fmt.Sprintf("digitaldrywood/detent#%d", number), "Todo"))
	}
	tracker := newFakeConnector(issues...)
	runner := &staticRunner{result: orchestrator.RunResult{TurnStarted: true}, err: errors.New("systemic backend failure")}
	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:          time.Millisecond,
		MaxConcurrentAgents:   1,
		MaxRetryBackoff:       time.Hour,
		FailureRetryBaseDelay: time.Hour,
		FailureBreaker: orchestrator.FailureBreakerConfig{
			SameClassLimit: 5,
			Window:         time.Hour,
			Cooldown:       time.Hour,
		},
		ActiveStates:           []string{"Todo", "In Progress"},
		ObservedStates:         []string{"Blocked"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{Connector: tracker, Runner: runner})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		return state.FailureBreaker.Active()
	})
	if state.FailureBreaker.Count != 5 {
		t.Fatalf("FailureBreaker.Count = %d, want 5", state.FailureBreaker.Count)
	}
	if got := runner.calls.Load(); got != 5 {
		t.Fatalf("runner calls = %d, want 5", got)
	}
	requests := runner.requests()
	seen := make(map[string]struct{}, len(requests))
	for _, request := range requests {
		seen[request.Issue.ID] = struct{}{}
	}
	if len(seen) != 5 {
		t.Fatalf("failed issue count = %d, want 5 distinct issues", len(seen))
	}

	fetches := tracker.fetchCandidateCalls()
	waitForFetchCalls(t, tracker, fetches+2)
	if got := runner.calls.Load(); got != 5 {
		t.Fatalf("runner calls after intake pause = %d, want 5", got)
	}
	state = waitForState(t, orch, func(state orchestrator.State) bool {
		for _, decision := range state.SchedulerDecisions {
			if decision.Reason == "project_failure_breaker_paused" {
				return true
			}
		}
		return false
	})
	snapshot := state.Snapshot(time.Now())
	if len(snapshot.FailureBreakers) != 1 || snapshot.FailureBreakers[0].Class == "" {
		t.Fatalf("snapshot failure breakers = %#v, want visible active reason", snapshot.FailureBreakers)
	}

	canary, err := orch.RequestProjectFailureBreakerCanary(t.Context())
	if err != nil {
		t.Fatalf("RequestProjectFailureBreakerCanary() error = %v", err)
	}
	if !canary.Requested || !canary.Active {
		t.Fatalf("RequestProjectFailureBreakerCanary() = %#v, want immediate canary", canary)
	}
	state = waitForState(t, orch, func(state orchestrator.State) bool {
		return runner.calls.Load() >= 10 && state.FailureBreaker.Active() && state.FailureBreaker.Count == 5 && state.FailureBreaker.CanaryIssueID == ""
	})
	if got := runner.calls.Load(); got != 10 {
		t.Fatalf("runner calls after immediate canary = %d, want 10", got)
	}
	fetches = tracker.fetchCandidateCalls()
	waitForFetchCalls(t, tracker, fetches+2)
	if got := runner.calls.Load(); got != 10 {
		t.Fatalf("runner calls after failed canary cooldown = %d, want 10", got)
	}
	if !state.FailureBreaker.ResumeAt.After(time.Now()) {
		t.Fatalf("FailureBreaker.ResumeAt = %s, want renewed cooldown", state.FailureBreaker.ResumeAt)
	}
}
