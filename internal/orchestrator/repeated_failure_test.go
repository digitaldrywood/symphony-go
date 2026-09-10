package orchestrator_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/orchestrator"
)

// tokenCeilingRunner fails every attempt with an error whose text varies per
// call, the way a session token ceiling failure reports a different
// total_tokens each attempt. The varying text keeps the instant-failure
// breaker from matching consecutive errors, which is exactly the gap the
// repeated-failure breaker covers.
type tokenCeilingRunner struct {
	calls        atomic.Int64
	failureCount int64
}

type restBudgetHeadroomRunner struct {
	calls   atomic.Int64
	resetAt time.Time
}

func (r *restBudgetHeadroomRunner) Run(_ context.Context, _ orchestrator.RunRequest) (orchestrator.RunResult, error) {
	n := r.calls.Add(1)
	return orchestrator.RunResult{}, fmt.Errorf(
		"run agent turn: worker github REST budget reached reserved headroom: consumer=shared_pool credential_identity=github-rest:worker remaining=%d reserve=1250 reset_at=%s",
		950-n, r.resetAt.Format(time.RFC3339),
	)
}

func (r *tokenCeilingRunner) Run(_ context.Context, _ orchestrator.RunRequest) (orchestrator.RunResult, error) {
	n := r.calls.Add(1)
	if r.failureCount > 0 && n > r.failureCount {
		return orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted}, nil
	}
	return orchestrator.RunResult{TurnStarted: true}, fmt.Errorf(
		"run agent turn: claude update rejected: session token ceiling exceeded: total_tokens=%d ceiling_tokens=2000000 source=max_session_tokens",
		2000000+n,
	)
}

func TestRunParksIssueAfterRepeatedCostlyWorkerFailures(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-repeated-fail", "wi-011cd179bc7ecf36b7197e4b", "Todo")
	tracker := newFakeConnector(issue)
	runner := &tokenCeilingRunner{}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Millisecond,
		FailureRetryBaseDelay:  time.Millisecond,
		ActiveStates:           []string{"Todo", "In Progress"},
		ObservedStates:         []string{"Blocked"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Blocked[issue.ID]
		return ok
	})

	if got := runner.calls.Load(); got != 5 {
		t.Fatalf("runner calls = %d, want 5", got)
	}
	reason := state.Blocked[issue.ID].Reason
	if !strings.Contains(reason, "repeated failure circuit breaker: ") {
		t.Fatalf("Blocked[%q].Reason = %q, want repeated failure circuit breaker prefix", issue.ID, reason)
	}
	if !strings.Contains(reason, "session token ceiling exceeded") {
		t.Fatalf("Blocked[%q].Reason = %q, want last worker error", issue.ID, reason)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after circuit breaker", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after circuit breaker", issue.ID)
	}
	if _, ok := state.RepeatedFailures[issue.ID]; ok {
		t.Fatalf("RepeatedFailures[%q] present after park", issue.ID)
	}
	updates := tracker.stateUpdateCalls()
	if len(updates) == 0 || updates[len(updates)-1] != (stateUpdateCall{issueID: issue.ID, state: "Blocked"}) {
		t.Fatalf("state updates = %#v, want final Blocked transition", updates)
	}
	comments := tracker.commentCalls()
	if len(comments) != 1 {
		t.Fatalf("comments = %#v, want one circuit breaker comment", comments)
	}
	if !strings.Contains(comments[0].body, "consecutive failed attempts") || !strings.Contains(comments[0].body, "session token ceiling exceeded") {
		t.Fatalf("comment body missing repeated failure details:\n%s", comments[0].body)
	}
}

func TestRunRepeatedFailureCounterSurvivesYieldlessCompletion(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-repeated-fail-reset", "digitaldrywood/detent#980", "Todo")
	tracker := newFakeConnector(issue)
	runner := &tokenCeilingRunner{failureCount: 4}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Millisecond,
		FailureRetryBaseDelay:  time.Millisecond,
		ActiveStates:           []string{"Todo", "In Progress"},
		ObservedStates:         []string{"Blocked"},
		TerminalStates:         []string{"Done", "Cancelled", "Canceled", "Closed"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Completed[issue.ID]
		return ok
	})

	if _, ok := state.Blocked[issue.ID]; ok {
		t.Fatalf("Blocked[%q] present after successful completion", issue.ID)
	}
	if got := state.RepeatedFailures[issue.ID].Count; got != 4 {
		t.Fatalf("RepeatedFailures[%q].Count = %d, want 4 after yieldless completion", issue.ID, got)
	}
}

func TestRunDefersRESTBudgetFailureAsCapacityWithoutBreaker(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-rest-budget-park", "digitaldrywood/detent#1824", "Todo")
	tracker := newFakeConnector(issue)
	resetAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	runner := &restBudgetHeadroomRunner{resetAt: resetAt}
	metrics := &workflowMetricsRecorder{}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Millisecond,
		FailureRetryBaseDelay:  time.Millisecond,
		ActiveStates:           []string{"Todo", "In Progress"},
		ObservedStates:         []string{"Blocked"},
		TerminalStates:         []string{"Done", "Cancelled"},
		ContinuationRetryDelay: time.Second,
	}, orchestrator.Dependencies{
		Connector:       tracker,
		Runner:          runner,
		WorkflowMetrics: metrics,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	state := waitForState(t, orch, func(state orchestrator.State) bool {
		_, ok := state.Retry[issue.ID]
		return ok
	})
	stop()

	retry := state.Retry[issue.ID]
	if want := resetAt.Add(5 * time.Second); !retry.DueAt.Equal(want) {
		t.Fatalf("retry DueAt = %s, want %s", retry.DueAt, want)
	}
	if got := runner.calls.Load(); got != 1 {
		t.Fatalf("runner calls = %d, want 1 before reset", got)
	}
	if _, ok := state.Blocked[issue.ID]; ok {
		t.Fatalf("Blocked[%q] present after capacity deferral", issue.ID)
	}
	if _, ok := state.RepeatedFailures[issue.ID]; ok {
		t.Fatalf("RepeatedFailures[%q] present after capacity deferral", issue.ID)
	}
	if state.FailureBreaker.Active() {
		t.Fatalf("FailureBreaker = %#v, want inactive", state.FailureBreaker)
	}
	for _, event := range metrics.snapshot() {
		if event.PhaseName == "Blocked" {
			t.Fatalf("workflow event = %#v, want no breaker transition", event)
		}
	}
	if comments := tracker.commentCalls(); len(comments) != 0 {
		t.Fatalf("comments = %#v, want no breaker explanation", comments)
	}
}

func TestRunDispatchTransitionsTemplateVocabularyIssueToProduction(t *testing.T) {
	t.Parallel()

	issue := testIssue("issue-production-transition", "wi-local-video-1", "Todo")
	tracker := newFakeConnector(issue)
	runner := &staticRunner{
		result: orchestrator.RunResult{FinalState: orchestrator.FinalStateCompleted},
	}

	orch, err := orchestrator.New(orchestrator.Config{
		PollInterval:           time.Millisecond,
		MaxConcurrentAgents:    1,
		MaxRetryBackoff:        time.Hour,
		FailureRetryBaseDelay:  time.Hour,
		ActiveStates:           []string{"Todo", "Production", "Rework"},
		ObservedStates:         []string{"Backlog", "Review", "Blocked"},
		TerminalStates:         []string{"Ready for Pickup", "Done", "Cancelled"},
		ContinuationRetryDelay: time.Millisecond,
	}, orchestrator.Dependencies{
		Connector: tracker,
		Runner:    runner,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	stop := runOrchestrator(t, orch)
	defer stop()

	waitForStateUpdate(t, tracker, stateUpdateCall{issueID: issue.ID, state: "Production"})
	updates := tracker.stateUpdateCalls()
	if len(updates) == 0 || updates[0] != (stateUpdateCall{issueID: issue.ID, state: "Production"}) {
		t.Fatalf("state updates = %#v, want Production as the dispatch-start transition", updates)
	}
}
