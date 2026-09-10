package orchestrator

import (
	"fmt"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func TestWorkspacePreparationDrainsInstanceAndPreservesIssueFailureBreakers(t *testing.T) {
	t.Parallel()

	const retryLimit = 3
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "dangling gitdir",
			err:  fmt.Errorf("%w: create workspace: dangling gitdir", runpkg.ErrWorkspacePreparation),
		},
		{
			name: "stale clean recovery",
			err:  fmt.Errorf("%w: create workspace: recover stale clean workspace", runpkg.ErrWorkspacePreparation),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			issue := connector.Issue{ID: "issue-workspace", Identifier: "digitaldrywood/detent#1907", State: "In Progress"}
			tracker := &terminalRetryConnector{issues: map[string]connector.Issue{issue.ID: cloneIssue(issue)}}
			attempts := &terminalRetryWorkAttemptStore{}
			cfg := normalizeConfig(Config{
				ActiveStates:   []string{"Todo", "In Progress"},
				ObservedStates: []string{"Blocked"},
				TerminalStates: []string{"Done"},
				FailureBreaker: FailureBreakerConfig{
					SameClassLimit: repeatedFailureThreshold,
					Window:         time.Hour,
					Cooldown:       time.Hour,
				},
			})
			orch := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
			state := newState(cfg)
			state.InstantFailures[issue.ID] = InstantFailure{Issue: issue, Count: 2}
			state.RepeatedFailures[issue.ID] = RepeatedFailure{Issue: issue, Count: 2}
			base := time.Date(2026, 8, 18, 20, 0, 0, 0, time.UTC)

			for attempt := 1; attempt <= retryLimit; attempt++ {
				completedAt := base.Add(time.Duration(attempt) * time.Minute)
				running := Running{
					Issue:         issue,
					Mode:          runpkg.RunModePlan,
					Attempt:       attempt,
					WorkAttemptID: int64(attempt),
					StartedAt:     completedAt.Add(-time.Minute),
				}
				state.Running[issue.ID] = running
				orch.upsertWorkAttemptSnapshot(&state, telemetry.WorkAttempt{
					AttemptID: int64(attempt), IssueID: issue.ID, Identifier: issue.Identifier,
					Status: string(store.WorkAttemptStatusActive), StartedAt: running.StartedAt,
				})
				orch.handleRunResult(t.Context(), &state, runpkg.Completion{
					IssueID:      issue.ID,
					Request:      runpkg.RunRequest{Mode: runpkg.RunModePlan},
					Err:          tt.err,
					CompletedAt:  completedAt,
					RetryAttempt: attempt + 1,
					RetryDelay:   time.Second,
				})

				if state.InstantFailures[issue.ID].Count != 2 || state.RepeatedFailures[issue.ID].Count != 2 {
					t.Fatalf("attempt %d failure breakers = instant %#v repeated %#v, want preserved counts", attempt, state.InstantFailures[issue.ID], state.RepeatedFailures[issue.ID])
				}
				if len(state.Retry) != 0 || len(state.Blocked) != 0 || len(tracker.comments) != 0 {
					t.Fatal("workspace failure retained issue retry accounting")
				}
			}
			if !state.FailureBreaker.Active() || projectFailureBreakerAllowsDispatch(&state, base.Add(3*time.Minute)) {
				t.Fatal("instance did not drain after three failures")
			}

			if failures := state.FailureBreaker.Failures[workAttemptErrorWorkspace]; len(failures) != retryLimit {
				t.Fatalf("FailureBreaker.Failures[%q] = %#v, want %d preserved failures", workAttemptErrorWorkspace, failures, retryLimit)
			}

		})
	}
}
