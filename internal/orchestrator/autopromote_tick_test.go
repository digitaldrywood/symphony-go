package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/gate"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/workpad"
)

func TestTickAutoPromoteHumanReviewIssues(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 14, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	recentReview := now.Add(-30 * time.Second)

	tests := []struct {
		name                 string
		cfg                  AutoPromoteConfig
		issue                connector.Issue
		wantUpdates          []autoPromoteTickUpdate
		wantCommentFragments []string
		wantLogFragments     []string
		rejectLogFragments   []string
	}{
		{
			name: "promotes ready issue to merging",
			cfg: AutoPromoteConfig{
				Enabled:       true,
				QuietDuration: 10 * time.Minute,
			},
			issue: autoPromoteTickIssue("issue-ready", []string{"bug"}, &connector.PullRequest{
				Number:                 42,
				URL:                    "https://github.test/digitaldrywood/detent/pull/42",
				State:                  "OPEN",
				CIStatus:               "success",
				CodexReviewState:       "COMMENTED",
				CodexReviewSubmittedAt: &oldReview,
			}),
			wantUpdates: []autoPromoteTickUpdate{{
				issueID: "issue-ready",
				state:   "Merging",
			}},
			wantCommentFragments: []string{
				"Auto-promoted this issue from Human Review to Merging.",
				"reason: ready",
				"https://github.test/digitaldrywood/detent/pull/42",
			},
		},
		{
			name: "promotes pre-existing linked pull request with arbitrary branch",
			cfg: AutoPromoteConfig{
				Enabled:       true,
				QuietDuration: 10 * time.Minute,
			},
			issue: func() connector.Issue {
				issue := autoPromoteTickIssue("I_74", []string{"bug"}, &connector.PullRequest{
					Number:                 186,
					URL:                    "https://github.com/gopherguides/corp/pull/186",
					BranchName:             "claude/add-object-lifecycle-module-i6pKl",
					State:                  "OPEN",
					MergeableState:         "clean",
					CIStatus:               "success",
					CodexReviewState:       "COMMENTED",
					CodexReviewSubmittedAt: &oldReview,
				})
				issue.Identifier = "gopherguides/corp#74"
				issue.PRRepository = "gopherguides/corp"
				return issue
			}(),
			wantUpdates: []autoPromoteTickUpdate{{
				issueID: "I_74",
				state:   "Merging",
			}},
			wantCommentFragments: []string{
				"Auto-promoted this issue from Human Review to Merging.",
				"reason: ready",
				"https://github.com/gopherguides/corp/pull/186",
			},
			rejectLogFragments: []string{
				"reason=missing_pull_request",
			},
		},
		{
			name: "linked pull request without required automated review waits for review",
			cfg: AutoPromoteConfig{
				Enabled:       true,
				QuietDuration: 10 * time.Minute,
			},
			issue: autoPromoteTickIssue("issue-linked-missing-review", []string{"bug"}, &connector.PullRequest{
				Number:     390,
				URL:        "https://github.test/digitaldrywood/detent/pull/390",
				BranchName: "detent/detent-digitaldrywood_detent_387-29d3e4765f21",
				State:      "OPEN",
				CIStatus:   "pass",
			}),
			wantLogFragments: []string{
				"reason=automated_review_missing",
			},
			rejectLogFragments: []string{
				"reason=missing_pull_request",
			},
		},
		{
			name: "degraded pull request hydration waits",
			cfg: AutoPromoteConfig{
				Enabled:       true,
				QuietDuration: 10 * time.Minute,
			},
			issue: autoPromoteTickIssue("issue-degraded-pr", []string{"bug"}, &connector.PullRequest{
				Number:                  391,
				URL:                     "https://github.test/digitaldrywood/detent/pull/391",
				State:                   "OPEN",
				MergeableState:          "clean",
				CIStatus:                "success",
				CodexReviewState:        "COMMENTED",
				CodexReviewSubmittedAt:  &oldReview,
				HydrationDegradedReason: connector.PullRequestHydrationReasonStaleCachedPullData,
			}),
			wantLogFragments: []string{
				"reason=pull_request_hydration_unavailable",
				"pull_request_hydration_degraded_reason=stale_cached_pull_request",
			},
			rejectLogFragments: []string{
				"target_state=Merging",
			},
		},
		{
			name: "routes P1 findings to rework",
			cfg: AutoPromoteConfig{
				Enabled:       true,
				QuietDuration: 10 * time.Minute,
				ReworkLimit:   0,
			},
			issue: autoPromoteTickIssue("issue-p1", []string{"bug"}, &connector.PullRequest{
				Number:                 43,
				URL:                    "https://github.test/digitaldrywood/detent/pull/43",
				State:                  "OPEN",
				CIStatus:               "pass",
				CodexReviewState:       "P1",
				CodexReviewSubmittedAt: &oldReview,
				CodexReviewFindings: []connector.PullRequestFinding{{
					Body: "![P1 Badge](https://example.test/p1.svg) Unsafe migration.",
					URL:  "https://github.test/digitaldrywood/detent/pull/43#pullrequestreview-1",
				}},
			}),
			wantUpdates: []autoPromoteTickUpdate{{
				issueID: "issue-p1",
				state:   "Rework",
			}},
			wantCommentFragments: []string{
				"Auto-promote routed this issue from Human Review to Rework.",
				"reason: p1_findings",
				"Unsafe migration.",
				"https://github.test/digitaldrywood/detent/pull/43#pullrequestreview-1",
			},
		},
		{
			name: "routes failing ci to rework by default",
			cfg: AutoPromoteConfig{
				Enabled:       true,
				QuietDuration: 10 * time.Minute,
			},
			issue: autoPromoteTickIssue("issue-red-ci", []string{"bug"}, &connector.PullRequest{
				Number:   48,
				URL:      "https://github.test/digitaldrywood/detent/pull/48",
				State:    "OPEN",
				CIStatus: "fail",
				SlowChecks: []connector.PullRequestCheck{
					{Name: "browser-e2e", Status: "completed", Conclusion: "failure"},
					{Name: "lint", Status: "completed", Conclusion: "failure"},
					{Name: "backend", Status: "completed", Conclusion: "success"},
				},
			}),
			wantUpdates: []autoPromoteTickUpdate{{
				issueID: "issue-red-ci",
				state:   "Rework",
			}},
			wantCommentFragments: []string{
				"Auto-promote routed this issue from Human Review to Rework: current-head CI is failing.",
				"reason: ci_not_green",
				"ci_status: red",
				"failed_checks: browser-e2e, lint",
				"https://github.test/digitaldrywood/detent/pull/48",
			},
			wantLogFragments: []string{
				"failed_checks=\"browser-e2e, lint\"",
			},
		},
		{
			name: "routes unresolved review threads to rework",
			cfg: AutoPromoteConfig{
				Enabled:       true,
				QuietDuration: 10 * time.Minute,
			},
			issue: autoPromoteTickIssue("issue-unresolved-review-threads", []string{"bug"}, &connector.PullRequest{
				Number:   2103,
				URL:      "https://github.test/digitaldrywood/detent/pull/2104",
				State:    "OPEN",
				CIStatus: "pass",
				UnresolvedReviewThreads: []connector.PullRequestReviewThread{
					{Path: "internal/orchestrator/autopromote.go", Line: 181},
					{Path: "internal/connector/github/pull_requests.go", Line: 1092},
				},
			}),
			wantUpdates: []autoPromoteTickUpdate{{
				issueID: "issue-unresolved-review-threads",
				state:   "Rework",
			}},
			wantCommentFragments: []string{
				"Auto-promote routed this issue from Human Review to Rework: linked PR has 2 unresolved review threads.",
				"reason: unresolved_review_threads",
				"unresolved_review_threads: 2",
				"first_unresolved_review_thread: internal/orchestrator/autopromote.go:181",
			},
			wantLogFragments: []string{
				"reason=unresolved_review_threads",
				"target_state=Rework",
			},
		},
		{
			name: "routes conflicting pull request to rework",
			cfg: AutoPromoteConfig{
				Enabled:       true,
				QuietDuration: 0,
				Gate: gate.Config{
					Kind:            gate.KindCommand,
					CIFailureAction: gate.CIFailureActionRework,
				},
			},
			issue: autoPromoteTickIssue("issue-conflicting-pr", []string{"bug"}, &connector.PullRequest{
				Number:         49,
				URL:            "https://github.test/digitaldrywood/detent/pull/49",
				State:          "OPEN",
				MergeableState: "dirty",
			}),
			wantUpdates: []autoPromoteTickUpdate{{
				issueID: "issue-conflicting-pr",
				state:   "Rework",
			}},
			wantCommentFragments: []string{
				"Auto-promote routed this issue from Human Review to Rework: linked PR has merge conflicts.",
				"reason: merge_conflicts",
				"https://github.test/digitaldrywood/detent/pull/49",
			},
			wantLogFragments: []string{
				"reason=merge_conflicts",
				"target_state=Rework",
			},
		},
		{
			name: "waits for quiet period",
			cfg: AutoPromoteConfig{
				Enabled:       true,
				QuietDuration: 10 * time.Minute,
			},
			issue: autoPromoteTickIssue("issue-recent", []string{"bug"}, &connector.PullRequest{
				Number:                 44,
				URL:                    "https://github.test/digitaldrywood/detent/pull/44",
				State:                  "OPEN",
				CIStatus:               "pass",
				CodexReviewState:       "COMMENTED",
				CodexReviewSubmittedAt: &recentReview,
			}),
		},
		{
			name: "skips closed pull request",
			cfg: AutoPromoteConfig{
				Enabled:       true,
				QuietDuration: 10 * time.Minute,
			},
			issue: autoPromoteTickIssue("issue-closed-pr", []string{"bug"}, &connector.PullRequest{
				Number:                 47,
				URL:                    "https://github.test/digitaldrywood/detent/pull/47",
				State:                  "CLOSED",
				CIStatus:               "pass",
				CodexReviewState:       "COMMENTED",
				CodexReviewSubmittedAt: &oldReview,
			}),
		},
		{
			name: "honors evaluator label filters",
			cfg: AutoPromoteConfig{
				Enabled:            true,
				QuietDuration:      10 * time.Minute,
				AllowedIssueLabels: []string{"release"},
			},
			issue: autoPromoteTickIssue("issue-label", []string{"bug"}, &connector.PullRequest{
				Number:                 45,
				URL:                    "https://github.test/digitaldrywood/detent/pull/45",
				State:                  "OPEN",
				CIStatus:               "pass",
				CodexReviewState:       "COMMENTED",
				CodexReviewSubmittedAt: &oldReview,
			}),
		},
		{
			name: "disabled config does not evaluate",
			cfg: AutoPromoteConfig{
				Enabled:       false,
				QuietDuration: 10 * time.Minute,
			},
			issue: autoPromoteTickIssue("issue-disabled", []string{"bug"}, &connector.PullRequest{
				Number:                 46,
				URL:                    "https://github.test/digitaldrywood/detent/pull/46",
				State:                  "OPEN",
				CIStatus:               "pass",
				CodexReviewState:       "COMMENTED",
				CodexReviewSubmittedAt: &oldReview,
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{
				PollInterval:        time.Minute,
				MaxConcurrentAgents: 1,
				AutoPromote:         tt.cfg,
				ActiveStates:        []string{"Todo", "In Progress", "Rework", "Merging"},
				TerminalStates:      []string{"Done", "Cancelled"},
			})
			state := newState(cfg)
			mergingSlot := dispatchTestIssue("issue-merging-slot", "Merging")
			state.Running[mergingSlot.ID] = Running{Issue: mergingSlot}
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{tt.issue}}
			var logs strings.Builder
			orch := &Orchestrator{
				cfg:       cfg,
				connector: tracker,
				logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
			}

			orch.tick(context.Background(), &state, now)

			if !reflect.DeepEqual(tracker.updates, tt.wantUpdates) {
				t.Fatalf("updates = %#v, want %#v", tracker.updates, tt.wantUpdates)
			}
			if len(tracker.fetchByStatesRequests) != 1 {
				t.Fatalf("FetchIssuesByStates() calls = %d, want 1", len(tracker.fetchByStatesRequests))
			}
			if !autoPromoteTickStatesEqual(tracker.fetchByStatesRequests[0], []string{"Blocked", "Human Review", "Merging"}) {
				t.Fatalf("FetchIssuesByStates() states = %#v, want Blocked/Human Review/Merging", tracker.fetchByStatesRequests[0])
			}
			if len(tt.wantCommentFragments) == 0 {
				if len(tracker.comments) != 0 {
					t.Fatalf("comments = %#v, want none", tracker.comments)
				}
			} else {
				if len(tracker.comments) != 1 {
					t.Fatalf("comments = %#v, want one comment", tracker.comments)
				}
				for _, fragment := range tt.wantCommentFragments {
					if !strings.Contains(tracker.comments[0].body, fragment) {
						t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
					}
				}
			}
			for _, fragment := range tt.wantLogFragments {
				if !strings.Contains(logs.String(), fragment) {
					t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
				}
			}
			for _, fragment := range tt.rejectLogFragments {
				if strings.Contains(logs.String(), fragment) {
					t.Fatalf("logs %q contain rejected fragment %q", logs.String(), fragment)
				}
			}
		})
	}
}

func TestApplyAutoPromoteDecisionArtifactReworkTicks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		initialState string
		statuses     []string
		wantWrites   int
	}{
		{
			name:         "already in rework remains a no-op",
			initialState: "Rework",
			statuses:     []string{"recut", "recut", "changes_requested", "changes_requested", "recut"},
			wantWrites:   0,
		},
		{
			name:         "source routes once and changed rework status remains a no-op",
			initialState: "Review",
			statuses:     []string{"recut", "recut", "changes_requested", "changes_requested", "recut"},
			wantWrites:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{
				AutoPromote: AutoPromoteConfig{
					Enabled:     true,
					SourceState: "Review",
					PassState:   "Ready for Pickup",
					ReworkState: "Rework",
					Gate: gate.Config{
						Kind: gate.KindArtifact,
						Artifact: gate.ArtifactConfig{
							StatusField:    "render_status",
							PassStatuses:   []string{"approved"},
							WaitStatuses:   []string{"queued"},
							ReworkStatuses: []string{"recut", "changes_requested"},
						},
					},
				},
				ActiveStates:   []string{"Todo", "In Progress", "Rework"},
				ObservedStates: []string{"Review"},
				TerminalStates: []string{"Done", "Cancelled"},
			})
			issue := autoPromoteTickIssue("issue-artifact-rework", nil, nil)
			issue.State = tt.initialState
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
			orch := &Orchestrator{
				cfg:       cfg,
				connector: tracker,
				logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			state := newState(cfg)
			now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)

			for tick, status := range tt.statuses {
				current := cloneIssue(tracker.stateIssues[0])
				current.Fields = map[string]string{"render_status": status}
				summary := AutoPromoteSummaryFromIssue(current)
				summary.ArtifactStatus = status
				decision := EvaluateAutoPromote(current, summary, cfg.AutoPromote, now.Add(time.Duration(tick)*time.Minute))
				if decision.Action != AutoPromoteActionRework {
					t.Fatalf("tick %d decision = %#v, want rework", tick, decision)
				}
				targetState := autoPromoteTargetState(decision.Action, cfg.AutoPromote)
				orch.applyAutoPromoteDecision(
					t.Context(),
					&state,
					current,
					summary,
					decision,
					targetState,
					now.Add(time.Duration(tick)*time.Minute),
				)
			}

			if got := len(tracker.updates); got != tt.wantWrites {
				t.Fatalf("state updates = %d, want %d: %#v", got, tt.wantWrites, tracker.updates)
			}
			if got := len(tracker.comments); got != tt.wantWrites {
				t.Fatalf("comments = %d, want %d: %#v", got, tt.wantWrites, tracker.comments)
			}
			if got := len(state.RecentEvents); got != tt.wantWrites {
				t.Fatalf("recent events = %d, want %d: %#v", got, tt.wantWrites, state.RecentEvents)
			}
			if tt.wantWrites == 1 {
				for _, fragment := range []string{
					"Auto-promote routed this issue from Review to Rework.",
					"reason: artifact_status_rework",
				} {
					if !strings.Contains(tracker.comments[0].body, fragment) {
						t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
					}
				}
			}
		})
	}
}

func TestTickAutoPromoteCompletedActiveIssues(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)

	tests := []struct {
		name                 string
		issue                connector.Issue
		gateWaitReason       string
		wantUpdates          []autoPromoteTickUpdate
		wantCommentFragments []string
	}{
		{
			name: "promotes active completed issue directly to merging",
			issue: func() connector.Issue {
				issue := autoPromoteTickIssue("issue-active-ready", []string{"bug"}, &connector.PullRequest{
					Number:                 142,
					URL:                    "https://github.test/digitaldrywood/detent/pull/142",
					State:                  "OPEN",
					CIStatus:               "success",
					CodexReviewState:       "COMMENTED",
					CodexReviewSubmittedAt: &oldReview,
				})
				issue.State = "In Progress"
				return issue
			}(),
			wantUpdates: []autoPromoteTickUpdate{{issueID: "issue-active-ready", state: "Merging"}},
			wantCommentFragments: []string{
				"Auto-promoted this issue from In Progress to Merging.",
				"reason: ready",
				"https://github.test/digitaldrywood/detent/pull/142",
			},
		},
		{
			name: "promotes completed Rework gate wait directly to merging",
			issue: func() connector.Issue {
				issue := autoPromoteTickIssue("issue-rework-ready", []string{"bug"}, &connector.PullRequest{
					Number:                 2030,
					URL:                    "https://github.test/digitaldrywood/detent/pull/2030",
					State:                  "OPEN",
					HeadSHA:                "same-head",
					MergeableState:         "clean",
					CIStatus:               "success",
					CodexReviewState:       "COMMENTED",
					CodexReviewSubmittedAt: &oldReview,
				})
				issue.State = "Rework"
				return issue
			}(),
			gateWaitReason: completedReworkGateWaitReason,
			wantUpdates:    []autoPromoteTickUpdate{{issueID: "issue-rework-ready", state: "Merging"}},
			wantCommentFragments: []string{
				"Auto-promoted this issue from Rework to Merging.",
				"reason: ready",
				"https://github.test/digitaldrywood/detent/pull/2030",
			},
		},
		{
			name: "promotes completed todo issue directly to merging",
			issue: func() connector.Issue {
				issue := autoPromoteTickIssue("issue-todo-ready", []string{"bug"}, &connector.PullRequest{
					Number:                 145,
					URL:                    "https://github.test/digitaldrywood/detent/pull/145",
					State:                  "OPEN",
					CIStatus:               "success",
					CodexReviewState:       "COMMENTED",
					CodexReviewSubmittedAt: &oldReview,
				})
				issue.State = "Todo"
				return issue
			}(),
			wantUpdates: []autoPromoteTickUpdate{{issueID: "issue-todo-ready", state: "Merging"}},
			wantCommentFragments: []string{
				"Auto-promoted this issue from Todo to Merging.",
				"reason: ready",
				"https://github.test/digitaldrywood/detent/pull/145",
			},
		},
		{
			name: "routes active completed issue directly to rework",
			issue: func() connector.Issue {
				issue := autoPromoteTickIssue("issue-active-rework", []string{"bug"}, &connector.PullRequest{
					Number:                 143,
					URL:                    "https://github.test/digitaldrywood/detent/pull/143",
					State:                  "OPEN",
					CIStatus:               "failure",
					CodexReviewState:       "COMMENTED",
					CodexReviewSubmittedAt: &oldReview,
				})
				issue.State = "In Progress"
				return issue
			}(),
			wantUpdates: []autoPromoteTickUpdate{{issueID: "issue-active-rework", state: "Rework"}},
			wantCommentFragments: []string{
				"Auto-promote routed this issue from In Progress to Rework: current-head CI is failing.",
				"reason: ci_not_green",
				"https://github.test/digitaldrywood/detent/pull/143",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{
				PollInterval:        time.Minute,
				MaxConcurrentAgents: 1,
				AutoPromote: AutoPromoteConfig{
					Enabled:       true,
					QuietDuration: 10 * time.Minute,
					Gate:          gate.Config{Kind: gate.KindCommand},
				},
				ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
				TerminalStates: []string{"Done", "Cancelled"},
			})
			state := newState(cfg)
			state.Completed[tt.issue.ID] = Completed{
				Issue:          tt.issue,
				FinalState:     FinalStateCompleted,
				GateWaitReason: tt.gateWaitReason,
			}
			mergingSlot := dispatchTestIssue(tt.issue.ID+"-merging-slot", "Merging")
			state.Running[mergingSlot.ID] = Running{Issue: mergingSlot}
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{tt.issue}}
			orch := &Orchestrator{
				cfg:       cfg,
				connector: tracker,
				logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			}

			orch.tick(context.Background(), &state, now)

			if !reflect.DeepEqual(tracker.updates, tt.wantUpdates) {
				t.Fatalf("updates = %#v, want %#v", tracker.updates, tt.wantUpdates)
			}
			if len(tracker.comments) != 1 {
				t.Fatalf("comments = %#v, want one auto-promote audit comment", tracker.comments)
			}
			for _, fragment := range tt.wantCommentFragments {
				if !strings.Contains(tracker.comments[0].body, fragment) {
					t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
				}
			}
			if _, ok := state.Completed[tt.issue.ID]; ok {
				t.Fatalf("Completed[%q] present after auto-promote transition", tt.issue.ID)
			}
		})
	}
}

func TestAutoPromoteDoesNotReconcileReworkBeforeCompletion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 26, 18, 34, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-sticky-rework", []string{"bug"}, &connector.PullRequest{
		Number:         2928,
		URL:            "https://github.test/getparable/parable/pull/2928",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	issue.State = "Rework"
	cfg := normalizeConfig(Config{
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			GateWaitState: autoPromoteGateWaitReview,
			Gate: gate.Config{
				Kind:                   gate.KindCommand,
				RequireAutomatedReview: new(false),
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})

	tests := []struct {
		name           string
		completedState string
		running        bool
		wantTransition bool
	}{
		{name: "operator moved completed item to rework", completedState: "In Progress"},
		{name: "rework worker is running", completedState: "In Progress", running: true},
		{name: "rework worker completed", completedState: "Rework", wantTransition: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := newState(cfg)
			state.Completed[issue.ID] = Completed{
				Issue:      promotedIssue(issue, tt.completedState, now.Add(-time.Hour)),
				FinalState: FinalStateCompleted,
			}
			if tt.running {
				state.Running[issue.ID] = Running{
					Issue:               issue,
					DispatchSourceState: "Rework",
					StartedAt:           now.Add(-time.Minute),
				}
			}
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
			orch := &Orchestrator{
				cfg:       cfg,
				connector: tracker,
				logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			}

			result := orch.transitionCompletedActiveIssuesToReview(t.Context(), &state, []connector.Issue{issue}, now)

			_, transitioned := result.transitioned[issue.ID]
			if transitioned != tt.wantTransition {
				t.Fatalf("transitioned = %v, want %v", transitioned, tt.wantTransition)
			}
			if got := len(tracker.updates); got != boolInt(tt.wantTransition) {
				t.Fatalf("updates = %#v, want transition %v", tracker.updates, tt.wantTransition)
			}
		})
	}
}

func TestTickRoutesCompletedActiveArtifactToReview(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	issue := artifactCompletionTransitionIssue("Production", "approved")
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			SourceState:   "Review",
			PassState:     "Ready for Pickup",
			ReworkState:   "Rework",
			GateWaitState: autoPromoteGateWaitSource,
			Gate:          artifactCompletionTestGate(),
		},
		ActiveStates:   []string{"Todo", "Production", "Rework"},
		ObservedStates: []string{"Review"},
		TerminalStates: []string{"Ready for Pickup", "Done", "Cancelled"},
	})
	state := newState(cfg)
	state.Completed[issue.ID] = Completed{
		Issue:      issue,
		FinalState: FinalStateCompleted,
	}
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	orch.tick(context.Background(), &state, now)

	wantUpdates := []autoPromoteTickUpdate{{issueID: issue.ID, state: "Review"}}
	if !reflect.DeepEqual(tracker.updates, wantUpdates) {
		t.Fatalf("updates = %#v, want %#v", tracker.updates, wantUpdates)
	}
	if got := state.Completed[issue.ID].Issue.State; got != "Review" {
		t.Fatalf("Completed issue state = %q, want Review", got)
	}
}

func TestTickReleasesCompletedArtifactReworkNoopForNextDispatch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 18, 17, 28, 32, 0, time.UTC)
	issue := artifactCompletionTransitionIssue("Rework", "recut")
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:     true,
			SourceState: "Review",
			PassState:   "Ready for Pickup",
			ReworkState: "Rework",
			Gate:        artifactCompletionTestGate(),
		},
		ActiveStates:   []string{"Todo", "Production", "Rework"},
		ObservedStates: []string{"Review"},
		TerminalStates: []string{"Ready for Pickup", "Done", "Cancelled"},
	})
	state := newState(cfg)
	state.Completed[issue.ID] = Completed{
		Issue:       issue,
		CompletedAt: now,
		FinalState:  FinalStateCompleted,
	}
	state.Retry[issue.ID] = Retry{Issue: issue, Attempt: 1, DueAt: now}
	mergingSlot := dispatchTestIssue("issue-merging-slot", "Merging")
	state.Running[mergingSlot.ID] = Running{Issue: mergingSlot}
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	attempts := &recordingWorkAttemptStore{}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(&logs, nil)),
	}

	orch.tick(t.Context(), &state, now)

	if _, ok := state.Completed[issue.ID]; ok {
		t.Fatalf("Completed[%q] present after same-state auto-promote", issue.ID)
	}
	if retry, ok := state.Retry[issue.ID]; !ok || retry.Attempt != 1 {
		t.Fatalf("Retry[%q] = %#v, want preserved continuation attempt 1", issue.ID, retry)
	}
	if len(attempts.decisions) != 1 {
		t.Fatalf("scheduler decisions after completion tick = %#v, want one", attempts.decisions)
	}
	if got := attempts.decisions[0]; got.IssueID != issue.ID || got.Reason != dispatchSkipProjectCapacityFull {
		t.Fatalf("scheduler decision = %#v, want issue %q skipped for %q", got, issue.ID, dispatchSkipProjectCapacityFull)
	}
	for _, fragment := range []string{
		`level=INFO msg="auto promote decision"`,
		"action=rework",
		"target_state=Rework",
		`level=WARN msg="completed artifact gate status unchanged after successful rework"`,
		"gate_status_field=render_status",
		"gate_status=recut",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}

	nextRetry := state.Retry[issue.ID]
	orch.tick(t.Context(), &state, nextRetry.DueAt)

	if len(attempts.decisions) != 2 {
		t.Fatalf("scheduler decisions after next tick = %#v, want two", attempts.decisions)
	}
	if got := attempts.decisions[1]; got.IssueID != issue.ID || got.Reason != dispatchSkipProjectCapacityFull {
		t.Fatalf("scheduler decision = %#v, want issue %q skipped for %q", got, issue.ID, dispatchSkipProjectCapacityFull)
	}
	if retry, ok := state.Retry[issue.ID]; !ok || retry.Attempt != 1 {
		t.Fatalf("Retry[%q] after capacity skip = %#v, want rescheduled attempt 1", issue.ID, retry)
	}
}

func TestTickAutoPromoteRecoversActiveIssueAfterRestart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 8, 12, 30, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-restart-gate-pending", []string{"bug"}, &connector.PullRequest{
		Number:                 144,
		URL:                    "https://github.test/digitaldrywood/detent/pull/144",
		State:                  "OPEN",
		HeadSHA:                "restart-head",
		CIStatus:               "pending",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	issue.State = "In Progress"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 0,
			Gate:          gate.Config{Kind: gate.KindCommand},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	mergingSlot := dispatchTestIssue("issue-restart-merging-slot", "Merging")
	state.Running[mergingSlot.ID] = Running{Issue: mergingSlot}
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	prNumber := int64(issue.PullRequest.Number)
	attempts := &recordingWorkAttemptStore{history: []store.WorkAttempt{{
		ProjectID:          cfg.Project.ID,
		IssueID:            issue.ID,
		Identifier:         issue.Identifier,
		IssueURL:           issue.URL,
		PRNumber:           &prNumber,
		WorkerType:         "agent",
		Status:             store.WorkAttemptStatusTerminal,
		StartedAt:          now.Add(-15 * time.Minute),
		CompletedAt:        now.Add(-10 * time.Minute),
		TerminalState:      store.WorkAttemptTerminalSuccess,
		WorkerMetadataJSON: implementProgressMetadataJSON(autoPromoteReworkSignature{PRNumber: prNumber, HeadSHA: "restart-head"}, store.WorkAttemptTerminalSuccess),
	}}}
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	orch.tick(context.Background(), &state, now)

	if len(tracker.updates) != 0 {
		t.Fatalf("updates with pending CI = %#v, want none", tracker.updates)
	}
	if len(tracker.comments) != 0 {
		t.Fatalf("comments with pending CI = %#v, want none", tracker.comments)
	}
	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after pending restart recovery tick", issue.ID)
	}
	if completed, ok := state.Completed[issue.ID]; !ok || completed.FinalState != FinalStateCompleted {
		t.Fatalf("Completed[%q] = %#v, want durable successful completion restored", issue.ID, completed)
	}
	if len(attempts.historyQueries) == 0 {
		t.Fatal("durable work attempt history was not queried")
	}

	tracker.stateIssues[0].PullRequest.CIStatus = "success"
	orch.tick(context.Background(), &state, now.Add(time.Minute))

	wantUpdates := []autoPromoteTickUpdate{{issueID: issue.ID, state: "Merging"}}
	if !reflect.DeepEqual(tracker.updates, wantUpdates) {
		t.Fatalf("updates after CI pass = %#v, want %#v", tracker.updates, wantUpdates)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments after CI pass = %#v, want one auto-promote audit comment", tracker.comments)
	}
	for _, fragment := range []string{
		"Auto-promoted this issue from In Progress to Merging.",
		"reason: ready",
		"https://github.test/digitaldrywood/detent/pull/144",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
}

func TestTickAutoPromoteRecoversOptionalReviewDeadlineAfterRestart(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 13, 19, 0, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-restart-optional-review", []string{"bug"}, &connector.PullRequest{
		Number:         1297,
		URL:            "https://github.test/digitaldrywood/detent/pull/1297",
		State:          "OPEN",
		HeadSHA:        "optional-head",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	cfg := normalizeConfig(Config{
		Project:             scheduler.ProjectCandidate{ID: "detent"},
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:         true,
			QuietDuration:   0,
			GateWaitState:   autoPromoteGateWaitReview,
			GateWaitTimeout: 15 * time.Minute,
			Gate: gate.Config{
				Kind:            gate.KindCommand,
				AutomatedReview: gate.AutomatedReviewOptional,
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	mergingSlot := dispatchTestIssue("issue-restart-optional-merging-slot", "Merging")
	state.Running[mergingSlot.ID] = Running{Issue: mergingSlot}
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	prNumber := int64(issue.PullRequest.Number)
	completedAt := now.Add(-20 * time.Minute)
	attempts := &recordingWorkAttemptStore{history: []store.WorkAttempt{{
		ProjectID:          cfg.Project.ID,
		IssueID:            issue.ID,
		Identifier:         issue.Identifier,
		IssueURL:           issue.URL,
		PRNumber:           &prNumber,
		WorkerType:         "agent",
		Status:             store.WorkAttemptStatusTerminal,
		StartedAt:          completedAt.Add(-5 * time.Minute),
		CompletedAt:        completedAt,
		TerminalState:      store.WorkAttemptTerminalSuccess,
		WorkerMetadataJSON: implementProgressMetadataJSON(autoPromoteReworkSignature{PRNumber: prNumber, HeadSHA: "optional-head"}, store.WorkAttemptTerminalSuccess),
	}}}
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	orch.tick(context.Background(), &state, now)

	wantUpdates := []autoPromoteTickUpdate{{issueID: issue.ID, state: "Merging"}}
	if !reflect.DeepEqual(tracker.updates, wantUpdates) {
		t.Fatalf("updates = %#v, want %#v", tracker.updates, wantUpdates)
	}
	if len(attempts.historyQueries) == 0 {
		t.Fatal("durable work attempt history was not queried")
	}
}

func TestLatestSuccessfulGateWaitAttemptRequiresCurrentImplementationEvidence(t *testing.T) {
	t.Parallel()

	currentPR := int64(144)
	otherPR := int64(143)
	currentSignature := autoPromoteReworkSignature{PRNumber: currentPR, HeadSHA: "current-head"}
	staleSignature := autoPromoteReworkSignature{PRNumber: currentPR, HeadSHA: "stale-head"}
	otherSignature := autoPromoteReworkSignature{PRNumber: otherPR, HeadSHA: "other-head"}
	issue := autoPromoteTickIssue("issue-gate-wait-evidence", []string{"bug"}, &connector.PullRequest{
		Number:  144,
		State:   "OPEN",
		HeadSHA: "current-head",
	})
	issue.State = "In Progress"

	tests := []struct {
		name     string
		attempts []store.WorkAttempt
		wantID   int64
	}{
		{
			name: "ignores plan success",
			attempts: []store.WorkAttempt{{
				ID:                 1,
				TerminalState:      store.WorkAttemptTerminalSuccess,
				WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{"run_mode": runpkg.RunModePlan}),
			}},
		},
		{
			name: "ignores implementation success without PR association",
			attempts: []store.WorkAttempt{{
				ID:                 2,
				TerminalState:      store.WorkAttemptTerminalSuccess,
				WorkerMetadataJSON: implementProgressMetadataJSON(autoPromoteReworkSignature{}, store.WorkAttemptTerminalSuccess),
			}},
		},
		{
			name: "ignores implementation success for another PR",
			attempts: []store.WorkAttempt{{
				ID:                 3,
				PRNumber:           &otherPR,
				TerminalState:      store.WorkAttemptTerminalSuccess,
				WorkerMetadataJSON: implementProgressMetadataJSON(otherSignature, store.WorkAttemptTerminalSuccess),
			}},
		},
		{
			name: "ignores current PR without head evidence",
			attempts: []store.WorkAttempt{{
				ID:                 4,
				PRNumber:           &currentPR,
				TerminalState:      store.WorkAttemptTerminalSuccess,
				WorkerMetadataJSON: implementProgressMetadataJSON(autoPromoteReworkSignature{}, store.WorkAttemptTerminalSuccess),
			}},
		},
		{
			name: "ignores success for an older current PR head",
			attempts: []store.WorkAttempt{{
				ID:                 5,
				PRNumber:           &currentPR,
				TerminalState:      store.WorkAttemptTerminalSuccess,
				WorkerMetadataJSON: implementProgressMetadataJSON(staleSignature, store.WorkAttemptTerminalSuccess),
			}},
		},
		{
			name: "accepts current PR and head from combined evidence",
			attempts: []store.WorkAttempt{{
				ID:                 6,
				PRNumber:           &currentPR,
				TerminalState:      store.WorkAttemptTerminalSuccess,
				WorkerMetadataJSON: implementProgressMetadataJSON(autoPromoteReworkSignature{HeadSHA: "current-head"}, store.WorkAttemptTerminalSuccess),
			}},
			wantID: 6,
		},
		{
			name: "accepts current PR from completion record",
			attempts: []store.WorkAttempt{{
				ID:                 7,
				TerminalState:      store.WorkAttemptTerminalSuccess,
				WorkerMetadataJSON: implementProgressMetadataJSON(currentSignature, store.WorkAttemptTerminalSuccess),
			}},
			wantID: 7,
		},
		{
			name: "skips newer unrelated success for older current completion",
			attempts: []store.WorkAttempt{
				{
					ID:                 9,
					TerminalState:      store.WorkAttemptTerminalSuccess,
					WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{"run_mode": runpkg.RunModePlan}),
				},
				{
					ID:                 8,
					TerminalState:      store.WorkAttemptTerminalSuccess,
					WorkerMetadataJSON: implementProgressMetadataJSON(currentSignature, store.WorkAttemptTerminalSuccess),
				},
			},
			wantID: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			attempts := &recordingWorkAttemptStore{history: tt.attempts}
			orch := &Orchestrator{
				cfg:          normalizeConfig(Config{Project: scheduler.ProjectCandidate{ID: "detent"}}),
				workAttempts: attempts,
			}

			attempt, ok, err := orch.latestSuccessfulGateWaitAttempt(context.Background(), issue)
			if err != nil {
				t.Fatalf("latestSuccessfulGateWaitAttempt() error = %v", err)
			}
			if ok != (tt.wantID > 0) {
				t.Fatalf("latestSuccessfulGateWaitAttempt() ok = %v, want %v", ok, tt.wantID > 0)
			}
			if attempt.ID != tt.wantID {
				t.Fatalf("latestSuccessfulGateWaitAttempt() ID = %d, want %d", attempt.ID, tt.wantID)
			}
		})
	}
}

func TestRestoreDurableReworkGateWaitCompletion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 28, 18, 45, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		Project: scheduler.ProjectCandidate{ID: "detent"},
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			GateWaitState: autoPromoteGateWaitSource,
			Gate:          gate.Config{Kind: gate.KindCommand, RequireAutomatedReview: new(false)},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tests := []struct {
		name             string
		recordedFailures []string
		prepareIssue     func(*connector.Issue)
		includeMarker    bool
		wantRestore      bool
	}{
		{name: "restores unchanged exact head", includeMarker: true, wantRestore: true},
		{
			name:          "head movement invalidates completion",
			includeMarker: true,
			prepareIssue: func(issue *connector.Issue) {
				issue.PullRequest.HeadSHA = "new-head"
			},
		},
		{
			name:          "new failing check invalidates completion",
			includeMarker: true,
			prepareIssue: func(issue *connector.Issue) {
				issue.PullRequest.RequiredCheckFailures = []connector.PullRequestCheck{{Name: "Test", Status: "completed", Conclusion: "failure"}}
			},
		},
		{
			name:          "new merge conflict invalidates completion",
			includeMarker: true,
			prepareIssue: func(issue *connector.Issue) {
				issue.PullRequest.MergeableState = "dirty"
			},
		},
		{
			name:             "cleared failing check retains completion",
			recordedFailures: []string{"Test"},
			includeMarker:    true,
			wantRestore:      true,
		},
		{
			name:          "new P1 review invalidates completion",
			includeMarker: true,
			prepareIssue: func(issue *connector.Issue) {
				submittedAt := now.Add(-time.Minute)
				issue.PullRequest.CodexReviewState = "P1"
				issue.PullRequest.CodexReviewSubmittedAt = &submittedAt
				issue.PullRequest.CodexReviewFindings = []connector.PullRequestFinding{{Body: "Fix the race."}}
			},
		},
		{
			name:          "later Rework entry invalidates completion",
			includeMarker: true,
			prepareIssue: func(issue *connector.Issue) {
				enteredAt := now.Add(-time.Minute)
				issue.StageUpdatedAt = &enteredAt
			},
		},
		{name: "ordinary success without marker is not restored"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := autoPromoteTickIssue("issue-rework-gate-restart", []string{"bug"}, &connector.PullRequest{
				Number:         2030,
				State:          "OPEN",
				HeadSHA:        "same-head",
				MergeableState: "clean",
				CIStatus:       "success",
			})
			issue.State = "Rework"
			issue.Comments = []connector.IssueComment{{Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```"}}
			recordedIssue := cloneIssue(issue)
			if tt.prepareIssue != nil {
				tt.prepareIssue(&issue)
			}
			signature := autoPromoteReworkSignature{PRNumber: 2030, HeadSHA: "same-head", FailedChecks: tt.recordedFailures}
			attempt := successfulReworkGateWaitAttempt(now.Add(-2*time.Minute), recordedIssue, signature, tt.includeMarker)
			attempts := &recordingWorkAttemptStore{history: []store.WorkAttempt{attempt}}
			orch := &Orchestrator{cfg: cfg, workAttempts: attempts, connector: &implementProgressConnector{refreshed: issue}}
			state := newState(cfg)

			orch.restoreDurableGateWaitCompletions(t.Context(), &state, []connector.Issue{issue})

			completed, restored := state.Completed[issue.ID]
			if restored != tt.wantRestore {
				t.Fatalf("Completed[%q] present = %v, want %v", issue.ID, restored, tt.wantRestore)
			}
			if !tt.wantRestore {
				return
			}
			if completed.GateWaitReason != completedReworkGateWaitReason {
				t.Fatalf("GateWaitReason = %q, want %q", completed.GateWaitReason, completedReworkGateWaitReason)
			}
			decision := newDispatchPlanner(cfg).dispatchableIssueDecision(issue, &state, false, now, "")
			if decision.dispatchable || decision.reason != dispatchSkipAwaitingGate {
				t.Fatalf("dispatch decision = %#v, want awaiting gate", decision)
			}
		})
	}
}

func successfulReworkGateWaitAttempt(
	completedAt time.Time,
	issue connector.Issue,
	signature autoPromoteReworkSignature,
	includeMarker bool,
) store.WorkAttempt {
	prNumber := signature.PRNumber
	metadata := implementCompletionProgressMetadata(implementCompletionProgressDecision{
		Outcome:            store.WorkAttemptTerminalSuccess,
		Reason:             "unchanged_signature_clean_diff",
		CurrentSignature:   signature,
		WorkspaceDiffStats: DiffStats{Status: "clean"},
		TrackerState:       "Rework",
	})
	if includeMarker {
		metadata = mergeWorkAttemptMetadata(metadata, completionGateWaitMetadata(completedReworkGateWaitReason, issue))
	}
	return store.WorkAttempt{
		ID:                 2030,
		ProjectID:          "detent",
		IssueID:            issue.ID,
		Identifier:         issue.Identifier,
		IssueURL:           issue.URL,
		PRNumber:           &prNumber,
		WorkerType:         "agent",
		Lane:               "Rework",
		Status:             store.WorkAttemptStatusTerminal,
		StartedAt:          completedAt.Add(-time.Minute),
		CompletedAt:        completedAt,
		TerminalState:      store.WorkAttemptTerminalSuccess,
		WorkerMetadataJSON: marshalWorkAttemptJSON(metadata),
	}
}

func TestTickAutoPromotesCompletedGateWaitWhileDispatchRunning(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	now := time.Date(2026, 7, 9, 18, 50, 0, 0, time.UTC)
	oldReview := now.Add(-10 * time.Minute)
	issue := autoPromoteTickIssue("issue-running-gate-wait", []string{"bug"}, &connector.PullRequest{
		Number:                 1125,
		URL:                    "https://github.test/digitaldrywood/detent/pull/1125",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "pass",
		CodexReviewSubmittedAt: &oldReview,
	})
	issue.State = "In Progress"
	issue.Comments = []connector.IssueComment{{
		Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```",
	}}
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:         true,
			QuietDuration:   0,
			GateWaitState:   autoPromoteGateWaitSource,
			NoProgressLimit: 3,
			Gate: gate.Config{
				Kind:                   gate.KindCommand,
				RequireAutomatedReview: new(false),
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	runtimeStore, err := store.Open(ctx, store.Config{Backend: store.BackendSQLite, Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = runtimeStore.Close() })
	attemptID, err := runtimeStore.StartWorkAttempt(ctx, store.WorkAttemptStart{
		ProjectID:      defaultWorkflowMetricsProjectID,
		IssueID:        issue.ID,
		Identifier:     issue.Identifier,
		WorkerType:     "agent",
		Lane:           issue.State,
		AttemptNumber:  1,
		StartedAt:      now.Add(-time.Minute),
		LeaseExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}
	state.Completed[issue.ID] = Completed{
		Issue:       issue,
		CompletedAt: now.Add(-5 * time.Minute),
		FinalState:  FinalStateCompleted,
	}
	state.Running[issue.ID] = Running{
		Issue:         issue,
		WorkAttemptID: attemptID,
		Generation:    1,
		StartedAt:     now.Add(-time.Minute),
	}
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	orch := &Orchestrator{
		cfg:           cfg,
		connector:     tracker,
		laneMutations: runtimeStore,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	result := orch.autoPromoteHumanReviewIssues(ctx, &state, []connector.Issue{issue}, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Merging"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if _, ok := result.transitioned[issue.ID]; !ok {
		t.Fatalf("transitioned = %#v, want %s", result.transitioned, issue.ID)
	}
	if _, ok := state.Running[issue.ID]; !ok {
		t.Fatalf("Running[%q] missing; promotion must not wait for stale dispatch completion", issue.ID)
	}
	if _, ok := state.Blocked[issue.ID]; ok {
		t.Fatalf("Blocked[%q] present after gate promotion", issue.ID)
	}
}

func TestTickAutoPromoteHydratesWorkpadBlockerBeforeTransition(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 6, 15, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-workpad-blocker", []string{"bug"}, &connector.PullRequest{
		Number:                 185,
		URL:                    "https://github.test/digitaldrywood/creswoodcorners-phone/pull/185",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "pass",
		CodexReviewSubmittedAt: &oldReview,
	})
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate:          gate.Config{Kind: gate.KindCommand, RequireAutomatedReview: new(false)},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	mergingSlot := dispatchTestIssue("issue-merging-slot", "Merging")
	state.Running[mergingSlot.ID] = Running{Issue: mergingSlot}
	tracker := &autoPromoteTickConnector{
		stateIssues: []connector.Issue{issue},
		issueComments: map[string][]connector.IssueComment{
			issue.ID: {{
				Body: "## Codex Workpad\n\n### Blockers\n- no generated seasonal MP3s were copied into `assets/audio/`\n- Gate A/B/C owner listening approval is still required before approved audio assets are copied and committed",
			}},
		},
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	orch.tick(context.Background(), &state, now)

	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want no transition while Workpad blocker is present", tracker.updates)
	}
	if got, want := tracker.fetchComments, []string{issue.ID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FetchIssueComments issue IDs = %#v, want %#v", got, want)
	}
	for _, fragment := range []string{
		"action=await_review",
		"reason=workpad_blocker",
		"workpad_blocker=\"no generated seasonal MP3s were copied into `assets/audio/`; Gate A/B/C owner listening approval is still required before approved audio assets are copied and committed\"",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
}

func TestTickAutoPromoteFetchesStructuredWorkpadOverStaleBlockerReason(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 19, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-stale-blocker-reason", []string{"bug"}, &connector.PullRequest{
		Number:                 1494,
		URL:                    "https://github.test/digitaldrywood/detent/pull/1494",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "pass",
		CodexReviewSubmittedAt: &oldReview,
	})
	issue.BlockerReason = "Blocked by: #1462 stale issue-body prose"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate:          gate.Config{Kind: gate.KindCommand, RequireAutomatedReview: new(false)},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	tracker := &autoPromoteTickConnector{
		stateIssues: []connector.Issue{issue},
		issueComments: map[string][]connector.IssueComment{
			issue.ID: {{
				Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```",
				URL:  "https://github.test/comment/structured-complete",
			}},
		},
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	result := orch.autoPromoteHumanReviewIssues(context.Background(), &state, []connector.Issue{issue}, now)

	if got, want := tracker.fetchComments, []string{issue.ID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FetchIssueComments issue IDs = %#v, want %#v", got, want)
	}
	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Merging"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if _, ok := result.transitioned[issue.ID]; !ok {
		t.Fatalf("transitioned = %#v, want %s", result.transitioned, issue.ID)
	}
	for _, fragment := range []string{
		"reason=ready",
		"workpad_signal_source=structured",
		"workpad_comment_url=https://github.test/comment/structured-complete",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
	if strings.Contains(logs.String(), "reason=workpad_blocker") {
		t.Fatalf("logs %q contain stale workpad_blocker decision", logs.String())
	}
}

func TestTickAutoPromoteResolvesClosedWorkpadBlockerBeforeTransition(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 8, 22, 30, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-resolved-workpad-blocker", []string{"bug"}, &connector.PullRequest{
		Number:                 1480,
		URL:                    "https://github.test/digitaldrywood/pyroapex/pull/1480",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "pass",
		CodexReviewSubmittedAt: &oldReview,
	})
	issue.BlockedBy = []connector.BlockedRef{{Identifier: "digitaldrywood/detent#1462"}}
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate:          gate.Config{Kind: gate.KindCommand, RequireAutomatedReview: new(false)},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	mergingSlot := dispatchTestIssue("issue-merging-slot", "Merging")
	state.Running[mergingSlot.ID] = Running{Issue: mergingSlot}
	tracker := &autoPromoteTickConnector{
		stateIssues: []connector.Issue{issue},
		issueComments: map[string][]connector.IssueComment{
			issue.ID: {{
				Body: "## Codex Workpad\n\n### Blockers\n- Blocked by: #1462\n\n### Validation\n- make check passed.",
			}},
		},
		resolvedIssues: []connector.Issue{{
			ID:         "issue-1462",
			Identifier: "digitaldrywood/detent#1462",
			State:      "Done",
		}},
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	result := orch.autoPromoteHumanReviewIssues(context.Background(), &state, []connector.Issue{issue}, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Merging"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if _, ok := result.transitioned[issue.ID]; !ok {
		t.Fatalf("transitioned = %#v, want %s", result.transitioned, issue.ID)
	}
	if got, want := tracker.fetchIdentifiers, [][]string{{"digitaldrywood/detent#1462"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FetchIssueStatesByIdentifiers = %#v, want %#v", got, want)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one auto-promote audit comment", tracker.comments)
	}
	for _, fragment := range []string{
		"action=promote",
		"reason=ready",
		"resolved_workpad_blockers=digitaldrywood/detent#1462",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
	if strings.Contains(logs.String(), "reason=workpad_blocker") {
		t.Fatalf("logs %q contain workpad_blocker after resolved dependency hydration", logs.String())
	}
}

func TestTickAutoPromoteResolvesMergedWorkpadBlockerBeforeTransition(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 8, 22, 35, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-merged-workpad-blocker", []string{"bug"}, &connector.PullRequest{
		Number:                 1481,
		URL:                    "https://github.test/digitaldrywood/pyroapex/pull/1481",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "pass",
		CodexReviewSubmittedAt: &oldReview,
	})
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate:          gate.Config{Kind: gate.KindCommand, RequireAutomatedReview: new(false)},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	tracker := &autoPromoteTickConnector{
		stateIssues: []connector.Issue{issue},
		issueComments: map[string][]connector.IssueComment{
			issue.ID: {{
				Body: "## Codex Workpad\n\n### Blockers\n- Blocked by: #1462\n\n### Validation\n- make check passed.",
			}},
		},
		resolvedIssues: []connector.Issue{{
			ID:          "issue-1462",
			Identifier:  "digitaldrywood/detent#1462",
			State:       "In Progress",
			PullRequest: &connector.PullRequest{Number: 1482, State: "MERGED"},
		}},
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	result := orch.autoPromoteHumanReviewIssues(context.Background(), &state, []connector.Issue{issue}, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Merging"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if _, ok := result.transitioned[issue.ID]; !ok {
		t.Fatalf("transitioned = %#v, want %s", result.transitioned, issue.ID)
	}
	if !strings.Contains(logs.String(), "resolved_workpad_blockers=digitaldrywood/detent#1462") {
		t.Fatalf("logs %q missing resolved_workpad_blockers", logs.String())
	}
}

func TestTickAutoPromoteCommentsOnceForInvalidStructuredWorkpad(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 13, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-invalid-workpad-status", []string{"bug"}, &connector.PullRequest{
		Number:                 1490,
		URL:                    "https://github.test/digitaldrywood/detent/pull/1490",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "pass",
		CodexReviewSubmittedAt: &oldReview,
	})
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate:          gate.Config{Kind: gate.KindCommand, RequireAutomatedReview: new(false)},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	invalidWorkpad := connector.IssueComment{
		Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: blocked\nblockers: []\nhuman_action: null\n```",
		URL:  "https://github.test/comment/invalid-workpad",
	}
	tracker := &autoPromoteTickConnector{
		stateIssues: []connector.Issue{issue},
		issueComments: map[string][]connector.IssueComment{
			issue.ID: {invalidWorkpad},
		},
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	result := orch.autoPromoteHumanReviewIssues(context.Background(), &State{}, []connector.Issue{issue}, now)

	if len(result.transitioned) != 0 {
		t.Fatalf("transitioned = %#v, want no transition", result.transitioned)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want none", tracker.updates)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one invalid Workpad comment", tracker.comments)
	}
	for _, fragment := range []string{
		"<!-- detent-workpad-status-invalid:",
		"status blocked requires at least one blocker ref, human_action, or reason_code",
		"https://github.test/comment/invalid-workpad",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("invalid comment %q missing %q", tracker.comments[0].body, fragment)
		}
	}
	for _, fragment := range []string{
		"reason=workpad_status_invalid",
		"workpad_signal_source=structured",
		"workpad_comment_url=https://github.test/comment/invalid-workpad",
		"workpad_status_hash=",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}

	tracker.issueComments[issue.ID] = append(tracker.issueComments[issue.ID], connector.IssueComment{Body: tracker.comments[0].body})
	orch.autoPromoteHumanReviewIssues(context.Background(), &State{}, []connector.Issue{issue}, now)
	if len(tracker.comments) != 1 {
		t.Fatalf("comments after dedupe = %#v, want still one invalid Workpad comment", tracker.comments)
	}
}

func TestTickAutoPromoteRoutesSuccessfulInvalidStructuredWorkpadToRework(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 9, 13, 30, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-invalid-workpad-success", []string{"bug"}, &connector.PullRequest{
		Number:                 1491,
		URL:                    "https://github.test/digitaldrywood/detent/pull/1491",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "pass",
		CodexReviewSubmittedAt: &oldReview,
	})
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate:          gate.Config{Kind: gate.KindCommand, RequireAutomatedReview: new(false)},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	state.Completed[issue.ID] = Completed{
		Issue:      issue,
		FinalState: FinalStateCompleted,
	}
	tracker := &autoPromoteTickConnector{
		stateIssues: []connector.Issue{issue},
		issueComments: map[string][]connector.IssueComment{
			issue.ID: {{
				Body: "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: human-review\nblockers: []\nhuman_action: null\n```",
				URL:  "https://github.test/comment/invalid-status-value",
			}},
		},
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	result := orch.autoPromoteHumanReviewIssues(context.Background(), &state, []connector.Issue{issue}, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Rework"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if _, ok := result.transitioned[issue.ID]; !ok {
		t.Fatalf("transitioned = %#v, want %s", result.transitioned, issue.ID)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one rework comment", tracker.comments)
	}
	for _, fragment := range []string{
		"Auto-promote routed this issue from Human Review to Rework",
		"reason: workpad_status_invalid",
		"human-review",
		"in_progress, blocked, complete",
		"https://github.test/comment/invalid-status-value",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
}

func TestAutoPromoteReworkLimitDistinctTransitions(t *testing.T) {
	t.Parallel()

	type laneEvent struct {
		lane     string
		previous string
		reason   string
	}
	for _, tt := range []struct {
		name      string
		events    []laneEvent
		wantCount int
		wantState string
	}{
		{
			name: "one real rework with duplicate observations",
			events: []laneEvent{
				{"Rework", "In Progress", "ci_not_green"},
				{"Rework", "", "tracker_state_observed"},
				{"Rework", "", "tracker_state_observed"},
			},
			wantCount: 1,
			wantState: "Rework",
		},
		{
			name: "three genuine reworks",
			events: []laneEvent{
				{"Rework", "In Progress", "ci_not_green"},
				{"In Progress", "Rework", "state_transition"},
				{"Rework", "In Progress", "ci_not_green"},
				{"In Progress", "Rework", "state_transition"},
				{"Rework", "In Progress", "ci_not_green"},
			},
			wantCount: 3,
			wantState: "Blocked",
		},
		{
			name: "reobserved existing lane",
			events: []laneEvent{
				{"Rework", "", "tracker_state_observed"},
				{" reWORK ", "", "tracker_state_observed"},
				{"Rework", "", "tracker_state_observed"},
			},
			wantCount: 1,
			wantState: "Rework",
		},
		{
			name: "observed genuine lane changes",
			events: []laneEvent{
				{"In Progress", "", "tracker_state_observed"},
				{"Rework", "", "tracker_state_observed"},
				{"In Progress", "", "tracker_state_observed"},
				{"Rework", "", "tracker_state_observed"},
				{"In Progress", "", "tracker_state_observed"},
				{"Rework", "", "tracker_state_observed"},
			},
			wantCount: 3,
			wantState: "Blocked",
		},
		{
			name: "explicit transitions with missing intervening entries",
			events: []laneEvent{
				{"Rework", "In Progress", "ci_not_green"},
				{"Rework", "In Progress", "ci_not_green"},
				{"Rework", "In Progress", "ci_not_green"},
			},
			wantCount: 3,
			wantState: "Blocked",
		},
		{
			name: "explicit same lane observation",
			events: []laneEvent{
				{"Rework", " reWORK ", "tracker_state_observed"},
			},
			wantState: "Rework",
		},
		{
			name: "other lane observations",
			events: []laneEvent{
				{"In Progress", "", "tracker_state_observed"},
				{"In Progress", "", "tracker_state_observed"},
			},
			wantState: "Rework",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
			issue := autoPromoteTickIssue("issue-rework-observations", nil, &connector.PullRequest{
				Number:   2149,
				HeadSHA:  "same-head",
				State:    "OPEN",
				CIStatus: "fail",
			})
			metrics := &autoPromoteWorkflowMetricsRecorder{}
			for i, event := range tt.events {
				if _, err := metrics.RecordWorkflowPhaseEvent(t.Context(), store.WorkflowPhaseEvent{
					IssueID:           issue.ID,
					PhaseType:         store.WorkflowPhaseTypeLane,
					PhaseName:         event.lane,
					PreviousPhaseName: event.previous,
					Status:            "entered",
					Reason:            event.reason,
					StartedAt:         now.Add(time.Duration(i-len(tt.events)) * time.Minute),
					MetadataJSON:      autoPromoteReworkEventMetadata(2149, "same-head"),
				}); err != nil {
					t.Fatal(err)
				}
			}
			for _, event := range []store.WorkflowPhaseEvent{
				{PhaseType: store.WorkflowPhaseTypeAgentSession, PhaseName: "Rework", Status: "entered"},
				{PhaseType: store.WorkflowPhaseTypeLane, PhaseName: "Rework", Status: "exited"},
			} {
				event.IssueID = issue.ID
				event.StartedAt = now
				event.MetadataJSON = autoPromoteReworkEventMetadata(2149, "same-head")
				if _, err := metrics.RecordWorkflowPhaseEvent(t.Context(), event); err != nil {
					t.Fatal(err)
				}
			}
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
			orch := &Orchestrator{
				cfg:             normalizeConfig(Config{AutoPromote: AutoPromoteConfig{Enabled: true, ReworkLimit: 3}}),
				connector:       tracker,
				workflowMetrics: metrics,
			}
			summary := AutoPromoteSummary{}
			limit, err := orch.autoPromoteReworkLimit(t.Context(), issue, summary)
			if err != nil {
				t.Fatal(err)
			}
			if limit.Count != tt.wantCount {
				t.Fatalf("rework count = %d, want %d", limit.Count, tt.wantCount)
			}
			if tt.name == "one real rework with duplicate observations" {
				want := []autoPromoteReworkReasonCount{{Reason: "ci_not_green", Count: 1}}
				if !reflect.DeepEqual(limit.ReasonCounts, want) {
					t.Fatalf("reason counts = %#v, want %#v", limit.ReasonCounts, want)
				}
			}
			state := newState(orch.cfg)
			gotState, applied := orch.applyAutoPromoteDecisionWithTarget(t.Context(), &state, issue, summary,
				AutoPromoteDecision{Action: AutoPromoteActionRework, Reason: AutoPromoteReasonCINotGreen}, "Rework", now)
			if !applied || gotState != tt.wantState {
				t.Fatalf("transition = (%q, %t), want (%q, true)", gotState, applied, tt.wantState)
			}
		})
	}
}

func TestTickAutoPromoteBlocksWhenReworkLimitReached(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 2, 16, 10, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-rework-limit", []string{"bug"}, &connector.PullRequest{
		Number:                 43,
		URL:                    "https://github.test/digitaldrywood/detent/pull/43",
		State:                  "OPEN",
		CIStatus:               "pass",
		CodexReviewState:       "P1",
		CodexReviewSubmittedAt: &oldReview,
		CodexReviewFindings: []connector.PullRequestFinding{{
			Body: "![P1 Badge](https://example.test/p1.svg) Unsafe migration.",
			URL:  "https://github.test/digitaldrywood/detent/pull/43#pullrequestreview-1",
		}},
	})
	issue.Identifier = "digitaldrywood/detent#857"
	issue.URL = "https://github.test/digitaldrywood/detent/issues/857"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			ReworkLimit:   1,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	metrics := &autoPromoteWorkflowMetricsRecorder{}
	if _, err := metrics.RecordWorkflowPhaseEvent(context.Background(), store.WorkflowPhaseEvent{
		ProjectID:    defaultWorkflowMetricsProjectID,
		IssueID:      issue.ID,
		Identifier:   issue.Identifier,
		IssueURL:     issue.URL,
		PhaseType:    store.WorkflowPhaseTypeLane,
		PhaseName:    "Rework",
		Reason:       string(AutoPromoteReasonP1Findings),
		Status:       "entered",
		StartedAt:    now.Add(-2 * time.Hour),
		MetadataJSON: "{}",
	}); err != nil {
		t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
	}
	orch := &Orchestrator{
		cfg:             cfg,
		connector:       tracker,
		workflowMetrics: metrics,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	orch.tick(context.Background(), &state, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Blocked"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one Blocked handoff comment", tracker.comments)
	}
	for _, fragment := range []string{
		"Auto-promote routed this issue from Human Review to Blocked because the Rework limit was reached.",
		"rework_limit: 1",
		"prior_rework_transitions: 1",
		"current_rework_reason: p1_findings",
		"repeated_rework_reasons: p1_findings x1",
		"Unsafe migration.",
		"https://github.test/digitaldrywood/detent/pull/43#pullrequestreview-1",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
	events := metrics.snapshot()
	if len(events) != 3 {
		t.Fatalf("workflow metric events = %#v, want prior Rework plus exit/Blocked enter", events)
	}
	blocked := events[2]
	if blocked.PhaseName != "Blocked" || blocked.Status != "entered" || blocked.Reason != "rework_limit" {
		t.Fatalf("blocked metric = %#v, want Blocked entered with rework_limit reason", blocked)
	}
}

func TestTickAutoPromoteBlocksRepeatedCINotGreenSignature(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 8, 14, 30, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-red-ci-loop", []string{"bug"}, &connector.PullRequest{
		Number:   1046,
		URL:      "https://github.test/digitaldrywood/detent/pull/1046",
		State:    "OPEN",
		HeadSHA:  "same-head",
		CIStatus: "fail",
		RequiredCheckFailures: []connector.PullRequestCheck{
			{Name: "Test", Status: "completed", Conclusion: "failure"},
			{Name: "Tier-1 Race Tests", Status: "completed", Conclusion: "failure"},
		},
	})
	issue.Identifier = "digitaldrywood/detent#1046"
	issue.URL = "https://github.test/digitaldrywood/detent/issues/1046"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			ReworkLimit:   1,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	metrics := &autoPromoteWorkflowMetricsRecorder{}
	prNumber := int64(1046)
	if _, err := metrics.RecordWorkflowPhaseEvent(context.Background(), store.WorkflowPhaseEvent{
		ProjectID:    defaultWorkflowMetricsProjectID,
		IssueID:      issue.ID,
		Identifier:   issue.Identifier,
		IssueURL:     issue.URL,
		PRNumber:     &prNumber,
		PhaseType:    store.WorkflowPhaseTypeLane,
		PhaseName:    "Rework",
		Reason:       string(AutoPromoteReasonCINotGreen),
		Status:       "entered",
		StartedAt:    now.Add(-time.Hour),
		MetadataJSON: autoPromoteReworkEventMetadata(1046, "same-head", "Test", "Tier-1 Race Tests"),
	}); err != nil {
		t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
	}
	orch := &Orchestrator{
		cfg:             cfg,
		connector:       tracker,
		workflowMetrics: metrics,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	orch.tick(context.Background(), &state, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Blocked"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one Blocked handoff comment", tracker.comments)
	}
	for _, fragment := range []string{
		"Rework limit was reached",
		"prior_rework_transitions: 1",
		"current_rework_reason: ci_not_green",
		"repeated_rework_reasons: ci_not_green x1",
		"failed_checks: Test, Tier-1 Race Tests",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
	events := metrics.snapshot()
	blocked := events[len(events)-1]
	if blocked.PhaseName != "Blocked" || blocked.Reason != "rework_limit" {
		t.Fatalf("latest workflow event = %#v, want Blocked rework_limit entry", blocked)
	}
	metadata, ok := workflowLaneMetadataFromJSON(blocked.MetadataJSON)
	if !ok || metadata.ReworkBreaker == nil || metadata.ReworkBreaker.Reason != string(AutoPromoteReasonCINotGreen) {
		t.Fatalf("blocked metadata = %#v, want ci_not_green rework breaker reason", metadata)
	}
}

func TestTickAutoUnparksClearedReworkBreaker(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 21, 0, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-cleared-rework-breaker", []string{"bug"}, &connector.PullRequest{
		Number:         1585,
		URL:            "https://github.test/digitaldrywood/pyroapex/pull/1585",
		State:          "OPEN",
		HeadSHA:        "parked-head",
		MergeableState: "clean",
		CIStatus:       "success",
		CheckRunCount:  4,
	})
	issue.Identifier = "digitaldrywood/pyroapex#1521"
	issue.URL = "https://github.test/digitaldrywood/pyroapex/issues/1521"
	issue.State = "Blocked"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:     true,
			ReworkLimit: 3,
			Gate: gate.Config{
				Kind:            gate.KindCommand,
				AutomatedReview: gate.AutomatedReviewOff,
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	metrics := &autoPromoteWorkflowMetricsRecorder{}
	prNumber := int64(1585)
	for _, event := range []store.WorkflowPhaseEvent{
		{
			ProjectID:    defaultWorkflowMetricsProjectID,
			IssueID:      issue.ID,
			Identifier:   issue.Identifier,
			IssueURL:     issue.URL,
			PRNumber:     &prNumber,
			PhaseType:    store.WorkflowPhaseTypeLane,
			PhaseName:    autoPromoteReworkState,
			Reason:       string(AutoPromoteReasonCINotGreen),
			Status:       "entered",
			StartedAt:    now.Add(-2 * time.Hour),
			MetadataJSON: autoPromoteReworkEventMetadata(1585, "parked-head", "Test"),
		},
		{
			ProjectID:    defaultWorkflowMetricsProjectID,
			IssueID:      issue.ID,
			Identifier:   issue.Identifier,
			IssueURL:     issue.URL,
			PRNumber:     &prNumber,
			PhaseType:    store.WorkflowPhaseTypeLane,
			PhaseName:    blockedStatusState,
			Reason:       "rework_limit",
			Status:       "entered",
			StartedAt:    now.Add(-time.Hour),
			MetadataJSON: autoPromoteReworkEventMetadata(1585, "parked-head", "Test"),
		},
	} {
		if _, err := metrics.RecordWorkflowPhaseEvent(context.Background(), event); err != nil {
			t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
		}
	}
	orch := &Orchestrator{
		cfg:             cfg,
		connector:       tracker,
		workflowMetrics: metrics,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	orch.tick(context.Background(), &state, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: autoPromoteMergingState}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one breaker recovery audit comment", tracker.comments)
	}
	for _, fragment := range []string{"Auto-unparked", "Blocked to Merging", "ci_not_green", "parked-head"} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
}

func TestTickAutoPromoteResetsReworkLimitAfterHeadSHAChange(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 8, 14, 45, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-red-ci-new-head", []string{"bug"}, &connector.PullRequest{
		Number:   1047,
		URL:      "https://github.test/digitaldrywood/detent/pull/1047",
		State:    "OPEN",
		HeadSHA:  "new-head",
		CIStatus: "fail",
		RequiredCheckFailures: []connector.PullRequestCheck{{
			Name:       "Test",
			Status:     "completed",
			Conclusion: "failure",
		}},
	})
	issue.Identifier = "digitaldrywood/detent#1047"
	issue.URL = "https://github.test/digitaldrywood/detent/issues/1047"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			ReworkLimit:   1,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	metrics := &autoPromoteWorkflowMetricsRecorder{}
	prNumber := int64(1047)
	if _, err := metrics.RecordWorkflowPhaseEvent(context.Background(), store.WorkflowPhaseEvent{
		ProjectID:    defaultWorkflowMetricsProjectID,
		IssueID:      issue.ID,
		Identifier:   issue.Identifier,
		IssueURL:     issue.URL,
		PRNumber:     &prNumber,
		PhaseType:    store.WorkflowPhaseTypeLane,
		PhaseName:    "Rework",
		Reason:       string(AutoPromoteReasonCINotGreen),
		Status:       "entered",
		StartedAt:    now.Add(-time.Hour),
		MetadataJSON: autoPromoteReworkEventMetadata(1047, "old-head", "Test"),
	}); err != nil {
		t.Fatalf("RecordWorkflowPhaseEvent() error = %v", err)
	}
	orch := &Orchestrator{
		cfg:             cfg,
		connector:       tracker,
		workflowMetrics: metrics,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	orch.tick(context.Background(), &state, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Rework"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if len(tracker.comments) != 1 || strings.Contains(tracker.comments[0].body, "Rework limit was reached") {
		t.Fatalf("comments = %#v, want ordinary Rework handoff", tracker.comments)
	}
	events := metrics.snapshot()
	rework := events[len(events)-1]
	signature := autoPromoteReworkSignatureFromEvent(rework)
	if rework.PhaseName != "Rework" || signature.HeadSHA != "new-head" || !slices.Equal(signature.FailedChecks, []string{"Test"}) {
		t.Fatalf("latest Rework event = %#v signature = %#v, want new-head Test signature", rework, signature)
	}
}

func TestTickRetriesTransientHumanReviewCIBeforeRework(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 14, 5, 0, 0, time.UTC)
	reviewedAt := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-transient-ci", []string{"bug"}, &connector.PullRequest{
		Number:                 51,
		URL:                    "https://github.test/digitaldrywood/detent/pull/51",
		State:                  "OPEN",
		HeadSHA:                "head-transient",
		CIStatus:               "fail",
		BranchName:             "detent/transient-ci",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &reviewedAt,
		TransientFailedChecks: []connector.PullRequestCheck{{
			ID:            9001,
			WorkflowRunID: 8001,
			Name:          "Checks",
			Status:        "completed",
			Conclusion:    "failure",
			DetailsURL:    "https://github.test/digitaldrywood/detent/actions/runs/8001/job/9001",
		}},
	})
	retryLimit := 2
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled: true,
			Gate: gate.Config{
				Kind:                  gate.KindCommand,
				CIFailureAction:       gate.CIFailureActionRework,
				TransientCIRetryLimit: &retryLimit,
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	tracker := &autoPromoteTickConnector{
		stateIssues:        []connector.Issue{issue},
		candidateIssuesSet: true,
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	orch.tick(context.Background(), &state, now)

	if len(tracker.reruns) != 1 || tracker.reruns[0].issueID != issue.ID || len(tracker.reruns[0].checks) != 1 {
		t.Fatalf("reruns = %#v, want one rerun for transient check", tracker.reruns)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want no Rework transition while retrying transient CI", tracker.updates)
	}
	if len(tracker.comments) != 1 || !strings.Contains(tracker.comments[0].body, "Transient CI failure detected") {
		t.Fatalf("comments = %#v, want transient CI retry audit comment", tracker.comments)
	}
}

func TestTickDoesNotRetryTransientHumanReviewCIBeforeGateBlocksOnCI(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 14, 6, 0, 0, time.UTC)
	reviewedAt := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-transient-ci-waiting-review", []string{"bug"}, &connector.PullRequest{
		Number:                 53,
		URL:                    "https://github.test/digitaldrywood/detent/pull/53",
		State:                  "OPEN",
		HeadSHA:                "head-transient",
		CIStatus:               "fail",
		BranchName:             "detent/transient-ci",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &reviewedAt,
		TransientFailedChecks: []connector.PullRequestCheck{{
			ID:            9003,
			WorkflowRunID: 8003,
			Name:          "Checks",
			Status:        "completed",
			Conclusion:    "failure",
			DetailsURL:    "https://github.test/digitaldrywood/detent/actions/runs/8003/job/9003",
		}},
	})
	retryLimit := 2
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:            true,
			AllowedIssueLabels: []string{"allowed"},
			Gate: gate.Config{
				Kind:                  gate.KindCommand,
				CIFailureAction:       gate.CIFailureActionRework,
				TransientCIRetryLimit: &retryLimit,
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	tracker := &autoPromoteTickConnector{
		stateIssues:        []connector.Issue{issue},
		candidateIssuesSet: true,
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	orch.tick(context.Background(), &state, now)

	if len(tracker.reruns) != 0 {
		t.Fatalf("reruns = %#v, want no reruns before CI is the blocking gate reason", tracker.reruns)
	}
	if len(state.TransientCheckRetries) != 0 {
		t.Fatalf("TransientCheckRetries = %#v, want none before CI blocks promotion", state.TransientCheckRetries)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want no Rework transition while label is disallowed", tracker.updates)
	}
}

func TestTickRetriesTransientMergingCIBeforeRework(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 12, 14, 7, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-merging-transient-ci", []string{"bug"}, &connector.PullRequest{
		Number:     52,
		URL:        "https://github.test/digitaldrywood/detent/pull/52",
		State:      "OPEN",
		HeadSHA:    "head-transient",
		CIStatus:   "fail",
		BranchName: "detent/transient-ci",
		TransientFailedChecks: []connector.PullRequestCheck{{
			ID:            9002,
			WorkflowRunID: 8002,
			Name:          "Checks",
			Status:        "completed",
			Conclusion:    "failure",
			DetailsURL:    "https://github.test/digitaldrywood/detent/actions/runs/8002/job/9002",
		}},
	})
	issue.State = "Merging"
	retryLimit := 2
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled: true,
			Gate: gate.Config{
				Kind:                  gate.KindCommand,
				CIFailureAction:       gate.CIFailureActionRework,
				TransientCIRetryLimit: &retryLimit,
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		ObservedStates: []string{"Human Review", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	tracker := &autoPromoteTickConnector{
		stateIssues:        []connector.Issue{issue},
		candidateIssuesSet: true,
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	orch.tick(context.Background(), &state, now)

	if len(tracker.reruns) != 1 || tracker.reruns[0].issueID != issue.ID || len(tracker.reruns[0].checks) != 1 {
		t.Fatalf("reruns = %#v, want one rerun for transient check", tracker.reruns)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want no Rework transition while retrying transient CI", tracker.updates)
	}
	if len(tracker.comments) != 1 || !strings.Contains(tracker.comments[0].body, "Transient CI failure detected") {
		t.Fatalf("comments = %#v, want transient CI retry audit comment", tracker.comments)
	}
}

func TestObservedStatusFetchStatesForTickDoesNotThrottleCustomPassState(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		AutoPromote: AutoPromoteConfig{
			Enabled:     true,
			SourceState: "Review",
			PassState:   "Ready for Pickup",
			ReworkState: "Production Rework",
			Gate: gate.Config{
				Kind: gate.KindArtifact,
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Production Rework"},
		ObservedStates: []string{"Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	state := newState(cfg)
	state.Running["issue-merging"] = Running{
		Issue: connector.Issue{
			ID:    "issue-merging",
			State: "Merging",
		},
	}
	orch := Orchestrator{cfg: cfg}

	got := orch.observedStatusFetchStatesForTick(&state)
	want := []string{"Blocked", "Review", "Merging"}
	if !autoPromoteTickStatesEqual(got, want) {
		t.Fatalf("observedStatusFetchStatesForTick() = %#v, want %#v", got, want)
	}
}

func TestTickAutoPromoteLogsNonTransitionDecisionsAtInfo(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 22, 14, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-missing-review", []string{"bug"}, &connector.PullRequest{
		Number:     390,
		URL:        "https://github.test/digitaldrywood/detent/pull/390",
		BranchName: "detent/detent-digitaldrywood_detent_387-29d3e4765f21",
		State:      "OPEN",
		CIStatus:   "pass",
	})
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	state := newState(cfg)
	orch.tick(context.Background(), &state, now)

	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want none", tracker.updates)
	}
	for _, fragment := range []string{
		"level=INFO",
		"auto promote decision",
		"action=await_review",
		"reason=automated_review_missing",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
}

func TestTickAutoPromoteDefersWhenPullRequestHydrationRateLimited(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 7, 24, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	prior := autoPromoteTickIssue("issue-rate-limited-pr", []string{"bug"}, &connector.PullRequest{
		Number:                 77,
		URL:                    "https://github.test/digitaldrywood/creswoodcorners-phone/pull/77",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	prior.Identifier = "digitaldrywood/creswoodcorners-phone#69"
	current := autoPromoteTickIssue("issue-rate-limited-pr", []string{"bug"}, &connector.PullRequest{
		Number:                     77,
		HydrationUnavailableReason: "rate_limited",
	})
	current.Identifier = prior.Identifier
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{current}}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	state := newState(cfg)
	state.Pipeline = []connector.Issue{prior}
	orch.tick(context.Background(), &state, now)

	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want none", tracker.updates)
	}
	if strings.Contains(logs.String(), "reason=missing_pull_request") {
		t.Fatalf("logs %q contain missing_pull_request", logs.String())
	}
	for _, fragment := range []string{
		"reason=pull_request_hydration_unavailable",
		"pull_request_hydration_reason=rate_limited",
		"pull_request_number=77",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
	if len(state.Pipeline) != 1 || state.Pipeline[0].PullRequest == nil {
		t.Fatalf("Pipeline = %#v, want retained pull request metadata", state.Pipeline)
	}
	pr := state.Pipeline[0].PullRequest
	if pr.URL != "https://github.test/digitaldrywood/creswoodcorners-phone/pull/77" {
		t.Fatalf("retained PullRequest.URL = %q, want prior URL", pr.URL)
	}
	if pr.HydrationUnavailableReason != "rate_limited" {
		t.Fatalf("retained HydrationUnavailableReason = %q, want rate_limited", pr.HydrationUnavailableReason)
	}
}

func TestLogAutoPromoteDecisionIncludesHydrationReasons(t *testing.T) {
	t.Parallel()

	retryAt := time.Date(2026, 6, 25, 12, 5, 0, 0, time.UTC)
	tests := []struct {
		name        string
		pullRequest *connector.PullRequest
		want        []string
	}{
		{
			name: "primary exhausted",
			pullRequest: &connector.PullRequest{
				Number:                     77,
				HydrationUnavailableReason: connector.PullRequestHydrationReasonPrimaryExhausted,
			},
			want: []string{"pull_request_hydration_reason=primary_exhausted"},
		},
		{
			name: "secondary throttled",
			pullRequest: &connector.PullRequest{
				Number:                     77,
				HydrationUnavailableReason: connector.PullRequestHydrationReasonSecondaryThrottled,
				HydrationNextRetryAt:       &retryAt,
			},
			want: []string{
				"pull_request_hydration_reason=secondary_throttled",
				"pull_request_hydration_next_retry_at=2026-06-25T12:05:00Z",
			},
		},
		{
			name: "rest budget reserved",
			pullRequest: &connector.PullRequest{
				Number:                     77,
				HydrationUnavailableReason: connector.PullRequestHydrationReasonRESTBudgetReserved,
			},
			want: []string{"pull_request_hydration_reason=rest_budget_reserved"},
		},
		{
			name: "stale cached data",
			pullRequest: &connector.PullRequest{
				Number:                  77,
				HydrationDegradedReason: connector.PullRequestHydrationReasonStaleCachedPullData,
				HydrationNextRetryAt:    &retryAt,
			},
			want: []string{
				"pull_request_hydration_degraded_reason=stale_cached_pull_request",
				"pull_request_hydration_next_retry_at=2026-06-25T12:05:00Z",
			},
		},
		{
			name: "stale successful check run",
			pullRequest: &connector.PullRequest{
				Number:   77,
				CIStatus: "pass",
				StaleSuccessfulChecks: []connector.PullRequestCheck{{
					Name:       "Installer Smoke (ubuntu-latest)",
					Status:     "in_progress",
					Conclusion: "success",
				}},
			},
			want: []string{
				"ci_anomaly=stale_successful_check_run",
				"stale_successful_checks=\"Installer Smoke (ubuntu-latest)\"",
				"ci_anomaly_action=treated_completed_successful_check_runs_as_passed",
			},
		},
		{
			name: "review state disagreement",
			pullRequest: &connector.PullRequest{
				Number:                  77,
				CodexReviewAPIState:     "APPROVED",
				CodexReviewBodySeverity: "P1",
			},
			want: []string{
				"review_api_state=APPROVED",
				"review_body_severity=P1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logs strings.Builder
			orch := &Orchestrator{
				logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
			}
			issue := autoPromoteTickIssue("issue-hydration-log", []string{"bug"}, tt.pullRequest)
			orch.logAutoPromoteDecision(issue, AutoPromoteDecision{
				Action: AutoPromoteActionSkip,
				Reason: AutoPromoteReasonPullRequestHydrationUnavailable,
			}, "")

			for _, fragment := range tt.want {
				if !strings.Contains(logs.String(), fragment) {
					t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
				}
			}
		})
	}
}

func TestLogAutoPromoteDecisionNamesPendingChecks(t *testing.T) {
	t.Parallel()

	var logs strings.Builder
	orch := &Orchestrator{
		logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
	issue := autoPromoteTickIssue("issue-pending-ci-log", []string{"bug"}, &connector.PullRequest{
		Number:        1648,
		CIStatus:      "pending",
		RunningChecks: []string{"Checks"},
		RequiredCheckFailures: []connector.PullRequestCheck{{
			Name:   "Race Tests",
			Status: "pending",
		}},
	})
	orch.logAutoPromoteDecision(issue, AutoPromoteDecision{
		Action:   AutoPromoteActionSkip,
		Reason:   AutoPromoteReasonCINotGreen,
		CIStatus: "pending",
	}, "")

	if !strings.Contains(logs.String(), `pending_checks="Checks, Race Tests"`) {
		t.Fatalf("logs %q missing pending check names", logs.String())
	}
}

func TestLogAutoPromoteDecisionIncludesOperationalCompletionKind(t *testing.T) {
	t.Parallel()

	var logs strings.Builder
	orch := &Orchestrator{
		logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
	orch.logAutoPromoteDecision(
		autoPromoteTickIssue("issue-operational-log", []string{"operations"}, nil),
		AutoPromoteDecision{
			Action:              AutoPromoteActionComplete,
			Reason:              AutoPromoteReasonOperationalCompletion,
			OperationalEvidence: "Backfill verified.",
		},
		"Done",
	)

	if !strings.Contains(logs.String(), "completion_kind=operational") {
		t.Fatalf("logs %q missing operational completion kind", logs.String())
	}
}

func TestTickReconcilesStaleTodoLinkedPullRequests(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	ready := autoPromoteTickIssue("issue-ready-todo", []string{"bug"}, &connector.PullRequest{
		Number:                 36,
		URL:                    "https://github.test/digitaldrywood/creswoodcorners-phone/pull/36",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	ready.State = "Todo"
	ready.Identifier = "digitaldrywood/creswoodcorners-phone#33"
	conflicting := autoPromoteTickIssue("issue-conflicting-todo", []string{"bug"}, &connector.PullRequest{
		Number:         38,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/38",
		State:          "OPEN",
		MergeableState: "DIRTY",
		CIStatus:       "success",
	})
	conflicting.State = "Todo"
	conflicting.Identifier = "digitaldrywood/creswoodcorners-phone#32"
	unresolved := autoPromoteTickIssue("issue-unresolved-todo", []string{"bug"}, &connector.PullRequest{
		Number:                 39,
		URL:                    "https://github.test/digitaldrywood/creswoodcorners-phone/pull/39",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
		UnresolvedReviewThreads: []connector.PullRequestReviewThread{{
			Path: "internal/orchestrator/autopromote_tick.go",
			Line: 976,
		}},
	})
	unresolved.State = "Todo"
	unresolved.Identifier = "digitaldrywood/creswoodcorners-phone#34"
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{ready, conflicting, unresolved}}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	state := newState(cfg)
	orch.tick(context.Background(), &state, now)

	wantUpdates := []autoPromoteTickUpdate{
		{issueID: "issue-ready-todo", state: "Merging"},
		{issueID: "issue-conflicting-todo", state: "Rework"},
		{issueID: "issue-unresolved-todo", state: "Rework"},
	}
	if !reflect.DeepEqual(tracker.updates, wantUpdates) {
		t.Fatalf("updates = %#v, want %#v", tracker.updates, wantUpdates)
	}
	if want := []string{"issue-ready-todo", "issue-conflicting-todo", "issue-unresolved-todo"}; !reflect.DeepEqual(tracker.reviewThreadHydrations, want) {
		t.Fatalf("review thread hydrations = %#v, want %#v", tracker.reviewThreadHydrations, want)
	}
	if len(tracker.comments) != 3 {
		t.Fatalf("comments = %#v, want stale todo reconciliation comments", tracker.comments)
	}
	wantComments := map[string][]string{
		"issue-ready-todo": {
			"Auto-promoted this issue from Todo to Merging.",
			"reason: ready",
			"https://github.test/digitaldrywood/creswoodcorners-phone/pull/36",
		},
		"issue-conflicting-todo": {
			"Auto-promote routed this issue from Todo to Rework: linked PR has merge conflicts.",
			"reason: merge_conflicts",
			"mergeable_state: dirty",
			"https://github.test/digitaldrywood/creswoodcorners-phone/pull/38",
		},
		"issue-unresolved-todo": {
			"Auto-promote routed this issue from Todo to Rework: linked PR has 1 unresolved review thread.",
			"reason: unresolved_review_threads",
			"unresolved_review_threads: 1",
			"first_unresolved_review_thread: internal/orchestrator/autopromote_tick.go:976",
			"https://github.test/digitaldrywood/creswoodcorners-phone/pull/39",
		},
	}
	for _, comment := range tracker.comments {
		for _, fragment := range wantComments[comment.issueID] {
			if !strings.Contains(comment.body, fragment) {
				t.Fatalf("comment for %s = %q, missing %q", comment.issueID, comment.body, fragment)
			}
		}
	}
	for _, fragment := range []string{
		"stale_todo_pr_reconciled",
		"reason=ready",
		"reason=merge_conflicts",
		"reason=unresolved_review_threads",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
}

func TestStaleTodoPullRequestDecisionRoutesUnresolvedThreadsWhenDisabled(t *testing.T) {
	t.Parallel()

	decision := staleTodoPullRequestDecision(
		connector.Issue{},
		AutoPromoteSummary{UnresolvedReviewThreads: []connector.PullRequestReviewThread{{Path: "main.go", Line: 10}}},
		AutoPromoteConfig{Gate: gate.Config{Kind: gate.KindCommand}},
		time.Time{},
	)
	if decision.Action != AutoPromoteActionRework || decision.Reason != AutoPromoteReasonUnresolvedReviewThreads {
		t.Fatalf("decision = %#v, want unresolved review thread rework", decision)
	}
}

func TestTickReconcilesStaleTodoMergedPullRequestToDone(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 7, 15, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-pyroapex-1462", []string{"bug"}, &connector.PullRequest{
		Number:   1471,
		URL:      "https://github.test/digitaldrywood/pyroapex/pull/1471",
		State:    "MERGED",
		CIStatus: "success",
	})
	issue.State = "Todo"
	issue.Identifier = "digitaldrywood/pyroapex#1462"
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	attempts := &recordingWorkAttemptStore{}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	state := newState(cfg)
	orch.tick(context.Background(), &state, now)

	wantUpdates := []autoPromoteTickUpdate{{issueID: issue.ID, state: "Done"}}
	if !reflect.DeepEqual(tracker.updates, wantUpdates) {
		t.Fatalf("updates = %#v, want %#v", tracker.updates, wantUpdates)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one merged PR reconciliation comment", tracker.comments)
	}
	for _, fragment := range []string{
		"Reconciled this issue from Todo to Done because its linked PR is already merged.",
		"reason: pull_request_merged",
		"https://github.test/digitaldrywood/pyroapex/pull/1471",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
	if strings.Contains(logs.String(), "skip_reason=duplicate_pull_request_work") {
		t.Fatalf("logs %q contain duplicate_pull_request_work skip", logs.String())
	}
	for _, decision := range attempts.decisions {
		if decision.IssueID == issue.ID && decision.Reason == dispatchSkipDuplicatePullRequest {
			t.Fatalf("scheduler decision = %#v, want merged PR reconciliation instead of duplicate skip", decision)
		}
	}
}

func TestTickParksStaleTodoMergedPullRequestWhenHydrationUnavailable(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 7, 15, 5, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-pyroapex-merged-stale", []string{"bug"}, &connector.PullRequest{
		Number:                  1472,
		URL:                     "https://github.test/digitaldrywood/pyroapex/pull/1472",
		State:                   "MERGED",
		CIStatus:                "success",
		HydrationDegradedReason: connector.PullRequestHydrationReasonStaleCachedPullData,
	})
	issue.State = "Todo"
	issue.Identifier = "digitaldrywood/pyroapex#1464"
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	state := newState(cfg)
	orch.tick(context.Background(), &state, now)

	wantUpdates := []autoPromoteTickUpdate{{issueID: issue.ID, state: "Human Review"}}
	if !reflect.DeepEqual(tracker.updates, wantUpdates) {
		t.Fatalf("updates = %#v, want %#v", tracker.updates, wantUpdates)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one hydration reconciliation comment", tracker.comments)
	}
	for _, fragment := range []string{
		"Reconciled this issue from Todo to Human Review because linked PR status hydration is unavailable.",
		"reason: pull_request_hydration_unavailable",
		"pull_request_hydration_degraded_reason: stale_cached_pull_request",
		"https://github.test/digitaldrywood/pyroapex/pull/1472",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
	for _, fragment := range []string{
		"reason=pull_request_hydration_unavailable",
		"pull_request_hydration_degraded_reason=stale_cached_pull_request",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
}

func TestTickReconcilesStaleTodoMergedPullRequestWithFailedChecksToRework(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 7, 15, 10, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-pyroapex-1463", []string{"bug"}, &connector.PullRequest{
		Number:   1470,
		URL:      "https://github.test/digitaldrywood/pyroapex/pull/1470",
		State:    "MERGED",
		CIStatus: "fail",
		RequiredCheckFailures: []connector.PullRequestCheck{{
			Name:       "Tier-1 Race Tests",
			Status:     "completed",
			Conclusion: "failure",
		}},
	})
	issue.State = "Todo"
	issue.Identifier = "digitaldrywood/pyroapex#1463"
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	attempts := &recordingWorkAttemptStore{}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:          cfg,
		connector:    tracker,
		workAttempts: attempts,
		logger:       slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	state := newState(cfg)
	orch.tick(context.Background(), &state, now)

	wantUpdates := []autoPromoteTickUpdate{{issueID: issue.ID, state: "Rework"}}
	if !reflect.DeepEqual(tracker.updates, wantUpdates) {
		t.Fatalf("updates = %#v, want %#v", tracker.updates, wantUpdates)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one failed merged PR reconciliation comment", tracker.comments)
	}
	for _, fragment := range []string{
		"Reconciled this issue from Todo to Rework because its merged linked PR has failing CI evidence.",
		"reason: ci_not_green",
		"ci_status: fail",
		"failed_checks: Tier-1 Race Tests",
		"https://github.test/digitaldrywood/pyroapex/pull/1470",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
	if strings.Contains(logs.String(), "skip_reason=duplicate_pull_request_work") {
		t.Fatalf("logs %q contain duplicate_pull_request_work skip", logs.String())
	}
	for _, decision := range attempts.decisions {
		if decision.IssueID == issue.ID && decision.Reason == dispatchSkipDuplicatePullRequest {
			t.Fatalf("scheduler decision = %#v, want failed merged PR reconciliation instead of duplicate skip", decision)
		}
	}
}

func TestAutoPromoteFailedChecksIgnoreCancelledAndSkippedArtifacts(t *testing.T) {
	t.Parallel()

	pullRequest := &connector.PullRequest{
		SlowChecks: []connector.PullRequestCheck{
			{Name: "Cancelled coverage", Conclusion: "cancelled"},
			{Name: "Skipped label gate", Conclusion: "skipped"},
			{Name: "Neutral snapshot", Conclusion: "neutral"},
		},
		RequiredCheckFailures: []connector.PullRequestCheck{
			{Name: "Checks", Conclusion: "failure"},
			{Name: "Canceled spelling", Conclusion: "canceled"},
		},
	}

	want := []string{"Neutral snapshot", "Checks"}
	if got := autoPromoteFailedChecksFromPullRequest(pullRequest); !reflect.DeepEqual(got, want) {
		t.Fatalf("autoPromoteFailedChecksFromPullRequest() = %#v, want %#v", got, want)
	}
}

func TestTickReconcilesStaleTodoHydratesWorkpadBlockerBeforePromotion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 6, 16, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate:          gate.Config{Kind: gate.KindCommand, RequireAutomatedReview: new(false)},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-stale-todo-workpad-blocker", []string{"bug"}, &connector.PullRequest{
		Number:                 180,
		URL:                    "https://github.test/digitaldrywood/creswoodcorners-phone/pull/180",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "pass",
		CodexReviewSubmittedAt: &oldReview,
	})
	issue.State = "Todo"
	issue.Identifier = "digitaldrywood/creswoodcorners-phone#175"
	tracker := &autoPromoteTickConnector{
		stateIssues: []connector.Issue{issue},
		issueComments: map[string][]connector.IssueComment{
			issue.ID: {{
				Body: "## Codex Workpad\n\n### Blockers\n- Gate A/B/C owner listening approval is still required before approved audio assets are copied and committed.",
			}},
		},
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}

	state := newState(cfg)
	orch.tick(context.Background(), &state, now)

	wantUpdates := []autoPromoteTickUpdate{{issueID: issue.ID, state: "Human Review"}}
	if !reflect.DeepEqual(tracker.updates, wantUpdates) {
		t.Fatalf("updates = %#v, want %#v", tracker.updates, wantUpdates)
	}
	if got, want := tracker.fetchComments, []string{issue.ID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FetchIssueComments issue IDs = %#v, want %#v", got, want)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one stale Todo reconciliation comment", tracker.comments)
	}
	for _, fragment := range []string{
		"Reconciled this issue from Todo to Human Review because it already has a linked PR.",
		"reason: workpad_blocker",
		"https://github.test/digitaldrywood/creswoodcorners-phone/pull/180",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
	for _, fragment := range []string{
		"reason=workpad_blocker",
		"target_state=\"Human Review\"",
		"workpad_blocker=\"Gate A/B/C owner listening approval is still required before approved audio assets are copied and committed.\"",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
}

func TestTickDoesNotReconcileActiveTodoPullRequests(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 24, 13, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	running := autoPromoteTickIssue("issue-running-todo-pr", []string{"bug"}, &connector.PullRequest{
		Number:                 40,
		URL:                    "https://github.test/digitaldrywood/detent/pull/40",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	running.State = "Todo"
	claimed := autoPromoteTickIssue("issue-claimed-todo-pr", []string{"bug"}, &connector.PullRequest{
		Number:                 41,
		URL:                    "https://github.test/digitaldrywood/detent/pull/41",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	claimed.State = "Todo"
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{running, claimed}}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	state := newState(cfg)
	state.Running[running.ID] = Running{Issue: cloneIssue(running), StartedAt: now.Add(-time.Minute)}
	state.Claimed[running.ID] = Claimed{Issue: cloneIssue(running), ClaimedAt: now.Add(-time.Minute)}
	state.Claimed[claimed.ID] = Claimed{Issue: cloneIssue(claimed), ClaimedAt: now.Add(-time.Minute)}

	orch.tick(context.Background(), &state, now)

	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want none for active Todo PRs", tracker.updates)
	}
	if len(tracker.comments) != 0 {
		t.Fatalf("comments = %#v, want none for active Todo PRs", tracker.comments)
	}
	if _, ok := state.Running[running.ID]; !ok {
		t.Fatalf("Running[%q] missing after stale Todo PR reconciliation", running.ID)
	}
	if _, ok := state.Claimed[running.ID]; !ok {
		t.Fatalf("Claimed[%q] missing after stale Todo PR reconciliation", running.ID)
	}
	if _, ok := state.Claimed[claimed.ID]; !ok {
		t.Fatalf("Claimed[%q] missing after stale Todo PR reconciliation", claimed.ID)
	}
}

func TestTickAutoPromoteRunsValidatorStage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate: gate.Config{
				Kind: gate.KindCommand,
				Validator: gate.ValidatorConfig{
					Enabled:  true,
					MinScore: 0.8,
					BlockOn:  []string{"p1"},
				},
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-validator", []string{"enhancement"}, &connector.PullRequest{
		Number:                 522,
		URL:                    "https://github.test/digitaldrywood/detent/pull/522",
		BranchName:             "detent/digitaldrywood_detent_522",
		HeadSHA:                "head-validator",
		State:                  "OPEN",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	validator := &autoPromoteTickValidator{
		result: gate.ValidatorResult{
			Submitted: true,
			Verdict:   gate.ValidatorVerdictPass,
			Score:     0.91,
			Summary:   "Acceptance criteria pass.",
		},
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		validator: validator,
		logger:    slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	state := newState(cfg)
	orch.tick(context.Background(), &state, now)

	waitForValidatorRequests(t, validator, 1)
	waitForValidatorResult(t, orch, issue)
	if got := tracker.updates; len(got) != 0 {
		t.Fatalf("updates after scheduling validator = %#v, want none", got)
	}
	requests := validator.Requests()
	if requests[0].Issue.ID != "issue-validator" {
		t.Fatalf("validator issue = %#v, want issue-validator", requests[0].Issue)
	}

	mergingSlot := dispatchTestIssue("issue-validator-merging-slot", "Merging")
	state.Running[mergingSlot.ID] = Running{Issue: mergingSlot}
	orch.tick(context.Background(), &state, now.Add(time.Second))

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: "issue-validator", state: "Merging"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if len(tracker.prComments) != 1 {
		t.Fatalf("pull request comments = %#v, want validator result comment", tracker.prComments)
	}
	for _, fragment := range []string{"Validator verdict: pass", "score: 0.91", "Acceptance criteria pass."} {
		if !strings.Contains(tracker.prComments[0].body, fragment) {
			t.Fatalf("pull request comment %q missing %q", tracker.prComments[0].body, fragment)
		}
	}
}

func TestTickAutoPromoteStartsValidatorBeforeAutomatedReview(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	cfg := autoPromoteValidatorTestConfig()
	issue := autoPromoteTickIssue("issue-validator-no-review", []string{"bug"}, &connector.PullRequest{
		Number:     1298,
		URL:        "https://github.test/digitaldrywood/detent/pull/1298",
		BranchName: "detent/digitaldrywood_detent_1298",
		HeadSHA:    "head-validator-no-review",
		State:      "OPEN",
		CIStatus:   "success",
	})
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	validator := &autoPromoteTickValidator{
		result: gate.ValidatorResult{
			Submitted: true,
			Verdict:   gate.ValidatorVerdictPass,
			Score:     0.95,
		},
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		validator: validator,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)

	orch.tick(t.Context(), &state, now)

	waitForValidatorRequests(t, validator, 1)
	if got := state.AutoPromoteDecisions[issue.ID].Reason; got != AutoPromoteReasonValidatorMissing {
		t.Fatalf("auto-promote reason = %q, want %q", got, AutoPromoteReasonValidatorMissing)
	}
}

func TestTickAutoPromoteValidatorUnavailableRoutesRework(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	memo := openValidatorMemoStore(t)
	now := time.Date(2026, 7, 13, 10, 30, 0, 0, time.UTC)
	cfg := autoPromoteValidatorTestConfig()
	issue := autoPromoteTickIssue("issue-validator-unavailable", []string{"bug"}, &connector.PullRequest{
		Number:     1299,
		URL:        "https://github.test/digitaldrywood/detent/pull/1299",
		BranchName: "detent/digitaldrywood_detent_1299",
		HeadSHA:    "head-validator-unavailable",
		State:      "OPEN",
		CIStatus:   "success",
	})
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:           cfg,
		connector:     tracker,
		validatorMemo: memo,
		logger:        slog.New(slog.NewTextHandler(&logs, nil)),
		now:           func() time.Time { return now },
	}
	state := newState(cfg)

	orch.tick(ctx, &state, now)
	waitForValidatorResult(t, orch, issue)
	verdict := waitForPersistedValidatorFailure(t, memo, store.ValidatorVerdictKey{
		ProjectID: "detent",
		IssueID:   issue.ID,
		HeadSHA:   issue.PullRequest.HeadSHA,
	}, gate.DefaultValidatorMaxAttempts)
	if verdict.Summary != "validator review production could not start: validator runner unavailable" {
		t.Fatalf("validator summary = %q", verdict.Summary)
	}

	orch.tick(ctx, &state, now.Add(time.Second))
	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Rework"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	for _, fragment := range []string{"validator stage unavailable", "issue_id=issue-validator-unavailable", "pull_request=1299"} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs missing %q:\n%s", fragment, logs.String())
		}
	}
}

func TestTickAutoPromoteUsesPersistedValidatorVerdictAfterRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	memo := openValidatorMemoStore(t)
	now := time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	cfg := autoPromoteValidatorTestConfig()
	issue := autoPromoteTickIssue("issue-validator-restart", []string{"enhancement"}, &connector.PullRequest{
		Number:                 858,
		URL:                    "https://github.test/digitaldrywood/detent/pull/858",
		BranchName:             "detent/digitaldrywood_detent_858",
		HeadSHA:                "head-validator-restart",
		State:                  "OPEN",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	validator := &autoPromoteTickValidator{
		result: gate.ValidatorResult{
			Submitted: true,
			Verdict:   gate.ValidatorVerdictPass,
			Score:     0.94,
			Summary:   "Stored validator result.",
			Findings: []gate.Finding{{
				Severity: "p2",
				Body:     "non-blocking note",
				Path:     "internal/orchestrator/autopromote_tick.go",
				Line:     12,
			}},
		},
	}
	orch := &Orchestrator{
		cfg:           cfg,
		connector:     &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
		validator:     validator,
		validatorMemo: memo,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		now: func() time.Time {
			return now
		},
	}
	state := newState(cfg)
	orch.tick(ctx, &state, now)
	waitForValidatorRequests(t, validator, 1)
	waitForPersistedValidatorVerdict(t, memo, store.ValidatorVerdictKey{
		ProjectID: "detent",
		IssueID:   issue.ID,
		HeadSHA:   "head-validator-restart",
	})

	restartedValidator := &autoPromoteTickValidator{err: errors.New("validator should not dispatch")}
	restartedTracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	restarted := &Orchestrator{
		cfg:           cfg,
		connector:     restartedTracker,
		validator:     restartedValidator,
		validatorMemo: memo,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		now: func() time.Time {
			return now.Add(time.Minute)
		},
	}
	restartedState := newState(cfg)
	mergingSlot := dispatchTestIssue("issue-validator-restart-merging-slot", "Merging")
	restartedState.Running[mergingSlot.ID] = Running{Issue: mergingSlot}
	restarted.tick(ctx, &restartedState, now.Add(time.Minute))

	if got := restartedValidator.Requests(); len(got) != 0 {
		t.Fatalf("validator requests after restart = %#v, want none", got)
	}
	if got, want := restartedTracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Merging"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates after restart = %#v, want %#v", got, want)
	}
	if len(restartedTracker.prComments) != 1 {
		t.Fatalf("pull request comments after restart = %#v, want one validator result comment", restartedTracker.prComments)
	}
	for _, fragment := range []string{"Validator verdict: pass", "score: 0.94", "Stored validator result.", "non-blocking note"} {
		if !strings.Contains(restartedTracker.prComments[0].body, fragment) {
			t.Fatalf("pull request comment %q missing %q", restartedTracker.prComments[0].body, fragment)
		}
	}
}

func TestTickAutoPromoteValidatorVerdictHeadSHAInvalidatesMemo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	memo := openValidatorMemoStore(t)
	now := time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	cfg := autoPromoteValidatorTestConfig()
	issue := autoPromoteTickIssue("issue-validator-new-head", []string{"enhancement"}, &connector.PullRequest{
		Number:                 859,
		URL:                    "https://github.test/digitaldrywood/detent/pull/859",
		BranchName:             "detent/digitaldrywood_detent_859",
		HeadSHA:                "head-validator-old",
		State:                  "OPEN",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	if err := memo.RecordValidatorVerdict(ctx, store.ValidatorVerdict{
		ProjectID:  "detent",
		IssueID:    issue.ID,
		HeadSHA:    "head-validator-old",
		Identifier: issue.Identifier,
		Submitted:  true,
		Verdict:    gate.ValidatorVerdictPass,
		Score:      0.99,
		RecordedAt: now.Add(-time.Minute),
		UpdatedAt:  now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("RecordValidatorVerdict() error = %v", err)
	}

	fresh := cloneIssue(issue)
	fresh.PullRequest.HeadSHA = "head-validator-new"
	validator := &autoPromoteTickValidator{
		result: gate.ValidatorResult{
			Submitted: true,
			Verdict:   gate.ValidatorVerdictPass,
			Score:     0.91,
		},
	}
	orch := &Orchestrator{
		cfg:           cfg,
		connector:     &autoPromoteTickConnector{stateIssues: []connector.Issue{fresh}},
		validator:     validator,
		validatorMemo: memo,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		now: func() time.Time {
			return now
		},
	}
	state := newState(cfg)
	orch.tick(ctx, &state, now)

	waitForValidatorRequests(t, validator, 1)
	requests := validator.Requests()
	if requests[0].Issue.PullRequest == nil || requests[0].Issue.PullRequest.HeadSHA != "head-validator-new" {
		t.Fatalf("validator request head SHA = %#v, want new head", requests[0].Issue.PullRequest)
	}
	waitForValidatorResult(t, orch, fresh)
}

func TestTickAutoPromoteValidatorFailureBackoff(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newAutoPromoteTickClock(time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC))
	oldReview := clock.Now().Add(-20 * time.Minute)
	cfg := autoPromoteValidatorTestConfig()
	cfg.FailureRetryBaseDelay = 30 * time.Second
	cfg.MaxRetryBackoff = 2 * time.Minute
	issue := autoPromoteTickIssue("issue-validator-backoff", []string{"enhancement"}, &connector.PullRequest{
		Number:                 860,
		URL:                    "https://github.test/digitaldrywood/detent/pull/860",
		BranchName:             "detent/digitaldrywood_detent_860",
		HeadSHA:                "head-validator-backoff",
		State:                  "OPEN",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	validator := &autoPromoteTickValidator{err: errors.New("validator unavailable")}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		validator: validator,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:       clock.Now,
	}
	state := newState(cfg)

	orch.tick(ctx, &state, clock.Now())
	waitForValidatorRequests(t, validator, 1)
	failure := waitForValidatorFailure(t, orch, issue, 1)
	if got, want := failure.NextRetryAt, clock.Now().Add(30*time.Second); !got.Equal(want) {
		t.Fatalf("NextRetryAt = %s, want %s", got, want)
	}

	immediate := clock.Now().Add(time.Second)
	clock.Set(immediate)
	orch.tick(ctx, &state, immediate)
	if got := len(validator.Requests()); got != 1 {
		t.Fatalf("validator requests during backoff = %d, want 1", got)
	}

	resumeAt := failure.NextRetryAt
	clock.Set(resumeAt)
	orch.tick(ctx, &state, resumeAt)
	waitForValidatorRequests(t, validator, 2)
}

func TestTickAutoPromoteValidatorFailureExhaustionRoutesRework(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	memo := openValidatorMemoStore(t)
	clock := newAutoPromoteTickClock(time.Date(2026, 7, 13, 11, 0, 0, 0, time.UTC))
	cfg := autoPromoteValidatorTestConfig()
	cfg.FailureRetryBaseDelay = time.Second
	cfg.MaxRetryBackoff = time.Second
	issue := autoPromoteTickIssue("issue-validator-exhausted", []string{"bug"}, &connector.PullRequest{
		Number:     1298,
		URL:        "https://github.test/digitaldrywood/detent/pull/1298",
		BranchName: "detent/digitaldrywood_detent_1298",
		HeadSHA:    "head-validator-exhausted",
		State:      "OPEN",
		CIStatus:   "success",
	})
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	validator := &autoPromoteTickValidator{err: errors.New("validator returned no output")}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:           cfg,
		connector:     tracker,
		validator:     validator,
		validatorMemo: memo,
		logger:        slog.New(slog.NewTextHandler(&logs, nil)),
		now:           clock.Now,
	}
	state := newState(cfg)
	key := store.ValidatorVerdictKey{
		ProjectID: "detent",
		IssueID:   issue.ID,
		HeadSHA:   issue.PullRequest.HeadSHA,
	}

	orch.tick(ctx, &state, clock.Now())
	failure := waitForValidatorFailure(t, orch, issue, 1)
	first := waitForPersistedValidatorFailure(t, memo, key, 1)
	if first.Verdict != gate.ValidatorVerdictError || first.Submitted || first.NextRetryAt == nil {
		t.Fatalf("first persisted validator failure = %#v", first)
	}

	clock.Set(failure.NextRetryAt)
	orch.tick(ctx, &state, clock.Now())
	failure = waitForValidatorFailure(t, orch, issue, 2)
	waitForPersistedValidatorFailure(t, memo, key, 2)

	clock.Set(failure.NextRetryAt)
	orch.tick(ctx, &state, clock.Now())
	orch.validatorWG.Wait()
	waitForValidatorRequests(t, validator, gate.DefaultValidatorMaxAttempts)
	waitForValidatorResult(t, orch, issue)
	exhausted := waitForPersistedValidatorFailure(t, memo, key, gate.DefaultValidatorMaxAttempts)
	if exhausted.NextRetryAt != nil {
		t.Fatalf("exhausted validator retry_at = %s, want nil", exhausted.NextRetryAt)
	}

	clock.Set(clock.Now().Add(time.Second))
	orch.tick(ctx, &state, clock.Now())
	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Rework"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	for _, fragment := range []string{
		"validator stage failed; retry scheduled",
		"validator stage retries exhausted",
		"issue_id=issue-validator-exhausted",
		"pull_request=1298",
		"validator returned no output",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs missing %q:\n%s", fragment, logs.String())
		}
	}
	if len(tracker.prComments) != 1 || !strings.Contains(tracker.prComments[0].body, "Validator verdict: error") {
		t.Fatalf("pull request comments = %#v, want validator error", tracker.prComments)
	}
}

func TestTickAutoPromoteRecordsValidatorReworkHandoff(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 14, 11, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate: gate.Config{
				Kind: gate.KindCommand,
				Validator: gate.ValidatorConfig{
					Enabled:  true,
					MinScore: 0.8,
					BlockOn:  []string{"p1"},
				},
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-validator-rework", []string{"enhancement"}, &connector.PullRequest{
		Number:                 856,
		URL:                    "https://github.test/digitaldrywood/detent/pull/856",
		BranchName:             "detent/digitaldrywood_detent_856",
		HeadSHA:                "head-validator-rework",
		State:                  "OPEN",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	validator := &autoPromoteTickValidator{
		result: gate.ValidatorResult{
			Submitted: true,
			Verdict:   gate.ValidatorVerdictRework,
			Score:     0.42,
			Summary:   "Missing deterministic rework context.",
			Findings: []gate.Finding{{
				Severity: "p1",
				Body:     "Prior validator finding is absent from rework prompt.",
				Path:     "internal/runner/prompt.go",
				Line:     44,
			}},
		},
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		validator: validator,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	state := newState(cfg)
	orch.tick(context.Background(), &state, now)
	waitForValidatorRequests(t, validator, 1)
	waitForValidatorResult(t, orch, issue)
	orch.tick(context.Background(), &state, now.Add(time.Second))

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: "issue-validator-rework", state: "Rework"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	handoff, ok := state.PriorAttempts[issue.ID]
	if !ok {
		t.Fatalf("PriorAttempts[%q] missing", issue.ID)
	}
	if handoff.Source != "auto_promote" || handoff.Reason != string(AutoPromoteReasonValidatorBlockedSeverity) {
		t.Fatalf("handoff = %#v, want auto_promote validator_blocked_severity", handoff)
	}
	if handoff.Validator.Verdict != gate.ValidatorVerdictRework || handoff.Validator.Score != 0.42 {
		t.Fatalf("handoff validator = %#v", handoff.Validator)
	}
	if len(handoff.Validator.Findings) != 1 || handoff.Validator.Findings[0].Path != "internal/runner/prompt.go" || handoff.Validator.Findings[0].Line != 44 {
		t.Fatalf("handoff findings = %#v", handoff.Validator.Findings)
	}
}

func TestRunDrainsInFlightValidatorStageOnShutdown(t *testing.T) {
	t.Parallel()

	oldReview := time.Now().Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-validator-shutdown", []string{"bug"}, &connector.PullRequest{
		Number:                 826,
		URL:                    "https://github.test/digitaldrywood/detent/pull/826",
		BranchName:             "detent/digitaldrywood_detent_826",
		HeadSHA:                "head-validator-shutdown",
		State:                  "OPEN",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	runningIssue := dispatchTestIssue("issue-running-shutdown", "Todo")
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue, runningIssue}}
	runner := newBlockingAutoPromoteValidatorRunner()
	t.Cleanup(runner.Release)
	globalGate := scheduler.NewGlobalDispatchGate(scheduler.NewRoundRobin(scheduler.Config{Capacity: 1}))

	orch, err := New(Config{
		PollInterval:        time.Hour,
		MaxConcurrentAgents: 1,
		Project:             scheduler.ProjectCandidate{ID: "alpha", Weight: 1},
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate: gate.Config{
				Kind: gate.KindCommand,
				Validator: gate.ValidatorConfig{
					Enabled: true,
				},
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	}, Dependencies{
		Connector:          tracker,
		Runner:             runner,
		GlobalDispatchGate: globalGate,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- orch.Run(runCtx)
	}()

	select {
	case request := <-runner.started:
		if request.Issue.ID != issue.ID {
			t.Fatalf("validator issue ID = %q, want %q", request.Issue.ID, issue.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("validator did not start")
	}
	select {
	case request := <-runner.runStarted:
		if request.Issue.ID != runningIssue.ID {
			t.Fatalf("run issue ID = %q, want %q", request.Issue.ID, runningIssue.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("worker run did not start")
	}

	cancel()

	select {
	case <-runner.canceled:
	case <-time.After(time.Second):
		t.Fatal("validator did not observe shutdown cancellation")
	}
	waitForGlobalDispatchSlot(t, globalGate, "bravo")

	select {
	case err := <-runDone:
		t.Fatalf("Run() returned before validator exited: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	runner.Release()

	select {
	case <-runner.done:
	case <-time.After(time.Second):
		t.Fatal("validator did not exit")
	}

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after validator exited")
	}
}

func TestTickRequeuesObservedStaleMergingIssueForDispatch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 24, 15, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-stale-merging", []string{"bug"}, &connector.PullRequest{
		Number:                 54,
		URL:                    "https://github.test/digitaldrywood/creswoodcorners-phone/pull/54",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/creswoodcorners-phone#49"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tracker := &autoPromoteTickConnector{
		stateIssues:        []connector.Issue{issue},
		candidateIssuesSet: true,
	}
	runner := newWorkerHostRunner()
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:        cfg,
		connector:  tracker,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
		logger:     slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)

	orch.tick(context.Background(), &state, now)

	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != issue.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, issue.ID)
	}
	if request.Issue.State != "Merging" {
		t.Fatalf("RunRequest.Issue.State = %q, want Merging", request.Issue.State)
	}
	if _, ok := state.Running[issue.ID]; !ok {
		t.Fatalf("Running[%q] missing after stale Merging dispatch", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; !ok {
		t.Fatalf("Claimed[%q] missing after stale Merging dispatch", issue.ID)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want none", tracker.updates)
	}
	for _, fragment := range []string{"merge_worker_pickup", "source=stale_merging", "merge_worker_attempt", "pull_request_number=54"} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
	if running := state.Running[issue.ID]; running.cancel != nil {
		running.cancel()
	}
}

func TestTickDefersStaleMergingCandidateWhenObservedHydrationUnavailable(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 16, 4, 0, 0, time.UTC)
	retryAt := now.Add(3 * time.Minute)
	candidate := autoPromoteTickIssue("issue-stale-merging-rate-limited", []string{"bug"}, &connector.PullRequest{
		Number:         80,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/80",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "b8e85ef7554b4f9cf385adba88ed151e2f69a4f0",
	})
	candidate.State = "Merging"
	candidate.Identifier = "digitaldrywood/creswoodcorners-phone#79"
	observed := cloneIssue(candidate)
	observed.PullRequest.HydrationUnavailableReason = connector.PullRequestHydrationReasonSecondaryThrottled
	observed.PullRequest.HydrationNextRetryAt = &retryAt
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tracker := &autoPromoteTickConnector{
		stateIssues:        []connector.Issue{observed},
		candidateIssues:    []connector.Issue{candidate},
		candidateIssuesSet: true,
	}
	runner := newWorkerHostRunner()
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:        cfg,
		connector:  tracker,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
		logger:     slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)

	orch.tick(context.Background(), &state, now)

	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want none", tracker.updates)
	}
	if _, ok := state.Running[candidate.ID]; ok {
		t.Fatalf("Running[%q] present, want no merge worker dispatch", candidate.ID)
	}
	if _, ok := state.Claimed[candidate.ID]; ok {
		t.Fatalf("Claimed[%q] present, want no merge worker claim", candidate.ID)
	}
	select {
	case request := <-runner.started:
		t.Fatalf("unexpected merge worker dispatch = %#v", request)
	default:
	}
	for _, fragment := range []string{
		"stale_merging_pr_reconciliation_deferred",
		"reason=pull_request_hydration_unavailable",
		"pull_request_hydration_reason=secondary_throttled",
		"pull_request_hydration_next_retry_at=2026-06-25T16:07:00Z",
		"pull_request_number=80",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
	for _, fragment := range []string{"merge_worker_pickup", "merge_worker_attempt"} {
		if strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q contain %q, want no merge worker pickup or attempt", logs.String(), fragment)
		}
	}
}

func TestTickDispatchesFreshAutoPromotedMergingIssue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 24, 17, 27, 1, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-auto-promoted-merging", []string{"bug"}, &connector.PullRequest{
		Number:                 70,
		URL:                    "https://github.test/digitaldrywood/creswoodcorners-phone/pull/70",
		State:                  "OPEN",
		MergeableState:         "clean",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	issue.Identifier = "digitaldrywood/creswoodcorners-phone#62"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	runner := newWorkerHostRunner()
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:        cfg,
		connector:  tracker,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
		logger:     slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)

	orch.tick(context.Background(), &state, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Merging"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != issue.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, issue.ID)
	}
	if request.Issue.State != "Merging" {
		t.Fatalf("RunRequest.Issue.State = %q, want Merging", request.Issue.State)
	}
	for _, fragment := range []string{
		"merge_worker_pickup",
		"source=auto_promote",
		"merge_worker_attempt",
		"pull_request_number=70",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
	if running := state.Running[issue.ID]; running.cancel != nil {
		running.cancel()
	}
}

func TestTickDraftPullRequestDoesNotBlockReadyPeerMerge(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC)
	draft := autoPromoteTickIssue("issue-draft-peer", []string{"bug"}, &connector.PullRequest{
		Number:         45,
		URL:            "https://github.test/digitaldrywood/video-studio/pull/45",
		State:          "OPEN",
		Draft:          true,
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "draft-head",
	})
	draft.Identifier = "digitaldrywood/video-studio#41"
	ready := autoPromoteTickIssue("issue-ready-peer", []string{"bug"}, &connector.PullRequest{
		Number:         55,
		URL:            "https://github.test/digitaldrywood/video-studio/pull/55",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "ready-head",
	})
	ready.Identifier = "digitaldrywood/video-studio#50"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		MergeMethod: "squash",
		AutoPromote: AutoPromoteConfig{
			Enabled: true,
			Gate: gate.Config{
				Kind:                   gate.KindCommand,
				RequireAutomatedReview: new(false),
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	baseConnector := &autoPromoteTickConnector{stateIssues: []connector.Issue{draft, ready}}
	tracker := &autoPromoteTickMergeConnector{autoPromoteTickConnector: baseConnector}
	runner := newWorkerHostRunner()
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:        cfg,
		connector:  tracker,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
		logger:     slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)

	orch.tick(t.Context(), &state, now)

	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != ready.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want ready peer %q", request.Issue.ID, ready.ID)
	}
	if _, ok := state.Running[draft.ID]; ok {
		t.Fatalf("Running[%q] present, want draft peer to acquire no slot", draft.ID)
	}
	if running := state.Running[ready.ID]; running.cancel != nil {
		running.cancel()
	}
	orch.handleRunResult(t.Context(), &state, runpkg.Completion{
		IssueID:     ready.ID,
		CompletedAt: now.Add(time.Minute),
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			Output:     "validated current-head CI and updated the Workpad",
		},
	})

	if len(tracker.merges) != 1 || tracker.merges[0].number != 55 {
		t.Fatalf("merges = %#v, want ready PR #55 merged", tracker.merges)
	}
	wantUpdates := []autoPromoteTickUpdate{
		{issueID: ready.ID, state: "Merging"},
		{issueID: ready.ID, state: "Done"},
	}
	if !reflect.DeepEqual(baseConnector.updates, wantUpdates) {
		t.Fatalf("updates = %#v, want %#v", baseConnector.updates, wantUpdates)
	}
	logLines := strings.Split(logs.String(), "\n")
	draftSlotAcquisition := false
	for _, line := range logLines {
		if strings.Contains(line, "merge_worker_slot_acquired") && strings.Contains(line, "identifier="+draft.Identifier) {
			draftSlotAcquisition = true
			break
		}
	}
	if draftSlotAcquisition {
		t.Fatalf("logs = %q, want draft peer to acquire zero merge slots", logs.String())
	}
	for _, fragment := range []string{
		"identifier=" + draft.Identifier + " action=skip reason=draft_pull_request",
		"merge_worker_slot_acquired",
		"identifier=" + ready.Identifier,
		"merge_worker_success",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs = %q, missing %q", logs.String(), fragment)
		}
	}
}

func TestTickReconcilesStaleMergingPullRequestStates(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 24, 18, 0, 0, 0, time.UTC)
	merged := autoPromoteTickIssue("issue-merged-pr", []string{"bug"}, &connector.PullRequest{
		Number:         71,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/71",
		State:          "MERGED",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	merged.State = "Merging"
	merged.Identifier = "digitaldrywood/creswoodcorners-phone#63"
	conflicting := autoPromoteTickIssue("issue-conflicting-merging", []string{"bug"}, &connector.PullRequest{
		Number:         72,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/72",
		State:          "OPEN",
		MergeableState: "DIRTY",
		CIStatus:       "success",
	})
	conflicting.State = "Merging"
	conflicting.Identifier = "digitaldrywood/creswoodcorners-phone#64"
	pending := autoPromoteTickIssue("issue-pending-merging", []string{"bug"}, &connector.PullRequest{
		Number:         74,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/74",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "pending",
	})
	pending.State = "Merging"
	pending.Identifier = "digitaldrywood/creswoodcorners-phone#66"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tracker := &autoPromoteTickConnector{
		stateIssues:        []connector.Issue{merged, conflicting, pending},
		candidateIssuesSet: true,
	}
	runner := newWorkerHostRunner()
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:        cfg,
		connector:  tracker,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
		logger:     slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)

	orch.tick(context.Background(), &state, now)

	wantUpdates := []autoPromoteTickUpdate{
		{issueID: "issue-merged-pr", state: "Done"},
	}
	if !reflect.DeepEqual(tracker.updates, wantUpdates) {
		t.Fatalf("updates = %#v, want %#v", tracker.updates, wantUpdates)
	}
	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != conflicting.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want dirty queue head %q", request.Issue.ID, conflicting.ID)
	}
	if _, ok := state.Running[conflicting.ID]; !ok {
		t.Fatalf("Running[%q] missing after dirty Merging queue head dispatch", conflicting.ID)
	}
	if _, ok := state.Running[pending.ID]; ok {
		t.Fatalf("Running[%q] present, want same-repo sibling left queued", pending.ID)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one reconciliation comment", tracker.comments)
	}
	wantComments := map[string][]string{
		"issue-merged-pr": {
			"Reconciled this issue from Merging to Done.",
			"reason: pull_request_merged",
			"https://github.test/digitaldrywood/creswoodcorners-phone/pull/71",
		},
	}
	for _, comment := range tracker.comments {
		for _, fragment := range wantComments[comment.issueID] {
			if !strings.Contains(comment.body, fragment) {
				t.Fatalf("comment for %s = %q, missing %q", comment.issueID, comment.body, fragment)
			}
		}
	}
	for _, fragment := range []string{
		"stale_merging_pr_reconciled",
		"reason=pull_request_merged",
		"merge_worker_attempt",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
	if strings.Contains(logs.String(), "reason=merge_conflicts") {
		t.Fatalf("logs %q contain merge_conflicts, want dirty Merging PR handled by merge worker", logs.String())
	}
	if running := state.Running[conflicting.ID]; running.cancel != nil {
		running.cancel()
	}
}

func TestStaleMergingOperationalCompletionReconcilesDone(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tests := []struct {
		name       string
		body       string
		wantState  string
		wantReason string
	}{
		{
			name:       "declared operational completion",
			body:       operationalCompletionWorkpadBody("Runner service is healthy and accepting jobs."),
			wantState:  "Done",
			wantReason: string(AutoPromoteReasonOperationalCompletion),
		},
		{
			name:       "ordinary no diff completion",
			body:       "## Codex Workpad\n\n```detent-status\nschema: 1\nstatus: complete\nblockers: []\nhuman_action: null\n```",
			wantState:  "Human Review",
			wantReason: string(AutoPromoteReasonMissingPullRequest),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := autoPromoteTickIssue("issue-operational-merging", []string{"bug"}, nil)
			issue.State = "Merging"
			if tt.wantReason == string(AutoPromoteReasonOperationalCompletion) {
				issue.Description = operationalCompletionAuthorizationBody()
			}
			issue.Comments = []connector.IssueComment{{
				Body: tt.body,
				URL:  "https://github.test/comment/operational-merging",
			}}
			decision := staleMergingPullRequestDecisionForIssue(issue, cfg)
			if decision.targetState != tt.wantState || decision.reason != tt.wantReason {
				t.Fatalf("decision = %#v, want state %q reason %q", decision, tt.wantState, tt.wantReason)
			}
			if tt.wantReason != string(AutoPromoteReasonOperationalCompletion) {
				return
			}
			comment := staleMergingPullRequestComment(issue, decision)
			if strings.Contains(comment, string(AutoPromoteReasonMissingPullRequest)) {
				t.Fatalf("comment %q contains missing_pull_request", comment)
			}
			for _, fragment := range []string{
				"operational_evidence: Runner service is healthy and accepting jobs.",
				"workpad_comment: https://github.test/comment/operational-merging",
			} {
				if !strings.Contains(comment, fragment) {
					t.Fatalf("comment %q missing fragment %q", comment, fragment)
				}
			}
		})
	}
}

func TestAutoPromoteOperationalCompletionAfterRuntimeStateLoss(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 18, 18, 30, 0, 0, time.UTC)
	workpadRecordedAt := now.Add(-2 * time.Minute)
	successfulAttempt := store.WorkAttempt{
		ID:            1,
		StartedAt:     now.Add(-time.Minute),
		CompletedAt:   now.Add(-30 * time.Second),
		TerminalState: store.WorkAttemptTerminalSuccess,
		WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
			"run_mode": runpkg.RunModeImplement,
			implementProgressMetadataKey: implementProgressRecord{
				Outcome:            string(store.WorkAttemptTerminalSuccess),
				Reason:             implementOperationalCompletion,
				WorkspaceDiffStats: implementProgressDiffStats{Status: "clean"},
				WorkpadStatus:      workpad.StatusComplete,
				ProgressKinds:      []string{"operational_completion"},
				CompletionKind:     workpad.CompletionOperational,
			},
		}),
	}
	tests := []struct {
		name        string
		attempts    []store.WorkAttempt
		wantRestore bool
	}{
		{name: "successful attempt after declaration", attempts: []store.WorkAttempt{successfulAttempt}, wantRestore: true},
		{name: "no terminal attempt"},
		{
			name: "attempt before declaration",
			attempts: []store.WorkAttempt{func() store.WorkAttempt {
				attempt := successfulAttempt
				attempt.CompletedAt = workpadRecordedAt.Add(-time.Second)
				return attempt
			}()},
		},
		{
			name: "attempt without operational completion marker",
			attempts: []store.WorkAttempt{func() store.WorkAttempt {
				attempt := successfulAttempt
				attempt.WorkerMetadataJSON = marshalWorkAttemptJSON(map[string]any{
					"run_mode": runpkg.RunModeImplement,
					implementProgressMetadataKey: implementProgressRecord{
						Outcome:       string(store.WorkAttemptTerminalSuccess),
						Reason:        implementProgressReasonNonDiff,
						WorkpadStatus: workpad.StatusComplete,
					},
				})
				return attempt
			}()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := autoPromoteTickIssue("issue-operational-restart", []string{"bug"}, nil)
			issue.State = "In Progress"
			issue.Description = operationalCompletionAuthorizationBody()
			issue.Comments = []connector.IssueComment{{
				Body:      operationalCompletionWorkpadBody("Runner service is healthy and accepting jobs."),
				URL:       "https://github.test/comment/operational-restart",
				CreatedAt: &workpadRecordedAt,
			}}
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
			attempts := &recordingWorkAttemptStore{history: tt.attempts}
			cfg := normalizeConfig(Config{
				AutoPromote: AutoPromoteConfig{
					Enabled: true,
					Gate:    gate.Config{Kind: gate.KindCommand},
				},
				ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
				TerminalStates: []string{"Done", "Cancelled"},
			})
			orch := &Orchestrator{cfg: cfg, connector: tracker, workAttempts: attempts}
			state := newState(cfg)

			orch.restoreDurableGateWaitCompletions(t.Context(), &state, []connector.Issue{issue})
			_, restored := state.Completed[issue.ID]
			if restored != tt.wantRestore {
				t.Fatalf("state.Completed[%q] present = %v, want %v", issue.ID, restored, tt.wantRestore)
			}

			result := orch.autoPromoteHumanReviewIssues(t.Context(), &state, []connector.Issue{issue}, now)
			_, transitioned := result.transitioned[issue.ID]
			if transitioned != tt.wantRestore {
				t.Fatalf("transitioned[%q] present = %v, want %v", issue.ID, transitioned, tt.wantRestore)
			}
			wantUpdates := []autoPromoteTickUpdate(nil)
			if tt.wantRestore {
				wantUpdates = []autoPromoteTickUpdate{{issueID: issue.ID, state: "Done"}}
			}
			if got, want := tracker.updates, wantUpdates; !reflect.DeepEqual(got, want) {
				t.Fatalf("updates = %#v, want %#v", got, want)
			}
			if len(result.dispatchCandidates) != 0 {
				t.Fatalf("dispatchCandidates = %#v, want none", result.dispatchCandidates)
			}
			if tt.wantRestore {
				if len(tracker.comments) != 1 || strings.Contains(tracker.comments[0].body, string(AutoPromoteReasonMissingPullRequest)) {
					t.Fatalf("comments = %#v, want one operational audit comment", tracker.comments)
				}
				if !strings.Contains(tracker.comments[0].body, "reason: operational_completion") {
					t.Fatalf("comment = %q, want operational completion reason", tracker.comments[0].body)
				}
			} else if len(tracker.comments) != 0 {
				t.Fatalf("comments = %#v, want none", tracker.comments)
			}
			if len(attempts.historyQueries) != 1 {
				t.Fatalf("history queries = %d, want 1", len(attempts.historyQueries))
			}
		})
	}
}

func TestReconcileStaleMergingOperationalCompletionWaitsForDurableAttempt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 18, 19, 0, 0, 0, time.UTC)
	workpadRecordedAt := now.Add(-2 * time.Minute)
	completedAttempt := store.WorkAttempt{
		CompletedAt:   now.Add(-time.Minute),
		TerminalState: store.WorkAttemptTerminalSuccess,
		WorkerMetadataJSON: marshalWorkAttemptJSON(map[string]any{
			"run_mode": runpkg.RunModeImplement,
			implementProgressMetadataKey: implementProgressRecord{
				Outcome:        string(store.WorkAttemptTerminalSuccess),
				Reason:         implementOperationalCompletion,
				WorkpadStatus:  workpad.StatusComplete,
				ProgressKinds:  []string{"operational_completion"},
				CompletionKind: workpad.CompletionOperational,
			},
		}),
	}
	tests := []struct {
		name     string
		attempts []store.WorkAttempt
		wantDone bool
	}{
		{name: "waits without terminal attempt"},
		{name: "completes after terminal attempt", attempts: []store.WorkAttempt{completedAttempt}, wantDone: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := autoPromoteTickIssue("issue-operational-stale-merging", []string{"bug"}, nil)
			issue.State = "Merging"
			issue.Description = operationalCompletionAuthorizationBody()
			issue.Comments = []connector.IssueComment{{
				Body:      operationalCompletionWorkpadBody("Runner service is healthy and accepting jobs."),
				URL:       "https://github.test/comment/operational-stale-merging",
				CreatedAt: &workpadRecordedAt,
			}}
			tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
			cfg := normalizeConfig(Config{
				AutoPromote:    AutoPromoteConfig{Enabled: true, Gate: gate.Config{Kind: gate.KindCommand}},
				ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
				TerminalStates: []string{"Done", "Cancelled"},
			})
			orch := &Orchestrator{
				cfg:          cfg,
				connector:    tracker,
				workAttempts: &recordingWorkAttemptStore{history: tt.attempts},
				logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			state := newState(cfg)

			transitioned := orch.reconcileStaleMergingPullRequestIssues(t.Context(), &state, []connector.Issue{issue}, now)

			_, done := transitioned[issue.ID]
			if done != tt.wantDone {
				t.Fatalf("transitioned[%q] present = %v, want %v", issue.ID, done, tt.wantDone)
			}
			wantUpdates := []autoPromoteTickUpdate(nil)
			if tt.wantDone {
				wantUpdates = []autoPromoteTickUpdate{{issueID: issue.ID, state: "Done"}}
			}
			if !reflect.DeepEqual(tracker.updates, wantUpdates) {
				t.Fatalf("updates = %#v, want %#v", tracker.updates, wantUpdates)
			}
			for _, comment := range tracker.comments {
				if strings.Contains(comment.body, string(AutoPromoteReasonMissingPullRequest)) {
					t.Fatalf("comment = %q, must not report missing_pull_request", comment.body)
				}
			}
		})
	}
}

func TestStaleMergingLinkedPullRequestDoesNotDependOnBranchName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		branch string
	}{
		{name: "pre-existing arbitrary branch", branch: "claude/add-object-lifecycle-module-i6pKl"},
		{name: "Detent-generated branch", branch: "detent/gopherguides_corp_74"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := connector.Issue{
				ID:           "I_74",
				Identifier:   "gopherguides/corp#74",
				State:        "Merging",
				PRRepository: "gopherguides/corp",
				PullRequest: &connector.PullRequest{
					Number:         186,
					URL:            "https://github.com/gopherguides/corp/pull/186",
					BranchName:     tt.branch,
					State:          "OPEN",
					MergeableState: "clean",
					CIStatus:       "success",
				},
			}

			decision := staleMergingPullRequestDecisionForIssue(issue, normalizeConfig(Config{
				ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
				TerminalStates: []string{"Done", "Cancelled"},
			}))
			if decision != (staleMergingPullRequestDecision{}) {
				t.Fatalf("staleMergingPullRequestDecisionForIssue() = %#v, want issue retained in Merging", decision)
			}
		})
	}
}

func TestTickAdvancesStaleMergingLaneAfterFrontPRReconcilesDone(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 24, 18, 30, 0, 0, time.UTC)
	front := autoPromoteTickIssue("issue-front-merged", []string{"bug"}, &connector.PullRequest{
		Number:         71,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/71",
		State:          "MERGED",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	front.State = "Merging"
	front.Identifier = "digitaldrywood/creswoodcorners-phone#63"
	next := autoPromoteTickIssue("issue-next-ready", []string{"bug"}, &connector.PullRequest{
		Number:         72,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/72",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	next.State = "Merging"
	next.Identifier = "digitaldrywood/creswoodcorners-phone#64"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tracker := &autoPromoteTickConnector{
		stateIssues:        []connector.Issue{front, next},
		candidateIssuesSet: true,
	}
	runner := newWorkerHostRunner()
	orch := &Orchestrator{
		cfg:        cfg,
		connector:  tracker,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)

	orch.tick(context.Background(), &state, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: front.ID, state: "Done"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != next.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, next.ID)
	}
	if _, ok := state.Running[next.ID]; !ok {
		t.Fatalf("Running[%q] missing after front PR reconciliation", next.ID)
	}
	if running := state.Running[next.ID]; running.cancel != nil {
		running.cancel()
	}
}

func TestTickDispatchesDirtySameRepoMergingQueueHeadForRefresh(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 22, 0, 0, 0, time.UTC)
	headCreatedAt := now.Add(-2 * time.Hour)
	siblingCreatedAt := now.Add(-time.Hour)
	head := autoPromoteTickIssue("issue-head-dirty", []string{"bug"}, &connector.PullRequest{
		Number:         75,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/75",
		State:          "OPEN",
		MergeableState: "DIRTY",
		CIStatus:       "success",
	})
	head.State = "Merging"
	head.Identifier = "digitaldrywood/creswoodcorners-phone#66"
	head.CreatedAt = &headCreatedAt
	sibling := autoPromoteTickIssue("issue-sibling-dirty", []string{"bug"}, &connector.PullRequest{
		Number:         76,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/76",
		State:          "OPEN",
		MergeableState: "DIRTY",
		CIStatus:       "success",
	})
	sibling.State = "Merging"
	sibling.Identifier = "digitaldrywood/creswoodcorners-phone#68"
	sibling.CreatedAt = &siblingCreatedAt
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tracker := &autoPromoteTickConnector{
		stateIssues:        []connector.Issue{sibling, head},
		candidateIssuesSet: true,
	}
	runner := newWorkerHostRunner()
	orch := &Orchestrator{
		cfg:        cfg,
		connector:  tracker,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)

	orch.tick(context.Background(), &state, now)

	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want no Rework transition before merge-worker refresh", tracker.updates)
	}
	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != head.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want queue head %q", request.Issue.ID, head.ID)
	}
	if _, ok := state.Running[head.ID]; !ok {
		t.Fatalf("Running[%q] missing after dirty Merging queue head dispatch", head.ID)
	}
	if _, ok := state.Running[sibling.ID]; ok {
		t.Fatalf("Running[%q] present, want same-repo sibling left queued", sibling.ID)
	}
	if running := state.Running[head.ID]; running.cancel != nil {
		running.cancel()
	}
}

func TestTickReworksRedStaleMergingQueueHead(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 22, 15, 0, 0, time.UTC)
	issue := autoPromoteTickIssue("issue-head-red", []string{"bug"}, &connector.PullRequest{
		Number:         75,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/75",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "fail",
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/creswoodcorners-phone#66"
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tracker := &autoPromoteTickConnector{
		stateIssues:        []connector.Issue{issue},
		candidateIssuesSet: true,
	}
	runner := newWorkerHostRunner()
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:        cfg,
		connector:  tracker,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
		logger:     slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)

	orch.tick(context.Background(), &state, now)

	wantUpdates := []autoPromoteTickUpdate{{issueID: issue.ID, state: "Rework"}}
	if !reflect.DeepEqual(tracker.updates, wantUpdates) {
		t.Fatalf("updates = %#v, want %#v", tracker.updates, wantUpdates)
	}
	select {
	case request := <-runner.started:
		t.Fatalf("unexpected merge worker dispatch = %#v", request)
	default:
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one stale Merging reconciliation comment", tracker.comments)
	}
	for _, fragment := range []string{
		"Reconciled this issue from Merging to Rework.",
		"reason: ci_not_green",
		"ci_status: fail",
		"https://github.test/digitaldrywood/creswoodcorners-phone/pull/75",
	} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment = %q, missing %q", tracker.comments[0].body, fragment)
		}
	}
	for _, fragment := range []string{"stale_merging_pr_reconciled", "reason=ci_not_green", "target_state=Rework"} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
	if strings.Contains(logs.String(), "merge_worker_attempt") {
		t.Fatalf("logs %q contain merge_worker_attempt, want no merge worker dispatch for red CI", logs.String())
	}
}

func TestMergeWorkerLogsRunResultSuccessAndFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 24, 19, 0, 0, 0, time.UTC)
	enteredAt := now.Add(-8 * time.Minute)
	slotAcquiredAt := now.Add(-6 * time.Minute)
	startedAt := now.Add(-5 * time.Minute)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:      []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-merge-log", []string{"bug"}, &connector.PullRequest{
		Number:         73,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/73",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "head-merge-log",
		BaseSHA:        "base-merge-log",
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/creswoodcorners-phone#65"

	var failureLogs strings.Builder
	failureState := newState(cfg)
	failureState.MergeTimings[issue.ID] = MergeTiming{
		EnteredMergingAt:          enteredAt,
		MergeWorkerSlotAcquiredAt: slotAcquiredAt,
		MergeStartedAt:            startedAt,
	}
	failureState.Running[issue.ID] = Running{TurnCount: 1, Issue: cloneIssue(issue), StartedAt: startedAt}
	failureOrch := &Orchestrator{
		cfg:    cfg,
		logger: slog.New(slog.NewTextHandler(&failureLogs, nil)),
	}
	failureOrch.handleRunResult(context.Background(), &failureState, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Err:         errors.New("merge command failed"),
	})
	for _, fragment := range []string{
		"merge_failed",
		"reason=runner_failed",
		"merge command failed",
		"queue_wait_seconds=120",
		"active_merge_duration_seconds=360",
		"total_merging_seconds=480",
		"head_sha=head-merge-log",
		"base_sha=base-merge-log",
	} {
		if !strings.Contains(failureLogs.String(), fragment) {
			t.Fatalf("failure logs %q missing fragment %q", failureLogs.String(), fragment)
		}
	}
	if timing := failureState.MergeTimings[issue.ID]; timing.MergeFailedAt.IsZero() || timing.MergeFailureReason != "runner_failed" {
		t.Fatalf("failure MergeTimings[%q] = %#v, want failed terminal state", issue.ID, timing)
	}

	var successLogs strings.Builder
	successIssue := cloneIssue(issue)
	successIssue.Closed = true
	successIssue.ClosedReason = "completed"
	successState := newState(cfg)
	successState.MergeTimings[successIssue.ID] = MergeTiming{
		EnteredMergingAt:          enteredAt,
		MergeWorkerSlotAcquiredAt: slotAcquiredAt,
		MergeStartedAt:            startedAt,
	}
	successState.Running[successIssue.ID] = Running{TurnCount: 1, Issue: cloneIssue(successIssue), StartedAt: startedAt}
	successOrch := &Orchestrator{
		cfg:       cfg,
		connector: &autoPromoteTickConnector{stateIssues: []connector.Issue{successIssue}},
		logger:    slog.New(slog.NewTextHandler(&successLogs, nil)),
	}
	successOrch.completeTerminalRunning(context.Background(), &successState, successIssue.ID, successState.Running[successIssue.ID], now, TokenTotals{})
	for _, fragment := range []string{
		"merge_completed",
		"final_state=Done",
		"pull_request_number=73",
		"queue_wait_seconds=120",
		"active_merge_duration_seconds=360",
		"total_merging_seconds=480",
		"head_sha=head-merge-log",
		"base_sha=base-merge-log",
	} {
		if !strings.Contains(successLogs.String(), fragment) {
			t.Fatalf("success logs %q missing fragment %q", successLogs.String(), fragment)
		}
	}
	completed := successState.Completed[successIssue.ID]
	if completed.MergeTiming.MergedAt.IsZero() || completed.MergeTiming.ActiveMergeDurationSeconds != 360 {
		t.Fatalf("Completed[%q].MergeTiming = %#v, want successful terminal durations", successIssue.ID, completed.MergeTiming)
	}
}

func TestStaleMergingQueueDispatchCandidatesFiltersUnsafePullRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pullRequest *connector.PullRequest
		closed      bool
		want        bool
	}{
		{
			name: "ready",
			pullRequest: &connector.PullRequest{
				State:          "OPEN",
				MergeableState: "clean",
				CIStatus:       "success",
			},
			want: true,
		},
		{
			name: "behind and ready for base refresh",
			pullRequest: &connector.PullRequest{
				State:          "OPEN",
				MergeableState: "behind",
				CIStatus:       "success",
			},
			want: true,
		},
		{
			name: "behind with pending required check",
			pullRequest: &connector.PullRequest{
				State:          "OPEN",
				MergeableState: "behind",
				CIStatus:       "pending",
				RequiredCheckFailures: []connector.PullRequestCheck{{
					Name:   "Portability Verify (windows-latest)",
					Status: "in_progress",
				}},
			},
		},
		{
			name: "behind with pending optional check",
			pullRequest: &connector.PullRequest{
				State:          "OPEN",
				MergeableState: "behind",
				CIStatus:       "pending",
			},
			want: true,
		},
		{
			name:        "missing pull request",
			pullRequest: nil,
		},
		{
			name:   "closed issue with merged pull request",
			closed: true,
			pullRequest: &connector.PullRequest{
				State:    "MERGED",
				CIStatus: "success",
			},
		},
		{
			name:   "closed issue with stale open pull request",
			closed: true,
			pullRequest: &connector.PullRequest{
				State:          "OPEN",
				MergeableState: "clean",
				CIStatus:       "success",
			},
		},
		{
			name: "draft pull request",
			pullRequest: &connector.PullRequest{
				State:          "OPEN",
				Draft:          true,
				MergeableState: "clean",
				CIStatus:       "success",
			},
		},
		{
			name: "conflicting pull request",
			pullRequest: &connector.PullRequest{
				State:          "OPEN",
				MergeableState: "dirty",
				CIStatus:       "success",
			},
			want: true,
		},
		{
			name: "non green ci",
			pullRequest: &connector.PullRequest{
				State:          "OPEN",
				MergeableState: "clean",
				CIStatus:       "pending",
			},
			want: true,
		},
		{
			name: "failed ci",
			pullRequest: &connector.PullRequest{
				State:          "OPEN",
				MergeableState: "clean",
				CIStatus:       "failure",
			},
		},
		{
			name: "hydration unavailable",
			pullRequest: &connector.PullRequest{
				State:                      "OPEN",
				MergeableState:             "clean",
				CIStatus:                   "success",
				HydrationUnavailableReason: "rate_limited",
			},
		},
		{
			name: "hydration degraded",
			pullRequest: &connector.PullRequest{
				State:                   "OPEN",
				MergeableState:          "clean",
				CIStatus:                "success",
				HydrationDegradedReason: connector.PullRequestHydrationReasonStaleCachedPullData,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := normalizeConfig(Config{
				MaxConcurrentAgents: 1,
				MaxConcurrentAgentsByState: map[string]int{
					"Merging": 1,
				},
				ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
				TerminalStates: []string{"Done", "Cancelled"},
			})
			state := newState(cfg)
			orch := &Orchestrator{cfg: cfg}
			issue := autoPromoteTickIssue("issue-"+strings.ReplaceAll(tt.name, " ", "-"), []string{"bug"}, tt.pullRequest)
			issue.State = "Merging"
			issue.Closed = tt.closed
			got := orch.staleMergingQueueDispatchCandidates(&state, []connector.Issue{issue}, time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC))
			if tt.want {
				if len(got) != 1 || got[0].ID != issue.ID {
					t.Fatalf("staleMergingQueueDispatchCandidates() = %#v, want %s", got, issue.ID)
				}
				return
			}
			if len(got) != 0 {
				t.Fatalf("staleMergingQueueDispatchCandidates() = %#v, want none", got)
			}
		})
	}
}

func TestStaleMergingQueueDispatchCandidatesRequiresApprovalLabel(t *testing.T) {
	t.Parallel()

	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		AutoPromote: AutoPromoteConfig{Gate: gate.Config{
			Kind:          gate.KindHumanReview,
			ApprovalLabel: "Ready to Merge",
		}},
		ActiveStates:   []string{"Merging"},
		TerminalStates: []string{"Done"},
	})
	issue := autoPromoteTickIssue("issue-approval-revoked", []string{"bug"}, &connector.PullRequest{
		Number:         1435,
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	issue.State = "Merging"
	orch := &Orchestrator{cfg: cfg}
	state := newState(cfg)

	if got := orch.staleMergingQueueDispatchCandidates(&state, []connector.Issue{issue}, time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)); len(got) != 0 {
		t.Fatalf("staleMergingQueueDispatchCandidates() = %#v, want none without approval label", got)
	}
}

func TestMergeWorkerDispatchCandidatesPreservesScheduledRetry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 24, 20, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:      []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-retrying-merge", []string{"bug"}, &connector.PullRequest{
		Number:         74,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/74",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	issue.State = "Merging"
	state := newState(cfg)
	state.Claimed[issue.ID] = Claimed{
		Issue:     cloneIssue(issue),
		ClaimedAt: now.Add(-time.Minute),
	}
	state.Retry[issue.ID] = Retry{
		Issue:   cloneIssue(issue),
		Attempt: 2,
		DueAt:   now.Add(time.Hour),
		Error:   "merge worker failed",
	}
	orch := &Orchestrator{cfg: cfg}

	got := orch.mergeWorkerDispatchCandidates(&state, []connector.Issue{issue}, now)
	if len(got) != 0 {
		t.Fatalf("mergeWorkerDispatchCandidates() = %#v, want none while retry is scheduled", got)
	}
	if claimed, ok := state.Claimed[issue.ID]; !ok {
		t.Fatalf("Claimed[%q] missing after stale Merging dispatch candidate scan", issue.ID)
	} else if claimed.Issue.ID != issue.ID {
		t.Fatalf("Claimed[%q].Issue.ID = %q, want %q", issue.ID, claimed.Issue.ID, issue.ID)
	}
	if retry, ok := state.Retry[issue.ID]; !ok {
		t.Fatalf("Retry[%q] missing after stale Merging dispatch candidate scan", issue.ID)
	} else if retry.Attempt != 2 {
		t.Fatalf("Retry[%q].Attempt = %d, want 2", issue.ID, retry.Attempt)
	}
}

func TestMergeWorkerDispatchCandidatesPrefersReadyHead(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 15, 8, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		ActiveStates:   []string{"Merging"},
		TerminalStates: []string{"Done"},
	})
	waitingCreatedAt := now.Add(-time.Hour)
	readyCreatedAt := now.Add(-time.Minute)
	waiting := nativeMergeQueueTestIssue(1321, "pending")
	waiting.ID = "issue-waiting-head"
	waiting.CreatedAt = &waitingCreatedAt
	ready := nativeMergeQueueTestIssue(1322, "success")
	ready.ID = "issue-ready-head"
	ready.Identifier = "digitaldrywood/pyroapex#1322"
	ready.PRRepository = "digitaldrywood/pyroapex"
	ready.PullRequest.URL = "https://github.test/digitaldrywood/pyroapex/pull/1322"
	ready.CreatedAt = &readyCreatedAt
	state := newState(cfg)
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:    cfg,
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}

	got := orch.mergeWorkerDispatchCandidates(&state, []connector.Issue{waiting, ready}, now)
	if len(got) != 1 || got[0].ID != ready.ID {
		t.Fatalf("mergeWorkerDispatchCandidates() = %#v, want ready issue %q", got, ready.ID)
	}
	for _, fragment := range []string{
		"merge_worker_queue_cycle",
		"queue_depth=2",
		"ready_count=1",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
}

func TestMergeWorkerDispatchCandidatesAgesWaitingHeadAheadOfReadyHead(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 9, 4, 20, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		ActiveStates:   []string{"Merging"},
		TerminalStates: []string{"Done"},
	})
	agedAt := now.Add(-3 * time.Hour)
	recentAt := now.Add(-time.Minute)
	aged := nativeMergeQueueTestIssue(1748, "pending")
	aged.ID = "issue-aged-head"
	aged.StageUpdatedAt = &agedAt
	recent := nativeMergeQueueTestIssue(1749, "success")
	recent.ID = "issue-recent-ready-head"
	recent.Identifier = "digitaldrywood/pyroapex#1749"
	recent.PRRepository = "digitaldrywood/pyroapex"
	recent.PullRequest.URL = "https://github.test/digitaldrywood/pyroapex/pull/1749"
	recent.StageUpdatedAt = &recentAt
	state := newState(cfg)
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:    cfg,
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}

	got := orch.mergeWorkerDispatchCandidates(&state, []connector.Issue{aged, recent}, now)
	if len(got) != 1 || got[0].ID != aged.ID {
		t.Fatalf("mergeWorkerDispatchCandidates() = %#v, want aged issue %q", got, aged.ID)
	}
	for _, fragment := range []string{
		"selection_position=1",
		"selection_reason=aged_head",
		"lane_age_seconds=10800",
		"fairness_age_seconds=7200",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
}

func TestPrioritizeReadyMergingIssuesFairness(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 9, 4, 20, 0, 0, time.UTC)
	threshold := 2 * time.Hour
	issue := func(id string, number int, repository string, ciStatus string, enteredAt time.Time) connector.Issue {
		value := nativeMergeQueueTestIssue(number, ciStatus)
		value.ID = id
		value.Identifier = fmt.Sprintf("%s#%d", repository, number)
		value.PRRepository = repository
		value.PullRequest.URL = fmt.Sprintf("https://github.test/%s/pull/%d", repository, number)
		value.StageUpdatedAt = &enteredAt
		return value
	}
	aged := issue("aged", 1748, "digitaldrywood/detent", "pending", now.Add(-threshold-time.Second))
	oldestAged := issue("oldest-aged", 1747, "digitaldrywood/archive", "pending", now.Add(-threshold-time.Hour))
	boundary := issue("boundary", 1749, "digitaldrywood/pyroapex", "pending", now.Add(-threshold))
	cleanFirst := issue("clean-first", 1750, "digitaldrywood/phone", "success", now.Add(-time.Hour))
	cleanSecond := issue("clean-second", 1751, "digitaldrywood/outlet", "success", now.Add(-time.Minute))
	invalidated := cloneIssue(aged)
	invalidated.ID = "invalidated"
	invalidated.Identifier = "digitaldrywood/detent#1752"
	invalidated.PullRequest.Number = 1752
	invalidated.PullRequest.MergeableState = "behind"
	invalidated.PullRequest.CIStatus = "pending"

	tests := []struct {
		name        string
		issues      []connector.Issue
		state       func() *State
		wantOrder   []string
		wantReasons map[string]string
		wantSticky  string
	}{
		{
			name:        "empty queue",
			wantOrder:   []string{},
			wantReasons: map[string]string{},
		},
		{
			name:        "all clean queue preserves order",
			issues:      []connector.Issue{cleanFirst, cleanSecond},
			wantOrder:   []string{"clean-first", "clean-second"},
			wantReasons: map[string]string{"clean-first": mergeSelectionReasonClean, "clean-second": mergeSelectionReasonClean},
		},
		{
			name:        "aged head outranks clean head",
			issues:      []connector.Issue{cleanSecond, aged},
			wantOrder:   []string{"aged", "clean-second"},
			wantReasons: map[string]string{"aged": mergeSelectionReasonAged, "clean-second": mergeSelectionReasonClean},
		},
		{
			name:        "age boundary is inclusive",
			issues:      []connector.Issue{cleanFirst, boundary},
			wantOrder:   []string{"boundary", "clean-first"},
			wantReasons: map[string]string{"boundary": mergeSelectionReasonAged, "clean-first": mergeSelectionReasonClean},
		},
		{
			name:        "continuous clean arrivals stay behind oldest aged head",
			issues:      []connector.Issue{cleanFirst, aged, cleanSecond, oldestAged},
			wantOrder:   []string{"oldest-aged", "aged", "clean-first", "clean-second"},
			wantReasons: map[string]string{"oldest-aged": mergeSelectionReasonAged, "aged": mergeSelectionReasonAged, "clean-first": mergeSelectionReasonClean, "clean-second": mergeSelectionReasonClean},
		},
		{
			name:   "invalidated aged retry remains sticky",
			issues: []connector.Issue{cleanFirst, invalidated},
			state: func() *State {
				state := State{Retry: map[string]Retry{
					invalidated.ID: {Issue: invalidated, DueAt: now.Add(time.Minute)},
				}}
				return &state
			},
			wantOrder:   []string{"invalidated", "clean-first"},
			wantReasons: map[string]string{"invalidated": mergeSelectionReasonStickyAged, "clean-first": mergeSelectionReasonClean},
			wantSticky:  "invalidated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issues := cloneIssues(tt.issues)
			var state *State
			if tt.state != nil {
				state = tt.state()
			}
			priority := prioritizeReadyMergingIssues(issues, state, now, threshold)
			gotOrder := make([]string, 0, len(issues))
			for _, issue := range issues {
				gotOrder = append(gotOrder, issue.ID)
			}
			if !reflect.DeepEqual(gotOrder, tt.wantOrder) {
				t.Fatalf("order = %#v, want %#v", gotOrder, tt.wantOrder)
			}
			if !reflect.DeepEqual(priority.reasons, tt.wantReasons) {
				t.Fatalf("reasons = %#v, want %#v", priority.reasons, tt.wantReasons)
			}
			if priority.stickyIssueID != tt.wantSticky {
				t.Fatalf("sticky issue = %q, want %q", priority.stickyIssueID, tt.wantSticky)
			}
		})
	}
}

func TestLogMergeWorkerQueueCycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 3,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		ActiveStates:   []string{"Merging"},
		TerminalStates: []string{"Done"},
	})
	readyIssue := func(id, identifier string, number int) connector.Issue {
		issue := autoPromoteTickIssue(id, []string{"bug"}, &connector.PullRequest{
			Number:         number,
			URL:            fmt.Sprintf("https://github.test/digitaldrywood/detent/pull/%d", number),
			State:          "OPEN",
			MergeableState: "clean",
			CIStatus:       "success",
			HeadSHA:        fmt.Sprintf("head-%d", number),
		})
		issue.State = autoPromoteMergingState
		issue.Identifier = identifier
		return issue
	}
	occupant := readyIssue("issue-occupant", "digitaldrywood/detent#1540", 1550)
	firstWaiting := readyIssue("issue-first-waiting", "digitaldrywood/detent#1541", 1551)
	secondWaiting := readyIssue("issue-second-waiting", "digitaldrywood/detent#1542", 1552)
	firstWaiting.StageUpdatedAt = timePointer(now.Add(-3 * time.Hour))

	tests := []struct {
		name        string
		state       State
		issues      []connector.Issue
		want        []string
		doesNotWant []string
	}{
		{
			name: "saturated lane with backlog",
			state: func() State {
				state := newState(cfg)
				state.Running[occupant.ID] = Running{
					Issue:     cloneIssue(occupant),
					StartedAt: now.Add(-5 * time.Minute),
				}
				return state
			}(),
			issues: []connector.Issue{occupant, firstWaiting, secondWaiting},
			want: []string{
				"queue_depth=3",
				"ready_count=3",
				"aged_count=1",
				"oldest_lane_age_seconds=10800",
				"fairness_age_seconds=7200",
				"lane_occupied=true",
				"lane_saturated=true",
				"lane_occupant_count=1",
				"queued_behind=2",
				"occupying_issue_id=issue-occupant",
				"occupying_issue_identifier=digitaldrywood/detent#1540",
				"occupying_issue_number=1540",
				"occupancy_seconds=300",
			},
		},
		{
			name:   "free lane with empty queue",
			state:  newState(cfg),
			issues: nil,
			want: []string{
				"queue_depth=0",
				"ready_count=0",
				"aged_count=0",
				"oldest_lane_age_seconds=0",
				"fairness_age_seconds=7200",
				"lane_occupied=false",
				"lane_saturated=false",
				"lane_occupant_count=0",
				"queued_behind=0",
			},
			doesNotWant: []string{"occupying_issue_id", "occupancy_seconds"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logs strings.Builder
			orch := &Orchestrator{
				cfg:    cfg,
				logger: slog.New(slog.NewTextHandler(&logs, nil)),
			}

			orch.logMergeWorkerQueueCycle(&tt.state, tt.issues, now)

			for _, fragment := range tt.want {
				if !strings.Contains(logs.String(), fragment) {
					t.Errorf("logs %q missing fragment %q", logs.String(), fragment)
				}
			}
			for _, fragment := range tt.doesNotWant {
				if strings.Contains(logs.String(), fragment) {
					t.Errorf("logs %q contain fragment %q", logs.String(), fragment)
				}
			}
		})
	}
}

func TestMergeWorkerHeadReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*connector.Issue)
		want   bool
	}{
		{name: "clean green current head", want: true},
		{name: "pending checks", mutate: func(issue *connector.Issue) { issue.PullRequest.CIStatus = "pending" }},
		{name: "behind green", mutate: func(issue *connector.Issue) { issue.PullRequest.MergeableState = "behind" }},
		{name: "missing required check", mutate: func(issue *connector.Issue) {
			issue.PullRequest.RequiredCheckFailures = []connector.PullRequestCheck{{Name: "Test", Status: "missing"}}
		}},
		{name: "missing head", mutate: func(issue *connector.Issue) { issue.PullRequest.HeadSHA = "" }},
		{name: "draft", mutate: func(issue *connector.Issue) { issue.PullRequest.Draft = true }},
		{name: "not merging", mutate: func(issue *connector.Issue) { issue.State = "In Progress" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := nativeMergeQueueTestIssue(1325, "success")
			if tt.mutate != nil {
				tt.mutate(&issue)
			}
			if got := mergeWorkerHeadReady(issue); got != tt.want {
				t.Fatalf("mergeWorkerHeadReady() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestDecideMergeBaseRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		mergeableState  string
		ciStatus        string
		requiredChecks  []connector.PullRequestCheck
		laneAvailable   bool
		globalAvailable bool
		want            mergeBaseRefreshDecision
	}{
		{
			name:            "behind and ready",
			mergeableState:  "behind",
			ciStatus:        "success",
			laneAvailable:   true,
			globalAvailable: true,
			want:            mergeBaseRefreshDecision{applicable: true, proceed: true},
		},
		{
			name:           "behind with pending required check",
			mergeableState: "behind",
			ciStatus:       "pending",
			requiredChecks: []connector.PullRequestCheck{{Name: "Portability Verify", Status: "in_progress"}},
			laneAvailable:  true,
			want:           mergeBaseRefreshDecision{applicable: true, reason: mergeBaseRefreshRequiredChecksPending},
		},
		{
			name:            "behind with pending optional check",
			mergeableState:  "behind",
			ciStatus:        "pending",
			laneAvailable:   true,
			globalAvailable: true,
			want:            mergeBaseRefreshDecision{applicable: true, proceed: true},
		},
		{
			name:            "behind with occupied merge lane",
			mergeableState:  "behind",
			ciStatus:        "success",
			globalAvailable: true,
			want:            mergeBaseRefreshDecision{applicable: true, reason: mergeBaseRefreshLaneUnavailable},
		},
		{
			name:           "behind without global capacity",
			mergeableState: "behind",
			ciStatus:       "success",
			laneAvailable:  true,
			want:           mergeBaseRefreshDecision{applicable: true, reason: mergeBaseRefreshGlobalUnavailable},
		},
		{
			name:            "not behind",
			mergeableState:  "clean",
			ciStatus:        "success",
			laneAvailable:   true,
			globalAvailable: true,
			want:            mergeBaseRefreshDecision{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issue := nativeMergeQueueTestIssue(1692, tt.ciStatus)
			issue.PullRequest.MergeableState = tt.mergeableState
			issue.PullRequest.RequiredCheckFailures = tt.requiredChecks
			got := decideMergeBaseRefresh(issue, tt.laneAvailable, tt.globalAvailable)
			if got != tt.want {
				t.Fatalf("decideMergeBaseRefresh() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestMergeWorkerDispatchCandidatesSelectsOneQueueHeadPerRepository(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 21, 30, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 3,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 3,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	phoneHeadCreatedAt := now.Add(-3 * time.Hour)
	phoneSiblingCreatedAt := now.Add(-2 * time.Hour)
	outletCreatedAt := now.Add(-time.Hour)
	phoneHead := autoPromoteTickIssue("issue-phone-head", []string{"bug"}, &connector.PullRequest{
		Number:         75,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/75",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	phoneHead.State = "Merging"
	phoneHead.Identifier = "digitaldrywood/creswoodcorners-phone#66"
	phoneHead.CreatedAt = &phoneHeadCreatedAt
	phoneSibling := autoPromoteTickIssue("issue-phone-sibling", []string{"bug"}, &connector.PullRequest{
		Number:         76,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/76",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	phoneSibling.State = "Merging"
	phoneSibling.Identifier = "digitaldrywood/creswoodcorners-phone#68"
	phoneSibling.CreatedAt = &phoneSiblingCreatedAt
	outlet := autoPromoteTickIssue("issue-outlet-head", []string{"bug"}, &connector.PullRequest{
		Number:         89,
		URL:            "https://github.test/digitaldrywood/creswoodcornersoutlet/pull/89",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	outlet.State = "Merging"
	outlet.Identifier = "digitaldrywood/creswoodcornersoutlet#89"
	outlet.CreatedAt = &outletCreatedAt
	state := newState(cfg)
	orch := &Orchestrator{cfg: cfg}

	got := orch.mergeWorkerDispatchCandidates(&state, []connector.Issue{phoneSibling, outlet, phoneHead}, now)
	gotIDs := make([]string, 0, len(got))
	for _, issue := range got {
		gotIDs = append(gotIDs, issue.ID)
	}
	wantIDs := []string{"issue-phone-head", "issue-outlet-head"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("mergeWorkerDispatchCandidates() ids = %#v, want %#v", gotIDs, wantIDs)
	}
}

func TestMergeWorkerDispatchCandidatesConsumesNotReadyQueueHeadRepository(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 21, 45, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 3,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 3,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	phoneHeadCreatedAt := now.Add(-3 * time.Hour)
	phoneSiblingCreatedAt := now.Add(-2 * time.Hour)
	outletCreatedAt := now.Add(-time.Hour)
	phoneHead := autoPromoteTickIssue("issue-phone-head-hydration-blocked", []string{"bug"}, &connector.PullRequest{
		Number:                     75,
		URL:                        "https://github.test/digitaldrywood/creswoodcorners-phone/pull/75",
		State:                      "OPEN",
		MergeableState:             "clean",
		CIStatus:                   "success",
		HeadSHA:                    "head-phone-blocked",
		HydrationUnavailableReason: connector.PullRequestHydrationReasonSecondaryThrottled,
	})
	phoneHead.State = "Merging"
	phoneHead.Identifier = "digitaldrywood/creswoodcorners-phone#66"
	phoneHead.CreatedAt = &phoneHeadCreatedAt
	phoneSibling := autoPromoteTickIssue("issue-phone-sibling-ready", []string{"bug"}, &connector.PullRequest{
		Number:         76,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/76",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "head-phone-ready",
	})
	phoneSibling.State = "Merging"
	phoneSibling.Identifier = "digitaldrywood/creswoodcorners-phone#68"
	phoneSibling.CreatedAt = &phoneSiblingCreatedAt
	outlet := autoPromoteTickIssue("issue-outlet-head-ready", []string{"bug"}, &connector.PullRequest{
		Number:         89,
		URL:            "https://github.test/digitaldrywood/creswoodcornersoutlet/pull/89",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "head-outlet-ready",
	})
	outlet.State = "Merging"
	outlet.Identifier = "digitaldrywood/creswoodcornersoutlet#89"
	outlet.CreatedAt = &outletCreatedAt
	state := newState(cfg)
	orch := &Orchestrator{cfg: cfg}

	got := orch.mergeWorkerDispatchCandidates(&state, []connector.Issue{phoneSibling, outlet, phoneHead}, now)
	gotIDs := make([]string, 0, len(got))
	for _, issue := range got {
		gotIDs = append(gotIDs, issue.ID)
	}
	wantIDs := []string{"issue-outlet-head-ready"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("mergeWorkerDispatchCandidates() ids = %#v, want %#v", gotIDs, wantIDs)
	}
}

func TestMergeWorkerDispatchCandidatesWaitsWhenMergingLaneFull(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 21, 0, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	running := autoPromoteTickIssue("issue-running-merge", []string{"bug"}, &connector.PullRequest{
		Number:         72,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/72",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	running.State = "Merging"
	running.Identifier = "digitaldrywood/creswoodcorners-phone#72"
	waiting := autoPromoteTickIssue("issue-waiting-merge", []string{"bug"}, &connector.PullRequest{
		Number:         75,
		URL:            "https://github.test/digitaldrywood/creswoodcornersoutlet/pull/75",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	waiting.State = "Merging"
	waiting.Identifier = "digitaldrywood/creswoodcornersoutlet#75"
	state := newState(cfg)
	state.Running[running.ID] = Running{
		Issue:     cloneIssue(running),
		StartedAt: now.Add(-time.Minute),
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:    cfg,
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}

	got := orch.mergeWorkerDispatchCandidates(&state, []connector.Issue{waiting}, now)
	if len(got) != 0 {
		t.Fatalf("mergeWorkerDispatchCandidates() = %#v, want none while Merging lane is full", got)
	}
	logText := logs.String()
	for _, fragment := range []string{
		"merge_worker_slot_wait",
		"reason=project_state_capacity_full",
		"project_state_capacity=1",
		"project_state_used=1",
		"project_state_available=0",
		"pull_request_number=75",
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("logs %q missing fragment %q", logText, fragment)
		}
	}
	if strings.Contains(logText, "merge_worker_pickup") {
		t.Fatalf("logs %q contain merge_worker_pickup, want wait telemetry without pickup", logText)
	}
}

func TestFetchTickIssuesIncludesMergingStatusHydrationWhenMergingLaneFull(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 21, 5, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	running := autoPromoteTickIssue("issue-running-merge", []string{"bug"}, &connector.PullRequest{
		Number:         72,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/72",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	running.State = "Merging"
	stale := autoPromoteTickIssue("issue-stale-merge", []string{"bug"}, &connector.PullRequest{
		Number:         75,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/75",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	stale.State = "Merging"
	tracker := &autoPromoteTickConnector{
		stateIssues:           []connector.Issue{running, stale},
		candidateIssues:       []connector.Issue{},
		candidateIssuesSet:    true,
		fetchByStatesRequests: nil,
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.Running[running.ID] = Running{
		Issue:     cloneIssue(running),
		StartedAt: now.Add(-time.Minute),
	}

	fetched, ok := orch.fetchTickIssues(context.Background(), &state, now, githubBudgetReserveDecision{})
	if !ok {
		t.Fatal("fetchTickIssues() ok = false, want true")
	}
	if !fetched.statusOK {
		t.Fatal("fetchTickIssues().statusOK = false, want true")
	}
	if len(tracker.candidateByStates) != 1 {
		t.Fatalf("FetchCandidateIssuesByStates requests = %#v, want one candidate fetch", tracker.candidateByStates)
	}
	for _, stateName := range tracker.candidateByStates[0] {
		if normalizeState(stateName) == normalizeState(autoPromoteMergingState) {
			t.Fatalf("FetchCandidateIssuesByStates states = %#v, want Merging omitted while lane is full", tracker.candidateByStates[0])
		}
	}
	if len(tracker.fetchByStatesRequests) != 1 {
		t.Fatalf("FetchIssuesByStates requests = %#v, want one observed status fetch", tracker.fetchByStatesRequests)
	}
	mergingFetched := false
	for _, stateName := range tracker.fetchByStatesRequests[0] {
		if normalizeState(stateName) == normalizeState(autoPromoteMergingState) {
			mergingFetched = true
		}
	}
	if !mergingFetched {
		t.Fatalf("FetchIssuesByStates states = %#v, want Merging included while lane is full", tracker.fetchByStatesRequests[0])
	}
	if got := len(issuesInStates(fetched.status, []string{autoPromoteMergingState})); got != 2 {
		t.Fatalf("Merging status issue count = %d, want 2", got)
	}
}

func TestTickPreservesDueMergingRetryWhenLaneFullAndCandidateFetchOmitted(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 21, 7, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 2,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		FailureRetryBaseDelay: time.Minute,
		MaxRetryBackoff:       time.Hour,
		ActiveStates:          []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:        []string{"Done", "Cancelled"},
	})
	running := autoPromoteTickIssue("issue-running-merge", []string{"bug"}, &connector.PullRequest{
		Number:         72,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/72",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	running.State = "Merging"
	retrying := autoPromoteTickIssue("issue-retrying-merge", []string{"bug"}, &connector.PullRequest{
		Number:         75,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/75",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	retrying.State = "Merging"
	tracker := &autoPromoteTickConnector{
		stateIssues:        []connector.Issue{running, retrying},
		candidateIssues:    []connector.Issue{},
		candidateIssuesSet: true,
	}
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := newState(cfg)
	state.Running[running.ID] = Running{
		Issue:     cloneIssue(running),
		StartedAt: now.Add(-time.Minute),
	}
	state.Claimed[retrying.ID] = Claimed{
		Issue:     cloneIssue(retrying),
		ClaimedAt: now.Add(-time.Minute),
	}
	state.Retry[retrying.ID] = Retry{
		Issue:   cloneIssue(retrying),
		Attempt: 2,
		DueAt:   now.Add(-time.Second),
		Error:   "run agent turn: stream turn: EOF",
	}

	orch.tick(context.Background(), &state, now)

	if len(tracker.candidateByStates) != 1 {
		t.Fatalf("FetchCandidateIssuesByStates requests = %#v, want one candidate fetch", tracker.candidateByStates)
	}
	for _, stateName := range tracker.candidateByStates[0] {
		if normalizeState(stateName) == normalizeState(autoPromoteMergingState) {
			t.Fatalf("FetchCandidateIssuesByStates states = %#v, want Merging omitted while lane is full", tracker.candidateByStates[0])
		}
	}
	if retry, ok := state.Retry[retrying.ID]; !ok {
		t.Fatalf("Retry[%q] missing while Merging lane is full", retrying.ID)
	} else if retry.Attempt != 2 || retry.Error != "run agent turn: stream turn: EOF" {
		t.Fatalf("Retry[%q] = %#v, want original retry preserved", retrying.ID, retry)
	}
	if _, ok := state.Claimed[retrying.ID]; !ok {
		t.Fatalf("Claimed[%q] missing while Merging lane is full", retrying.ID)
	}
	if _, ok := state.Running[retrying.ID]; ok {
		t.Fatalf("Running[%q] present while Merging lane is full", retrying.ID)
	}
}

func TestHandleRunResultBlocksMergeWorkerAfterRepeatedInterruptedSessions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 25, 21, 10, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:   1,
		FailureRetryBaseDelay: time.Minute,
		MaxRetryBackoff:       time.Hour,
		ActiveStates:          []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:        []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-exhausted-merge", []string{"bug"}, &connector.PullRequest{
		Number:         72,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/72",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	issue.State = "Merging"
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{TurnCount: 1,
		Issue:     cloneIssue(issue),
		Attempt:   3,
		StartedAt: now.Add(-time.Minute),
	}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Err:         errors.New("codex turn failed: status interrupted: null"),
	})

	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after exhausted merge runner failures", issue.ID)
	}
	if got := tracker.updates; !reflect.DeepEqual(got, []autoPromoteTickUpdate{{issueID: issue.ID, state: blockedStatusState}}) {
		t.Fatalf("updates = %#v, want Blocked transition", got)
	}
	blocked, ok := state.Blocked[issue.ID]
	if !ok || blocked.Issue.State != blockedStatusState {
		t.Fatalf("Blocked[%q] = %#v, want Blocked issue", issue.ID, blocked)
	}
	orch.setBlockedStatusIssue(t.Context(), &state, connector.Issue{
		ID:         issue.ID,
		Identifier: issue.Identifier,
		Title:      issue.Title,
		State:      blockedStatusState,
	}, now.Add(time.Minute))
	if got := state.Blocked[issue.ID].Reason; got != blocked.Reason {
		t.Fatalf("Blocked[%q].Reason after status refresh = %q, want preserved %q", issue.ID, got, blocked.Reason)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one exhausted retry comment", tracker.comments)
	}
	for _, fragment := range []string{mergeWorkerRetryExhaustedReason, "status interrupted: null", "pull request"} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
	if !strings.Contains(logs.String(), "merge_worker_failure") {
		t.Fatalf("logs %q missing merge_worker_failure", logs.String())
	}
}

func TestHandleRunResultRetriesMergeWorkerWhenRunCompletesWithoutTerminalState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 13, 15, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:   1,
		FailureRetryBaseDelay: time.Minute,
		MaxRetryBackoff:       time.Hour,
		ActiveStates:          []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:        []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-incomplete-merge", []string{"bug"}, &connector.PullRequest{
		Number:         75,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/75",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	issue.State = "Merging"
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:    cfg,
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:      cloneIssue(issue),
		Attempt:    1,
		StartedAt:  now.Add(-time.Minute),
		WorkerHost: "worker-a",
	}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateCompleted},
	})

	if _, ok := state.Completed[issue.ID]; ok {
		t.Fatalf("Completed[%q] present after incomplete merge worker result", issue.ID)
	}
	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after incomplete merge worker result", issue.ID)
	}
	retry, ok := state.Retry[issue.ID]
	if !ok {
		t.Fatalf("Retry[%q] missing after incomplete merge worker result", issue.ID)
	}
	if retry.Attempt != 2 {
		t.Fatalf("Retry[%q].Attempt = %d, want 2", issue.ID, retry.Attempt)
	}
	if retry.WorkerHost != "worker-a" {
		t.Fatalf("Retry[%q].WorkerHost = %q, want worker-a", issue.ID, retry.WorkerHost)
	}
	if retry.Error != "merge worker completed without reaching a terminal issue or pull request state" {
		t.Fatalf("Retry[%q].Error = %q", issue.ID, retry.Error)
	}
	if !retry.DueAt.Equal(now.Add(time.Minute * 2)) {
		t.Fatalf("Retry[%q].DueAt = %v, want %v", issue.ID, retry.DueAt, now.Add(time.Minute*2))
	}
	for _, fragment := range []string{
		"merge_worker_failure",
		"reason=terminal_state_missing",
		"completed without reaching a terminal issue or pull request state",
		"pull_request_number=75",
	} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
}

func TestHandleRunResultProgrammaticallyMergesCleanMergeWorkerWithoutTerminalState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 13, 15, 30, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:   1,
		FailureRetryBaseDelay: time.Minute,
		MaxRetryBackoff:       time.Hour,
		MergeMethod:           "merge",
		ActiveStates:          []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:        []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-clean-merge", []string{"bug"}, &connector.PullRequest{
		Number:         76,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/76",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "head-clean",
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/creswoodcorners-phone#68"
	tracker := &autoPromoteTickMergeConnector{
		autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:       cloneIssue(issue),
		Attempt:     1,
		StartedAt:   now.Add(-time.Minute),
		WorkerHost:  "worker-a",
		TurnCount:   3,
		LastEvent:   "workpad_update",
		LastMessage: "validated current-head CI and updated the Workpad, but left the PR open",
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			Output:     "validated current-head CI and updated the Workpad",
		},
	})

	if len(tracker.merges) != 1 {
		t.Fatalf("merges = %#v, want one programmatic merge", tracker.merges)
	}
	if got := tracker.merges[0]; got.repository != "digitaldrywood/creswoodcorners-phone" || got.number != 76 || got.headSHA != "head-clean" || got.method != "merge" {
		t.Fatalf("merge request = %#v, want repository digitaldrywood/creswoodcorners-phone PR 76 head-clean using merge", got)
	}
	if got := tracker.hydrations; !reflect.DeepEqual(got, []autoPromoteTickHydration{{
		issueID:    issue.ID,
		repository: "digitaldrywood/creswoodcorners-phone",
		number:     76,
	}}) {
		t.Fatalf("hydrations = %#v, want fresh PR hydration", got)
	}
	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Done"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after programmatic merge", issue.ID)
	}
	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after programmatic merge", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after programmatic merge", issue.ID)
	}
	completed, ok := state.Completed[issue.ID]
	if !ok {
		t.Fatalf("Completed[%q] missing after programmatic merge", issue.ID)
	}
	if completed.FinalState != "Done" {
		t.Fatalf("Completed[%q].FinalState = %q, want Done", issue.ID, completed.FinalState)
	}
	if completed.Issue.PullRequest == nil || completed.Issue.PullRequest.State != "MERGED" {
		t.Fatalf("Completed[%q].Issue.PullRequest = %#v, want merged PR", issue.ID, completed.Issue.PullRequest)
	}
	for _, fragment := range []string{"merge_worker_programmatic_merge", "merge_worker_success"} {
		if !strings.Contains(logs.String(), fragment) {
			t.Fatalf("logs %q missing fragment %q", logs.String(), fragment)
		}
	}
	if strings.Contains(logs.String(), "terminal_state_missing") {
		t.Fatalf("logs %q contain terminal_state_missing", logs.String())
	}
}

func TestHandleRunResultDoesNotProgrammaticallyMergeWhenFreshPullRequestNoLongerGreen(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 13, 15, 45, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:   1,
		FailureRetryBaseDelay: time.Minute,
		MaxRetryBackoff:       time.Hour,
		ActiveStates:          []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:        []string{"Done", "Cancelled"},
	})
	runningIssue := autoPromoteTickIssue("issue-stale-merge-pr", []string{"bug"}, &connector.PullRequest{
		Number:         76,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/76",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "stale-head",
	})
	runningIssue.State = "Merging"
	runningIssue.Identifier = "digitaldrywood/creswoodcorners-phone#68"
	refreshedIssue := cloneIssue(runningIssue)
	refreshedIssue.PullRequest = nil
	hydratedIssue := cloneIssue(runningIssue)
	hydratedIssue.PullRequest.CIStatus = "failure"
	hydratedIssue.PullRequest.HeadSHA = "fresh-head"
	tracker := &autoPromoteTickMergeConnector{
		autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{refreshedIssue}},
		hydratedIssues:           []connector.Issue{hydratedIssue},
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	state.Running[runningIssue.ID] = Running{
		Issue:      cloneIssue(runningIssue),
		Attempt:    1,
		StartedAt:  now.Add(-time.Minute),
		WorkerHost: "worker-a",
	}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     runningIssue.ID,
		CompletedAt: now,
		Result: runpkg.RunResult{
			FinalState: runpkg.FinalStateCompleted,
			Output:     "validated current-head CI and updated the Workpad",
		},
	})

	if len(tracker.merges) != 0 {
		t.Fatalf("merges = %#v, want none when fresh PR status is not green", tracker.merges)
	}
	if got := tracker.hydrations; !reflect.DeepEqual(got, []autoPromoteTickHydration{{
		issueID:    runningIssue.ID,
		repository: "digitaldrywood/creswoodcorners-phone",
		number:     76,
	}}) {
		t.Fatalf("hydrations = %#v, want fresh PR hydration", got)
	}
	if got := tracker.updates; len(got) != 0 {
		t.Fatalf("updates = %#v, want none when fresh PR status is not green", got)
	}
	retry, ok := state.Retry[runningIssue.ID]
	if !ok {
		t.Fatalf("Retry[%q] missing after fresh PR status prevented programmatic merge", runningIssue.ID)
	}
	if retry.Attempt != 2 {
		t.Fatalf("Retry[%q].Attempt = %d, want 2", runningIssue.ID, retry.Attempt)
	}
	if !strings.Contains(logs.String(), "reason=terminal_state_missing") {
		t.Fatalf("logs %q missing terminal_state_missing retry", logs.String())
	}
}

func TestHandleRunResultAbandonsIncompleteMergeWorkerWhileDraining(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 13, 16, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:      []string{"Done", "Cancelled"},
		Claiming: ClaimingConfig{
			Enabled:    true,
			LeaseField: "Lease",
		},
	})
	issue := autoPromoteTickIssue("issue-incomplete-merge-drain", []string{"bug"}, &connector.PullRequest{
		Number:         75,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/75",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
		HeadSHA:        "head-clean",
	})
	issue.State = "Merging"
	issue.Identifier = "digitaldrywood/creswoodcorners-phone#68"
	tracker := &autoPromoteTickMergeConnector{
		autoPromoteTickConnector: &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}},
	}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	state.Draining = true
	state.Running[issue.ID] = Running{
		Issue:      cloneIssue(issue),
		Attempt:    1,
		StartedAt:  now.Add(-time.Minute),
		WorkerHost: "worker-a",
	}
	state.Claimed[issue.ID] = Claimed{Issue: cloneIssue(issue), ClaimedAt: now.Add(-time.Minute)}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateCompleted},
	})

	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("Running[%q] present after draining incomplete merge worker result", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after draining incomplete merge worker result", issue.ID)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after draining incomplete merge worker result", issue.ID)
	}
	if len(tracker.hydrations) != 0 {
		t.Fatalf("hydrations = %#v, want none while draining", tracker.hydrations)
	}
	if len(tracker.merges) != 0 {
		t.Fatalf("merges = %#v, want none while draining", tracker.merges)
	}
	if got := tracker.setFields; !reflect.DeepEqual(got, []autoPromoteTickSetField{{
		issueID: issue.ID,
		field:   "Lease",
		value:   "",
	}}) {
		t.Fatalf("set fields = %#v, want lease release", got)
	}
	if len(tracker.comments) != 0 {
		t.Fatalf("comments = %#v, want none while draining", tracker.comments)
	}
	if len(tracker.updates) != 0 {
		t.Fatalf("updates = %#v, want none while draining", tracker.updates)
	}
	if strings.Contains(logs.String(), "terminal_state_missing") {
		t.Fatalf("logs %q contain terminal_state_missing", logs.String())
	}
}

func TestHandleRunResultCompletesMergeWorkerWhenLatestIssueIsTerminal(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 13, 18, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents: 1,
		ActiveStates:        []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:      []string{"Done", "Cancelled"},
	})
	runningIssue := autoPromoteTickIssue("issue-terminal-merge", []string{"bug"}, &connector.PullRequest{
		Number:         75,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/75",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	runningIssue.State = "Merging"
	terminalIssue := cloneIssue(runningIssue)
	terminalIssue.Closed = true
	terminalIssue.ClosedReason = "completed"
	terminalIssue.PullRequest.State = "MERGED"
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{terminalIssue}}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	state.Running[runningIssue.ID] = Running{
		Issue:     cloneIssue(runningIssue),
		StartedAt: now.Add(-time.Minute),
	}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     runningIssue.ID,
		CompletedAt: now,
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateCompleted},
	})

	if _, ok := state.Running[runningIssue.ID]; ok {
		t.Fatalf("Running[%q] present after terminal merge worker result", runningIssue.ID)
	}
	if _, ok := state.Retry[runningIssue.ID]; ok {
		t.Fatalf("Retry[%q] present after terminal merge worker result", runningIssue.ID)
	}
	completed, ok := state.Completed[runningIssue.ID]
	if !ok {
		t.Fatalf("Completed[%q] missing after terminal merge worker result", runningIssue.ID)
	}
	if completed.FinalState != "Done" {
		t.Fatalf("Completed[%q].FinalState = %q, want Done", runningIssue.ID, completed.FinalState)
	}
	if got := tracker.updates; !reflect.DeepEqual(got, []autoPromoteTickUpdate{{issueID: runningIssue.ID, state: "Done"}}) {
		t.Fatalf("updates = %#v, want Done reconciliation", got)
	}
	if !strings.Contains(logs.String(), "merge_worker_success") {
		t.Fatalf("logs %q missing merge_worker_success", logs.String())
	}
	if strings.Contains(logs.String(), "terminal_state_missing") {
		t.Fatalf("logs %q contain terminal_state_missing", logs.String())
	}
}

func TestHandleRunResultBlocksMergeWorkerAfterRepeatedIncompleteResults(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 26, 13, 20, 0, 0, time.UTC)
	cfg := normalizeConfig(Config{
		MaxConcurrentAgents:   1,
		FailureRetryBaseDelay: time.Minute,
		MaxRetryBackoff:       time.Hour,
		ActiveStates:          []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates:        []string{"Done", "Cancelled"},
	})
	issue := autoPromoteTickIssue("issue-incomplete-merge-exhausted", []string{"bug"}, &connector.PullRequest{
		Number:         76,
		URL:            "https://github.test/digitaldrywood/creswoodcorners-phone/pull/76",
		State:          "OPEN",
		MergeableState: "clean",
		CIStatus:       "success",
	})
	issue.State = "Merging"
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	var logs strings.Builder
	orch := &Orchestrator{
		cfg:       cfg,
		connector: tracker,
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	}
	state := newState(cfg)
	state.Running[issue.ID] = Running{
		Issue:     cloneIssue(issue),
		Attempt:   3,
		StartedAt: now.Add(-time.Minute),
	}

	orch.handleRunResult(context.Background(), &state, runpkg.Completion{
		IssueID:     issue.ID,
		CompletedAt: now,
		Result:      runpkg.RunResult{FinalState: runpkg.FinalStateCompleted},
	})

	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after repeated incomplete merge results", issue.ID)
	}
	if got := tracker.updates; !reflect.DeepEqual(got, []autoPromoteTickUpdate{{issueID: issue.ID, state: blockedStatusState}}) {
		t.Fatalf("updates = %#v, want Blocked transition", got)
	}
	if len(tracker.comments) != 1 {
		t.Fatalf("comments = %#v, want one exhausted retry comment", tracker.comments)
	}
	for _, fragment := range []string{mergeWorkerRetryExhaustedReason, "terminal issue or pull request state", "pull request"} {
		if !strings.Contains(tracker.comments[0].body, fragment) {
			t.Fatalf("comment %q missing fragment %q", tracker.comments[0].body, fragment)
		}
	}
	if !strings.Contains(logs.String(), "reason=terminal_state_missing") {
		t.Fatalf("logs %q missing terminal_state_missing", logs.String())
	}
}

func TestTickAutoPromoteMergingIssueDispatchesAndClearsStaleMemory(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 23, 15, 0, 0, 0, time.UTC)
	oldReview := now.Add(-20 * time.Minute)
	issue := autoPromoteTickIssue("issue-promoted-merge", []string{"bug"}, &connector.PullRequest{
		Number:                 639,
		URL:                    "https://github.test/digitaldrywood/detent/pull/639",
		State:                  "OPEN",
		CIStatus:               "success",
		CodexReviewState:       "COMMENTED",
		CodexReviewSubmittedAt: &oldReview,
	})
	cfg := normalizeConfig(Config{
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 3,
		MaxConcurrentAgentsByState: map[string]int{
			"Merging": 1,
		},
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
	tracker := &autoPromoteTickConnector{stateIssues: []connector.Issue{issue}}
	runner := newWorkerHostRunner()
	orch := &Orchestrator{
		cfg:        cfg,
		connector:  tracker,
		supervisor: newTestSupervisor(t, runner, cfg),
		runResults: make(chan runpkg.Completion, 1),
	}
	state := newState(cfg)
	state.Completed[issue.ID] = Completed{
		Issue:       cloneIssue(issue),
		CompletedAt: now.Add(-5 * time.Minute),
		FinalState:  "Human Review",
	}
	state.Claimed[issue.ID] = Claimed{
		Issue:     cloneIssue(issue),
		ClaimedAt: now.Add(-5 * time.Minute),
	}
	state.Retry[issue.ID] = Retry{
		Issue:   cloneIssue(issue),
		Attempt: 1,
		DueAt:   now.Add(time.Hour),
	}
	runningMerging := dispatchTestIssue("issue-running-merge", "Merging")
	state.Running[runningMerging.ID] = Running{Issue: runningMerging}

	orch.tick(context.Background(), &state, now)

	if got, want := tracker.updates, []autoPromoteTickUpdate{{issueID: issue.ID, state: "Merging"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updates = %#v, want %#v", got, want)
	}
	select {
	case request := <-runner.started:
		t.Fatalf("unexpected dispatch while Merging limit is full = %#v", request)
	default:
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("Claimed[%q] present after Merging auto-promote", issue.ID)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatalf("Retry[%q] present after Merging auto-promote", issue.ID)
	}

	orch.tick(context.Background(), &state, now.Add(time.Minute))

	select {
	case request := <-runner.started:
		t.Fatalf("unexpected dispatch while Merging limit is full on candidate refresh = %#v", request)
	default:
	}

	delete(state.Running, runningMerging.ID)
	orch.tick(context.Background(), &state, now.Add(2*time.Minute))

	request := receiveWorkerHostRunRequest(t, runner.started)
	if request.Issue.ID != issue.ID {
		t.Fatalf("RunRequest.Issue.ID = %q, want %q", request.Issue.ID, issue.ID)
	}
	if request.Issue.State != "Merging" {
		t.Fatalf("RunRequest.Issue.State = %q, want Merging", request.Issue.State)
	}
	if running := state.Running[issue.ID]; running.cancel != nil {
		running.cancel()
	}
	if _, ok := state.Completed[issue.ID]; ok {
		t.Fatalf("Completed[%q] present after Merging dispatch", issue.ID)
	}
}

func autoPromoteTickIssue(id string, labels []string, pullRequest *connector.PullRequest) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = id
	issue.Identifier = "digitaldrywood/detent#42"
	issue.Title = "Auto promote test"
	issue.State = "Human Review"
	issue.Labels = append([]string(nil), labels...)
	issue.PullRequest = pullRequest
	return issue
}

func autoPromoteTickStatesEqual(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]struct{}, len(got))
	for _, state := range got {
		seen[strings.ToLower(strings.TrimSpace(state))] = struct{}{}
	}
	for _, state := range want {
		if _, ok := seen[strings.ToLower(strings.TrimSpace(state))]; !ok {
			return false
		}
	}
	return true
}

type autoPromoteTickUpdate struct {
	issueID string
	state   string
}

type autoPromoteTickComment struct {
	issueID string
	body    string
}

type autoPromoteTickSetField struct {
	issueID string
	field   string
	value   string
}

type autoPromoteTickMerge struct {
	repository string
	number     int
	headSHA    string
	method     string
}

type autoPromoteTickRerun struct {
	issueID string
	checks  []connector.PullRequestCheck
}

type autoPromoteTickHydration struct {
	issueID    string
	repository string
	number     int
}

type autoPromoteTickRelabel struct {
	repository string
	number     int
	label      string
	stagger    time.Duration
}

func autoPromoteReworkEventMetadata(prNumber int, headSHA string, failedChecks ...string) string {
	issue := connector.Issue{
		PullRequest: &connector.PullRequest{
			Number:  prNumber,
			HeadSHA: headSHA,
		},
	}
	for _, check := range failedChecks {
		issue.PullRequest.RequiredCheckFailures = append(issue.PullRequest.RequiredCheckFailures, connector.PullRequestCheck{
			Name:       check,
			Status:     "completed",
			Conclusion: "failure",
		})
	}
	return workflowLaneMetadataJSON(issue, workflowLaneMetadata{})
}

type autoPromoteWorkflowMetricsRecorder struct {
	mu     sync.Mutex
	events []store.WorkflowPhaseEvent
}

func (r *autoPromoteWorkflowMetricsRecorder) RecordWorkflowPhaseEvent(_ context.Context, event store.WorkflowPhaseEvent) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if event.ID <= 0 {
		event.ID = int64(len(r.events) + 1)
	}
	r.events = append(r.events, event)
	return event.ID, nil
}

func (r *autoPromoteWorkflowMetricsRecorder) UpdateWorkflowPhaseEventMetadata(_ context.Context, eventID int64, metadataJSON string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for index := range r.events {
		if r.events[index].ID == eventID {
			r.events[index].MetadataJSON = metadataJSON
			return nil
		}
	}
	return store.ErrNotFound
}

func (r *autoPromoteWorkflowMetricsRecorder) IssueWorkflowTimeline(_ context.Context, identity store.IssueIdentity) (store.WorkflowTimeline, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	events := make([]store.WorkflowPhaseEvent, 0, len(r.events))
	for _, event := range r.events {
		if event.IssueID != "" && event.IssueID == identity.IssueID {
			events = append(events, event)
			continue
		}
		if event.Identifier != "" && event.Identifier == identity.Identifier {
			events = append(events, event)
			continue
		}
		if event.IssueURL != "" && event.IssueURL == identity.IssueURL {
			events = append(events, event)
		}
	}
	return store.WorkflowTimeline{Events: events}, nil
}

func (r *autoPromoteWorkflowMetricsRecorder) snapshot() []store.WorkflowPhaseEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]store.WorkflowPhaseEvent(nil), r.events...)
}

type autoPromoteTickConnector struct {
	stateIssues            []connector.Issue
	candidateIssues        []connector.Issue
	candidateIssuesSet     bool
	candidateByStates      [][]string
	fetchByStatesRequests  [][]string
	fetchComments          []string
	fetchIdentifiers       [][]string
	issueComments          map[string][]connector.IssueComment
	resolvedIssues         []connector.Issue
	updates                []autoPromoteTickUpdate
	comments               []autoPromoteTickComment
	prComments             []autoPromoteTickComment
	setFields              []autoPromoteTickSetField
	reruns                 []autoPromoteTickRerun
	updateErr              error
	reviewThreadHydrations []string
}

type autoPromoteTickMergeConnector struct {
	*autoPromoteTickConnector
	merges         []autoPromoteTickMerge
	hydrations     []autoPromoteTickHydration
	relabels       []autoPromoteTickRelabel
	relabelStarted chan autoPromoteTickRelabel
	relabelRelease <-chan struct{}
	hydratedIssues []connector.Issue
	err            error
	hydrateErr     error
}

func (c *autoPromoteTickConnector) Name() string {
	return "auto-promote-tick"
}

func (c *autoPromoteTickConnector) FetchCandidateIssues(ctx context.Context) ([]connector.Issue, error) {
	return c.FetchCandidateIssuesByStates(ctx, []string{"Todo", "In Progress", "Rework", "Merging"})
}

func (c *autoPromoteTickConnector) FetchCandidateIssuesByStates(_ context.Context, states []string) ([]connector.Issue, error) {
	c.candidateByStates = append(c.candidateByStates, append([]string(nil), states...))
	if c.candidateIssuesSet {
		return issuesInStates(c.candidateIssues, states), nil
	}
	return issuesInStates(c.stateIssues, states), nil
}

func (c *autoPromoteTickConnector) FetchIssuesByStates(_ context.Context, states []string) ([]connector.Issue, error) {
	c.fetchByStatesRequests = append(c.fetchByStatesRequests, append([]string(nil), states...))
	wanted := make(map[string]struct{}, len(states))
	for _, state := range states {
		wanted[strings.ToLower(strings.TrimSpace(state))] = struct{}{}
	}
	issues := make([]connector.Issue, 0, len(c.stateIssues))
	for _, issue := range c.stateIssues {
		if _, ok := wanted[strings.ToLower(strings.TrimSpace(issue.State))]; ok {
			issues = append(issues, cloneIssue(issue))
		}
	}
	return issues, nil
}

func (c *autoPromoteTickConnector) FetchIssueComments(_ context.Context, issue connector.Issue) ([]connector.IssueComment, error) {
	c.fetchComments = append(c.fetchComments, strings.TrimSpace(issue.ID))
	return cloneIssueComments(c.issueComments[strings.TrimSpace(issue.ID)]), nil
}

func (c *autoPromoteTickConnector) FetchIssueStatesByIDs(_ context.Context, ids []string) ([]connector.Issue, error) {
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	issues := make([]connector.Issue, 0, len(c.stateIssues))
	for _, issue := range c.stateIssues {
		if _, ok := wanted[issue.ID]; ok {
			issues = append(issues, cloneIssue(issue))
		}
	}
	return issues, nil
}

func (c *autoPromoteTickConnector) FetchIssueStatesByIdentifiers(_ context.Context, identifiers []string) ([]connector.Issue, error) {
	c.fetchIdentifiers = append(c.fetchIdentifiers, append([]string(nil), identifiers...))
	wanted := make(map[string]struct{}, len(identifiers))
	for _, identifier := range identifiers {
		wanted[strings.ToLower(strings.TrimSpace(identifier))] = struct{}{}
	}
	issues := make([]connector.Issue, 0, len(c.resolvedIssues))
	for _, issue := range c.resolvedIssues {
		if _, ok := wanted[strings.ToLower(strings.TrimSpace(issue.Identifier))]; ok {
			issues = append(issues, cloneIssue(issue))
		}
	}
	return issues, nil
}

func (c *autoPromoteTickConnector) HydratePullRequestReviewThreads(_ context.Context, issue connector.Issue) (connector.Issue, error) {
	c.reviewThreadHydrations = append(c.reviewThreadHydrations, strings.TrimSpace(issue.ID))
	return cloneIssue(issue), nil
}

func (c *autoPromoteTickConnector) CreateComment(_ context.Context, issueID string, body string) error {
	c.comments = append(c.comments, autoPromoteTickComment{issueID: issueID, body: body})
	return nil
}

func (c *autoPromoteTickConnector) CreatePullRequestComment(_ context.Context, repository string, number int, body string) error {
	c.prComments = append(c.prComments, autoPromoteTickComment{issueID: repository, body: body})
	return nil
}

func (c *autoPromoteTickConnector) RerunPullRequestChecks(_ context.Context, issue connector.Issue, checks []connector.PullRequestCheck) error {
	c.reruns = append(c.reruns, autoPromoteTickRerun{
		issueID: issue.ID,
		checks:  append([]connector.PullRequestCheck(nil), checks...),
	})
	return nil
}

func (c *autoPromoteTickMergeConnector) HydratePullRequest(_ context.Context, issue connector.Issue) (connector.Issue, error) {
	c.hydrations = append(c.hydrations, autoPromoteTickHydration{
		issueID:    issue.ID,
		repository: pullRequestRepository(issue),
		number:     pullRequestNumber(issue),
	})
	if c.hydrateErr != nil {
		return cloneIssue(issue), c.hydrateErr
	}
	issues := c.hydratedIssues
	if len(issues) == 0 && c.autoPromoteTickConnector != nil {
		issues = c.stateIssues
	}
	for _, candidate := range issues {
		if strings.TrimSpace(candidate.ID) != "" && strings.TrimSpace(candidate.ID) == strings.TrimSpace(issue.ID) {
			return cloneIssue(candidate), nil
		}
		if strings.TrimSpace(candidate.Identifier) != "" && strings.EqualFold(strings.TrimSpace(candidate.Identifier), strings.TrimSpace(issue.Identifier)) {
			return cloneIssue(candidate), nil
		}
		if pullRequestNumber(candidate) > 0 &&
			pullRequestNumber(candidate) == pullRequestNumber(issue) &&
			strings.EqualFold(pullRequestRepository(candidate), pullRequestRepository(issue)) {
			return cloneIssue(candidate), nil
		}
	}
	return cloneIssue(issue), nil
}

func (c *autoPromoteTickMergeConnector) ReapplyPullRequestLabel(ctx context.Context, repository string, number int, label string, stagger time.Duration) error {
	relabel := autoPromoteTickRelabel{
		repository: repository,
		number:     number,
		label:      label,
		stagger:    stagger,
	}
	c.relabels = append(c.relabels, relabel)
	if c.relabelStarted != nil {
		c.relabelStarted <- relabel
	}
	if c.relabelRelease != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.relabelRelease:
		}
	}
	return nil
}

func (c *autoPromoteTickMergeConnector) MergePullRequest(_ context.Context, repository string, number int, headSHA string, method string) error {
	c.merges = append(c.merges, autoPromoteTickMerge{repository: repository, number: number, headSHA: headSHA, method: method})
	if c.err != nil {
		return c.err
	}
	for index := range c.stateIssues {
		issue := &c.stateIssues[index]
		if pullRequestNumber(*issue) != number || !strings.EqualFold(pullRequestRepository(*issue), repository) || issue.PullRequest == nil {
			continue
		}
		issue.PullRequest.State = "MERGED"
		now := time.Date(2026, 6, 26, 13, 15, 31, 0, time.UTC)
		issue.PullRequest.ActivityAt = &now
		issue.UpdatedAt = &now
		return nil
	}
	return nil
}

type autoPromoteTickValidator struct {
	mu       sync.Mutex
	result   gate.ValidatorResult
	requests []ValidatorRequest
	err      error
}

func (v *autoPromoteTickValidator) Validate(_ context.Context, req ValidatorRequest) (gate.ValidatorResult, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.requests = append(v.requests, req)
	return v.result, v.err
}

type blockingAutoPromoteValidatorRunner struct {
	releaseOnce sync.Once
	started     chan ValidatorRequest
	runStarted  chan RunRequest
	canceled    chan struct{}
	release     chan struct{}
	done        chan struct{}
}

func newBlockingAutoPromoteValidatorRunner() *blockingAutoPromoteValidatorRunner {
	return &blockingAutoPromoteValidatorRunner{
		started:    make(chan ValidatorRequest, 1),
		runStarted: make(chan RunRequest, 1),
		canceled:   make(chan struct{}),
		release:    make(chan struct{}),
		done:       make(chan struct{}),
	}
}

func (r *blockingAutoPromoteValidatorRunner) Run(ctx context.Context, req RunRequest) (RunResult, error) {
	r.runStarted <- req
	<-ctx.Done()
	return RunResult{FinalState: runpkg.FinalStateFailed}, ctx.Err()
}

func (r *blockingAutoPromoteValidatorRunner) Validate(ctx context.Context, req ValidatorRequest) (gate.ValidatorResult, error) {
	r.started <- req
	defer close(r.done)

	select {
	case <-r.release:
		return gate.ValidatorResult{Submitted: true, Verdict: gate.ValidatorVerdictPass, Score: 1}, nil
	case <-ctx.Done():
		close(r.canceled)
		<-r.release
		return gate.ValidatorResult{}, ctx.Err()
	}
}

func (r *blockingAutoPromoteValidatorRunner) Release() {
	r.releaseOnce.Do(func() {
		close(r.release)
	})
}

func (v *autoPromoteTickValidator) Requests() []ValidatorRequest {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]ValidatorRequest(nil), v.requests...)
}

func waitForValidatorRequests(t *testing.T, validator *autoPromoteTickValidator, count int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(validator.Requests()) >= count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("validator requests = %d, want at least %d", len(validator.Requests()), count)
}

func waitForValidatorResult(t *testing.T, orch *Orchestrator, issue connector.Issue) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, _, ok := orch.validatorStageResult(context.Background(), issue); ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("validator result was not recorded")
}

func waitForPersistedValidatorVerdict(t *testing.T, memo store.ValidatorMemoStore, key store.ValidatorVerdictKey) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := memo.ValidatorVerdict(context.Background(), key); err == nil {
			return
		} else if !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("ValidatorVerdict() error = %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("validator verdict was not persisted for %#v", key)
}

func waitForPersistedValidatorFailure(t *testing.T, memo store.ValidatorMemoStore, key store.ValidatorVerdictKey, attempt int) store.ValidatorVerdict {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		verdict, err := memo.ValidatorVerdict(t.Context(), key)
		if err == nil && verdict.FailureAttempts >= attempt {
			return verdict
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("ValidatorVerdict() error = %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("persisted validator failure attempt %d was not recorded", attempt)
	return store.ValidatorVerdict{}
}

func waitForValidatorFailure(t *testing.T, orch *Orchestrator, issue connector.Issue, attempt int) validatorStageFailure {
	t.Helper()

	identity := validatorStageIdentityForIssue(issue)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		orch.validatorMu.Lock()
		failure, ok := orch.validatorFailures[identity.Key]
		orch.validatorMu.Unlock()
		if ok && failure.Attempt >= attempt {
			return failure
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("validator failure attempt %d was not recorded", attempt)
	return validatorStageFailure{}
}

func openValidatorMemoStore(t *testing.T) store.Store {
	t.Helper()

	backend, err := store.Open(context.Background(), store.Config{
		Backend: store.BackendSQLite,
		Path:    filepath.Join(t.TempDir(), "detent.db"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return backend
}

func autoPromoteValidatorTestConfig() Config {
	return normalizeConfig(Config{
		Project:             scheduler.ProjectCandidate{ID: "detent", Weight: 1},
		PollInterval:        time.Minute,
		MaxConcurrentAgents: 1,
		AutoPromote: AutoPromoteConfig{
			Enabled:       true,
			QuietDuration: 10 * time.Minute,
			Gate: gate.Config{
				Kind: gate.KindCommand,
				Validator: gate.ValidatorConfig{
					Enabled:  true,
					MinScore: 0.8,
					BlockOn:  []string{"p1"},
				},
			},
		},
		ActiveStates:   []string{"Todo", "In Progress", "Rework", "Merging"},
		TerminalStates: []string{"Done", "Cancelled"},
	})
}

type autoPromoteTickClock struct {
	mu  sync.Mutex
	now time.Time
}

func newAutoPromoteTickClock(now time.Time) *autoPromoteTickClock {
	return &autoPromoteTickClock{now: now}
}

func (c *autoPromoteTickClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *autoPromoteTickClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

func waitForGlobalDispatchSlot(t *testing.T, globalGate scheduler.ProjectDispatchGate, projectID string) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		slot, ok, err := globalGate.TryAcquire(t.Context(), scheduler.ProjectCandidate{ID: projectID, Weight: 1}, scheduler.SlotRequest{State: "Todo"}, time.Now())
		if err != nil {
			t.Fatalf("TryAcquire() error = %v", err)
		}
		if ok {
			if err := globalGate.Release(slot); err != nil {
				t.Fatalf("Release() error = %v", err)
			}
			return
		}

		select {
		case <-deadline:
			t.Fatal("global dispatch slot was not released before validator drain completed")
		case <-ticker.C:
		}
	}
}

func (c *autoPromoteTickConnector) UpdateIssueState(_ context.Context, issueID string, state string) error {
	c.updates = append(c.updates, autoPromoteTickUpdate{issueID: issueID, state: state})
	if c.updateErr != nil {
		return c.updateErr
	}
	for index := range c.stateIssues {
		if c.stateIssues[index].ID == issueID {
			c.stateIssues[index].State = state
		}
	}
	return nil
}

func (c *autoPromoteTickConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *autoPromoteTickConnector) SetField(_ context.Context, issueID string, field string, value string) error {
	c.setFields = append(c.setFields, autoPromoteTickSetField{issueID: issueID, field: field, value: value})
	for index := range c.stateIssues {
		if c.stateIssues[index].ID != issueID {
			continue
		}
		if c.stateIssues[index].Fields == nil {
			c.stateIssues[index].Fields = map[string]string{}
		}
		c.stateIssues[index].Fields[field] = value
	}
	return nil
}
