package orchestrator

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/scheduler"
	"github.com/digitaldrywood/detent/internal/store"
)

func TestHandleWorkAttemptRecoveryAbandonsActiveAttemptAndAudits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	runtimeStore := openWorkAttemptRecoveryStore(t, ctx)
	now := time.Date(2026, 7, 9, 14, 0, 0, 0, time.UTC)
	issue := recoveryTestIssue()
	attemptID := startRecoveryWorkAttempt(t, ctx, runtimeStore, issue, store.WorkAttemptStatusActive, "", now.Add(-10*time.Minute))
	tracker := &recoveryTestConnector{}
	orch := newWorkAttemptRecoveryOrchestratorWithConnector(t, runtimeStore, nil, tracker)
	state := newState(orch.cfg)
	state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: attemptID}
	state.Claimed[issue.ID] = Claimed{Issue: issue, ClaimedAt: now.Add(-10 * time.Minute)}

	response, err := orch.handleWorkAttemptRecovery(ctx, &state, WorkAttemptRecoveryRequest{
		ProjectID: "detent",
		AttemptID: attemptID,
		Action:    WorkAttemptRecoveryAbandon,
		Confirm:   true,
		Reason:    "worker host is known dead",
		Operator:  "ops",
	}, now)
	if err != nil {
		t.Fatalf("handleWorkAttemptRecovery() error = %v", err)
	}
	if response.Status != "succeeded" || response.AuditEventID <= 0 {
		t.Fatalf("response = %#v, want succeeded response with audit id", response)
	}
	if _, ok := state.Running[issue.ID]; ok {
		t.Fatalf("state.Running still contains %q after abandon", issue.ID)
	}
	if _, ok := state.Claimed[issue.ID]; ok {
		t.Fatalf("state.Claimed still contains %q after abandon", issue.ID)
	}
	if tracker.fieldIssueID != issue.ID || tracker.fieldName != "Detent Lease" || tracker.fieldValue != "" {
		t.Fatalf("SetField call = %q %q %q, want lease release", tracker.fieldIssueID, tracker.fieldName, tracker.fieldValue)
	}
	receipt, err := runtimeStore.WorkAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("WorkAttempt() error = %v", err)
	}
	if receipt.TerminalState != store.WorkAttemptTerminalAbandoned || receipt.ErrorClass != "operator_abandoned" {
		t.Fatalf("receipt = %#v, want operator abandoned terminal receipt", receipt)
	}
	event := recoveryTimelineEvent(t, ctx, runtimeStore, issue.ID, WorkAttemptRecoveryAbandon)
	if event.Status != "succeeded" || event.PhaseType != store.WorkflowPhaseTypeRecovery {
		t.Fatalf("audit event = %#v, want succeeded recovery event", event)
	}
	if !strings.Contains(event.MetadataJSON, `"operator":"ops"`) {
		t.Fatalf("audit metadata = %s, want operator recorded", event.MetadataJSON)
	}
}

func TestHandleWorkAttemptRecoveryRejectsUnsupportedStateAndAudits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	runtimeStore := openWorkAttemptRecoveryStore(t, ctx)
	now := time.Date(2026, 7, 9, 14, 0, 0, 0, time.UTC)
	issue := recoveryTestIssue()
	attemptID := startRecoveryWorkAttempt(t, ctx, runtimeStore, issue, store.WorkAttemptStatusTerminal, store.WorkAttemptTerminalSuccess, now.Add(-10*time.Minute))
	orch := newWorkAttemptRecoveryOrchestrator(t, runtimeStore, nil)
	state := newState(orch.cfg)

	_, err := orch.handleWorkAttemptRecovery(ctx, &state, WorkAttemptRecoveryRequest{
		ProjectID: "detent",
		AttemptID: attemptID,
		Action:    WorkAttemptRecoveryRetryFresh,
	}, now)
	var recoveryErr *WorkAttemptRecoveryError
	if !errors.As(err, &recoveryErr) {
		t.Fatalf("handleWorkAttemptRecovery() error = %v, want WorkAttemptRecoveryError", err)
	}
	if recoveryErr.Code != WorkAttemptRecoveryUnsupportedState {
		t.Fatalf("recovery error code = %q, want %q", recoveryErr.Code, WorkAttemptRecoveryUnsupportedState)
	}
	event := recoveryTimelineEvent(t, ctx, runtimeStore, issue.ID, WorkAttemptRecoveryRetryFresh)
	if event.Status != "rejected" {
		t.Fatalf("audit event status = %q, want rejected", event.Status)
	}
}

func TestHandleWorkAttemptRecoveryQueuesResumeRetryWhenEligible(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	runtimeStore := openWorkAttemptRecoveryStore(t, ctx)
	now := time.Date(2026, 7, 9, 14, 0, 0, 0, time.UTC)
	issue := recoveryTestIssue()
	attemptID := startRecoveryWorkAttempt(t, ctx, runtimeStore, issue, store.WorkAttemptStatusTerminal, store.WorkAttemptTerminalFailure, now.Add(-10*time.Minute))
	sessionID, err := runtimeStore.StartSession(ctx, store.SessionStart{
		ProjectID:        "detent",
		IssueID:          issue.ID,
		Identifier:       issue.Identifier,
		IssueURL:         issue.URL,
		StartedAt:        now.Add(-8 * time.Minute),
		Model:            "gpt-5-codex",
		RequestedModel:   "gpt-5-codex",
		AgentBackendID:   "codex",
		AgentBackendKind: "codex",
		AgentRole:        "code",
	})
	if err != nil {
		t.Fatalf("StartSession() error = %v", err)
	}
	if err := runtimeStore.FinishSession(ctx, sessionID, store.SessionFinish{
		CompletedAt:       now.Add(-7 * time.Minute),
		FinalState:        "completed",
		Model:             "gpt-5-codex",
		ProviderThreadID:  "thread-979",
		ProviderSessionID: "session-979",
	}); err != nil {
		t.Fatalf("FinishSession() error = %v", err)
	}
	orch := newWorkAttemptRecoveryOrchestrator(t, runtimeStore, nil)
	state := newState(orch.cfg)

	response, err := orch.handleWorkAttemptRecovery(ctx, &state, WorkAttemptRecoveryRequest{
		ProjectID: "detent",
		AttemptID: attemptID,
		Action:    WorkAttemptRecoveryRetryResume,
		Reason:    "resume from completed session",
	}, now)
	if err != nil {
		t.Fatalf("handleWorkAttemptRecovery() error = %v", err)
	}
	if !response.ResumeEligible || response.ResumeState == nil || response.ResumeState.DetentSessionID != sessionID {
		t.Fatalf("response resume state = %#v, want session %d eligible", response.ResumeState, sessionID)
	}
	retry, ok := state.Retry[issue.ID]
	if !ok {
		t.Fatalf("state.Retry missing %q", issue.ID)
	}
	if retry.Attempt != 2 || !retry.DueAt.Equal(now) {
		t.Fatalf("retry = %#v, want attempt 2 due now", retry)
	}
	if retry.RetryMode != runpkg.RetryModeResume {
		t.Fatalf("retry mode = %q, want %q", retry.RetryMode, runpkg.RetryModeResume)
	}
	if retry.ResumeState.DetentSessionID != sessionID || retry.ResumeState.ProviderSessionID != "session-979" {
		t.Fatalf("retry resume state = %#v, want selected session", retry.ResumeState)
	}
	if _, ok := state.BudgetRefusals[issue.ID]; ok {
		t.Fatalf("state.BudgetRefusals still contains %q after explicit operator retry", issue.ID)
	}
	event := recoveryTimelineEvent(t, ctx, runtimeStore, issue.ID, WorkAttemptRecoveryRetryResume)
	if event.Status != "queued" || !strings.Contains(event.MetadataJSON, `"resume_eligible":true`) {
		t.Fatalf("audit event = %#v, want succeeded resume audit", event)
	}
}

func openWorkAttemptRecoveryStore(t *testing.T, ctx context.Context) store.Store {
	t.Helper()

	runtimeStore, err := store.Open(ctx, store.Config{
		Backend: store.BackendSQLite,
		Path:    filepath.Join(t.TempDir(), "detent.db"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtimeStore.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return runtimeStore
}

func newWorkAttemptRecoveryOrchestrator(t *testing.T, runtimeStore store.Store, reaper WorkspaceReaper) *Orchestrator {
	t.Helper()

	return newWorkAttemptRecoveryOrchestratorWithConnector(t, runtimeStore, reaper, &recoveryTestConnector{})
}

func newWorkAttemptRecoveryOrchestratorWithConnector(
	t *testing.T,
	runtimeStore store.Store,
	reaper WorkspaceReaper,
	tracker connector.Connector,
) *Orchestrator {
	t.Helper()

	orch, err := New(Config{
		PollInterval:                  time.Hour,
		MaxConcurrentAgents:           1,
		Project:                       scheduler.ProjectCandidate{ID: "detent"},
		ActiveStates:                  []string{"Todo", "In Progress"},
		TerminalStates:                []string{"Done", "Cancelled"},
		ContinuationRetryDelay:        time.Second,
		FailureRetryBaseDelay:         time.Second,
		MaxRetryBackoff:               time.Minute,
		WorkspaceCleanupSweepInterval: time.Hour,
		Claiming: ClaimingConfig{
			Enabled:    true,
			LeaseField: "Detent Lease",
		},
	}, Dependencies{
		Connector:       tracker,
		WorkAttempts:    runtimeStore,
		WorkflowMetrics: runtimeStore,
		AgentResume:     runtimeStore,
		WorkspaceReaper: reaper,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return orch
}

func startRecoveryWorkAttempt(
	t *testing.T,
	ctx context.Context,
	runtimeStore store.Store,
	issue connector.Issue,
	status store.WorkAttemptStatus,
	terminalState store.WorkAttemptTerminalState,
	startedAt time.Time,
) int64 {
	t.Helper()

	attemptID, err := runtimeStore.StartWorkAttempt(ctx, store.WorkAttemptStart{
		ProjectID:      "detent",
		IssueID:        issue.ID,
		Identifier:     issue.Identifier,
		IssueURL:       issue.URL,
		Repo:           issue.PRRepository,
		WorkerType:     "agent",
		WorkerHost:     "worker-a",
		Lane:           issue.State,
		AttemptNumber:  1,
		StartedAt:      startedAt,
		LeaseExpiresAt: startedAt.Add(10 * time.Minute),
		Phase:          "running",
		StatusMessage:  "worker running",
		NextAction:     "wait",
	})
	if err != nil {
		t.Fatalf("StartWorkAttempt() error = %v", err)
	}
	if status == store.WorkAttemptStatusTerminal {
		if err := runtimeStore.CompleteWorkAttempt(ctx, store.WorkAttemptCompletion{
			AttemptID:     attemptID,
			CompletedAt:   startedAt.Add(5 * time.Minute),
			Status:        store.WorkAttemptStatusTerminal,
			TerminalState: terminalState,
			Phase:         "completed",
			StatusMessage: string(terminalState),
		}); err != nil {
			t.Fatalf("CompleteWorkAttempt() error = %v", err)
		}
	}
	return attemptID
}

func recoveryTimelineEvent(
	t *testing.T,
	ctx context.Context,
	runtimeStore store.Store,
	issueID string,
	action WorkAttemptRecoveryAction,
) store.WorkflowPhaseEvent {
	t.Helper()

	timeline, err := runtimeStore.IssueWorkflowTimeline(ctx, store.IssueIdentity{ProjectID: "detent", IssueID: issueID})
	if err != nil {
		t.Fatalf("IssueWorkflowTimeline() error = %v", err)
	}
	for _, event := range timeline.Events {
		if event.PhaseType == store.WorkflowPhaseTypeRecovery && event.PhaseName == string(action) {
			return event
		}
	}
	t.Fatalf("missing recovery event %q in %#v", action, timeline.Events)
	return store.WorkflowPhaseEvent{}
}

func recoveryTestIssue() connector.Issue {
	return connector.Issue{
		ID:           "issue-979",
		Identifier:   "digitaldrywood/detent#979",
		URL:          "https://github.com/digitaldrywood/detent/issues/979",
		State:        "In Progress",
		PRRepository: "digitaldrywood/detent",
	}
}

type recoveryTestConnector struct {
	fieldIssueID string
	fieldName    string
	fieldValue   string
}

func (*recoveryTestConnector) Name() string {
	return "recovery-test"
}

func (*recoveryTestConnector) FetchCandidateIssues(context.Context) ([]connector.Issue, error) {
	return nil, nil
}

func (*recoveryTestConnector) FetchIssuesByStates(context.Context, []string) ([]connector.Issue, error) {
	return nil, nil
}

func (*recoveryTestConnector) FetchIssueStatesByIDs(context.Context, []string) ([]connector.Issue, error) {
	return []connector.Issue{recoveryTestIssue()}, nil
}

func (*recoveryTestConnector) CreateComment(context.Context, string, string) error {
	return nil
}

func (*recoveryTestConnector) UpdateIssueState(context.Context, string, string) error {
	return nil
}

func (*recoveryTestConnector) SetAssignee(context.Context, string, string) error {
	return nil
}

func (c *recoveryTestConnector) SetField(_ context.Context, issueID string, fieldName string, value string) error {
	c.fieldIssueID = issueID
	c.fieldName = fieldName
	c.fieldValue = value
	return nil
}

func TestRecoveryPreservesIndependentRuntimeHolds(t *testing.T) {
	t.Parallel()
	for _, source := range []BlockedSource{BlockedSourceDependency, BlockedSourceOperatorStop, BlockedSourceOwnership, BlockedSourceMergeDuration} {
		t.Run(string(source), func(t *testing.T) {
			runtimeStore := openWorkAttemptRecoveryStore(t, t.Context())
			issue := recoveryTestIssue()
			now := time.Now().UTC()
			attemptID := startRecoveryWorkAttempt(t, t.Context(), runtimeStore, issue, store.WorkAttemptStatusTerminal, store.WorkAttemptTerminalFailure, now.Add(-time.Minute))
			orch := newWorkAttemptRecoveryOrchestrator(t, runtimeStore, nil)
			state := newState(orch.cfg)
			state.Blocked[issue.ID] = Blocked{Issue: issue, Source: source, Reason: "independent predicate remains false"}
			response, err := orch.handleWorkAttemptRecovery(t.Context(), &state, WorkAttemptRecoveryRequest{AttemptID: attemptID, Action: WorkAttemptRecoveryRetryFresh}, now)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := state.Blocked[issue.ID]; !ok {
				t.Fatal("recovery erased an independent hold")
			}
			if response.Status != "blocked" || !strings.Contains(response.Message, "independent predicate") {
				t.Fatalf("response conceals independent hold: %#v", response)
			}
		})
	}
}

func TestRecoveryDoesNotQueueBehindRunningWorker(t *testing.T) {
	t.Parallel()
	runtimeStore := openWorkAttemptRecoveryStore(t, t.Context())
	issue := recoveryTestIssue()
	now := time.Now().UTC()
	attemptID := startRecoveryWorkAttempt(t, t.Context(), runtimeStore, issue, store.WorkAttemptStatusTerminal, store.WorkAttemptTerminalFailure, now.Add(-time.Minute))
	orch := newWorkAttemptRecoveryOrchestrator(t, runtimeStore, nil)
	state := newState(orch.cfg)
	state.Running[issue.ID] = Running{Issue: issue, WorkAttemptID: attemptID + 1}
	response, err := orch.handleWorkAttemptRecovery(t.Context(), &state, WorkAttemptRecoveryRequest{AttemptID: attemptID, Action: WorkAttemptRecoveryRetryFresh}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := state.Retry[issue.ID]; ok {
		t.Fatal("recovery queued another retry behind a running worker")
	}
	if response.Status != "running" {
		t.Fatalf("status = %q, want running", response.Status)
	}
}
