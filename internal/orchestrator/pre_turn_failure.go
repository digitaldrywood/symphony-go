package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

func preTurnFailureClass(event runpkg.Completion, running Running) string {
	if event.Err == nil || errors.Is(event.Err, context.Canceled) || event.Result.TurnStarted || running.TurnCount > 0 || running.WorkProductPushed || running.Tokens.TotalTokens > 0 || event.Result.Tokens.TotalTokens > 0 || running.Cancellation != nil {
		return ""
	}
	if capacityErr, ok := backendcapacity.As(event.Err); ok {
		switch capacityErr.Details.Kind {
		case backendcapacity.StartupTimeoutKind:
			return backendcapacity.StartupTimeoutErrorClass
		case backendcapacity.StartupFailureKind:
			return backendcapacity.StartupFailureErrorClass
		default:
			return ""
		}
	}
	if errors.Is(event.Err, runpkg.ErrWorkspacePreparation) {
		return workAttemptErrorWorkspace
	}
	if errors.Is(event.Err, runpkg.ErrMergeWorkerStartupTimeout) {
		return backendcapacity.StartupTimeoutErrorClass
	}
	var deliverableErr *runpkg.DeliverableCommandError
	var recoveryErr *runpkg.DeliverableRecoveryError
	if errors.As(event.Err, &deliverableErr) || errors.As(event.Err, &recoveryErr) || runpkg.IsDeliverableConfigurationError(event.Err) || errors.Is(event.Err, runpkg.ErrSessionTokenCeilingExceeded) || isGitHubRESTBudgetHeadroomError(event.Err) {
		return ""
	}
	if runnerWorkAttemptErrorClass(event.Err) == workAttemptErrorRunner {
		return workAttemptErrorRunner
	}
	return ""
}

func (o *Orchestrator) handlePreTurnFailure(ctx context.Context, state *State, event runpkg.Completion, running Running) bool {
	class := preTurnFailureClass(event, running)
	if class == "" {
		return false
	}
	breaker := &state.FailureBreaker
	failures := []ProjectFailure(nil)
	if breaker.PreTurn {
		for _, previous := range breaker.Failures {
			failures = append(failures, previous...)
		}
	} else {
		resetProjectFailureBreaker(breaker)
	}
	breaker.PreTurn = true
	breaker.Failures = map[string][]ProjectFailure{class: failures}
	if breaker.Active() {
		breaker.Class = class
	}
	o.recordProjectFailureBreakerEvidence(state, o.projectFailureEvidence(state, event.IssueID, event.Err, event.Err.Error(), event.CompletedAt), class, event.CompletedAt)
	terminal := store.WorkAttemptTerminalFailure
	if class == backendcapacity.StartupTimeoutErrorClass {
		terminal = store.WorkAttemptTerminalTimedOut
	}
	metadata := map[string]any{"dispatch_source_state": running.DispatchSourceState}
	if capacityErr, ok := backendcapacity.As(event.Err); ok {
		metadata = mergeWorkAttemptMetadata(metadata, startupFailureMetadata(capacityErr.Details))
	}
	o.completeDurableWorkAttemptWithMetadata(ctx, state, running, event.CompletedAt, terminal, class, event.Err.Error(), "failed", "instance failed before first turn", metadata)
	releaseBackendCapacityProbe(state, running)
	o.restorePreTurnIssue(ctx, state, running, event.CompletedAt)
	return true
}

func preTurnAttempt(attempt telemetry.WorkAttempt) bool {
	var metrics struct {
		Turns  *int `json:"turns"`
		Tokens int  `json:"total_tokens"`
	}
	if workAttemptHasPushedProduct(attempt) || json.Unmarshal([]byte(attempt.MetricsJSON), &metrics) != nil && strings.TrimSpace(attempt.MetricsJSON) != "" || metrics.Turns != nil && *metrics.Turns > 0 || metrics.Tokens > 0 {
		return false
	}
	switch strings.TrimSpace(attempt.ErrorClass) {
	case workAttemptErrorWorkspace, backendcapacity.StartupTimeoutErrorClass, backendcapacity.StartupFailureErrorClass:
		return true
	case workAttemptErrorRunner:
		return metrics.Turns != nil
	default:
		return false
	}
}

func (o *Orchestrator) restorePreTurnIssue(ctx context.Context, state *State, running Running, at time.Time) (connector.Issue, bool) {
	issue := running.Issue
	changed := false
	target := strings.TrimSpace(running.DispatchSourceState)
	if target == "" && normalizeState(issue.State) == normalizeState(planImplementationState) && !terminalAttemptHasWorkProduct(issue, running.WorkProductPushed) {
		target = terminalAttemptTodoState(o.cfg.ActiveStates)
	}
	if o.connector != nil && target != "" && normalizeState(issue.State) != normalizeState(target) && (running.DispatchTargetState == "" || normalizeState(issue.State) == normalizeState(running.DispatchTargetState)) {
		if err := o.updateIssueState(ctx, state, issue, target, at, terminalAttemptWithoutWorkProductReason, laneMutationAcceptCompletion); err != nil {
			if o.logger != nil {
				o.logger.Warn("pre-turn failure lane restoration failed", "issue_id", issue.ID, "error", err)
			}
		} else {
			issue.State = target
			changed = true
		}
	}
	o.releaseTerminalAttemptClaim(ctx, state, issue, at)
	delete(state.Retry, issue.ID)
	delete(state.PriorAttempts, issue.ID)
	return issue, changed
}
