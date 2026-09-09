package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/agentoverride"
	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
)

const workAttemptRetryIntentPhase = "operator_retry_intent"

type workAttemptRetryIntent struct {
	Schema           int                                  `json:"schema"`
	Request          WorkAttemptRecoveryRequest           `json:"request"`
	IssueID          string                               `json:"issue_id"`
	TrackerUpdatedAt *time.Time                           `json:"tracker_updated_at,omitempty"`
	Configuration    bool                                 `json:"configuration"`
	Park             *workflowLaneBlockedRecoveryMetadata `json:"park,omitempty"`
	ParkedAt         time.Time                            `json:"parked_at,omitempty"`
	RecordedAt       time.Time                            `json:"recorded_at"`
	ResumeState      *WorkAttemptResumeState              `json:"resume_state,omitempty"`
}

func (o *Orchestrator) recoveryIntentAcknowledgesPark(ctx context.Context, issue connector.Issue, park workflowLaneBlockedRecoveryMetadata, parkedAt time.Time) bool {
	intent, found, err := o.latestWorkAttemptRetryIntent(ctx, issue)
	if err != nil || !found || !intent.Configuration || intent.Park == nil || !configurationRecoveryParkCause(park.Cause) || !sameBlockedRecoveryPark(*intent.Park, park) || !intent.ParkedAt.Equal(parkedAt) {
		return false
	}
	return o.validateRecoveryConfiguration(ctx, issue, intent.Park.RunMode) == nil
}

func (o *Orchestrator) latestWorkAttemptRetryIntent(ctx context.Context, issue connector.Issue) (workAttemptRetryIntent, bool, error) {
	reader, ok := o.workflowMetrics.(WorkflowMetricsTimelineReader)
	if !ok {
		return workAttemptRetryIntent{}, false, errors.New("durable recovery history is unavailable")
	}
	timeline, err := reader.IssueWorkflowTimeline(ctx, store.IssueIdentity{ProjectID: o.workflowMetricsProjectID(), IssueID: issue.ID, Identifier: issue.Identifier, IssueURL: issue.URL})
	if err != nil {
		return workAttemptRetryIntent{}, false, err
	}
	var intent workAttemptRetryIntent
	var latestID int64
	for _, event := range timeline.Events {
		if event.PhaseType != store.WorkflowPhaseTypeRecovery || event.PhaseName != workAttemptRetryIntentPhase || event.ID <= latestID {
			continue
		}
		if err := json.Unmarshal([]byte(event.MetadataJSON), &intent); err != nil {
			return intent, false, fmt.Errorf("decode recovery intent: %w", err)
		}
		if intent.Schema != 1 || intent.IssueID != issue.ID || intent.Request.AttemptID <= 0 || intent.RecordedAt.IsZero() ||
			(intent.Request.Action != WorkAttemptRecoveryRetryFresh && intent.Request.Action != WorkAttemptRecoveryRetryResume) {
			return intent, false, errors.New("invalid durable recovery intent")
		}
		latestID = event.ID
	}
	return intent, latestID > 0, nil
}

func (o *Orchestrator) describeWorkAttemptRecovery(ctx context.Context, state *State, receipt WorkAttemptRecoveryResponse) WorkAttemptRecoveryResponse {
	issueID := receipt.Attempt.IssueID
	if running, active := state.Running[issueID]; active {
		receipt.CurrentAttemptID = running.WorkAttemptID
		receipt.Status, receipt.Message = "running", "a worker is running for this issue"
		receipt.NextAction = "observe current worker progress"
		return receipt
	}
	intent, found, err := o.latestWorkAttemptRetryIntent(ctx, recoveryIssueFromReceipt(receipt.Attempt))
	if err != nil || !found || intent.Request.AttemptID != receipt.Attempt.AttemptID {
		return receipt
	}
	latest, err := o.workAttempts.ListRecentTerminalWorkAttempts(ctx, store.WorkAttemptHistoryQuery{ProjectID: receipt.Attempt.ProjectID, IssueID: issueID, Limit: 1})
	if err == nil && len(latest) > 0 && latest[0].ID > intent.Request.AttemptID {
		receipt.CurrentAttemptID = latest[0].ID
		receipt.Status, receipt.Message = "superseded", "recovery advanced to a newer attempt; inspect its receipt for the outcome"
		return receipt
	}
	receipt.Queued = true
	receipt.Status, receipt.Message = "queued", "recovery intent recorded; awaiting dispatch"
	receipt.NextAction = "scheduler rechecks current dispatch predicates on each tick"
	issue, _ := o.recoveryIssue(state, receipt.Attempt)
	receipt.Blockers = o.currentWorkAttemptRecoveryBlockers(ctx, state, issue, receipt, time.Now().UTC())
	if len(receipt.Blockers) > 0 {
		receipt.Status = "blocked"
		receipt.Message = "recovery intent recorded; blocked: " + strings.Join(receipt.Blockers, "; ")
	}
	return receipt
}

func (o *Orchestrator) prepareWorkAttemptRetryIntent(ctx context.Context, issue connector.Issue, request WorkAttemptRecoveryRequest, receipt WorkAttemptRecoveryResponse, now time.Time) (workAttemptRetryIntent, error) {
	if err := o.validateRecoveryConfiguration(ctx, issue, workAttemptRunMode(receipt.Attempt)); err != nil {
		if issueConfigurationFailure(err, "", "") {
			return workAttemptRetryIntent{}, recoveryError(WorkAttemptRecoveryUnsupportedState, "correct the issue agent override before recovery: "+err.Error())
		}
		return workAttemptRetryIntent{}, recoveryError(WorkAttemptRecoveryUnavailable, "configuration validation is unavailable; retry after service recovery: "+err.Error())
	}
	if receipt.PolicyMismatch != "" {
		return workAttemptRetryIntent{}, recoveryError(WorkAttemptRecoveryUnsupportedState, receipt.PolicyMismatch)
	}
	active, err := o.workAttempts.ListActiveWorkAttempts(ctx, store.WorkAttemptQuery{ProjectID: receipt.Attempt.ProjectID})
	if err != nil {
		return workAttemptRetryIntent{}, err
	}
	for _, attempt := range active {
		if attempt.IssueID == issue.ID {
			return workAttemptRetryIntent{}, recoveryError(WorkAttemptRecoveryUnsupportedState, "an active durable attempt still owns this issue")
		}
	}
	latest, err := o.workAttempts.ListRecentTerminalWorkAttempts(ctx, store.WorkAttemptHistoryQuery{ProjectID: receipt.Attempt.ProjectID, IssueID: issue.ID, Limit: 1})
	if err != nil {
		return workAttemptRetryIntent{}, err
	}
	if len(latest) != 1 || latest[0].ID != request.AttemptID {
		return workAttemptRetryIntent{}, recoveryError(WorkAttemptRecoveryUnsupportedState, "a newer attempt supersedes this recovery request")
	}
	previous, found, err := o.latestWorkAttemptRetryIntent(ctx, issue)
	if err != nil {
		return workAttemptRetryIntent{}, err
	}
	if found && previous.Request.AttemptID == request.AttemptID {
		if previous.Request.Action != request.Action {
			return previous, recoveryError(WorkAttemptRecoveryUnsupportedState, "a different retry mode is already recorded for this attempt")
		}
		return previous, nil
	}
	intent := workAttemptRetryIntent{Schema: 1, Request: request, IssueID: issue.ID, TrackerUpdatedAt: issue.UpdatedAt, RecordedAt: now,
		Configuration: issueConfigurationFailure(nil, receipt.Attempt.ErrorClass, receipt.Attempt.ErrorMessage)}
	if request.Action == WorkAttemptRecoveryRetryResume {
		intent.ResumeState = receipt.ResumeState
	}
	if park, ok := o.currentBlockedRecoveryPark(ctx, nil, issue); ok {
		intent.Park = &park
		intent.ParkedAt, _ = o.currentBlockedRecoveryParkedAt(ctx, nil, issue)
	}
	if intent.Configuration && normalizeState(issue.State) == normalizeState(blockedStatusState) && intent.Park == nil {
		return intent, recoveryError(WorkAttemptRecoveryUnavailable, "current configuration park evidence is unavailable; refresh the issue and retry")
	}
	data, err := json.Marshal(intent)
	if err != nil {
		return intent, err
	}
	_, err = o.workflowMetrics.RecordWorkflowPhaseEvent(ctx, store.WorkflowPhaseEvent{
		ProjectID: receipt.Attempt.ProjectID, IssueID: issue.ID, Identifier: issue.Identifier, IssueURL: issue.URL,
		PhaseType: store.WorkflowPhaseTypeRecovery, PhaseName: workAttemptRetryIntentPhase, Status: "queued",
		Reason: "operator retry intent recorded; dispatch predicates remain enforced", StartedAt: now, FinishedAt: now, MetadataJSON: string(data),
	})
	return intent, err
}

func (o *Orchestrator) applyWorkAttemptRetryIntent(ctx context.Context, state *State, issue connector.Issue, intent workAttemptRetryIntent, receipt WorkAttemptRecoveryResponse, now time.Time) (WorkAttemptRecoveryResponse, error) {
	receipt.Action = intent.Request.Action
	receipt.Queued = true
	receipt.NextAction = "scheduler rechecks current dispatch predicates on each tick"
	if blocked, held := state.Blocked[issue.ID]; held && blocked.Source == BlockedSourceProjectStatus && blocked.Reason == "operator_recovery_pending" {
		delete(state.Blocked, issue.ID)
	}
	if running, active := state.Running[issue.ID]; active {
		receipt.CurrentAttemptID = running.WorkAttemptID
		receipt.Queued = false
		receipt.Status, receipt.Message = "running", "a worker is already running for this issue"
		return receipt, nil
	}
	if intent.Request.Action == WorkAttemptRecoveryRetryResume {
		if !receipt.ResumeEligible || intent.ResumeState == nil {
			receipt.Blockers = append(receipt.Blockers, "recorded resume session is no longer eligible")
		}
		receipt.ResumeState = intent.ResumeState
	}
	if intent.TrackerUpdatedAt != nil && (issue.UpdatedAt == nil || issue.UpdatedAt.Before(*intent.TrackerUpdatedAt)) {
		receipt.Blockers = append(receipt.Blockers, "stale tracker response; waiting for current issue revision")
	}
	if err := o.validateRecoveryConfiguration(ctx, issue, workAttemptRunMode(receipt.Attempt)); err != nil {
		kind := "configuration validation unavailable"
		if issueConfigurationFailure(err, "", "") {
			kind = "issue_configuration"
		}
		receipt.Blockers = append(receipt.Blockers, kind+": "+err.Error())
	}
	if len(receipt.Blockers) == 0 && intent.Configuration {
		o.clearConfigurationFailureEvidence(state, issue.ID, intent.RecordedAt)
		if normalizeState(issue.State) == normalizeState(blockedStatusState) && intent.Park != nil && configurationRecoveryParkCause(intent.Park.Cause) {
			current, found := o.currentBlockedRecoveryPark(ctx, state, issue)
			parkedAt, dated := o.currentBlockedRecoveryParkedAt(ctx, state, issue)
			if found && dated && parkedAt.Equal(intent.ParkedAt) && sameBlockedRecoveryPark(current, *intent.Park) {
				target := terminalAttemptTodoState(o.cfg.ActiveStates)
				metadata := workflowLaneMetadata{BlockedRecovery: intent.Park}
				if err := o.updateIssueStateByIDStrictWithMetadata(ctx, state, issue.ID, issue, target, now, "operator_configuration_recovery", metadata, laneMutationRevokeWorker); err != nil {
					return receipt, err
				}
				issue.State = target
				if blocked, held := state.Blocked[issue.ID]; held && blocked.Source == BlockedSourceProjectStatus && blocked.Reason == current.Cause && !blocked.BlockedAt.After(intent.RecordedAt) {
					delete(state.Blocked, issue.ID)
				}
			}
		}
		if blocked, held := state.Blocked[issue.ID]; held && blocked.Source == BlockedSourceProjectStatus && blocked.Reason == "issue_configuration" && !blocked.BlockedAt.After(intent.RecordedAt) {
			delete(state.Blocked, issue.ID)
		}
	}
	o.retainUnacknowledgedRecoveryParks(ctx, state, []connector.Issue{issue})
	receipt.Blockers = append(receipt.Blockers, o.currentWorkAttemptRecoveryBlockers(ctx, state, issue, receipt, now)...)
	if len(receipt.Blockers) > 0 {
		receipt.Status = "blocked"
		receipt.Message = "recovery intent recorded; blocked: " + strings.Join(receipt.Blockers, "; ")
		if _, held := state.Blocked[issue.ID]; !held {
			state.Blocked[issue.ID] = Blocked{Issue: cloneIssue(issue), Source: BlockedSourceProjectStatus, Reason: "operator_recovery_pending", RecoveryRemedy: receipt.Message}
		}
		return receipt, nil
	}
	if retry, queued := state.Retry[issue.ID]; !queued || retry.RecoveryAttemptID != intent.Request.AttemptID {
		receipt = o.queueWorkAttemptRetry(state, issue, intent.Request, receipt, now)
	} else {
		receipt.Status, receipt.Message = "queued", "work attempt retry already queued; awaiting dispatch"
	}
	return receipt, nil
}

func (o *Orchestrator) currentWorkAttemptRecoveryBlockers(ctx context.Context, state *State, issue connector.Issue, receipt WorkAttemptRecoveryResponse, now time.Time) []string {
	var blockers []string
	if receipt.PolicyMismatch != "" {
		blockers = append(blockers, receipt.PolicyMismatch)
	}
	if blocked, held := state.Blocked[issue.ID]; held {
		reason := blocked.Reason
		if reason == "operator_recovery_pending" {
			reason = blocked.RecoveryRemedy
		}
		blockers = append(blockers, reason)
	}
	if blockedRefsUnresolved(issue.BlockedBy, o.cfg.TerminalStates) {
		blockers = append(blockers, "unresolved issue dependencies")
	}
	if signal := issue.WorkpadSignal; signal != nil && signal.Status == "blocked" && (signal.HumanAction != "" || len(signal.Blockers) > 0) {
		blockers = append(blockers, "workpad blockers or human action remain unresolved")
	}
	if o.cfg.Claiming.Enabled && !o.claimable(ctx, issue, o.claimOwner(), now) {
		blockers = append(blockers, "issue is owned by another live worker")
	}
	planner := o.dispatchPlanner()
	if authorization := planner.authorizationDecision(issue); !authorization.Matched {
		blockers = append(blockers, "authorization selector: "+authorization.Detail)
	}
	if planner.needsAssignee(issue) {
		blockers = append(blockers, "required worker assignment is missing")
	}
	if activeTrackerUnavailable(state) && trackerDependentDispatch(issue) {
		blockers = append(blockers, "tracker unavailable")
	}
	if activeCIUnavailable(state) && ciDependentDispatch(issue) {
		blockers = append(blockers, "CI unavailable")
	}
	if forgeAvailabilityBlocks(state, issue, Retry{}, o.cfg.ForgeHost, now) {
		blockers = append(blockers, "forge unavailable")
	}
	if workerGitHubMonitorBlocks(state, issue.ID, Retry{}, now) {
		blockers = append(blockers, "worker GitHub monitor unavailable")
	}
	if _, paused := activeGitHubRESTCapacityOutage(state, now); paused {
		blockers = append(blockers, "GitHub REST capacity paused")
	}
	if pullRequestHydrationBlocksDispatch(issue) {
		blockers = append(blockers, "pull request evidence unavailable")
	}
	if artifactGateWaitStatusBlocksDispatch(issue, o.cfg.AutoPromote.Gate) || autoPromoteActiveGatePendingIssue(issue, state, o.cfg, o.cfg.AutoPromote) {
		blockers = append(blockers, "artifact gate pending")
	}
	if _, deferred := state.deferredCompletions[issue.ID]; deferred {
		blockers = append(blockers, "completion persistence pending")
	}
	if !stateIn(issue.State, o.cfg.ActiveStates) {
		blockers = append(blockers, "tracker lane "+issue.State+" is not runnable")
	}
	if refusal, held := state.BudgetRefusals[issue.ID]; held {
		blockers = append(blockers, "budget: "+refusal.Code)
	}
	if state.FailureBreaker.Active() && !projectFailureBreakerAllowsDispatch(state, now) {
		blockers = append(blockers, "project_failure_breaker: "+state.FailureBreaker.Class+"; recheck "+state.FailureBreaker.ResumeAt.Format(time.RFC3339))
	}
	if state.Draining || o.dispatchQuiesced() {
		blockers = append(blockers, "dispatch is draining or paused")
	}
	if retry, held := state.Retry[issue.ID]; held && (retry.CIUnavailable || retry.TrackerUnavailable || retry.ForgeUnavailable || retry.CompletionDeferred || retry.GitHubMonitor || retry.Wait.Kind != "") {
		blockers = append(blockers, "existing resource wait: "+retry.Error+"; recheck "+retry.DueAt.Format(time.RFC3339))
	}
	if scope, known := o.backendCapacityScope(runpkg.RunRequest{Issue: issue, Mode: workAttemptRunMode(receipt.Attempt), SelectorContext: o.selectorContext()}); known {
		for _, key := range sortedKeys(state.BackendOutages) {
			outage := state.BackendOutages[key]
			probeAt := outage.NextProbeAt
			if probeAt.IsZero() {
				probeAt = outage.ResumeAt
			}
			if outage.Scope.Matches(scope) && (outage.ProbeIssueID != "" || now.Before(probeAt)) {
				blockers = append(blockers, outage.Kind+": "+outage.Reason+"; recheck "+probeAt.Format(time.RFC3339))
			}
		}
	}
	return blockers
}

func (o *Orchestrator) validateRecoveryConfiguration(ctx context.Context, issue connector.Issue, mode string) error {
	if _, _, err := agentoverride.FromIssueBody(issue.Description); err != nil {
		return &runpkg.IssueConfigurationError{Field: "block", Reason: err.Error()}
	}
	if o.providerCapacity != nil {
		_, err := o.providerCapacity.DispatchCapacity(ctx, runpkg.RunRequest{Issue: issue, Mode: mode, SelectorContext: o.selectorContext()})
		return err
	}
	return nil
}

func (o *Orchestrator) reconcileIssueConfigurationHolds(ctx context.Context, state *State, issues []connector.Issue, now time.Time) {
	for _, issue := range issues {
		if _, running := state.Running[issue.ID]; running {
			continue
		}
		err := o.validateRecoveryConfiguration(ctx, issue, o.dispatchMode(ctx, state, issue))
		if err != nil && issueConfigurationFailure(err, "", "") {
			if blocked, held := state.Blocked[issue.ID]; !held || blocked.Source == BlockedSourceProjectStatus && blocked.Reason == "issue_configuration" {
				state.Blocked[issue.ID] = Blocked{Issue: cloneIssue(issue), Source: BlockedSourceProjectStatus, Reason: "issue_configuration", BlockedAt: now,
					AttemptError: err.Error(), RecoveryRemedy: "correct the issue detent-agent override", NeedsHumanAttention: true}
			}
		} else if err == nil {
			if blocked, held := state.Blocked[issue.ID]; held && blocked.Source == BlockedSourceProjectStatus && blocked.Reason == "issue_configuration" && blocked.Recovery == nil {
				delete(state.Blocked, issue.ID)
			}
		}
	}
}

func configurationRecoveryParkCause(cause string) bool {
	switch cause {
	case "issue_configuration", instantFailureCircuitBreakerCause, repeatedFailureCircuitBreakerCause, terminalAttemptRetryLimitCause:
		return true
	default:
		return false
	}
}

func (o *Orchestrator) clearConfigurationFailureEvidence(state *State, issueID string, through time.Time) {
	if failure, found := state.InstantFailures[issueID]; found && !failure.LastFailureAt.After(through) && issueConfigurationFailure(nil, "", failure.Error) {
		delete(state.InstantFailures, issueID)
	}
	if failure, found := state.RepeatedFailures[issueID]; found && !failure.LastFailureAt.After(through) && issueConfigurationFailure(nil, "", failure.Error) {
		delete(state.RepeatedFailures, issueID)
	}
	breaker := &state.FailureBreaker
	changedActive := false
	for class, failures := range breaker.Failures {
		kept := failures[:0]
		for _, failure := range failures {
			if failure.IssueID == issueID && !failure.At.After(through) && issueConfigurationFailure(nil, "", failure.ErrorMessage) {
				changedActive = changedActive || class == breaker.Class
				continue
			}
			kept = append(kept, failure)
		}
		if len(kept) == 0 {
			delete(breaker.Failures, class)
		} else {
			breaker.Failures[class] = kept
		}
	}
	if changedActive {
		breaker.Count = len(breaker.Failures[breaker.Class])
	}
	if changedActive && len(breaker.Failures[breaker.Class]) < normalizeFailureBreakerConfig(breaker.Config).SameClassLimit {
		resumeAt := breaker.ResumeAt
		breaker.Class = ""
		breaker.Count = 0
		breaker.FirstFailureAt = time.Time{}
		breaker.TrippedAt = time.Time{}
		breaker.ResumeAt = time.Time{}
		breaker.CanaryIssueID = ""
		releaseProjectFailureBreakerRetries(state, resumeAt, through)
	}
}

func (o *Orchestrator) restoreWorkAttemptRetryIntents(ctx context.Context, state *State, issues []connector.Issue, now time.Time) []connector.Issue {
	if o.workAttempts == nil || o.workflowMetrics == nil {
		return nil
	}
	var transitions []connector.Issue
	for _, issue := range issues {
		intent, found, err := o.latestWorkAttemptRetryIntent(ctx, issue)
		if err != nil || !found {
			continue
		}
		latest, err := o.workAttempts.ListRecentTerminalWorkAttempts(ctx, store.WorkAttemptHistoryQuery{ProjectID: o.cfg.Project.ID, IssueID: issue.ID, Limit: 1})
		if err != nil || len(latest) != 1 || latest[0].ID != intent.Request.AttemptID {
			continue
		}
		receipt, err := o.workAttemptRecoveryReceipt(ctx, o.cfg.Project.ID, intent.Request.AttemptID, now)
		if err != nil {
			continue
		}
		if _, err := o.applyWorkAttemptRetryIntent(ctx, state, issue, intent, receipt, now); err != nil && o.logger != nil {
			o.logger.Warn("reconcile durable operator recovery failed", "issue_id", issue.ID, "error", err)
		}
		if retry, queued := state.Retry[issue.ID]; queued && retry.RecoveryAttemptID == intent.Request.AttemptID {
			transitions = append(transitions, retry.Issue)
		}
	}
	return transitions
}
