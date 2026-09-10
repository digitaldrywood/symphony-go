package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

type WorkAttemptRecoveryAction string

const (
	WorkAttemptRecoveryInspect          WorkAttemptRecoveryAction = "inspect"
	WorkAttemptRecoveryAbandon          WorkAttemptRecoveryAction = "abandon"
	WorkAttemptRecoveryRetryFresh       WorkAttemptRecoveryAction = "retry_fresh"
	WorkAttemptRecoveryRetryResume      WorkAttemptRecoveryAction = "retry_resume"
	WorkAttemptRecoveryCleanupWorkspace WorkAttemptRecoveryAction = "cleanup_workspace"
)

type WorkAttemptRecoveryErrorCode string

const (
	WorkAttemptRecoveryInvalidRequest        WorkAttemptRecoveryErrorCode = "invalid_request"
	WorkAttemptRecoveryNotFound              WorkAttemptRecoveryErrorCode = "work_attempt_not_found"
	WorkAttemptRecoveryUnavailable           WorkAttemptRecoveryErrorCode = "work_attempt_recovery_unavailable"
	WorkAttemptRecoveryUnsupportedState      WorkAttemptRecoveryErrorCode = "unsupported_recovery_state"
	WorkAttemptRecoveryConfirmationRequired  WorkAttemptRecoveryErrorCode = "confirmation_required"
	WorkAttemptRecoveryActionFailed          WorkAttemptRecoveryErrorCode = "recovery_action_failed"
	WorkAttemptRecoveryIssueIdentityRequired WorkAttemptRecoveryErrorCode = "issue_identity_required"
)

type WorkAttemptRecoveryError struct {
	Code    WorkAttemptRecoveryErrorCode `json:"code"`
	Message string                       `json:"message"`
}

func (e *WorkAttemptRecoveryError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

type WorkAttemptRecoveryRequest struct {
	ProjectID string                    `json:"project_id,omitempty"`
	AttemptID int64                     `json:"attempt_id,omitempty"`
	Action    WorkAttemptRecoveryAction `json:"action,omitempty"`
	Confirm   bool                      `json:"confirm,omitempty"`
	Reason    string                    `json:"reason,omitempty"`
	Operator  string                    `json:"operator,omitempty"`
}

type WorkAttemptRecoveryResponse struct {
	CurrentAttemptID int64                                 `json:"current_attempt_id,omitempty"`
	Blockers         []string                              `json:"blockers,omitempty"`
	NextAction       string                                `json:"next_action,omitempty"`
	Queued           bool                                  `json:"queued"`
	PolicyMismatch   string                                `json:"policy_mismatch,omitempty"`
	Attempt          telemetry.WorkAttempt                 `json:"attempt"`
	Action           WorkAttemptRecoveryAction             `json:"action,omitempty"`
	Status           string                                `json:"status,omitempty"`
	Message          string                                `json:"message,omitempty"`
	Available        []WorkAttemptRecoveryActionDescriptor `json:"available_actions"`
	ResumeEligible   bool                                  `json:"resume_eligible"`
	ResumeState      *WorkAttemptResumeState               `json:"resume_state,omitempty"`
	AuditEventID     int64                                 `json:"audit_event_id,omitempty"`
	ConfirmationKey  string                                `json:"confirmation_key,omitempty"`
}

type WorkAttemptRecoveryActionDescriptor struct {
	Action               WorkAttemptRecoveryAction `json:"action"`
	Label                string                    `json:"label"`
	RequiresConfirmation bool                      `json:"requires_confirmation"`
	Destructive          bool                      `json:"destructive"`
	Reason               string                    `json:"reason,omitempty"`
}

type WorkAttemptResumeState struct {
	DetentSessionID   int64     `json:"detent_session_id"`
	ProviderThreadID  string    `json:"provider_thread_id,omitempty"`
	ProviderSessionID string    `json:"provider_session_id,omitempty"`
	RequestedModel    string    `json:"requested_model,omitempty"`
	Model             string    `json:"model,omitempty"`
	AgentBackendID    string    `json:"agent_backend_id,omitempty"`
	AgentBackendKind  string    `json:"agent_backend_kind,omitempty"`
	AgentRole         string    `json:"agent_role,omitempty"`
	CompletedAt       time.Time `json:"completed_at"`
}

type workAttemptRecoveryRequest struct {
	at          time.Time
	request     WorkAttemptRecoveryRequest
	reply       chan workAttemptRecoveryReply
	receiptOnly bool
}

type workAttemptRecoveryReply struct {
	response WorkAttemptRecoveryResponse
	err      error
}

func (o *Orchestrator) WorkAttemptReceipt(ctx context.Context, projectID string, attemptID int64) (WorkAttemptRecoveryResponse, error) {
	return o.requestWorkAttemptRecovery(ctx, WorkAttemptRecoveryRequest{ProjectID: projectID, AttemptID: attemptID}, true)
}

func (o *Orchestrator) RecoverWorkAttempt(ctx context.Context, request WorkAttemptRecoveryRequest) (WorkAttemptRecoveryResponse, error) {
	return o.requestWorkAttemptRecovery(ctx, request, false)
}

func (o *Orchestrator) requestWorkAttemptRecovery(ctx context.Context, request WorkAttemptRecoveryRequest, receiptOnly bool) (WorkAttemptRecoveryResponse, error) {
	if o == nil || o.workAttempts == nil {
		return WorkAttemptRecoveryResponse{}, recoveryError(WorkAttemptRecoveryUnavailable, "work attempt recovery is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queued := workAttemptRecoveryRequest{
		at:          time.Now().UTC(),
		request:     request,
		reply:       make(chan workAttemptRecoveryReply, 1),
		receiptOnly: receiptOnly,
	}
	select {
	case <-ctx.Done():
		return WorkAttemptRecoveryResponse{}, ctx.Err()
	case <-o.done:
		return WorkAttemptRecoveryResponse{}, ErrStopped
	case o.recoveryRequests <- queued:
	}
	select {
	case <-ctx.Done():
		return WorkAttemptRecoveryResponse{}, ctx.Err()
	case <-o.done:
		return WorkAttemptRecoveryResponse{}, ErrStopped
	case reply := <-queued.reply:
		return reply.response, reply.err
	}
}

func (o *Orchestrator) handleWorkAttemptRecovery(ctx context.Context, state *State, request WorkAttemptRecoveryRequest, now time.Time) (WorkAttemptRecoveryResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	request = normalizeWorkAttemptRecoveryRequest(request)
	receipt, err := o.workAttemptRecoveryReceipt(ctx, request.ProjectID, request.AttemptID, now)
	if err != nil {
		return WorkAttemptRecoveryResponse{}, err
	}
	attempt := receipt.Attempt
	action := request.Action
	if action == "" {
		err := recoveryError(WorkAttemptRecoveryInvalidRequest, "recovery action is required")
		o.recordWorkAttemptRecoveryAudit(ctx, attempt, request, "rejected", err.Error(), now, receipt.ResumeState)
		return receipt, err
	}
	if !workAttemptRecoveryActionKnown(action) {
		err := recoveryError(WorkAttemptRecoveryInvalidRequest, "unsupported recovery action "+string(action))
		o.recordWorkAttemptRecoveryAudit(ctx, attempt, request, "rejected", err.Error(), now, receipt.ResumeState)
		return receipt, err
	}
	if action == WorkAttemptRecoveryInspect {
		auditID := o.recordWorkAttemptRecoveryAudit(ctx, attempt, request, "succeeded", "work attempt receipt inspected", now, receipt.ResumeState)
		receipt.Action = action
		receipt.Status = "succeeded"
		receipt.Message = "work attempt receipt inspected"
		receipt.AuditEventID = auditID
		receipt = o.describeWorkAttemptRecovery(ctx, state, receipt)
		o.recordWorkAttemptRecoveryStateEvent(state, receipt, now)
		return receipt, nil
	}
	if !workAttemptRecoveryActionAvailable(receipt.Available, action) {
		err := recoveryError(WorkAttemptRecoveryUnsupportedState, "recovery action "+string(action)+" is not supported for this work attempt state")
		o.recordWorkAttemptRecoveryAudit(ctx, attempt, request, "rejected", err.Error(), now, receipt.ResumeState)
		return receipt, err
	}
	if workAttemptRecoveryRequiresConfirmation(action) && !request.Confirm {
		err := recoveryError(WorkAttemptRecoveryConfirmationRequired, "recovery action "+string(action)+" requires confirm=true")
		o.recordWorkAttemptRecoveryAudit(ctx, attempt, request, "rejected", err.Error(), now, receipt.ResumeState)
		return receipt, err
	}

	response := receipt
	response.Action = action
	switch action {
	case WorkAttemptRecoveryAbandon:
		response, err = o.abandonWorkAttempt(ctx, state, request, now, receipt)
	case WorkAttemptRecoveryRetryFresh, WorkAttemptRecoveryRetryResume:
		response, err = o.retryWorkAttempt(ctx, state, request, now, receipt)
	case WorkAttemptRecoveryCleanupWorkspace:
		response, err = o.cleanupWorkAttemptWorkspace(ctx, state, request, now, receipt)
	default:
		err = recoveryError(WorkAttemptRecoveryInvalidRequest, "unsupported recovery action "+string(action))
	}
	if err != nil {
		o.recordWorkAttemptRecoveryAudit(ctx, attempt, request, "failed", err.Error(), now, receipt.ResumeState)
		return response, err
	}
	response.AuditEventID = o.recordWorkAttemptRecoveryAudit(ctx, response.Attempt, request, response.Status, response.Message, now, response.ResumeState)
	o.recordWorkAttemptRecoveryStateEvent(state, response, now)
	return response, nil
}

func (o *Orchestrator) workAttemptRecoveryReceipt(ctx context.Context, projectID string, attemptID int64, now time.Time) (WorkAttemptRecoveryResponse, error) {
	if o == nil || o.workAttempts == nil {
		return WorkAttemptRecoveryResponse{}, recoveryError(WorkAttemptRecoveryUnavailable, "work attempt recovery is unavailable")
	}
	if attemptID <= 0 {
		return WorkAttemptRecoveryResponse{}, recoveryError(WorkAttemptRecoveryInvalidRequest, "attempt_id is required")
	}
	projectID = strings.TrimSpace(projectID)
	attempt, err := o.workAttempts.WorkAttempt(ctx, attemptID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return WorkAttemptRecoveryResponse{}, recoveryError(WorkAttemptRecoveryNotFound, "work attempt not found")
		}
		return WorkAttemptRecoveryResponse{}, fmt.Errorf("read work attempt: %w", err)
	}
	if projectID != "" && strings.TrimSpace(attempt.ProjectID) != projectID {
		return WorkAttemptRecoveryResponse{}, recoveryError(WorkAttemptRecoveryNotFound, "work attempt not found")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	snapshot := telemetryWorkAttempt(attempt, now)
	resumeState, resumeEligible := o.latestWorkAttemptResumeState(ctx, attempt)
	available := availableWorkAttemptRecoveryActions(snapshot, resumeEligible, o.reaper != nil)
	var policyMismatch string
	if err := o.checkAttemptPolicy(attempt); err != nil {
		policyMismatch = err.Error()
	}
	return WorkAttemptRecoveryResponse{
		PolicyMismatch:  policyMismatch,
		Attempt:         snapshot,
		Available:       available,
		ResumeEligible:  resumeEligible,
		ResumeState:     resumeState,
		ConfirmationKey: workAttemptRecoveryConfirmationKey(snapshot),
	}, nil
}

func (o *Orchestrator) abandonWorkAttempt(
	ctx context.Context,
	state *State,
	request WorkAttemptRecoveryRequest,
	now time.Time,
	receipt WorkAttemptRecoveryResponse,
) (WorkAttemptRecoveryResponse, error) {
	message := strings.TrimSpace(request.Reason)
	if message == "" {
		message = "operator marked work attempt abandoned"
	}
	completion := store.WorkAttemptCompletion{
		AttemptID:              receipt.Attempt.AttemptID,
		CompletedAt:            now,
		Status:                 store.WorkAttemptStatusTerminal,
		TerminalState:          store.WorkAttemptTerminalAbandoned,
		ErrorClass:             "operator_abandoned",
		ErrorMessage:           o.operatorText(message),
		Phase:                  "operator_recovery",
		StatusMessage:          "operator marked attempt abandoned",
		WaitReason:             "operator_action",
		GitHubRateSnapshotJSON: o.githubRateSnapshotJSON(state),
		CIState:                receipt.Attempt.CIState,
		CapacitySnapshotJSON:   o.capacitySnapshotJSON(state, recoveryIssueFromReceipt(receipt.Attempt)),
		WorkerMetadataJSON:     receipt.Attempt.WorkerMetadataJSON,
		MetricsJSON:            receipt.Attempt.MetricsJSON,
		NextAction:             "retry or cleanup",
	}
	if err := o.workAttempts.CompleteWorkAttempt(ctx, completion); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return receipt, recoveryError(WorkAttemptRecoveryUnsupportedState, "active work attempt could not be abandoned because it is already terminal")
		}
		return receipt, fmt.Errorf("abandon work attempt: %w", err)
	}
	issueID := strings.TrimSpace(receipt.Attempt.IssueID)
	if issueID == "" {
		if issue, ok := o.recoveryIssue(state, receipt.Attempt); ok {
			issueID = issue.ID
		}
	}
	releaseClaimErr := o.abandonClaim(ctx, issueID)
	o.clearLiveWorkAttemptState(state, receipt.Attempt)
	updated, err := o.workAttemptRecoveryReceipt(ctx, request.ProjectID, request.AttemptID, now)
	if err != nil {
		return receipt, err
	}
	o.upsertWorkAttemptSnapshot(state, updated.Attempt)
	updated.Action = request.Action
	updated.Status = "succeeded"
	updated.Message = "work attempt abandoned"
	if releaseClaimErr != nil {
		updated.Message = "work attempt abandoned; tracker lease release failed"
	}
	return updated, nil
}

func (o *Orchestrator) retryWorkAttempt(
	ctx context.Context,
	state *State,
	request WorkAttemptRecoveryRequest,
	now time.Time,
	receipt WorkAttemptRecoveryResponse,
) (WorkAttemptRecoveryResponse, error) {
	issue, ok := o.recoveryIssue(state, receipt.Attempt)
	if !ok {
		return receipt, recoveryError(WorkAttemptRecoveryIssueIdentityRequired, "work attempt issue identity is required for retry")
	}
	if request.Action == WorkAttemptRecoveryRetryResume && !receipt.ResumeEligible {
		return receipt, recoveryError(WorkAttemptRecoveryUnsupportedState, "resume retry requires an eligible completed session")
	}
	if running, active := state.Running[issue.ID]; active {
		receipt.CurrentAttemptID = running.WorkAttemptID
		receipt.Action = request.Action
		receipt.Status = "running"
		receipt.Message = "a worker is already running for this issue"
		return receipt, nil
	}
	issues, err := o.connector.FetchIssueStatesByIDs(ctx, []string{issue.ID})
	if err != nil {
		return receipt, fmt.Errorf("revalidate recovery issue: %w", err)
	}
	if len(issues) != 1 || issues[0].ID != issue.ID {
		return receipt, recoveryError(WorkAttemptRecoveryNotFound, "current tracker issue is unavailable")
	}
	issue = issues[0]
	intent, err := o.prepareWorkAttemptRetryIntent(ctx, issue, request, receipt, now)
	if err != nil {
		return receipt, err
	}
	return o.applyWorkAttemptRetryIntent(ctx, state, issue, intent, receipt, now)
}

func (o *Orchestrator) queueWorkAttemptRetry(state *State, issue connector.Issue, request WorkAttemptRecoveryRequest, receipt WorkAttemptRecoveryResponse, now time.Time) WorkAttemptRecoveryResponse {
	attempt := receipt.Attempt.AttemptNumber + 1
	if attempt <= 1 {
		attempt = 1
	}
	reason := strings.TrimSpace(request.Reason)
	if reason == "" {
		reason = "operator requested work attempt retry"
	}
	if request.Action == WorkAttemptRecoveryRetryResume {
		reason = "operator requested work attempt retry with resume"
	}
	retryMode := runpkg.RetryModeFresh
	resumeState := store.AgentResumeState{}
	if request.Action == WorkAttemptRecoveryRetryResume {
		retryMode = runpkg.RetryModeResume
		resumeState = storeAgentResumeState(receipt.ResumeState)
	}
	issue = cloneIssue(issue)
	delete(state.Blocked, issue.ID)
	delete(state.Completed, issue.ID)
	state.Retry[issue.ID] = Retry{
		RecoveryAttemptID: request.AttemptID,
		Issue:             issue,
		Attempt:           attempt,
		DueAt:             now,
		Error:             o.operatorText(reason),
		RetryMode:         retryMode,
		ResumeState:       resumeState,
	}
	state.Claimed[issue.ID] = Claimed{
		Issue:     issue,
		ClaimedAt: now,
	}
	receipt.Action = request.Action
	receipt.Status = "queued"
	receipt.Queued = true
	receipt.NextAction = "scheduler rechecks current dispatch predicates"
	receipt.Message = "work attempt retry queued"
	if request.Action == WorkAttemptRecoveryRetryResume {
		receipt.Message = "work attempt resume retry queued"
	}
	receipt.Available = availableWorkAttemptRecoveryActions(receipt.Attempt, receipt.ResumeEligible, o.reaper != nil)
	return receipt
}

func (o *Orchestrator) cleanupWorkAttemptWorkspace(
	ctx context.Context,
	state *State,
	request WorkAttemptRecoveryRequest,
	now time.Time,
	receipt WorkAttemptRecoveryResponse,
) (WorkAttemptRecoveryResponse, error) {
	issue, ok := o.recoveryIssue(state, receipt.Attempt)
	if !ok {
		return receipt, recoveryError(WorkAttemptRecoveryIssueIdentityRequired, "work attempt issue identity is required for workspace cleanup")
	}
	if _, reaped := state.ReapedWorkspaces[issue.ID]; reaped {
		receipt.Action = request.Action
		receipt.Status = "succeeded"
		receipt.Message = "workspace cleanup already completed for this issue"
		return receipt, nil
	}
	if !o.reapWorkspace(ctx, state, issue, "operator_recovery", now) {
		return receipt, recoveryError(WorkAttemptRecoveryActionFailed, "workspace cleanup failed or could not be verified")
	}
	receipt.Action = request.Action
	receipt.Status = "succeeded"
	receipt.Message = "workspace cleanup completed"
	return receipt, nil
}

func (o *Orchestrator) latestWorkAttemptResumeState(ctx context.Context, attempt store.WorkAttempt) (*WorkAttemptResumeState, bool) {
	if o == nil || o.agentResume == nil {
		return nil, false
	}
	if err := o.checkAttemptPolicy(attempt); err != nil {
		return nil, false
	}
	state, err := o.agentResume.LatestIssueAgentResumeState(ctx, store.IssueIdentity{
		ProjectID:  attempt.ProjectID,
		IssueID:    attempt.IssueID,
		Identifier: attempt.Identifier,
		IssueURL:   attempt.IssueURL,
	})
	if err != nil {
		return nil, false
	}
	return workAttemptResumeState(state), true
}

func (o *Orchestrator) recordWorkAttemptRecoveryAudit(
	ctx context.Context,
	attempt telemetry.WorkAttempt,
	request WorkAttemptRecoveryRequest,
	status string,
	message string,
	now time.Time,
	resumeState *WorkAttemptResumeState,
) int64 {
	if o == nil || o.workflowMetrics == nil {
		return 0
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	metadata := workAttemptRecoveryMetadata(attempt, request, resumeState)
	id, err := o.workflowMetrics.RecordWorkflowPhaseEvent(ctx, store.WorkflowPhaseEvent{
		ProjectID:      o.workflowMetricsProjectID(),
		IssueID:        attempt.IssueID,
		Identifier:     attempt.Identifier,
		IssueURL:       attempt.IssueURL,
		PRNumber:       cloneInt64Pointer(attempt.PRNumber),
		PhaseType:      store.WorkflowPhaseTypeRecovery,
		PhaseName:      string(request.Action),
		Reason:         strings.TrimSpace(message),
		Status:         strings.TrimSpace(status),
		StartedAt:      now,
		FinishedAt:     now,
		EndpointFamily: "operator",
		MetadataJSON:   metadata,
	})
	if err != nil {
		if o.logger != nil {
			o.logger.Warn("record work attempt recovery audit failed", "attempt_id", attempt.AttemptID, "action", request.Action, "error", err)
		}
		return 0
	}
	return id
}

func (o *Orchestrator) recordWorkAttemptRecoveryStateEvent(state *State, response WorkAttemptRecoveryResponse, now time.Time) {
	if state == nil {
		return
	}
	eventName := "work_attempt_recovery_" + response.Status
	message := strings.TrimSpace(response.Message)
	if message == "" {
		message = string(response.Action)
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   eventName,
		Message: fmt.Sprintf("%s for %s", message, workAttemptReceiptLabel(response.Attempt)),
	})
}

func (o *Orchestrator) clearLiveWorkAttemptState(state *State, attempt telemetry.WorkAttempt) {
	if state == nil {
		return
	}
	issueID := strings.TrimSpace(attempt.IssueID)
	if issueID == "" {
		return
	}
	if running, ok := state.Running[issueID]; ok && running.WorkAttemptID == attempt.AttemptID {
		cancelRunning(state, issueID, "operator.work_attempt_recovery")
		o.releaseGlobalDispatchSlot(running.globalSlot)
		o.heartbeats.remove(issueID)
		delete(state.Running, issueID)
	}
	delete(state.Claimed, issueID)
	delete(state.Retry, issueID)
	delete(state.BudgetRefusals, issueID)
}

func (o *Orchestrator) recoveryIssue(state *State, attempt telemetry.WorkAttempt) (connector.Issue, bool) {
	issue := recoveryIssueFromReceipt(attempt)
	if strings.TrimSpace(issue.ID) == "" {
		return connector.Issue{}, false
	}
	if found, ok := findRecoveryIssue(state, issue); ok {
		issue = mergeIssueTrackerFields(issue, found)
	}
	return issue, true
}

func recoveryIssueFromReceipt(attempt telemetry.WorkAttempt) connector.Issue {
	issue := connector.NewIssue()
	issue.ID = strings.TrimSpace(attempt.IssueID)
	issue.Identifier = strings.TrimSpace(attempt.Identifier)
	issue.URL = strings.TrimSpace(attempt.IssueURL)
	issue.State = strings.TrimSpace(attempt.Lane)
	issue.PRRepository = strings.TrimSpace(attempt.Repo)
	issue.PRNumber = connectorPRNumber(attempt.PRNumber)
	return issue
}

func findRecoveryIssue(state *State, target connector.Issue) (connector.Issue, bool) {
	if state == nil {
		return connector.Issue{}, false
	}
	for _, running := range state.Running {
		if recoveryIssuesMatch(target, running.Issue) {
			return running.Issue, true
		}
	}
	for _, retry := range state.Retry {
		if recoveryIssuesMatch(target, retry.Issue) {
			return retry.Issue, true
		}
	}
	for _, blocked := range state.Blocked {
		if recoveryIssuesMatch(target, blocked.Issue) {
			return blocked.Issue, true
		}
	}
	for _, completed := range state.Completed {
		if recoveryIssuesMatch(target, completed.Issue) {
			return completed.Issue, true
		}
	}
	for _, claimed := range state.Claimed {
		if recoveryIssuesMatch(target, claimed.Issue) {
			return claimed.Issue, true
		}
	}
	for _, issue := range state.BoardIssues {
		if recoveryIssuesMatch(target, issue) {
			return issue, true
		}
	}
	for _, issue := range state.Pipeline {
		if recoveryIssuesMatch(target, issue) {
			return issue, true
		}
	}
	return connector.Issue{}, false
}

func availableWorkAttemptRecoveryActions(attempt telemetry.WorkAttempt, resumeEligible bool, reaperAvailable bool) []WorkAttemptRecoveryActionDescriptor {
	actions := []WorkAttemptRecoveryActionDescriptor{
		workAttemptRecoveryActionDescriptor(WorkAttemptRecoveryInspect),
	}
	status := strings.TrimSpace(attempt.Status)
	terminalState := strings.TrimSpace(attempt.TerminalState)
	switch {
	case status == string(store.WorkAttemptStatusActive):
		actions = append(actions, workAttemptRecoveryActionDescriptor(WorkAttemptRecoveryAbandon))
	case status == string(store.WorkAttemptStatusTerminal):
		if workAttemptTerminalStateRetryable(terminalState) {
			actions = append(actions, workAttemptRecoveryActionDescriptor(WorkAttemptRecoveryRetryFresh))
			if resumeEligible {
				actions = append(actions, workAttemptRecoveryActionDescriptor(WorkAttemptRecoveryRetryResume))
			}
		}
		if reaperAvailable {
			actions = append(actions, workAttemptRecoveryActionDescriptor(WorkAttemptRecoveryCleanupWorkspace))
		}
	}
	return actions
}

func workAttemptRecoveryActionDescriptor(action WorkAttemptRecoveryAction) WorkAttemptRecoveryActionDescriptor {
	switch action {
	case WorkAttemptRecoveryAbandon:
		return WorkAttemptRecoveryActionDescriptor{
			Action:               action,
			Label:                "Abandon",
			RequiresConfirmation: true,
			Destructive:          true,
			Reason:               "marks an active attempt terminal and clears live worker state",
		}
	case WorkAttemptRecoveryRetryFresh:
		return WorkAttemptRecoveryActionDescriptor{
			Action: action,
			Label:  "Retry fresh",
			Reason: "queues a fresh scheduler retry",
		}
	case WorkAttemptRecoveryRetryResume:
		return WorkAttemptRecoveryActionDescriptor{
			Action: action,
			Label:  "Retry with resume",
			Reason: "queues a retry using the latest eligible completed session",
		}
	case WorkAttemptRecoveryCleanupWorkspace:
		return WorkAttemptRecoveryActionDescriptor{
			Action:               action,
			Label:                "Clean workspace",
			RequiresConfirmation: true,
			Destructive:          true,
			Reason:               "reruns workspace cleanup and may delete worktrees, branches, or processes",
		}
	default:
		return WorkAttemptRecoveryActionDescriptor{
			Action: WorkAttemptRecoveryInspect,
			Label:  "Receipt",
			Reason: "returns the durable attempt receipt",
		}
	}
}

func workAttemptRecoveryActionAvailable(actions []WorkAttemptRecoveryActionDescriptor, action WorkAttemptRecoveryAction) bool {
	for _, candidate := range actions {
		if candidate.Action == action {
			return true
		}
	}
	return false
}

func workAttemptRecoveryActionKnown(action WorkAttemptRecoveryAction) bool {
	switch action {
	case WorkAttemptRecoveryInspect, WorkAttemptRecoveryAbandon, WorkAttemptRecoveryRetryFresh, WorkAttemptRecoveryRetryResume, WorkAttemptRecoveryCleanupWorkspace:
		return true
	default:
		return false
	}
}

func workAttemptRecoveryRequiresConfirmation(action WorkAttemptRecoveryAction) bool {
	switch action {
	case WorkAttemptRecoveryAbandon, WorkAttemptRecoveryCleanupWorkspace:
		return true
	default:
		return false
	}
}

func workAttemptTerminalStateRetryable(terminalState string) bool {
	switch strings.TrimSpace(terminalState) {
	case string(store.WorkAttemptTerminalFailure),
		string(store.WorkAttemptTerminalCancelled),
		string(store.WorkAttemptTerminalTimedOut),
		string(store.WorkAttemptTerminalAbandoned),
		string(store.WorkAttemptTerminalNoProgress):
		return true
	default:
		return false
	}
}

func normalizeWorkAttemptRecoveryRequest(request WorkAttemptRecoveryRequest) WorkAttemptRecoveryRequest {
	return WorkAttemptRecoveryRequest{
		ProjectID: strings.TrimSpace(request.ProjectID),
		AttemptID: request.AttemptID,
		Action:    WorkAttemptRecoveryAction(strings.TrimSpace(string(request.Action))),
		Confirm:   request.Confirm,
		Reason:    strings.TrimSpace(request.Reason),
		Operator:  strings.TrimSpace(request.Operator),
	}
}

func recoveryIssuesMatch(target connector.Issue, candidate connector.Issue) bool {
	if recoveryNonEmptyEqual(target.ID, candidate.ID) {
		return true
	}
	if recoveryNonEmptyEqual(target.Identifier, candidate.Identifier) {
		return true
	}
	return recoveryNonEmptyEqual(target.URL, candidate.URL)
}

func recoveryNonEmptyEqual(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return left != "" && right != "" && left == right
}

func workAttemptResumeState(state store.AgentResumeState) *WorkAttemptResumeState {
	return &WorkAttemptResumeState{
		DetentSessionID:   state.DetentSessionID,
		ProviderThreadID:  state.ProviderThreadID,
		ProviderSessionID: state.ProviderSessionID,
		RequestedModel:    state.RequestedModel,
		Model:             state.Model,
		AgentBackendID:    state.AgentBackendID,
		AgentBackendKind:  state.AgentBackendKind,
		AgentRole:         state.AgentRole,
		CompletedAt:       state.CompletedAt,
	}
}

func storeAgentResumeState(state *WorkAttemptResumeState) store.AgentResumeState {
	if state == nil {
		return store.AgentResumeState{}
	}
	return store.AgentResumeState{
		DetentSessionID:   state.DetentSessionID,
		ProviderThreadID:  state.ProviderThreadID,
		ProviderSessionID: state.ProviderSessionID,
		RequestedModel:    state.RequestedModel,
		Model:             state.Model,
		AgentBackendID:    state.AgentBackendID,
		AgentBackendKind:  state.AgentBackendKind,
		AgentRole:         state.AgentRole,
		CompletedAt:       state.CompletedAt,
	}
}

func workAttemptRecoveryMetadata(attempt telemetry.WorkAttempt, request WorkAttemptRecoveryRequest, resumeState *WorkAttemptResumeState) string {
	metadata := map[string]any{
		"attempt_id":           attempt.AttemptID,
		"action":               request.Action,
		"confirm":              request.Confirm,
		"operator":             request.Operator,
		"status":               attempt.Status,
		"terminal_state":       attempt.TerminalState,
		"worker_host":          attempt.WorkerHost,
		"worker_type":          attempt.WorkerType,
		"phase":                attempt.Phase,
		"resume_eligible":      resumeState != nil,
		"confirmation_key":     workAttemptRecoveryConfirmationKey(attempt),
		"operator_reason":      request.Reason,
		"requires_confirmable": workAttemptRecoveryRequiresConfirmation(request.Action),
	}
	if resumeState != nil {
		metadata["resume_state"] = resumeState
	}
	payload, err := json.Marshal(metadata)
	if err != nil {
		return "{}"
	}
	return string(payload)
}

func workAttemptRecoveryConfirmationKey(attempt telemetry.WorkAttempt) string {
	return fmt.Sprintf("work-attempt-%d", attempt.AttemptID)
}

func workAttemptReceiptLabel(attempt telemetry.WorkAttempt) string {
	if label := strings.TrimSpace(attempt.Identifier); label != "" {
		return label
	}
	if label := strings.TrimSpace(attempt.IssueID); label != "" {
		return label
	}
	return fmt.Sprintf("attempt %d", attempt.AttemptID)
}

func recoveryError(code WorkAttemptRecoveryErrorCode, message string) *WorkAttemptRecoveryError {
	return &WorkAttemptRecoveryError{
		Code:    code,
		Message: strings.TrimSpace(message),
	}
}

func connectorPRNumber(value *int64) *int {
	if value == nil {
		return nil
	}
	number := int(*value)
	return &number
}
