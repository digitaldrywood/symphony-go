package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/backendcapacity"
	"github.com/digitaldrywood/detent/internal/connector"
	runpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/runtimeoutput"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

const (
	projectFailureClassSessionTokenCeiling = "session_token_ceiling"
	projectFailureClassDeliverableCommand  = "deliverable_command_failure"
	projectFailureClassNoProgress          = "no_progress"
	projectFailureClassRunnerFinalState    = "runner_final_state"
	projectFailureClassBackendError        = "backend_error"
	projectFailureClassRunnerError         = "runner_error"
	projectFailureBreakerDispatchPaused    = "project_failure_breaker_paused"
)

type FailureBreakerCanaryResult struct {
	Requested     bool
	Active        bool
	CanaryIssueID string
	ResumeAt      time.Time
}

func newProjectFailureBreaker(cfg FailureBreakerConfig) ProjectFailureBreaker {
	return ProjectFailureBreaker{
		Config:   normalizeFailureBreakerConfig(cfg),
		Failures: map[string][]ProjectFailure{},
	}
}

func cloneProjectFailureBreaker(breaker ProjectFailureBreaker) ProjectFailureBreaker {
	cloned := breaker
	cloned.Failures = make(map[string][]ProjectFailure, len(breaker.Failures))
	for class, failures := range breaker.Failures {
		cloned.Failures[class] = append([]ProjectFailure(nil), failures...)
	}
	return cloned
}

func normalizeFailureBreakerConfig(cfg FailureBreakerConfig) FailureBreakerConfig {
	if cfg.SameClassLimit <= 0 {
		cfg.SameClassLimit = defaultFailureBreakerSameClassLimit
	}
	if cfg.Window <= 0 {
		cfg.Window = defaultFailureBreakerWindow
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = defaultFailureBreakerCooldown
	}
	return cfg
}

func (b ProjectFailureBreaker) Active() bool {
	return strings.TrimSpace(b.Class) != ""
}

func projectFailureBreakerAllowsDispatch(state *State, now time.Time) bool {
	if state == nil || !state.FailureBreaker.Active() {
		return true
	}
	if strings.TrimSpace(state.FailureBreaker.CanaryIssueID) != "" {
		return false
	}
	return !now.Before(state.FailureBreaker.ResumeAt)
}

func tryReserveProjectFailureBreakerCanary(state *State, issueID string, now time.Time) (bool, bool) {
	if state == nil || !state.FailureBreaker.Active() {
		return false, true
	}
	if !projectFailureBreakerAllowsDispatch(state, now) {
		return false, false
	}
	state.FailureBreaker.CanaryIssueID = strings.TrimSpace(issueID)
	return true, true
}

func releaseProjectFailureBreakerCanary(state *State, issueID string) {
	if state == nil || strings.TrimSpace(issueID) == "" {
		return
	}
	if state.FailureBreaker.CanaryIssueID == strings.TrimSpace(issueID) {
		state.FailureBreaker.CanaryIssueID = ""
	}
}

func (o *Orchestrator) deferProjectFailureBreakerCanary(state *State, issueID string, at time.Time, delay time.Duration) {
	if state == nil || !state.FailureBreaker.Active() || state.FailureBreaker.CanaryIssueID != strings.TrimSpace(issueID) {
		return
	}
	if delay <= 0 {
		delay = defaultOverloadRetryDelay
	}
	if at.IsZero() {
		at = o.clockNow()
	}
	state.FailureBreaker.CanaryIssueID = ""
	state.FailureBreaker.ResumeAt = at.Add(delay)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      at,
		Event:   "project_failure_breaker_canary_deferred",
		Message: "project failure breaker canary deferred after transient backend capacity",
	})
}

func (o *Orchestrator) requestProjectFailureBreakerCanary(state *State, now time.Time) FailureBreakerCanaryResult {
	if state == nil || !state.FailureBreaker.Active() {
		return FailureBreakerCanaryResult{}
	}
	result := FailureBreakerCanaryResult{
		Active:        true,
		CanaryIssueID: strings.TrimSpace(state.FailureBreaker.CanaryIssueID),
		ResumeAt:      state.FailureBreaker.ResumeAt,
	}
	if result.CanaryIssueID != "" {
		return result
	}
	blockedUntil := state.FailureBreaker.ResumeAt
	state.FailureBreaker.ResumeAt = now
	releaseProjectFailureBreakerRetries(state, blockedUntil, now)
	result.Requested = true
	result.ResumeAt = now
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "project_failure_breaker_canary_requested",
		Message: "operator requested an immediate project failure breaker canary for " + state.FailureBreaker.Class,
	})
	return result
}

func releaseProjectFailureBreakerRetries(state *State, blockedUntil time.Time, releasedAt time.Time) {
	if state == nil {
		return
	}
	for issueID, retry := range state.Retry {
		if retry.DueAt.Equal(blockedUntil) && retry.DueAt.After(releasedAt) {
			retry.DueAt = releasedAt
			state.Retry[issueID] = retry
		}
	}
}

func (o *Orchestrator) reloadProjectFailureBreaker(state *State, cfg FailureBreakerConfig, now time.Time) {
	if state == nil {
		return
	}
	state.FailureBreaker.Config = normalizeFailureBreakerConfig(cfg)
	pruneProjectFailures(&state.FailureBreaker, now)
	if !state.FailureBreaker.Active() {
		return
	}
	state.FailureBreaker.ResumeAt = now
	if strings.TrimSpace(state.FailureBreaker.CanaryIssueID) != "" {
		return
	}
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      now,
		Event:   "project_failure_breaker_canary_ready",
		Message: "project failure breaker canary enabled after workflow reload for " + state.FailureBreaker.Class,
	})
}

func (o *Orchestrator) recordProjectAttemptOutcome(
	state *State,
	issueID string,
	completedAt time.Time,
	terminalState store.WorkAttemptTerminalState,
	err error,
	errorClass string,
	errorMessage string,
) {
	if state == nil {
		return
	}
	if completedAt.IsZero() {
		completedAt = time.Now()
		if o != nil && o.now != nil {
			completedAt = o.now()
		}
	}
	switch terminalState {
	case store.WorkAttemptTerminalSuccess:
		o.recordProjectFailureBreakerSuccess(state, strings.TrimSpace(issueID), completedAt)
		return
	case store.WorkAttemptTerminalFailure, store.WorkAttemptTerminalTimedOut, store.WorkAttemptTerminalNoProgress:
	default:
		releaseProjectFailureBreakerCanary(state, issueID)
		return
	}

	class := projectAttemptFailureClass(err, terminalState, errorClass, errorMessage)
	if class == "" {
		return
	}
	o.recordProjectFailureBreakerEvidence(state, o.projectFailureEvidence(state, issueID, err, errorMessage, completedAt), class, completedAt)
}

func (o *Orchestrator) projectFailureEvidence(state *State, issueID string, err error, errorMessage string, at time.Time) ProjectFailure {
	issueID = strings.TrimSpace(issueID)
	issue, runtimeIdentity, capacityScope := projectFailureIssueContext(state, issueID)
	backendID := strings.TrimSpace(capacityScope.BackendID)
	backendKind := strings.TrimSpace(capacityScope.BackendKind)
	provider := strings.TrimSpace(capacityScope.Provider)
	if backendID == "" {
		backendID = strings.TrimSpace(runtimeIdentity.BackendID)
	}
	if backendKind == "" {
		backendKind = strings.TrimSpace(runtimeIdentity.BackendKind)
	}
	if provider == "" {
		provider = strings.TrimSpace(runtimeIdentity.Provider.Value)
	}
	if capacityErr, ok := backendcapacity.As(err); ok {
		scope := capacityErr.Scope.Normalize()
		backendID = scope.BackendID
		backendKind = scope.BackendKind
		provider = scope.Provider
	}
	representativeError := projectFailureRepresentativeError(o, err, errorMessage)
	return ProjectFailure{
		IssueID:      issueID,
		Identifier:   strings.TrimSpace(issue.Identifier),
		IssueURL:     strings.TrimSpace(issue.URL),
		Title:        strings.TrimSpace(issue.Title),
		ErrorMessage: representativeError,
		Cause:        projectFailureOperatorCause(err, representativeError),
		BackendID:    backendID,
		BackendKind:  backendKind,
		Provider:     provider,
		At:           at,
	}
}

func projectFailureIssueContext(state *State, issueID string) (connector.Issue, agentidentity.Identity, backendcapacity.Scope) {
	if state == nil {
		return connector.Issue{ID: issueID}, agentidentity.Identity{}, backendcapacity.Scope{}
	}
	if running, ok := state.Running[issueID]; ok {
		return cloneIssue(running.Issue), running.RuntimeIdentity, running.CapacityScope
	}
	if blocked, ok := state.Blocked[issueID]; ok {
		return cloneIssue(blocked.Issue), agentidentity.Identity{}, backendcapacity.Scope{}
	}
	if retry, ok := state.Retry[issueID]; ok {
		return cloneIssue(retry.Issue), agentidentity.Identity{}, retry.CapacityScope
	}
	if completed, ok := state.Completed[issueID]; ok {
		return cloneIssue(completed.Issue), completed.RuntimeIdentity, backendcapacity.Scope{}
	}
	for _, issues := range [][]connector.Issue{state.BoardIssues, state.Pipeline} {
		for _, issue := range issues {
			if strings.TrimSpace(issue.ID) == issueID {
				return cloneIssue(issue), agentidentity.Identity{}, backendcapacity.Scope{}
			}
		}
	}
	return connector.Issue{ID: issueID}, agentidentity.Identity{}, backendcapacity.Scope{}
}

func projectFailureRepresentativeError(o *Orchestrator, err error, fallback string) string {
	message := strings.TrimSpace(fallback)
	var carrier interface {
		BackendErrorMessage() string
	}
	if errors.As(err, &carrier) {
		if value := strings.TrimSpace(carrier.BackendErrorMessage()); value != "" {
			message = value
		}
	}
	if message == "" && err != nil {
		message = strings.TrimSpace(err.Error())
	}
	message = strings.Join(strings.Fields(message), " ")
	if o != nil {
		message = o.operatorText(message)
	}
	return runtimeoutput.Truncate(message, 1024).Value
}

func projectFailureOperatorCause(err error, representativeError string) string {
	if capacityErr, ok := backendcapacity.As(err); ok {
		if cause := strings.TrimSpace(capacityErr.Details.Reason); cause != "" {
			return cause
		}
		return "provider capacity exhausted"
	}
	lower := strings.ToLower(representativeError)
	if strings.Contains(lower, "usage limit") || strings.Contains(lower, "you've hit your limit") || strings.Contains(lower, "you have hit your limit") {
		return "provider usage limit reached"
	}
	return runtimeoutput.Truncate(representativeError, 240).Value
}

func (o *Orchestrator) recordProjectFailureBreakerSuccess(state *State, issueID string, at time.Time) {
	if state.FailureBreaker.Active() {
		canaryIssueID := strings.TrimSpace(state.FailureBreaker.CanaryIssueID)
		if canaryIssueID == "" || canaryIssueID != issueID {
			return
		}
		o.closeProjectFailureBreakerAfterCanary(state, issueID, at, "successful attempt")
		return
	}
	resetProjectFailureBreaker(&state.FailureBreaker)
}

func (o *Orchestrator) recordProjectFailureBreakerProgress(state *State, issueID string, at time.Time) {
	if state != nil && state.FailureBreaker.PreTurn && !state.FailureBreaker.Active() {
		resetProjectFailureBreaker(&state.FailureBreaker)
	}
	if state == nil || !state.FailureBreaker.Active() || strings.TrimSpace(state.FailureBreaker.CanaryIssueID) != strings.TrimSpace(issueID) {
		return
	}
	o.closeProjectFailureBreakerAfterCanary(state, issueID, at, "successful backend start")
}

func (o *Orchestrator) closeProjectFailureBreakerAfterCanary(state *State, issueID string, at time.Time, outcome string) {
	closedClass := state.FailureBreaker.Class
	resetProjectFailureBreaker(&state.FailureBreaker)
	o.activateDispatchRecovery(
		state,
		dispatchRecoveryProjectFailureBreaker,
		closedClass,
		at,
		issueID,
	)
	recordStateEvent(state, telemetry.ActivityEvent{
		At:      at,
		Event:   "project_failure_breaker_closed",
		Message: "project failure breaker closed after " + outcome + " for " + closedClass,
	})
}

func (o *Orchestrator) recordProjectFailureBreakerFailure(state *State, issueID string, class string, at time.Time) {
	o.recordProjectFailureBreakerEvidence(state, ProjectFailure{IssueID: strings.TrimSpace(issueID), At: at}, class, at)
}

func (o *Orchestrator) recordProjectFailureBreakerEvidence(state *State, failure ProjectFailure, class string, at time.Time) {
	breaker := &state.FailureBreaker
	breaker.Config = normalizeFailureBreakerConfig(breaker.Config)
	limit, cooldown := breaker.Config.SameClassLimit, breaker.Config.Cooldown
	if breaker.PreTurn {
		limit = consecutiveRetryCycleLimit
		cooldown = normalizeBlockedRecoveryConfig(o.cfg.BlockedRecovery).BreakerCooldown
	} else {
		pruneProjectFailures(breaker, at)
	}

	if breaker.Active() && breaker.Class != class {
		closedClass := breaker.Class
		resetProjectFailureBreaker(breaker)
		recordStateEvent(state, telemetry.ActivityEvent{
			At:      at,
			Event:   "project_failure_breaker_closed",
			Message: "project failure breaker closed after different failure class " + class + " replaced " + closedClass,
		})
	}

	failure.At = at
	failures := append(breaker.Failures[class], failure)
	breaker.Failures[class] = failures
	if breaker.Active() {
		breaker.Count = len(failures)
		breaker.FirstFailureAt = failures[0].At
		breaker.TrippedAt = at
		breaker.ResumeAt = at.Add(cooldown)
		breaker.CanaryIssueID = ""
		o.logProjectFailureBreaker(state, at, "project_failure_breaker_retripped")
		return
	}
	if len(failures) < limit {
		return
	}

	breaker.Class = class
	breaker.Count = len(failures)
	breaker.FirstFailureAt = failures[0].At
	breaker.TrippedAt = at
	breaker.ResumeAt = at.Add(cooldown)
	breaker.CanaryIssueID = ""
	o.logProjectFailureBreaker(state, at, "project_failure_breaker_tripped")
}

func (o *Orchestrator) logProjectFailureBreaker(state *State, at time.Time, event string) {
	breaker := state.FailureBreaker
	message := fmt.Sprintf(
		"paused project dispatch after %d %s failures within %s",
		breaker.Count,
		breaker.Class,
		breaker.Config.Window,
	)
	recordStateEvent(state, telemetry.ActivityEvent{At: at, Event: event, Message: message})
	if o.logger != nil {
		o.logger.Warn(
			"project failure circuit breaker tripped",
			"event", event,
			"project_id", o.cfg.Project.ID,
			"failure_class", breaker.Class,
			"failure_count", breaker.Count,
			"window_seconds", int64(breaker.Config.Window/time.Second),
			"cooldown_seconds", int64(breaker.Config.Cooldown/time.Second),
			"resume_at", breaker.ResumeAt,
		)
	}
}

func resetProjectFailureBreaker(breaker *ProjectFailureBreaker) {
	if breaker == nil {
		return
	}
	config := normalizeFailureBreakerConfig(breaker.Config)
	*breaker = newProjectFailureBreaker(config)
}

func pruneProjectFailures(breaker *ProjectFailureBreaker, now time.Time) {
	if breaker == nil || breaker.PreTurn {
		return
	}
	breaker.Config = normalizeFailureBreakerConfig(breaker.Config)
	cutoff := now.Add(-breaker.Config.Window)
	for class, failures := range breaker.Failures {
		kept := failures[:0]
		for _, failure := range failures {
			if !failure.At.Before(cutoff) {
				kept = append(kept, failure)
			}
		}
		if len(kept) == 0 {
			delete(breaker.Failures, class)
			continue
		}
		breaker.Failures[class] = kept
	}
}

func projectAttemptFailureClass(
	err error,
	terminalState store.WorkAttemptTerminalState,
	errorClass string,
	errorMessage string,
) string {
	combined := strings.ToLower(strings.TrimSpace(errorMessage))
	if err != nil {
		combined += "\n" + strings.ToLower(strings.TrimSpace(err.Error()))
	}
	if errors.Is(err, runpkg.ErrSessionTokenCeilingExceeded) || strings.Contains(combined, "session token ceiling exceeded") {
		return projectFailureClassSessionTokenCeiling
	}
	if command := deliverableFailureCommand(err, combined); command != "" {
		return projectFailureClassDeliverableCommand + ":" + command
	}
	if strings.Contains(combined, "deliverable command failed") {
		return projectFailureClassDeliverableCommand + ":unknown"
	}
	var backendError interface {
		BackendErrorBody() string
	}
	if errors.As(err, &backendError) {
		if body := strings.TrimSpace(backendError.BackendErrorBody()); body != "" {
			return projectFailureClassBackendError + ":" + projectFailureHash(body)
		}
	}
	var backendMessage interface {
		BackendErrorMessage() string
	}
	if errors.As(err, &backendMessage) {
		if message := strings.TrimSpace(backendMessage.BackendErrorMessage()); message != "" {
			return projectFailureClassBackendError + ":" + projectFailureHash(message)
		}
	}
	if class := normalizeProjectFailureClass(errorClass); class == backendcapacity.StartupFailureErrorClass || class == backendcapacity.StartupTimeoutErrorClass {
		return backendcapacity.StartupFailureErrorClass
	} else if class != "" && class != workAttemptErrorRunner {
		return class
	}
	if terminalState == store.WorkAttemptTerminalNoProgress {
		return projectFailureClassNoProgress
	}
	if err == nil && terminalState == store.WorkAttemptTerminalFailure {
		return projectFailureClassRunnerFinalState + ":" + projectFailureHash(errorMessage)
	}
	message := errorMessage
	if err != nil {
		message = err.Error()
	}
	if strings.TrimSpace(message) == "" {
		return ""
	}
	return projectFailureClassRunnerError + ":" + projectFailureHash(message)
}

func deliverableFailureCommand(err error, message string) string {
	var deliverableErr *runpkg.DeliverableCommandError
	if errors.As(err, &deliverableErr) && deliverableErr != nil {
		return normalizeDeliverableFailureCommand(deliverableErr.Operation)
	}
	const prefix = "deliverable command failed ("
	index := strings.Index(message, prefix)
	if index < 0 {
		return ""
	}
	command := message[index+len(prefix):]
	if end := strings.Index(command, ")"); end >= 0 {
		command = command[:end]
	}
	return normalizeDeliverableFailureCommand(command)
}

func normalizeDeliverableFailureCommand(command string) string {
	command = strings.Join(strings.Fields(strings.TrimSpace(command)), " ")
	if len(command) > 160 {
		command = command[:160]
	}
	return command
}

func normalizeProjectFailureClass(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var builder strings.Builder
	lastSeparator := false
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
			lastSeparator = false
			continue
		}
		if !lastSeparator {
			builder.WriteByte('_')
			lastSeparator = true
		}
	}
	return strings.Trim(builder.String(), "_")
}

func projectFailureHash(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:6])
}
