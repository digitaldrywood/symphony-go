package web

import (
	"encoding/json"
	"errors"
	"html"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/project"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

type workAttemptAPIResponse struct {
	AttemptID              int64    `json:"attempt_id"`
	ProjectID              string   `json:"project_id,omitempty"`
	IssueID                string   `json:"issue_id,omitempty"`
	Identifier             string   `json:"identifier,omitempty"`
	IssueURL               *string  `json:"issue_url,omitempty"`
	PRNumber               *int64   `json:"pr_number,omitempty"`
	Repo                   string   `json:"repo,omitempty"`
	WorkerType             string   `json:"worker_type,omitempty"`
	WorkerHost             *string  `json:"worker_host,omitempty"`
	Lane                   string   `json:"lane,omitempty"`
	AttemptNumber          int      `json:"attempt_number,omitempty"`
	Status                 string   `json:"status,omitempty"`
	TerminalState          string   `json:"terminal_state,omitempty"`
	Phase                  string   `json:"phase,omitempty"`
	StatusMessage          *string  `json:"status_message,omitempty"`
	CurrentCommand         *string  `json:"current_command,omitempty"`
	WaitReason             *string  `json:"wait_reason,omitempty"`
	ErrorClass             *string  `json:"error_class,omitempty"`
	ErrorMessage           *string  `json:"error_message,omitempty"`
	NextAction             *string  `json:"next_action,omitempty"`
	StartedAt              *string  `json:"started_at,omitempty"`
	LeaseExpiresAt         *string  `json:"lease_expires_at,omitempty"`
	HeartbeatAt            *string  `json:"heartbeat_at,omitempty"`
	CompletedAt            *string  `json:"completed_at,omitempty"`
	Stale                  bool     `json:"stale,omitempty"`
	ReceiptURL             string   `json:"receipt_url"`
	RecoveryURL            string   `json:"recovery_url"`
	SupportedRecoveryHints []string `json:"supported_recovery_hints,omitempty"`
}

type workAttemptRecoveryRequestPayload struct {
	Action   string `json:"action" form:"action"`
	Confirm  bool   `json:"confirm" form:"confirm"`
	Reason   string `json:"reason" form:"reason"`
	Operator string `json:"operator" form:"operator"`
}

func (s *Server) apiWorkAttemptReceipt(c echo.Context) error {
	if s.recovery == nil {
		return c.JSON(http.StatusServiceUnavailable, errorResponse("orchestrator_unavailable", "Orchestrator is unavailable"))
	}
	attemptID, err := workAttemptIDParam(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("invalid_request", "attempt_id must be a positive integer"))
	}
	projectID := strings.TrimSpace(c.Param("project_id"))
	response, err := s.recovery.WorkAttemptReceipt(c.Request().Context(), projectID, attemptID)
	if err != nil {
		return workAttemptRecoveryAPIError(c, err)
	}
	if htmxRequest(c) {
		return c.HTML(http.StatusOK, workAttemptReceiptHTML(response))
	}
	return c.JSON(http.StatusOK, response)
}

func (s *Server) apiWorkAttemptRecovery(c echo.Context) error {
	if s.recovery == nil {
		return c.JSON(http.StatusServiceUnavailable, errorResponse("orchestrator_unavailable", "Orchestrator is unavailable"))
	}
	attemptID, err := workAttemptIDParam(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("invalid_request", "attempt_id must be a positive integer"))
	}
	payload, err := workAttemptRecoveryPayload(c)
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, errorResponse("invalid_request", "Request body must be valid JSON or form data"))
	}
	response, err := s.recovery.RecoverWorkAttempt(c.Request().Context(), orchestrator.WorkAttemptRecoveryRequest{
		ProjectID: strings.TrimSpace(c.Param("project_id")),
		AttemptID: attemptID,
		Action:    orchestrator.WorkAttemptRecoveryAction(payload.Action),
		Confirm:   payload.Confirm,
		Reason:    payload.Reason,
		Operator:  payload.Operator,
	})
	if err != nil {
		return workAttemptRecoveryAPIError(c, err)
	}
	if htmxRequest(c) {
		c.Response().Header().Set("HX-Trigger", "workAttemptRecovery")
		return c.HTML(http.StatusOK, workAttemptRecoveryHTML(response, false))
	}
	return c.JSON(http.StatusOK, response)
}

func workAttemptEntries(entries []telemetry.WorkAttempt) []workAttemptAPIResponse {
	payload := make([]workAttemptAPIResponse, 0, len(entries))
	for _, entry := range entries {
		payload = append(payload, workAttemptEntry(entry))
	}
	return payload
}

func workAttemptEntry(entry telemetry.WorkAttempt) workAttemptAPIResponse {
	receiptURL := workAttemptReceiptURL(entry)
	return workAttemptAPIResponse{
		AttemptID:              entry.AttemptID,
		ProjectID:              entry.ProjectID,
		IssueID:                entry.IssueID,
		Identifier:             entry.Identifier,
		IssueURL:               optionalString(entry.IssueURL),
		PRNumber:               entry.PRNumber,
		Repo:                   entry.Repo,
		WorkerType:             entry.WorkerType,
		WorkerHost:             optionalString(entry.WorkerHost),
		Lane:                   entry.Lane,
		AttemptNumber:          entry.AttemptNumber,
		Status:                 entry.Status,
		TerminalState:          entry.TerminalState,
		Phase:                  entry.Phase,
		StatusMessage:          optionalString(entry.StatusMessage),
		CurrentCommand:         optionalString(entry.CurrentCommand),
		WaitReason:             optionalString(entry.WaitReason),
		ErrorClass:             optionalString(entry.ErrorClass),
		ErrorMessage:           optionalString(entry.ErrorMessage),
		NextAction:             optionalString(entry.NextAction),
		StartedAt:              timestampString(entry.StartedAt),
		LeaseExpiresAt:         timestampStringPtr(entry.LeaseExpiresAt),
		HeartbeatAt:            timestampStringPtr(entry.HeartbeatAt),
		CompletedAt:            timestampStringPtr(entry.CompletedAt),
		Stale:                  entry.Stale,
		ReceiptURL:             receiptURL,
		RecoveryURL:            receiptURL + "/recovery",
		SupportedRecoveryHints: workAttemptRecoveryHints(entry),
	}
}

func workAttemptRecoveryAPIError(c echo.Context, err error) error {
	status := http.StatusInternalServerError
	code := "work_attempt_recovery_failed"
	message := "Work attempt recovery failed"
	var recoveryErr *orchestrator.WorkAttemptRecoveryError
	switch {
	case errors.As(err, &recoveryErr):
		code = string(recoveryErr.Code)
		message = recoveryErr.Message
		status = workAttemptRecoveryHTTPStatus(recoveryErr.Code)
	case errors.Is(err, project.ErrProjectNotFound), errors.Is(err, project.ErrNotRunning), errors.Is(err, orchestrator.ErrStopped):
		status = http.StatusServiceUnavailable
		code = "orchestrator_unavailable"
		message = "Orchestrator is unavailable"
	}
	if htmxRequest(c) {
		return c.HTML(status, workAttemptRecoveryHTML(orchestrator.WorkAttemptRecoveryResponse{Status: "failed", Message: message}, true))
	}
	return c.JSON(status, errorResponse(code, message))
}

func workAttemptRecoveryHTTPStatus(code orchestrator.WorkAttemptRecoveryErrorCode) int {
	switch code {
	case orchestrator.WorkAttemptRecoveryInvalidRequest:
		return http.StatusBadRequest
	case orchestrator.WorkAttemptRecoveryNotFound:
		return http.StatusNotFound
	case orchestrator.WorkAttemptRecoveryUnavailable:
		return http.StatusServiceUnavailable
	case orchestrator.WorkAttemptRecoveryUnsupportedState, orchestrator.WorkAttemptRecoveryIssueIdentityRequired:
		return http.StatusConflict
	case orchestrator.WorkAttemptRecoveryConfirmationRequired:
		return http.StatusPreconditionRequired
	case orchestrator.WorkAttemptRecoveryActionFailed:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}

func workAttemptRecoveryPayload(c echo.Context) (workAttemptRecoveryRequestPayload, error) {
	var payload workAttemptRecoveryRequestPayload
	if err := c.Bind(&payload); err != nil {
		return workAttemptRecoveryRequestPayload{}, err
	}
	if raw := strings.TrimSpace(c.FormValue("confirm")); raw != "" {
		confirmed, ok := parseWorkAttemptRecoveryConfirm(raw)
		if ok {
			payload.Confirm = confirmed
		}
	}
	return workAttemptRecoveryRequestPayload{
		Action:   strings.TrimSpace(payload.Action),
		Confirm:  payload.Confirm,
		Reason:   strings.TrimSpace(payload.Reason),
		Operator: strings.TrimSpace(payload.Operator),
	}, nil
}

func parseWorkAttemptRecoveryConfirm(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "on", "yes":
		return true, true
	}
	confirmed, err := strconv.ParseBool(raw)
	return confirmed, err == nil
}

func workAttemptIDParam(c echo.Context) (int64, error) {
	id, err := strconv.ParseInt(strings.TrimSpace(c.Param("attempt_id")), 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("invalid attempt id")
	}
	return id, nil
}

func workAttemptRecoveryHTML(response orchestrator.WorkAttemptRecoveryResponse, failed bool) string {
	kindClass := "text-ok"
	if failed {
		kindClass = "text-err"
	} else if response.Status == "blocked" || response.Status == "queued" {
		kindClass = "text-muted"
	}
	message := strings.TrimSpace(response.Message)
	if message == "" {
		message = strings.TrimSpace(response.Status)
	}
	if message == "" {
		message = "Recovery action completed"
	}
	if response.NextAction != "" {
		message += "; next: " + response.NextAction
	}
	return `<span class="font-mono text-xs ` + kindClass + `">` + html.EscapeString(message) + `</span>`
}

func workAttemptReceiptHTML(response orchestrator.WorkAttemptRecoveryResponse) string {
	attempt := response.Attempt
	var metadata struct {
		Policy policy.Descriptor `json:"policy"`
	}
	if err := json.Unmarshal([]byte(attempt.WorkerMetadataJSON), &metadata); err != nil || metadata.Policy.Validate() != nil {
		metadata.Policy = policy.Descriptor{}
	}
	type receiptField struct {
		label string
		value string
	}
	fields := []receiptField{
		{label: "Recovery", value: response.Status},
		{label: "Recovery blockers", value: strings.Join(response.Blockers, "; ")},
		{label: "Recovery next", value: response.NextAction},
		{label: "Worker", value: attempt.WorkerHost},
		{label: "Phase", value: attempt.Phase},
		{label: "Command", value: attempt.CurrentCommand},
		{label: "Wait", value: attempt.WaitReason},
		{label: "Error class", value: attempt.ErrorClass},
		{label: "Error", value: attempt.ErrorMessage},
		{label: "Status", value: attempt.Status},
		{label: "Next", value: attempt.NextAction},
	}
	if metadata.Policy.ID != "" {
		fields = append(fields,
			receiptField{label: "Pinned policy", value: metadata.Policy.ID},
			receiptField{label: "Policy revision", value: metadata.Policy.SourceRevision},
			receiptField{label: "Configuration digest", value: metadata.Policy.ConfigDigest},
		)
	}
	if response.PolicyMismatch != "" {
		fields = append(fields, receiptField{label: "Policy mismatch", value: response.PolicyMismatch})
	}
	var b strings.Builder
	b.WriteString(`<div class="rounded-md border border-line bg-surface px-3 py-2 text-xs text-sec">`)
	b.WriteString(`<div class="font-mono text-text">attempt #`)
	b.WriteString(html.EscapeString(strconv.FormatInt(attempt.AttemptID, 10)))
	if attempt.Identifier != "" {
		b.WriteString(` - `)
		b.WriteString(html.EscapeString(attempt.Identifier))
	}
	b.WriteString(`</div><dl class="mt-2 grid gap-1 sm:grid-cols-2">`)
	for _, field := range fields {
		value := strings.TrimSpace(field.value)
		if value == "" {
			value = "n/a"
		}
		b.WriteString(`<div class="min-w-0"><dt class="text-2xs font-semibold uppercase text-dim">`)
		b.WriteString(html.EscapeString(field.label))
		b.WriteString(`</dt><dd class="break-words font-mono text-text [overflow-wrap:anywhere]">`)
		b.WriteString(html.EscapeString(value))
		b.WriteString(`</dd></div>`)
	}
	b.WriteString(`</dl>`)
	if response.ResumeEligible && response.ResumeState != nil {
		sessionID := strings.TrimSpace(response.ResumeState.ProviderSessionID)
		if sessionID == "" {
			sessionID = strconv.FormatInt(response.ResumeState.DetentSessionID, 10)
		}
		b.WriteString(`<p class="mt-2 font-mono text-ok">resume eligible: `)
		b.WriteString(html.EscapeString(sessionID))
		b.WriteString(`</p>`)
	}
	b.WriteString(`</div>`)
	return b.String()
}

func workAttemptReceiptURL(entry telemetry.WorkAttempt) string {
	projectID := strings.Trim(strings.TrimSpace(entry.ProjectID), "/")
	if projectID == "" {
		projectID = "default"
	}
	return "/api/v1/projects/" + projectID + "/work-attempts/" + strconv.FormatInt(entry.AttemptID, 10)
}

func workAttemptRecoveryHints(entry telemetry.WorkAttempt) []string {
	hints := []string{"inspect"}
	if entry.Status == "active" {
		return append(hints, "abandon")
	}
	if entry.Status != "terminal" {
		return hints
	}
	switch entry.TerminalState {
	case "failure", "cancelled", "timed_out", "abandoned", "no_progress":
		hints = append(hints, "retry_fresh")
	}
	return append(hints, "cleanup_workspace")
}
