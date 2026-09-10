package web_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/orchestrator"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/web"
)

func TestWorkAttemptReceiptAPI(t *testing.T) {
	t.Parallel()

	recovery := &fakeWorkAttemptRecovery{
		receipt: orchestrator.WorkAttemptRecoveryResponse{
			Attempt: telemetry.WorkAttempt{
				AttemptID:  42,
				ProjectID:  "detent",
				IssueID:    "issue-979",
				Identifier: "digitaldrywood/detent#979",
				Status:     "active",
			},
			Available: []orchestrator.WorkAttemptRecoveryActionDescriptor{
				{Action: orchestrator.WorkAttemptRecoveryInspect, Label: "Receipt"},
				{Action: orchestrator.WorkAttemptRecoveryAbandon, Label: "Abandon", RequiresConfirmation: true},
			},
		},
	}
	server := newWorkAttemptRecoveryAPIServer(t, recovery)

	rec := performJSON(t, server.Handler(), http.MethodGet, "/api/v1/projects/detent/work-attempts/42", "", map[string]string{
		"Authorization": "Bearer detent_test_token",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var payload orchestrator.WorkAttemptRecoveryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload.Attempt.AttemptID != 42 || len(payload.Available) != 2 {
		t.Fatalf("payload = %#v, want receipt with available recovery actions", payload)
	}
	if recovery.receiptProjectID != "detent" || recovery.receiptAttemptID != 42 {
		t.Fatalf("receipt call project=%q attempt=%d, want detent/42", recovery.receiptProjectID, recovery.receiptAttemptID)
	}
}

func TestWorkAttemptReceiptHTMXAllowsDashboardUICookie(t *testing.T) {
	t.Parallel()

	recovery := &fakeWorkAttemptRecovery{
		receipt: orchestrator.WorkAttemptRecoveryResponse{
			Attempt: telemetry.WorkAttempt{
				AttemptID:      42,
				ProjectID:      "detent",
				IssueID:        "issue-979",
				Identifier:     "digitaldrywood/detent#979",
				Status:         "active",
				WorkerHost:     "worker-a",
				Phase:          "running",
				CurrentCommand: "make check",
			},
		},
	}
	server := newWorkAttemptRecoveryAPIServer(t, recovery)

	dashboard := httptest.NewRecorder()
	dashboardReq := httptest.NewRequest(http.MethodGet, "/", nil)
	server.Handler().ServeHTTP(dashboard, dashboardReq)
	if dashboard.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d, want %d; body = %s", dashboard.Code, http.StatusOK, dashboard.Body.String())
	}
	cookies := dashboard.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("dashboard response did not set UI API cookie")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/projects/detent/work-attempts/42", nil)
	req.Header.Set("HX-Request", "true")
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("receipt status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{"attempt #42", "worker-a", "make check"} {
		if !strings.Contains(body, want) {
			t.Fatalf("HTMX receipt missing %q: %s", want, body)
		}
	}
	if recovery.receiptProjectID != "detent" || recovery.receiptAttemptID != 42 {
		t.Fatalf("receipt call project=%q attempt=%d, want detent/42", recovery.receiptProjectID, recovery.receiptAttemptID)
	}
}

func TestWorkAttemptRecoveryAPIRequiresConfirmation(t *testing.T) {
	t.Parallel()

	recovery := &fakeWorkAttemptRecovery{
		recoverErr: &orchestrator.WorkAttemptRecoveryError{
			Code:    orchestrator.WorkAttemptRecoveryConfirmationRequired,
			Message: "recovery action cleanup_workspace requires confirm=true",
		},
	}
	server := newWorkAttemptRecoveryAPIServer(t, recovery)

	form := url.Values{"action": {"cleanup_workspace"}}
	rec := performRecoveryForm(t, server.Handler(), http.MethodPost, "/api/v1/projects/detent/work-attempts/42/recovery", form, map[string]string{
		"Authorization": "Bearer detent_test_token",
	})
	if rec.Code != http.StatusPreconditionRequired {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusPreconditionRequired, rec.Body.String())
	}
	if recovery.recoverRequest.Confirm {
		t.Fatal("Confirm = true, want false for request without confirmation")
	}
	if !strings.Contains(rec.Body.String(), "confirmation_required") {
		t.Fatalf("body missing confirmation code: %s", rec.Body.String())
	}
}

func TestWorkAttemptRecoveryAPIHTMXSuccess(t *testing.T) {
	t.Parallel()

	recovery := &fakeWorkAttemptRecovery{
		recover: orchestrator.WorkAttemptRecoveryResponse{
			Action:  orchestrator.WorkAttemptRecoveryCleanupWorkspace,
			Status:  "succeeded",
			Message: "workspace cleanup completed",
		},
	}
	server := newWorkAttemptRecoveryAPIServer(t, recovery)

	form := url.Values{"action": {"cleanup_workspace"}, "confirm": {"true"}}
	rec := performRecoveryForm(t, server.Handler(), http.MethodPost, "/api/v1/projects/detent/work-attempts/42/recovery", form, map[string]string{
		"Authorization": "Bearer detent_test_token",
		"HX-Request":    "true",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !recovery.recoverRequest.Confirm {
		t.Fatal("Confirm = false, want true")
	}
	if !strings.Contains(rec.Body.String(), "workspace cleanup completed") {
		t.Fatalf("body missing HTMX feedback: %s", rec.Body.String())
	}
	if rec.Header().Get("HX-Trigger") != "workAttemptRecovery" {
		t.Fatalf("HX-Trigger = %q, want workAttemptRecovery", rec.Header().Get("HX-Trigger"))
	}
}

func TestRecoveryFeedbackDistinguishesQueuedBlockedAndRunning(t *testing.T) {
	for _, status := range []string{"queued", "blocked", "running"} {
		t.Run(status, func(t *testing.T) {
			recovery := &fakeWorkAttemptRecovery{recover: orchestrator.WorkAttemptRecoveryResponse{
				Status: status, Message: "recovery " + status, NextAction: "recheck current predicates",
			}}
			server := newWorkAttemptRecoveryAPIServer(t, recovery)
			rec := performRecoveryForm(t, server.Handler(), http.MethodPost, "/api/v1/projects/detent/work-attempts/42/recovery", url.Values{"action": {"retry_fresh"}}, map[string]string{
				"Authorization": "Bearer detent_test_token", "HX-Request": "true",
			})
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "recovery "+status) || !strings.Contains(rec.Body.String(), "next: recheck current predicates") {
				t.Fatalf("feedback = %d %s", rec.Code, rec.Body.String())
			}
			if status != "running" && strings.Contains(rec.Body.String(), "text-ok") {
				t.Fatal("unfinished recovery is styled as success")
			}
		})
	}
}

func newWorkAttemptRecoveryAPIServer(t *testing.T, recovery *fakeWorkAttemptRecovery) *web.Server {
	t.Helper()

	deps := testDeps(t)
	deps.Recovery = recovery
	server, err := web.NewServer(web.Config{
		GlobalConfig: globalconfig.Config{APIToken: "detent_test_token"},
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return server
}

func performRecoveryForm(t *testing.T, handler http.Handler, method string, path string, form url.Values, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

type fakeWorkAttemptRecovery struct {
	receipt          orchestrator.WorkAttemptRecoveryResponse
	receiptErr       error
	receiptProjectID string
	receiptAttemptID int64
	recover          orchestrator.WorkAttemptRecoveryResponse
	recoverErr       error
	recoverRequest   orchestrator.WorkAttemptRecoveryRequest
}

func (r *fakeWorkAttemptRecovery) WorkAttemptReceipt(_ context.Context, projectID string, attemptID int64) (orchestrator.WorkAttemptRecoveryResponse, error) {
	r.receiptProjectID = projectID
	r.receiptAttemptID = attemptID
	return r.receipt, r.receiptErr
}

func (r *fakeWorkAttemptRecovery) RecoverWorkAttempt(_ context.Context, request orchestrator.WorkAttemptRecoveryRequest) (orchestrator.WorkAttemptRecoveryResponse, error) {
	r.recoverRequest = request
	return r.recover, r.recoverErr
}
