package web_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/explain"
	"github.com/digitaldrywood/detent/internal/store"
	"github.com/digitaldrywood/detent/internal/web"
)

func TestIssueExplanationAPIReferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		reference string
	}{
		{name: "issue ID", reference: "issue-1639"},
		{name: "canonical identifier", reference: "digitaldrywood/detent#1639"},
		{name: "URL", reference: "https://github.com/digitaldrywood/detent/issues/1639?notification=thread#event"},
		{name: "bare number", reference: "1639"},
		{name: "hash number", reference: "#1639"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			observedAt := time.Date(2026, 8, 7, 20, 0, 0, 0, time.UTC)
			want := explain.IssueExplanation{
				Schema:       explain.SchemaVersion,
				Found:        true,
				ObservedAt:   observedAt,
				Identity:     explain.Identity{ProjectID: "detent", IssueID: "issue-1639", Identifier: "digitaldrywood/detent#1639"},
				CurrentLane:  explain.Lane{Name: "In Progress", Freshness: explain.SourceLive},
				Eligibility:  explain.Eligibility{State: explain.EligibilityEligible, Refusals: []explain.EligibilityDecision{}, Source: explain.SourceAvailable},
				Sessions:     explain.Sessions{Source: explain.SourceAvailable},
				RequiredGate: explain.Gate{State: explain.GatePending, SourceState: explain.SourceAvailable, Failures: []string{}, Running: []string{"test"}},
				Sources:      []explain.SourceStatus{{Name: "snapshot", State: explain.SourceLive}},
				Evidence:     []explain.EvidenceReference{},
			}
			fake := &fakeIssueExplainer{result: want}
			server := newIssueExplanationServer(t, fake)
			query := url.Values{"reference": {tt.reference}, "schema": {"3"}}
			recorder := performJSON(t, server.Handler(), http.MethodGet, "/api/v1/projects/detent/issues/explanation?"+query.Encode(), "", map[string]string{
				"Authorization": "Bearer detent_test_token",
			})
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
			}
			if fake.query.ProjectID != "detent" || fake.query.Reference != tt.reference {
				t.Fatalf("query = %#v, want project detent and reference %q", fake.query, tt.reference)
			}
			if !fake.hadDeadline {
				t.Fatal("Explain() context had no deadline")
			}
			var got explain.IssueExplanation
			if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("response = %#v, want exact DTO %#v", got, want)
			}
		})
	}
}

func TestIssueExplanationAPIProblems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		explainer  web.IssueExplainer
		wantStatus int
		wantCode   string
	}{
		{name: "missing reference", path: "/api/v1/projects/detent/issues/explanation", explainer: &fakeIssueExplainer{}, wantStatus: http.StatusBadRequest, wantCode: "bad_request"},
		{name: "invalid version", path: "/api/v1/projects/detent/issues/explanation?reference=1639&schema=invalid", explainer: &fakeIssueExplainer{}, wantStatus: http.StatusBadRequest, wantCode: "bad_request"},
		{name: "version conflict", path: "/api/v1/projects/detent/issues/explanation?reference=1639&schema=1", explainer: &fakeIssueExplainer{}, wantStatus: http.StatusConflict, wantCode: "version_conflict"},
		{name: "ambiguous reference", path: "/api/v1/projects/detent/issues/explanation?reference=1639", explainer: &fakeIssueExplainer{err: &explain.AmbiguousIdentityError{ProjectID: "detent", Field: "issue_id", Values: []string{"a", "b"}}}, wantStatus: http.StatusConflict, wantCode: "ambiguous_reference"},
		{name: "not found", path: "/api/v1/projects/detent/issues/explanation?reference=1639", explainer: &fakeIssueExplainer{err: explain.ErrNotFound}, wantStatus: http.StatusNotFound, wantCode: "issue_not_found"},
		{name: "runtime error", path: "/api/v1/projects/detent/issues/explanation?reference=1639", explainer: &fakeIssueExplainer{err: errors.New("database closed")}, wantStatus: http.StatusServiceUnavailable, wantCode: "runtime_unavailable"},
		{name: "runtime missing", path: "/api/v1/projects/detent/issues/explanation?reference=1639", wantStatus: http.StatusServiceUnavailable, wantCode: "runtime_unavailable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := newIssueExplanationServer(t, tt.explainer)
			recorder := performJSON(t, server.Handler(), http.MethodGet, tt.path, "", map[string]string{
				"Authorization": "Bearer detent_test_token",
			})
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, tt.wantStatus, recorder.Body.String())
			}
			var payload struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if payload.Error.Code != tt.wantCode {
				t.Fatalf("error code = %q, want %q", payload.Error.Code, tt.wantCode)
			}
		})
	}
}

func TestIssueExplanationAPIReturnsDegradedDTO(t *testing.T) {
	t.Parallel()

	want := explain.IssueExplanation{
		Schema:       explain.SchemaVersion,
		Found:        false,
		ObservedAt:   time.Date(2026, 8, 7, 20, 0, 0, 0, time.UTC),
		Identity:     explain.Identity{ProjectID: "detent"},
		CurrentLane:  explain.Lane{Freshness: explain.SourceUnavailable, Degraded: true},
		Eligibility:  explain.Eligibility{State: explain.EligibilityUnknown, Refusals: []explain.EligibilityDecision{}, Source: explain.SourceUnavailable},
		Sessions:     explain.Sessions{Source: explain.SourceUnavailable},
		RequiredGate: explain.Gate{State: explain.GateUnavailable, SourceState: explain.SourceUnavailable, Failures: []string{}, Running: []string{}},
		Sources:      []explain.SourceStatus{{Name: "snapshot", State: explain.SourceUnavailable, Code: "not_published"}},
		Evidence:     []explain.EvidenceReference{},
	}
	server := newIssueExplanationServer(t, &fakeIssueExplainer{result: want})
	recorder := performJSON(t, server.Handler(), http.MethodGet, "/api/v1/projects/detent/issues/explanation?reference=%231639", "", map[string]string{
		"Authorization": "Bearer detent_test_token",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var got explain.IssueExplanation
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response = %#v, want degraded DTO %#v", got, want)
	}
}

func TestIssueExplanationAPIRequiresAuthentication(t *testing.T) {
	t.Parallel()

	fake := &fakeIssueExplainer{result: explain.IssueExplanation{Schema: explain.SchemaVersion}}
	server := newIssueExplanationServer(t, fake)
	path := "/api/v1/projects/detent/issues/explanation?reference=1639"

	unauthorized := performJSON(t, server.Handler(), http.MethodGet, path, "", nil)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}
	post := performJSON(t, server.Handler(), http.MethodPost, path, "", nil)
	if post.Code != http.StatusUnauthorized {
		t.Fatalf("POST status = %d, want %d; body = %s", post.Code, http.StatusUnauthorized, post.Body.String())
	}
	if fake.calls != 0 {
		t.Fatalf("Explain() calls = %d, want 0", fake.calls)
	}
}

func TestIssueExplanationAPIAcknowledgesCurrentParkSequence(t *testing.T) {
	t.Parallel()

	backend, err := store.Open(t.Context(), store.Config{Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	fake := &fakeIssueExplainer{result: explain.IssueExplanation{
		Schema:      explain.SchemaVersion,
		Identity:    explain.Identity{ProjectID: "detent", IssueID: "issue-1639", Identifier: "digitaldrywood/detent#1639"},
		ParkSummary: explain.ParkSummary{ParkCount: 4, Causes: []explain.ParkCauseSummary{}},
	}}
	deps := testDeps(t)
	deps.Store = backend
	deps.IssueExplainer = fake
	server, err := web.NewServer(web.Config{GlobalConfig: globalconfig.Config{APIToken: "detent_test_token"}}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	path := "/api/v1/projects/detent/issues/explanation?reference=1639&schema=3"
	recorder := performJSON(t, server.Handler(), http.MethodPost, path, "", map[string]string{"Authorization": "Bearer detent_test_token"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var got explain.IssueExplanation
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ParkSummary.AcknowledgedParkSequence != 4 || got.ParkSummary.AcknowledgedAt == nil {
		t.Fatalf("acknowledgement response = %#v", got.ParkSummary)
	}
	fake.result.ParkSummary.ParkCount = 2
	stale := performJSON(t, server.Handler(), http.MethodPost, path, "", map[string]string{"Authorization": "Bearer detent_test_token"})
	if stale.Code != http.StatusOK {
		t.Fatalf("stale acknowledgement status = %d", stale.Code)
	}
	var replay explain.IssueExplanation
	if err := json.Unmarshal(stale.Body.Bytes(), &replay); err != nil {
		t.Fatal(err)
	}
	if replay.ParkSummary.AcknowledgedParkSequence != 4 || replay.ParkSummary.AcknowledgedAt == nil || !replay.ParkSummary.AcknowledgedAt.Equal(*got.ParkSummary.AcknowledgedAt) {
		t.Fatalf("API reported a regressed acknowledgement: %#v", replay.ParkSummary)
	}
	persisted, err := backend.(store.ParkSummaryStore).IssueParkSummary(t.Context(), store.IssueIdentity{ProjectID: "detent", IssueID: "issue-1639"})
	if err != nil {
		t.Fatalf("IssueParkSummary() error = %v", err)
	}
	if persisted.AcknowledgedParkSequence != 4 {
		t.Fatalf("persisted sequence = %d, want 4", persisted.AcknowledgedParkSequence)
	}
}

func TestIssueExplanationAPICreditsAcceptedProgress(t *testing.T) {
	t.Parallel()

	backend, err := store.Open(t.Context(), store.Config{Path: filepath.Join(t.TempDir(), "detent.db")})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	creditedAt := time.Date(2026, 8, 28, 12, 34, 56, 0, time.UTC)
	fake := &fakeIssueExplainer{result: explain.IssueExplanation{
		Schema:   explain.SchemaVersion,
		Identity: explain.Identity{ProjectID: "detent", IssueID: "issue-2015", Identifier: "digitaldrywood/detent#2015", IssueURL: "https://github.com/digitaldrywood/detent/issues/2015"},
	}}
	deps := testDeps(t)
	deps.Store = backend
	deps.IssueExplainer = fake
	server, err := web.NewServer(web.Config{
		GlobalConfig: globalconfig.Config{APIToken: "detent_test_token"},
		Now:          func() time.Time { return creditedAt },
	}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	path := "/api/v1/projects/detent/issues/progress-credit?reference=%232015&schema=3"
	recorder := performJSON(t, server.Handler(), http.MethodPost, path, "", map[string]string{"Authorization": "Bearer detent_test_token"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var got store.IssueProgressCredit
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Identifier != "digitaldrywood/detent#2015" || !got.CreditedAt.Equal(creditedAt) {
		t.Fatalf("response = %#v, want credited issue at %s", got, creditedAt)
	}
	persisted, err := backend.(store.ProgressCreditStore).IssueProgressCredit(t.Context(), store.IssueIdentity{ProjectID: "detent", IssueID: "issue-2015"})
	if err != nil {
		t.Fatalf("IssueProgressCredit() error = %v", err)
	}
	if !persisted.CreditedAt.Equal(creditedAt) {
		t.Fatalf("persisted credit = %#v, want %s", persisted, creditedAt)
	}
}

type fakeIssueExplainer struct {
	result      explain.IssueExplanation
	err         error
	query       explain.Query
	calls       int
	hadDeadline bool
}

func (f *fakeIssueExplainer) Explain(ctx context.Context, query explain.Query) (explain.IssueExplanation, error) {
	f.calls++
	f.query = query
	_, f.hadDeadline = ctx.Deadline()
	return f.result, f.err
}

func newIssueExplanationServer(t *testing.T, explainer web.IssueExplainer) *web.Server {
	t.Helper()

	deps := testDeps(t)
	deps.IssueExplainer = explainer
	server, err := web.NewServer(web.Config{GlobalConfig: globalconfig.Config{APIToken: "detent_test_token"}}, deps)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return server
}
