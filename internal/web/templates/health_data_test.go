package templates

import (
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/observability"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/web/ui/primitives"
)

func TestHealthViewVerdicts(t *testing.T) {
	now := time.Date(2026, 7, 4, 16, 42, 7, 0, time.UTC)
	tests := []struct {
		name        string
		snapshot    telemetry.Snapshot
		wantKind    primitives.Kind
		wantVerdict string
	}{
		{
			name: "healthy quota reads nominal",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				RateLimits: &telemetry.RateLimits{
					GitHubREST: &telemetry.RateLimitBucket{Remaining: 4878, Used: 122, Limit: 5000},
				},
			},
			wantKind:    primitives.KindOK,
			wantVerdict: "All systems nominal.",
		},
		{
			name:        "no snapshot stays neutral",
			snapshot:    telemetry.Snapshot{GeneratedAt: now},
			wantKind:    primitives.KindNeutral,
			wantVerdict: "Waiting for the first health snapshot.",
		},
		{
			name: "refresh pacing is distinct from failure",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				Refresh: telemetry.Refresh{
					Status:               telemetry.RefreshStatusBehind,
					NextRefreshOverdue:   true,
					BehindBySeconds:      30,
					ObservedSweepSeconds: 160,
				},
			},
			wantKind:    primitives.KindOK,
			wantVerdict: "All systems nominal.",
		},
		{
			name: "host pressure hold explains dispatch wait",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				IOPressure: telemetry.IOPressure{
					Supported: true, Full: telemetry.PressureAverages{Avg10: 63.64}, FullAvg10Max: 5, DispatchHeld: true,
				},
			},
			wantKind:    primitives.KindWarn,
			wantVerdict: "Dispatch is waiting for host pressure.",
		},
		{
			name: "degraded pressure capacity explains bounded dispatch",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				IOPressure: telemetry.IOPressure{
					Supported: true, Full: telemetry.PressureAverages{Avg10: 37}, FullAvg10Max: 5,
					DegradedMaxConcurrentAgents: 1, EffectiveMaxConcurrentAgents: 1,
					CapacityConstrained: true, ConstrainedSince: now.Add(-5 * time.Minute), ConstrainedForMS: 300000,
				},
			},
			wantKind:    primitives.KindWarn,
			wantVerdict: "Dispatch is constrained by host pressure.",
		},
		{
			name: "tracker unavailability requires attention",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				TrackerUnavailable: []telemetry.TrackerCondition{{
					ProjectID: "detent", Connector: "github", Operation: "observed_status", ErrorClass: "server", NextProbeAt: now.Add(time.Minute),
				}},
			},
			wantKind:    primitives.KindErr,
			wantVerdict: "Tracker is unavailable.",
		},
		{
			name: "CI unavailability requires attention",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				CIUnavailable: []telemetry.CICondition{{
					ProjectID: "detent", UnstartedCheckCount: 6, PullRequestCount: 2, OldestQueueSeconds: 2_820,
				}},
			},
			wantKind:    primitives.KindErr,
			wantVerdict: "CI is unavailable.",
		},
		{
			name: "fault staleness requires attention",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				StalenessWarnings: []telemetry.StalenessWarning{{
					ID: "fault", Class: observability.ClassFault, ProjectID: "detent", Identifier: "digitaldrywood/detent#1960",
				}},
			},
			wantKind:    primitives.KindErr,
			wantVerdict: "Fleet work needs attention.",
		},
		{
			name: "diagnostic staleness stays nominal",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				StalenessWarnings: []telemetry.StalenessWarning{{
					ID: "diagnostic", Class: observability.ClassDiagnostic, ProjectID: "detent",
				}},
			},
			wantKind:    primitives.KindOK,
			wantVerdict: "All systems nominal.",
		},
		{
			name: "review queue staleness stays nominal",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				StalenessWarnings: []telemetry.StalenessWarning{{
					ID: "review", Class: observability.ClassReviewQueue, ProjectID: "detent", WaitingOnHuman: true,
				}},
			},
			wantKind:    primitives.KindOK,
			wantVerdict: "All systems nominal.",
		},
		{
			name: "forge write unavailability requires attention",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				ForgeUnavailable: []telemetry.ForgeCondition{{
					ProjectID: "detent", Host: "github.com", Operation: "git push", ErrorClass: "server", NextProbeAt: now.Add(time.Minute),
				}},
			},
			wantKind:    primitives.KindErr,
			wantVerdict: "Forge writes are unavailable.",
		},
		{
			name: "dispatch stall requires attention",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				DispatchStalls: []telemetry.DispatchStatus{{
					ProjectID: "detent", CandidateCount: 8, WaitReason: "authorization selector excludes every candidate", WaitReasonCode: "authorization_selector_declined", StallDurationSeconds: 10_800, Stalled: true, NeedsHumanAttention: true, Class: observability.ClassFault,
				}},
			},
			wantKind:    primitives.KindErr,
			wantVerdict: "Dispatch is stalled.",
		},
		{
			name: "backend capacity outage requires attention",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				BackendOutages: []telemetry.BackendOutage{{
					BackendID: "codex",
					Provider:  "openai",
					ResumeAt:  now.Add(44 * time.Minute),
				}},
			},
			wantKind:    primitives.KindErr,
			wantVerdict: "Backend codex at usage limit.",
		},
		{
			name: "subscription outage names condition",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				BackendOutages: []telemetry.BackendOutage{{
					BackendID: "codex",
					Provider:  "openai",
					Reason:    "subscription window exhausted",
					ResumeAt:  now.Add(44 * time.Minute),
				}},
			},
			wantKind:    primitives.KindErr,
			wantVerdict: "Backend codex: subscription window exhausted.",
		},
		{
			name: "failure breaker requires attention",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				FailureBreakers: []telemetry.FailureBreaker{{
					ProjectID: "detent",
				}},
			},
			wantKind:    primitives.KindErr,
			wantVerdict: "Project failure breaker active — 1 project.",
		},
		{
			name: "waiting recovery stays diagnostic",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				DispatchRecoveries: []telemetry.DispatchRecovery{{
					ProjectID: "detent",
					Kind:      "github_rest",
					Status:    "waiting",
				}},
			},
			wantKind:    primitives.KindOK,
			wantVerdict: "All systems nominal.",
		},
		{
			name: "stranded active issue requires attention",
			snapshot: telemetry.Snapshot{
				GeneratedAt: now,
				StrandedActiveIssues: []telemetry.StrandedIssue{{
					ProjectID: "detent", Identifier: "digitaldrywood/detent#1606", DurationSeconds: 900,
				}},
			},
			wantKind:    primitives.KindErr,
			wantVerdict: "Active work has no live worker.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view := healthViewFromDashboard(DashboardData{Snapshot: tt.snapshot})
			if view.Kind != tt.wantKind {
				t.Fatalf("kind = %q, want %q", view.Kind, tt.wantKind)
			}
			if view.Verdict != tt.wantVerdict {
				t.Fatalf("verdict = %q, want %q", view.Verdict, tt.wantVerdict)
			}
			if !view.CheckedAt.Equal(now) {
				t.Fatalf("checked at = %s", view.CheckedAt)
			}
		})
	}
}

func TestHealthPreTurnInstanceDrain(t *testing.T) {
	t.Parallel()
	for _, class := range []string{"runner_error", "workspace_preparation", "backend_startup_timeout"} {
		t.Run(class, func(t *testing.T) {
			t.Parallel()
			resume := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
			rows := healthFailureBreakerRows([]telemetry.FailureBreaker{{InstanceDrained: true, ProjectID: "detent", Class: class, RepresentativeError: "startup failed", ResumeAt: resume}})
			if len(rows) != 1 || rows[0].Status != "Drained" || rows[0].Component != "Instance · detent" || rows[0].Detail != class+": startup failed" || rows[0].Resets != resume.Format(time.RFC3339) {
				t.Fatalf("health rows = %#v", rows)
			}
		})
	}
}

func TestHealthRowsSurfaceHostPressure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	rows := healthPressureRows(telemetry.Snapshot{
		MemoryPressure: telemetry.MemoryPressure{
			Supported: true, Some: telemetry.PressureAverages{Avg60: 0}, SomeAvg60Max: 10, ObservedAt: now,
		},
		IOPressure: telemetry.IOPressure{
			Supported: true, Some: telemetry.PressureAverages{Avg10: 78.81}, Full: telemetry.PressureAverages{Avg10: 63.64}, FullAvg10Max: 5,
			DegradedMaxConcurrentAgents: 1, EffectiveMaxConcurrentAgents: 1, CapacityConstrained: true,
			ConstrainedSince: now.Add(-5 * time.Minute), ConstrainedForMS: 300000, ObservedAt: now,
		},
		CPUPressure: telemetry.CPUPressure{
			SomeAvg10Max: 80, LastError: "read /proc/pressure/cpu: permission denied", ObservedAt: now,
		},
	})

	if len(rows) != 3 {
		t.Fatalf("pressure rows = %#v, want three", rows)
	}
	if rows[0].Status != "Healthy" || !strings.Contains(rows[0].Detail, "some avg60 0.00% / 10.00%") {
		t.Fatalf("memory row = %#v", rows[0])
	}
	if rows[1].Status != "Limited to 1 agent" || !strings.Contains(rows[1].Detail, "full avg10 63.64% / 5.00%") || !strings.Contains(rows[1].Detail, "some avg10 78.81%") || !strings.Contains(rows[1].Detail, "constrained for 5m0s") {
		t.Fatalf("IO row = %#v", rows[1])
	}
	if rows[2].Status != "Unavailable" || !strings.Contains(rows[2].Detail, "permission denied") {
		t.Fatalf("CPU row = %#v", rows[2])
	}
}

func TestHealthRowsIncludeOnlyFaultStaleness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		warning telemetry.StalenessWarning
		want    bool
	}{
		{
			name: "fault",
			warning: telemetry.StalenessWarning{
				ID: "fault", Class: observability.ClassFault, ProjectID: "detent", Identifier: "digitaldrywood/detent#1960", IssueURL: "https://github.com/digitaldrywood/detent/issues/1960", Detail: "operator action required",
			},
			want: true,
		},
		{name: "diagnostic", warning: telemetry.StalenessWarning{ID: "diagnostic", Class: observability.ClassDiagnostic}},
		{name: "review queue", warning: telemetry.StalenessWarning{ID: "review", Class: observability.ClassReviewQueue, WaitingOnHuman: true}},
		{name: "legacy diagnostic fallback", warning: telemetry.StalenessWarning{ID: "legacy-diagnostic"}},
		{name: "legacy review fallback", warning: telemetry.StalenessWarning{ID: "legacy-review", WaitingOnHuman: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rows := healthStalenessRows(healthFaultStalenessWarnings([]telemetry.StalenessWarning{tt.warning}))
			if got := len(rows) == 1; got != tt.want {
				t.Fatalf("fault row present = %v, want %v: %#v", got, tt.want, rows)
			}
			if !tt.want {
				return
			}
			if rows[0].Kind != primitives.KindErr || rows[0].Status != "Needs attention" || rows[0].Link != tt.warning.IssueURL {
				t.Fatalf("fault row = %#v", rows[0])
			}
		})
	}
}

func TestHealthTrackerProviderStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     telemetry.ProviderStatus
		wantDetail string
		wantLink   string
	}{
		{
			name: "corroborating incident",
			status: telemetry.ProviderStatus{
				Provider: "GitHub",
				State:    telemetry.ProviderStatusCorroborated,
				Incident: &telemetry.ProviderIncident{
					Name:       "GitHub service disruption",
					URL:        "https://stspg.io/example",
					Status:     "mitigating",
					Components: []string{"API Requests", "Issues"},
				},
			},
			wantDetail: "GitHub incident affecting API Requests and Issues — mitigating",
			wantLink:   "https://stspg.io/example",
		},
		{name: "no incident", status: telemetry.ProviderStatus{Provider: "GitHub", State: telemetry.ProviderStatusNoMatch}, wantDetail: "no matching provider incident"},
		{name: "unreachable provider", status: telemetry.ProviderStatus{Provider: "GitHub", State: telemetry.ProviderStatusUnavailable}, wantDetail: "provider status unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			condition := telemetry.TrackerCondition{ProjectID: "detent", Connector: "github", ErrorClass: "server", ProviderStatus: &tt.status}
			rows := healthTrackerUnavailableRows([]telemetry.TrackerCondition{condition})
			if len(rows) != 1 || !strings.Contains(rows[0].Detail, tt.wantDetail) || rows[0].Link != tt.wantLink {
				t.Fatalf("healthTrackerUnavailableRows() = %#v, want detail %q and link %q", rows, tt.wantDetail, tt.wantLink)
			}
			if detail := trackerUnavailableHealthDetail([]telemetry.TrackerCondition{condition}); !strings.Contains(detail, tt.wantDetail) {
				t.Fatalf("trackerUnavailableHealthDetail() = %q, want containing %q", detail, tt.wantDetail)
			}
		})
	}
}

func TestHealthDispatchStallRows(t *testing.T) {
	t.Parallel()

	rows := healthRows(telemetry.Snapshot{DispatchStalls: []telemetry.DispatchStatus{{
		ProjectID: "detent", CandidateCount: 8, WaitReason: "authorization selector excludes every candidate", WaitReasonCode: "authorization_selector_declined", StallDurationSeconds: 10_800, Stalled: true,
	}}})
	var got healthRow
	for _, row := range rows {
		if row.ID == "health-dispatch-stall-detent" {
			got = row
			break
		}
	}
	if got.Kind != primitives.KindErr || got.Status != "Needs attention" || !strings.Contains(got.Detail, "8 candidates skipped for 3h") || !strings.Contains(got.Detail, "authorization selector") {
		t.Fatalf("dispatch stall row = %#v", got)
	}
}

func TestHealthCopyPayload(t *testing.T) {
	t.Parallel()

	checkedAt := time.Date(2026, 8, 7, 22, 21, 52, 0, time.FixedZone("CDT", -5*60*60))
	detailAt := time.Date(2026, 8, 7, 20, 15, 0, 0, time.FixedZone("CDT", -5*60*60))
	resetAt := time.Date(2026, 8, 7, 23, 0, 47, 0, time.FixedZone("CDT", -5*60*60))
	tests := []struct {
		name string
		view healthView
		want string
	}{
		{
			name: "no rows",
			view: healthView{
				Verdict: "Waiting for the first health snapshot.",
				Detail:  "No signals have been reported.",
			},
			want: "Detent health — 0 signals — checked unavailable\n" +
				"Waiting for the first health snapshot. No signals have been reported.",
		},
		{
			name: "warning rows only",
			view: healthView{
				Verdict:   "Fleet work is stale.",
				Detail:    "2 warnings need operator attention.",
				CheckedAt: checkedAt,
				Rows: []healthRow{
					{Component: "Fleet staleness · detent", Kind: primitives.KindWarn, Status: "Stale", Detail: "digitaldrywood/detent#1651 · repeated decision · 1h21m", Resets: "on progress"},
					{Component: "Fleet staleness · detent", Kind: primitives.KindWarn, Status: "Reminder due", Detail: "digitaldrywood/detent#1650 · waiting for operator", Resets: "on progress"},
				},
			},
			want: "Detent health — 2 signals — checked 2026-08-08T03:21:52Z\n" +
				"Fleet work is stale. 2 warnings need operator attention.\n\n" +
				"[WARN] Fleet staleness · detent | Stale | digitaldrywood/detent#1651 · repeated decision · 1h21m | resets on progress\n" +
				"[WARN] Fleet staleness · detent | Reminder due | digitaldrywood/detent#1650 · waiting for operator | resets on progress",
		},
		{
			name: "mixed warning healthy and quota rows",
			view: healthView{
				Verdict:   "GitHub API pressure detected.",
				Detail:    "REST requests are approaching the limit; next reset " + localTimeToken(resetAt, LocalTimeOnly) + ".",
				CheckedAt: checkedAt,
				Rows: []healthRow{
					{Component: "GitHub REST", Kind: primitives.KindWarn, Status: "Backoff", Detail: "Requests in backoff", Quota: "4,795 / 5,000", ResetAt: resetAt},
					{Component: "Scheduler", Kind: primitives.KindOK, Status: "Running", Detail: "2 active sessions", Resets: "—"},
				},
			},
			want: "Detent health — 2 signals — checked 2026-08-08T03:21:52Z\n" +
				"GitHub API pressure detected. REST requests are approaching the limit; next reset 2026-08-08T04:00:47Z.\n\n" +
				"[WARN] GitHub REST | Backoff | Requests in backoff | 4,795 / 5,000 | resets 2026-08-08T04:00:47Z\n" +
				"[OK]   Scheduler | Running | 2 active sessions | resets —",
		},
		{
			name: "detail timestamp and zero-value timestamps",
			view: healthView{
				Verdict:   "All systems nominal.",
				Detail:    "Signals are current.",
				CheckedAt: checkedAt,
				Rows: []healthRow{
					{Component: "Detent update", Kind: primitives.KindNeutral, Status: "Scheduled", Detail: "Last check", DetailAt: detailAt},
					{Component: "Backoff", Kind: primitives.KindOK, Status: "None", Detail: "No endpoints in backoff"},
				},
			},
			want: "Detent health — 2 signals — checked 2026-08-08T03:21:52Z\n" +
				"All systems nominal. Signals are current.\n\n" +
				"[INFO] Detent update | Scheduled | Last check · observed 2026-08-08T01:15:00Z | resets —\n" +
				"[OK]   Backoff | None | No endpoints in backoff | resets —",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := healthCopyPayload(tt.view); got != tt.want {
				t.Fatalf("healthCopyPayload() =\n%q\nwant\n%q", got, tt.want)
			}
		})
	}
}

func TestHealthStrandedActiveRowsGroupByProject(t *testing.T) {
	t.Parallel()

	rows := healthStrandedActiveRows([]telemetry.StrandedIssue{
		{ProjectID: "pyroapex", Identifier: "digitaldrywood/pyroapex#10", DurationSeconds: 1200},
		{ProjectID: "detent", Identifier: "digitaldrywood/detent#1607", DurationSeconds: 720, LastRefusalReason: "budget cooldown"},
		{ProjectID: "detent", Identifier: "digitaldrywood/detent#1606", DurationSeconds: 900, LastRefusalReason: "priority reservation"},
	})

	if len(rows) != 2 {
		t.Fatalf("healthStrandedActiveRows() = %#v, want two project rows", rows)
	}
	if rows[0].Component != "Active work · detent" || rows[0].Status != "No live worker" {
		t.Fatalf("detent row = %#v", rows[0])
	}
	for _, want := range []string{"digitaldrywood/detent#1606", "15m", "priority reservation", "digitaldrywood/detent#1607", "12m", "budget cooldown"} {
		if !strings.Contains(rows[0].Detail, want) {
			t.Fatalf("detent detail = %q, want %q", rows[0].Detail, want)
		}
	}
	if rows[1].Component != "Active work · pyroapex" || !strings.Contains(rows[1].Detail, "none recorded") {
		t.Fatalf("pyroapex row = %#v", rows[1])
	}
}

func TestHealthAdmissionProposalRowsGroupByProject(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	rows := healthAdmissionProposalRows([]telemetry.AdmissionProposal{
		{ProjectID: "docs", IssueIdentifier: "digitaldrywood/docs#20", Confidence: 0.91, CreatedAt: now.Add(-30 * time.Minute), ExpiresAt: now.Add(90 * time.Minute)},
		{ProjectID: "detent", IssueIdentifier: "digitaldrywood/detent#1587", Confidence: 0.76, CreatedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(22 * time.Hour)},
		{ProjectID: "detent", IssueIdentifier: "digitaldrywood/detent#1586", Confidence: 0.88, CreatedAt: now.Add(-time.Hour), ExpiresAt: now.Add(23 * time.Hour)},
	}, now)

	if len(rows) != 2 {
		t.Fatalf("healthAdmissionProposalRows() = %#v, want two project rows", rows)
	}
	if rows[0].Component != "Admission · detent" || rows[0].Status != "2 awaiting decisions" || rows[0].Resets != "on decision" {
		t.Fatalf("detent row = %#v", rows[0])
	}
	for _, want := range []string{
		"digitaldrywood/detent#1586 · 88% confidence · age 1h 0m · expires in 23h 0m",
		"digitaldrywood/detent#1587 · 76% confidence · age 2h 0m · expires in 22h 0m",
	} {
		if !strings.Contains(rows[0].Detail, want) {
			t.Fatalf("detent detail = %q, want %q", rows[0].Detail, want)
		}
	}
	if rows[1].Component != "Admission · docs" || rows[1].Status != "1 awaiting decision" {
		t.Fatalf("docs row = %#v", rows[1])
	}

	view := healthViewFromDashboard(DashboardData{Snapshot: telemetry.Snapshot{
		GeneratedAt:        now,
		AdmissionProposals: []telemetry.AdmissionProposal{{ProjectID: "detent"}},
	}})
	if view.Kind != primitives.KindOK || view.Verdict != "All systems nominal." {
		t.Fatalf("health verdict = (%q, %q)", view.Kind, view.Verdict)
	}
}

func TestBackendCapacityProjectDetailDoesNotInflateVerdict(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 55, 0, 0, time.UTC)
	resumeAt := now.Add(44 * time.Minute)
	snapshot := telemetry.Snapshot{
		GeneratedAt: now,
		BackendOutages: []telemetry.BackendOutage{
			{ProjectID: "detent", BackendID: "codex", Provider: "openai", ResumeAt: resumeAt},
			{ProjectID: "docs", BackendID: "codex", Provider: "openai", ResumeAt: resumeAt},
		},
	}
	view := healthViewFromDashboard(DashboardData{Snapshot: snapshot})
	if view.Verdict != "Backend codex at usage limit." {
		t.Fatalf("verdict = %q, want one backend outage", view.Verdict)
	}
	summaries := boardBackendCapacitySummaries(snapshot.BackendOutages, time.Time{})
	if len(summaries) != 1 || summaries[0].Title != "Backend codex at usage limit — 2 projects" {
		t.Fatalf("Board summaries = %#v", summaries)
	}
	html := renderBoardComponent(t, HealthSnapshotV2(DashboardData{Snapshot: snapshot}))
	for _, project := range []string{"Project detent", "Project docs"} {
		if !strings.Contains(html, project) {
			t.Fatalf("Health detail missing %q:\n%s", project, html)
		}
	}
}

func TestHealthRowsIncludeBackendCapacityOutage(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 55, 0, 0, time.UTC)
	rows := healthRows(telemetry.Snapshot{
		GeneratedAt: now,
		BackendOutages: []telemetry.BackendOutage{{
			BackendID: "codex",
			Provider:  "openai",
			ResumeAt:  now.Add(44 * time.Minute),
		}},
	})
	row := rows[len(rows)-1]
	if row.Component != "Backend codex" || row.Status != "Usage limit" || !row.ResetAt.Equal(now.Add(44*time.Minute)) {
		t.Fatalf("backend outage row = %+v", row)
	}
}

func TestHealthRowsNameSubscriptionExhaustion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 19, 22, 0, 0, 0, time.UTC)
	rows := healthRows(telemetry.Snapshot{
		GeneratedAt: now,
		BackendOutages: []telemetry.BackendOutage{{
			BackendID: "codex",
			Provider:  "openai",
			Reason:    "subscription window exhausted",
			ResumeAt:  now.Add(44 * time.Minute),
		}},
	})
	row := rows[len(rows)-1]
	if row.Status != "Subscription exhausted" || !strings.Contains(row.Detail, "subscription window exhausted") {
		t.Fatalf("backend outage row = %+v", row)
	}
}

func TestHealthRefreshRowsDegradeAtFailureThreshold(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 17, 14, 0, 0, 0, time.UTC)
	lastSuccess := now.Add(-time.Minute)
	lastErrorAt := now.Add(-10 * time.Second)
	snapshot := telemetry.Snapshot{
		GeneratedAt: now,
		Refresh: telemetry.Refresh{
			StaleAfterSeconds: 120,
			FailureThreshold:  3,
			Sources: []telemetry.RefreshSource{{
				ProjectID:     "detent",
				Name:          telemetry.RefreshSourceCandidates,
				LastSuccessAt: &lastSuccess,
				FailureStreak: 2,
				LastError:     "status 503",
				LastErrorAt:   &lastErrorAt,
			}},
		},
	}

	rows := healthRefreshRows(snapshot)
	if len(rows) != 1 || rows[0].Kind != primitives.KindOK || rows[0].Status != "Current" {
		t.Fatalf("health row before threshold = %#v", rows)
	}
	if strings.Contains(rows[0].Detail, "status 503") {
		t.Fatalf("health row exposed transient error before threshold: %q", rows[0].Detail)
	}

	snapshot.Refresh.Sources[0].FailureStreak = 3
	rows = healthRefreshRows(snapshot)
	if len(rows) != 1 || rows[0].Kind != primitives.KindWarn || rows[0].Status != "Stale" {
		t.Fatalf("health row at threshold = %#v", rows)
	}
	if !strings.Contains(rows[0].Detail, "candidate fetch") || !strings.Contains(rows[0].Detail, "3 consecutive failures") || !strings.Contains(rows[0].Detail, "status 503") {
		t.Fatalf("health row missing failure detail: %q", rows[0].Detail)
	}
	view := healthViewFromDashboard(DashboardData{Snapshot: snapshot})
	if view.Kind != primitives.KindErr || view.Verdict != "Tracker refresh failed." {
		t.Fatalf("health verdict = (%q, %q)", view.Kind, view.Verdict)
	}
}

func TestRefreshFailureDetail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		failure telemetry.RefreshFailure
		want    []string
	}{
		{
			name: "consecutive candidate failures",
			failure: telemetry.RefreshFailure{
				Source:        telemetry.RefreshSourceCandidates,
				FailureStreak: 3,
				Condition:     "GitHub candidate query",
				LastError:     "status 503",
			},
			want: []string{"candidate fetch", "3 consecutive failures", "GitHub candidate query", "status 503"},
		},
		{
			name:    "sourceless project failure",
			failure: telemetry.RefreshFailure{LastError: "runtime unavailable"},
			want:    []string{"runtime unavailable"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := refreshFailureDetail(tt.failure)
			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Fatalf("refreshFailureDetail() = %q, want %q", got, want)
				}
			}
		})
	}
}

func TestFleetFreshnessPreservesSourcelessProjectFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 17, 15, 0, 0, 0, time.UTC)
	lastSuccess := now.Add(-time.Second)
	snapshot := telemetry.Snapshot{
		GeneratedAt: now,
		Refresh: telemetry.Refresh{
			Status: telemetry.RefreshStatusDegraded,
			Sources: []telemetry.RefreshSource{
				{ProjectID: "detent", Name: telemetry.RefreshSourceCandidates, LastSuccessAt: &lastSuccess},
				{ProjectID: "docs", Name: telemetry.RefreshSourceProject, Degraded: true, LastError: "runtime unavailable"},
			},
		},
	}

	if got := refreshFreshnessKind(snapshot); got != primitives.KindErr {
		t.Fatalf("refreshFreshnessKind() = %q, want %q", got, primitives.KindErr)
	}
	rows := diagnosticsConditionRows(snapshot)
	if len(rows) != 1 || rows[0].ProjectID != "docs" || rows[0].Class != observability.ClassFault {
		t.Fatalf("diagnostics refresh rows = %#v", rows)
	}
	if !strings.Contains(rows[0].Detail, "runtime unavailable") {
		t.Fatalf("degraded project detail = %q", rows[0].Detail)
	}
}

func TestHealthBudgetRowsShowEffectiveCapAndOverride(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	cap := 200.0
	rows := healthBudgetRows(DashboardData{
		Snapshot: telemetry.Snapshot{GeneratedAt: now},
		Projects: []ProjectSmallMultiple{{
			ID:               "detent",
			Name:             "Detent",
			BudgetEnabled:    true,
			BudgetObservedAt: now,
			CurrentSpendUSD:  170,
			PerDayMaxUSD:     cap,
			BudgetResetAt:    now.Add(9 * time.Hour),
			BudgetOverride: &telemetry.BudgetOverride{
				ProjectID:    "detent",
				PerDayMaxUSD: &cap,
				ExpiresAt:    now.Add(4 * time.Hour),
				Reason:       "release work",
			},
		}},
	})
	if len(rows) != 1 {
		t.Fatalf("healthBudgetRows() len = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.Kind != primitives.KindNeutral || row.Status != "Approaching limit" || row.QuotaPct != 85 {
		t.Fatalf("budget row = %#v", row)
	}
	if !strings.Contains(row.Detail, "override daily $200.00") || !strings.Contains(row.Detail, "expires in 4h0m0s") || !strings.Contains(row.Detail, "release work") {
		t.Fatalf("budget detail = %q", row.Detail)
	}
}

func TestHealthRows(t *testing.T) {
	resetAt := time.Date(2026, 7, 4, 17, 0, 0, 0, time.UTC)
	snapshot := telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 4, 16, 42, 0, 0, time.UTC),
		Counts:      telemetry.Counts{Running: 2, Queue: 1},
		RateLimits: &telemetry.RateLimits{
			GitHubREST:    &telemetry.RateLimitBucket{Remaining: 822, Limit: 5000, ResetAt: &resetAt},
			GitHubGraphQL: &telemetry.RateLimitBucket{Remaining: 78, Limit: 5000, Status: telemetry.RateLimitStatusBackoff},
		},
	}

	rows := healthRows(snapshot)
	if len(rows) != 2 {
		t.Fatalf("expected scheduler and update rows, got %d", len(rows))
	}
	scheduler := rows[0]
	if scheduler.Status != "Running" || !strings.Contains(scheduler.Detail, "2 active sessions") {
		t.Fatalf("scheduler row = %+v", scheduler)
	}
	if update := rows[1]; update.Status != "Disabled" {
		t.Fatalf("update row = %+v", update)
	}
	if got := gitHubAPIHealth(snapshot); got.State != gitHubAPIHealthStateBackoff {
		t.Fatalf("diagnostics GitHub API state = %q, want backoff", got.State)
	}
}

func TestHealthRowsShowProviderRateWindowPacing(t *testing.T) {
	t.Parallel()
	resetAt := time.Date(2026, 8, 7, 17, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		limits    *telemetry.RateLimits
		wantRows  int
		wantNames []string
	}{
		{
			name: "primary depressed",
			limits: &telemetry.RateLimits{
				Primary: &telemetry.RateLimitBucket{Remaining: 48, Limit: 100, ResetAt: &resetAt},
			},
			wantRows:  1,
			wantNames: []string{"Primary"},
		},
		{
			name: "primary and secondary depressed",
			limits: &telemetry.RateLimits{
				Primary:   &telemetry.RateLimitBucket{Remaining: 80, Limit: 100},
				Secondary: &telemetry.RateLimitBucket{Remaining: 30, Limit: 100},
			},
			wantRows:  2,
			wantNames: []string{"Primary", "Secondary"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rows := healthRows(telemetry.Snapshot{RateLimits: tt.limits})
			if len(rows) != 2 {
				t.Fatalf("health rows = %#v, want only scheduler and update", rows)
			}
			diagnosticRows := rateLimitRows(tt.limits)
			if len(diagnosticRows) != tt.wantRows {
				t.Fatalf("diagnostic provider rows = %#v, want %d", diagnosticRows, tt.wantRows)
			}
			for index, row := range diagnosticRows {
				if row.Name != tt.wantNames[index] {
					t.Fatalf("diagnostic provider row %d = %#v", index, row)
				}
			}
		})
	}
}

func TestHealthRowsExhaustedByRemainingCount(t *testing.T) {
	// Zero remaining with no explicit status must read Exhausted so the
	// details row matches the exhaustion verdict.
	snapshot := telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 4, 16, 42, 0, 0, time.UTC),
		RateLimits: &telemetry.RateLimits{
			GitHubREST: &telemetry.RateLimitBucket{Remaining: 0, Limit: 5000},
		},
	}
	if rows := healthRows(snapshot); len(rows) != 2 {
		t.Fatalf("health rows = %#v, want no rate-limit diagnostic", rows)
	}
	if got := gitHubAPIHealth(snapshot); got.State != gitHubAPIHealthStateExhausted {
		t.Fatalf("diagnostics GitHub API state = %q, want exhausted", got.State)
	}
}

func TestHealthRowsRESTUsageBackoff(t *testing.T) {
	// A secondary REST throttle lives in RESTUsage, not a bucket status; the
	// Backoff row must still surface it.
	backoffUntil := time.Date(2026, 7, 4, 16, 45, 0, 0, time.UTC)
	snapshot := telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 4, 16, 42, 0, 0, time.UTC),
		RateLimits: &telemetry.RateLimits{
			GitHubREST: &telemetry.RateLimitBucket{Remaining: 4000, Limit: 5000},
			RESTUsage:  &telemetry.RESTUsage{RateLimited: true, BackoffUntil: &backoffUntil},
		},
	}
	rows := healthRows(snapshot)
	if len(rows) != 2 {
		t.Fatalf("health rows = %#v, want no REST backoff diagnostic", rows)
	}
	if got := gitHubAPIHealth(snapshot); got.State != gitHubAPIHealthStateBackoff {
		t.Fatalf("diagnostics GitHub API state = %q, want backoff", got.State)
	}
}

func TestHealthRowsIdleWithoutData(t *testing.T) {
	rows := healthRows(telemetry.Snapshot{})
	if len(rows) != 2 {
		t.Fatalf("expected scheduler and update rows, got %d", len(rows))
	}
	if rows[0].Status != "Idle" || rows[1].Status != "Disabled" {
		t.Fatalf("idle rows = %+v", rows)
	}
}

func TestHealthUpdateRowShowsRuntimeStatus(t *testing.T) {
	t.Parallel()

	lastCheck := time.Date(2026, 7, 11, 14, 0, 0, 0, time.UTC)
	nextCheck := lastCheck.Add(12 * time.Hour)
	row := healthUpdateRow(telemetry.Update{
		Enabled:            true,
		AutoApplyEnabled:   true,
		CheckIntervalHours: 12,
		State:              "scheduled",
		LastCheckAt:        &lastCheck,
		LastAppliedVersion: "1.2.4",
		NextCheckAt:        &nextCheck,
	})
	if row.Kind != primitives.KindOK || row.Status != "Scheduled" {
		t.Fatalf("healthUpdateRow() = %+v", row)
	}
	if !strings.Contains(row.Detail, "last applied 1.2.4") || !row.DetailAt.Equal(lastCheck) || !row.ResetAt.Equal(nextCheck) {
		t.Fatalf("healthUpdateRow() status detail = %+v", row)
	}
}

func TestHealthUpdateRowShowsDeferralAge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		age  time.Duration
		want string
	}{
		{name: "hours", age: 4 * time.Hour, want: "pending idle (4h, max 6h)"},
		{name: "minutes", age: 30 * time.Minute, want: "pending idle (30m, max 6h)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 8, 20, 14, 0, 0, 0, time.UTC)
			pendingSince := now.Add(-tt.age)
			row := healthUpdateRowAt(telemetry.Update{
				Enabled:          true,
				AutoApplyEnabled: true,
				State:            "pending_idle",
				PendingSince:     &pendingSince,
				MaxDeferralHours: 6,
			}, now)
			if row.Status != tt.want {
				t.Fatalf("healthUpdateRowAt().Status = %q, want %q", row.Status, tt.want)
			}
		})
	}
}

func TestHealthExhaustedRowDetailNotHealthy(t *testing.T) {
	snapshot := telemetry.Snapshot{
		GeneratedAt: time.Date(2026, 7, 4, 16, 0, 0, 0, time.UTC),
		RateLimits: &telemetry.RateLimits{
			GitHubREST: &telemetry.RateLimitBucket{Remaining: 0, Limit: 5000},
			RESTUsage:  &telemetry.RESTUsage{TotalRequests: 4200},
		},
	}
	if got := gitHubAPIHealth(snapshot); got.State != gitHubAPIHealthStateExhausted || strings.Contains(got.Detail, "Within budget") {
		t.Fatalf("diagnostics GitHub API view = %#v, want exhausted detail", got)
	}
}
