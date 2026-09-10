package templates

import (
	"sort"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/observability"
	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/web/ui/primitives"
)

// healthView renders the whole Health page: one verdict sentence, one
// details table, one footnote. An at-rest system reads as one line.
type healthView struct {
	Kind           primitives.Kind
	Verdict        string
	Detail         string
	DetailAt       time.Time
	DetailRelative bool
	CheckedAt      time.Time
	Rows           []healthRow
	Footnote       string
}

type healthRow struct {
	ID        string
	Component string
	Kind      primitives.Kind
	Status    string
	Detail    string
	Link      string
	Quota     string
	QuotaPct  int
	QuotaWarn bool
	Resets    string
	ResetAt   time.Time
	DetailAt  time.Time
}

func healthViewFromDashboard(data DashboardData) healthView {
	snapshot := data.Snapshot
	dispatchFaults := healthFaultDispatchStalls(snapshot.DispatchStalls)
	stalenessFaults := healthFaultStalenessWarnings(snapshot.StalenessWarnings)
	backendFaults := healthFaultBackendOutages(snapshot.BackendOutages)
	breakerFaults := actionableBoardFailureBreakers(snapshot.FailureBreakers)
	recoveryFaults := healthFaultDispatchRecoveries(snapshot.DispatchRecoveries, snapshot.GeneratedAt)
	view := healthView{
		CheckedAt: snapshot.GeneratedAt,
		Footnote:  "Project budget bars warn at 80% and turn red at the cap.",
		Rows:      append(append(healthRows(snapshot), healthActiveHoursRows(data)...), healthBudgetRows(data)...),
	}
	if len(dispatchFaults) > 0 {
		view.Kind = primitives.KindErr
		view.Verdict = "Dispatch is stalled."
		view.Detail = boardCountLabel(len(dispatchFaults), "Project needs", "Projects need") + " human attention."
		return view
	}
	if len(snapshot.TrackerUnavailable) > 0 {
		view.Kind = primitives.KindErr
		view.Verdict = "Tracker is unavailable."
		view.Detail = trackerUnavailableHealthDetail(snapshot.TrackerUnavailable)
		return view
	}
	if len(snapshot.ForgeUnavailable) > 0 {
		view.Kind = primitives.KindErr
		view.Verdict = "Forge writes are unavailable."
		view.Detail = forgeUnavailableHealthDetail(snapshot.ForgeUnavailable)
		return view
	}
	if len(snapshot.CIUnavailable) > 0 {
		view.Kind = primitives.KindErr
		view.Verdict = "CI is unavailable."
		view.Detail = ciUnavailableHealthDetail(snapshot.CIUnavailable)
		return view
	}
	if len(stalenessFaults) > 0 {
		view.Kind = primitives.KindErr
		view.Verdict = "Fleet work needs attention."
		view.Detail = stalenessFaultHealthDetail(stalenessFaults)
		return view
	}
	if len(snapshot.StrandedActiveIssues) > 0 {
		view.Kind = primitives.KindErr
		view.Verdict = "Active work has no live worker."
		view.Detail = strandedActiveHealthDetail(snapshot.StrandedActiveIssues)
		return view
	}
	if len(recoveryFaults) > 0 {
		view.Kind = primitives.KindErr
		view.Verdict = "Dispatch recovery is overdue."
		view.Detail = boardCountLabel(len(recoveryFaults), "Project needs", "Projects need") + " operator attention."
		return view
	}
	if len(backendFaults) > 0 {
		view.Kind = primitives.KindErr
		view.Verdict = backendCapacityHealthVerdict(snapshot)
		view.Detail, view.DetailAt, view.DetailRelative = backendCapacityOutageDetailParts(backendFaults[0], snapshot.GeneratedAt)
		return view
	}
	if detail := hostPressureHoldDetail(snapshot); detail != "" {
		view.Kind = primitives.KindWarn
		view.Verdict = "Dispatch is constrained by host pressure."
		if snapshot.MemoryPressure.DispatchHeld || snapshot.IOPressure.DispatchHeld || snapshot.CPUPressure.DispatchHeld {
			view.Verdict = "Dispatch is waiting for host pressure."
		}
		view.Detail = detail
		return view
	}
	if refreshSnapshotPartiallyFailed(snapshot) {
		view.Kind = primitives.KindWarn
		view.Verdict = "Tracker refresh is partially degraded."
		view.Detail = refreshPartialFailureSummary(snapshot) + ". Healthy project data remains live."
		return view
	}
	if refreshSnapshotFailed(snapshot) {
		view.Kind = primitives.KindErr
		view.Verdict = "Tracker refresh failed."
		view.Detail = "Live tracker state cannot be refreshed; the board is using last-known data."
		return view
	}
	if detail := strings.TrimSpace(snapshot.Update.LastError); detail != "" {
		view.Kind = primitives.KindErr
		view.Verdict = "Automatic update failed."
		view.Detail = detail
		return view
	}
	if summary, ok := boardFailureBreakerSummary(breakerFaults); ok {
		view.Kind = primitives.KindErr
		view.Verdict = summary.Title + "."
		view.Detail = "Dispatch resumes through a canary after the configured cooldown."
		return view
	}
	if !diagnosticsSnapshotHasLoadedData(snapshot) {
		view.Kind = primitives.KindNeutral
		view.Verdict = "Waiting for the first health snapshot."
		view.Detail = "No live telemetry is available yet."
		return view
	}
	view.Kind = primitives.KindOK
	view.Verdict = "All systems nominal."
	view.Detail = "No fault-class conditions need operator attention."
	return view
}

func healthFaultSnapshot(snapshot telemetry.Snapshot) telemetry.Snapshot {
	snapshot.FailureBreakers = actionableBoardFailureBreakers(snapshot.FailureBreakers)
	snapshot.StalenessWarnings = healthFaultStalenessWarnings(snapshot.StalenessWarnings)
	backendOutages := make([]telemetry.BackendOutage, 0, len(snapshot.BackendOutages))
	for _, outage := range snapshot.BackendOutages {
		if observability.BackendOutage(outage.Kind) == observability.ClassFault {
			backendOutages = append(backendOutages, outage)
		}
	}
	snapshot.BackendOutages = backendOutages
	snapshot.DispatchRecoveries = healthFaultDispatchRecoveries(snapshot.DispatchRecoveries, snapshot.GeneratedAt)
	return snapshot
}

func healthActiveHoursRows(data DashboardData) []healthRow {
	rows := make([]healthRow, 0, len(data.Projects))
	for _, project := range data.Projects {
		active := project.ActiveHours
		if !active.Configured {
			continue
		}
		row := healthRow{
			ID:        "health-active-hours-" + boardCardSlug(project.ID),
			Component: "Active hours · " + projectSmallMultipleName(project),
			Kind:      primitives.KindOK,
		}
		switch {
		case active.OverrideActive:
			row.Status = "Override"
			row.Detail = "Dispatch allowed outside the recurring window"
			row.Resets = "at override expiry"
			if active.OverrideUntil != nil {
				row.ResetAt = *active.OverrideUntil
				row.Detail += " until " + projectActiveHoursTime(active.OverrideUntil, active.Timezone, "Mon, Jan 2 at 15:04 MST")
			}
		case active.WindowOpen:
			row.Status = "Open"
			row.Detail = "Dispatch admission is open in " + active.Timezone
			row.Resets = "at window close"
			if active.NextClose != nil {
				row.ResetAt = *active.NextClose
				row.Detail += " until " + projectActiveHoursTime(active.NextClose, active.Timezone, "Mon, Jan 2 at 15:04 MST")
			}
		default:
			row.Status = "Off hours"
			row.Detail = "New dispatches are waiting for the next window in " + active.Timezone
			row.Resets = "at window open"
			if active.NextOpen != nil {
				row.ResetAt = *active.NextOpen
				row.Detail += " at " + projectActiveHoursTime(active.NextOpen, active.Timezone, "Mon, Jan 2 at 15:04 MST")
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func healthBudgetRows(data DashboardData) []healthRow {
	rows := make([]healthRow, 0, len(data.Projects))
	for _, project := range data.Projects {
		if !project.BudgetEnabled || project.PerDayMaxUSD <= 0 {
			continue
		}
		percent := int(project.CurrentSpendUSD / project.PerDayMaxUSD * 100)
		if percent < 0 {
			percent = 0
		}
		kind := primitives.KindOK
		status := "Healthy"
		if percent >= 100 {
			kind = primitives.KindErr
			status = "At limit"
		} else if percent >= 80 {
			kind = primitives.KindNeutral
			status = "Approaching limit"
		}
		detail := formatUSD(project.CurrentSpendUSD) + " notional USD today"
		if project.BudgetOverride != nil {
			observedAt := project.BudgetObservedAt
			if observedAt.IsZero() {
				observedAt = data.Snapshot.GeneratedAt
			}
			remaining := project.BudgetOverride.ExpiresAt.Sub(observedAt).Round(time.Second)
			if remaining < 0 {
				remaining = 0
			}
			detail += " · override daily " + optionalUSD(project.BudgetOverride.PerDayMaxUSD) + " / issue " + optionalUSD(project.BudgetOverride.PerIssueMaxUSD) + " expires in " + remaining.String() + " at " + project.BudgetOverride.ExpiresAt.Format(time.RFC3339) + " · " + project.BudgetOverride.Reason
		}
		rows = append(rows, healthRow{
			ID:        "health-budget-" + boardCardSlug(project.ID),
			Component: "Budget · " + projectSmallMultipleName(project),
			Kind:      kind,
			Status:    status,
			Detail:    detail,
			Quota:     formatUSD(project.CurrentSpendUSD) + " / " + formatUSD(project.PerDayMaxUSD),
			QuotaPct:  percent,
			QuotaWarn: percent >= 80,
			ResetAt:   project.BudgetResetAt,
		})
	}
	return rows
}

func healthRows(snapshot telemetry.Snapshot) []healthRow {
	rows := make([]healthRow, 0, 4)
	rows = append(rows, healthPressureRows(snapshot)...)
	for _, stall := range healthFaultDispatchStalls(snapshot.DispatchStalls) {
		rows = append(rows, healthDispatchStallRow(stall))
	}
	rows = append(rows, healthTrackerUnavailableRows(snapshot.TrackerUnavailable)...)
	rows = append(rows, healthForgeUnavailableRows(snapshot.ForgeUnavailable)...)
	rows = append(rows, healthCIUnavailableRows(snapshot.CIUnavailable)...)
	rows = append(rows, healthStalenessRows(healthFaultStalenessWarnings(snapshot.StalenessWarnings))...)
	rows = append(rows, healthStrandedActiveRows(snapshot.StrandedActiveIssues)...)
	rows = append(rows, healthDispatchRecoveryRows(healthFaultDispatchRecoveries(snapshot.DispatchRecoveries, snapshot.GeneratedAt), snapshot.GeneratedAt)...)
	rows = append(rows, healthRefreshFailureRows(snapshot.RefreshFailures())...)
	rows = append(rows, healthFailureBreakerRows(actionableBoardFailureBreakers(snapshot.FailureBreakers))...)
	rows = append(rows, healthSchedulerRow(snapshot), healthUpdateRowAt(snapshot.Update, snapshot.GeneratedAt))
	for _, release := range healthReleases(snapshot) {
		rows = append(rows, healthReleaseRow(release))
	}
	for _, outage := range healthFaultBackendOutages(snapshot.BackendOutages) {
		rows = append(rows, healthBackendOutageRow(outage, snapshot.GeneratedAt))
	}
	return rows
}

func hostPressureHoldDetail(snapshot telemetry.Snapshot) string {
	constraints := make([]string, 0, 3)
	if snapshot.MemoryPressure.DispatchHeld {
		constraints = append(constraints, "memory some avg60 "+percentLabel(snapshot.MemoryPressure.Some.Avg60)+" holds dispatch")
	}
	if pressure := snapshot.IOPressure; pressure.CapacityConstrained || pressure.DispatchHeld {
		constraints = append(constraints, hostPressureCapacityDetail(
			"IO full avg10 "+percentLabel(pressure.Full.Avg10),
			pressure.DispatchHeld,
			pressure.EffectiveMaxConcurrentAgents,
			pressure.ConstrainedForMS,
		))
	}
	if pressure := snapshot.CPUPressure; pressure.CapacityConstrained || pressure.DispatchHeld {
		constraints = append(constraints, hostPressureCapacityDetail(
			"CPU some avg10 "+percentLabel(pressure.Some.Avg10),
			pressure.DispatchHeld,
			pressure.EffectiveMaxConcurrentAgents,
			pressure.ConstrainedForMS,
		))
	}
	if len(constraints) == 0 {
		return ""
	}
	return strings.Join(constraints, ", ") + "."
}

func hostPressureCapacityDetail(metric string, held bool, capacity int, constrainedForMS int64) string {
	detail := metric
	if held {
		detail += " holds dispatch"
	} else {
		detail += " limits admission to " + boardCountLabel(capacity, "concurrent agent", "concurrent agents")
	}
	if constrainedForMS > 0 {
		detail += " for " + (time.Duration(constrainedForMS) * time.Millisecond).Round(time.Second).String()
	}
	return detail
}

func healthPressureRows(snapshot telemetry.Snapshot) []healthRow {
	rows := make([]healthRow, 0, 3)
	if pressure := snapshot.MemoryPressure; pressure.SomeAvg60Max > 0 {
		rows = append(rows, healthPressureRow(
			"memory",
			pressure.Supported,
			pressure.DispatchHeld,
			pressure.DispatchHeld,
			0,
			0,
			false,
			pressure.LastError,
			pressure.ObservedAt,
			"some avg60",
			pressure.Some.Avg60,
			pressure.SomeAvg60Max,
			"",
		))
	}
	if pressure := snapshot.IOPressure; pressure.FullAvg10Max > 0 {
		rows = append(rows, healthPressureRow(
			"IO",
			pressure.Supported,
			pressure.DispatchHeld,
			pressure.CapacityConstrained,
			pressure.EffectiveMaxConcurrentAgents,
			pressure.ConstrainedForMS,
			true,
			pressure.LastError,
			pressure.ObservedAt,
			"full avg10",
			pressure.Full.Avg10,
			pressure.FullAvg10Max,
			"some avg10 "+percentLabel(pressure.Some.Avg10),
		))
	}
	if pressure := snapshot.CPUPressure; pressure.SomeAvg10Max > 0 {
		rows = append(rows, healthPressureRow(
			"CPU",
			pressure.Supported,
			pressure.DispatchHeld,
			pressure.CapacityConstrained,
			pressure.EffectiveMaxConcurrentAgents,
			pressure.ConstrainedForMS,
			true,
			pressure.LastError,
			pressure.ObservedAt,
			"some avg10",
			pressure.Some.Avg10,
			pressure.SomeAvg10Max,
			"",
		))
	}
	return rows
}

func healthPressureRow(
	resource string,
	supported bool,
	held bool,
	constrained bool,
	effectiveCapacity int,
	constrainedForMS int64,
	hysteresis bool,
	lastError string,
	observedAt time.Time,
	metric string,
	current float64,
	threshold float64,
	companion string,
) healthRow {
	row := healthRow{
		ID:        "health-host-pressure-" + strings.ToLower(resource),
		Component: "Host pressure · " + resource,
		Kind:      primitives.KindOK,
		Status:    "Healthy",
		Detail:    metric + " " + percentLabel(current) + " / " + percentLabel(threshold) + " threshold",
		Resets:    "continuous",
		DetailAt:  observedAt,
	}
	if companion != "" {
		row.Detail += " · " + companion
	}
	switch {
	case strings.TrimSpace(lastError) != "":
		row.Kind = primitives.KindWarn
		row.Status = "Unavailable"
		row.Detail = strings.TrimSpace(lastError)
		row.Resets = "on successful read"
	case !supported:
		row.Kind = primitives.KindNeutral
		row.Status = "Unsupported"
		row.Detail = "Linux PSI is unavailable on this host"
		row.Resets = "—"
	case constrained || held:
		row.Kind = primitives.KindWarn
		row.Status = "Holding dispatch"
		if !held {
			row.Status = "Limited to " + boardCountLabel(effectiveCapacity, "agent", "agents")
		}
		if constrainedForMS > 0 {
			row.Detail += " · constrained for " + (time.Duration(constrainedForMS) * time.Millisecond).Round(time.Second).String()
		}
		row.Resets = "when pressure falls"
		if hysteresis {
			row.Resets = "at 80% of threshold"
		}
	}
	return row
}

func healthFaultStalenessWarnings(warnings []telemetry.StalenessWarning) []telemetry.StalenessWarning {
	faults := make([]telemetry.StalenessWarning, 0, len(warnings))
	for _, warning := range warnings {
		if stalenessConditionClass(warning) == observability.ClassFault {
			faults = append(faults, warning)
		}
	}
	return faults
}

func healthStalenessRows(warnings []telemetry.StalenessWarning) []healthRow {
	rows := make([]healthRow, 0, len(warnings))
	for _, warning := range warnings {
		component := "Fleet fault"
		if projectID := strings.TrimSpace(warning.ProjectID); projectID != "" {
			component += " · " + projectID
		}
		detail := stalenessHealthTarget(warning) + " · " + strings.TrimSpace(warning.Detail)
		if warning.AgeSeconds > 0 {
			detail += " · " + formatDuration(float64(warning.AgeSeconds))
		}
		rows = append(rows, healthRow{
			ID:        "health-staleness-" + boardCardSlug(warning.ID),
			Component: component,
			Kind:      primitives.KindErr,
			Status:    "Needs attention",
			Detail:    detail,
			Link:      strings.TrimSpace(warning.IssueURL),
			Resets:    "operator action",
		})
	}
	return rows
}

func stalenessFaultHealthDetail(warnings []telemetry.StalenessWarning) string {
	if len(warnings) == 1 {
		return stalenessHealthTarget(warnings[0]) + " needs operator attention."
	}
	return formatCount(len(warnings)) + " fault-class conditions need operator attention."
}

func stalenessHealthTarget(warning telemetry.StalenessWarning) string {
	for _, value := range []string{warning.Identifier, warning.IssueID, warning.ProjectID} {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return "fleet"
}

func healthFaultDispatchRecoveries(recoveries []telemetry.DispatchRecovery, observedAt time.Time) []telemetry.DispatchRecovery {
	faults := make([]telemetry.DispatchRecovery, 0, len(recoveries))
	for _, recovery := range recoveries {
		if observability.DispatchRecovery(recovery.Status, recovery.ResumeAt, observedAt) == observability.ClassFault {
			faults = append(faults, recovery)
		}
	}
	return faults
}

func healthDispatchRecoveryRows(recoveries []telemetry.DispatchRecovery, observedAt time.Time) []healthRow {
	rows := make([]healthRow, 0, len(recoveries))
	for index, recovery := range recoveries {
		detail, detailAt, _ := dispatchRecoveryDetailParts(recovery, observedAt)
		rows = append(rows, healthRow{
			ID:        "health-dispatch-recovery-" + boardAlertRowSlug(recovery.ProjectID+recovery.Kind, index),
			Component: "Dispatch recovery · " + diagnosticsConditionProject(recovery.ProjectID),
			Kind:      primitives.KindErr,
			Status:    "Overdue",
			Detail:    detail,
			ResetAt:   detailAt,
		})
	}
	return rows
}

func healthRefreshFailureRows(failures []telemetry.RefreshFailure) []healthRow {
	rows := make([]healthRow, 0, len(failures))
	for index, failure := range failures {
		projectID := diagnosticsConditionProject(failure.ProjectID)
		rows = append(rows, healthRow{
			ID:        "health-refresh-failure-" + boardAlertRowSlug(failure.ProjectID+string(failure.Source), index),
			Component: "Tracker refresh · " + projectID,
			Kind:      primitives.KindErr,
			Status:    "Failed",
			Detail:    refreshFailureDetail(failure),
			Resets:    "on successful refresh",
			DetailAt:  diagnosticsConditionObservedAt(failure.LastErrorAt, time.Time{}),
		})
	}
	return rows
}

func healthFaultDispatchStalls(stalls []telemetry.DispatchStatus) []telemetry.DispatchStatus {
	faults := make([]telemetry.DispatchStatus, 0, len(stalls))
	for _, stall := range stalls {
		if dispatchConditionClass(stall) == observability.ClassFault {
			faults = append(faults, stall)
		}
	}
	return faults
}

func healthFaultBackendOutages(outages []telemetry.BackendOutage) []telemetry.BackendOutage {
	faults := make([]telemetry.BackendOutage, 0, len(outages))
	for _, outage := range outages {
		if observability.BackendOutage(outage.Kind) == observability.ClassFault {
			faults = append(faults, outage)
		}
	}
	return backendCapacityOutages(faults)
}

func healthFailureBreakerRows(breakers []telemetry.FailureBreaker) []healthRow {
	rows := make([]healthRow, 0, len(breakers))
	for index, breaker := range breakers {
		projectID := diagnosticsConditionProject(breaker.ProjectID)
		row := healthRow{
			ID:        "health-failure-breaker-" + boardAlertRowSlug(projectID+breaker.Class, index),
			Component: "Failure breaker · " + projectID,
			Kind:      primitives.KindErr,
			Status:    "Needs attention",
			Detail:    failureBreakerCauseLabel(breaker),
			Resets:    "operator action",
		}
		if breaker.InstanceDrained {
			row.Component = "Instance · " + projectID
			row.Status = "Drained"
			row.Detail = breaker.Class + ": " + breaker.RepresentativeError
			row.Resets = breaker.ResumeAt.UTC().Format(time.RFC3339)
		}
		rows = append(rows, row)
	}
	return rows
}

func healthDispatchStallRow(stall telemetry.DispatchStatus) healthRow {
	projectID := strings.TrimSpace(stall.ProjectID)
	if projectID == "" {
		projectID = "Fleet"
	}
	detail := boardCountLabel(stall.CandidateCount, "candidate", "candidates") + " skipped for " + formatDuration(float64(stall.StallDurationSeconds))
	if waitReason := strings.TrimSpace(stall.WaitReason); waitReason != "" {
		detail += " · " + waitReason
	}
	return healthRow{
		ID:        "health-dispatch-stall-" + boardCardSlug(projectID),
		Component: "Dispatch · " + projectID,
		Kind:      primitives.KindErr,
		Status:    "Needs attention",
		Detail:    detail,
		Resets:    "operator action",
	}
}

func healthCIUnavailableRows(conditions []telemetry.CICondition) []healthRow {
	rows := make([]healthRow, 0, len(conditions))
	for index, condition := range conditions {
		projectID := strings.TrimSpace(condition.ProjectID)
		if projectID == "" {
			projectID = "project"
		}
		rows = append(rows, healthRow{
			ID:        "health-ci-unavailable-" + boardAlertRowSlug(projectID, index),
			Component: "CI · " + projectID,
			Kind:      primitives.KindErr,
			Status:    "Unavailable",
			Detail: boardCountLabel(condition.UnstartedCheckCount, "queued check", "queued checks") +
				" never started " + ciUnavailableConditionDetail(condition),
			Resets:   "when checks start",
			DetailAt: condition.LastObservedAt,
		})
	}
	return rows
}

func healthTrackerUnavailableRows(conditions []telemetry.TrackerCondition) []healthRow {
	rows := make([]healthRow, 0, len(conditions))
	for index, condition := range conditions {
		projectID := strings.TrimSpace(condition.ProjectID)
		if projectID == "" {
			projectID = "project"
		}
		connectorName := strings.TrimSpace(condition.Connector)
		if connectorName == "" {
			connectorName = "Tracker"
		}
		detail := "tracker_unavailable · " + strings.Trim(strings.TrimSpace(condition.Operation)+" · "+strings.TrimSpace(condition.ErrorClass), " ·")
		link := ""
		if providerSummary, providerLink := trackerProviderStatusSummary(condition); providerSummary != "" {
			detail = providerSummary
			link = providerLink
			if condition.ProviderStatus != nil && condition.ProviderStatus.Incident != nil {
				detail += " · " + condition.ProviderStatus.Incident.Name
			}
		}
		rows = append(rows, healthRow{
			ID:        "health-tracker-unavailable-" + boardAlertRowSlug(projectID, index),
			Component: connectorName + " tracker · " + projectID,
			Kind:      primitives.KindErr,
			Status:    "Unavailable",
			Detail:    detail,
			Link:      link,
			Resets:    "on successful canary",
			ResetAt:   condition.NextProbeAt,
			DetailAt:  condition.LastObservedAt,
		})
	}
	return rows
}

func healthForgeUnavailableRows(conditions []telemetry.ForgeCondition) []healthRow {
	rows := make([]healthRow, 0, len(conditions))
	for index, condition := range conditions {
		projectID := strings.TrimSpace(condition.ProjectID)
		if projectID == "" {
			projectID = "project"
		}
		host := strings.TrimSpace(condition.Host)
		if host == "" {
			host = "configured forge"
		}
		detail := "forge_unavailable · " + strings.Trim(strings.TrimSpace(condition.Operation)+" · "+strings.TrimSpace(condition.ErrorClass), " ·")
		rows = append(rows, healthRow{
			ID:        "health-forge-unavailable-" + boardAlertRowSlug(projectID+"-"+host, index),
			Component: "Forge writes · " + host + " · " + projectID,
			Kind:      primitives.KindErr,
			Status:    "Unavailable",
			Detail:    detail,
			Resets:    "on successful write canary",
			ResetAt:   condition.NextProbeAt,
			DetailAt:  condition.LastObservedAt,
		})
	}
	return rows
}

func trackerUnavailableHealthDetail(conditions []telemetry.TrackerCondition) string {
	if len(conditions) == 1 {
		condition := conditions[0]
		if providerSummary, _ := trackerProviderStatusSummary(condition); providerSummary != "" {
			return providerSummary + "; tracker-dependent dispatch is paused."
		}
		connectorName := strings.TrimSpace(condition.Connector)
		if connectorName == "" {
			connectorName = "Configured"
		}
		return connectorName + " tracker reads are unavailable; tracker-dependent dispatch is paused."
	}
	return boardCountLabel(len(conditions), "tracker connector is", "tracker connectors are") + " unavailable; tracker-dependent dispatch is paused."
}

func trackerProviderStatusSummary(condition telemetry.TrackerCondition) (string, string) {
	status := condition.ProviderStatus
	if status == nil {
		return "", ""
	}
	switch status.State {
	case telemetry.ProviderStatusCorroborated:
		if status.Incident == nil {
			return "provider status corroborated the outage", ""
		}
		provider := strings.TrimSpace(status.Provider)
		if provider == "" {
			provider = "Provider"
		}
		summary := provider + " incident"
		if len(status.Incident.Components) > 0 {
			summary += " affecting " + healthNaturalList(status.Incident.Components)
		}
		if phase := strings.TrimSpace(status.Incident.Status); phase != "" {
			summary += " — " + phase
		}
		return summary, strings.TrimSpace(status.Incident.URL)
	case telemetry.ProviderStatusNoMatch:
		return "no matching provider incident", ""
	case telemetry.ProviderStatusUnavailable:
		return "provider status unavailable", ""
	case telemetry.ProviderStatusPending:
		return "provider status check pending", ""
	default:
		return "", ""
	}
}

func healthNaturalList(values []string) string {
	clean := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			clean = append(clean, value)
		}
	}
	switch len(clean) {
	case 0:
		return ""
	case 1:
		return clean[0]
	case 2:
		return clean[0] + " and " + clean[1]
	default:
		return strings.Join(clean[:len(clean)-1], ", ") + ", and " + clean[len(clean)-1]
	}
}

func forgeUnavailableHealthDetail(conditions []telemetry.ForgeCondition) string {
	if len(conditions) == 1 {
		host := strings.TrimSpace(conditions[0].Host)
		if host == "" {
			host = "The configured forge"
		}
		return host + " cannot accept push or pull-request writes; write-dependent dispatch is paused."
	}
	return boardCountLabel(len(conditions), "forge host cannot", "forge hosts cannot") + " accept writes; write-dependent dispatch is paused."
}

func ciUnavailableHealthDetail(conditions []telemetry.CICondition) string {
	checkCount := 0
	pullRequestCount := 0
	oldestQueueSeconds := int64(0)
	for _, condition := range conditions {
		checkCount += condition.UnstartedCheckCount
		pullRequestCount += condition.PullRequestCount
		oldestQueueSeconds = max(oldestQueueSeconds, condition.OldestQueueSeconds)
	}
	detail := boardCountLabel(checkCount, "check is", "checks are") + " queued and unstarted across " +
		boardCountLabel(pullRequestCount, "PR", "PRs")
	if oldestQueueSeconds > 0 {
		detail += "; oldest queued " + formatDuration(float64(oldestQueueSeconds))
	}
	return detail + "."
}

func healthAdmissionProposalRows(proposals []telemetry.AdmissionProposal, observedAt time.Time) []healthRow {
	byProject := make(map[string][]telemetry.AdmissionProposal)
	for _, proposal := range proposals {
		projectID := strings.TrimSpace(proposal.ProjectID)
		if projectID == "" {
			projectID = "project"
		}
		byProject[projectID] = append(byProject[projectID], proposal)
	}
	projects := make([]string, 0, len(byProject))
	for projectID := range byProject {
		projects = append(projects, projectID)
	}
	sort.Strings(projects)

	rows := make([]healthRow, 0, len(projects))
	for _, projectID := range projects {
		projectProposals := byProject[projectID]
		sort.Slice(projectProposals, func(i, j int) bool {
			return admissionProposalTarget(projectProposals[i]) < admissionProposalTarget(projectProposals[j])
		})
		details := make([]string, 0, len(projectProposals))
		for _, proposal := range projectProposals {
			details = append(details, admissionProposalTarget(proposal)+" · "+admissionProposalTiming(proposal, observedAt))
		}
		rows = append(rows, healthRow{
			ID:        "health-admission-proposals-" + boardCardSlug(projectID),
			Component: "Admission · " + projectID,
			Kind:      primitives.KindNeutral,
			Status:    boardCountLabel(len(projectProposals), "awaiting decision", "awaiting decisions"),
			Detail:    strings.Join(details, "; "),
			Resets:    "on decision",
		})
	}
	return rows
}

func healthStrandedActiveRows(issues []telemetry.StrandedIssue) []healthRow {
	byProject := make(map[string][]telemetry.StrandedIssue)
	for _, issue := range issues {
		projectID := strings.TrimSpace(issue.ProjectID)
		if projectID == "" {
			projectID = "project"
		}
		byProject[projectID] = append(byProject[projectID], issue)
	}
	projects := make([]string, 0, len(byProject))
	for projectID := range byProject {
		projects = append(projects, projectID)
	}
	sort.Strings(projects)

	rows := make([]healthRow, 0, len(projects))
	for _, projectID := range projects {
		projectIssues := byProject[projectID]
		sort.Slice(projectIssues, func(i, j int) bool {
			return strandedActiveHealthTarget(projectIssues[i]) < strandedActiveHealthTarget(projectIssues[j])
		})
		details := make([]string, 0, len(projectIssues))
		for _, issue := range projectIssues {
			details = append(details, strandedActiveHealthIssueDetail(issue))
		}
		rows = append(rows, healthRow{
			ID:        "health-stranded-active-" + boardCardSlug(projectID),
			Component: "Active work · " + projectID,
			Kind:      primitives.KindErr,
			Status:    "No live worker",
			Detail:    strings.Join(details, "; "),
			Resets:    "on dispatch",
		})
	}
	return rows
}

func strandedActiveHealthDetail(issues []telemetry.StrandedIssue) string {
	if len(issues) == 1 {
		return strandedActiveHealthIssueDetail(issues[0]) + "."
	}
	return formatCount(len(issues)) + " active issues have no live worker."
}

func strandedActiveHealthIssueDetail(issue telemetry.StrandedIssue) string {
	detail := strandedActiveHealthTarget(issue) + " · " + formatDuration(float64(issue.DurationSeconds))
	reason := strings.TrimSpace(issue.LastRefusalReason)
	if reason == "" {
		reason = "none recorded"
	}
	return detail + " · last refusal: " + reason
}

func strandedActiveHealthTarget(issue telemetry.StrandedIssue) string {
	for _, value := range []string{issue.Identifier, issue.IssueID, issue.IssueURL} {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return "issue"
}

func healthCopyPayload(view healthView) string {
	var payload strings.Builder
	payload.WriteString("Detent health — ")
	payload.WriteString(boardCountLabel(len(view.Rows), "signal", "signals"))
	payload.WriteString(" — checked ")
	if view.CheckedAt.IsZero() {
		payload.WriteString("unavailable")
	} else {
		payload.WriteString(view.CheckedAt.UTC().Format(time.RFC3339))
	}
	payload.WriteByte('\n')
	payload.WriteString(healthCopyPlainText(strings.TrimSpace(strings.TrimSpace(view.Verdict) + " " + strings.TrimSpace(view.Detail))))

	for index, row := range view.Rows {
		if index == 0 {
			payload.WriteString("\n\n")
		} else {
			payload.WriteByte('\n')
		}
		payload.WriteString(healthCopyKind(row.Kind))
		payload.WriteByte(' ')
		payload.WriteString(healthCopyPlainText(strings.TrimSpace(row.Component)))
		payload.WriteString(" | ")
		payload.WriteString(healthCopyPlainText(strings.TrimSpace(row.Status)))
		payload.WriteString(" | ")
		payload.WriteString(healthCopyPlainText(strings.TrimSpace(row.Detail)))
		if !row.DetailAt.IsZero() {
			payload.WriteString(" · observed ")
			payload.WriteString(row.DetailAt.UTC().Format(time.RFC3339))
		}
		if quota := strings.TrimSpace(row.Quota); quota != "" {
			payload.WriteString(" | ")
			payload.WriteString(healthCopyPlainText(quota))
		}
		payload.WriteString(" | resets ")
		if !row.ResetAt.IsZero() {
			payload.WriteString(row.ResetAt.UTC().Format(time.RFC3339))
		} else if resets := strings.TrimSpace(row.Resets); resets != "" {
			payload.WriteString(healthCopyPlainText(resets))
		} else {
			payload.WriteString("—")
		}
	}

	return payload.String()
}

func healthCopyPlainText(value string) string {
	const prefix = "{{detent-time:"
	for {
		start := strings.Index(value, prefix)
		if start < 0 {
			return value
		}
		endOffset := strings.Index(value[start+len(prefix):], "}}")
		if endOffset < 0 {
			return value
		}
		end := start + len(prefix) + endOffset
		token := value[start+len(prefix) : end]
		separator := strings.IndexByte(token, ':')
		if separator < 0 {
			return value
		}
		replacement := token[separator+1:]
		if parsed, err := time.Parse(time.RFC3339Nano, replacement); err == nil {
			replacement = parsed.UTC().Format(time.RFC3339)
		}
		value = value[:start] + replacement + value[end+2:]
	}
}

func healthCopyKind(kind primitives.Kind) string {
	switch kind {
	case primitives.KindOK:
		return "[OK]  "
	case primitives.KindWarn:
		return "[WARN]"
	case primitives.KindErr:
		return "[ERR] "
	default:
		return "[INFO]"
	}
}

func healthUpdateRow(update telemetry.Update) healthRow {
	return healthUpdateRowAt(update, time.Now())
}

func healthUpdateRowAt(update telemetry.Update, now time.Time) healthRow {
	row := healthRow{
		ID:        "health-update",
		Component: "Detent update",
		Kind:      primitives.KindNeutral,
		Status:    "Disabled",
		Detail:    "Automatic update checks are disabled",
		Resets:    "—",
	}
	if !update.Enabled {
		if update.LastAppliedVersion != "" {
			row.Detail += " · last applied " + update.LastAppliedVersion
		}
		return row
	}
	row.Kind = primitives.KindOK
	row.Status = strings.ReplaceAll(strings.TrimSpace(update.DisplayState(now)), "_", " ")
	if row.Status == "" || row.Status == "scheduled" {
		row.Status = "Scheduled"
	}
	row.Detail = "Checks every " + formatCount(update.CheckIntervalHours) + " hours"
	if update.AutoApplyEnabled {
		row.Detail += " · auto-apply enabled"
	} else {
		row.Detail += " · notify only"
	}
	if update.AvailableVersion != "" {
		row.Kind = primitives.KindNeutral
		row.Detail += " · " + update.AvailableVersion + " available"
	}
	if update.LastAppliedVersion != "" {
		row.Detail += " · last applied " + update.LastAppliedVersion
	}
	if update.LastCheckAt != nil {
		row.DetailAt = *update.LastCheckAt
	}
	if update.NextCheckAt != nil {
		row.ResetAt = *update.NextCheckAt
	}
	if update.LastError != "" {
		row.Kind = primitives.KindErr
		row.Status = "Failed"
		row.Detail = update.LastError
	}
	return row
}

func detentUpdatePending(update telemetry.Update) bool {
	return strings.TrimSpace(update.State) == "pending_idle" && strings.TrimSpace(update.AvailableVersion) != ""
}

func detentPendingUpdateVersion(update telemetry.Update) string {
	version := strings.TrimSpace(update.AvailableVersion)
	if version == "" {
		return "update"
	}
	return version
}

func healthReleases(snapshot telemetry.Snapshot) []telemetry.Release {
	if len(snapshot.Releases) > 0 {
		return snapshot.Releases
	}
	if !snapshot.Release.IsZero() {
		return []telemetry.Release{snapshot.Release}
	}
	return nil
}

func healthReleaseRow(release telemetry.Release) healthRow {
	component := "Auto-release"
	if strings.TrimSpace(release.ProjectID) != "" {
		component += " · " + release.ProjectID
	}
	status := strings.ReplaceAll(strings.TrimSpace(release.State), "_", " ")
	if status == "" {
		status = "Waiting"
	}
	detail := "Last " + release.LastRelease + " · " + formatCount(release.UnreleasedMerges) + " unreleased merges"
	if release.LastRelease == "" {
		detail = formatCount(release.UnreleasedMerges) + " unreleased merges · no prior release"
	}
	kind := primitives.KindOK
	switch release.State {
	case "failed":
		kind = primitives.KindErr
		detail = release.LastError
	case "waiting_for_ci", "rerunning_ci", "release_pending":
		kind = primitives.KindNeutral
	}
	row := healthRow{ID: "health-release-" + boardCardSlug(release.ProjectID), Component: component, Kind: kind, Status: status, Detail: detail, Resets: "—"}
	if release.NextTriggerAt != nil {
		row.ResetAt = *release.NextTriggerAt
	}
	return row
}

func healthBackendOutageRow(outage telemetry.BackendOutage, now time.Time) healthRow {
	detail, resumeAt, _ := backendCapacityOutageDetailParts(outage, now)
	status := "Usage limit"
	if strings.EqualFold(strings.TrimSpace(outage.Reason), "subscription window exhausted") {
		status = "Subscription exhausted"
	}
	return healthRow{
		ID:        "health-backend-" + boardCardSlug(outage.BackendID),
		Component: "Backend " + outage.BackendID,
		Kind:      primitives.KindErr,
		Status:    status,
		Detail:    detail,
		DetailAt:  resumeAt,
		Resets:    "—",
		ResetAt:   outage.ResumeAt,
	}
}

func healthSchedulerRow(snapshot telemetry.Snapshot) healthRow {
	running := runningCount(snapshot)
	queued := queueCount(snapshot)
	row := healthRow{
		ID:        "health-scheduler",
		Component: "Scheduler",
		Kind:      primitives.KindOK,
		Status:    "Running",
		Detail:    runningCountLabel(snapshot) + " active sessions · " + formatCount(queued) + " queued",
		Resets:    "—",
	}
	if !runtimeCountComplete(snapshot) {
		row.Kind = primitives.KindNeutral
		row.Status = "Starting"
		row.Detail = "Active session count is not yet complete"
		return row
	}
	if running == 0 && queued == 0 {
		row.Status = "Idle"
		row.Detail = "No active sessions or queued work"
	}
	return row
}

func healthVerdictGlyphClass(kind primitives.Kind) string {
	return "text-sm leading-none " + boardExtraTextClass(kind)
}

func healthRowClass(last bool) string {
	base := "grid grid-cols-[180px_140px_minmax(0,1fr)_200px_90px] items-center gap-3.5 px-4 py-2.5"
	if !last {
		return base + " border-b border-line"
	}
	return base
}

func healthQuotaBarClass(warn bool) string {
	if warn {
		return "h-full bg-warn"
	}
	return "h-full bg-sec"
}

func healthQuotaBarKindClass(kind primitives.Kind, warn bool) string {
	if kind == primitives.KindErr {
		return "h-full bg-err"
	}
	return healthQuotaBarClass(warn)
}

func HealthShellDataFromDashboardV2(data DashboardData) DashboardShellData {
	shell := DashboardShellDataFromDashboard(data)
	shell.ActiveNav = "health"
	shell.IncludeDashboardCharts = false
	return shell
}
