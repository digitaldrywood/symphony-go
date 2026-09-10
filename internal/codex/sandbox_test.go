package codex

import (
	"fmt"
	"maps"
	"reflect"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/config"
)

func TestTurnSandboxPolicyForWorkspaceMergesExtraWritableRoots(t *testing.T) {
	t.Parallel()

	policy := map[string]any{
		"networkAccess": true,
		"writableRoots": []string{"/existing"},
		"writable_roots": []any{
			"/legacy",
			42,
		},
	}

	got, ok := turnSandboxPolicyForWorkspace("workspace-write", policy, []string{"/extra", "/existing", " "}).(map[string]any)
	if !ok {
		t.Fatalf("turnSandboxPolicyForWorkspace() = %T, want map", got)
	}

	if got["type"] != "workspaceWrite" {
		t.Fatalf("policy type = %#v, want workspaceWrite", got["type"])
	}
	if got["networkAccess"] != true {
		t.Fatalf("policy networkAccess = %#v, want true", got["networkAccess"])
	}
	if roots, ok := got["writableRoots"].([]string); !ok || !reflect.DeepEqual(roots, []string{"/existing", "/legacy", "/extra"}) {
		t.Fatalf("policy writableRoots = %#v, want merged roots", got["writableRoots"])
	}
	if _, ok := got["writable_roots"]; ok {
		t.Fatalf("policy writable_roots = %#v, want absent", got["writable_roots"])
	}
	if _, ok := policy["type"]; ok {
		t.Fatalf("original policy type = %#v, want absent", policy["type"])
	}
}

func TestTurnSandboxPolicyPreservesExplicitNonWorkspacePolicy(t *testing.T) {
	t.Parallel()

	policy := map[string]any{
		"type":          "dangerFullAccess",
		"networkAccess": true,
	}

	got := turnSandboxPolicyForWorkspace("workspace-write", policy, []string{"/extra"})
	if !reflect.DeepEqual(got, policy) {
		t.Fatalf("turnSandboxPolicyForWorkspace() = %#v; want explicit non-workspace policy", got)
	}
	if policy["type"] != "dangerFullAccess" {
		t.Fatalf("policy type = %#v, want dangerFullAccess", policy["type"])
	}
	if _, ok := policy["writableRoots"]; ok {
		t.Fatalf("policy writableRoots = %#v, want absent", policy["writableRoots"])
	}
}

func TestOptionsFromConfigClonesApprovalPolicy(t *testing.T) {
	t.Parallel()

	rawPolicy := map[string]any{
		"reject": map[string]any{
			"rules": true,
		},
	}
	rules := []config.DeliverableElicitationRule{{
		Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets",
	}}
	options := OptionsFromConfig(config.CodexOptions{
		ApprovalPolicy:                  config.MapValue(rawPolicy),
		DeliverableElicitationAllowlist: rules,
		ThreadSandbox:                   "workspace-write",
		StallTimeoutMS:                  1250,
		TurnSandboxPolicy: map[string]any{
			"type": "workspaceWrite",
		},
	})
	rawPolicy["reject"].(map[string]any)["rules"] = false
	rules[0].Tool = "github.delete_issue"

	policy, ok := options.ApprovalPolicy.(map[string]any)
	if !ok {
		t.Fatalf("ApprovalPolicy = %T, want map", options.ApprovalPolicy)
	}
	reject, ok := policy["reject"].(map[string]any)
	if !ok {
		t.Fatalf("ApprovalPolicy.reject = %T, want map", policy["reject"])
	}
	if reject["rules"] != true {
		t.Fatalf("ApprovalPolicy.reject.rules = %#v, want cloned true", reject["rules"])
	}
	wantRule := MCPElicitationRule{Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets"}
	if len(options.DeliverableElicitationAllowlist) != 1 || options.DeliverableElicitationAllowlist[0] != wantRule {
		t.Fatalf("DeliverableElicitationAllowlist = %#v, want %#v", options.DeliverableElicitationAllowlist, wantRule)
	}
	if options.ThreadSandbox != "workspace-write" {
		t.Fatalf("ThreadSandbox = %q, want workspace-write", options.ThreadSandbox)
	}
	if options.StallTimeout != 1250*time.Millisecond {
		t.Fatalf("StallTimeout = %v, want 1.25s", options.StallTimeout)
	}
}

func TestTurnSandboxPolicyTypes(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ thread, kind string }{
		{"workspace-write", "workspaceWrite"},
		{"danger-full-access", "dangerFullAccess"},
		{"read-only", "readOnly"},
	} {
		for _, explicit := range []bool{false, true} {
			for _, roots := range [][]string{nil, {"/extra"}} {
				t.Run(fmt.Sprintf("%s/explicit=%t/roots=%d", tt.thread, explicit, len(roots)), func(t *testing.T) {
					t.Parallel()
					policy := map[string]any{"networkAccess": false}
					if explicit {
						policy["type"] = tt.kind
					}
					original := maps.Clone(policy)
					got, ok := turnSandboxPolicyForWorkspace(tt.thread, policy, roots).(map[string]any)
					if !ok || got["type"] != tt.kind || got["networkAccess"] != false {
						t.Fatalf("policy = %#v, want type %s and networkAccess false", got, tt.kind)
					}
					if tt.kind == "workspaceWrite" && len(roots) > 0 {
						if !reflect.DeepEqual(got["writableRoots"], roots) {
							t.Fatalf("roots = %#v, want %v", got["writableRoots"], roots)
						}
					} else if _, exists := got["writableRoots"]; exists {
						t.Fatalf("unexpected writableRoots: %#v", got)
					}
					if !reflect.DeepEqual(policy, original) {
						t.Fatalf("input mutated: %#v", policy)
					}
				})
			}
		}
	}
}
