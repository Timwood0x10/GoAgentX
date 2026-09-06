package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/ares_config"
	"github.com/Timwood0x10/ares/internal/core/models"
)

// TestResolveDAGExecution pins the M4-A1 gate mapping: the zero config (an
// absent dag_execution section) selects legacy ReAct, and only an explicit
// enabled=true flips the gate. The gate must never open by default.
func TestResolveDAGExecution(t *testing.T) {
	tests := []struct {
		name    string
		config  ares_config.DAGExecutionConfig
		enabled bool
	}{
		{"absent section stays off", ares_config.DAGExecutionConfig{}, false},
		{"explicit false stays off", ares_config.DAGExecutionConfig{Enabled: false, MaxPlanDepth: 3}, false},
		{"explicit true opens gate", ares_config.DAGExecutionConfig{Enabled: true}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate := resolveDAGExecution(tt.config)
			if gate.Enabled != tt.enabled {
				t.Errorf("resolveDAGExecution(%+v).Enabled = %v, want %v",
					tt.config, gate.Enabled, tt.enabled)
			}
			// The resolved gate must still switch between the two bodies.
			chat, router := &stubBody{}, &stubBody{}
			want := chat
			if tt.enabled {
				want = router
			}
			if got := gate.Select(chat, router); got != want {
				t.Errorf("gate.Select(chat, router) with enabled=%v picked the wrong body",
					tt.enabled)
			}
		})
	}
}

// stubBody is a distinct-identity Cognition so the test can tell which body
// the gate selected. It is never executed in the gate tests; the adapter
// tests drive it with a canned outcome.
type stubBody struct {
	outcome    *agentfabric.StepOutcome
	outcomeErr error
}

func (s *stubBody) ExecuteStep(_ context.Context, _ *models.Task) (*agentfabric.StepOutcome, error) {
	return s.outcome, s.outcomeErr
}

// TestResolveMaxPlanDepth pins the M4-A2 depth mapping: zero/absent means the
// planner default (agentfabric.DefaultMaxPlanDepth), a positive value passes
// through, and a negative — which validation rejects at load — can never
// widen or remove the guard even if it reaches the resolver.
func TestResolveMaxPlanDepth(t *testing.T) {
	tests := []struct {
		name       string
		configured int
		want       int
	}{
		{"absent means planner default", 0, agentfabric.DefaultMaxPlanDepth},
		{"custom depth passes through", 3, 3},
		{"negative falls back to default", -1, agentfabric.DefaultMaxPlanDepth},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := ares_config.DAGExecutionConfig{MaxPlanDepth: tt.configured}
			if got := resolveMaxPlanDepth(cfg); got != tt.want {
				t.Errorf("resolveMaxPlanDepth(max_plan_depth=%d) = %d, want %d",
					tt.configured, got, tt.want)
			}
		})
	}
}

// TestResolveReaperGrace pins the P0-1 grace mapping: zero/absent passes
// through as 0 so the reaper's own 30s default stays the single source of
// truth; a positive config value wins; a negative (unreachable through
// Validate, defended anyway) degrades to the default rather than disabling
// the read-window protection.
func TestResolveReaperGrace(t *testing.T) {
	tests := []struct {
		name   string
		config ares_config.DAGExecutionConfig
		want   time.Duration
	}{
		{"absent section defaults to reaper", ares_config.DAGExecutionConfig{}, 0},
		{"zero passes through", ares_config.DAGExecutionConfig{ReaperGrace: 0}, 0},
		{"positive wins", ares_config.DAGExecutionConfig{ReaperGrace: 2 * time.Minute}, 2 * time.Minute},
		{"negative degrades to default", ares_config.DAGExecutionConfig{ReaperGrace: -time.Second}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveReaperGrace(tt.config); got != tt.want {
				t.Errorf("resolveReaperGrace(%+v) = %s, want %s", tt.config, got, tt.want)
			}
		})
	}
}

// TestPeerCapabilities_PartitionTraffic pins the M4-B2/C4 canary isolation:
// the gate-off and gate-on sets are disjoint by construction — a legacy
// primary-type task matches only gate-off peers, an ares/* task only gate-on
// peers — so canary peers never attract legacy traffic and vice versa, with
// no scheduler exclusion primitive required.
func TestPeerCapabilities_PartitionTraffic(t *testing.T) {
	off := peerCapabilities("researcher", false, []string{"grep", "read"})
	if len(off) != 1 || off[0] != "researcher" {
		t.Errorf("gate-off caps = %v, want exactly [researcher] (legacy unchanged)", off)
	}

	on := peerCapabilities("researcher", true, []string{"grep", "read"})
	want := map[string]bool{"ares/root": true, "ares/plan": true, "ares/answer": true, "tool/grep": true, "tool/read": true}
	if len(on) != len(want) {
		t.Fatalf("gate-on caps = %v, want exactly the L2 set", on)
	}
	for _, c := range on {
		if !want[c] {
			t.Errorf("gate-on caps contain %q, want only the L2 set", c)
		}
	}
	for _, c := range on {
		if c == "researcher" {
			t.Error("gate-on caps must NOT contain the primary type — that would attract legacy traffic to canary peers")
		}
	}

	empty := peerCapabilities("researcher", true, nil)
	if len(empty) != 3 {
		t.Errorf("gate-on caps with no tools = %v, want exactly the 3 ares/* capabilities", empty)
	}
	blank := peerCapabilities("researcher", true, []string{"", "grep"})
	for _, c := range blank {
		if c == "tool/" {
			t.Error("blank tool names must be skipped, not advertised as bare tool/")
		}
	}
}

// TestSelectRecoveryBody pins the M4-C3 remainder dispatch: only gate-open +
// L2-capability tasks take the router; everything else falls back to the
// legacy executor (nil = caller keeps newPeerExecutor).
func TestSelectRecoveryBody(t *testing.T) {
	router := &stubBody{}
	gateOn := agentfabric.DAGExecution{Enabled: true}
	gateOff := agentfabric.DAGExecution{}

	tests := []struct {
		name       string
		gate       agentfabric.DAGExecution
		router     agentfabric.Cognition
		capability string
		wantRouter bool
	}{
		{"gate off keeps legacy even for L2 caps", gateOff, router, "ares/plan", false},
		{"gate on keeps legacy for primary caps", gateOn, router, "researcher", false},
		{"gate on routes plan tasks to router", gateOn, router, "ares/plan", true},
		{"gate on routes tool tasks to router", gateOn, router, "tool/grep", true},
		{"gate on routes answer tasks to router", gateOn, router, "ares/answer", true},
		{"nil router never selected", gateOn, nil, "ares/plan", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectRecoveryBody(tt.gate, tt.router, tt.capability)
			if tt.wantRouter && got == nil {
				t.Errorf("capability %q with gate on must take the router", tt.capability)
			}
			if !tt.wantRouter && got != nil {
				t.Errorf("capability %q must fall back to legacy, got router", tt.capability)
			}
		})
	}
}

// TestNewCognitionExecutor_TranslatesOutcome pins the adapter contract:
// Done/Checkpoint/Result ride field-for-field across the boundary, and body
// errors propagate without translation.
func TestNewCognitionExecutor_TranslatesOutcome(t *testing.T) {
	if _, err := newCognitionExecutor("a1", "ares/plan", nil); err == nil {
		t.Error("nil body must fail at construction, not at first quantum")
	}

	want := &agentfabric.StepOutcome{Done: true, Checkpoint: "ck"}
	exec, err := newCognitionExecutor("a1", models.AgentType("ares/plan"),
		&stubBody{outcome: want})
	if err != nil {
		t.Fatalf("construction error = %v", err)
	}
	if exec.ID() != "a1" || exec.Type() != models.AgentType("ares/plan") {
		t.Errorf("identity not preserved: %q %q", exec.ID(), exec.Type())
	}
	got, err := exec.ExecuteStep(context.Background(), &models.Task{})
	if err != nil {
		t.Fatalf("ExecuteStep error = %v", err)
	}
	if !got.Done || got.Checkpoint != "ck" {
		t.Errorf("outcome not translated field-for-field: %+v", got)
	}

	boom := errors.New("boom")
	execErr, err := newCognitionExecutor("a2", "x", &stubBody{outcomeErr: boom})
	if err != nil {
		t.Fatalf("construction error = %v", err)
	}
	if _, err := execErr.ExecuteStep(context.Background(), &models.Task{}); !errors.Is(err, boom) {
		t.Errorf("body error must propagate untranslated, got %v", err)
	}
}
