package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/ares_config"
	"github.com/Timwood0x10/ares/internal/core/models"
)

// resolveDAGExecution maps the kernel dag_execution config section onto the
// agentfabric execution gate (M4-A1). The zero config selects legacy ReAct
// behavior, so an absent section behaves exactly like today — flipping the
// gate requires a deliberate operator edit, never a default.
func resolveDAGExecution(c ares_config.DAGExecutionConfig) agentfabric.DAGExecution {
	return agentfabric.DAGExecution{Enabled: c.Enabled}
}

// resolveMaxPlanDepth maps the configured plan-depth cap onto the planner's
// MaxDepth (M4-A2). Zero/negative means "planner default"
// (agentfabric.DefaultMaxPlanDepth): validation rejects negatives at load
// time, and the planner itself treats non-positive as default, so an invalid
// value can never widen or remove the guard even if it reaches the resolver.
func resolveMaxPlanDepth(c ares_config.DAGExecutionConfig) int {
	if c.MaxPlanDepth <= 0 {
		return agentfabric.DefaultMaxPlanDepth
	}
	return c.MaxPlanDepth
}

// resolveReaperGrace maps the configured terminal-task reaper grace onto a
// duration (P0-1). Zero/absent passes through as 0, which the reaper itself
// defaults to 30s — the single default lives in taskfabric, not here. A
// negative cannot reach the resolver (Validate rejects it at load), and the
// reaper treats non-positive as its default, so a bad value can never
// disable the grace window.
func resolveReaperGrace(c ares_config.DAGExecutionConfig) time.Duration {
	if c.ReaperGrace <= 0 {
		return 0
	}
	return c.ReaperGrace
}

// peerCapabilities builds one peer's advertised capability set (M4-B2/C4).
// Gate off: only the primary type (legacy behavior, unchanged). Gate on:
// the L2 set (ares/root, ares/plan, ares/answer, tool/<name> per bound tool)
// and deliberately NOT the primary type — the two sets partition traffic by
// construction, so canary peers never attract legacy traffic and vice versa,
// with no scheduler exclusion primitive required.
func peerCapabilities(primary string, gateEnabled bool, toolNames []string) []string {
	if !gateEnabled {
		return []string{primary}
	}
	caps := []string{"ares/root", "ares/plan", "ares/answer"}
	for _, name := range toolNames {
		if name == "" {
			continue
		}
		caps = append(caps, "tool/"+name)
	}
	return caps
}

// selectRecoveryBody picks the recovery-bound execution body for one task
// (M4-C3 remainder): the L2 router when the gate is open AND the task is an
// L2 session task, nil otherwise (caller falls back to the legacy ReAct
// executor). Recovery-bound tasks bypass the normal candidate pool, so the
// canary partition cannot isolate them — the dispatch must happen here,
// per task, or a rescued L2 task would run on the wrong body.
func selectRecoveryBody(gate agentfabric.DAGExecution, router agentfabric.Cognition, capability string) agentfabric.Cognition {
	if !gate.Enabled || router == nil {
		return nil
	}
	if !agentfabric.IsL2Capability(capability) {
		return nil
	}
	return router
}

// cognitionExecutor adapts an agentfabric Cognition to the scheduler's
// CapabilityExecutor contract for recovery-bound tasks. It is the same
// field-for-field StepOutcome translation the fabric executor performs
// (Done/Checkpoint/Result ride opaquely, so both chat resume checkpoints
// and L2 planner quanta survive the boundary).
type cognitionExecutor struct {
	id  string
	typ models.AgentType
	cog agentfabric.Cognition
}

// newCognitionExecutor builds a recovery-bound executor over the given
// execution body. A nil body is a wiring error, surfaced at construction.
func newCognitionExecutor(agentID string, capability models.AgentType, cog agentfabric.Cognition) (*cognitionExecutor, error) {
	if cog == nil {
		return nil, fmt.Errorf("peer mode: recovery executor %q has no execution body", agentID)
	}
	return &cognitionExecutor{id: agentID, typ: capability, cog: cog}, nil
}

// ID implements CapabilityExecutor.
func (e *cognitionExecutor) ID() string { return e.id }

// Type implements CapabilityExecutor.
func (e *cognitionExecutor) Type() models.AgentType { return e.typ }

// ExecuteStep implements CapabilityExecutor by delegating one quantum to the
// wrapped body and translating the outcome field-for-field.
func (e *cognitionExecutor) ExecuteStep(ctx context.Context, task *models.Task) (*sub.StepOutcome, error) {
	out, err := e.cog.ExecuteStep(ctx, task)
	if err != nil {
		return nil, err
	}
	return &sub.StepOutcome{Done: out.Done, Checkpoint: out.Checkpoint, Result: out.Result}, nil
}
