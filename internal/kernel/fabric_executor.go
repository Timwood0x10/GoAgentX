package kernel

import (
	"context"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// fabricExecutor returns a CapabilityExecutor adapter for a live fabric agent,
// or nil when the agent is unknown, not IDLE, or has no execution body (A1:
// managed but not schedulable). Called when Schedule wins with a fabric agent
// that is not in the static executor registry.
func (s *Scheduler) fabricExecutor(agentID string) CapabilityExecutor {
	if s.agents == nil {
		return nil
	}
	if !s.agents.IsIdle(agentID) {
		return nil
	}
	a, err := s.agents.Get(agentID)
	if err != nil || a == nil {
		return nil
	}
	if !a.Executable() {
		return nil
	}
	return &fabricAgentExecutor{agents: s.agents, id: agentID}
}

// fabricAgentExecutor adapts a live agentfabric.Agent to the scheduler's
// CapabilityExecutor contract (aresos-agentos-plan B1: scheduler 候选来自
// agentfabric 动态群体). Execution delegates to the agent's injected
// Cognition (A1), so a spawned fabric agent is a REAL executor — not a
// phantom. StepOutcome semantics match sub's by construction (both carry
// Done/Checkpoint/Result).
type fabricAgentExecutor struct {
	agents *agentfabric.Fabric
	id     string
}

// ID returns the fabric agent's identity.
func (e *fabricAgentExecutor) ID() string { return e.id }

// Type returns the fabric agent's primary declared capability (the first of
// the declared set; the full set is used for candidate scoring).
func (e *fabricAgentExecutor) Type() models.AgentType {
	a, err := e.agents.Get(e.id)
	if err != nil || len(a.Capabilities) == 0 {
		return models.AgentTypeTop
	}
	return models.AgentType(a.Capabilities[0])
}

// ExecuteStep runs one quantum through the fabric agent's Cognition (A1).
func (e *fabricAgentExecutor) ExecuteStep(ctx context.Context, task *models.Task) (*sub.StepOutcome, error) {
	a, err := e.agents.Get(e.id)
	if err != nil {
		return nil, err
	}
	out, err := a.ExecuteStep(ctx, task)
	if err != nil {
		return nil, err
	}
	return &sub.StepOutcome{Done: out.Done, Checkpoint: out.Checkpoint, Result: out.Result}, nil
}

// appendFabricCandidates appends every live, IDLE, executable fabric agent as
// a candidate (B1). Agents already in the registered pool are skipped — the
// registry wins (it may carry a richer binding than the fabric snapshot).
// Capabilities use the agent's FULL declared set (not the single primary
// Type()) so the capability scorer matches any overlap with the task's
// required capability.
func (s *Scheduler) appendFabricCandidates(cands []taskfabric.Candidate, registered map[string]CapabilityExecutor) []taskfabric.Candidate {
	if s.agents == nil {
		return cands
	}
	for _, id := range s.agents.Agents() {
		// C1: the fabric population is the single candidate source in peer
		// mode — a same-id static registration does NOT mask the managed
		// fabric copy (executeUnbound already filters unbound static
		// registrations; the fabric agent is the live, kill-visible one).
		// Recovery-bound executors are reserved for their task and excluded.
		if s.isBoundToAnyTask(id) {
			continue
		}
		a, err := s.agents.Get(id)
		if err != nil || a == nil {
			continue
		}
		if !s.agents.IsIdle(id) {
			continue
		}
		if !a.Executable() {
			// Managed but not schedulable (A1): no Cognition injected.
			continue
		}
		cands = append(cands, taskfabric.Candidate{
			AgentID:      id,
			Capabilities: append([]string(nil), a.Capabilities...),
			Load:         s.tracker.Load(id),
			Confidence:   s.tracker.Confidence(id),
			Priority:     s.tracker.Priority(id),
		})
	}
	return cands
}
