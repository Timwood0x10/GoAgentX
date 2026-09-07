package kernel

import (
	"sort"
	"strings"
	"sync"
)

// LoadTracker records per-agent execution statistics so scheduling decisions
// can use real load and confidence instead of static placeholders (v0.3.0 GAP4:
// P2 F1). The scheduler creates one tracker per scheduler instance and passes
// it to every Candidate constructor so the Score formula reflects actual agent
// load and evolution confidence.
//
// W4 Evolution feedback: SetAgentConfidence lets the evolution feedback loop
// (EvolutionExecutionFeedback.Apply) override an agent's confidence after a
// batch of task results. SetCapabilityConfidence does the same at the
// capability level (higher priority than agent-level). ConfidenceFor returns
// capability-level first, then agent-level, then the neutral default (1.0).
//
// Thread-safe: the scheduler's drain loop and the feedback loop may call
// methods concurrently (the same lock protects Load and Confidence).
type LoadTracker struct {
	mu sync.Mutex

	// done, ok, priority, load are per-agent histograms.
	done     map[string]float64
	ok       map[string]float64
	priority map[string]float64
	load     map[string]float64

	// agentConfidenceOverride and capabilityConfidenceOverride are set by the
	// evolution feedback loop (W4) and GA scheduler (F1). capability level
	// takes precedence over agent level; agent level falls back to 1.0.
	agentConfidenceOverride      map[string]float64
	capabilityConfidenceOverride map[string]float64
}

func NewLoadTracker() *LoadTracker {
	return &LoadTracker{
		done:                         make(map[string]float64),
		ok:                           make(map[string]float64),
		priority:                     make(map[string]float64),
		load:                         make(map[string]float64),
		agentConfidenceOverride:      make(map[string]float64),
		capabilityConfidenceOverride: make(map[string]float64),
	}
}

func (t *LoadTracker) SetPriority(agentID string, priority float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.priority[agentID] = priority
}

func (t *LoadTracker) Priority(agentID string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if v, ok := t.priority[agentID]; ok {
		return v
	}
	return 0.0
}

func (t *LoadTracker) Begin(agentID string) {
	t.mu.Lock()
	t.load[agentID]++
	t.mu.Unlock()
}

func (t *LoadTracker) End(agentID string, success bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Release the busy slot acquired by Begin: load is the CURRENT busy
	// fraction, so an agent that finished a quantum must be schedulable again.
	// Without the decrement, load climbs monotonically and Score's (1-load)
	// factor zeroes out every agent that ever ran once (F1: later rounds get
	// "no capable candidate" even with live, idle executors).
	if t.load[agentID] > 0 {
		t.load[agentID]--
	}
	t.done[agentID]++
	if success {
		t.ok[agentID]++
	}
}

// EndNeutral releases the busy slot acquired by Begin WITHOUT recording an
// outcome: neither a success nor a failure enters the agent's history.
//
// Why it exists: cooperative preemption hands a RUNNING task back to READY,
// so the stale holder's completion is rejected by the fencing token
// (ErrNotOwner / ErrEpochMismatch). That rejection is a benign race the
// executor did not cause — counting it as a failure would poison the agent's
// historical success rate toward 0, and Score's confidence factor would make
// the preempted task permanently unschedulable (BUG-KSCHED-001 follow-up).
func (t *LoadTracker) EndNeutral(agentID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.load[agentID] > 0 {
		t.load[agentID]--
	}
}

func (t *LoadTracker) Load(agentID string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.load[agentID]
}

func (t *LoadTracker) Confidence(agentID string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if v, ok := t.agentConfidenceOverride[agentID]; ok {
		return v
	}
	if total, ok := t.done[agentID]; ok && total > 0 {
		return t.ok[agentID] / total
	}
	return 1.0
}

func (t *LoadTracker) SetAgentConfidence(agentID string, confidence float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Negative values clear the override so Confidence falls back to the
	// historical success rate or the neutral prior (ConfidenceInjector
	// contract: "<= 0 resets to the neutral prior (1.0)"). 0.0 remains a
	// VALID override (a 0% success rate must keep an agent at the bottom of
	// the ranking — the F1 GA tests rely on it).
	if confidence < 0 {
		delete(t.agentConfidenceOverride, agentID)
		return
	}
	t.agentConfidenceOverride[agentID] = confidence
}

func (t *LoadTracker) SetCapabilityConfidence(agentID, capability string, confidence float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := agentID + "|" + capability
	// Negative values clear the capability override so ConfidenceFor falls
	// back to the agent-level confidence / neutral prior (ConfidenceInjector
	// contract: "a negative value (< 0) clears it").
	if confidence < 0 {
		delete(t.capabilityConfidenceOverride, key)
		return
	}
	t.capabilityConfidenceOverride[key] = confidence
}

func (t *LoadTracker) ConfidenceFor(agentID, capability string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := agentID + "|" + capability
	if v, ok := t.capabilityConfidenceOverride[key]; ok {
		return v
	}
	if v, ok := t.agentConfidenceOverride[agentID]; ok {
		return v
	}
	if total, ok := t.done[agentID]; ok && total > 0 {
		return t.ok[agentID] / total
	}
	return 1.0
}

// AgentLoadSnapshot is one agent's row in a LoadTracker snapshot — the
// read-only view the runtime introspection panel consumes (monitoring.md
// Domain A). All values are copies; mutating them never touches the tracker.
type AgentLoadSnapshot struct {
	// AgentID is the tracked agent.
	AgentID string `json:"agentID"`
	// Done is the total finished executions; Ok is the successful subset.
	Done float64 `json:"done"`
	Ok   float64 `json:"ok"`
	// Priority is the agent's scheduling priority.
	Priority float64 `json:"priority"`
	// Load is the current in-flight quantum count.
	Load float64 `json:"load"`
	// ConfidenceOverride is the agent-level override (evolution feedback W4);
	// HasConfidenceOverride reports whether one is set (0.0 is a VALID
	// override, so the bool — not the value — carries presence).
	ConfidenceOverride    float64 `json:"confidenceOverride"`
	HasConfidenceOverride bool    `json:"hasConfidenceOverride"`
	// CapabilityOverrides are this agent's per-capability confidence
	// overrides, keyed by capability name (GA scheduler F1).
	CapabilityOverrides map[string]float64 `json:"capabilityOverrides"`
}

// LoadTrackerSnapshot is a point-in-time copy of every tracked agent,
// ordered by AgentID for stable panel rendering.
type LoadTrackerSnapshot struct {
	Agents []AgentLoadSnapshot `json:"agents"`
}

// Snapshot returns a consistent copy of all tracked agents. It takes the
// tracker lock once and copies everything under it, so callers can render or
// serialize the result without holding any scheduler locks and without racing
// concurrent Begin/End updates (monitoring.md Phase 0: read-only snapshots).
func (t *LoadTracker) Snapshot() LoadTrackerSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	ids := make(map[string]struct{})
	for id := range t.done {
		ids[id] = struct{}{}
	}
	for id := range t.ok {
		ids[id] = struct{}{}
	}
	for id := range t.priority {
		ids[id] = struct{}{}
	}
	for id := range t.load {
		ids[id] = struct{}{}
	}
	for id := range t.agentConfidenceOverride {
		ids[id] = struct{}{}
	}

	out := LoadTrackerSnapshot{Agents: make([]AgentLoadSnapshot, 0, len(ids))}
	for id := range ids {
		a := AgentLoadSnapshot{
			AgentID:             id,
			Done:                t.done[id],
			Ok:                  t.ok[id],
			Priority:            t.priority[id],
			Load:                t.load[id],
			CapabilityOverrides: make(map[string]float64),
		}
		if v, ok := t.agentConfidenceOverride[id]; ok {
			a.ConfidenceOverride = v
			a.HasConfidenceOverride = true
		}
		prefix := id + "|"
		for key, v := range t.capabilityConfidenceOverride {
			if capName, found := strings.CutPrefix(key, prefix); found {
				a.CapabilityOverrides[capName] = v
			}
		}
		out.Agents = append(out.Agents, a)
	}
	sort.Slice(out.Agents, func(i, j int) bool {
		return out.Agents[i].AgentID < out.Agents[j].AgentID
	})
	return out
}
