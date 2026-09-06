// Package planprojection projects workflow engine steps into taskfabric PlanSteps.
//
// This is the SINGLE projection function that maps engine.Step →
// taskfabric.PlanStep, closing the "two graphs" gap in the evolution loop:
// before this package, the live MutableDAG and the compiled task set were
// separate objects that could silently diverge. The projection lives in
// its own package (not in taskfabric) so the kernel never imports the
// planner package — the caller (cmd layer) projects engine.Step onto
// PlanStep, then hands the batch to Fabric.CompilePlan.
package planprojection

import (
	"strconv"

	"github.com/Timwood0x10/ares/internal/taskfabric"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// CompileRecord captures the provenance of one CompilePlan call so the
// introspect layer can answer "which generation, which gate, which compile
// produced the current task set".
type CompileRecord struct {
	Generation int      `json:"generation"`
	DAGVersion uint64   `json:"dag_version"`
	CompileID  string   `json:"compile_id"`
	PlanIDs    []string `json:"plan_ids"`
	StepCount  int      `json:"step_count"`
}

// ProjectStep converts a single engine.Step into a taskfabric.PlanStep.
//
// Mapping rules (fixed, see ares-evolution-loop-tasklist-zh.md C1.1):
//
//	PlanStep.ID         ← Step.ID
//	PlanStep.Capability  ← Step.AgentType
//	PlanStep.DependsOn   ← Step.DependsOn (copied)
//	PlanStep.MaxRetries  ← Step.RetryPolicy.MaxAttempts (nil → 0)
//	PlanStep.Priority    ← Step.Metadata["priority"] parsed as int; parse
//	                       failure or missing → 0
//	PlanStep.Payload     ← {"input": Step.Input} merged with Step.Metadata
//	                       (metadata keys win on conflict)
//	PlanStep.Origin      ← not filled (json:"-", stamped by the Kernel)
//
// Explicitly discarded (HITL frozen or execution-time state):
//   - Interrupt, Timeout, RecoveryPolicy, Name,
//     Status/Output/Error/StartedAt/FinishedAt
func ProjectStep(s *engine.Step) taskfabric.PlanStep {
	if s == nil {
		return taskfabric.PlanStep{}
	}

	deps := make([]string, len(s.DependsOn))
	copy(deps, s.DependsOn)

	maxRetries := 0
	if s.RetryPolicy != nil {
		maxRetries = s.RetryPolicy.MaxAttempts
	}

	payload := map[string]any{
		"input": s.Input,
	}
	for k, v := range s.Metadata {
		payload[k] = v
	}

	return taskfabric.PlanStep{
		ID:         s.ID,
		Capability: s.AgentType,
		DependsOn:  deps,
		MaxRetries: maxRetries,
		Priority:   parsePriority(s.Metadata),
		Payload:    payload,
		SessionID:  parseSessionID(s.Metadata),
	}
}

// ProjectSteps converts a batch of engine.Steps into PlanSteps. The order
// is preserved (callers rely on input order for deterministic CompilePlan).
func ProjectSteps(steps []*engine.Step) []taskfabric.PlanStep {
	result := make([]taskfabric.PlanStep, 0, len(steps))
	for _, s := range steps {
		result = append(result, ProjectStep(s))
	}
	return result
}

// parsePriority reads the "priority" key from step metadata. A missing or
// unparseable value yields 0 (normal priority). We try int first, then
// string-to-int, matching the loose typing of map[string]string metadata.
func parsePriority(meta map[string]string) int {
	if meta == nil {
		return 0
	}
	raw, ok := meta["priority"]
	if !ok || raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return n
}

// parseSessionID reads the "session_id" key from step metadata. A missing or
// empty value yields "" (session-less, legacy behavior). This is how the L2
// session root's SessionID propagates to every tool/plan node grown from it:
// the plannerCognition stamps session_id into the node's Metadata, and
// ProjectStep carries it into the PlanStep so CompilePlan can stamp it onto
// the checkpoint envelope.
func parseSessionID(meta map[string]string) string {
	if meta == nil {
		return ""
	}
	return meta["session_id"]
}
