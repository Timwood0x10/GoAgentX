package taskfabric

import (
	"context"
	"errors"
	"fmt"
)

// PlanStep is the minimal step description compiled into a Task batch. It is
// defined in taskfabric (not workflow/engine) so the kernel never imports the
// planner package — the caller (cmd layer) projects engine.Step onto it.
//
// See ares-repair-plan-zh.md appendix C (W9 / option A: workflow as a
// compile-time planning layer on top of the single execution kernel).
type PlanStep struct {
	// ID is the unique step id; becomes the fabric Task ID.
	ID string
	// Capability is the required executor capability (engine.Step.AgentType).
	Capability string
	// DependsOn lists step IDs that must COMPLETE before this step is READY.
	DependsOn []string
	// Priority drives preemption (higher wins); 0 = normal.
	Priority int
	// MaxRetries counts TOTAL attempts (taskfabric.CanRetry semantics).
	MaxRetries int
	// Payload carries the step's input metadata (surfaced via the checkpoint
	// envelope to the executor).
	Payload map[string]any
	// Origin is the provenance stamped by the Kernel (kernelctx.CallerID) —
	// never supplied by the LLM (same contract as CreateTask). json:"-"
	// keeps it out of every LLM-facing schema.
	Origin string `json:"-"`
}

// CompilePlan validates a batch of PlanSteps and creates them as READY tasks
// in one all-or-nothing transaction:
//
//   - every DependsOn reference must resolve — inside the batch first, then
//     against tasks already in the fabric (see resolveDependencies);
//   - the dependency graph must be acyclic (topological check);
//   - every Create must succeed — any failure rolls back the tasks already
//     created in this batch so a half-built DAG never pollutes the ready queue.
//
// It returns the created task IDs in input order.
//
// Args:
//   - ctx: unused today (Create is synchronous); kept for signature symmetry
//     with future async compilation.
//   - steps: the batch to compile; must be non-empty.
//
// Returns:
//   - []string: the created task IDs, in input order.
//   - error: ErrTaskExists / ErrTaskIDRequired / validation errors, wrapped.
func (f *Fabric) CompilePlan(ctx context.Context, steps []PlanStep) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("taskfabric: compile plan: %w", err)
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("taskfabric: compile plan: empty step batch")
	}
	byID := make(map[string]PlanStep, len(steps))
	for _, s := range steps {
		if s.ID == "" {
			return nil, fmt.Errorf("taskfabric: compile plan: step id required")
		}
		if _, dup := byID[s.ID]; dup {
			return nil, fmt.Errorf("taskfabric: compile plan: duplicate step id %q", s.ID)
		}
		byID[s.ID] = s
	}
	// Dependency closure: batch first, then tasks already in the fabric
	// (see resolveDependencies). Cross-batch resolution is what makes
	// runtime graph growth possible — a node grown at runtime depends on
	// nodes compiled by an earlier batch, which are typically already
	// COMPLETED and therefore let the new task go READY immediately.
	f.mu.Lock()
	err := resolveDependencies(steps, byID, f.tasks)
	f.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("taskfabric: compile plan: %w", err)
	}
	if err := detectPlanCycle(steps, byID); err != nil {
		return nil, err
	}
	// E1: sample the strategy attribution ONCE for the whole batch so every
	// task of the batch carries the same strategy even if the active strategy
	// changes mid-compilation. Create fills the stamp only when the envelope
	// field is still empty, so this pre-stamp wins.
	strategyID := f.strategyStampID()
	// All-or-nothing creation: roll back on any failure so the ready queue is
	// never polluted by a half-built DAG.
	created := make([]string, 0, len(steps))
	for _, s := range steps {
		deps := append([]string(nil), s.DependsOn...)
		t := &Task{
			ID:           s.ID,
			Capability:   s.Capability,
			Dependencies: deps,
			Priority:     s.Priority,
			Origin:       s.Origin,
			// MaxRetries <= 0 keeps the kernel default (2 = first attempt +
			// one retry); a positive value is honored verbatim.
			RetryPolicy: RetryPolicy{MaxRetries: s.MaxRetries},
		}
		if s.Payload != nil {
			env := &CheckpointEnvelope{Payload: s.Payload}
			if strategyID != "" {
				env.StrategyID = strategyID
			}
			t.Checkpoint = env
		}
		if err := f.Create(t); err != nil {
			// Roll back EVERY created id even if some Deletes fail: a partial
			// rollback must not shadow the original error, and the leftovers
			// must be reported so the operator can clean them up.
			var delErrs []error
			for _, id := range created {
				if delErr := f.Delete(id); delErr != nil {
					delErrs = append(delErrs, fmt.Errorf("rollback %q: %w", id, delErr))
				}
			}
			rollErr := fmt.Errorf("taskfabric: compile plan create %q: %w", s.ID, err)
			if len(delErrs) > 0 {
				// Join instead of %v-formatting so both the original create
				// error and every rollback failure stay reachable via
				// errors.Is/As on the returned error.
				return nil, errors.Join(rollErr,
					fmt.Errorf("rollback incomplete: %w", errors.Join(delErrs...)))
			}
			return nil, rollErr
		}
		created = append(created, s.ID)
	}
	return created, nil
}

// CompileNode compiles a single PlanStep into exactly one task. It is the
// entry point the incremental compiler uses when the live MutableDAG grows a
// node at runtime (TOOL_DAG_MAINLINE_DESIGN §4.1): one graph change, one
// task, and every other task of the graph is left untouched — including the
// ones that are currently RUNNING.
//
// The dependency rules are CompilePlan's, so a node may depend on a task
// compiled by an earlier batch (typically an already-COMPLETED plan node);
// depsCompletedLocked then reports it READY on the spot.
//
// Args:
//   - ctx: bounds the compile (see CompilePlan).
//   - step: the single step to compile; ID must be non-empty.
//
// Returns:
//   - string: the created task ID (== step.ID).
//   - error: ErrTaskExists / ErrTaskIDRequired / validation errors, wrapped.
func (f *Fabric) CompileNode(ctx context.Context, step PlanStep) (string, error) {
	ids, err := f.CompilePlan(ctx, []PlanStep{step})
	if err != nil {
		return "", err
	}
	if len(ids) != 1 {
		// Unreachable: CompilePlan returns one id per input step or an
		// error. Guarded so the 1:1 contract cannot silently widen.
		return "", fmt.Errorf("taskfabric: compile node %q: expected 1 task, got %d", step.ID, len(ids))
	}
	return ids[0], nil
}

// resolveDependencies validates the dependency closure of a compile batch.
//
// Resolution order is the whole point of this function:
//
//  1. inside the batch — the classic whole-graph compile;
//  2. a task already in the fabric — the runtime-growth case, where the new
//     node depends on something compiled by an earlier batch;
//  3. neither → an error, naming both ids.
//
// Order matters: a batch-local definition always wins, so a batch that
// redefines a node does not silently bind to a same-id task left behind from
// an earlier compile (that binding would resurrect a stale dependency edge).
//
// Caller must hold f.mu.
//
// Args:
//   - steps: the batch being compiled.
//   - byID: id → step for the batch.
//   - existing: the fabric's live task index (read-only here).
//
// Returns:
//   - error: the first unresolved dependency, or nil.
func resolveDependencies(steps []PlanStep, byID map[string]PlanStep, existing map[string]*Task) error {
	for _, s := range steps {
		for _, dep := range s.DependsOn {
			if _, ok := byID[dep]; ok {
				continue
			}
			if _, ok := existing[dep]; ok {
				continue
			}
			return fmt.Errorf("step %q depends on unknown step %q", s.ID, dep)
		}
	}
	return nil
}

// detectPlanCycle runs a depth-first color walk over the batch dependency
// graph and reports the first cycle found.
//
// Dependencies that resolve outside the batch (a task already in the fabric)
// are not walked: they existed before this batch, so no cycle introduced by
// this batch can pass through them, and treating them as zero-value batch
// members would pollute the walk with phantom nodes.
//
// Args:
//   - steps: the batch in input order.
//   - byID: id → step index for O(1) adjacency lookups.
//
// Returns:
//
//	error - a cycle description, or nil when the graph is a DAG.
func detectPlanCycle(steps []PlanStep, byID map[string]PlanStep) error {
	const (
		white = 0 // unvisited
		gray  = 1 // in the current DFS stack
		black = 2 // fully explored
	)
	color := make(map[string]int, len(steps))
	var visit func(id string, path []string) error
	visit = func(id string, path []string) error {
		color[id] = gray
		next := append(append([]string(nil), path...), id)
		for _, dep := range byID[id].DependsOn {
			if _, ok := byID[dep]; !ok {
				// Resolves to a task outside this batch: not ours to walk.
				continue
			}
			switch color[dep] {
			case gray:
				return fmt.Errorf("taskfabric: compile plan: dependency cycle: %v -> %s", next, dep)
			case white:
				if err := visit(dep, next); err != nil {
					return err
				}
			}
		}
		color[id] = black
		return nil
	}
	for _, s := range steps {
		if color[s.ID] == white {
			if err := visit(s.ID, nil); err != nil {
				return err
			}
		}
	}
	return nil
}
