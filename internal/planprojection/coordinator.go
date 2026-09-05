// Package planprojection provides the single projection function from
// workflow engine steps to taskfabric PlanSteps, plus a compile
// coordinator that records compile provenance for introspection.
//
// The projection lives in its own package (not in taskfabric) so the
// kernel never imports the planner package — the caller (cmd layer)
// projects engine.Step onto PlanStep, then hands the batch to
// Fabric.CompilePlan.
//
// Two compile paths exist, and the difference between them is the whole
// point of TOOL_DAG_MAINLINE_DESIGN §4.1:
//
//   - CompileDAG is the FULL path (cold start, ResetFromSteps): it reclaims
//     the tasks of the previous compile and rebuilds the batch.
//   - ApplyChange is the INCREMENTAL path (runtime graph growth): one graph
//     change moves one task. It never deletes a task it was not asked to
//     delete, so a RUNNING task is never torn down underneath its owner.
package planprojection

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Timwood0x10/ares/internal/ares_events"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// CompileCoordinator manages the projection → CompilePlan pipeline. It
// holds a reference to the task fabric and the event store, records
// compile provenance, and supports event-driven recompilation from
// MutableDAG GraphEvents.
type CompileCoordinator struct {
	fabric     *taskfabric.Fabric
	store      ares_events.EventStore
	generation int // tracks the current evolution generation

	// lastCompile is the most recent compile record (for introspection).
	mu          sync.RWMutex
	lastCompile CompileRecord

	// lastChange is the most recent incremental compile result
	// (ApplyChange). Kept beside lastCompile because "which task did the
	// last graph change move" is the question an operator asks when the
	// graph and the task set disagree.
	lastChange ChangeResult

	// planIDs is the set of fabric tasks this coordinator has created from
	// the DAG — the materialized answer to "what does the graph currently
	// map to". Incremental compiles mutate it in place (one id per
	// AddNode/RemoveNode) instead of rebuilding the batch, which is why
	// lastCompile.StepCount stays truthful between full compiles.
	planIDs map[string]struct{}

	// compileSeq generates unique compile IDs.
	compileSeq uint64
}

// NewCompileCoordinator creates a coordinator wired to the given fabric
// and event store. Either may be nil for testing (the methods are
// nil-safe and degrade gracefully).
func NewCompileCoordinator(fabric *taskfabric.Fabric, store ares_events.EventStore) *CompileCoordinator {
	return &CompileCoordinator{
		fabric: fabric,
		store:  store,
	}
}

// SetGeneration sets the current evolution generation. Called by the
// GA lifecycle when a new generation starts.
func (c *CompileCoordinator) SetGeneration(gen int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation = gen
}

// CompileDAG projects the DAG's steps into PlanSteps and calls
// Fabric.CompilePlan. It records a compile event with the generation,
// DAG version, compile ID, and plan IDs for introspection.
//
// This is the FULL compile path: it reclaims every task of the previous
// compile before rebuilding the batch. Use it for cold start and for
// ResetFromSteps (where the whole topology may have changed at once).
// Runtime graph mutations go through ApplyChange instead — a full rebuild
// cannot reclaim a RUNNING task, so it fails the whole batch with
// ErrTaskExists and the graph change is lost.
func (c *CompileCoordinator) CompileDAG(ctx context.Context, dag *engine.MutableDAG) (CompileRecord, error) {
	if dag == nil {
		return CompileRecord{}, fmt.Errorf("planprojection: compile DAG: nil dag")
	}
	if c == nil || c.fabric == nil {
		return CompileRecord{}, fmt.Errorf("planprojection: compile DAG: nil fabric")
	}

	dagVersion := dag.Version()
	steps := dag.Steps()
	planSteps := ProjectSteps(steps)

	compileID := c.nextCompileID()

	c.mu.RLock()
	generation := c.generation
	oldIDs := c.trackedIDsLocked()
	c.mu.RUnlock()

	// Reclaim the previous compile's tasks so the rebuild does not hit
	// ErrTaskExists. Best-effort: a task already acquired by a scheduler
	// cannot be deleted (it is owned), so it survives. That is NOT silent —
	// the ids are collected and folded into the error the rebuild then
	// produces, so "why did the recompile fail" is answerable from the error
	// instead of from a guess.
	var undeletable []string
	for _, id := range oldIDs {
		if err := c.fabric.Delete(id); err != nil {
			undeletable = append(undeletable, fmt.Sprintf("%s (%v)", id, err))
		}
	}

	planIDs, err := c.fabric.CompilePlan(ctx, planSteps)
	record := CompileRecord{
		Generation: generation,
		DAGVersion: dagVersion,
		CompileID:  compileID,
		PlanIDs:    planIDs,
		StepCount:  len(planSteps),
	}

	if err != nil {
		if len(undeletable) > 0 {
			return record, fmt.Errorf("planprojection: compile DAG: %w (tasks that could not be reclaimed: %s)",
				err, strings.Join(undeletable, ", "))
		}
		return record, fmt.Errorf("planprojection: compile DAG: %w", err)
	}

	c.mu.Lock()
	c.lastCompile = record
	c.planIDs = make(map[string]struct{}, len(planIDs))
	for _, id := range planIDs {
		c.planIDs[id] = struct{}{}
	}
	c.mu.Unlock()

	c.recordCompileEvent(ctx, record, nil)

	return record, nil
}

// LastCompile returns the most recent compile record. Safe for concurrent
// access; returns a zero value if no compile has happened yet.
func (c *CompileCoordinator) LastCompile() CompileRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastCompile
}

// CompileCount returns the total number of compile actions since startup
// (C5.1/C5.3). This is the compileSeq counter — the same source that
// generates compile IDs — exposed for introspection and metrics. A flat
// zero means no compile has fired, which indicates the GraphEvent
// subscription is not wired or the DAG has not been mutated.
//
// Both paths count: a full compile and an incremental apply are each one
// compile action.
func (c *CompileCoordinator) CompileCount() uint64 {
	return atomic.LoadUint64(&c.compileSeq)
}

// CompileID returns the most recent compile's unique identifier (C5.2).
// Empty when no compile has happened yet.
func (c *CompileCoordinator) CompileID() string {
	return c.LastCompile().CompileID
}

// DAGVersion returns the live DAG's mutation counter at the last compile
// (C5.2). Zero when no compile has happened yet.
func (c *CompileCoordinator) DAGVersion() uint64 {
	return c.LastCompile().DAGVersion
}

// SkippedOp.Op vocabulary. Declared once so goconst stays quiet and the set
// of actions the compiler can skip is grep-able.
const (
	opDelete          = "delete"
	opSetDependencies = "set_dependencies"
	opUpdatePayload   = "update_payload"
)

// SkippedOp is one incremental-compile action that could not be applied
// because the target task was in a state that forbids it (RUNNING/LEASED/
// SUSPENDED). It is returned rather than dropped: a graph change the
// compiler quietly swallowed is exactly the class of silent divergence
// this package exists to prevent, and the caller (the event subscription)
// logs every entry.
type SkippedOp struct {
	// TaskID is the task the action targeted.
	TaskID string
	// Op is the attempted action: "delete", "set_dependencies",
	// "update_payload".
	Op string
	// Err is why it was refused — normally wrapping
	// taskfabric.ErrTaskNotMutable or taskfabric.ErrTaskUndeletable.
	Err error
}

// ChangeResult reports what one incremental compile did.
type ChangeResult struct {
	// Change is the graph change that was projected.
	Change engine.ChangeType
	// CompileID identifies this compile action ("" when the change was a
	// no-op, i.e. evt.Success == false).
	CompileID string
	// DAGVersion is the live DAG's mutation counter after the change.
	DAGVersion uint64
	// Created / Removed / Updated hold the task ids touched, by action.
	Created []string
	Removed []string
	Updated []string
	// Skipped lists actions that could not be applied. Empty means the
	// change was projected completely.
	Skipped []SkippedOp
}

// Complete reports whether the change was projected without any skipped
// action.
func (r ChangeResult) Complete() bool { return len(r.Skipped) == 0 }

// markUpdated records id as updated, at most once per change: a composite
// change (ReplaceNode rewrites both deps and payload) touches one task twice
// and must not report two updated tasks.
func (r *ChangeResult) markUpdated(id string) {
	for _, got := range r.Updated {
		if got == id {
			return
		}
	}
	r.Updated = append(r.Updated, id)
}

// ApplyChange projects ONE graph change onto the fabric — the incremental
// compile path behind runtime graph growth (TOOL_DAG_MAINLINE_DESIGN §4.1).
//
// It is dispatching on ChangeType, not recompiling, because a full rebuild
// is exactly what the growth path cannot survive: Fabric.Delete refuses a
// RUNNING task, the rebuild then collides with it via ErrTaskExists, and
// CompilePlan's all-or-nothing rollback discards the whole batch — so the
// newly grown node never becomes a task.
//
// Args:
//   - ctx: bounds the compile.
//   - dag: the live DAG the change was applied to (the source of truth for
//     every step the change touches; the event is only a notification).
//   - evt: the graph event. A failed mutation (evt.Success == false) is a
//     no-op: nothing changed, so there is nothing to project.
//
// Returns:
//   - ChangeResult: what was created/removed/updated, and what was skipped.
//   - error: only when the change itself could not be projected at all
//     (a structural failure). Per-task refusals are in ChangeResult.Skipped,
//     not here — one immovable task must not fail the rest of the change.
func (c *CompileCoordinator) ApplyChange(ctx context.Context, dag *engine.MutableDAG, evt engine.GraphEvent) (ChangeResult, error) {
	if c == nil || c.fabric == nil {
		return ChangeResult{}, fmt.Errorf("planprojection: apply change: nil fabric")
	}
	if dag == nil {
		return ChangeResult{}, fmt.Errorf("planprojection: apply change: nil dag")
	}
	if !evt.Success {
		// Reporting a compile here would be a lie: it would stamp the
		// pre-change topology as the result of a change that never happened.
		return ChangeResult{Change: evt.Change.Type, DAGVersion: dag.Version()}, nil
	}

	res := ChangeResult{
		Change:     evt.Change.Type,
		CompileID:  c.nextCompileID(),
		DAGVersion: dag.Version(),
	}

	switch evt.Change.Type {
	case engine.ChangeAddNode:
		if err := c.applyAddNode(ctx, dag, evt.Change, &res); err != nil {
			return res, err
		}
	case engine.ChangeRemoveNode:
		c.applyRemoveNode(evt.Change, &res)
	case engine.ChangeAddEdge, engine.ChangeRemoveEdge:
		c.applyEdgeChange(dag, evt.Change, &res)
	case engine.ChangeSetNodeMetadata:
		c.applyMetadataChange(dag, evt.Change, &res)
	case engine.ChangeReplaceNode:
		if err := c.applyReplaceNode(ctx, dag, evt.Change, &res); err != nil {
			return res, err
		}
	default:
		return res, fmt.Errorf("planprojection: apply change: unknown change type %d", evt.Change.Type)
	}

	c.mu.Lock()
	c.lastCompile = CompileRecord{
		Generation: c.generation,
		DAGVersion: res.DAGVersion,
		CompileID:  res.CompileID,
		PlanIDs:    c.trackedIDsLocked(),
		StepCount:  len(c.planIDs),
	}
	record := c.lastCompile
	c.lastChange = res
	c.mu.Unlock()

	c.recordCompileEvent(ctx, record, &res)

	return res, nil
}

// LastChange returns the most recent incremental compile result. Zero-valued
// until ApplyChange runs. Safe for concurrent access.
func (c *CompileCoordinator) LastChange() ChangeResult {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastChange
}

// applyAddNode creates exactly one task for the added node. Its
// dependencies resolve against tasks already in the fabric, so a node grown
// onto an already-COMPLETED predecessor is READY immediately.
func (c *CompileCoordinator) applyAddNode(ctx context.Context, dag *engine.MutableDAG, ch engine.GraphChange, res *ChangeResult) error {
	step := ch.Step
	if step == nil {
		step = stepFor(dag, ch.NodeID)
	}
	if step == nil {
		return fmt.Errorf("add node %q: step not found in the live DAG", ch.NodeID)
	}
	id, err := c.fabric.CompileNode(ctx, ProjectStep(step))
	if err != nil {
		return fmt.Errorf("add node %q: %w", ch.NodeID, err)
	}
	c.addTracked(id)
	res.Created = append(res.Created, id)
	return nil
}

// applyRemoveNode deletes the removed node's task. A task a scheduler has
// already taken cannot be deleted — and must not be: dropping it mid-quantum
// would strand the runner. It stays tracked so a later Delete (or the next
// full compile) reclaims it, and the refusal is reported, not swallowed.
func (c *CompileCoordinator) applyRemoveNode(ch engine.GraphChange, res *ChangeResult) {
	if err := c.fabric.Delete(ch.NodeID); err != nil {
		res.Skipped = append(res.Skipped, SkippedOp{TaskID: ch.NodeID, Op: opDelete, Err: err})
		return
	}
	c.removeTracked(ch.NodeID)
	res.Removed = append(res.Removed, ch.NodeID)
}

// applyEdgeChange rewrites one task's dependencies to match the DAG. The
// DAG is the source of truth, not the event: an AddEdge and a RemoveEdge
// both end at "the target's deps are whatever the graph now says".
func (c *CompileCoordinator) applyEdgeChange(dag *engine.MutableDAG, ch engine.GraphChange, res *ChangeResult) {
	if stepFor(dag, ch.ToID) == nil {
		// The node is already gone (the handler runs asynchronously, so the
		// graph may have moved on). Rewriting its task's deps to "none"
		// would be projecting a state that never existed.
		res.Skipped = append(res.Skipped, SkippedOp{
			TaskID: ch.ToID, Op: opSetDependencies, Err: engine.ErrNodeNotFound,
		})
		return
	}
	if err := c.fabric.SetDependencies(ch.ToID, dag.ReadDeps(ch.ToID)); err != nil {
		res.Skipped = append(res.Skipped, SkippedOp{TaskID: ch.ToID, Op: opSetDependencies, Err: err})
		return
	}
	res.markUpdated(ch.ToID)
}

// applyMetadataChange rewrites a task's payload in place. It deliberately
// does NOT recreate the task: a pure attribute patch must not reset the
// task's CreatedAt (which would also re-stamp its submission-time strategy
// attribution, E1) nor disturb anything that already references it.
func (c *CompileCoordinator) applyMetadataChange(dag *engine.MutableDAG, ch engine.GraphChange, res *ChangeResult) {
	step := ch.Step
	if step == nil {
		step = stepFor(dag, ch.NodeID)
	}
	if step == nil {
		res.Skipped = append(res.Skipped, SkippedOp{
			TaskID: ch.NodeID, Op: opUpdatePayload, Err: engine.ErrNodeNotFound,
		})
		return
	}
	if err := c.fabric.UpdatePayload(ch.NodeID, ProjectStep(step).Payload); err != nil {
		res.Skipped = append(res.Skipped, SkippedOp{TaskID: ch.NodeID, Op: opUpdatePayload, Err: err})
		return
	}
	res.markUpdated(ch.NodeID)
}

// applyReplaceNode creates the replacement task, migrates the old node's
// successors onto it, then deletes the old task.
//
// Order matters: create → migrate → delete. A failure part-way leaves the
// graph still runnable (the old task is still there, or the new one is),
// whereas deleting first would strand the successors on a missing
// dependency.
//
// A same-ID replacement is an in-place rewrite, not a create/delete pair:
// ReplaceNode keeps the node's identity, so the fabric must too.
func (c *CompileCoordinator) applyReplaceNode(ctx context.Context, dag *engine.MutableDAG, ch engine.GraphChange, res *ChangeResult) error {
	newID, oldID := ch.NodeID, ch.OldNodeID
	if newID == oldID {
		if stepFor(dag, newID) == nil {
			return fmt.Errorf("replace node %q: step not found in the live DAG", newID)
		}
		if err := c.fabric.SetDependencies(newID, dag.ReadDeps(newID)); err != nil {
			res.Skipped = append(res.Skipped, SkippedOp{TaskID: newID, Op: opSetDependencies, Err: err})
		} else {
			res.markUpdated(newID)
		}
		c.applyMetadataChange(dag, ch, res)
		return nil
	}

	step := ch.Step
	if step == nil {
		step = stepFor(dag, newID)
	}
	if step == nil {
		return fmt.Errorf("replace node %q with %q: step not found in the live DAG", oldID, newID)
	}
	id, err := c.fabric.CompileNode(ctx, ProjectStep(step))
	if err != nil {
		return fmt.Errorf("replace node %q with %q: %w", oldID, newID, err)
	}
	c.addTracked(id)
	res.Created = append(res.Created, id)

	// Successors: ReplaceNode already migrated the edges in the DAG, so the
	// DAG holds each successor's post-replacement dependency list — rewrite
	// from the graph rather than patching id strings into fabric state.
	for _, succ := range c.fabric.Dependents(oldID) {
		if succ == newID {
			continue
		}
		if stepFor(dag, succ) == nil {
			res.Skipped = append(res.Skipped, SkippedOp{
				TaskID: succ, Op: opSetDependencies, Err: engine.ErrNodeNotFound,
			})
			continue
		}
		if err := c.fabric.SetDependencies(succ, dag.ReadDeps(succ)); err != nil {
			res.Skipped = append(res.Skipped, SkippedOp{TaskID: succ, Op: opSetDependencies, Err: err})
			continue
		}
		res.markUpdated(succ)
	}

	c.applyRemoveNode(engine.GraphChange{Type: engine.ChangeRemoveNode, NodeID: oldID}, res)
	return nil
}

// SubscribeGraphEvents subscribes to GraphEvents from the MutableDAG and
// projects each mutation onto the fabric through the incremental path. The
// subscription is managed: it is cleaned up when ctx is cancelled. The
// returned function can be called to unsubscribe early (e.g. during
// shutdown).
//
// This closes the "two graphs" gap: a GraphPatchExecutor mutation on the
// live MutableDAG reaches the task set so the next scheduler drain sees the
// updated topology.
func (c *CompileCoordinator) SubscribeGraphEvents(ctx context.Context, dag *engine.MutableDAG) func() {
	if dag == nil {
		return func() {}
	}
	subID, ch := dag.SubscribeWithID()
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				dag.Unsubscribe(subID)
				return
			case evt, ok := <-ch:
				if !ok {
					return
				}
				compileCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				res, err := c.ApplyChange(compileCtx, dag, evt)
				cancel()
				if err != nil {
					log.Error("planprojection: incremental compile failed",
						"change", int(evt.Change.Type), "node", evt.Change.NodeID, "error", err)
					continue
				}
				// Not silent: every task the compiler could not move is
				// logged with the reason, so "the graph changed but the
				// task set did not" is always attributable.
				for _, s := range res.Skipped {
					log.Warn("planprojection: incremental compile skipped action",
						"op", s.Op, "task_id", s.TaskID, "compile_id", res.CompileID, "error", s.Err)
				}
			}
		}
	}()

	return func() {
		dag.Unsubscribe(subID)
		<-done
	}
}

// stepFor returns the DAG's current step for id, or nil when the node is
// gone. The event handler runs asynchronously, so by the time a change is
// projected the graph may have moved on; nil means "nothing to project",
// never "project an empty step".
func stepFor(dag *engine.MutableDAG, id string) *engine.Step {
	return dag.StepIndex()[id]
}

// addTracked records a task id as compiled from the DAG.
func (c *CompileCoordinator) addTracked(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.planIDs == nil {
		c.planIDs = make(map[string]struct{}, 1)
	}
	c.planIDs[id] = struct{}{}
}

// removeTracked drops a task id from the compiled set.
func (c *CompileCoordinator) removeTracked(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.planIDs, id)
}

// trackedIDsLocked returns the tracked task ids, sorted for determinism.
// Caller must hold c.mu (either lock).
func (c *CompileCoordinator) trackedIDsLocked() []string {
	out := make([]string, 0, len(c.planIDs))
	for id := range c.planIDs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// nextCompileID generates a unique compile identifier.
func (c *CompileCoordinator) nextCompileID() string {
	n := atomic.AddUint64(&c.compileSeq, 1)
	return fmt.Sprintf("compile-%d", n)
}

// recordCompileEvent writes a compile lifecycle event to the event store
// for introspection. Best-effort: errors are not surfaced to the caller.
//
// res is non-nil only for incremental compiles; it enriches the event with
// the change type and the per-action outcome, which is what makes "this
// compile moved exactly one task" auditable after the fact.
func (c *CompileCoordinator) recordCompileEvent(ctx context.Context, record CompileRecord, res *ChangeResult) {
	if c.store == nil {
		return
	}
	payload := map[string]any{
		"generation":  record.Generation,
		"dag_version": record.DAGVersion,
		"compile_id":  record.CompileID,
		"plan_ids":    record.PlanIDs,
		"step_count":  record.StepCount,
	}
	if res != nil {
		payload["incremental"] = true
		payload["change_type"] = int(res.Change)
		payload["created"] = res.Created
		payload["removed"] = res.Removed
		payload["updated"] = res.Updated
		payload["skipped"] = len(res.Skipped)
	}
	evt := &ares_events.Event{
		ID:        fmt.Sprintf("compile-%s", record.CompileID),
		StreamID:  "evolution.compile",
		Type:      ares_events.EventType("evolution.compile"),
		Payload:   payload,
		Timestamp: time.Now(),
	}
	_ = c.store.Append(ctx, "evolution.compile", []*ares_events.Event{evt}, -1)
}
