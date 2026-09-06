package planprojection

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/taskfabric"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// chainDAG builds a root + n single-dependency tool nodes with NO subscription
// active: every growth event is "missed" by construction, so Reconcile is the
// only path that can materialize the tasks.
func chainDAG(t *testing.T, ctx context.Context, n int) *engine.MutableDAG {
	t.Helper()
	dag, err := engine.NewMutableDAG([]*engine.Step{{ID: "root", AgentType: "ares/root"}})
	require.NoError(t, err)
	prev := "root"
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i)
		require.NoError(t, dag.AddNode(ctx, &engine.Step{
			ID:        id,
			AgentType: "tool/echo",
			DependsOn: []string{prev},
		}))
		prev = id
	}
	return dag
}

// TestM15_ReconcileCreatesTasksForMissedBurst pins the missed-burst
// compensation: a wholly-missed burst (no subscription was ever active)
// converges in ONE Reconcile — root admission plus every node gets its task,
// with dependencies intact.
func TestM15_ReconcileCreatesTasksForMissedBurst(t *testing.T) {
	ctx := context.Background()
	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	dag := chainDAG(t, ctx, 10)

	res, err := coord.Reconcile(ctx, dag)
	require.NoError(t, err)
	require.Equal(t, engine.ChangeReconcile, res.Change)
	require.Len(t, res.Created, 11, "root admission + every missed node becomes a task in one pass")
	require.Empty(t, res.Skipped)
	require.Len(t, fabric.IDs(), 11)

	n9, err := fabric.Task("n9")
	require.NoError(t, err)
	require.Equal(t, []string{"n8"}, n9.Dependencies, "reconciled tasks keep their graph dependencies")
}

// TestM15_ReconcileAdoptsPreexistingTasks pins the restart case: tasks that
// predate the coordinator (batch-compiled outside the event path) are adopted
// into the tracked set and refreshed — not failed on ErrTaskExists.
func TestM15_ReconcileAdoptsPreexistingTasks(t *testing.T) {
	ctx := context.Background()
	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)

	_, err := fabric.CompilePlan(ctx, []taskfabric.PlanStep{
		{ID: "root", Capability: "ares/root", Payload: map[string]any{"input": "prompt"}},
		{ID: "n1", Capability: "tool/echo", DependsOn: []string{"root"}, Payload: map[string]any{"arg.q": "x"}},
	})
	require.NoError(t, err)
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "root", AgentType: "ares/root"},
		{ID: "n1", AgentType: "tool/echo", DependsOn: []string{"root"}, Metadata: map[string]string{"arg.q": "x"}},
	})
	require.NoError(t, err)

	res, err := coord.Reconcile(ctx, dag)
	require.NoError(t, err)
	require.Empty(t, res.Created, "nothing to create: both tasks predate the coordinator")
	require.Empty(t, res.Skipped)
	require.Equal(t, res.CompileID, coord.LastChange().CompileID)
	require.ElementsMatch(t, []string{"root", "n1"}, sortedIDs(t, fabric))
}

// TestM15_ReconcileRefreshesAdoptedTaskFromGraph pins that adoption CONVERGES:
// a task that predates the coordinator carrying a stale scheduling shape (no
// dependencies — so it is READY and would run BEFORE its predecessor) is
// rewritten from the graph, which is the source of truth. Adopting without
// refreshing would leave the ordering violation the reconcile exists to fix.
func TestM15_ReconcileRefreshesAdoptedTaskFromGraph(t *testing.T) {
	ctx := context.Background()
	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)

	// Compiled outside the coordinator: n1 has NO deps and a stale arg.
	_, err := fabric.CompilePlan(ctx, []taskfabric.PlanStep{
		{ID: "root", Capability: "ares/root", Payload: map[string]any{"input": "p"}},
		{ID: "n1", Capability: "tool/echo", Payload: map[string]any{"arg.q": "stale"}},
	})
	require.NoError(t, err)
	require.Contains(t, fabric.ReadyTasks(), "n1", "precondition: the stale task is runnable out of order")

	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "root", AgentType: "ares/root"},
		{ID: "n1", AgentType: "tool/echo", DependsOn: []string{"root"},
			Metadata: map[string]string{"arg.q": "fresh"}},
	})
	require.NoError(t, err)

	res, err := coord.Reconcile(ctx, dag)
	require.NoError(t, err)
	require.Empty(t, res.Skipped)
	require.Contains(t, res.Updated, "n1", "an adopted task is reported as moved, not as created")

	n1, err := fabric.Task("n1")
	require.NoError(t, err)
	require.Equal(t, []string{"root"}, n1.Dependencies, "deps are rewritten from the graph")
	require.NotContains(t, fabric.ReadyTasks(), "n1", "the restored edge blocks n1 until root completes")

	envelope, err := taskfabric.DecodeCheckpoint(n1.Checkpoint)
	require.NoError(t, err)
	require.Equal(t, "fresh", envelope.Payload["arg.q"], "payload is re-projected from the graph")
}

// TestM15_RedundantRemoveIsConvergenceNotRefusal pins the at-least-once
// delete: a Reconcile that compensated a dropped event deletes the task
// BEFORE the event is delivered, so the delivered ChangeRemoveNode finds
// nothing to delete. The postcondition already holds, so the change is a
// no-op — reporting it as Skipped would turn the compensation path into
// permanent warn noise and make Complete report a failure that never happened.
func TestM15_RedundantRemoveIsConvergenceNotRefusal(t *testing.T) {
	ctx := context.Background()
	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "root", AgentType: "ares/root"},
		{ID: "n1", AgentType: "tool/echo", DependsOn: []string{"root"}},
	})
	require.NoError(t, err)
	_, err = coord.CompileDAG(ctx, dag)
	require.NoError(t, err)

	require.NoError(t, dag.RemoveNode(ctx, "n1"))
	rec, err := coord.Reconcile(ctx, dag)
	require.NoError(t, err)
	require.Equal(t, []string{"n1"}, rec.Removed)

	res, err := coord.ApplyChange(ctx, dag, engine.GraphEvent{
		Change:  engine.GraphChange{Type: engine.ChangeRemoveNode, NodeID: "n1"},
		Success: true,
	})
	require.NoError(t, err)
	require.Empty(t, res.Skipped, "an already-absent task is converged, not refused")
	require.True(t, res.Complete())
	require.Empty(t, res.Removed, "this change removed nothing: the reconcile already did")
}

// TestM15_ReplaceNodeAdoptsTaskCreatedByReconcile pins the third at-least-once
// site: the replacement task may already exist because a Reconcile
// compensated the dropped ReplaceNode event first. The delivered event must
// still migrate the successors instead of dying on ErrTaskExists.
func TestM15_ReplaceNodeAdoptsTaskCreatedByReconcile(t *testing.T) {
	ctx := context.Background()
	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "root", AgentType: "ares/root"},
		{ID: "old", AgentType: "tool/echo", DependsOn: []string{"root"}},
		{ID: "succ", AgentType: "tool/echo", DependsOn: []string{"old"}},
	})
	require.NoError(t, err)
	_, err = coord.CompileDAG(ctx, dag)
	require.NoError(t, err)

	replacement := &engine.Step{ID: "new", AgentType: "tool/read", DependsOn: []string{"root"}}
	require.NoError(t, dag.ReplaceNode(ctx, "old", replacement))
	// The event was dropped; the reconcile compensates it first.
	_, err = coord.Reconcile(ctx, dag)
	require.NoError(t, err)

	res, err := coord.ApplyChange(ctx, dag, engine.GraphEvent{
		Change: engine.GraphChange{
			Type: engine.ChangeReplaceNode, NodeID: "new", OldNodeID: "old", Step: replacement,
		},
		Success: true,
	})
	require.NoError(t, err, "the replacement task already existing is convergence, not failure")
	require.Empty(t, res.Skipped)

	succ, err := fabric.Task("succ")
	require.NoError(t, err)
	require.Equal(t, []string{"new"}, succ.Dependencies, "the successor is migrated onto the replacement")
	require.ElementsMatch(t, []string{"root", "new", "succ"}, sortedIDs(t, fabric))
}

// TestM15_ReconcileDeletesStaleTrackedTasks pins the other direction: a task
// the coordinator tracks but the graph no longer holds is deleted.
func TestM15_ReconcileDeletesStaleTrackedTasks(t *testing.T) {
	ctx := context.Background()
	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "root", AgentType: "ares/root"},
		{ID: "n1", AgentType: "tool/echo", DependsOn: []string{"root"}},
	})
	require.NoError(t, err)

	_, err = coord.CompileDAG(ctx, dag)
	require.NoError(t, err)
	require.NoError(t, dag.RemoveNode(ctx, "n1"))

	res, err := coord.Reconcile(ctx, dag)
	require.NoError(t, err)
	require.Equal(t, []string{"n1"}, res.Removed)
	_, err = fabric.Task("n1")
	require.ErrorIs(t, err, taskfabric.ErrTaskNotFound)
}
