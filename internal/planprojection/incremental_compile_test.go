package planprojection

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/taskfabric"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// M0 acceptance suite (TOOL_DAG_MAINLINE_DESIGN §4.1).
//
// These four assertions are the gate for the whole mainline: without an
// incremental compiler, runtime graph growth (M2's plannerCognition growing
// tool nodes) dies on ErrTaskExists — verified, not estimated. Every test
// here drives a real Fabric and a real MutableDAG; none of them recompile.

// waitForChange drives one graph mutation through the event subscription and
// returns the resulting ChangeResult.
//
// It waits on LastChange().CompileID, not on CompileCount(): the compile id is
// allocated at the START of ApplyChange while the result is stored at the END,
// so the counter can be observed before the result is readable — polling it
// reads a half-finished compile. CompileID is written under the same mutex as
// the result, so observing a new id guarantees the result is complete.
func waitForChange(t *testing.T, coord *CompileCoordinator, mutation func()) ChangeResult {
	t.Helper()
	before := coord.LastChange().CompileID
	mutation()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if coord.LastChange().CompileID != before {
			return coord.LastChange()
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("incremental compile did not fire for the mutation")
	return ChangeResult{}
}

// sortedIDs returns the fabric's task ids in a deterministic order —
// Fabric.IDs iterates a map, so asserting on its raw output would flake.
func sortedIDs(t *testing.T, fabric *taskfabric.Fabric) []string {
	t.Helper()
	ids := fabric.IDs()
	sort.Strings(ids)
	return ids
}

// Assertion 1 (§4.1): one AddNode creates exactly one task, and every other
// task keeps its CreatedAt — proving nothing was rebuilt.
func TestM0_AddNodeCreatesExactlyOneTaskWithoutRebuilding(t *testing.T) {
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "plan", AgentType: "ares/plan"},
		{ID: "grep", AgentType: "tool/grep", DependsOn: []string{"plan"}},
	})
	require.NoError(t, err)

	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	_, err = coord.CompileDAG(context.Background(), dag)
	require.NoError(t, err)

	before := createdAtSnapshot(t, fabric)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unsub := coord.SubscribeGraphEvents(ctx, dag)
	defer unsub()

	res := waitForChange(t, coord, func() {
		require.NoError(t, dag.AddNode(ctx, &engine.Step{
			ID:        "read",
			AgentType: "tool/read",
			DependsOn: []string{"grep"},
		}))
	})
	require.Empty(t, res.Skipped, "nothing should be skipped on a plain AddNode")
	require.Equal(t, []string{"read"}, res.Created)
	require.Empty(t, res.Removed)
	require.Empty(t, res.Updated)

	// Exactly one new task exists.
	assert.Equal(t, []string{"grep", "plan", "read"}, sortedIDs(t, fabric))

	// Pre-existing tasks were NOT rebuilt: identity (CreatedAt) is intact.
	after := createdAtSnapshot(t, fabric)
	for id, at := range before {
		got, ok := after[id]
		require.True(t, ok, "task %q disappeared", id)
		assert.Equal(t, at, got, "task %q was rebuilt (CreatedAt moved)", id)
	}

	// The new node is compiled with the graph's dependency, not with none.
	task, err := fabric.Task("read")
	require.NoError(t, err)
	assert.Equal(t, []string{"grep"}, task.Dependencies)
}

// Assertion 2 (§4.1): a node grown onto an already-COMPLETED task is READY
// on the spot. This is the whole point of cross-batch dependency resolution
// — without it CompilePlan rejects the dependency as unknown.
func TestM0_NodeDependingOnCompletedTaskIsReadyImmediately(t *testing.T) {
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "plan", AgentType: "ares/plan"},
	})
	require.NoError(t, err)

	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	_, err = coord.CompileDAG(context.Background(), dag)
	require.NoError(t, err)

	// Drive "plan" to COMPLETED through the real state machine.
	epoch, err := fabric.Acquire("plan", "agent-1", time.Minute)
	require.NoError(t, err)
	require.NoError(t, fabric.Start("plan", "agent-1", epoch))
	require.NoError(t, fabric.Complete("plan", "agent-1", epoch))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unsub := coord.SubscribeGraphEvents(ctx, dag)
	defer unsub()

	waitForChange(t, coord, func() {
		require.NoError(t, dag.AddNode(ctx, &engine.Step{
			ID:        "grep",
			AgentType: "tool/grep",
			DependsOn: []string{"plan"},
		}))
	})

	ready, err := fabric.IsReady("grep")
	require.NoError(t, err)
	assert.True(t, ready, "a node depending on a COMPLETED task must be READY immediately")

	// It must also show up in the scheduler's work source.
	assert.Contains(t, fabric.ReadyTasks(), "grep")
}

// Assertion 3 (§4.1) — the one that matters most: with a RUNNING task in the
// fabric, AddNode still compiles, and the RUNNING task is untouched.
//
// Under the old full-recompile path this scenario failed the entire batch:
// Delete refuses a RUNNING task (ErrTaskUndeletable), the rebuild then hits
// ErrTaskExists on the same id, and CompilePlan rolls the whole batch back —
// so the newly grown node never became a task.
func TestM0_AddNodeWithRunningTaskPresentLeavesItUntouched(t *testing.T) {
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "plan", AgentType: "ares/plan"},
		{ID: "slow", AgentType: "tool/slow", DependsOn: []string{"plan"}},
	})
	require.NoError(t, err)

	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	_, err = coord.CompileDAG(context.Background(), dag)
	require.NoError(t, err)

	// "slow" is mid-quantum: LEASED → RUNNING, holding an epoch.
	epoch, err := fabric.Acquire("slow", "agent-1", time.Minute)
	require.NoError(t, err)
	require.NoError(t, fabric.Start("slow", "agent-1", epoch))
	before := createdAtSnapshot(t, fabric)["slow"]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unsub := coord.SubscribeGraphEvents(ctx, dag)
	defer unsub()

	res := waitForChange(t, coord, func() {
		require.NoError(t, dag.AddNode(ctx, &engine.Step{
			ID:        "read",
			AgentType: "tool/read",
			DependsOn: []string{"plan"},
		}))
	})
	require.Empty(t, res.Skipped)
	require.Equal(t, []string{"read"}, res.Created)

	// The compile succeeded and the RUNNING task is exactly as it was:
	// same state, same owner, same epoch, same identity.
	running, err := fabric.Task("slow")
	require.NoError(t, err)
	assert.Equal(t, taskfabric.StateRunning, running.State)
	assert.Equal(t, "agent-1", running.Owner)
	assert.Equal(t, epoch, running.Lease.Epoch)
	assert.Equal(t, before, running.CreatedAt, "the RUNNING task must not be recreated")

	// And the new node was compiled despite the in-flight work.
	_, err = fabric.Task("read")
	require.NoError(t, err)

	// The contrast is the point of M0: a FULL recompile in this state fails.
	fullCtx := context.Background()
	_, err = coord.CompileDAG(fullCtx, dag)
	require.Error(t, err, "a full recompile cannot reclaim a RUNNING task — that is why M0 exists")
	assert.Contains(t, err.Error(), "slow", "the error must name the task that could not be reclaimed")
}

// Assertion 4 (§4.1): SetNodeMetadata rewrites the payload in place — the
// task is not recreated, but the new metadata is visible to the executor.
func TestM0_SetNodeMetadataUpdatesPayloadWithoutRecreatingTask(t *testing.T) {
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "grep", AgentType: "tool/grep", Metadata: map[string]string{"budget": "3"}},
	})
	require.NoError(t, err)

	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	_, err = coord.CompileDAG(context.Background(), dag)
	require.NoError(t, err)

	before := createdAtSnapshot(t, fabric)["grep"]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unsub := coord.SubscribeGraphEvents(ctx, dag)
	defer unsub()

	res := waitForChange(t, coord, func() {
		require.NoError(t, dag.SetNodeMetadata("grep", map[string]string{"budget": "1"}))
	})
	require.Empty(t, res.Skipped)
	require.Equal(t, []string{"grep"}, res.Updated)
	require.Empty(t, res.Created, "a metadata patch must never recreate the task")

	task, err := fabric.Task("grep")
	require.NoError(t, err)
	assert.Equal(t, before, task.CreatedAt, "CreatedAt must not move — the task was not rebuilt")

	dc, err := taskfabric.DecodeCheckpoint(task.Checkpoint)
	require.NoError(t, err)
	assert.Equal(t, "1", dc.Payload["budget"], "the new metadata must reach the task payload")
}

// AddEdge / RemoveEdge move exactly one task's Dependencies.
func TestM0_EdgeChangesRewriteOneTask(t *testing.T) {
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "a", AgentType: "x"},
		{ID: "b", AgentType: "y"},
	})
	require.NoError(t, err)

	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	_, err = coord.CompileDAG(context.Background(), dag)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unsub := coord.SubscribeGraphEvents(ctx, dag)
	defer unsub()

	res := waitForChange(t, coord, func() {
		require.NoError(t, dag.AddEdge(ctx, "a", "b"))
	})
	require.Equal(t, []string{"b"}, res.Updated)
	require.Empty(t, res.Created)

	task, err := fabric.Task("b")
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, task.Dependencies)

	res = waitForChange(t, coord, func() {
		require.NoError(t, dag.RemoveEdge(ctx, "a", "b"))
	})
	require.Equal(t, []string{"b"}, res.Updated)
	task, err = fabric.Task("b")
	require.NoError(t, err)
	assert.Empty(t, task.Dependencies)
}

// RemoveNode deletes one task; a task that cannot be deleted is reported,
// never silently dropped.
func TestM0_RemoveNodeAndUndeletableIsReported(t *testing.T) {
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "a", AgentType: "x"},
		{ID: "b", AgentType: "y", DependsOn: []string{"a"}},
	})
	require.NoError(t, err)

	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	_, err = coord.CompileDAG(context.Background(), dag)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unsub := coord.SubscribeGraphEvents(ctx, dag)
	defer unsub()

	res := waitForChange(t, coord, func() {
		require.NoError(t, dag.RemoveNode(ctx, "b"))
	})
	require.Equal(t, []string{"b"}, res.Removed)
	assert.Equal(t, []string{"a"}, sortedIDs(t, fabric))
	assert.Equal(t, 1, coord.LastCompile().StepCount)

	// Now make "a" undeletable (RUNNING) and remove it from the graph: the
	// task must survive and the refusal must be in Skipped.
	epoch, err := fabric.Acquire("a", "agent-1", time.Minute)
	require.NoError(t, err)
	require.NoError(t, fabric.Start("a", "agent-1", epoch))

	res = waitForChange(t, coord, func() {
		require.NoError(t, dag.RemoveNode(ctx, "a"))
	})
	require.Len(t, res.Skipped, 1)
	assert.Equal(t, "a", res.Skipped[0].TaskID)
	assert.Equal(t, "delete", res.Skipped[0].Op)
	require.True(t, errors.Is(res.Skipped[0].Err, taskfabric.ErrTaskUndeletable))
	assert.False(t, res.Complete())
	// The task is still there and still running: the compiler refusing to
	// drop a live quantum is the behaviour, not a bug.
	assert.Equal(t, []string{"a"}, sortedIDs(t, fabric))
}

// ReplaceNode with a new id: the replacement task is created, the successor
// is migrated onto it, and the old task is deleted.
func TestM0_ReplaceNodeMigratesSuccessors(t *testing.T) {
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "a", AgentType: "x"},
		{ID: "old", AgentType: "y", DependsOn: []string{"a"}},
		{ID: "c", AgentType: "z", DependsOn: []string{"old"}},
	})
	require.NoError(t, err)

	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	_, err = coord.CompileDAG(context.Background(), dag)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unsub := coord.SubscribeGraphEvents(ctx, dag)
	defer unsub()

	res := waitForChange(t, coord, func() {
		require.NoError(t, dag.ReplaceNode(ctx, "old", &engine.Step{
			ID:        "new",
			AgentType: "y2",
			DependsOn: []string{"a"},
		}))
	})
	require.Empty(t, res.Skipped)
	require.Equal(t, []string{"new"}, res.Created)
	require.Equal(t, []string{"old"}, res.Removed)
	assert.Contains(t, res.Updated, "c", "the successor must be migrated onto the new node")

	assert.Equal(t, []string{"a", "c", "new"}, sortedIDs(t, fabric))

	// The successor now points at the replacement, so it stays reachable.
	c, err := fabric.Task("c")
	require.NoError(t, err)
	assert.Equal(t, []string{"new"}, c.Dependencies)
	// depsCompletedLocked only checks existence + COMPLETED, never who
	// compiled a task — the migrated edge is a real scheduling edge.
	assert.NotContains(t, fabric.ReadyTasks(), "c")
}

// ReplaceNode with the SAME id is an in-place rewrite: no create, no delete.
func TestM0_ReplaceNodeSameIDRewritesInPlace(t *testing.T) {
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "a", AgentType: "x"},
		{ID: "b", AgentType: "y", DependsOn: []string{"a"}},
	})
	require.NoError(t, err)

	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	_, err = coord.CompileDAG(context.Background(), dag)
	require.NoError(t, err)

	before := createdAtSnapshot(t, fabric)["b"]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unsub := coord.SubscribeGraphEvents(ctx, dag)
	defer unsub()

	res := waitForChange(t, coord, func() {
		require.NoError(t, dag.ReplaceNode(ctx, "b", &engine.Step{
			ID:        "b",
			AgentType: "y2",
			DependsOn: []string{"a"},
			Metadata:  map[string]string{"budget": "2"},
		}))
	})
	require.Empty(t, res.Skipped)
	require.Empty(t, res.Created, "a same-id replace must not create a task")
	require.Empty(t, res.Removed, "a same-id replace must not delete a task")
	assert.Contains(t, res.Updated, "b")

	task, err := fabric.Task("b")
	require.NoError(t, err)
	assert.Equal(t, before, task.CreatedAt)
	dc, err := taskfabric.DecodeCheckpoint(task.Checkpoint)
	require.NoError(t, err)
	assert.Equal(t, "2", dc.Payload["budget"])
}

// A failed mutation changed nothing, so it must not produce a compile.
// Reporting one would stamp the pre-change topology as the result of a
// change that never happened.
func TestM0_FailedMutationIsNotCompiled(t *testing.T) {
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{ID: "a", AgentType: "x"},
		{ID: "b", AgentType: "y", DependsOn: []string{"a"}},
	})
	require.NoError(t, err)

	fabric := taskfabric.NewFabric()
	coord := NewCompileCoordinator(fabric, nil)
	_, err = coord.CompileDAG(context.Background(), dag)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unsub := coord.SubscribeGraphEvents(ctx, dag)
	defer unsub()

	before := coord.CompileCount()
	// "a" has a dependent, so RemoveNode fails and publishes a failed event.
	require.ErrorIs(t, dag.RemoveNode(ctx, "a"), engine.ErrNodeHasDependents)

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, before, coord.CompileCount(), "a failed mutation must not be projected")
	assert.Equal(t, []string{"a", "b"}, sortedIDs(t, fabric))
}

// createdAtSnapshot captures every task's identity timestamp, the witness
// that proves a task was not rebuilt.
func createdAtSnapshot(t *testing.T, fabric *taskfabric.Fabric) map[string]time.Time {
	t.Helper()
	out := make(map[string]time.Time)
	for _, v := range fabric.TaskSnapshot() {
		out[v.TaskID] = v.CreatedAt
	}
	return out
}
