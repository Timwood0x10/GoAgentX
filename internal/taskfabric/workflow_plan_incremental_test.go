package taskfabric

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/ares_events"
)

// These tests pin the fabric-side contract the incremental compiler depends
// on. The compiler's own behaviour is tested in internal/planprojection; here
// the primitives themselves are the subject.

func TestCompileNode_DependencyResolvesAgainstExistingFabricTask(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "plan", Capability: "ares/plan"}))

	// "grep" depends on a task that is NOT in the batch. Before M0 this was
	// rejected as "depends on unknown step", which is what made runtime graph
	// growth impossible.
	id, err := f.CompileNode(context.Background(), PlanStep{
		ID:         "grep",
		Capability: "tool/grep",
		DependsOn:  []string{"plan"},
		Payload:    map[string]any{"input": "x"},
	})
	require.NoError(t, err)
	assert.Equal(t, "grep", id)

	task, err := f.Task("grep")
	require.NoError(t, err)
	assert.Equal(t, []string{"plan"}, task.Dependencies)
	assert.Equal(t, "tool/grep", task.Capability)
}

func TestCompilePlan_CrossBatchDependencyLetsTaskBecomeReady(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "plan", Capability: "ares/plan"}))
	epoch, err := f.Acquire("plan", "agent-1", time.Minute)
	require.NoError(t, err)
	require.NoError(t, f.Start("plan", "agent-1", epoch))
	require.NoError(t, f.Complete("plan", "agent-1", epoch))

	_, err = f.CompilePlan(context.Background(), []PlanStep{
		{ID: "grep", Capability: "tool/grep", DependsOn: []string{"plan"}},
	})
	require.NoError(t, err)

	// depsCompletedLocked only asks "does the dep exist and is it COMPLETED"
	// — it never cares which batch compiled it.
	ready, err := f.IsReady("grep")
	require.NoError(t, err)
	assert.True(t, ready)
}

func TestCompilePlan_UnknownDependencyIsStillRejected(t *testing.T) {
	f := NewFabric()
	_, err := f.CompilePlan(context.Background(), []PlanStep{
		{ID: "a", Capability: "x", DependsOn: []string{"ghost"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "depends on unknown step")
	assert.Contains(t, err.Error(), "ghost")
}

func TestCompilePlan_CycleWithinBatchIsStillDetected(t *testing.T) {
	f := NewFabric()
	_, err := f.CompilePlan(context.Background(), []PlanStep{
		{ID: "a", Capability: "x", DependsOn: []string{"b"}},
		{ID: "b", Capability: "y", DependsOn: []string{"a"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dependency cycle")
}

// A dependency that resolves outside the batch must not be walked by the
// cycle detector: it is not a batch node, and treating it as a zero-value
// one would add phantom nodes to the walk.
func TestCompilePlan_ExternalDependencyDoesNotPolluteCycleWalk(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "ext", Capability: "e"}))

	_, err := f.CompilePlan(context.Background(), []PlanStep{
		{ID: "a", Capability: "x", DependsOn: []string{"ext"}},
		{ID: "b", Capability: "y", DependsOn: []string{"a"}},
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(f.IDs()))
}

func TestSetDependencies(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "a", Capability: "x"}))
	require.NoError(t, f.Create(&Task{ID: "b", Capability: "y", Dependencies: []string{"a"}}))

	deps := []string{"a", "c"}
	require.NoError(t, f.SetDependencies("b", deps))

	// The fabric owns its copy: mutating the caller's slice must be inert.
	deps[0] = "mutated"
	task, err := f.Task("b")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "c"}, task.Dependencies)
}

func TestSetDependencies_StateGuards(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "a", Capability: "x"}))

	// Unknown task.
	require.ErrorIs(t, f.SetDependencies("ghost", nil), ErrTaskNotFound)

	// A running quantum was admitted against the dependency posture it read
	// at acquire time; rewriting under it is refused, loudly.
	epoch, err := f.Acquire("a", "agent-1", time.Minute)
	require.NoError(t, err)
	require.NoError(t, f.Start("a", "agent-1", epoch))
	err = f.SetDependencies("a", []string{"x"})
	require.ErrorIs(t, err, ErrTaskNotMutable)
	assert.Contains(t, err.Error(), "RUNNING", "the refusal must name the offending state")

	// A terminal task's dependency list is history.
	require.NoError(t, f.Complete("a", "agent-1", epoch))
	require.ErrorIs(t, f.SetDependencies("a", nil), ErrTaskNotMutable)
}

func TestUpdatePayload_PreservesEnvelopeFields(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{
		ID:         "a",
		Capability: "x",
		Checkpoint: &CheckpointEnvelope{
			SchemaVersion:    CurrentCheckpointSchemaVersion,
			StrategyID:       "strategy-7",
			UsedExperienceID: "exp-3",
			StepCheckpoint:   "quantum-progress",
			Payload:          map[string]any{"budget": "3"},
		},
	}))

	require.NoError(t, f.UpdatePayload("a", map[string]any{"budget": "1"}))

	dc, err := DecodeCheckpoint(mustTask(t, f, "a").Checkpoint)
	require.NoError(t, err)
	assert.Equal(t, "1", dc.Payload["budget"])
	// A metadata patch must not re-stamp attribution (E1) nor lose progress.
	assert.Equal(t, "strategy-7", dc.StrategyID)
	assert.Equal(t, "exp-3", dc.UsedExperienceID)
	assert.Equal(t, "quantum-progress", dc.StepCheckpoint)
}

func TestUpdatePayload_CopiesTheCallerMap(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "a", Capability: "x"}))

	in := map[string]any{"k": "v"}
	require.NoError(t, f.UpdatePayload("a", in))
	in["k"] = "mutated"

	dc, err := DecodeCheckpoint(mustTask(t, f, "a").Checkpoint)
	require.NoError(t, err)
	assert.Equal(t, "v", dc.Payload["k"])
}

func TestUpdatePayload_RunningTaskIsRefused(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "a", Capability: "x"}))
	epoch, err := f.Acquire("a", "agent-1", time.Minute)
	require.NoError(t, err)
	require.NoError(t, f.Start("a", "agent-1", epoch))

	err = f.UpdatePayload("a", map[string]any{"budget": "1"})
	require.ErrorIs(t, err, ErrTaskNotMutable)
	assert.Contains(t, err.Error(), "RUNNING")
}

// Storing nothing into a task that has nothing must not manufacture an
// envelope — that would flip TaskView.HasCheckpoint for no reason.
func TestUpdatePayload_EmptyIntoEmptyIsANoOp(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "a", Capability: "x"}))

	require.NoError(t, f.UpdatePayload("a", nil))
	assert.Nil(t, mustTask(t, f, "a").Checkpoint)
}

func TestUpdatePayload_UnknownTaskIsRejected(t *testing.T) {
	f := NewFabric()
	require.ErrorIs(t, f.UpdatePayload("ghost", nil), ErrTaskNotFound)
}

func TestDependents(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "a", Capability: "x"}))
	require.NoError(t, f.Create(&Task{ID: "b", Capability: "y", Dependencies: []string{"a"}}))
	require.NoError(t, f.Create(&Task{ID: "c", Capability: "z", Dependencies: []string{"a", "b"}}))
	require.NoError(t, f.Create(&Task{ID: "d", Capability: "w"}))

	assert.Equal(t, []string{"b", "c"}, f.Dependents("a"))
	assert.Equal(t, []string{"c"}, f.Dependents("b"))
	assert.Empty(t, f.Dependents("d"))
	// An id that is not itself a task can still be a dependency.
	assert.Empty(t, f.Dependents("ghost"))
}

// CompileNode must not disturb an unrelated RUNNING task — the exact
// scenario the old full-recompile path could not survive.
func TestCompileNode_LeavesRunningTaskUntouched(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "plan", Capability: "ares/plan"}))
	require.NoError(t, f.Create(&Task{ID: "slow", Capability: "tool/slow"}))
	epoch, err := f.Acquire("slow", "agent-1", time.Minute)
	require.NoError(t, err)
	require.NoError(t, f.Start("slow", "agent-1", epoch))
	before := mustTask(t, f, "slow").CreatedAt

	_, err = f.CompileNode(context.Background(), PlanStep{
		ID: "read", Capability: "tool/read", DependsOn: []string{"plan"},
	})
	require.NoError(t, err)

	slow := mustTask(t, f, "slow")
	assert.Equal(t, StateRunning, slow.State)
	assert.Equal(t, epoch, slow.Lease.Epoch)
	assert.Equal(t, before, slow.CreatedAt)
}

// SetDependencies / UpdatePayload are observability-only: they must not
// reach the durable store, because a restart rebuilds topology by
// re-compiling the DAG, not by folding these rewrites (see EventTaskUpdated).
func TestIncrementalRewritesAreNotPersisted(t *testing.T) {
	f := NewFabric()
	require.NoError(t, f.Create(&Task{ID: "a", Capability: "x"}))
	require.NoError(t, f.Create(&Task{ID: "b", Capability: "y"}))

	before := len(f.Events())
	require.NoError(t, f.SetDependencies("b", []string{"a"}))
	require.NoError(t, f.UpdatePayload("b", map[string]any{"k": "v"}))

	events := f.Events()
	require.Len(t, events, before+2)
	for _, ev := range events[before:] {
		assert.Equal(t, EventTaskUpdated, ev.Type)
		// The contract: unmapped in taskEventType ⇒ never appended to the
		// store, so nothing here is trusted for a cross-restart rebuild.
		assert.Equal(t, ares_events.EventType(""), taskEventType(ev.Type))
	}
	assert.False(t, isMustPersistEvent(EventTaskUpdated))
}

func mustTask(t *testing.T, f *Fabric, id string) *Task {
	t.Helper()
	task, err := f.Task(id)
	require.NoError(t, err)
	require.NotNil(t, task)
	return task
}
