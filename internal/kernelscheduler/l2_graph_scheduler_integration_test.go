package kernelscheduler

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
)

// echoBinder is a scripted ToolBinder whose tools echo their args back. It is
// injected into the session agent's router cognition so the M1 integration
// test can confirm data actually flowed through the scheduled tool call.
type echoBinder struct {
	called []string
}

func (b *echoBinder) CallTool(_ context.Context, name string, args map[string]any) (any, error) {
	b.called = append(b.called, name)
	if q, ok := args["query"]; ok {
		return fmt.Sprintf("echo(%s,%v)", name, q), nil
	}
	return fmt.Sprintf("echo(%s)", name), nil
}

func (b *echoBinder) ListTools() []string { return []string{"grep", "read"} }

func (b *echoBinder) IsToolIdempotent(string) bool { return true }

func (b *echoBinder) GetToolSchemas() []resources.ToolSchema { return nil }

// TestL2Graph_SchedulerExecutesThreeNodeChain is the M1 acceptance REVISED to
// route through the REAL scheduler (design review ruling, §4.3): a hand-written
// 3-node L2 plan (grep -> read -> answer) is compiled to fabric tasks (one per
// node, same IDs), a single session agent declares the full capability set, and
// the scheduler really runs the chain. Execution facts are read back from each
// task's checkpoint envelope — the graph holds only topology + Metadata (Output
// 落点 = fabric, decision C), never a node field.
func TestL2Graph_SchedulerExecutesThreeNodeChain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. The L2 plan: root carries the session-invariant prompt.
	plan, err := agentfabric.NewL2Graph("root", "find the answer", nil)
	require.NoError(t, err)
	require.NoError(t, plan.AddToolNode(ctx, "n1", "grep", map[string]any{"query": "x"}, "root"))
	require.NoError(t, plan.AddToolNode(ctx, "n2", "read", map[string]any{"query": "y"}, "n1"))
	require.NoError(t, plan.AddToolNode(ctx, "n3", "answer", nil, "n2"))

	// 2. Compile the plan into fabric tasks. The batch is PROJECTED from the
	// plan (not hand-written): step ID = task ID so the graph node and its
	// execution fact join by ID. n1 depends on nothing: the root is the
	// session invariant, not a scheduled task.
	fabric := taskfabric.NewFabric()
	compilePlan(t, ctx, fabric, plan)

	// 3. One session agent declaring the FULL capability set is enough: the
	// scheduler's capability scorer overlaps it with each task, and its router
	// cognition picks the body by the task's capability. Spawn leaves it IDLE
	// and Executable (CognitionFactory injected a Cognition).
	agents := agentfabric.NewFabric()
	binder := &echoBinder{}
	require.NoError(t, spawnSessionAgent(ctx, agents, binder))

	// 4. Run the real scheduler: it drains ready tasks, selects the winning
	// candidate by capability, executes the agent's Cognition, and translates
	// each quantum outcome through buildQuantumStep into the task envelope.
	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())
	sched.WithAgentFabric(agents)
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	// 5. Wait for the terminal answer node to reach COMPLETED — its readiness
	// implies the whole chain ran (n3 depends on n1, n2).
	waitForTaskState(t, fabric, "n3", taskfabric.StateCompleted, 3*time.Second)

	// 6. Read each node's execution fact from its fabric envelope by ID join.
	requireItemContent(t, fabric, "n1", "echo(grep,x)")
	requireItemContent(t, fabric, "n2", "echo(read,y)")
	requireItemContent(t, fabric, "n3", "L2 session complete")
	require.Equal(t, []string{"grep", "read"}, binder.called,
		"both tool nodes ran exactly once, in dependency order, through the real scheduler")
}

// TestL2Graph_RecompilesIdempotentAfterRestart pins the R.2 minimum bar for
// decision C: the graph is the ONLY state needed to replay. First the chain
// runs to COMPLETED in one fabric/agent world; then a FRESH fabric is compiled
// from the SAME plan and rerun to COMPLETED. No leftover task state leaks
// between runs (a fresh compile never collides), so the graph alone
// reconstructs the run. This is rebuild idempotency, NOT a crash simulation:
// no task dies mid-RUNNING here and no envelope is reloaded — a true kill -9
// variant (die mid-quantum, reload, resume) is an M2-gate item.
func TestL2Graph_RecompilesIdempotentAfterRestart(t *testing.T) {
	plan, err := agentfabric.NewL2Graph("root", "find the answer", nil)
	require.NoError(t, err)
	if err := plan.AddToolNode(context.Background(), "n1", "grep", map[string]any{"query": "x"}, "root"); err != nil {
		t.Fatalf("add n1: %v", err)
	}
	if err := plan.AddToolNode(context.Background(), "n2", "read", map[string]any{"query": "y"}, "n1"); err != nil {
		t.Fatalf("add n2: %v", err)
	}
	if err := plan.AddToolNode(context.Background(), "n3", "answer", nil, "n2"); err != nil {
		t.Fatalf("add n3: %v", err)
	}

	// First run compiles + completes.
	first := runChain(t, plan, &echoBinder{})
	requireItemContent(t, first, "n3", "L2 session complete")

	// "Restart": the SAME plan compiles into a FRESH fabric and completes again.
	again := runChain(t, plan, &echoBinder{})
	requireItemContent(t, again, "n1", "echo(grep,x)")
	requireItemContent(t, again, "n2", "echo(read,y)")
	requireItemContent(t, again, "n3", "L2 session complete")
}

// runChain compiles plan into a fresh fabric, wires one session agent, runs the
// real scheduler, and waits for the terminal answer node to COMPLETED.
func runChain(t *testing.T, plan *agentfabric.L2Graph, binder *echoBinder) *taskfabric.Fabric {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	compilePlan(t, ctx, fabric, plan)

	agents := agentfabric.NewFabric()
	require.NoError(t, spawnSessionAgent(ctx, agents, binder))

	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())
	sched.WithAgentFabric(agents)
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	waitForTaskState(t, fabric, "n3", taskfabric.StateCompleted, 3*time.Second)
	return fabric
}

// spawnSessionAgent spawns one IDLE agent whose cognition routers by capability.
func spawnSessionAgent(ctx context.Context, agents *agentfabric.Fabric, binder *echoBinder) error {
	_, err := agents.Spawn(ctx, agentfabric.SpawnSpec{
		Identity:     "session-agent",
		Capabilities: []string{"tool/grep", "tool/read", "ares/answer"},
		CognitionFactory: func([]string) agentfabric.Cognition {
			return agentfabric.NewRouterCognition(binder, slog.Default())
		},
	})
	return err
}

// compilePlan projects a plan's non-root nodes onto PlanSteps and compiles
// them as ONE batch (design App.S §S.5). Raw f.Create is deliberately NOT
// used here: it bypasses dependency-closure validation, cycle detection and
// all-or-nothing rollback — the entire M0 seam. Compiling through CompilePlan
// keeps step ID = task ID (the graph↔envelope join key) while also pinning
// that a plan which would be rejected (dangling dep, cycle) fails here
// instead of passing green into the scheduler.
func compilePlan(t *testing.T, ctx context.Context, f *taskfabric.Fabric, plan *agentfabric.L2Graph) {
	t.Helper()
	steps := plan.DAG().StepIndex()
	batch := make([]taskfabric.PlanStep, 0)
	for _, id := range nonRootOrder(t, plan) {
		s := steps[id]
		// The plan node's Metadata IS the tool args; carry it into the task
		// payload so the compiled task runs with the planned arguments.
		batch = append(batch, taskfabric.PlanStep{
			ID:         id,
			Capability: s.AgentType,
			DependsOn:  nonRootDeps(s.DependsOn, plan.Root()),
			MaxRetries: 3,
			Payload:    metadataPayload(s.Metadata),
		})
	}
	_, err := f.CompilePlan(ctx, batch)
	require.NoError(t, err)
}

// nonRootOrder returns the non-root node IDs of a plan in topological order.
func nonRootOrder(t *testing.T, plan *agentfabric.L2Graph) []string {
	t.Helper()
	order, err := plan.DAG().GetExecutionOrder()
	require.NoError(t, err)
	out := make([]string, 0, len(order))
	for _, id := range order {
		if id != plan.Root() {
			out = append(out, id)
		}
	}
	return out
}

// nonRootDeps filters the session root (a non-scheduled invariant) out of a
// node's dependency list so the fabric edges only mention scheduled tasks.
func nonRootDeps(deps []string, root string) []string {
	out := make([]string, 0, len(deps))
	for _, d := range deps {
		if d != root {
			out = append(out, d)
		}
	}
	return out
}

// metadataPayload converts a plan node's string-only Metadata (the tool args)
// into the any-payload the fabric envelope carries. Empty Metadata yields nil
// so CompilePlan leaves the task without a pre-execution envelope (same as a
// hand-built arg-less node — e.g. the answer node).
func metadataPayload(md map[string]string) map[string]any {
	if len(md) == 0 {
		return nil
	}
	p := make(map[string]any, len(md))
	for k, v := range md {
		p[k] = v
	}
	return p
}

// requireItemContent reads one task's COMPLETED envelope and asserts its first
// RecommendItem content equals want — the Output 落点 is the fabric envelope.
func requireItemContent(t *testing.T, f *taskfabric.Fabric, id, want string) {
	t.Helper()
	tk, err := f.Task(id)
	require.NoError(t, err)
	require.Equal(t, taskfabric.StateCompleted, tk.State, "task %q must have completed", id)
	dc, err := taskfabric.DecodeCheckpoint(tk.Checkpoint)
	require.NoError(t, err)
	sc, ok := dc.StepCheckpoint.(map[string]any)
	require.True(t, ok, "task %q checkpoint carries a {result,items,...} map", id)
	items := itemContents(t, sc)
	require.Len(t, items, 1, "task %q produced one item", id)
	require.Equal(t, want, items[0])
}

// itemContents extracts RecommendItem contents tolerantly. In-memory envelopes
// carry []*models.RecommendItem, but after a JSON round-trip (persistence /
// reload — exactly the R.2 restart path) the same field decodes as []any of
// maps. Asserting only the in-memory shape would fail precisely the restart
// coverage this helper exists to serve.
func itemContents(t *testing.T, sc map[string]any) []string {
	t.Helper()
	raw, ok := sc["items"]
	require.True(t, ok, "envelope carries items")
	switch items := raw.(type) {
	case []*models.RecommendItem:
		out := make([]string, 0, len(items))
		for _, it := range items {
			require.NotNil(t, it, "envelope item must not be nil")
			out = append(out, it.Content)
		}
		return out
	case []any:
		out := make([]string, 0, len(items))
		for _, e := range items {
			m, ok := e.(map[string]any)
			require.True(t, ok, "reloaded item decodes as a map, got %T", e)
			c, ok := m["content"].(string)
			require.True(t, ok, "reloaded item carries content, got %v", m)
			out = append(out, c)
		}
		return out
	default:
		t.Fatalf("items has unexpected shape %T", raw)
		return nil
	}
}

// TestItemContents_ToleratesReloadedEnvelope locks the []any branch of
// itemContents: without this test that branch is dead code that only runs on
// the day a restart test needs it.
func TestItemContents_ToleratesReloadedEnvelope(t *testing.T) {
	sc := map[string]any{
		"items": []any{
			map[string]any{"item_id": "n1", "content": "echo(grep,x)"},
		},
	}
	require.Equal(t, []string{"echo(grep,x)"}, itemContents(t, sc))
}
