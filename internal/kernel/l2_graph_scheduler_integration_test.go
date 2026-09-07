package kernel

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/planprojection"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
)

// answerBody is the content the hand-written plans put on their terminal
// answer node, so the assertions exercise the real answer path (content
// supplied on the node) instead of the no-content fallback.
const answerBody = "answer: tool chain complete"

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

// TestL2Graph_SchedulerExecutesThreeNodeChain is the acceptance test, revised
// to route through the REAL scheduler: a hand-written 3-node L2 plan
// (grep -> read -> answer) is compiled to fabric tasks (one per node, same
// IDs), a single session agent declares the full capability set, and the
// scheduler really runs the chain. Execution facts are read back from each
// task's checkpoint envelope — the graph holds only topology + Metadata,
// never a node field.
func TestL2Graph_SchedulerExecutesThreeNodeChain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. The L2 plan: root carries the session-invariant prompt.
	plan, err := agentfabric.NewL2Graph("root", "find the answer", nil)
	require.NoError(t, err)
	require.NoError(t, plan.AddToolNode(ctx, "n1", "grep", map[string]any{"query": "x"}, "root"))
	require.NoError(t, plan.AddToolNode(ctx, "n2", "read", map[string]any{"query": "y"}, "n1"))
	require.NoError(t, plan.AddToolNode(ctx, "n3", "answer", map[string]any{"content": answerBody}, "n2"))

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
	requireItemContent(t, fabric, "n3", answerBody)
	require.Equal(t, []string{"grep", "read"}, binder.called,
		"both tool nodes ran exactly once, in dependency order, through the real scheduler")
}

// TestL2Graph_RecompilesIdempotentAfterRestart pins the recovery minimum bar
// for rebuild: the graph is the ONLY state needed to replay. First the chain
// runs to COMPLETED in one fabric/agent world; then a FRESH fabric is compiled
// from the SAME plan and rerun to COMPLETED. No leftover task state leaks
// between runs (a fresh compile never collides), so the graph alone
// reconstructs the run. This is rebuild idempotency, NOT a crash simulation:
// no task dies mid-RUNNING here and no envelope is reloaded — a true kill -9
// variant (die mid-quantum, reload, resume) is a follow-up wiring item.
func TestL2Graph_RecompilesIdempotentAfterRestart(t *testing.T) {
	plan, err := agentfabric.NewL2Graph("root", "find the answer", nil)
	require.NoError(t, err)
	if err := plan.AddToolNode(context.Background(), "n1", "grep", map[string]any{"query": "x"}, "root"); err != nil {
		t.Fatalf("add n1: %v", err)
	}
	if err := plan.AddToolNode(context.Background(), "n2", "read", map[string]any{"query": "y"}, "n1"); err != nil {
		t.Fatalf("add n2: %v", err)
	}
	if err := plan.AddToolNode(context.Background(), "n3", "answer", map[string]any{"content": answerBody}, "n2"); err != nil {
		t.Fatalf("add n3: %v", err)
	}

	// First run compiles + completes.
	first := runChain(t, plan, &echoBinder{})
	requireItemContent(t, first, "n3", answerBody)

	// "Restart": the SAME plan compiles into a FRESH fabric and completes again.
	again := runChain(t, plan, &echoBinder{})
	requireItemContent(t, again, "n1", "echo(grep,x)")
	requireItemContent(t, again, "n2", "echo(read,y)")
	requireItemContent(t, again, "n3", answerBody)
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
// The set covers the session admission root plus every tool the tests grow.
func spawnSessionAgent(ctx context.Context, agents *agentfabric.Fabric, binder *echoBinder) error {
	_, err := agents.Spawn(ctx, agentfabric.SpawnSpec{
		Identity:     "session-agent",
		Capabilities: []string{"ares/root", "tool/grep", "tool/read", "tool/echo", "ares/answer"},
		CognitionFactory: func([]string) agentfabric.Cognition {
			return agentfabric.NewRouterCognition(binder, slog.Default())
		},
	})
	return err
}

// compilePlan projects a plan's non-root nodes onto PlanSteps and compiles
// them as ONE batch. Raw f.Create is deliberately NOT
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
// RecommendItem content equals want — the output lives in the fabric envelope.
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
// reload — exactly a restart) the same field decodes as []any of
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

// TestL2Graph_IncrementalEventsDriveSchedulerToCompletion is the event-path
// acceptance: L2 growth → SubscribeGraphEvents → the REAL Scheduler.Run →
// envelopes read back by ID join. Growth, incremental compile, and execution
// overlap (the subscriber and the scheduler run while nodes are still being
// added) — the M2 runtime shape, with -race watching for event/Step aliasing.
func TestL2Graph_IncrementalEventsDriveSchedulerToCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	coord := planprojection.NewCompileCoordinator(fabric, nil)

	plan, err := agentfabric.NewL2Graph("root", "find the answer", nil)
	require.NoError(t, err)
	stop := coord.SubscribeGraphEvents(ctx, plan.DAG())
	defer stop()

	// Session admission: the root is constructed pre-subscription (its
	// creation publishes no event), so it is compiled explicitly — the
	// pattern M2 session wiring follows. The admission task completes in one
	// zero-work quantum and carries the session prompt in its envelope.
	admitSessionRoot(t, ctx, fabric, plan)

	agents := agentfabric.NewFabric()
	binder := &echoBinder{}
	require.NoError(t, spawnSessionAgent(ctx, agents, binder))

	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())
	sched.WithAgentFabric(agents)
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	require.NoError(t, plan.AddToolNode(ctx, "n1", "grep", map[string]any{"query": "x"}, "root"))
	require.NoError(t, plan.AddToolNode(ctx, "n2", "read", map[string]any{"query": "y"}, "n1"))
	require.NoError(t, plan.AddToolNode(ctx, "n3", "answer", map[string]any{"content": answerBody}, "n2"))

	waitForTaskState(t, fabric, "n3", taskfabric.StateCompleted, 5*time.Second)

	requireItemContent(t, fabric, "root", "find the answer")
	requireItemContent(t, fabric, "n1", "echo(grep,x)")
	requireItemContent(t, fabric, "n2", "echo(read,y)")
	requireItemContent(t, fabric, "n3", answerBody)
	require.ElementsMatch(t, []string{"root", "n1", "n2", "n3"}, fabric.IDs(),
		"every grown node — admission root included — has exactly one task")
}

// TestL2Graph_BurstGrowthConvergesThroughEvents pins the D2 acceptance on the
// live seam: 70 nodes grown in a tight burst (past the 64-event hub buffer)
// while the subscriber compiles and the scheduler executes. Whether delivery
// or reconcile compensation materializes the tail, every node still ends
// COMPLETED with its envelope — convergence is asserted, not the path.
func TestL2Graph_BurstGrowthConvergesThroughEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 70
	fabric := taskfabric.NewFabric()
	coord := planprojection.NewCompileCoordinator(fabric, nil)

	plan, err := agentfabric.NewL2Graph("root", "burst", nil)
	require.NoError(t, err)
	stop := coord.SubscribeGraphEvents(ctx, plan.DAG())
	defer stop()

	admitSessionRoot(t, ctx, fabric, plan)

	agents := agentfabric.NewFabric()
	require.NoError(t, spawnSessionAgent(ctx, agents, &echoBinder{}))

	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())
	sched.WithAgentFabric(agents)
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	ids := make([]string, 0, n+1)
	ids = append(ids, "root")
	prev := "root"
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("b%d", i)
		require.NoError(t, plan.AddToolNode(ctx, id, "echo", map[string]any{"query": fmt.Sprintf("q%d", i)}, prev))
		ids = append(ids, id)
		prev = id
	}

	waitForAllCompleted(t, fabric, ids, 20*time.Second)
	requireItemContent(t, fabric, "b0", "echo(echo,q0)")
	requireItemContent(t, fabric, fmt.Sprintf("b%d", n-1), fmt.Sprintf("echo(echo,q%d)", n-1))
}

// admitSessionRoot compiles the L2 session root (which predates any event
// subscription) as the admission task. Its completion unblocks every tool
// node that depends on the root.
func admitSessionRoot(t *testing.T, ctx context.Context, fabric *taskfabric.Fabric, plan *agentfabric.L2Graph) {
	t.Helper()
	root := plan.DAG().StepIndex()[plan.Root()]
	require.NotNil(t, root, "L2 plan carries its session root")
	_, err := fabric.CompileNode(ctx, planprojection.ProjectStep(root))
	require.NoError(t, err)
}

// waitForAllCompleted polls until every listed task reads COMPLETED.
func waitForAllCompleted(t *testing.T, fabric *taskfabric.Fabric, ids []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		done := true
		for _, id := range ids {
			tk, err := fabric.Task(id)
			if err != nil || tk.State != taskfabric.StateCompleted {
				done = false
				break
			}
		}
		if done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("not all %d tasks completed within %s", len(ids), timeout)
}

// TestL2Graph_SessionIDCrossesGrowthAndProjection pins the seam the planner
// depends on and the unit tests bypass: the session id is stamped through
// AddToolNode's args map, and ProjectStep must lift it onto the PlanStep so
// Task.SessionID is populated for the NEXT plan quantum.
//
// It must cross that seam WITHOUT entering the tool-argument namespace: an
// arg.-prefixed session id would both vanish from the envelope (breaking the
// planner's session lookup) and reach CallTool as an undeclared argument,
// which a strict-schema tool rejects.
func TestL2Graph_SessionIDCrossesGrowthAndProjection(t *testing.T) {
	ctx := context.Background()
	plan, err := agentfabric.NewL2Graph("sess/s1/root", "prompt", nil)
	require.NoError(t, err)
	require.NoError(t, plan.AddToolNode(ctx, "n1", "grep",
		map[string]any{"query": "x", "session_id": "s1"}, "sess/s1/root"))

	step := plan.DAG().StepIndex()["n1"]
	require.Equal(t, map[string]string{"arg.query": "x", "session_id": "s1"}, step.Metadata,
		"tool args are namespaced; the session id is envelope plumbing and stays bare")

	ps := planprojection.ProjectStep(step)
	require.Equal(t, "s1", ps.SessionID, "the session id must reach the task, not be dropped")

	// The executing cognition sees the tool args only.
	binder := &strictQueryBinder{}
	cog := agentfabric.NewRouterCognition(binder, slog.Default())
	task := models.NewTask("n1", "tool/grep", nil)
	task.Payload = ps.Payload
	_, err = cog.ExecuteStep(ctx, task)
	require.NoError(t, err, "session id must not reach CallTool as a tool argument")
	require.Equal(t, map[string]any{"query": "x"}, binder.got)
}

// strictQueryBinder rejects any argument key it did not declare, like an MCP
// tool with additionalProperties:false.
type strictQueryBinder struct{ got map[string]any }

func (b *strictQueryBinder) CallTool(_ context.Context, name string, args map[string]any) (any, error) {
	for k := range args {
		if k != "query" {
			return nil, fmt.Errorf("strict: tool %s got undeclared arg %q", name, k)
		}
	}
	b.got = args
	return "ok", nil
}

func (b *strictQueryBinder) ListTools() []string { return []string{"grep"} }

func (b *strictQueryBinder) IsToolIdempotent(string) bool { return true }

func (b *strictQueryBinder) GetToolSchemas() []resources.ToolSchema { return nil }
