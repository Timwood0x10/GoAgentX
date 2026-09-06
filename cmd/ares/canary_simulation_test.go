package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/planprojection"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// canaryScript is a round-scripted ChatClient: round N (1-based) returns the
// Nth scripted response, then repeats the last one. Shared shape with the
// agentfabric test scripts, redeclared here because the scheduler package
// cannot import agentfabric's internal test helpers.
type canaryScript struct {
	mu        sync.Mutex
	calls     int
	responses []core.GenerateResponse
}

func canaryToolCall(id, name, args string) core.GenerateResponse {
	return core.GenerateResponse{
		ToolCalls: []core.ToolCall{{
			ID:   id,
			Type: "function",
			Function: core.FunctionCall{
				Name:      name,
				Arguments: args,
			},
		}},
	}
}

func (c *canaryScript) Chat(_ context.Context, _ []*core.LLMMessage, _ []core.Tool, _ map[string]any) (*core.GenerateResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	i := c.calls - 1
	if i >= len(c.responses) {
		i = len(c.responses) - 1
	}
	resp := c.responses[i]
	return &resp, nil
}

// canaryCall is one recorded tool attempt with its outcome and latency.
type canaryCall struct {
	tool    string
	success bool
	latency time.Duration
}

// canaryBinder echoes tool results while recording every attempt — the raw
// material for the canary numbers (call counts, success rate, latency).
type canaryBinder struct {
	mu    sync.Mutex
	calls []canaryCall
}

func (b *canaryBinder) CallTool(_ context.Context, name string, args map[string]any) (any, error) {
	start := time.Now()
	out := fmt.Sprintf("echo(%s,%v)", name, args["query"])
	b.mu.Lock()
	b.calls = append(b.calls, canaryCall{tool: name, success: true, latency: time.Since(start)})
	b.mu.Unlock()
	return out, nil
}

func (b *canaryBinder) ListTools() []string { return []string{"grep", "read", "echo"} }

func (b *canaryBinder) IsToolIdempotent(string) bool { return true }

func (b *canaryBinder) GetToolSchemas() []resources.ToolSchema {
	schema := func(name string) resources.ToolSchema {
		return resources.ToolSchema{
			Name: name,
			Parameters: &resources.ParameterSchema{
				// Type "object" is required: ParameterSchemaToMap emits it
				// verbatim into the LLM function signature, and providers
				// reject an empty top-level type (live-canary 400 proven).
				Type: "object",
				Properties: map[string]*resources.Parameter{
					"query": {Type: "string"},
				},
				Required: []string{"query"},
			},
		}
	}
	return []resources.ToolSchema{schema("grep"), schema("read"), schema("echo")}
}

func (b *canaryBinder) stats() (total, failed int, maxLatency time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.calls {
		total++
		if !c.success {
			failed++
		}
		if c.latency > maxLatency {
			maxLatency = c.latency
		}
	}
	return total, failed, maxLatency
}

// canarySession is one simulated canary session: its script, the expected
// terminal answer, and the expected tool-call count.
type canarySession struct {
	id        string
	script    []core.GenerateResponse
	answer    string
	wantTools int
}

// canaryRouterChat serves one shared planner while giving each session its
// own script: it matches the session prompt (messages[0], the root prompt
// assembleContext always leads with) back to the session that owns it.
type canaryRouterChat struct {
	mu      sync.Mutex
	scripts map[string]*canaryScript
}

func (c *canaryRouterChat) Chat(ctx context.Context, msgs []*core.LLMMessage, tools []core.Tool, params map[string]any) (*core.GenerateResponse, error) {
	c.mu.Lock()
	scripts := make(map[string]*canaryScript, len(c.scripts))
	for k, v := range c.scripts {
		scripts[k] = v
	}
	c.mu.Unlock()
	for _, m := range msgs {
		for id, script := range scripts {
			if strings.Contains(m.Content, id) {
				return script.Chat(ctx, msgs, tools, params)
			}
		}
	}
	return nil, fmt.Errorf("canary: no script matches session in %d messages", len(msgs))
}

// TestCanary_FullStackL2Sessions is the B2 simulated canary: five L2 sessions
// run concurrently through the REAL stack (planner → subscription → real
// scheduler → router → echo tools), and the test reports production-shaped
// numbers — session completion rate, tool-call success rate, per-session
// latency, depth-exhaustion count. These numbers are the simulated baseline
// the production canary is measured against; flipping the config on real
// traffic is an operations step documented in the mainline plan, not code.
func TestCanary_FullStackL2Sessions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sessions := []canarySession{
		{id: "canary-1", script: []core.GenerateResponse{
			canaryToolCall("c1-1", "grep", `{"query":"q1"}`),
			canaryToolCall("c1-2", "read", `{"query":"q2"}`),
			{Content: "answer one"},
		}, answer: "answer one", wantTools: 2},
		{id: "canary-2", script: []core.GenerateResponse{
			canaryToolCall("c2-1", "grep", `{"query":"q3"}`),
			canaryToolCall("c2-2", "read", `{"query":"q4"}`),
			{Content: "answer two"},
		}, answer: "answer two", wantTools: 2},
		{id: "canary-3", script: []core.GenerateResponse{
			canaryToolCall("c3-1", "grep", `{"query":"q5"}`),
			canaryToolCall("c3-2", "read", `{"query":"q6"}`),
			canaryToolCall("c3-3", "echo", `{"query":"q7"}`),
			{Content: "answer three"},
		}, answer: "answer three", wantTools: 3},
		{id: "canary-4", script: []core.GenerateResponse{
			{Content: "immediate answer"},
		}, answer: "immediate answer", wantTools: 0},
		{id: "canary-5", script: []core.GenerateResponse{
			{ToolCalls: []core.ToolCall{
				{ID: "c5-1", Type: "function", Function: core.FunctionCall{Name: "grep", Arguments: `{"query":"q8"}`}},
				{ID: "c5-2", Type: "function", Function: core.FunctionCall{Name: "read", Arguments: `{"query":"q9"}`}},
			}},
			{Content: "answer five"},
		}, answer: "answer five", wantTools: 2},
	}

	fabric := taskfabric.NewFabric()
	coord := planprojection.NewCompileCoordinator(fabric, nil)
	reg := agentfabric.NewSessionRegistry()
	binder := &canaryBinder{}

	// One shared planner serves every session, exactly like production: the
	// depth-exhaustion counter below is therefore process-wide, as in prod.
	routerChat := &canaryRouterChat{scripts: make(map[string]*canaryScript)}
	planner, err := agentfabric.NewPlannerCognition(agentfabric.PlannerDeps{
		ChatClient: routerChat,
		ToolBinder: binder,
		Sessions:   reg,
		Fabric:     fabric,
		Logger:     slog.Default(),
	})
	require.NoError(t, err)
	typedPlanner, ok := planner.(interface{ ForcedAnswers() uint64 })
	require.True(t, ok, "planner must expose the depth-exhaustion counter")

	agents := agentfabric.NewFabric()
	_, err = agents.Spawn(ctx, agentfabric.SpawnSpec{
		Identity:     "canary-agent",
		Capabilities: []string{"ares/root", "ares/plan", "ares/answer", "tool/grep", "tool/read", "tool/echo"},
		CognitionFactory: func([]string) agentfabric.Cognition {
			return agentfabric.NewRouterCognitionWithPlanner(binder, planner, reg, slog.Default())
		},
	})
	require.NoError(t, err)

	sched := NewKernelScheduler(fabric, map[string]CapabilityExecutor{}, newLoadTracker())
	sched.WithAgentFabric(agents)
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	compileCoord := func(subCtx context.Context, dag *engine.MutableDAG) (stop func()) {
		return coord.SubscribeGraphEvents(subCtx, dag)
	}
	started := make(map[string]time.Time, len(sessions))
	for _, s := range sessions {
		routerChat.mu.Lock()
		routerChat.scripts[s.id] = &canaryScript{responses: s.script}
		routerChat.mu.Unlock()

		g, err := reg.InitSession(ctx, s.id, "canary prompt "+s.id, nil, compileCoord)
		require.NoError(t, err)
		canaryAdmitRoot(t, ctx, fabric, g)

		// The initial plan quantum: SessionID rides the envelope so the
		// scheduled task carries it to the planner, like submitPeerTask.
		planID := agentfabric.SessionNodeID(s.id, 0, "plan", 0)
		require.NoError(t, fabric.Create(&taskfabric.Task{
			ID:         planID,
			Capability: "ares/plan",
			Checkpoint: &taskfabric.CheckpointEnvelope{
				SessionID: s.id,
				Payload:   map[string]any{"input": "canary prompt " + s.id},
			},
		}))
		started[s.id] = time.Now()
	}

	// Every session must terminate at its answer node with the scripted
	// content readable from the envelope.
	for _, s := range sessions {
		answerID := waitForCanaryAnswer(t, fabric, s.id, 20*time.Second)
		canaryRequireItemContent(t, fabric, answerID, s.answer)
		elapsed := time.Since(started[s.id])
		t.Logf("canary: session %s completed in %v", s.id, elapsed)
		require.Less(t, elapsed, 15*time.Second, "per-session latency bound")
	}

	// Tool numbers: 2+2+3+0+2 calls, all successful.
	total, failed, maxLatency := binder.stats()
	require.Equal(t, 9, total, "every planned tool node must execute exactly once")
	require.Equal(t, 0, failed, "echo tools do not fail in the simulated canary")
	t.Logf("canary: tool calls total=%d failed=%d max_call_latency=%v", total, failed, maxLatency)

	// The depth guard must never bind: deepest session runs 3 rounds.
	require.Equal(t, uint64(0), typedPlanner.ForcedAnswers(),
		"no session may hit the depth guard in the simulated canary")
	t.Logf("canary: sessions=5 completed=5 tool_success_rate=100%% depth_exhaustions=0")
}

// canaryAdmitRoot compiles the session root task, like production admission
// does: the planner's first quantum reads (or falls back from) it.
func canaryAdmitRoot(t *testing.T, ctx context.Context, fabric *taskfabric.Fabric, g *agentfabric.L2Graph) {
	t.Helper()
	rootStep := g.DAG().StepIndex()[g.Root()]
	require.NotNil(t, rootStep, "L2 plan carries its session root")
	_, err := fabric.CompileNode(ctx, planprojection.ProjectStep(rootStep))
	require.NoError(t, err)
}

// canaryRequireItemContent asserts one envelope item carrying want.
// Same read path as the scheduler integration tests, redeclared because
// cross-package test helpers are not importable.
func canaryRequireItemContent(t *testing.T, f *taskfabric.Fabric, id, want string) {
	t.Helper()
	tk, err := f.Task(id)
	require.NoError(t, err)
	require.Equal(t, taskfabric.StateCompleted, tk.State, "task %q must have completed", id)
	dc, err := taskfabric.DecodeCheckpoint(tk.Checkpoint)
	require.NoError(t, err)
	sc, ok := dc.StepCheckpoint.(map[string]any)
	require.True(t, ok, "task %q checkpoint carries a {result,items,...} map", id)
	raw, ok := sc["items"]
	require.True(t, ok, "envelope carries items")
	items, ok := raw.([]*models.RecommendItem)
	require.True(t, ok, "items decode as recommend items, got %T", raw)
	require.Len(t, items, 1, "task %q produced one item", id)
	require.Equal(t, want, items[0].Content)
}

// waitForCanaryAnswer polls until the session's terminal answer task reads
// COMPLETED, and returns its task id. Polling with a deadline (not sleep
// sync) per code_rules §7.3.
func waitForCanaryAnswer(t *testing.T, fabric *taskfabric.Fabric, sessionID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range fabric.IDs() {
			if !strings.Contains(id, sessionID) || !strings.Contains(id, "/answer#") {
				continue
			}
			if tk, err := fabric.Task(id); err == nil && tk.State == taskfabric.StateCompleted {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("session %s grew no completed answer within %s", sessionID, timeout)
	return ""
}
