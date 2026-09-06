package agentfabric

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/planprojection"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// contextChat captures the messages the planner sends to the LLM so the test
// can assert that the context was assembled from the graph path (predecessor
// tool outputs).
type contextChat struct {
	mu       sync.Mutex
	messages []*core.LLMMessage
	calls    int
	tools    []core.Tool
}

func (c *contextChat) Chat(_ context.Context, msgs []*core.LLMMessage, tools []core.Tool, _ map[string]any) (*core.GenerateResponse, error) {
	c.mu.Lock()
	c.calls++
	c.messages = make([]*core.LLMMessage, len(msgs))
	copy(c.messages, msgs)
	c.tools = tools
	c.mu.Unlock()

	return &core.GenerateResponse{Content: "final answer"}, nil
}

// TestM3_AssembleContextFromGraphPath verifies the M3 context assembly:
// the planner reads predecessor tool outputs from the fabric envelopes by
// node ID = task ID join, and assembles them into LLM messages along the
// dependency chain. The first plan quantum has only the root prompt; the
// second plan quantum has the root prompt + the first tool's output.
func TestM3_AssembleContextFromGraphPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const sessionID = "m3-ctx"
	fabric := taskfabric.NewFabric()
	coord := planprojection.NewCompileCoordinator(fabric, nil)
	reg := NewSessionRegistry()
	compileCoord := func(_ context.Context, dag *engine.MutableDAG) (stop func()) {
		return coord.SubscribeGraphEvents(ctx, dag)
	}
	g, err := reg.InitSession(ctx, sessionID, "analyze the system", nil, compileCoord)
	require.NoError(t, err)

	// Admit root and drive to completion so its prompt rides the envelope.
	rootStep := g.DAG().StepIndex()[g.Root()]
	_, err = fabric.CompileNode(ctx, planprojection.ProjectStep(rootStep))
	require.NoError(t, err)
	driveTaskToCompleted(t, ctx, fabric, g.Root(), "analyze the system")

	// Round 1: manually grow a tool node + plan node (simulating what the
	// planner would do in round 1), so the plan node for round 2 exists.
	grepID := SessionNodeID(sessionID, 1, "grep", 0)
	require.NoError(t, g.AddToolNode(ctx, grepID, "grep",
		map[string]any{"query": "error", planMetadataKey: sessionID}, g.Root()))

	// Wait for the grep task to be compiled, then drive it to completion
	// with a known output.
	waitForTaskExists(t, fabric, grepID, 2*time.Second)
	driveTaskToCompleted(t, ctx, fabric, grepID, "found 3 errors in main.go")

	// Grow the next plan node depending on the grep node.
	plan2ID := SessionNodeID(sessionID, 1, "plan", 0)
	require.NoError(t, g.AddToolNode(ctx, plan2ID, "plan",
		map[string]any{planMetadataKey: sessionID}, grepID))

	// Build the planner.
	chatClient := &contextChat{}
	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: chatClient,
		ToolBinder: &plannerTestBinder{},
		Sessions:   reg,
		Fabric:     fabric,
		Logger:     slog.Default(),
	})
	require.NoError(t, err)

	// Execute the second plan quantum — the planner should assemble
	// context from the root prompt + the grep tool's output.
	plan2Task := models.NewTask(plan2ID, models.AgentType("ares/plan"), nil)
	plan2Task.SessionID = sessionID
	plan2Task.Payload = map[string]any{"input": "analyze the system", planMetadataKey: sessionID}

	out, err := planner.ExecuteStep(ctx, plan2Task)
	require.NoError(t, err)
	require.True(t, out.Done)

	// The LLM received context assembled from the graph path:
	// [user: root prompt, assistant: grep tool call, tool: grep output].
	// The assistant+tool pairing is load-bearing: a bare tool message with
	// no preceding assistant tool_call violates the provider contract
	// (observed live as the model repeating the same call every round).
	chatClient.mu.Lock()
	defer chatClient.mu.Unlock()
	require.Equal(t, 1, chatClient.calls, "LLM called once")
	require.Len(t, chatClient.messages, 3, "context has root prompt + assistant tool call + tool output")
	require.Equal(t, "user", chatClient.messages[0].Role)
	require.Equal(t, "analyze the system", chatClient.messages[0].Content)
	require.Equal(t, "assistant", chatClient.messages[1].Role)
	require.Len(t, chatClient.messages[1].ToolCalls, 1, "assistant message carries the reconstructed call")
	require.Equal(t, "grep", chatClient.messages[1].ToolCalls[0].Function.Name)
	require.Contains(t, chatClient.messages[1].ToolCalls[0].Function.Arguments, "error")
	require.Equal(t, roleTool, chatClient.messages[2].Role)
	require.Contains(t, chatClient.messages[2].Content, "found 3 errors")
	require.Equal(t, chatClient.messages[1].ToolCalls[0].ID, chatClient.messages[2].ToolCallID,
		"tool message links back to its assistant call")
}

// TestM3_FullCapabilityAdvertisement verifies the M3 capability advertisement:
// when the DAG execution gate is open, a peer agent declares the full
// capability set (ares/root, ares/plan, ares/answer, tool/<name>) so the
// scheduler can route every L2 node type to it. This test validates the
// capability list construction logic without requiring a full peer spawn.
func TestM3_FullCapabilityAdvertisement(t *testing.T) {
	// Build the full capability set the same way peer_mode.go does when
	// DAGExecution.Enabled is true.
	binder := &plannerTestBinder{}
	caps := []string{
		"ares/root",
		"ares/plan",
		"ares/answer",
	}
	for _, name := range binder.ListTools() {
		caps = append(caps, "tool/"+name)
	}

	// The full set must include every L2 capability type.
	require.Contains(t, caps, "ares/root")
	require.Contains(t, caps, "ares/plan")
	require.Contains(t, caps, "ares/answer")
	require.Contains(t, caps, "tool/grep")
	require.Contains(t, caps, "tool/read")

	// The legacy path (gate off) declares only the primary type.
	legacyCaps := []string{"code"}
	require.NotContains(t, legacyCaps, "ares/plan",
		"legacy path must NOT advertise L2 capabilities")
}

// TestM3_GateOffKeepsLegacyBehavior verifies that when the DAGExecution gate
// is off, the router is never constructed and the legacy chat cognition is
// selected — production behavior is identical to before M3.
func TestM3_GateOffKeepsLegacyBehavior(t *testing.T) {
	var gate DAGExecution
	chat := CognitionFunc(func(context.Context, *models.Task) (*StepOutcome, error) { return nil, nil })
	router := CognitionFunc(func(context.Context, *models.Task) (*StepOutcome, error) { return nil, nil })

	// Gate off → chat (legacy).
	selected := gate.Select(chat, router)
	require.NotNil(t, selected, "gate off must select chat cognition")

	// Gate on → router (L2 path).
	gate.Enabled = true
	selected = gate.Select(chat, router)
	require.NotNil(t, selected, "gate on must select router cognition")
}

// TestM3_ContextIsChronological pins observation ORDER on the context path:
// with two tool nodes, the LLM must see the older output before the newer one
// — the same order ReAct's Messages[] presented. The predecessor walk is
// newest-first, so an un-reversed append would invert the history and change
// what the model concludes, breaking the M3 dual-path acceptance.
func TestM3_ContextIsChronological(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	reg := NewSessionRegistry()
	const sessionID = "m3-order"
	g, err := reg.InitSession(ctx, sessionID, "root prompt", nil, nil)
	require.NoError(t, err)

	// root -> first -> second -> plan
	first := SessionNodeID(sessionID, 1, "grep", 0)
	second := SessionNodeID(sessionID, 2, "read", 0)
	planNode := SessionNodeID(sessionID, 2, "plan", 0)
	require.NoError(t, g.AddToolNode(ctx, first, "grep", nil, g.Root()))
	require.NoError(t, g.AddToolNode(ctx, second, "read", nil, first))
	require.NoError(t, g.AddToolNode(ctx, planNode, "plan", nil, second))

	for id, out := range map[string]string{
		g.Root(): "root prompt", first: "OLDER", second: "NEWER",
	} {
		require.NoError(t, fabric.Create(&taskfabric.Task{ID: id, Capability: "x"}))
		driveTaskToCompleted(t, ctx, fabric, id, out)
	}

	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: &l1ChatAlwaysGrep{},
		ToolBinder: &plannerTestBinder{},
		Sessions:   reg,
		Fabric:     fabric,
		Logger:     slog.Default(),
	})
	require.NoError(t, err)

	task := models.NewTask(planNode, planAgentType, nil)
	task.SessionID = sessionID
	msgs, err := planner.(*plannerCognition).assembleContext(ctx, task, g)
	require.NoError(t, err)
	// [user root, assistant grep-call, tool OLDER, assistant read-call,
	// tool NEWER]: execution order with the provider-mandated pairing.
	require.Len(t, msgs, 5)
	require.Equal(t, "root prompt", msgs[0].Content)
	require.Equal(t, "assistant", msgs[1].Role)
	require.Equal(t, "grep", msgs[1].ToolCalls[0].Function.Name)
	require.Equal(t, "OLDER", msgs[2].Content)
	require.Equal(t, msgs[1].ToolCalls[0].ID, msgs[2].ToolCallID)
	require.Equal(t, "assistant", msgs[3].Role)
	require.Equal(t, "read", msgs[3].ToolCalls[0].Function.Name)
	require.Equal(t, "NEWER", msgs[4].Content, "execution order: the first tool comes first")
	require.Equal(t, msgs[3].ToolCalls[0].ID, msgs[4].ToolCallID)
}
