package agentfabric

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/planprojection"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// fakePlannerChat is a scripted ChatClient that drives the planner through
// two tool rounds then a final answer. It returns exactly one tool call per
// round for the first two calls, then a text answer — exercising the
// full plan→tool→plan→tool→answer growth.
type fakePlannerChat struct {
	mu    sync.Mutex
	calls int
}

func (c *fakePlannerChat) Chat(_ context.Context, _ []*core.LLMMessage, _ []core.Tool, _ map[string]any) (*core.GenerateResponse, error) {
	c.mu.Lock()
	c.calls++
	round := c.calls
	c.mu.Unlock()

	switch round {
	case 1:
		return &core.GenerateResponse{
			ToolCalls: []core.ToolCall{{
				ID:       "tc-1",
				Type:     "function",
				Function: core.FunctionCall{Name: "grep", Arguments: `{"query":"first"}`},
			}},
		}, nil
	case 2:
		return &core.GenerateResponse{
			ToolCalls: []core.ToolCall{{
				ID:       "tc-2",
				Type:     "function",
				Function: core.FunctionCall{Name: "read", Arguments: `{"query":"second"}`},
			}},
		}, nil
	default:
		return &core.GenerateResponse{Content: "the answer is 42"}, nil
	}
}

// plannerTestBinder is a ToolBinder whose tools echo their args back so
// toolCognition produces a real result in the fabric envelope.
type plannerTestBinder struct{}

func (b *plannerTestBinder) CallTool(_ context.Context, name string, args map[string]any) (any, error) {
	if q, ok := args["query"]; ok {
		return fmt.Sprintf("echo(%s,%v)", name, q), nil
	}
	return fmt.Sprintf("echo(%s)", name), nil
}

func (b *plannerTestBinder) ListTools() []string { return []string{"grep", "read"} }

func (b *plannerTestBinder) IsToolIdempotent(string) bool { return true }

// GetToolSchemas declares "grep" with a REQUIRED "query" and an OPTIONAL
// "limit". The optional parameter is the point: the scripted LLM calls grep
// with "query" only, so an args-derived ToolClass shape would be "query"
// while the schema-derived shape is "limit,query". The L1 lookup must use the
// schema shape on both the write and the read side, or every constraint
// silently falls through to permissive.
func (b *plannerTestBinder) GetToolSchemas() []resources.ToolSchema {
	return []resources.ToolSchema{{
		Name: "grep",
		Parameters: &resources.ParameterSchema{
			Properties: map[string]*resources.Parameter{
				"query": {Type: "string"},
				"limit": {Type: "integer"},
			},
			Required: []string{"query"},
		},
	}}
}

// TestPlannerCognition_GrowsTwoPlanRoundsWithToolNodes is the M2 acceptance
// test: a task that needs 2 rounds of tool calling grows a session graph
// with 2 plan nodes, 2 tool nodes, and 1 answer node. The graph topology
// is acyclic (GetExecutionOrder succeeds), every node gets a fabric task,
// and the session is released when the answer completes.
func TestPlannerCognition_GrowsTwoPlanRoundsWithToolNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const sessionID = "m2-test"
	fabric := taskfabric.NewFabric()

	// Wire the incremental compile coordinator so AddToolNode → fabric task.
	coord := planprojection.NewCompileCoordinator(fabric, nil)

	// Session registry with compile-coordinator wiring.
	reg := NewSessionRegistry()
	compileCoord := func(_ context.Context, dag *engine.MutableDAG) (stop func()) {
		return coord.SubscribeGraphEvents(ctx, dag)
	}
	g, err := reg.InitSession(ctx, sessionID, "find the answer", nil, compileCoord)
	require.NoError(t, err)

	// Admit the session root: compile it as a fabric task so the root
	// completes and its prompt rides the envelope.
	rootStep := g.DAG().StepIndex()[g.Root()]
	_, err = fabric.CompileNode(ctx, planprojection.ProjectStep(rootStep))
	require.NoError(t, err)

	// Drive the root to COMPLETED so its output is available.
	driveTaskToCompleted(t, ctx, fabric, g.Root(), "find the answer")

	// Build the planner cognition.
	chatClient := &fakePlannerChat{}
	binder := &plannerTestBinder{}
	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: chatClient,
		ToolBinder: binder,
		Sessions:   reg,
		Fabric:     fabric,
		Logger:     slog.Default(),
	})
	require.NoError(t, err)

	// The initial plan task: its predecessor is the root.
	initialPlanID := SessionNodeID(sessionID, 0, "plan", 0)
	planTask := models.NewTask(initialPlanID, models.AgentType("ares/plan"), nil)
	planTask.SessionID = sessionID
	planTask.Payload = map[string]any{
		"input":         "find the answer",
		planMetadataKey: sessionID,
	}

	// Round 1: planner calls LLM → gets grep tool call → grows grep node
	// + next plan node.
	out1, err := planner.ExecuteStep(ctx, planTask)
	require.NoError(t, err)
	require.True(t, out1.Done, "planner quantum must complete in one step")

	// The grep node must have a fabric task (via incremental compile).
	grepID := SessionNodeID(sessionID, 1, "grep", 0)
	waitForTaskExists(t, fabric, grepID, 2*time.Second)

	// Drive the grep task to completion so the planner can read its output.
	driveTaskToCompleted(t, ctx, fabric, grepID, "echo(grep,first)")

	// Round 2: the next plan node's task was created by incremental compile.
	plan2ID := SessionNodeID(sessionID, 1, "plan", 0)
	plan2Task := models.NewTask(plan2ID, models.AgentType("ares/plan"), nil)
	plan2Task.SessionID = sessionID
	plan2Task.Payload = map[string]any{
		"input":         "find the answer",
		planMetadataKey: sessionID,
	}

	out2, err := planner.ExecuteStep(ctx, plan2Task)
	require.NoError(t, err)
	require.True(t, out2.Done)

	// The read node must exist.
	readID := SessionNodeID(sessionID, 2, "read", 0)
	waitForTaskExists(t, fabric, readID, 2*time.Second)
	driveTaskToCompleted(t, ctx, fabric, readID, "echo(read,second)")

	// Round 3: the LLM returns a text answer (no tool calls).
	plan3ID := SessionNodeID(sessionID, 2, "plan", 0)
	plan3Task := models.NewTask(plan3ID, models.AgentType("ares/plan"), nil)
	plan3Task.SessionID = sessionID
	plan3Task.Payload = map[string]any{
		"input":         "find the answer",
		planMetadataKey: sessionID,
	}

	out3, err := planner.ExecuteStep(ctx, plan3Task)
	require.NoError(t, err)
	require.True(t, out3.Done)

	// An answer node must exist.
	answerID := SessionNodeID(sessionID, 3, "answer", 0)
	waitForTaskExists(t, fabric, answerID, 2*time.Second)

	// The graph topology is acyclic.
	order, err := g.DAG().GetExecutionOrder()
	require.NoError(t, err, "graph must be acyclic")
	require.Greater(t, len(order), 5, "graph must have root + plans + tools + answer")

	// The graph has exactly 2 plan nodes (depth 2) + 2 tool nodes + root
	// + answer.
	depth := g.PlanDepth()
	require.Equal(t, 2, depth, "two plan rounds must have grown")

	// Session release.
	require.NoError(t, reg.ReleaseSession(sessionID))

	// After release, GetSession returns ErrSessionNotFound.
	_, err = reg.GetSession(sessionID)
	require.ErrorIs(t, err, ErrSessionNotFound)

	// Chat client was called exactly 3 times (2 tool rounds + 1 answer).
	chatClient.mu.Lock()
	require.Equal(t, 3, chatClient.calls, "LLM called once per plan quantum")
	chatClient.mu.Unlock()
}

// TestPlannerCognition_MaxDepthForcesAnswer verifies the growth-depth guard
// (M2-④): when the plan depth reaches the max, the planner grows an answer
// node instead of another tool round.
func TestPlannerCognition_MaxDepthForcesAnswer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const sessionID = "m2-depth"
	fabric := taskfabric.NewFabric()
	coord := planprojection.NewCompileCoordinator(fabric, nil)
	reg := NewSessionRegistry()
	compileCoord := func(_ context.Context, dag *engine.MutableDAG) (stop func()) {
		return coord.SubscribeGraphEvents(ctx, dag)
	}
	g, err := reg.InitSession(ctx, sessionID, "prompt", nil, compileCoord)
	require.NoError(t, err)

	// Admit root.
	rootStep := g.DAG().StepIndex()[g.Root()]
	_, err = fabric.CompileNode(ctx, planprojection.ProjectStep(rootStep))
	require.NoError(t, err)
	driveTaskToCompleted(t, ctx, fabric, g.Root(), "prompt")

	// Planner with MaxDepth=1: the first plan quantum can grow, but the
	// second must force an answer.
	chatClient := &fakePlannerChat{}
	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: chatClient,
		ToolBinder: &plannerTestBinder{},
		Sessions:   reg,
		Fabric:     fabric,
		MaxDepth:   1,
		Logger:     slog.Default(),
	})
	require.NoError(t, err)

	// Round 1 (depth 0 < 1): grows a tool node.
	planID := SessionNodeID(sessionID, 0, "plan", 0)
	planTask := models.NewTask(planID, models.AgentType("ares/plan"), nil)
	planTask.SessionID = sessionID
	planTask.Payload = map[string]any{"input": "prompt", planMetadataKey: sessionID}

	out, err := planner.ExecuteStep(ctx, planTask)
	require.NoError(t, err)
	require.True(t, out.Done)
	require.Equal(t, 1, g.PlanDepth(), "one plan node grown")

	// Round 2 (depth 1 >= 1): forced answer, no more tool nodes.
	plan2ID := SessionNodeID(sessionID, 1, "plan", 0)
	plan2Task := models.NewTask(plan2ID, models.AgentType("ares/plan"), nil)
	plan2Task.SessionID = sessionID
	plan2Task.Payload = map[string]any{"input": "prompt", planMetadataKey: sessionID}

	out2, err := planner.ExecuteStep(ctx, plan2Task)
	require.NoError(t, err)
	require.True(t, out2.Done)
	require.Equal(t, 1, g.PlanDepth(), "no new plan node — forced answer instead")

	// An answer node was grown.
	answerID := SessionNodeID(sessionID, 2, "answer", 0)
	waitForTaskExists(t, fabric, answerID, 2*time.Second)
}

// TestReaper_HarvestsTerminalTasks verifies the M2-⑤ reaper: terminal tasks
// matching the session prefix are deleted; in-flight tasks survive; the
// grace period protects recently-completed tasks.
func TestReaper_HarvestsTerminalTasks(t *testing.T) {
	fabric := taskfabric.NewFabric()

	// Seed: a COMPLETED session task, a FAILED session task, a READY session
	// task, a non-session COMPLETED task.
	createTask(t, fabric, "sess/s1/tool-a", "tool/grep", taskfabric.StateCompleted)
	createTask(t, fabric, "sess/s1/tool-b", "tool/read", taskfabric.StateFailed)
	createTask(t, fabric, "sess/s1/tool-c", "tool/grep", taskfabric.StateReady)
	createTask(t, fabric, "peer-task-other-1", "code", taskfabric.StateCompleted)

	// Reaper with 1ns grace — the tasks were just created, but 1ns is
	// short enough that even a few nanoseconds' elapsed time exceeds
	// it. The production default is 30s; here we need immediate
	// harvesting, and 1ns avoids time.Sleep (code_rules_v2 §7.3).
	r := taskfabric.NewReaper(fabric, "sess/s1/", 1)
	removed := r.Sweep()
	require.Equal(t, 2, removed, "only COMPLETED + FAILED session tasks harvested")

	// The READY session task survives (Delete refuses non-terminal).
	_, err := fabric.Task("sess/s1/tool-c")
	require.NoError(t, err)

	// The non-session task survives (prefix filter).
	_, err = fabric.Task("peer-task-other-1")
	require.NoError(t, err)

	// The harvested tasks are gone.
	_, err = fabric.Task("sess/s1/tool-a")
	require.ErrorIs(t, err, taskfabric.ErrTaskNotFound)
	_, err = fabric.Task("sess/s1/tool-b")
	require.ErrorIs(t, err, taskfabric.ErrTaskNotFound)
}

// driveTaskToCompleted acquires, starts, and completes a task with the
// given output content so its envelope carries a real result.
func driveTaskToCompleted(t *testing.T, ctx context.Context, f *taskfabric.Fabric, taskID, content string) {
	t.Helper()
	// Acquire the task.
	epoch, err := f.Acquire(taskID, "test-agent", time.Minute)
	require.NoError(t, err)
	// Start it.
	require.NoError(t, f.Start(taskID, "test-agent", epoch))
	// Complete with a checkpoint carrying the output.
	items := []*models.RecommendItem{{ItemID: taskID, Content: content}}
	outMap := map[string]any{
		"result": "ok",
		"items":  items,
		"reason": "test completion",
	}
	env := taskfabric.EncodeCheckpoint(taskfabric.DecodedCheckpoint{
		StepCheckpoint: outMap,
	})
	require.NoError(t, f.CompleteWithCheckpoint(taskID, "test-agent", epoch, env))
}

// createTask creates a task and drives it to the given terminal state.
func createTask(t *testing.T, f *taskfabric.Fabric, id, capability string, state taskfabric.TaskState) {
	t.Helper()
	// For FAILED tasks, set MaxRetries=1 so CanRetry returns false after
	// one attempt. MaxRetries=0 means "unlimited retries" in this fabric
	// (CanRetry: MaxRetries <= 0 || Attempts < MaxRetries), so a 0-budget
	// task never settles FAILED — it requeues to READY on every Fail.
	maxRetries := 0
	if state == taskfabric.StateFailed {
		maxRetries = 1
	}
	require.NoError(t, f.Create(&taskfabric.Task{
		ID:          id,
		Capability:  capability,
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: maxRetries},
	}))
	if state == taskfabric.StateCompleted || state == taskfabric.StateFailed {
		epoch, err := f.Acquire(id, "reaper-test", time.Minute)
		require.NoError(t, err)
		require.NoError(t, f.Start(id, "reaper-test", epoch))
		if state == taskfabric.StateCompleted {
			require.NoError(t, f.Complete(id, "reaper-test", epoch))
		} else {
			require.NoError(t, f.Fail(id, "reaper-test", epoch))
			got, _ := f.Task(id)
			require.Equal(t, taskfabric.StateFailed, got.State,
				"task must settle FAILED after exhausting retry budget")
		}
	}
}

// waitForTaskExists polls until a task with the given ID exists in the fabric.
func waitForTaskExists(t *testing.T, f *taskfabric.Fabric, id string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := f.Task(id); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %q not found within %s", id, timeout)
}
