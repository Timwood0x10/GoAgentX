package agentfabric

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/planprojection"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// l1ChatAlwaysGrep is a scripted ChatClient that always returns one "grep"
// tool call. Used to test that the planner skips a disabled tool and instead
// grows an answer node.
type l1ChatAlwaysGrep struct{}

func (c *l1ChatAlwaysGrep) Chat(_ context.Context, _ []*core.LLMMessage, _ []core.Tool, _ map[string]any) (*core.GenerateResponse, error) {
	return &core.GenerateResponse{
		ToolCalls: []core.ToolCall{{
			ID:   "tc-grep",
			Type: "function",
			Function: core.FunctionCall{
				Name:      "grep",
				Arguments: `{"query":"test"}`,
			},
		}},
	}, nil
}

// l1ChatGrepThenAnswer returns a grep tool call on the first invocation,
// then a text answer on the second. Used to test budget exhaustion across
// multiple rounds.
type l1ChatGrepThenAnswer struct {
	calls int
}

func (c *l1ChatGrepThenAnswer) Chat(_ context.Context, _ []*core.LLMMessage, _ []core.Tool, _ map[string]any) (*core.GenerateResponse, error) {
	c.calls++
	if c.calls == 1 {
		return &core.GenerateResponse{
			ToolCalls: []core.ToolCall{{
				ID:   "tc-grep-1",
				Type: "function",
				Function: core.FunctionCall{
					Name:      "grep",
					Arguments: `{"query":"first"}`,
				},
			}},
		}, nil
	}
	return &core.GenerateResponse{Content: "done"}, nil
}

// buildTestL1DAG builds an L1 ToolClass DAG with configurable enabled/budget
// metadata for the "grep" tool. The node ID uses the SCHEMA-derived argShape
// (resources.ToolArgShape over plannerTestBinder's declared properties), which
// is what the production L1 builder writes — not the shape of any one call.
func buildTestL1DAG(t *testing.T, enabled string, budget string) *engine.MutableDAG {
	t.Helper()
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{
			ID:        resources.ToolClassID("grep", resources.ToolArgShape(grepSchema(t))),
			Name:      "grep",
			AgentType: "tool/grep",
			Metadata: map[string]string{
				l1MetaEnabled: enabled,
				l1MetaBudget:  budget,
			},
		},
	})
	require.NoError(t, err)
	return dag
}

// TestM5_DisabledToolNotGrown pins §6 constraint point: when L1 has
// enabled="false" for a ToolClass, the planner skips growing that tool node
// and instead grows an answer node (since no tool call was executed).
func TestM5_DisabledToolNotGrown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	coord := planprojection.NewCompileCoordinator(fabric, nil)
	reg := NewSessionRegistry()
	compileCoord := func(_ context.Context, dag *engine.MutableDAG) (stop func()) {
		return coord.SubscribeGraphEvents(ctx, dag)
	}

	const sessionID = "m5-disabled"
	g, err := reg.InitSession(ctx, sessionID, "test prompt", nil, compileCoord)
	require.NoError(t, err)

	// Admit root.
	rootStep := g.DAG().StepIndex()[g.Root()]
	_, err = fabric.CompileNode(ctx, planprojection.ProjectStep(rootStep))
	require.NoError(t, err)
	driveTaskToCompleted(t, ctx, fabric, g.Root(), "test prompt")

	// L1 with grep disabled.
	l1 := buildTestL1DAG(t, "false", "0")

	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: &l1ChatAlwaysGrep{},
		ToolBinder: &plannerTestBinder{},
		Sessions:   reg,
		Fabric:     fabric,
		L1DAG:      l1,
		Logger:     slog.Default(),
	})
	require.NoError(t, err)

	planTask := models.NewTask(
		SessionNodeID(sessionID, 0, "plan", 0),
		models.AgentType("ares/plan"), nil)
	planTask.SessionID = sessionID
	planTask.Payload = map[string]any{"input": "test prompt", planMetadataKey: sessionID}

	out, err := planner.ExecuteStep(ctx, planTask)
	require.NoError(t, err)
	require.True(t, out.Done)

	// The grep tool node was NOT grown — instead an answer node was grown
	// because the LLM's tool call was skipped.
	grepID := SessionNodeID(sessionID, 1, "grep", 0)
	assert.False(t, g.HasNode(grepID),
		"disabled tool node must not be grown in the L2 graph")

	// An answer node should exist (the planner forces an answer when all
	// tool calls are skipped, because there's nothing to execute).
	answerID := SessionNodeID(sessionID, 1, "answer", 0)
	assert.True(t, g.HasNode(answerID),
		"answer node must be grown when tool call is skipped by L1 constraint")
}

// TestM5_BudgetCapsInstances pins §6 budget: budget=1 means at most 1
// instance of the ToolClass in the L2 graph. The second grep call must be
// skipped.
func TestM5_BudgetCapsInstances(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	coord := planprojection.NewCompileCoordinator(fabric, nil)
	reg := NewSessionRegistry()
	compileCoord := func(_ context.Context, dag *engine.MutableDAG) (stop func()) {
		return coord.SubscribeGraphEvents(ctx, dag)
	}

	const sessionID = "m5-budget"
	g, err := reg.InitSession(ctx, sessionID, "test prompt", nil, compileCoord)
	require.NoError(t, err)

	// Admit root.
	rootStep := g.DAG().StepIndex()[g.Root()]
	_, err = fabric.CompileNode(ctx, planprojection.ProjectStep(rootStep))
	require.NoError(t, err)
	driveTaskToCompleted(t, ctx, fabric, g.Root(), "test prompt")

	// L1 with grep budget=1.
	l1 := buildTestL1DAG(t, "true", "1")

	chat := &l1ChatGrepThenAnswer{}
	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: chat,
		ToolBinder: &plannerTestBinder{},
		Sessions:   reg,
		Fabric:     fabric,
		L1DAG:      l1,
		Logger:     slog.Default(),
	})
	require.NoError(t, err)

	// Round 1: grep should grow (budget=1, count=0).
	planTask := models.NewTask(
		SessionNodeID(sessionID, 0, "plan", 0),
		models.AgentType("ares/plan"), nil)
	planTask.SessionID = sessionID
	planTask.Payload = map[string]any{"input": "test prompt", planMetadataKey: sessionID}

	out1, err := planner.ExecuteStep(ctx, planTask)
	require.NoError(t, err)
	require.True(t, out1.Done)

	grepID := SessionNodeID(sessionID, 1, "grep", 0)
	assert.True(t, g.HasNode(grepID),
		"first grep within budget must be grown")

	// Wait for the incremental compiler to create the fabric task for
	// the grep node, then drive it to completion so the next plan
	// quantum can fire.
	waitForTaskExists(t, fabric, grepID, 2*time.Second)
	driveTaskToCompleted(t, ctx, fabric, grepID, "echo(grep,test)")

	// Round 2: planner tries grep again but budget is exhausted.
	plan2Task := models.NewTask(
		SessionNodeID(sessionID, 1, "plan", 0),
		models.AgentType("ares/plan"), nil)
	plan2Task.SessionID = sessionID
	plan2Task.Payload = map[string]any{"input": "test prompt", planMetadataKey: sessionID}

	// Reset chat to return grep again (to test budget exhaustion).
	chat.calls = 0

	out2, err := planner.ExecuteStep(ctx, plan2Task)
	require.NoError(t, err)
	require.True(t, out2.Done)

	// A second grep at the same depth was NOT grown (budget exhausted).
	// Instead an answer node was grown (forced by grown==0 path).
	grep2ID := SessionNodeID(sessionID, 2, "grep", 0)
	assert.False(t, g.HasNode(grep2ID),
		"second grep with budget=1 must not be grown")
}

// TestM5_NilL1DAGIsPermissive pins the default: when no L1 graph is
// provided, all tool calls pass through (no constraints).
func TestM5_NilL1DAGIsPermissive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	coord := planprojection.NewCompileCoordinator(fabric, nil)
	reg := NewSessionRegistry()
	compileCoord := func(_ context.Context, dag *engine.MutableDAG) (stop func()) {
		return coord.SubscribeGraphEvents(ctx, dag)
	}

	const sessionID = "m5-nil"
	g, err := reg.InitSession(ctx, sessionID, "test prompt", nil, compileCoord)
	require.NoError(t, err)

	rootStep := g.DAG().StepIndex()[g.Root()]
	_, err = fabric.CompileNode(ctx, planprojection.ProjectStep(rootStep))
	require.NoError(t, err)
	driveTaskToCompleted(t, ctx, fabric, g.Root(), "test prompt")

	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: &l1ChatAlwaysGrep{},
		ToolBinder: &plannerTestBinder{},
		Sessions:   reg,
		Fabric:     fabric,
		L1DAG:      nil, // no L1 → permissive
		Logger:     slog.Default(),
	})
	require.NoError(t, err)

	planTask := models.NewTask(
		SessionNodeID(sessionID, 0, "plan", 0),
		models.AgentType("ares/plan"), nil)
	planTask.SessionID = sessionID
	planTask.Payload = map[string]any{"input": "test prompt", planMetadataKey: sessionID}

	out, err := planner.ExecuteStep(ctx, planTask)
	require.NoError(t, err)
	require.True(t, out.Done)

	grepID := SessionNodeID(sessionID, 1, "grep", 0)
	assert.True(t, g.HasNode(grepID),
		"nil L1 graph means permissive — tool must be grown")
}

// grepSchema returns the single declared schema plannerTestBinder exposes.
func grepSchema(t *testing.T) resources.ToolSchema {
	t.Helper()
	schemas := (&plannerTestBinder{}).GetToolSchemas()
	require.Len(t, schemas, 1)
	return schemas[0]
}

// TestM5_L1NodeIDMatchesWriterAndReader pins the ToolClass identity across the
// two sides that must agree: the L1 graph builder WRITES node IDs from the
// declared schema, and the planner READS them back before growing an L2 node.
// A call that omits an optional parameter must still resolve to the same
// ToolClass — otherwise the lookup misses and enabled=false is ignored.
func TestM5_L1NodeIDMatchesWriterAndReader(t *testing.T) {
	schema := grepSchema(t)
	written := resources.ToolClassID("grep", resources.ToolArgShape(schema))
	require.Equal(t, "grep#limit,query", written,
		"the writer shape is the sorted DECLARED property set, optional included")

	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: &l1ChatAlwaysGrep{},
		ToolBinder: &plannerTestBinder{},
		Sessions:   NewSessionRegistry(),
		Fabric:     taskfabric.NewFabric(),
		Logger:     slog.Default(),
	})
	require.NoError(t, err)

	read := planner.(*plannerCognition).l1ToolClassID("grep")
	assert.Equal(t, written, read,
		"the planner must resolve the same ToolClass the L1 builder wrote")
	assert.Empty(t, planner.(*plannerCognition).l1ToolClassID("unknown-tool"),
		"a tool with no schema has no ToolClass; the caller treats the miss as permissive")
}
