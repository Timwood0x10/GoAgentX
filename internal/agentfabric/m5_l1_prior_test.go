package agentfabric

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/planprojection"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// priorCapturingChat records the prompt it received and returns no tool calls
// (final answer), so the test can assert on prompt contents.
type priorCapturingChat struct {
	gotPrompt []*core.LLMMessage
}

func (c *priorCapturingChat) Chat(_ context.Context, prompt []*core.LLMMessage, _ []core.Tool, _ map[string]any) (*core.GenerateResponse, error) {
	c.gotPrompt = prompt
	return &core.GenerateResponse{Content: "done"}, nil
}

// buildPriorL1DAG builds an L1 DAG with a prior hint on the grep ToolClass.
func buildPriorL1DAG(t *testing.T, prior string) *engine.MutableDAG {
	t.Helper()
	dag, err := engine.NewMutableDAG([]*engine.Step{
		{
			ID:        resources.ToolClassID("grep", resources.ToolArgShape(grepSchema(t))),
			Name:      "grep",
			AgentType: "tool/grep",
			Metadata: map[string]string{
				l1MetaEnabled: "true",
				l1MetaBudget:  "0",
				l1MetaPrior:   prior,
			},
		},
	})
	require.NoError(t, err)
	return dag
}

// TestM5_PriorReachesPromptButNeverBlocks pins §6: prior is prompt-only —
// the hint text reaches the LLM, and growth is NOT blocked.
func TestM5_PriorReachesPromptButNeverBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	coord := planprojection.NewCompileCoordinator(fabric, nil)
	reg := NewSessionRegistry()
	compileCoord := func(_ context.Context, dag *engine.MutableDAG) (stop func()) {
		return coord.SubscribeGraphEvents(ctx, dag)
	}

	const sessionID = "m5-prior"
	g, err := reg.InitSession(ctx, sessionID, "test prompt", nil, compileCoord)
	require.NoError(t, err)
	rootStep := g.DAG().StepIndex()[g.Root()]
	_, err = fabric.CompileNode(ctx, planprojection.ProjectStep(rootStep))
	require.NoError(t, err)
	driveTaskToCompleted(t, ctx, fabric, g.Root(), "test prompt")

	chat := &priorCapturingChat{}
	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: &l1ChatGrepThenAnswerHint{inner: chat},
		ToolBinder: &plannerTestBinder{},
		Sessions:   reg,
		Fabric:     fabric,
		L1DAG:      buildPriorL1DAG(t, "prefer narrow queries"),
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

	// The prior hint must have reached the LLM prompt...
	found := false
	for _, m := range chat.gotPrompt {
		if m.Role == "system" && strings.Contains(m.Content, "prefer narrow queries") && strings.Contains(m.Content, "grep") {
			found = true
		}
	}
	assert.True(t, found, "prior hint must be injected as a system message naming the tool")

	// ...and the tool node must still have grown (prior never blocks).
	grepID := SessionNodeID(sessionID, 1, "grep", 0)
	assert.True(t, g.HasNode(grepID), "prior must not block tool growth")
}

// l1ChatGrepThenAnswerHint delegates to an inner capturing chat for the
// first quantum (grep tool call), so the prior injection (which happens
// before the LLM call) is observable on the inner recorder.
type l1ChatGrepThenAnswerHint struct {
	inner *priorCapturingChat
	calls int
}

func (c *l1ChatGrepThenAnswerHint) Chat(ctx context.Context, prompt []*core.LLMMessage, tools []core.Tool, extra map[string]any) (*core.GenerateResponse, error) {
	c.calls++
	c.inner.gotPrompt = prompt
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

// TestM5_HotUpdatedPriorIsPickedUp pins the M5 hot-update path: a
// SetNodeMetadata on the SAME L1 pointer is visible to the next quantum
// without rebuilding the planner.
func TestM5_HotUpdatedPriorIsPickedUp(t *testing.T) {
	l1 := buildPriorL1DAG(t, "")
	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: &priorCapturingChat{},
		ToolBinder: &plannerTestBinder{},
		Sessions:   NewSessionRegistry(),
		Fabric:     taskfabric.NewFabric(),
		L1DAG:      l1,
		Logger:     slog.Default(),
	})
	require.NoError(t, err)
	pc, ok := planner.(*plannerCognition)
	require.True(t, ok)
	assert.Equal(t, "", pc.l1ToolPrior("grep"))

	nodeID := resources.ToolClassID("grep", resources.ToolArgShape(grepSchema(t)))
	require.NoError(t, l1.SetNodeMetadata(nodeID, map[string]string{
		l1MetaEnabled: "true",
		l1MetaBudget:  "0",
		l1MetaPrior:   "hot hint",
	}))
	assert.Equal(t, "hot hint", pc.l1ToolPrior("grep"))
}
