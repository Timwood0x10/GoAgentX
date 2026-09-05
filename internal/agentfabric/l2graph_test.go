package agentfabric

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/core/models"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
)

// indexOf returns the position of id in order, or -1 when absent.
func indexOf(order []string, id string) int {
	for i, o := range order {
		if o == id {
			return i
		}
	}
	return -1
}

// stubBinder is a scripted ToolBinder for the M1 tests. Tools echo their args
// back so a test can assert data actually flowed into the tool call; a tool
// named in toolErr instead returns an error so the failure path is exercised.
type stubBinder struct {
	called  []string
	toolErr string
}

func (b *stubBinder) CallTool(_ context.Context, name string, args map[string]any) (any, error) {
	b.called = append(b.called, name)
	if b.toolErr == name {
		return nil, fmt.Errorf("stub: %s failed", name)
	}
	if q, ok := args["query"]; ok {
		return fmt.Sprintf("echo(%s,%v)", name, q), nil
	}
	return fmt.Sprintf("echo(%s)", name), nil
}

func (b *stubBinder) ListTools() []string { return []string{"grep", "read"} }

func (b *stubBinder) IsToolIdempotent(string) bool { return true }

func (b *stubBinder) GetToolSchemas() []resources.ToolSchema { return nil }

// TestL2Graph_TopologyPinsDependencies verifies the L2 container is a PLAN:
// it grows tool/answer nodes with dependency edges and reports a deterministic
// topological order, WITHOUT executing anything. Execution facts live in the
// fabric, not on the graph (design decision C, §4.3).
func TestL2Graph_TopologyPinsDependencies(t *testing.T) {
	ctx := context.Background()

	g, err := NewL2Graph("root", "find the answer", nil)
	require.NoError(t, err)
	require.NoError(t, g.AddToolNode(ctx, "n1", "grep", map[string]any{"query": "x"}, "root"))
	require.NoError(t, g.AddToolNode(ctx, "n2", "read", map[string]any{"query": "y"}, "n1"))
	require.NoError(t, g.AddToolNode(ctx, "n3", "answer", nil, "n2"))

	steps := g.DAG().StepIndex()
	require.Equal(t, []string{"root"}, steps["n1"].DependsOn)
	require.Equal(t, []string{"n1"}, steps["n2"].DependsOn)
	require.Equal(t, []string{"n2"}, steps["n3"].DependsOn)
	// The root carries the session-invariant prompt.
	require.Equal(t, "ares/root", steps["root"].AgentType)

	order, err := g.DAG().GetExecutionOrder()
	require.NoError(t, err)
	require.Len(t, order, 4, "root + 3 executable nodes are all planned")
	// Root is the session invariant and appears before every instance.
	require.Equal(t, "root", order[0])
	require.Less(t, indexOf(order, "n1"), indexOf(order, "n2"))
	require.Less(t, indexOf(order, "n2"), indexOf(order, "n3"))
}

// TestL2Graph_ArgsRoundTripJSON verifies structured args survive the
// string-only Metadata round-trip back into a usable map.
func TestL2Graph_ArgsRoundTripJSON(t *testing.T) {
	args, err := argsFromPayload(map[string]any{
		"query": `{"regex":"foo.*bar","case":true}`,
		"path":  "src/main.go",
	})
	require.NoError(t, err)
	obj, ok := args["query"].(map[string]any)
	require.True(t, ok, "JSON object arg must decode to a map")
	require.Equal(t, true, obj["case"])
	// A plain string arg passes through unchanged.
	require.Equal(t, "src/main.go", args["path"])
}

// TestL2Cognition_RouterDispatchesTool verifies tool/<name> capability routes
// to toolCognition: one CallTool runs and its result rides in the StepOutcome
// (the scheduler's buildQuantumStep then lands it in the fabric envelope).
func TestL2Cognition_RouterDispatchesTool(t *testing.T) {
	binder := &stubBinder{}
	cog := NewRouterCognition(binder, slog.Default())

	task := taskFor("n1", "tool/grep", map[string]any{"query": "x"})
	out, err := cog.ExecuteStep(context.Background(), task)
	require.NoError(t, err)
	require.True(t, out.Done)
	require.Equal(t, []string{"grep"}, binder.called)
	require.Equal(t, "echo(grep,x)", out.Result.Items[0].Content)
}

// TestL2Cognition_RouterDispatchesAnswer verifies ares/answer capability
// routes to answerCognition and yields the terminal result.
func TestL2Cognition_RouterDispatchesAnswer(t *testing.T) {
	cog := NewRouterCognition(&stubBinder{}, slog.Default())

	out, err := cog.ExecuteStep(context.Background(), taskFor("n3", "ares/answer", nil))
	require.NoError(t, err)
	require.True(t, out.Done)
	require.Equal(t, "L2 session complete", out.Result.Items[0].Content)
}

// TestL2Cognition_RouterUnknownCapabilityErrors pins that a capability the
// three bodies do not cover is rejected, not silently ignored.
func TestL2Cognition_RouterUnknownCapabilityErrors(t *testing.T) {
	cog := NewRouterCognition(&stubBinder{}, slog.Default())

	_, err := cog.ExecuteStep(context.Background(), taskFor("x", "foo/bar", nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported L2 capability")
}

// TestL2Cognition_RouterToolErrorSurfaces verifies a failing tool call comes
// back as an error (the scheduler converts executor errors to fabric.Fail).
func TestL2Cognition_RouterToolErrorSurfaces(t *testing.T) {
	binder := &stubBinder{toolErr: "grep"}

	_, err := NewRouterCognition(binder, slog.Default()).ExecuteStep(
		context.Background(), taskFor("n1", "tool/grep", map[string]any{"query": "x"}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "grep")
}

// TestL2Cognition_RouterBinderRequired verifies a tool capability without a
// binder is rejected (cannot execute a tool it cannot call).
func TestL2Cognition_RouterBinderRequired(t *testing.T) {
	_, err := NewRouterCognition(nil, slog.Default()).ExecuteStep(
		context.Background(), taskFor("n1", "tool/grep", map[string]any{"query": "x"}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "no binder")
}

// taskFor builds the models.Task a router sees — AgentType carries the node
// capability and Payload carries the args (restored by the scheduler from the
// fabric envelope).
func taskFor(id, capability string, payload map[string]any) *models.Task {
	task := models.NewTask(id, models.AgentType(capability), nil)
	task.Payload = payload
	return task
}
