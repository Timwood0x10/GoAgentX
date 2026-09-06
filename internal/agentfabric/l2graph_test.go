package agentfabric

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/core/models"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// TestDAGExecution_SelectKeepsLegacyBehaviorOff pins the gate default: the
// zero-value gate returns the chat cognition — production behavior identical
// to today.
func TestDAGExecution_SelectKeepsLegacyBehaviorOff(t *testing.T) {
	var gate DAGExecution
	chat := CognitionFunc(func(context.Context, *models.Task) (*StepOutcome, error) { return nil, nil })
	router := CognitionFunc(func(context.Context, *models.Task) (*StepOutcome, error) { return nil, nil })

	require.Equal(t, reflect.ValueOf(chat).Pointer(), reflect.ValueOf(gate.Select(chat, router)).Pointer())
	require.Equal(t, reflect.ValueOf(router).Pointer(),
		reflect.ValueOf(DAGExecution{Enabled: true}.Select(chat, router)).Pointer())
}

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
// fabric, never on the graph.
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
// string-only Metadata round-trip back into a usable map. Payload keys ride
// the arg. namespace (D3); unprefixed envelope plumbing never becomes a tool
// arg. A namespaced value that only looks like JSON stays a plain string
// rather than failing the extraction.
func TestL2Graph_ArgsRoundTripJSON(t *testing.T) {
	args := argsFromPayload(map[string]any{
		"arg.query":  `{"regex":"foo.*bar","case":true}`,
		"arg.path":   "src/main.go",
		"arg.broken": `{not json`,
		"input":      "session prompt, not a tool arg",
	})
	obj, ok := args["query"].(map[string]any)
	require.True(t, ok, "JSON object arg must decode to a map")
	require.Equal(t, true, obj["case"])
	// A plain string arg passes through unchanged.
	require.Equal(t, "src/main.go", args["path"])
	require.Equal(t, `{not json`, args["broken"], "unparseable JSON is a plain string, not a failure")
	require.NotContains(t, args, "input")
}

// TestL2Graph_AddToolNodeEmitsSingleEvent pins single-call growth: one
// AddToolNode is one AddNode whose Step already carries DependsOn, so
// subscribers see exactly one ChangeAddNode event — never an AddNode +
// AddEdge pair.
func TestL2Graph_AddToolNodeEmitsSingleEvent(t *testing.T) {
	ctx := context.Background()

	g, err := NewL2Graph("root", "find the answer", nil)
	require.NoError(t, err)
	subID, ch := g.DAG().SubscribeWithID()
	defer g.DAG().Unsubscribe(subID)

	require.NoError(t, g.AddToolNode(ctx, "n1", "grep", map[string]any{"query": "x"}, "root"))

	select {
	case evt := <-ch:
		require.True(t, evt.Success)
		require.Equal(t, engine.ChangeAddNode, evt.Change.Type)
		require.Equal(t, "n1", evt.Change.NodeID)
		require.Equal(t, []string{"root"}, evt.Change.Step.DependsOn)
	case <-time.After(2 * time.Second):
		t.Fatal("expected exactly one ChangeAddNode event")
	}
	// No follow-up AddEdge event may arrive: the edge is already in the node.
	select {
	case evt := <-ch:
		t.Fatalf("unexpected second event: type=%d node=%q", evt.Change.Type, evt.Change.NodeID)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestL2Graph_ArgsNamespacedInMetadata pins arg namespacing at the graph
// layer: tool args land in Metadata under the arg. prefix, so the projection
// cannot leak envelope keys into the tool call.
func TestL2Graph_ArgsNamespacedInMetadata(t *testing.T) {
	ctx := context.Background()

	g, err := NewL2Graph("root", "find the answer", nil)
	require.NoError(t, err)
	require.NoError(t, g.AddToolNode(ctx, "n1", "grep", map[string]any{"query": "x"}, "root"))

	md := g.DAG().StepIndex()["n1"].Metadata
	require.Equal(t, map[string]string{"arg.query": "x"}, md)
}

// TestL2Cognition_RouterDispatchesTool verifies tool/<name> capability routes
// to toolCognition: one CallTool runs and its result rides in the StepOutcome
// (the scheduler's buildQuantumStep then lands it in the fabric envelope).
func TestL2Cognition_RouterDispatchesTool(t *testing.T) {
	binder := &stubBinder{}
	cog := NewRouterCognition(binder, slog.Default())

	task := taskFor("n1", "tool/grep", map[string]any{"arg.query": "x", "input": "not a tool arg"})
	out, err := cog.ExecuteStep(context.Background(), task)
	require.NoError(t, err)
	require.True(t, out.Done)
	require.Equal(t, []string{"grep"}, binder.called)
	require.Equal(t, "echo(grep,x)", out.Result.Items[0].Content)
}

// TestL2Cognition_RouterDispatchesAnswer verifies ares/answer capability
// routes to answerCognition and emits the content the node carries.
func TestL2Cognition_RouterDispatchesAnswer(t *testing.T) {
	cog := NewRouterCognition(&stubBinder{}, slog.Default())

	out, err := cog.ExecuteStep(context.Background(),
		taskFor("n3", "ares/answer", map[string]any{"arg.content": "42 is the answer"}))
	require.NoError(t, err)
	require.True(t, out.Done)
	require.Equal(t, "42 is the answer", out.Result.Items[0].Content)
}

// TestL2Cognition_AnswerWithoutContentSaysSo pins code_rules_v2 §0.2 on the
// terminal node: with no supplied content and no summarizer wired, the body
// states the absence and the gap is logged — it must NOT return a
// success-sounding constant that reads like a real answer.
func TestL2Cognition_AnswerWithoutContentSaysSo(t *testing.T) {
	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
	cog := NewRouterCognition(&stubBinder{}, logger)

	out, err := cog.ExecuteStep(context.Background(), taskFor("n3", "ares/answer", nil))
	require.NoError(t, err)
	require.True(t, out.Done, "the session still terminates: a missing summary is not a task failure")
	require.Equal(t, unansweredBody, out.Result.Items[0].Content)
	require.Contains(t, logged.String(), "no summarizer is wired",
		"the unimplemented summary must be visible to operations, not silent")

	// Whitespace-only content is no content either.
	blank, err := cog.ExecuteStep(context.Background(),
		taskFor("n3", "ares/answer", map[string]any{"arg.content": "   "}))
	require.NoError(t, err)
	require.Equal(t, unansweredBody, blank.Result.Items[0].Content)
}

// TestL2Cognition_RouterDispatchesRoot verifies ares/root admits the session
// in one zero-work quantum: the session prompt (payload "input") becomes the
// root output, so planners read it from the envelope by ID-join.
func TestL2Cognition_RouterDispatchesRoot(t *testing.T) {
	cog := NewRouterCognition(&stubBinder{}, slog.Default())

	out, err := cog.ExecuteStep(context.Background(),
		taskFor("root", "ares/root", map[string]any{"input": "find the answer"}))
	require.NoError(t, err)
	require.True(t, out.Done)
	require.Equal(t, "find the answer", out.Result.Items[0].Content)
}

// TestL2Cognition_RouterUnknownCapabilityErrors pins that a capability the
// bodies do not cover is rejected, not silently ignored.
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
		context.Background(), taskFor("n1", "tool/grep", map[string]any{"arg.query": "x"}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "grep")
}

// strictBinder is a fake strict-schema tool: it rejects
// any argument key outside its declared set, like an MCP tool with
// additionalProperties:false would.
type strictBinder struct {
	declared map[string]bool
	got      map[string]any
}

func (b *strictBinder) CallTool(_ context.Context, name string, args map[string]any) (any, error) {
	for k := range args {
		if !b.declared[k] {
			return nil, fmt.Errorf("strict: tool %s got undeclared arg %q", name, k)
		}
	}
	b.got = args
	return "ok", nil
}

func (b *strictBinder) ListTools() []string { return []string{"grep"} }

func (b *strictBinder) IsToolIdempotent(string) bool { return true }

func (b *strictBinder) GetToolSchemas() []resources.ToolSchema { return nil }

// TestL2Cognition_StrictToolReceivesOnlyDeclaredArgs pins arg filtering end
// to end at the cognition layer: a tool node whose payload carries envelope
// plumbing ("input", scheduler-restore keys) still reaches CallTool with
// exactly its declared args.
func TestL2Cognition_StrictToolReceivesOnlyDeclaredArgs(t *testing.T) {
	binder := &strictBinder{declared: map[string]bool{"query": true}}
	cog := NewRouterCognition(binder, slog.Default())

	task := taskFor("n1", "tool/grep", map[string]any{
		"arg.query":  "x",
		"input":      "session prompt",
		"checkpoint": map[string]any{"stale": true},
	})
	out, err := cog.ExecuteStep(context.Background(), task)
	require.NoError(t, err)
	require.True(t, out.Done)
	require.Equal(t, map[string]any{"query": "x"}, binder.got)
}

// TestL2Cognition_RouterBinderRequired verifies a tool capability without a
// binder is rejected (cannot execute a tool it cannot call).
func TestL2Cognition_RouterBinderRequired(t *testing.T) {
	_, err := NewRouterCognition(nil, slog.Default()).ExecuteStep(
		context.Background(), taskFor("n1", "tool/grep", map[string]any{"arg.query": "x"}))
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

// TestL2Cognition_AnswerReleasesSession pins M4-B2 teardown: when a router
// with session wiring executes the terminal node, the session is released —
// the graph handle drops and the compile subscription stops, so no new nodes
// can grow into a finished session. A second execution still emits the answer
// (the release miss is attributable via log, not fatal).
func TestL2Cognition_AnswerReleasesSession(t *testing.T) {
	ctx := context.Background()
	reg := NewSessionRegistry()
	g, err := reg.InitSession(ctx, "rel-1", "prompt", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, g)

	router := NewRouterCognitionWithPlanner(&stubBinder{}, nil, reg, slog.Default())
	task := taskFor("n9", "ares/answer", map[string]any{"arg.content": "done"})
	task.SessionID = "rel-1"

	out, err := router.ExecuteStep(ctx, task)
	require.NoError(t, err)
	require.True(t, out.Done)
	require.Equal(t, "done", out.Result.Items[0].Content)

	_, err = reg.GetSession("rel-1")
	require.ErrorIs(t, err, ErrSessionNotFound, "terminal execution must release the session")

	again, err := router.ExecuteStep(ctx, task)
	require.NoError(t, err, "re-executing after release must still emit the answer")
	require.True(t, again.Done)
}

// TestL2Cognition_AnswerWithoutSessionsKeepsWorking pins that the release is
// opt-in: routers built without session wiring (legacy + tests) execute the
// terminal node exactly like before.
func TestL2Cognition_AnswerWithoutSessionsKeepsWorking(t *testing.T) {
	router := NewRouterCognition(&stubBinder{}, slog.Default())
	out, err := router.ExecuteStep(context.Background(),
		taskFor("n3", "ares/answer", map[string]any{"arg.content": "x"}))
	require.NoError(t, err)
	require.True(t, out.Done)
	require.Equal(t, "x", out.Result.Items[0].Content)
}

// TestIsL2Capability_PartitionsTraffic pins the canary partition: the L2 set
// (tool/* instances + three ares/* session nodes) is exactly what gate-on
// peers advertise, so scheduler routing cannot mix legacy and L2 traffic.
func TestIsL2Capability_PartitionsTraffic(t *testing.T) {
	l2 := []string{"tool/grep", "tool/read", "ares/root", "ares/plan", "ares/answer"}
	for _, c := range l2 {
		require.True(t, IsL2Capability(c), "%q must route to L2 peers", c)
	}
	legacy := []string{"researcher", "worker", "", "tool/", "ares/unknown", "TOOL/grep"}
	for _, c := range legacy {
		require.False(t, IsL2Capability(c), "%q must NOT route to L2 peers", c)
	}
}
