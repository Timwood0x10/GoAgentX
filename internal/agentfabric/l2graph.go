package agentfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// L2Graph is the per-session execution PLAN of one agent run
// (TOOL_DAG_MAINLINE_DESIGN §2): a session-local engine.MutableDAG whose
// nodes are TOOL INSTANCES — one node per actual tool execution — together
// with the session root that carries the durable, session-invariant
// prompt/params. It is phase-M1 delivery: an independent, testable container
// for the L1-constrained execution of a frozen DAG; it is NOT yet wired into
// the production serve path (that remains the default ReAct chatCognition
// until M3+ land, per the mainline "default-off, no dual loop" invariant).
//
// The L2 graph is a first-class engine.MutableDAG so it reuses every workflow
// primitive (topological order, patch/differ, node Metadata) and so evolution
// can later constrain its growth by reading L1's enabled/budget/prior into
// node Metadata. The graph holds topology + Metadata ONLY — it does not carry
// execution facts. Each non-root node maps 1:1 to a fabric task by ID; a
// node's execution result (Output) is read from that task's checkpoint
// envelope (Output 落点 = fabric, per design decision C in §4.3).
type L2Graph struct {
	mu sync.RWMutex // guards dag and root

	// dag is the session execution plan. Nodes are tool/answer instances.
	dag *engine.MutableDAG

	// root is the session root node carrying the session-invariant prompt and
	// params (Metadata). Answers and tool input are linked off it.
	root string
}

// NewL2Graph builds an empty L2 execution plan with the given session root.
//
// Args:
//   - rootID: the id of the session root node (durable across the session).
//   - prompt: the session-invariant prompt, stored on the root node.
//   - params: the session-invariant params, flattened onto the root node's
//     Metadata.
//
// Returns:
//   - *L2Graph, or error when the root node cannot be created.
func NewL2Graph(rootID, prompt string, params map[string]any) (*L2Graph, error) {
	if strings.TrimSpace(rootID) == "" {
		return nil, fmt.Errorf("agentfabric: L2 graph root id is required")
	}
	rootStep := &engine.Step{
		ID:        rootID,
		AgentType: "ares/root",
		Input:     prompt,
		Metadata:  metadataFromParams(params),
	}
	dag, err := engine.NewMutableDAG([]*engine.Step{rootStep})
	if err != nil {
		return nil, fmt.Errorf("agentfabric: create L2 graph: %w", err)
	}
	return &L2Graph{
		dag:  dag,
		root: rootID,
	}, nil
}

// Root returns the session root node id.
func (g *L2Graph) Root() string { return g.root }

// DAG returns the underlying execution graph. Callers must treat it as
// read-only unless initiated through graph mutations; the returned graph is
// the live object so mutation events propagate.
func (g *L2Graph) DAG() *engine.MutableDAG {
	return g.dag
}

// AddToolNode grows a tool-instance node into the session graph and links a
// dependency edge from the given predecessor (the session root, or the last
// tool node in a chain) to it.
//
// Args:
//   - ctx: bounds the graph mutation.
//   - id: the instance node id (unique within this session's L2 graph).
//   - tool: the tool name for a tool node; "answer" creates the terminal
//     answer node instead.
//   - args: the concrete tool arguments; written as node Metadata so the
//     executing cognition can read them without a graph walk.
//   - dependsOn: the node this instance depends on (output feeding into it).
func (g *L2Graph) AddToolNode(ctx context.Context, id, tool string, args map[string]any, dependsOn string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	var agentType string
	var md map[string]string
	if tool == "answer" {
		agentType = "ares/answer"
		md = metadataFromParams(args)
	} else {
		agentType = "tool/" + tool
		md = metadataFromParams(args)
	}
	step := &engine.Step{
		ID:        id,
		AgentType: agentType,
		Metadata:  md,
	}
	if err := g.dag.AddNode(ctx, step); err != nil {
		return fmt.Errorf("agentfabric: add tool node %q: %w", id, err)
	}
	if err := g.dag.AddEdge(ctx, dependsOn, id); err != nil {
		return fmt.Errorf("agentfabric: link %q -> %q: %w", dependsOn, id, err)
	}
	return nil
}

// routerCognition dispatches ONE agent's quantum to the sub-cognition named by
// the scheduled task's capability (TOOL_DAG_MAINLINE_DESIGN §3.1): tool/<name>
// → toolCognition, ares/answer → answerCognition. It is the production-forward
// seed of the three-body dispatch: a session agent declares its FULL capability
// set and the cognition factory returns ONE router that picks the body by the
// task's capability at execute time. The routing key (Task.Capability →
// candidate overlap → fabricAgentExecutor → this Cognition) is the same key
// the scheduler already resolves, so no new dispatch mechanism is introduced.
//
// Execution facts do NOT land on graph nodes (Output 落点 = fabric task
// envelope): this Cognition only returns a StepOutcome; the scheduler's
// buildQuantumStep re-wraps Result into the envelope and the dispatcher reads
// it from there. The L2 graph holds topology + Metadata (the plan) only.
type routerCognition struct {
	binder ToolBinder
	logger *slog.Logger
}

var _ Cognition = (*routerCognition)(nil)

// NewRouterCognition builds the capability-dispatch Cognition for an L2
// session agent. binder executes tool nodes (may be nil only when the agent
// declares no tool capabilities); logger is shared by the tool/answer bodies.
func NewRouterCognition(binder ToolBinder, logger *slog.Logger) Cognition {
	return &routerCognition{binder: binder, logger: logger}
}

// ExecuteStep routes by task.AgentType (the node's capability). Tool nodes
// tool/<name> run one CallTool; ares/answer emits the terminal result.
func (r *routerCognition) ExecuteStep(ctx context.Context, task *models.Task) (*StepOutcome, error) {
	name := string(task.AgentType)
	switch {
	case strings.HasPrefix(name, "tool/"):
		tool := strings.TrimPrefix(name, "tool/")
		if strings.TrimSpace(tool) == "" || r.binder == nil {
			return nil, fmt.Errorf("agentfabric: tool node %q has no binder", name)
		}
		return (&toolCognition{tool: tool, binder: r.binder, logger: r.logger}).ExecuteStep(ctx, task)
	case name == "ares/answer":
		return (&answerCognition{logger: r.logger}).ExecuteStep(ctx, task)
	default:
		return nil, fmt.Errorf("agentfabric: unsupported L2 capability %q", name)
	}
}

// toolCognition executes ONE tool call and completes the step in a single
// quantum (TOOL_DAG_MAINLINE_DESIGN §3.1). It is stateless — all inputs ride
// the task — so one instance can drive many tool nodes.
type toolCognition struct {
	tool   string
	binder ToolBinder
	logger *slog.Logger
}

var _ Cognition = (*toolCognition)(nil)

// ExecuteStep runs the single tool call. Args are read from the task payload
// keys (the node's Metadata) — the tool name is the node's capability.
func (c *toolCognition) ExecuteStep(ctx context.Context, task *models.Task) (*StepOutcome, error) {
	args, err := argsFromPayload(task.Payload)
	if err != nil {
		return nil, fmt.Errorf("agentfabric: tool %q args: %w", c.tool, err)
	}
	res, err := c.binder.CallTool(ctx, c.tool, args)
	if err != nil {
		return nil, fmt.Errorf("agentfabric: tool %q call: %w", c.tool, err)
	}
	result := models.NewTaskResult(task.TaskID, task.AgentType)
	result.SetSuccess([]*models.RecommendItem{{ItemID: task.TaskID, Content: stringify(res)}}, "tool "+c.tool+" completed")
	return &StepOutcome{Done: true, Result: result}, nil
}

// answerCognition produces the terminal answer for the session
// (TOOL_DAG_MAINLINE_DESIGN §3.1). In M1 it does not call the LLM — it
// assembles the terminal result from the supplied content, deferring any LLM
// summarization to the dedicated answer path in M2/M3.
type answerCognition struct {
	logger *slog.Logger
}

var _ Cognition = (*answerCognition)(nil)

// ExecuteStep completes the task with the session root's prompt as the
// answer body (M1 placeholder; M2 replaces with the LLM summary node).
func (c *answerCognition) ExecuteStep(ctx context.Context, task *models.Task) (*StepOutcome, error) {
	result := models.NewTaskResult(task.TaskID, task.AgentType)
	result.SetSuccess([]*models.RecommendItem{{ItemID: task.TaskID, Content: "L2 session complete"}}, "answer completed")
	return &StepOutcome{Done: true, Result: result}, nil
}

// metadataFromParams flattens a params map into the string-only Step.Metadata
// shape the workflow engine stores.
func metadataFromParams(params map[string]any) map[string]string {
	if len(params) == 0 {
		return nil
	}
	md := make(map[string]string, len(params))
	for k, v := range params {
		md[k] = stringify(v)
	}
	return md
}

// argsFromPayload re-extracts the tool arguments from a node's payload. The
// engine stores Metadata as strings, so a JSON-encoded value round-trips; the
// result is a fresh map the caller may mutate.
func argsFromPayload(payload map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(payload))
	if len(payload) == 0 {
		return out, nil
	}
	for k, v := range payload {
		switch vt := v.(type) {
		case string:
			// Only values that look like JSON objects are decoded; plain strings
			// (e.g. a file path) pass through as themselves.
			if len(vt) > 0 && (vt[0] == '{' || vt[0] == '[') {
				var decoded any
				if err := json.Unmarshal([]byte(vt), &decoded); err == nil {
					out[k] = decoded
					continue
				}
			}
			out[k] = vt
		default:
			out[k] = vt
		}
	}
	return out, nil
}

// stringify renders an arbitrary value as a stable string for storage in
// Step.Metadata / node outputs. JSON is used when possible so structured
// values survive a round-trip through argsFromPayload.
func stringify(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}
