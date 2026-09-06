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

// L2Graph is the per-session execution plan of one agent run: a session-local
// engine.MutableDAG whose nodes are TOOL INSTANCES — one node per actual tool
// execution — together with the session root that carries the durable,
// session-invariant prompt/params. It is an independent, testable container
// over a frozen tool DAG, and it is not yet wired into the production serve
// path — until it is, peers keep their default ReAct chatCognition and this
// graph stays test-only.
//
// The L2 graph is a first-class engine.MutableDAG so it reuses every workflow
// primitive (topological order, patch/differ, node Metadata) and so evolution
// can later constrain its growth by reading the capability graph's
// enabled/budget/prior into node Metadata. The graph holds topology +
// Metadata ONLY — it does not carry execution facts. Each non-root node maps
// 1:1 to a fabric task by ID; a node's execution result (Output) is read from
// that task's checkpoint envelope (execution facts always live in the fabric,
// never on the node).
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

// argMetadataPrefix namespaces planned tool arguments inside Step.Metadata.
// The projection merges every Metadata entry into the task payload, so an
// unprefixed arg key is indistinguishable from envelope plumbing ("input",
// the scheduler-restore "checkpoint" key) once it reaches the executing
// cognition. Only argMetadataPrefix-stripped keys are passed to CallTool;
// everything else is ignored, so envelope plumbing never reaches the tool.
const argMetadataPrefix = "arg."

// AddToolNode grows a tool-instance node into the session graph in ONE AddNode
// call, with the predecessor already in step DependsOn (the session root, or
// the last tool node in a chain).
//
// Single-call growth is load-bearing, not cosmetic: AddNode publishes exactly
// one ChangeAddNode event whose Step already carries the full dependency list,
// so the incremental compiler creates the task with its dependencies. The old
// two-step form (AddNode with empty DependsOn, then AddEdge) published a
// dependency-less node first — the compiler created a READY task for it and
// the later SetDependencies bounced off ErrTaskNotMutable, losing the edge
// forever — and AddEdge mutated the already-published *Step in place, racing
// any goroutine that received it.
//
// Args:
//   - ctx: bounds the graph mutation.
//   - id: the instance node id (unique within this session's L2 graph).
//   - tool: the tool name for a tool node; "answer" creates the terminal
//     answer node instead.
//   - args: the concrete tool arguments; written as node Metadata under the
//     argMetadataPrefix namespace so the executing cognition can read them
//     without a graph walk.
//   - dependsOn: the node this instance depends on (output feeding into it);
//     empty means no predecessor.
func (g *L2Graph) AddToolNode(ctx context.Context, id, tool string, args map[string]any, dependsOn string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	var agentType string
	if tool == "answer" {
		agentType = "ares/answer"
	} else {
		agentType = "tool/" + tool
	}
	step := &engine.Step{
		ID:        id,
		AgentType: agentType,
		Metadata:  argsMetadata(args),
	}
	if strings.TrimSpace(dependsOn) != "" {
		step.DependsOn = []string{dependsOn}
	}
	if err := g.dag.AddNode(ctx, step); err != nil {
		return fmt.Errorf("agentfabric: add tool node %q: %w", id, err)
	}
	return nil
}

// DAGExecution gates the L2 session-graph execution path. The zero value is
// the legacy ReAct behavior: the peer cognition factory returns the chat
// (tool-loop) cognition and the L2 graph machinery stays test-only. Enabled
// selects the router cognition that executes the tool/answer/root nodes grown
// on the session L2 graph. The gate defaults off so shipped behavior is
// unchanged.
type DAGExecution struct {
	// Enabled selects the L2 session-graph execution body over the ReAct loop.
	Enabled bool
}

// Select returns the production execution body for one peer agent: the router
// cognition when the gate is open, the chat (ReAct tool-loop) cognition
// otherwise. Both bodies implement Cognition, so the switch is a pure
// function of the gate — no new dispatch mechanism.
func (g DAGExecution) Select(chat, router Cognition) Cognition {
	if g.Enabled {
		return router
	}
	return chat
}

// routerCognition dispatches ONE agent's quantum to the sub-cognition named by
// the scheduled task's capability: tool/<name> → toolCognition, ares/answer →
// answerCognition, ares/root → rootCognition (session admission). It is the
// production-forward seed of the dispatch: a
// session agent declares its FULL capability set and the cognition factory
// returns ONE router that picks the body by the task's capability at execute
// time. The routing key (Task.Capability → candidate overlap →
// fabricAgentExecutor → this Cognition) is the same key the scheduler already
// resolves, so no new dispatch mechanism is introduced.
//
// Execution facts do NOT land on graph nodes: outputs live on the fabric task
// envelope. This Cognition only returns a StepOutcome; the scheduler's
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
// tool/<name> run one CallTool; ares/answer emits the terminal result;
// ares/root admits the session (zero-work, emits the session prompt).
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
	case name == "ares/root":
		return (&rootCognition{}).ExecuteStep(ctx, task)
	default:
		return nil, fmt.Errorf("agentfabric: unsupported L2 capability %q", name)
	}
}

// rootCognition admits one L2 session in a single zero-work quantum. The
// session root IS compiled as a fabric task like every other node — otherwise
// tool nodes could never resolve their DependsOn against it (CompileNode
// rejects dangling dependencies, and stripping the root edge is not an
// option: the edge pins the session order). Completing the admission emits
// the session prompt (the task payload's "input") as the root output, so the
// prompt lives in the root task's envelope — readable by the planner through
// the same ID-join used for any predecessor output — instead of only in graph
// Metadata.
type rootCognition struct{}

var _ Cognition = (*rootCognition)(nil)

// ExecuteStep completes the admission quantum with the session prompt.
func (c *rootCognition) ExecuteStep(_ context.Context, task *models.Task) (*StepOutcome, error) {
	prompt, _ := task.Payload["input"].(string)
	result := models.NewTaskResult(task.TaskID, task.AgentType)
	result.SetSuccess([]*models.RecommendItem{{ItemID: task.TaskID, Content: prompt}}, "session admitted")
	return &StepOutcome{Done: true, Result: result}, nil
}

// toolCognition executes ONE tool call and completes the step in a single
// quantum. It is stateless — all inputs ride the task — so one instance can
// drive many tool nodes.
type toolCognition struct {
	tool   string
	binder ToolBinder
	logger *slog.Logger
}

var _ Cognition = (*toolCognition)(nil)

// ExecuteStep runs the single tool call. Args are read from the task payload
// keys under the argMetadataPrefix namespace only (the node's Metadata,
// namespaced at AddToolNode time) — the tool name is the node's capability.
// Envelope plumbing ("input", scheduler-restore keys) never reaches CallTool,
// so strict-schema tools (additionalProperties:false) accept the call.
func (c *toolCognition) ExecuteStep(ctx context.Context, task *models.Task) (*StepOutcome, error) {
	res, err := c.binder.CallTool(ctx, c.tool, argsFromPayload(task.Payload))
	if err != nil {
		return nil, fmt.Errorf("agentfabric: tool %q call: %w", c.tool, err)
	}
	result := models.NewTaskResult(task.TaskID, task.AgentType)
	result.SetSuccess([]*models.RecommendItem{{ItemID: task.TaskID, Content: stringify(res)}}, "tool "+c.tool+" completed")
	return &StepOutcome{Done: true, Result: result}, nil
}

// answerContentKey is the arg a terminal answer node reads its body from,
// e.g. AddToolNode(ctx, id, "answer", map[string]any{"content": ...}, dep).
const answerContentKey = "content"

// unansweredBody is the body emitted when a terminal answer node carries no
// supplied content. It states the absence instead of reading like a result:
// nothing has summarized anything, and a success-sounding constant here would
// be a constant masquerading as logic (code_rules_v2 §0.2).
const unansweredBody = "no answer content supplied"

// answerCognition terminates the session on its terminal node. It does NOT
// summarize: it emits the content its own node carries (the answerContentKey
// arg) and says so plainly when the node carries none.
//
// TODO(tech-debt): no summarizer is wired here. Synthesizing an answer needs
// the PREDECESSORS' outputs, which live in their fabric task envelopes
// (node id = task id) — unreachable from a Cognition, whose only input is its
// own task, and reachable only by widening the Cognition interface, which the
// mainline invariant forbids. It therefore waits for the dedicated answer path
// that assembles context along the graph path and calls the LLM. Until then a
// content-less answer node reports the gap through the logger on every
// execution instead of looking successful.
type answerCognition struct {
	logger *slog.Logger
}

var _ Cognition = (*answerCognition)(nil)

// ExecuteStep completes the terminal node with the answer content supplied on
// the node, or with an explicit "no content" body plus a warning when the node
// carries none.
func (c *answerCognition) ExecuteStep(_ context.Context, task *models.Task) (*StepOutcome, error) {
	body, ok := argsFromPayload(task.Payload)[answerContentKey].(string)
	if !ok || strings.TrimSpace(body) == "" {
		body = unansweredBody
		if c.logger != nil {
			c.logger.Warn("agentfabric: answer node has no content and no summarizer is wired",
				"task_id", task.TaskID, "capability", string(task.AgentType))
		}
	}
	result := models.NewTaskResult(task.TaskID, task.AgentType)
	result.SetSuccess([]*models.RecommendItem{{ItemID: task.TaskID, Content: body}}, "answer node terminated session")
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

// argsMetadata namespaces tool arguments for storage in Step.Metadata (see
// argMetadataPrefix). A nil/empty args map yields nil Metadata, same as a
// hand-built arg-less node.
func argsMetadata(args map[string]any) map[string]string {
	if len(args) == 0 {
		return nil
	}
	md := make(map[string]string, len(args))
	for k, v := range args {
		md[argMetadataPrefix+k] = stringify(v)
	}
	return md
}

// argsFromPayload re-extracts the tool arguments from a node's payload,
// reading ONLY argMetadataPrefix-namespaced keys (prefix stripped). The
// engine stores Metadata as strings, so a JSON-encoded value round-trips; the
// result is a fresh map the caller may mutate. Unprefixed keys (projection
// "input", scheduler-restore plumbing) are not tool args and are ignored.
//
// Extraction cannot fail: a namespaced value that looks like JSON but does not
// parse is a legitimate plain string (Metadata is stringly typed), so it is
// passed through rather than rejected.
func argsFromPayload(payload map[string]any) map[string]any {
	out := make(map[string]any, len(payload))
	for k, v := range payload {
		name, ok := strings.CutPrefix(k, argMetadataPrefix)
		if !ok {
			continue
		}
		switch vt := v.(type) {
		case string:
			// Only values that look like JSON objects are decoded; plain strings
			// (e.g. a file path) pass through as themselves.
			if len(vt) > 0 && (vt[0] == '{' || vt[0] == '[') {
				var decoded any
				if err := json.Unmarshal([]byte(vt), &decoded); err == nil {
					out[name] = decoded
					continue
				}
			}
			out[name] = vt
		default:
			out[name] = vt
		}
	}
	return out
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
