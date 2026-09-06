package agentfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// maxPlanDepth is the default growth-depth upper bound for an L2 session
// graph (M2-④: 生长深度上界护栏). It caps how many plan-tool rounds a
// session can run before the planner is forced to answer — preventing
// unbounded graph growth from a runaway LLM that keeps requesting tools.
const maxPlanDepth = 10

// PlannerDeps carries the plannerCognition's dependencies, injected at
// construction time (M2-①). The planner reads predecessor outputs from the
// fabric (by node ID = task ID join), calls the LLM once, and grows tool/plan
// nodes into the L2 graph — it does NOT execute tools (the scheduler does).
type PlannerDeps struct {
	// ChatClient sends chat messages with tool support to the LLM.
	ChatClient ChatClient
	// ToolBinder advertises tool schemas to the LLM (the planner shows the
	// LLM what tools are available so it can decide which to call; execution
	// is the scheduler's job, not the planner's).
	ToolBinder ToolBinder
	// Sessions is the per-session L2 graph registry — the planner looks up
	// the graph by the task's SessionID.
	Sessions *SessionRegistry
	// Fabric is the task fabric used to read predecessor outputs by
	// node-ID = task-ID join (§2.2 decision C: node does not store Output).
	Fabric FabricReader
	// L1DAG is the L1 ToolClass capability graph (M5). The planner reads
	// each tool's enabled/budget/prior metadata before growing an L2 tool
	// node — enabled=false blocks growth, budget=N caps instances per
	// session. Nil = no L1 graph (constraints default to permissive).
	L1DAG *engine.MutableDAG
	// MaxDepth caps the plan-tool growth depth (0 = default maxPlanDepth).
	MaxDepth int
	// Logger is the shared logger.
	Logger *slog.Logger
}

// FabricReader is the minimal fabric surface the planner needs: read a task's
// checkpoint to extract its predecessor's output. Interface at the consumer
// (code_rules §5.2) so the planner stays decoupled from the fabric package.
type FabricReader interface {
	// Task returns a snapshot of the task (ErrTaskNotFound when unknown).
	Task(id string) (*taskfabric.Task, error)
}

// plannerCognition grows the L2 session graph by one plan-tool round per
// quantum (M2-①). It is the replacement for the ReAct loop's "chatStep":
// instead of executing tools inside a cognition, it grows tool nodes into
// the L2 graph and lets the scheduler execute them as first-class tasks.
//
// Lifecycle per quantum:
//  1. Read SessionID from the task → look up the L2Graph from the registry.
//  2. Read the predecessor's output from the fabric envelope (by node ID =
//     task ID join) to assemble the LLM context.
//  3. Call the LLM once with the available tool schemas.
//  4. If the LLM returns tool calls: AddNode each one to the L2 graph
//     (the incremental compiler projects them to fabric tasks), then grow
//     a new plan node depending on them. Done:true — the next plan quantum
//     resumes when the scheduler drains the new tasks.
//  5. If the LLM returns no tool calls: grow an answer node carrying the
//     final response. Done:true — the session terminates.
//
// The planner NEVER executes tools (§2.1: "plannerCognition 自己不执行工具，
// 只生长图"). Execution is the scheduler's job — tool nodes are dispatched
// to toolCognition through the same fabric → scheduler → routerCognition
// path as every other node.
type plannerCognition struct {
	chat     ChatClient
	binder   ToolBinder
	sessions *SessionRegistry
	fabric   FabricReader
	l1       *engine.MutableDAG // L1 ToolClass graph (M5), nil = permissive
	maxDepth int
	logger   *slog.Logger
}

var _ Cognition = (*plannerCognition)(nil)

// NewPlannerCognition constructs the L2 graph-growing Cognition (M2-①).
// A nil deps.ChatClient or nil deps.Sessions is a construction error: the
// planner cannot grow without an LLM to decide what to grow, and cannot
// find its graph without a session registry.
func NewPlannerCognition(deps PlannerDeps) (Cognition, error) {
	if deps.ChatClient == nil {
		return nil, fmt.Errorf("agentfabric: planner cognition requires ChatClient")
	}
	if deps.Sessions == nil {
		return nil, fmt.Errorf("agentfabric: planner cognition requires SessionRegistry")
	}
	if deps.Fabric == nil {
		return nil, fmt.Errorf("agentfabric: planner cognition requires FabricReader")
	}
	maxD := deps.MaxDepth
	if maxD <= 0 {
		maxD = maxPlanDepth
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &plannerCognition{
		chat:     deps.ChatClient,
		binder:   deps.ToolBinder,
		sessions: deps.Sessions,
		fabric:   deps.Fabric,
		l1:       deps.L1DAG,
		maxDepth: maxD,
		logger:   logger,
	}, nil
}

// planMetadataKey is the Metadata key the planner stamps onto every grown
// node so the node carries its session scope (ProjectStep extracts it into
// PlanStep.SessionID → envelope). This is how SessionID propagates from
// the session root to every tool node the planner grows. It is the reserved
// unprefixed Metadata key (see sessionMetadataKey in l2graph.go), so it lands
// on the envelope instead of in the tool-argument namespace.
const planMetadataKey = sessionMetadataKey

// roleTool is the LLM message role for tool outputs. Extracted as a
// constant because it appears 3+ times across the agentfabric package
// (planner_cognition.go + chat_cognition.go).
const roleTool = "tool"

// L1 ToolClass metadata keys (M5). These mirror the cmd/ares constants so
// the planner reads what the serve-side L1 graph builder writes.
const (
	l1MetaEnabled = "enabled"
	l1MetaBudget  = "budget"
)

// l1ToolClassID builds the L1 ToolClass node ID for a tool: toolName + "#" +
// argShape, where argShape comes from the tool's DECLARED schema via the
// shared resources.ToolArgShape.
//
// Deriving the shape from the schema — not from one call's arguments — is what
// makes this match the L1 graph builder's IDs. A call that omits an optional
// parameter carries a smaller key set than the declaration, so an
// args-derived shape would miss the L1 node and silently fall through to the
// permissive default, ignoring enabled=false.
//
// Returns "" when the tool has no schema: an unknown tool has no ToolClass to
// constrain, and the caller treats a lookup miss as permissive.
func (c *plannerCognition) l1ToolClassID(toolName string) string {
	if c.binder == nil {
		return ""
	}
	for _, s := range c.binder.GetToolSchemas() {
		if s.Name == toolName {
			return resources.ToolClassID(toolName, resources.ToolArgShape(s))
		}
	}
	return ""
}

// ExecuteStep runs one plan-tool growth quantum: read predecessor output →
// call LLM → grow tool/answer nodes into the L2 graph. The task's
// SessionID selects the graph; the task's TaskID is the plan node that
// triggered this quantum (its predecessor chain defines the context path).
//
// The first plan quantum references a task ID that is NOT a graph node —
// only the root exists at session start. The planner uses the root (or the
// last tool node on the predecessor chain) as the predecessor for the tool
// nodes it grows. Subsequent plan quanta reference plan nodes that were
// grown by the previous quantum's growToolNodes, so they ARE in the graph.
func (c *plannerCognition) ExecuteStep(ctx context.Context, task *models.Task) (*StepOutcome, error) {
	if task == nil {
		return nil, fmt.Errorf("agentfabric: planner cognition: nil task")
	}

	sessionID := task.SessionID
	if sessionID == "" {
		return nil, fmt.Errorf("agentfabric: planner cognition: task %q has no session id", task.TaskID)
	}

	g, err := c.sessions.GetSession(sessionID)
	if err != nil {
		return nil, fmt.Errorf("agentfabric: planner cognition: %w", err)
	}

	// Determine the current plan-tool growth depth and check the guard
	// (M2-④: 生长深度上界护栏). PlanDepth counts plan nodes grown by
	// growToolNodes — the initial plan quantum has depth 0.
	depth := g.PlanDepth()
	if depth >= c.maxDepth {
		// Growth-depth upper bound reached: force an answer node so the
		// session terminates instead of growing unbounded.
		c.logger.Warn("planner: max plan depth reached, forcing answer",
			"session", sessionID, "depth", depth, "max", c.maxDepth)
		return c.growAnswerNode(ctx, g, task, "max plan depth reached")
	}

	// Assemble the LLM context from the predecessor path.
	prompt, err := c.assembleContext(ctx, task, g)
	if err != nil {
		return nil, fmt.Errorf("agentfabric: planner cognition: assemble context: %w", err)
	}

	// Build the tool schemas for the LLM.
	var llmTools []core.Tool
	if c.binder != nil {
		schemas := c.binder.GetToolSchemas()
		llmTools = make([]core.Tool, 0, len(schemas))
		for _, s := range schemas {
			llmTools = append(llmTools, toCoreTool(s))
		}
	}

	// Call the LLM once with the context and tool schemas.
	resp, err := c.chat.Chat(ctx, prompt, llmTools, nil)
	if err != nil {
		return nil, fmt.Errorf("agentfabric: planner cognition: chat: %w", err)
	}

	// No tool calls: the LLM gave a final answer. Grow an answer node.
	if len(resp.ToolCalls) == 0 {
		return c.growAnswerNode(ctx, g, task, resp.Content)
	}

	// Tool calls: grow tool nodes + a new plan node depending on them.
	grown, err := c.growToolNodes(ctx, g, task, resp.ToolCalls, sessionID)
	if err != nil {
		return nil, fmt.Errorf("agentfabric: planner cognition: grow nodes: %w", err)
	}
	if grown == 0 {
		// All tool calls were skipped by L1 constraints (enabled=false
		// or budget exhausted). Force an answer so the session terminates
		// instead of looping on a planner that can never grow a tool.
		return c.growAnswerNode(ctx, g, task, resp.Content)
	}

	result := models.NewTaskResult(task.TaskID, task.AgentType)
	result.SetSuccess(nil, "planner grew "+strconv.Itoa(grown)+" tool nodes")
	return &StepOutcome{Done: true, Result: result}, nil
}

// assembleContext builds the LLM message list from the predecessor path:
// the session prompt (root output) + the outputs of every tool node on the
// path from root to this plan node. Outputs are read from the fabric task
// envelopes by node ID = task ID join (§2.2 decision C).
func (c *plannerCognition) assembleContext(ctx context.Context, task *models.Task, g *L2Graph) ([]*core.LLMMessage, error) {
	// Read the session prompt from the root task's envelope.
	rootID := g.Root()
	rootPrompt, err := c.readNodeOutput(rootID)
	if err != nil {
		// The root may not have completed yet if this is the first plan
		// quantum; fall back to the payload's "input".
		rootPrompt, _ = task.Payload["input"].(string)
	}

	messages := []*core.LLMMessage{
		{Role: "user", Content: rootPrompt},
	}

	// Walk the predecessor chain from this plan node back to the root. The
	// walk is newest-first, so the collected outputs are REVERSED before
	// being appended: the LLM must observe tool results in execution order,
	// the same order ReAct's Messages[] presented them. Appending in walk
	// order would invert the history and change what the model concludes.
	var observed []string
	predID := g.Predecessor(task.TaskID)
	for predID != "" && predID != rootID {
		output, err := c.readNodeOutput(predID)
		switch {
		case err != nil:
			// A predecessor whose task is gone is a HOLE in the history, not
			// an empty step: the envelope is the only place output lives
			// (decision C), so losing it silently would let the model plan on
			// a truncated past. Surface it and keep walking.
			c.logger.Warn("planner: predecessor output unreadable; context has a hole",
				"node", predID, "error", err)
		case output != "":
			observed = append(observed, output)
		}
		predID = g.Predecessor(predID)
	}
	for i := len(observed) - 1; i >= 0; i-- {
		messages = append(messages, &core.LLMMessage{Role: roleTool, Content: observed[i]})
	}

	return messages, nil
}

// readNodeOutput reads one node's execution output from its fabric task
// envelope by node ID = task ID join. Returns "" when the task is not
// found or has no output yet (e.g. not yet completed).
func (c *plannerCognition) readNodeOutput(nodeID string) (string, error) {
	tk, err := c.fabric.Task(nodeID)
	if err != nil {
		return "", err
	}
	if tk.State != taskfabric.StateCompleted {
		return "", nil
	}
	dc, err := taskfabric.DecodeCheckpoint(tk.Checkpoint)
	if err != nil {
		return "", err
	}
	return extractOutputContent(dc.StepCheckpoint), nil
}

// growToolNodes grows one tool node per LLM tool call, then grows a new plan
// node depending on all of them. Each tool node carries its args in the
// arg. namespace (D3) and the session_id in Metadata (so ProjectStep
// propagates it to the envelope).
//
// The predecessor for the first tool node is the current plan node when it
// exists in the graph (subsequent quanta), or the root node when it doesn't
// (the first plan quantum — the initial plan task ID is not a graph node,
// only the root exists at session start). Tools within one round are chained
// sequentially (the next depends on the previous).
//
// M5: before growing each tool node, the planner checks the L1 ToolClass
// graph's enabled/budget metadata. enabled=false skips the node (§6
// constraint point: "节点长不长出来"). budget=N caps how many instances
// of this ToolClass can exist in the L2 graph per session. A nil L1 graph
// means no constraints (permissive default).
//
// Returns the number of tool nodes actually grown (0 when all calls were
// skipped by L1 constraints, in which case the caller forces an answer node).
func (c *plannerCognition) growToolNodes(
	ctx context.Context,
	g *L2Graph,
	task *models.Task,
	toolCalls []core.ToolCall,
	sessionID string,
) (int, error) {
	depth := g.PlanDepth()
	grown := 0

	// Determine the predecessor for the first tool node: the current plan
	// node when it exists, otherwise the root (first plan quantum).
	prev := task.TaskID
	if !g.HasNode(prev) {
		prev = g.Root()
	}

	for seq, tc := range toolCalls {
		toolName := tc.Function.Name

		// M5: check L1 constraints before growing the node.
		if !c.isToolEnabled(toolName) {
			c.logger.Warn("planner: tool disabled by L1 constraint; skipping",
				"tool", toolName, "session", sessionID)
			continue
		}
		if !c.toolBudgetRemaining(g, toolName) {
			c.logger.Warn("planner: tool budget exhausted by L1 constraint; skipping",
				"tool", toolName, "session", sessionID)
			continue
		}

		nodeID := SessionNodeID(sessionID, depth+1, toolName, seq)

		// Parse the tool arguments.
		args := map[string]any{}
		if tc.Function.Arguments != "" {
			if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
				return grown, fmt.Errorf("parse tool %s args: %w", toolName, err)
			}
		}

		// Stamp session_id into Metadata so ProjectStep propagates it.
		metadata := args
		if metadata == nil {
			metadata = map[string]any{}
		}
		metadata[planMetadataKey] = sessionID

		if err := g.AddToolNode(ctx, nodeID, toolName, metadata, prev); err != nil {
			return grown, fmt.Errorf("add tool node %s: %w", nodeID, err)
		}
		// Chain sequential tools within the same round: the next tool
		// depends on this one (data flow).
		prev = nodeID
		grown++
	}

	// When every tool call was skipped by L1 constraints, do NOT grow a new
	// plan node — the caller forces an answer node instead. Growing a
	// plan node here would inflate PlanDepth and shift the answer node's
	// ID, breaking the "no tools grown → answer at this depth" contract.
	if grown == 0 {
		return grown, nil
	}

	// Grow a new plan node depending on the last tool node. The next plan
	// quantum reads the tool outputs and decides the next round.
	newPlanID := SessionNodeID(sessionID, depth+1, "plan", 0)
	planArgs := map[string]any{planMetadataKey: sessionID}
	if err := g.AddToolNode(ctx, newPlanID, "plan", planArgs, prev); err != nil {
		return grown, fmt.Errorf("add plan node %s: %w", newPlanID, err)
	}
	return grown, nil
}

// isToolEnabled reads the L1 ToolClass graph's "enabled" metadata for the
// given tool. Returns true (permissive) when the L1 graph is nil, the node
// is not found, or the metadata is missing/empty. Returns false only when
// the L1 node explicitly sets enabled="false".
func (c *plannerCognition) isToolEnabled(toolName string) bool {
	if c.l1 == nil {
		return true
	}
	nodeID := c.l1ToolClassID(toolName)
	steps := c.l1.StepIndex()
	step, ok := steps[nodeID]
	if !ok {
		return true // unknown ToolClass → permissive
	}
	val, ok := step.Metadata[l1MetaEnabled]
	if !ok || val == "" {
		return true
	}
	return val != "false"
}

// toolBudgetRemaining checks whether the L2 graph still has budget for one
// more instance of the given ToolClass. budget=0 (or missing) = unlimited.
// budget=N means at most N instances of this ToolClass can exist in the L2
// graph at a time. The count is by ToolClass (toolName#argShape), not by
// tool name alone, so two different argument shapes of the same tool each
// get their own budget.
func (c *plannerCognition) toolBudgetRemaining(g *L2Graph, toolName string) bool {
	if c.l1 == nil {
		return true
	}
	nodeID := c.l1ToolClassID(toolName)
	steps := c.l1.StepIndex()
	step, ok := steps[nodeID]
	if !ok {
		return true // unknown ToolClass → permissive
	}
	budgetStr, ok := step.Metadata[l1MetaBudget]
	if !ok || budgetStr == "" || budgetStr == "0" {
		return true // unlimited
	}
	budget, err := strconv.Atoi(budgetStr)
	if err != nil || budget <= 0 {
		return true // unparseable or unlimited
	}
	count := g.CountToolClass(toolName)
	return count < budget
}

// growAnswerNode grows a terminal answer node carrying the final response and
// completes the quantum with Done:true. The answer node's content is read by
// answerCognition when the scheduler executes it.
//
// The predecessor is the current plan node when it exists in the graph
// (subsequent quanta), or the root node when it doesn't (the first plan
// quantum). When the plan node IS in the graph, its predecessor (the last
// tool node) is the natural dependency for the answer.
func (c *plannerCognition) growAnswerNode(
	ctx context.Context,
	g *L2Graph,
	task *models.Task,
	content string,
) (*StepOutcome, error) {
	depth := g.PlanDepth()
	answerID := SessionNodeID(task.SessionID, depth+1, "answer", 0)

	// Determine predecessor: the current plan node when in graph, else root.
	pred := task.TaskID
	if !g.HasNode(pred) {
		pred = g.Root()
	}

	args := map[string]any{
		"content":       content,
		planMetadataKey: task.SessionID,
	}
	if err := g.AddToolNode(ctx, answerID, "answer", args, pred); err != nil {
		return nil, fmt.Errorf("agentfabric: planner cognition: add answer node: %w", err)
	}

	result := models.NewTaskResult(task.TaskID, task.AgentType)
	result.SetSuccess(nil, "planner grew answer node")
	return &StepOutcome{Done: true, Result: result}, nil
}

// extractOutputContent reads the textual output from a step checkpoint. The
// scheduler's buildQuantumStep wraps the StepOutcome.Result into the
// checkpoint's StepCheckpoint as a {result, items, ...} map; this helper
// extracts the first item's Content field. Returns "" when the checkpoint
// does not carry a recognizable output shape.
func extractOutputContent(sc any) string {
	if sc == nil {
		return ""
	}
	m, ok := sc.(map[string]any)
	if !ok {
		return ""
	}
	// The scheduler stores {result: "ok", items: []*RecommendItem, ...} or
	// after a JSON round-trip {items: [{content: "...", ...}]}.
	raw, ok := m["items"]
	if !ok {
		return ""
	}
	switch items := raw.(type) {
	case []*models.RecommendItem:
		if len(items) > 0 {
			return items[0].Content
		}
	case []any:
		if len(items) > 0 {
			if im, ok := items[0].(map[string]any); ok {
				if c, ok := im["content"].(string); ok {
					return c
				}
			}
		}
	}
	return ""
}

// toCoreTool converts a resources.ToolSchema to a core.Tool for the LLM.
// This is the same conversion the chatCognition uses.
func toCoreTool(s resources.ToolSchema) core.Tool {
	return resources.ToolSchemaToLLMTool(s)
}
