package agentfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/agents"
	"github.com/Timwood0x10/ares/internal/ares_callbacks"
	"github.com/Timwood0x10/ares/internal/ares_events"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/errors"
	kctx "github.com/Timwood0x10/ares/internal/kernel/ctx"
	"github.com/Timwood0x10/ares/internal/llm/output"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
)

// This file is the A1.4 delivery (aresos-agentos-plan §3 A1.4): the sub-agent
// tool-loop execution logic (chatStep / decodeChatStepState / renderPrompt)
// is MOVED down into agentfabric as the DEFAULT Cognition implementation —
// not re-written, ported and stripped of the leader dependency. A fabric
// agent spawned with a ChatCognition is a fully self-contained cognitive
// process: it holds its LLM client and tool binder directly, no sub.Agent
// wrapper. (code_rules: in the peer mode this is the single
// production execution path; the legacy leader runtime was removed in
// v0.3.1/C1.)

// Event payload keys shared with the ares_events pipeline. They mirror the
// sub executor's key names so both execution paths emit identical events.
const (
	KeyAgentID = "agent_id"
	KeyError   = "error"
	KeyStatus  = "status"
)

// defaultMaxToolRounds is the tool-loop round budget per execution when the
// cognition is constructed without an explicit cap (matches the sub
// executor's default).
const defaultMaxToolRounds = 5

// stepSchemaVersion is the chatStepState schema version (code_rules:
// snapshot structs carry a version; decodeChatStepState rejects unknown ones).
const stepSchemaVersion = 1

// ChatClient sends chat messages with tool support to the LLM (interface at
// the consumer, code_rules). It is the minimal surface the tool-loop
// needs; *llm.FailoverClient and the sub executor's ChatClient both satisfy
// it.
type ChatClient interface {
	Chat(ctx context.Context, messages []*core.LLMMessage, tools []core.Tool, params map[string]any) (*core.GenerateResponse, error)
}

// ToolBinder is the minimal tool execution surface the tool-loop needs
// (interface at the consumer, code_rules). It mirrors the sub
// executor's binding contract so the same binder wires both paths.
type ToolBinder interface {
	CallTool(ctx context.Context, name string, args map[string]any) (any, error)
	ListTools() []string
	IsToolIdempotent(name string) bool
	GetToolSchemas() []resources.ToolSchema
}

// chatStepState is the resumable program-counter block (PCB) of one
// tool-calling execution (plan P1.1 Execution Quantum: reason → tool call →
// observation → checkpoint). A quantum (ExecuteStep) advances it by exactly
// one ReAct round; when the LLM answers without tool calls it carries the
// final answer to completion. It is JSON round-trippable so it can ride the
// fabric's opaque checkpoint slot across a yield→resume cycle, and the resume
// path always decodes into a fresh copy so a stored checkpoint is never
// mutated in place across quanta (code_rules).
//
// TaskID is the resume identity check (§6.2): a checkpoint that does not
// belong to the task being resumed is refused instead of executed.
type chatStepState struct {
	SchemaVersion int                `json:"schema_version"`
	TaskID        string             `json:"task_id"`
	Round         int                `json:"round"`
	MaxRounds     int                `json:"max_rounds"`
	Prompt        string             `json:"prompt"`
	Params        map[string]any     `json:"params,omitempty"`
	Messages      []*core.LLMMessage `json:"messages"`
	// ToolUses counts how many times each tool has been called in this
	// session. It rides the checkpoint so a resumed task does not reset the
	// node's budget (C5): without persistence, every yield would hand the
	// tool a fresh budget and the cap would never bind. Optional field —
	// a pre-budget checkpoint decodes to nil, which reads as "no uses yet".
	ToolUses map[string]int `json:"tool_uses,omitempty"`
}

// ChatCognitionDeps carries the tool-loop's dependencies. It is the
// constructor argument so a SpawnSpec's CognitionFactory can capture the
// runtime wiring (LLM client, tool binder, prompt template) exactly once.
type ChatCognitionDeps struct {
	// ChatClient enables native tool calling via the Chat API. When nil (and
	// LLMAdapter is set) the cognition degrades to text-only generation.
	ChatClient ChatClient
	// LLMAdapter is the text-only generation path, used when the chat client
	// is unavailable or the tool-loop budget is exhausted. When both are nil
	// the cognition cannot execute (construction error, surfaced at
	// ExecuteStep).
	LLMAdapter output.LLMAdapter
	// ToolBinder executes tool calls and advertises tool schemas to the LLM.
	ToolBinder ToolBinder
	// StrategySource is the optional live evolution strategy (prompt + LLM
	// param overrides). Nil = no strategy.
	StrategySource agents.StrategySource
	// Template renders the worker prompt template.
	Template *output.TemplateEngine
	// PromptTemplate is the worker prompt template (e.g.
	// cfg.Prompts.Recommendation).
	PromptTemplate string
	// MaxToolRounds caps the tool-loop rounds (0 = default 5).
	MaxToolRounds int
	// EventStore emits ares_events for tool/LLM calls (may be nil).
	EventStore ares_events.EventStore
	// Callbacks emits lifecycle callback events (may be nil).
	Callbacks ares_callbacks.Emitter
	// AgentID is the identity used for event emission.
	AgentID string
	// Profile is the construction-time agent role (W4). When set, its
	// Instructions are prepended to the worker prompt as the default role
	// instructions. It is the fallback for a peer pinned to a role via config
	// (createPeerSubAgents / newPeerChatCognition); an explicit Handoff switch
	// (a role carved into the execution context, P0-3) still takes precedence.
	// Nil = no role instructions (the roleless peer).
	Profile *agents.AgentProfile
}

// chatCognition is the A1.4 default Cognition: the tool-loop execution body
// moved down from the sub executor. It is stateless between quanta — the
// resumable chatStepState rides in the task's payload checkpoint, exactly as
// the sub executor's quantum path — so one instance can drive many tasks
// concurrently.
type chatCognition struct {
	chatClient     ChatClient
	llmAdapter     output.LLMAdapter
	toolBinder     ToolBinder
	strategySource agents.StrategySource
	template       *output.TemplateEngine
	promptTpl      string
	maxToolRounds  int
	eventStore     ares_events.EventStore
	callbacks      ares_callbacks.Emitter
	agentID        string
	profile        *agents.AgentProfile
	logger         *slog.Logger
}

var _ Cognition = (*chatCognition)(nil)

// NewChatCognition constructs the default tool-loop Cognition (A1.4).
// A nil deps is rejected so a mis-wired spawn fails loudly instead of
// producing a phantom execution body (code_rules: no silent no-op).
func NewChatCognition(deps ChatCognitionDeps) (Cognition, error) {
	if deps.ChatClient == nil && deps.LLMAdapter == nil {
		return nil, errors.New("agentfabric: chat cognition requires ChatClient or LLMAdapter")
	}
	maxRounds := deps.MaxToolRounds
	if maxRounds <= 0 {
		maxRounds = defaultMaxToolRounds
	}
	if deps.Template == nil {
		deps.Template = output.NewTemplateEngine()
	}
	return &chatCognition{
		chatClient:     deps.ChatClient,
		llmAdapter:     deps.LLMAdapter,
		toolBinder:     deps.ToolBinder,
		strategySource: deps.StrategySource,
		template:       deps.Template,
		promptTpl:      deps.PromptTemplate,
		maxToolRounds:  maxRounds,
		eventStore:     deps.EventStore,
		callbacks:      deps.Callbacks,
		agentID:        deps.AgentID,
		profile:        deps.Profile,
		logger:         slog.Default(),
	}, nil
}

// ExecuteStep runs exactly one execution quantum (P1.1) of the tool-loop and
// returns its outcome — the same semantics as the sub executor's quantum
// path. The resumable state (chatStepState) rides in
// task.Payload["checkpoint"]: the scheduler stores it in the fabric's opaque
// checkpoint slot on yield and re-surfaces it here on resume.
func (c *chatCognition) ExecuteStep(ctx context.Context, task *models.Task) (*StepOutcome, error) {
	if task == nil {
		return nil, errors.ErrInvalidInput
	}
	result := models.NewTaskResult(task.TaskID, task.AgentType)
	start := time.Now()

	// Decode the resume checkpoint first: its presence marks a resumed
	// quantum, and the tool-start lifecycle event must fire only on the first
	// one (end/error fire on the final quantum — the same shape as Execute).
	st, found, err := c.decodeChatStepState(task)
	if err != nil {
		return nil, err
	}
	if !found {
		c.emitCallback(&ares_callbacks.Context{
			Event:   ares_callbacks.EventToolStart,
			AgentID: c.agentID,
			Input:   task.TaskID,
		})
	}

	chatAvailable := c.chatClient != nil && c.toolBinder != nil && len(c.toolBinder.GetToolSchemas()) > 0
	if st == nil {
		prompt, params, err := c.renderPromptAndParams(ctx, task)
		if err != nil {
			return nil, err
		}
		if !chatAvailable {
			// Text-only: a single LLM call completes the task in one quantum.
			items, err := c.executeWithLLMTextOnly(ctx, prompt, params)
			if err != nil {
				return nil, err
			}
			result.SetSuccess(items, "LLM recommendation completed")
			result.Duration = time.Since(start)
			c.emitCallback(&ares_callbacks.Context{Event: ares_callbacks.EventToolEnd, AgentID: c.agentID, Duration: time.Since(start)})
			return &StepOutcome{Done: true, Result: result}, nil
		}
		st = &chatStepState{
			SchemaVersion: stepSchemaVersion,
			TaskID:        task.TaskID,
			MaxRounds:     c.maxToolRounds,
			Prompt:        prompt,
			Params:        params,
			Messages:      []*core.LLMMessage{{Role: "user", Content: prompt}},
		}
	} else if !chatAvailable {
		// A resumed checkpoint needs the same executor shape that produced it.
		// Missing chat/tools wiring mid-task is a config error, not a retryable
		// LLM failure — surface it instead of silently restarting.
		return nil, fmt.Errorf("agentfabric: resumed step requires chat executor (chat_client=%v tool_binder=%v)", c.chatClient != nil, c.toolBinder != nil)
	}

	// One ReAct round — or a text-only degradation when the round budget is
	// already spent (the previous quantum ran the last tool round).
	var items []*models.RecommendItem
	var done bool
	if st.Round >= st.MaxRounds {
		c.logger.Warn("Chat API tool loop exceeded max rounds, degrading to text-only",
			"max_rounds", st.MaxRounds,
			"msg_count", len(st.Messages),
		)
		items, err = c.executeWithLLMTextOnly(ctx, st.Prompt, st.Params)
		done = true
	} else {
		items, done, err = c.chatStep(ctx, st)
	}
	if err != nil {
		return nil, err
	}

	if !done {
		// Yield (P1.1): progress was made but the task is not complete. The
		// fabric SUSPENDEDs the task with this checkpoint preserved; a later
		// quantum resumes from it.
		return &StepOutcome{Checkpoint: st}, nil
	}
	result.SetSuccess(items, "LLM recommendation completed")
	result.Duration = time.Since(start)
	c.emitCallback(&ares_callbacks.Context{Event: ares_callbacks.EventToolEnd, AgentID: c.agentID, Duration: time.Since(start)})
	return &StepOutcome{Done: true, Result: result}, nil
}

// decodeChatStepState restores a resumable chatStepState from a task's
// payload["checkpoint"] entry. It always decodes into a fresh copy via JSON
// so the fabric's stored checkpoint is never mutated across quanta
// (code_rules). Resume is refused when the checkpoint belongs to
// another task or carries an unknown schema version (§6.1).
func (c *chatCognition) decodeChatStepState(task *models.Task) (*chatStepState, bool, error) {
	if task == nil || task.Payload == nil {
		return nil, false, nil
	}
	raw, ok := task.Payload["checkpoint"]
	if !ok || raw == nil {
		return nil, false, nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, false, errors.Wrap(err, "marshal step checkpoint")
	}
	var st chatStepState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, false, errors.Wrap(err, "unmarshal step checkpoint")
	}
	if st.SchemaVersion != stepSchemaVersion {
		return nil, false, fmt.Errorf("agentfabric: step checkpoint schema version %d unsupported (want %d)", st.SchemaVersion, stepSchemaVersion)
	}
	if st.TaskID != task.TaskID {
		return nil, false, fmt.Errorf("agentfabric: step checkpoint for task %q does not match task %q, refusing resume", st.TaskID, task.TaskID)
	}
	return &st, true, nil
}

// chatStep advances a tool-calling execution by exactly one ReAct round: one
// Chat API call, every requested tool call executed, and the resulting
// messages appended to the state. It is the single loop body of the cognition.
//
// Contract: the caller has verified st.Round < st.MaxRounds.
func (c *chatCognition) chatStep(ctx context.Context, st *chatStepState) ([]*models.RecommendItem, bool, error) {
	schemas := c.toolBinder.GetToolSchemas()

	// Y.3-ACT: filter the tool schemas by the active strategy's whitelist
	// (Params["tools"]) before converting them to LLM tools. An empty or
	// missing whitelist means "all tools" (zero-value usable). Filtering at
	// the schema layer — not at CallTool time — ensures the LLM never sees a
	// tool it should not call, avoiding wasted rounds and not_found pollution.
	whitelist := agents.ToolWhitelistFromParams(st.Params)
	if whitelist != nil {
		filtered := make([]resources.ToolSchema, 0, len(schemas))
		for _, s := range schemas {
			if whitelist[s.Name] {
				filtered = append(filtered, s)
			}
		}
		// Guard: a whitelist with ZERO intersection with the registered tools
		// (e.g. a mutated Params["tools"] naming a tool that does not exist)
		// must not leave the LLM with an empty tool list. Fall back to the full
		// set rather than degrade to zero tools — an empty tool list is a
		// functional dead-end, while ignoring a bad whitelist keeps the agent
		// usable and is observable via the remainder=full behavior.
		if len(filtered) == 0 {
			c.logger.Warn("tool whitelist matched no registered tools; falling back to full set",
				"whitelist", whitelist, "registered", len(schemas))
			filtered = schemas
		}
		schemas = filtered
	}

	// C5 budget gate: drop tools whose per-session call budget is spent, at the
	// same schema layer as the whitelist. budget<=0 means unlimited, so the
	// non-evolved path is untouched. Same zero-result fallback as the whitelist:
	// a budget that would exhaust EVERY tool must not hand the LLM an empty tool
	// list — that dead-end is worse than an over-run budget, and the fallback is
	// observable in the log.
	if budget := agents.ToolBudgetFromParams(st.Params); budget > 0 {
		allowed := make([]resources.ToolSchema, 0, len(schemas))
		for _, s := range schemas {
			if agents.ToolAllowedByBudget(s.Name, st.ToolUses, budget) {
				allowed = append(allowed, s)
			}
		}
		if len(allowed) == 0 {
			c.logger.Warn("tool budget exhausted for every advertised tool; falling back to full set",
				"budget", budget, "uses", st.ToolUses, "advertised", len(schemas))
		} else {
			schemas = allowed
		}
	}

	llmTools := make([]core.Tool, 0, len(schemas))
	for _, s := range schemas {
		llmTools = append(llmTools, resources.ToolSchemaToLLMTool(s))
	}

	c.emitEvent(ctx, ares_events.EventLLMCall, map[string]any{
		KeyAgentID:   c.agentID,
		"round":      st.Round + 1,
		"max_rounds": st.MaxRounds,
		"tool_count": len(llmTools),
		"msg_count":  len(st.Messages),
	})

	resp, err := c.chatClient.Chat(ctx, st.Messages, llmTools, st.Params)
	if err != nil {
		return nil, false, errors.Wrap(err, "chat API call failed")
	}

	// No tool calls: LLM gave a final text answer.
	if len(resp.ToolCalls) == 0 {
		c.logger.Debug("Chat API returned final text", "round", st.Round+1, "content_len", len(resp.Content))
		items, err := c.parseRecommendResult(resp.Content)
		return items, true, err
	}

	// Append the assistant message with its tool calls, then execute each call
	// and append its observation as a tool message — the conversation grows
	// exactly as the pre-quantum implementation did, so a resumed round sees
	// the accumulated context.
	c.logger.Debug("Chat API returned tool calls", "round", st.Round+1, "count", len(resp.ToolCalls))
	st.Messages = append(st.Messages, &core.LLMMessage{
		Role:      "assistant",
		Content:   resp.Content,
		ToolCalls: resp.ToolCalls,
	})
	for seq, tc := range resp.ToolCalls {
		// C5 intra-round enforcement: the schema gate above runs once per
		// round, but a single round can carry N calls to the same tool. A
		// call already over budget is skipped — never executed, never
		// counted — or one round could overshoot the cap by up to
		// len(ToolCalls)-1. The skip is reported as the tool observation so
		// the assistant message keeps its paired tool reply; no tool events
		// are emitted because the call never ran (there is no
		// success/failure signal to project or score). Deliberately NOT a
		// CallTool-time rejection (cf. ToolAllowedByBudget): the model must
		// not be offered what it may not spend.
		if budget := agents.ToolBudgetFromParams(st.Params); budget > 0 &&
			!agents.ToolAllowedByBudget(tc.Function.Name, st.ToolUses, budget) {
			st.Messages = append(st.Messages, &core.LLMMessage{
				Role:       roleTool,
				Content:    fmt.Sprintf("tool %s skipped: per-session budget (%d) exhausted", tc.Function.Name, budget),
				ToolCallID: tc.ID,
			})
			continue
		}
		// C5: count the call against the node budget BEFORE executing, so a
		// failing tool still spends its budget — otherwise a tool that always
		// errors would be retried without limit and the cap would not bind.
		if st.ToolUses == nil {
			st.ToolUses = map[string]int{}
		}
		st.ToolUses[tc.Function.Name]++

		c.emitEvent(ctx, ares_events.EventToolCallStarted, map[string]any{
			ares_events.EventKeyAgentID:    c.agentID,
			ares_events.EventKeyToolName:   tc.Function.Name,
			ares_events.EventKeyToolCallID: tc.ID,
			ares_events.EventKeyRound:      st.Round,
			ares_events.EventKeySeq:        seq,
		})

		result, err := c.executeToolCall(ctx, tc)
		success := err == nil
		if err != nil {
			c.logger.Warn("tool execution failed", "tool", tc.Function.Name, "error", err)
			result = fmt.Sprintf("error: %s", err.Error())
		}

		// C1: unified tool-call completed contract (round/seq/success/error/
		// arg_shape). success/error were previously dropped by this executor —
		// the fire-and-forget emit wrote only identity keys, so the trajectory
		// projection (Y1 C2) could not tell a failed tool step from a healthy
		// one.
		c.emitEvent(ctx, ares_events.EventToolCallCompleted, ares_events.ToolCompletedPayload{
			AgentID:     c.agentID,
			ToolName:    tc.Function.Name,
			ToolCallID:  tc.ID,
			Round:       st.Round,
			Seq:         seq,
			Success:     success,
			Error:       toolErrorMessage(err),
			ArgShape:    ares_events.ToolArgShape(tc.Function.Arguments),
			ExtraResult: result,
		}.AsMap())
		st.Messages = append(st.Messages, &core.LLMMessage{
			Role:       roleTool,
			Content:    result,
			ToolCallID: tc.ID,
		})
	}
	st.Round++
	return nil, false, nil
}

// executeToolCall parses arguments and calls the tool via the binder.
func (c *chatCognition) executeToolCall(ctx context.Context, tc core.ToolCall) (string, error) {
	var args map[string]any
	if tc.Function.Arguments != "" {
		if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
			return "", errors.Wrap(err, "parse tool arguments")
		}
	}

	// Stamp the caller identity into the tool context before invoking the
	// tool so Kernel syscalls (agentsyscall) can enforce provenance
	// (Task.Origin / ParentID) from the context, never from LLM args.
	result, err := c.toolBinder.CallTool(kctx.WithCallerID(ctx, c.agentID), tc.Function.Name, args)
	if err != nil {
		return "", err
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Sprintf("%v", result), nil
	}
	return string(resultJSON), nil
}

// renderPromptAndParams renders the worker prompt (with the active evolution
// strategy's template/param overrides and the active role's instructions) and
// the per-call LLM params.
func (c *chatCognition) renderPromptAndParams(ctx context.Context, task *models.Task) (string, map[string]any, error) {
	tpl := c.promptTpl
	params := map[string]any{}
	if st := c.activeStrategy(ctx); st != nil {
		if st.Prompt != "" {
			tpl = st.Prompt
		}
		for k, v := range st.Params {
			params[k] = v
		}
	}

	// C5: overlay node-level ToolStep attributes (tools/budget/prior) from the
	// task payload onto the global strategy params, with NODE OVER GLOBAL
	// priority (§8.5). A ProjectStep node's Metadata rides this exact payload.
	params = agents.MergeNodeParams(params, task.Payload)

	prompt, err := c.renderPrompt(tpl, task)
	if err != nil {
		return "", nil, err
	}

	// C5: prior is prompt-only — it biases tool choice without removing any
	// tool from the advertised set (that is the whitelist's / budget's job).
	prompt = agents.ApplyPriorHint(prompt, agents.PriorHintFromParams(params))

	// P0-3: prepend the active role's system instructions when the execution
	// was switched to a specialized role via Handoff (Ch.10 multi-stage role
	// transition).
	if instructions := c.activeRoleInstructions(ctx); instructions != "" {
		prompt = instructions + "\n\n" + prompt
	}
	c.logger.Debug("Generated prompt", "preview", prompt[:min(200, len(prompt))])
	return prompt, params, nil
}

// renderPrompt renders the worker prompt template with the task data. It
// fails fast on an empty result: an empty user message is rejected by
// OpenAI-compatible providers with a 400.
func (c *chatCognition) renderPrompt(tpl string, task *models.Task) (string, error) {
	promptData := map[string]any{
		"Category": string(task.AgentType),
	}

	// Carry the original task input into the template as {{.input}}.
	if task.Payload != nil {
		if desc, ok := task.Payload["task_desc"].(string); ok && desc != "" {
			promptData["input"] = desc
		}
	}
	if _, ok := promptData["input"]; !ok {
		promptData["input"] = string(task.AgentType)
	}

	// Generic profile fields (lowercase keys to match {{index . "key"}}).
	if task.UserProfile != nil {
		if len(task.UserProfile.Preferences) > 0 {
			for k, v := range task.UserProfile.Preferences {
				promptData[k] = v
			}
		}
		if task.UserProfile.Budget != nil {
			promptData["budget"] = formatBudget(task.UserProfile.Budget)
		}
		if len(task.UserProfile.Style) > 0 {
			promptData["style"] = task.UserProfile.Style
		}
	}

	prompt, err := c.template.Render(tpl, promptData)
	if err != nil {
		return "", errors.Wrap(err, "render prompt")
	}
	if strings.TrimSpace(prompt) == "" {
		return "", fmt.Errorf("render prompt: empty prompt (template=%q)", tpl)
	}
	return prompt, nil
}

// activeStrategy fetches the currently-deployed evolution strategy, if any.
// Errors are logged and ignored so a missing store never breaks execution.
func (c *chatCognition) activeStrategy(ctx context.Context) *agents.ActiveStrategy {
	if c.strategySource == nil {
		return nil
	}
	st, err := c.strategySource.GetActiveStrategy(ctx)
	if err != nil {
		c.logger.Warn("failed to read active strategy", "error", err)
		return nil
	}
	return st
}

// executeWithLLMTextOnly performs a text-only LLM generation.
func (c *chatCognition) executeWithLLMTextOnly(ctx context.Context, prompt string, params map[string]any) ([]*models.RecommendItem, error) {
	if c.llmAdapter == nil {
		return nil, errors.New("agentfabric: no text-only LLM adapter available")
	}
	c.emitEvent(ctx, ares_events.EventLLMCall, map[string]any{
		KeyAgentID: c.agentID,
		"prompt":   prompt[:min(200, len(prompt))],
	})
	response, err := c.llmAdapter.GenerateWithParams(ctx, prompt, params)
	if err != nil {
		c.emitEvent(ctx, ares_events.EventLLMCall, map[string]any{
			KeyAgentID: c.agentID,
			KeyError:   err.Error(),
			KeyStatus:  "failed",
		})
		return nil, errors.Wrap(err, "LLM call failed")
	}
	c.logger.Debug("LLM response", "preview", response[:min(500, len(response))])
	return c.parseRecommendResult(response)
}

// parseRecommendResult parses the LLM text response into RecommendItems,
// wrapping prose answers in a single item (never a fake failure).
func (c *chatCognition) parseRecommendResult(response string) ([]*models.RecommendItem, error) {
	parser := output.NewParser()
	result, err := parser.ParseRecommendResult(response)
	if err == nil && result != nil && result.Items != nil {
		c.logger.Info("Parsed result items", "count", len(result.Items))
		return result.Items, nil
	}
	if trimmed := strings.TrimSpace(response); trimmed != "" {
		c.logger.Debug("Strict parse failed, wrapping prose", "error", err)
		item := &models.RecommendItem{
			ItemID:      fmt.Sprintf("prose-%s-%d", c.agentID, time.Now().UnixNano()),
			Category:    "general",
			Name:        "Agent output",
			Description: trimmed[:min(500, len(trimmed))],
			Content:     trimmed,
		}
		return []*models.RecommendItem{item}, nil
	}
	return nil, errors.Wrap(err, "parse result")
}

// emitCallback emits a lifecycle callback event if the emitter is set.
func (c *chatCognition) emitCallback(ctx *ares_callbacks.Context) {
	if c.callbacks == nil {
		return
	}
	c.callbacks.Emit(ctx)
}

// emitEvent appends a single event using the canonical ares_events.Emit
// helper. No-op if eventStore is nil.
func (c *chatCognition) emitEvent(ctx context.Context, eventType ares_events.EventType, payload map[string]any) {
	if !ares_events.Emit(ctx, c.eventStore, c.agentID, eventType, "agentfabric", payload) {
		c.logger.Warn("failed to emit event", "event_type", eventType, "stream_id", c.agentID)
	}
}

// toolErrorMessage normalizes a tool execution error into the C1 event
// contract's error field. nil -> "" so the unified payload is JSON-friendly
// and the projection layer can distinguish "failed with message" from
// "no error" without an extra key.
func toolErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// formatBudget formats a PriceRange for the prompt template.
func formatBudget(budget *models.PriceRange) string {
	if budget == nil {
		return "0 - 10000"
	}
	return fmt.Sprintf("%.0f - %.0f", budget.Min, budget.Max)
}

// activeRoleInstructions returns the system instructions to prepend to the
// worker prompt. An explicit Handoff switch (a role carved into the execution
// context via agents.GetFromContext, P0-3) wins; otherwise it falls back to
// the cognition's construction-time profile (Profile from ChatCognitionDeps,
// W4). Empty string = no role instructions. The ctx-first order keeps P0-3
// runtime role transitions authoritative while a config-pinned peer still gets
// its role instructions without an explicit Handoff.
func (c *chatCognition) activeRoleInstructions(ctx context.Context) string {
	profile := agents.GetFromContext(ctx)
	if profile == nil {
		profile = c.profile
	}
	if profile == nil {
		return ""
	}
	return profile.Instructions
}
