package agentloop

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/api/service/llm"
	"github.com/Timwood0x10/ares/api/tools"
	ares_events "github.com/Timwood0x10/ares/internal/ares_events"
	memory "github.com/Timwood0x10/ares/internal/ares_memory"
	memctx "github.com/Timwood0x10/ares/internal/ares_memory/context"
	kctx "github.com/Timwood0x10/ares/internal/kernel/ctx"
	"github.com/Timwood0x10/ares/internal/logger"
)

// log is the module-scoped structured logger for the agent loop. Best-effort
// event emission failures are routed here (never silently swallowed) so an
// observability regression is visible in production even when Tracer is nil.
var log = logger.Module("agentloop")

// DefaultMaxIterations is the default cap on the ReAct tool-calling loop when
// Request.MaxIter is <= 0. It mirrors sdk.defaultMaxIterations so the engine
// can stand alone without importing the sdk package.
const DefaultMaxIterations = 10

// Role constants for LLM messages emitted by the engine loop.
const (
	roleAssistant = "assistant"
	roleTool      = "tool"
)

// maxIterationsReachedMsg is the Result.Output value (and trace token) the
// engine returns when it hits the iteration cap without a final answer. It is
// shared by production and test code so the contract is defined in one place.
const maxIterationsReachedMsg = "max iterations reached"

// maxTokensReachedMsg is the Result.Output value (and trace token) the engine
// returns when Request.MaxTokens is exceeded before a final answer.
const maxTokensReachedMsg = "max tokens reached"

// timeoutReachedMsg is the Result.Output value (and trace token) the engine
// returns when Request.Timeout is exceeded before a final answer.
const timeoutReachedMsg = "timeout reached"

// DiscoverToolsName is the well-known name of the runtime discovery meta-tool.
// When the engine executes a tool call with this name, it parses the result as a
// JSON array of tool names and asks ToolExpander to turn them into LLM tool
// definitions, appending them to the active set for subsequent iterations.
//
// The same constant is intentionally duplicated in internal/tools/toolsource
// (the package that builds the meta-tool implementation). agentloop must not
// import toolsource, so the name is replicated here; a shared test asserts the
// two constants stay equal.
const DiscoverToolsName = "discover_tools"

// HumanInputFunc is called before each tool call to request human approval.
// Return true to approve, false to skip the tool call, or an error to abort.
// It is structurally identical to sdk.HumanInputFunc; the sdk converts its
// func type to this one when building a Request (no import cycle).
type HumanInputFunc func(ctx context.Context, toolName string, args map[string]any) (bool, error)

// LLMCaller is the subset of the LLM service the engine needs: generate a
// completion and report the provider for friendly error hints.
type LLMCaller interface {
	Generate(ctx context.Context, req *core.GenerateRequest) (*core.GenerateResponse, error)
	GetProvider() core.LLMProvider
}

// ToolExecutor executes a registered tool by name. It matches
// tools.Registry.Execute's signature (returns a Result value, not a pointer).
type ToolExecutor interface {
	Execute(ctx context.Context, name string, args map[string]any) (tools.Result, error)
}

// ToolExpander resolves runtime-discovered tool names into LLM tool definitions
// so they can be appended to the active tool set when an agent calls the
// discover_tools meta-tool. nil disables runtime expansion (the meta-tool
// result is still returned to the LLM as text, but no new tools become
// callable).
//
// The sdk provides the concrete adapter (it looks names up in the available
// tool pool and converts them via toCoreTools); agentloop only depends on this
// narrow interface to stay decoupled from toolsource.
type ToolExpander interface {
	Expand(ctx context.Context, names []string) ([]core.Tool, error)
}

// EventSink appends events to an event stream. It is the minimal subset of
// ares_events.EventStore required by the engine.
type EventSink interface {
	Append(ctx context.Context, streamID string, events []*ares_events.Event, expectedVersion int64) error
}

// MemorySink persists conversation messages. It is the minimal subset of
// memory.MemoryManager required by the engine. Session creation stays in the
// sdk because it must precede memory-context building (BuildContext), which
// the sdk owns.
type MemorySink interface {
	AddMessage(ctx context.Context, sessionID, role, content string) error
}

// structuredMemorySink is the optional extension of MemorySink that persists
// full message metadata (including ToolCalls) so multi-turn context rebuilds
// retain the "assistant called these tools" trail. memory.MemoryManager
// implements it; minimal mocks may not, in which case the engine falls back
// to AddMessage (content-only).
type structuredMemorySink interface {
	AddStructuredMessage(ctx context.Context, sessionID string, msg memctx.Message) error
}

// toMemToolCalls converts core tool calls into the memory-layer representation.
func toMemToolCalls(calls []core.ToolCall) []memctx.ToolCall {
	out := make([]memctx.ToolCall, 0, len(calls))
	for _, c := range calls {
		out = append(out, memctx.ToolCall{
			ID:   c.ID,
			Type: c.Type,
			Function: memctx.ToolCallFunction{
				Name:      c.Function.Name,
				Arguments: c.Function.Arguments,
			},
		})
	}
	return out
}

// Compile-time checks that the concrete sdk wiring types satisfy the engine's
// narrow interfaces. If a signature drifts, these fail the build.
var (
	_ LLMCaller    = (*llm.Service)(nil)
	_ ToolExecutor = (*tools.Registry)(nil)
	_ EventSink    = (ares_events.EventStore)(nil)
	_ MemorySink   = (memory.MemoryManager)(nil)
)

// Engine encapsulates one agent execution lifecycle: a pre-built message list
// plus tool definitions driven through a ReAct loop over the LLM to a final
// result. It has no dependency on sdk.Runtime, so it is independently
// testable with mock LLM/tools/memory/events.
//
// All fields are optional via nil checks: nil Events disables event emission,
// nil Memory disables persistence, nil Tracer disables trace logging.
type Engine struct {
	// LLM generates completions and reports the provider for error hints.
	LLM LLMCaller
	// Tools executes tool calls by name.
	Tools ToolExecutor
	// Events appends tool-call and task-completed events; nil disables emission.
	Events EventSink
	// Memory persists assistant messages; nil disables persistence.
	Memory MemorySink
	// Tracer receives trace log lines (same format as sdk.Agent.Run). When nil,
	// trace logging is disabled. The sdk passes log.Printf when tracing is on.
	Tracer func(format string, args ...any)
	// MemEnabled mirrors sdk.Runtime.memEnabled; gates assistant AddMessage calls.
	MemEnabled bool
	// DistillEnabled gates TaskCompleted emission (mirrors distillSvc != nil in
	// the sdk). Tool-call events are gated on Events != nil independently.
	DistillEnabled bool
}

// Request is one agent execution request. The caller (sdk.Agent.Run) builds
// Messages (system instruction + memory/knowledge context + user input) and
// creates the session before delegating the ReAct loop to the engine.
type Request struct {
	// Messages is the pre-built initial message list. The engine appends
	// assistant and tool messages to a copy during the loop.
	Messages []*core.LLMMessage
	// Tools is the core.Tool definitions passed to the LLM for function calling.
	Tools []core.Tool
	// MaxIter caps the ReAct loop iterations; <=0 falls back to DefaultMaxIterations.
	MaxIter int
	// MaxTokens caps the cumulative prompt+completion tokens across all LLM
	// calls in this run; <=0 means no token budget. The loop stops as soon as
	// the accumulated usage exceeds the cap and returns "max tokens reached".
	MaxTokens int
	// Timeout caps the total wall-clock duration of the run; <=0 means no time
	// budget. The loop stops when the deadline is exceeded and returns
	// "timeout reached". The per-call context (ctx) is unchanged — Timeout is
	// an additional budget the engine enforces between LLM calls.
	Timeout time.Duration
	// AgentName identifies the agent for trace logs and tool-call event streams.
	AgentName string
	// SessionID is the memory session used for AddMessage and the TaskCompleted stream.
	SessionID string
	// Input is the original user input, used in the TaskCompleted event payload.
	Input string
	// HumanInput, when non-nil, is called before each tool call for approval.
	HumanInput HumanInputFunc
	// ToolExpander, when non-nil, enables runtime tool discovery: when the LLM
	// calls the discover_tools meta-tool, the engine parses its result as a JSON
	// array of tool names and asks ToolExpander to resolve them into LLM tool
	// definitions, which are appended (deduped by Function.Name) to the active
	// tool set for subsequent iterations. nil disables expansion (the meta-tool
	// result is still returned to the LLM as text, but no new tools become
	// callable). When no discover_tools call happens, behaviour is identical to
	// passing no expander.
	ToolExpander ToolExpander
	// ToolWhitelist, when non-nil, restricts which of the active tools reach the
	// LLM on every iteration (Y1 方案C C5 third-executor wiring). An empty or
	// missing map means "all tools" (zero-value usable). This mirrors the
	// Params["tools"] whitelist the two peer executors (chat_cognition.go,
	// sub/executor.go) apply via agents.ToolWhitelistFromParams — the agentloop
	// ReAct loop is the third production execution body, so without this it was
	// the only path that ignored the tool-selection knob. Filtering happens
	// before the LLM sees the tool list, not at execution time.
	ToolWhitelist map[string]bool
}

// Result mirrors the execution outcome of sdk.Agent.Run, expressed without
// sdk-specific types so the engine stays decoupled.
type Result struct {
	// Output is the final LLM answer (or "max iterations reached").
	Output string
	// ToolCalls is the total number of tool calls executed.
	ToolCalls int
	// MemoryUsed reports whether memory persistence was enabled for this run.
	MemoryUsed bool
	// InputTokens is the cumulative prompt token count across all LLM calls.
	InputTokens int
	// OutputTokens is the cumulative completion token count across all LLM calls.
	OutputTokens int
	// Duration is the wall-clock time spent in Run.
	Duration time.Duration
}

// iterState carries the mutable loop state so helper methods stay under the
// parameter-count limit (rule: params <= 5).
type iterState struct {
	messages    []*core.LLMMessage
	toolCount   int
	inputTok    int
	outputTok   int
	activeTools []core.Tool
}

// Run executes the ReAct loop against the pre-built messages and returns the
// final result. The loop mirrors sdk.Agent.Run exactly:
//
//  1. Call the LLM with the current messages and tool definitions.
//  2. Append the assistant message; persist it to memory when enabled.
//  3. If no tool calls, emit TaskCompleted and return the final answer.
//  4. Otherwise execute each tool call (with human-in-the-loop approval),
//     append tool results, emit tool-call events, and continue.
//  5. After MaxIter iterations without a final answer, return "max iterations
//     reached".
func (e *Engine) Run(ctx context.Context, req *Request) (*Result, error) {
	start := time.Now()
	// Copy req.Tools so runtime expansion never mutates the caller's slice.
	// When no discover_tools call happens, activeTools stays equal to req.Tools
	// and the GenerateRequest is identical to the pre-expansion behaviour.
	activeTools := make([]core.Tool, len(req.Tools))
	copy(activeTools, req.Tools)
	// Copy req.Messages so that append does not write into the caller's
	// backing array when the caller used append (cap > len). Without this
	// copy, retry/multi-agent scenarios that reuse the same Messages slice
	// would see messages from one run leak into another.
	msgs := make([]*core.LLMMessage, len(req.Messages))
	copy(msgs, req.Messages)
	st := &iterState{messages: msgs, activeTools: activeTools}
	maxIter := req.MaxIter
	if maxIter <= 0 {
		maxIter = DefaultMaxIterations
	}

	for iter := 0; iter < maxIter; iter++ {
		e.trace("[ares:trace] %s → LLM call (iter %d, %d msgs, %d tools)",
			req.AgentName, iter, len(st.messages), len(st.activeTools))

		// Thread-state observability: emit the LLM-call phase so the Runtime
		// (as the OS scheduling the agent thread) can observe which phase the
		// thread is in — LLM reasoning vs tool execution vs completed. The
		// tool-call phases are already emitted; this closes the gap so an
		// observer sees the full thread timeline, not just tool boundaries.
		e.emitLLMCall(ctx, req.AgentName, iter, len(st.messages), len(st.activeTools))

		// C5: the agentloop is the third execution body — respect the tool
		// whitelist before handing the tool set to the LLM. An empty whitelist
		// means "all tools".
		granted := st.activeTools
		if len(req.ToolWhitelist) > 0 {
			granted = make([]core.Tool, 0, len(st.activeTools))
			for _, t := range st.activeTools {
				if req.ToolWhitelist[t.Function.Name] {
					granted = append(granted, t)
				}
			}
		}

		resp, err := e.LLM.Generate(ctx, &core.GenerateRequest{
			Messages: st.messages,
			Tools:    granted,
		})
		if err != nil {
			return nil, FriendlyErr("llm generate", e.LLM.GetProvider(), err)
		}

		st.inputTok += resp.Usage.PromptTokens
		st.outputTok += resp.Usage.CompletionTokens

		// Token budget: stop the loop as soon as cumulative usage reaches the
		// cap so a runaway agent cannot burn unbounded tokens before hitting
		// the iteration cap (primitive 4: bounded autonomous execution).
		if req.MaxTokens > 0 && st.inputTok+st.outputTok >= req.MaxTokens {
			e.trace("[ares:trace] %s ⚠ %s (%d total)", req.AgentName, maxTokensReachedMsg, st.inputTok+st.outputTok)
			e.emitTaskCompleted(ctx, req.SessionID, req.Input, req.AgentName, maxTokensReachedMsg)
			return &Result{
				Output:       maxTokensReachedMsg,
				ToolCalls:    st.toolCount,
				MemoryUsed:   e.MemEnabled,
				InputTokens:  st.inputTok,
				OutputTokens: st.outputTok,
				Duration:     time.Since(start),
			}, nil
		}

		// Wall-clock budget: stop between LLM calls once the deadline passes.
		if req.Timeout > 0 && time.Since(start) > req.Timeout {
			e.trace("[ares:trace] %s ⚠ %s (%v)", req.AgentName, timeoutReachedMsg, time.Since(start).Round(time.Millisecond))
			e.emitTaskCompleted(ctx, req.SessionID, req.Input, req.AgentName, timeoutReachedMsg)
			return &Result{
				Output:       timeoutReachedMsg,
				ToolCalls:    st.toolCount,
				MemoryUsed:   e.MemEnabled,
				InputTokens:  st.inputTok,
				OutputTokens: st.outputTok,
				Duration:     time.Since(start),
			}, nil
		}

		// Append the assistant message.
		st.messages = append(st.messages, &core.LLMMessage{
			Role:      roleAssistant,
			Content:   resp.Content,
			ToolCalls: resp.ToolCalls,
		})

		if e.MemEnabled && e.Memory != nil {
			// Persist the assistant message with its ToolCalls when the memory
			// layer supports structured messages, so multi-turn context rebuild
			// keeps the tool-call trail (not just Content). Fall back to the
			// content-only AddMessage for minimal sinks/mocks.
			if sm, ok := e.Memory.(structuredMemorySink); ok && len(resp.ToolCalls) > 0 {
				// best-effort: memory persistence failure must not abort the
				// agent loop — the in-memory message trail is already
				// appended above and the conversation can continue. A
				// dropped structured message means context rebuild may lose
				// tool-call trails, which degrades quality but not safety.
				_ = sm.AddStructuredMessage(ctx, req.SessionID, memctx.Message{
					Role:      roleAssistant,
					Content:   resp.Content,
					Time:      time.Now(),
					ToolCalls: toMemToolCalls(resp.ToolCalls),
				})
			} else {
				// best-effort: same rationale as the structured path —
				// memory persistence is not on the critical path of the
				// agent loop. The in-memory trail is authoritative for
				// the current turn; a failed persist only affects future
				// context rebuilds.
				_ = e.Memory.AddMessage(ctx, req.SessionID, roleAssistant, resp.Content)
			}
		}

		// Final answer: no tool calls.
		if len(resp.ToolCalls) == 0 {
			e.trace("[ares:trace] %s ✓ done (%d tools, %d total tokens, %v)",
				req.AgentName, st.toolCount, st.inputTok+st.outputTok,
				time.Since(start).Round(time.Millisecond))
			e.emitTaskCompleted(ctx, req.SessionID, req.Input, req.AgentName, resp.Content)
			return &Result{
				Output:       resp.Content,
				ToolCalls:    st.toolCount,
				MemoryUsed:   e.MemEnabled,
				InputTokens:  st.inputTok,
				OutputTokens: st.outputTok,
				Duration:     time.Since(start),
			}, nil
		}

		if err := e.executeToolCalls(ctx, req, st, iter, resp.ToolCalls); err != nil {
			return nil, err
		}
	}

	e.trace("[ares:trace] %s ⚠ %s (%d)", req.AgentName, maxIterationsReachedMsg, maxIter)
	// The run still reached a terminal state (the cap), so emit TaskCompleted
	// so the distill pipeline observes runs that ended via the iteration cap.
	// Previously the cap path returned without emitting, silently dropping the
	// result of any run that exhausted its budget.
	e.emitTaskCompleted(ctx, req.SessionID, req.Input, req.AgentName, maxIterationsReachedMsg)
	return &Result{
		Output:       maxIterationsReachedMsg,
		ToolCalls:    st.toolCount,
		MemoryUsed:   e.MemEnabled,
		InputTokens:  st.inputTok,
		OutputTokens: st.outputTok,
		Duration:     time.Since(start),
	}, nil
}

// executeToolCalls runs the human-in-the-loop check, event emission, and tool
// execution for one batch of tool calls, appending tool messages to st.messages.
// Returns an error only when HumanInput returns an error (aborting the run).
func (e *Engine) executeToolCalls(ctx context.Context, req *Request, st *iterState, iter int, calls []core.ToolCall) error {
	for seq, tc := range calls {
		args := parseArgs(tc.Function.Arguments)

		// Human-in-the-loop check.
		if req.HumanInput != nil {
			approved, err := req.HumanInput(ctx, tc.Function.Name, args)
			if err != nil {
				return fmt.Errorf("human input: %w", err)
			}
			if !approved {
				e.trace("[ares:trace] %s → tool call REJECTED by human: %s",
					req.AgentName, tc.Function.Name)
				st.messages = append(st.messages, &core.LLMMessage{
					Role:       roleTool,
					ToolCallID: tc.ID,
					Content: fmt.Sprintf("Tool call %s was rejected by human operator",
						tc.Function.Name),
				})
				continue
			}
		}

		st.toolCount++
		e.emitToolEvent(ctx, req.AgentName, ares_events.EventToolCallStarted,
			map[string]any{
				ares_events.EventKeyToolName:   tc.Function.Name,
				ares_events.EventKeyToolCallID: tc.ID,
				ares_events.EventKeyRound:      iter,
				ares_events.EventKeySeq:        seq,
				ares_events.EventKeyTool:       tc.Function.Name,
			})
		e.trace("[ares:trace] %s → tool call: %s(%s)",
			req.AgentName, tc.Function.Name, tc.Function.Arguments)

		// Stamp the caller identity into the context before executing the
		// tool so Kernel syscalls (agentsyscall) can enforce provenance
		// (Task.Origin / ParentID) from the context, never from LLM args.
		result, err := e.Tools.Execute(kctx.WithCallerID(ctx, req.AgentName), tc.Function.Name, args)
		resultContent := ""
		if err != nil {
			resultContent = fmt.Sprintf("Error: %v", err)
		} else {
			// discover_tools returns Data as a JSON-encoded string; %v of a
			// string yields the raw JSON text, which expandDiscoveredTools
			// parses below. This implicit string-JSON contract is documented
			// on discoverToolsTool.Execute; changing Data's type would break
			// expansion silently (the parse-error branch traces and returns).
			resultContent = fmt.Sprintf("%v", result.Data)
		}

		st.messages = append(st.messages, &core.LLMMessage{
			Role:       roleTool,
			ToolCallID: tc.ID,
			Content:    resultContent,
		})

		e.emitToolEvent(ctx, req.AgentName, ares_events.EventToolCallCompleted,
			map[string]any{
				ares_events.EventKeyToolName:   tc.Function.Name,
				ares_events.EventKeyToolCallID: tc.ID,
				ares_events.EventKeyTool:       tc.Function.Name,
				ares_events.EventKeyRound:      iter,
				ares_events.EventKeySeq:        seq,
				ares_events.EventKeySuccess:    err == nil,
				ares_events.EventKeyError:      toolErrorMessage(err),
				ares_events.EventKeyArgShape:   ares_events.ToolArgShape(tc.Function.Arguments),
				"args":                         tc.Function.Arguments,
				"result":                       resultContent,
			})

		// Runtime tool discovery: when the agent called the discover_tools
		// meta-tool, expand the returned names into LLM tool defs and append
		// them (deduped) to the active set for subsequent iterations. The tool
		// result is already appended above, so expansion failures are non-fatal.
		// Skip expansion on execution error or a non-success Result (e.g. empty
		// query / source failure): those return a plain error string, not JSON,
		// so parsing would only emit a spurious "parse failed" trace.
		if tc.Function.Name == DiscoverToolsName && err == nil && result.Success {
			e.expandDiscoveredTools(ctx, req, st, resultContent)
		}
	}
	return nil
}

// expandDiscoveredTools parses a discover_tools result as a JSON array of
// {name, description} objects and asks req.ToolExpander to resolve the names
// into LLM tool definitions, appending any new ones (deduped by Function.Name)
// to st.activeTools. It is non-fatal: parse errors, expander errors, and
// per-tool duplicates are traced and skipped so the run continues with the tool
// result already in messages. No-op when req.ToolExpander is nil (discovery
// expansion disabled).
func (e *Engine) expandDiscoveredTools(ctx context.Context, req *Request, st *iterState, resultContent string) {
	if req.ToolExpander == nil {
		return
	}
	// The discover_tools meta-tool returns Data as a JSON string of
	// [{"name":..., "description":...}] objects. We only need the names.
	var entries []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(resultContent), &entries); err != nil {
		e.trace("[ares:trace] %s ⚠ discover_tools result parse failed: %v",
			req.AgentName, err)
		return
	}
	if len(entries) == 0 {
		return
	}
	names := make([]string, 0, len(entries))
	for _, en := range entries {
		if en.Name != "" {
			names = append(names, en.Name)
		}
	}
	expanded, err := req.ToolExpander.Expand(ctx, names)
	if err != nil {
		e.trace("[ares:trace] %s ⚠ discover_tools expand failed: %v",
			req.AgentName, err)
		return
	}
	for _, t := range expanded {
		if toolNameInSet(t.Function.Name, st.activeTools) {
			continue
		}
		st.activeTools = append(st.activeTools, t)
		e.trace("[ares:trace] %s + discovered tool: %s",
			req.AgentName, t.Function.Name)
	}
}

// toolNameInSet reports whether name already appears in tools by Function.Name.
// Used for dedup during runtime tool expansion; the active set is small so a
// linear scan is sufficient.
func toolNameInSet(name string, tools []core.Tool) bool {
	for _, t := range tools {
		if t.Function.Name == name {
			return true
		}
	}
	return false
}

// emitLLMCall appends an LLM-call phase event to the agent-name stream
// (thread-state observability): it marks the reasoning phase of the agent
// thread, complementing the tool-call Started/Completed events. Payload
// carries the iteration, message count and active tool count so an observer
// can reconstruct the thread's context at the call boundary. No-op when
// Events is nil. Emission is best-effort observability, not control flow: an
// Append failure is logged but never aborts the agent loop.
func (e *Engine) emitLLMCall(ctx context.Context, agentName string, iter, msgCount, toolCount int) {
	if e.Events == nil {
		return
	}
	if err := e.Events.Append(ctx, agentName, []*ares_events.Event{{
		Type:     ares_events.EventLLMCall,
		StreamID: agentName,
		Payload: map[string]any{
			"iter":       iter,
			"msg_count":  msgCount,
			"tool_count": toolCount,
		},
	}}, 0); err != nil {
		log.Warn("emit llm call event failed",
			"agent", agentName,
			"iter", iter,
			"error", err)
	}
}

// toolErrorMessage normalizes a tool execution error into the C1 event
// contract's error field, matching the peer executors (chat_cognition.go,
// sub/executor.go). nil -> "" so the unified payload is JSON-friendly and the
// trajectory projection can distinguish "failed with message" from "no error".
func toolErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// emitToolEvent appends a single tool-call event (Started or Completed) to the
// agent-name stream. It passes expectedVersion=0 (auto-detect) so appends never
// conflict with the stream's current version — matching emitTaskCompleted and
// the ares_events.Emit helper. Previously it passed expectedVersion=toolCount-1,
// which became stale after the first tool call's Started+Completed advanced the
// stream version, causing every subsequent event to be rejected with
// ErrVersionConflict and silently dropped. No-op when Events is nil.
//
// Event emission is best-effort observability, not control flow: an Append
// failure is logged (never silently swallowed) but never aborts the agent loop.
func (e *Engine) emitToolEvent(
	ctx context.Context,
	agentName string,
	evType ares_events.EventType,
	payload map[string]any,
) {
	if e.Events == nil {
		return
	}
	if err := e.Events.Append(ctx, agentName, []*ares_events.Event{{
		Type:     evType,
		StreamID: agentName,
		Payload:  payload,
	}}, 0); err != nil {
		log.Warn("emit tool event failed",
			"agent", agentName,
			"event_type", evType,
			"error", err)
	}
}

// emitTaskCompleted appends a TaskCompleted event to the session stream,
// replicating ares_events.Emit: a fresh event ID, ModuleName "runtime", the
// task/result/tenant payload, and expectedVersion 0 (auto-detect). No-op when
// Events is nil or DistillEnabled is false (mirrors distillSvc != nil gating).
// Append failures are best-effort: logged and never abort the agent loop.
func (e *Engine) emitTaskCompleted(ctx context.Context, sessionID, input, agentName, result string) {
	if e.Events == nil || !e.DistillEnabled {
		return
	}
	if err := e.Events.Append(ctx, sessionID, []*ares_events.Event{{
		ID:         ares_events.NewEventID(),
		StreamID:   sessionID,
		Type:       ares_events.EventTaskCompleted,
		ModuleName: "runtime",
		Payload: map[string]any{
			ares_events.EventKeyTask:     input,
			ares_events.EventKeyResult:   result,
			ares_events.EventKeyTenantID: ares_events.DefaultTenantID,
			"agent_id":                   agentName,
		},
		Timestamp: time.Now(),
	}}, 0); err != nil {
		log.Warn("emit task completed event failed",
			"session_id", sessionID,
			"agent", agentName,
			"error", err)
	}
}

// trace forwards a formatted line to the Tracer when one is configured. It is
// a no-op when Tracer is nil (tracing disabled).
func (e *Engine) trace(format string, args ...any) {
	if e.Tracer != nil {
		e.Tracer(format, args...)
	}
}

// parseArgs unmarshals a JSON arguments string into a map. Returns nil for
// empty or invalid JSON, mirroring sdk.parseArgs.
func parseArgs(raw string) map[string]any {
	if raw == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		log.Warn("parseArgs: invalid JSON tool arguments",
			"raw", raw,
			"error", err)
		return nil
	}
	return m
}

// FriendlyErr wraps an LLM error with an actionable hint based on the provider.
// Exported so the sdk can reuse the same hint table (single source of truth)
// for both New() and engine-driven runs. The message format matches the
// original sdk.friendlyErr exactly.
func FriendlyErr(scope string, provider core.LLMProvider, origErr error) error {
	hints := map[core.LLMProvider]string{
		core.LLMProviderOpenAI:     "→ Set OPENAI_API_KEY or check https://platform.openai.com/account/api-keys",
		core.LLMProviderAnthropic:  "→ Set ANTHROPIC_API_KEY or check https://console.anthropic.com/",
		core.LLMProviderOpenRouter: "→ Set OPENROUTER_API_KEY or check https://openrouter.ai/keys",
		core.LLMProviderOllama:     "→ Run: ollama run llama3.2  (Ollama may not be running)",
	}
	msg := fmt.Sprintf("%s: %v", scope, origErr)
	if hint, ok := hints[provider]; ok {
		msg += "\n  " + hint
	}
	// B10: wrap with %w so errors.Is/As can match the underlying cause.
	return fmt.Errorf("%s: %w", msg, origErr)
}
