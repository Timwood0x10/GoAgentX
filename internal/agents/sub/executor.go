package sub

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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

// FallbackHandler produces a recommendation fallback result for a given task type.
// Used when the LLM is unavailable or fails. Returns items, explanation, error.
type FallbackHandler func(ctx context.Context, task *models.Task) ([]*models.RecommendItem, string, error)

// ChatClient sends chat messages with tool support to the LLM.
// When set on the executor, the agent can use native tool calling
// instead of text-only prompt generation. The optional params map carries
// per-call overrides (temperature, max_tokens, top_k) from the active
// evolution strategy so the live agent can be steered at runtime.
type ChatClient interface {
	Chat(ctx context.Context, messages []*core.LLMMessage, tools []core.Tool, params map[string]any) (*core.GenerateResponse, error)
}

const defaultMaxToolRounds = 5

// stepSchemaVersion is the current chatStepState schema (code_rules:
// snapshot structs carry a version; decodeChatStepState rejects unknown ones).
const stepSchemaVersion = 1

// chatStepState is the resumable program-counter block (PCB) of one
// tool-calling execution (plan P1.1 Execution Quantum: reason → tool call →
// observation → checkpoint). A quantum (ExecuteStep) advances it by exactly
// one ReAct round; when the LLM answers without tool calls it carries the
// final answer to completion. It is JSON round-trippable so it can ride the
// fabric's opaque checkpoint slot across a yield→resume cycle, and the resume
// path always decodes into a fresh copy so a stored checkpoint is never
// mutated in place across quanta .
//
// TaskID is the resume identity check: a checkpoint that does not
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
	// session, persisted across quanta so a resumed task keeps spending the
	// same node budget (C5). Mirrors agentfabric/chat_cognition.go.
	ToolUses map[string]int `json:"tool_uses,omitempty"`
}

// taskExecutor executes recommendation tasks.
type taskExecutor struct {
	mu               sync.RWMutex // protects eventStore, agentID, ares_callbacks, fallbackHandlers
	toolBinder       ToolBinder
	llmAdapter       output.LLMAdapter
	chatClient       ChatClient            // Optional: enables native tool calling via Chat API
	strategySource   agents.StrategySource // Optional: live strategy overrides (prompt + LLM params)
	maxToolRounds    int                   // Max tool-calling iterations (default 5)
	template         *output.TemplateEngine
	promptTpl        string
	validator        *output.Validator
	maxRetries       int
	retryOnFail      bool // Retry LLM call when validation fails
	strictMode       bool // Return error on validation failure
	logger           *slog.Logger
	eventStore       ares_events.EventStore // Optional: emits ares_events for tool/LLM calls
	agentID          string                 // Agent ID for event emission
	ares_callbacks   ares_callbacks.Emitter // Optional: emits lifecycle callback ares_events.
	fallbackHandlers map[models.AgentType]FallbackHandler
	// profile is the agent role (W4). When set, Execute/ExecuteStep apply it
	// into the task context so activeRoleInstructions reads the role's
	// instructions — closing the read-side contract in agents/profile.go.
	profile *agents.AgentProfile
}

// TaskExecutorOption configures a taskExecutor instance during construction.
type TaskExecutorOption func(*taskExecutor)

// WithTaskExecutorCallbacks returns a TaskExecutorOption that sets the callback emitter.
// The emitter will receive lifecycle ares_events (tool.start, tool.end, tool.error)
// during task execution.
// WithTaskExecutorCallbacks returns a TaskExecutorOption that wires the given
// emitter as the lifecycle callback sink.
func WithTaskExecutorCallbacks(emitter ares_callbacks.Emitter) TaskExecutorOption {
	return func(e *taskExecutor) {
		e.ares_callbacks = emitter
	}
}

// WithProfile returns a TaskExecutorOption that pins the agent role. The
// profile is applied into every task context (W4 write side), so the read
// side — agents.GetFromContext via activeRoleInstructions — finally receives
// the role instructions in production.
func WithProfile(p *agents.AgentProfile) TaskExecutorOption {
	return func(e *taskExecutor) {
		e.profile = p
	}
}

// profileCtx applies the executor's role into the task context. No-op when no
// profile is configured.
func (e *taskExecutor) profileCtx(ctx context.Context) context.Context {
	return agents.WithProfile(ctx, e.profile)
}

// WithChatClient returns a TaskExecutorOption that enables native tool calling
// via the Chat API. When set, the executor will pass tool definitions to the LLM
// and handle tool_calls in a loop until the LLM returns a final text response.
func WithChatClient(client ChatClient) TaskExecutorOption {
	return func(e *taskExecutor) {
		e.chatClient = client
	}
}

// WithMaxToolRounds sets the maximum number of tool-calling iterations.
// Defaults to 5 if not set. A value of 0 means no tool calling.
func WithMaxToolRounds(n int) TaskExecutorOption {
	return func(e *taskExecutor) {
		e.maxToolRounds = n
	}
}

// WithStrategySource returns a TaskExecutorOption that injects a live
// evolution StrategySource. When set, the active strategy can override the
// prompt template and supply per-call LLM parameter overrides (temperature,
// max_tokens, top_k) so the running agent is steered by the GA at runtime.
func WithStrategySource(src agents.StrategySource) TaskExecutorOption {
	return func(e *taskExecutor) {
		e.strategySource = src
	}
}

// NewTaskExecutor creates a new TaskExecutor with LLM support.
func NewTaskExecutor(
	toolBinder ToolBinder,
	llmAdapter output.LLMAdapter,
	template *output.TemplateEngine,
	promptTpl string,
	validator *output.Validator,
	maxRetries int,
	opts ...TaskExecutorOption,
) TaskExecutor {
	return NewTaskExecutorWithValidation(toolBinder, llmAdapter, template, promptTpl, validator, maxRetries, false, false, opts...)
}

// NewTaskExecutorWithValidation creates a new TaskExecutor with validation config.
func NewTaskExecutorWithValidation(
	toolBinder ToolBinder,
	llmAdapter output.LLMAdapter,
	template *output.TemplateEngine,
	promptTpl string,
	validator *output.Validator,
	maxRetries int,
	retryOnFail bool,
	strictMode bool,
	opts ...TaskExecutorOption,
) TaskExecutor {
	if maxRetries <= 0 {
		maxRetries = 3
	}
	e := &taskExecutor{
		toolBinder:    toolBinder,
		llmAdapter:    llmAdapter,
		template:      template,
		promptTpl:     promptTpl,
		validator:     validator,
		maxRetries:    maxRetries,
		retryOnFail:   retryOnFail,
		strictMode:    strictMode,
		maxToolRounds: defaultMaxToolRounds,
		logger:        slog.Default(),
	}
	e.fallbackHandlers = make(map[models.AgentType]FallbackHandler)
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// RegisterFallback registers a type-specific fallback handler used when
// the LLM is unavailable or execution fails. If no handler is registered
// for an agent type, executeByType returns an empty result with a warning
// instead of erroring out.
func (e *taskExecutor) RegisterFallback(agentType models.AgentType, handler FallbackHandler) {
	if handler == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fallbackHandlers == nil {
		e.fallbackHandlers = make(map[models.AgentType]FallbackHandler)
	}
	e.fallbackHandlers[agentType] = handler
}

// SetEventStore configures the executor to emit ares_events for tool/LLM calls.
func (e *taskExecutor) SetEventStore(store ares_events.EventStore, agentID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.eventStore = store
	e.agentID = agentID
}

// SetCallbacks configures the callback emitter for lifecycle event emission.
func (e *taskExecutor) SetCallbacks(emitter ares_callbacks.Emitter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ares_callbacks = emitter
}

// emitCallback emits a lifecycle callback event if the emitter is set.
func (e *taskExecutor) emitCallback(ctx *ares_callbacks.Context) {
	e.mu.RLock()
	emitter := e.ares_callbacks
	e.mu.RUnlock()
	if emitter == nil {
		return
	}
	emitter.Emit(ctx)
}

// emitEvent appends a single event using the canonical ares_events.Emit helper.
// No-op if eventStore is nil.
func (e *taskExecutor) emitEvent(ctx context.Context, eventType ares_events.EventType, payload map[string]any) {
	e.mu.RLock()
	store, agentID := e.eventStore, e.agentID
	e.mu.RUnlock()
	if !ares_events.Emit(ctx, store, agentID, eventType, "sub", payload) {
		log.Warn("failed to emit event", "event_type", eventType, "stream_id", agentID)
	}
}

// toolErrorMessage normalizes a tool execution error into the C1 event
// contract's error field (the same contract chat_cognition.go uses). nil ->
// "" so the unified payload is JSON-friendly and the trajectory projection
// can distinguish "failed with message" from "no error".
func toolErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// emitSubTaskResult publishes a sub_task.result event when a task completes,
// so consumers (SkillOutcomeRecorder, experience distillation) observe the
// outcome. No-op when the event store is nil or the task carries no id.
func (e *taskExecutor) emitSubTaskResult(ctx context.Context, task *models.Task, result *models.TaskResult) {
	if task == nil || task.TaskID == "" || result == nil {
		return
	}
	status := "success"
	if !result.Success || result.Error != "" {
		status = "failure"
	}
	// Snapshot the agent ID under the read lock. emitSubTaskResult runs on every
	// completion path (including the deferred one in Execute), and its payload is
	// built before emitEvent takes the lock, so a direct e.agentID read here can
	// race with the write in SetEventStore (same class as the D9 fix).
	e.mu.RLock()
	agentID := e.agentID
	e.mu.RUnlock()
	e.emitEvent(ctx, ares_events.EventSubTaskResult, map[string]any{
		"task_id":     task.TaskID,
		"agent_id":    agentID,
		"status":      status,
		"capability":  string(task.AgentType),
		"result":      result.Reason,
		"duration_ms": result.Duration.Milliseconds(),
	})
}

// Execute executes a task and returns result.
func (e *taskExecutor) Execute(ctx context.Context, task *models.Task) (*models.TaskResult, error) {
	result := models.NewTaskResult("", models.AgentTypeTop)
	if task == nil {
		result.SetError(errors.ErrInvalidInput.Error())
		return result, nil
	}

	// W2: emit the sub_task.result event on every completion path so skill
	// outcome recording and experience distillation observe the outcome.
	defer func() {
		e.emitSubTaskResult(ctx, task, result)
	}()

	// W4 write side: pin the agent role into the task context so the prompt
	// pipeline (activeRoleInstructions) reads the role instructions.
	ctx = e.profileCtx(ctx)

	// Snapshot the agent ID under the read lock so concurrent SetEventStore
	// calls cannot race with the emit paths below (D9: data-race fix).
	e.mu.RLock()
	agentID := e.agentID
	e.mu.RUnlock()

	result = models.NewTaskResult(task.TaskID, task.AgentType)
	startTime := time.Now()

	// Emit tool start event.
	e.emitCallback(&ares_callbacks.Context{
		Event:   ares_callbacks.EventToolStart,
		AgentID: agentID,
		Input:   task.TaskID,
	})

	// If no LLM adapter, use fallback execution
	if e.llmAdapter == nil {
		items, reason, err := e.executeByType(ctx, task)
		if err != nil {
			result.SetError(err.Error())
			e.emitCallback(&ares_callbacks.Context{
				Event:    ares_callbacks.EventToolError,
				AgentID:  agentID,
				Error:    err,
				Duration: time.Since(startTime),
			})
			return result, nil
		}
		result.SetSuccess(items, reason)
		result.Duration = time.Since(startTime)
		e.emitCallback(&ares_callbacks.Context{
			Event:    ares_callbacks.EventToolEnd,
			AgentID:  agentID,
			Duration: time.Since(startTime),
		})
		return result, nil
	}

	// Get profile from task (either from UserProfile field or Payload)
	var profile *models.UserProfile
	if task.UserProfile != nil {
		profile = task.UserProfile
	} else if task.Payload != nil {
		if p, ok := task.Payload["profile"].(*models.UserProfile); ok {
			profile = p
		}
	}

	if profile == nil {
		// Fallback to type-specific execution
		items, reason, err := e.executeByType(ctx, task)
		if err != nil {
			result.SetError(err.Error())
			e.emitCallback(&ares_callbacks.Context{
				Event:    ares_callbacks.EventToolError,
				AgentID:  agentID,
				Error:    err,
				Duration: time.Since(startTime),
			})
			return result, nil
		}
		result.SetSuccess(items, reason)
		result.Duration = time.Since(startTime)
		e.emitCallback(&ares_callbacks.Context{
			Event:    ares_callbacks.EventToolEnd,
			AgentID:  agentID,
			Duration: time.Since(startTime),
		})
		return result, nil
	}

	// Execute LLM-based recommendation
	items, err := e.executeWithLLM(ctx, task, profile)
	if err != nil {
		log.Debug("LLM execution failed, using fallback", "error", err)
		// Fallback to type-specific execution
		fallbackItems, reason, fallbackErr := e.executeByType(ctx, task)
		if fallbackErr != nil {
			log.Debug("Fallback also failed", "error", fallbackErr)
			result.SetError(err.Error())
			e.emitCallback(&ares_callbacks.Context{
				Event:    ares_callbacks.EventToolError,
				AgentID:  agentID,
				Error:    err,
				Duration: time.Since(startTime),
			})
			return result, nil
		}
		log.Debug("Using fallback", "item_count", len(fallbackItems))
		result.SetSuccess(fallbackItems, reason)
		result.Duration = time.Since(startTime)
		e.emitCallback(&ares_callbacks.Context{
			Event:    ares_callbacks.EventToolEnd,
			AgentID:  agentID,
			Duration: time.Since(startTime),
		})
		return result, nil
	}

	result.SetSuccess(items, "LLM recommendation completed")
	result.Duration = time.Since(startTime)
	e.emitCallback(&ares_callbacks.Context{
		Event:    ares_callbacks.EventToolEnd,
		AgentID:  agentID,
		Duration: time.Since(startTime),
	})
	return result, nil
}

// ExecuteStep runs exactly one execution quantum (plan P1.1 Execution
// Quantum) and returns its outcome. A quantum is one ReAct round of the
// tool-calling loop (reason → tool call → observation) or — for executors
// without a chat client / tools — the entire fallback or text-only execution
// in a single step. The resumable state (chatStepState) rides in
// task.Payload["checkpoint"]: the scheduler stores it in the fabric's opaque
// checkpoint slot on yield and re-surfaces it here on resume, so the executor
// itself stays stateless between quanta and can run different tasks
// concurrently.
//
// The kernel scheduler calls this via sub.Agent.ExecuteStep inside
// taskfabric.RunQuantum (Done→COMPLETED, !Done→SUSPENDED, Err→FAIL/requeue).
// Execute remains the run-to-completion entry for the message-driven path.
func (e *taskExecutor) ExecuteStep(ctx context.Context, task *models.Task) (*StepOutcome, error) {
	if task == nil {
		return nil, errors.ErrInvalidInput
	}

	// W4 write side: pin the agent role into the quantum context.
	ctx = e.profileCtx(ctx)

	// Snapshot agent ID for the duration of this quantum (D9: data-race fix).
	e.mu.RLock()
	agentID := e.agentID
	e.mu.RUnlock()

	result := models.NewTaskResult(task.TaskID, task.AgentType)
	start := time.Now()

	// Decode the resume checkpoint first: its presence marks a resumed quantum,
	// and the tool-start lifecycle event must fire only on the first one
	// (end/error fire on the final quantum — the same shape as Execute's run).
	st, found, err := e.decodeChatStepState(task)
	if err != nil {
		return nil, err
	}
	if !found {
		e.emitCallback(&ares_callbacks.Context{
			Event:   ares_callbacks.EventToolStart,
			AgentID: agentID,
			Input:   task.TaskID,
		})
	}

	// Non-LLM and profile-less paths are single-quantum by construction.
	if e.llmAdapter == nil || profileFromTask(task) == nil {
		return e.singleQuantum(ctx, task, result, start, agentID, func() ([]*models.RecommendItem, string, error) {
			return e.executeByType(ctx, task)
		})
	}

	// LLM path: resume a yielded tool loop or start a fresh one.
	chatAvailable := e.chatClient != nil && e.toolBinder != nil && len(e.toolBinder.GetToolSchemas()) > 0
	if st == nil {
		prompt, params, err := e.renderPromptAndParams(ctx, task, profileFromTask(task))
		if err != nil {
			return nil, err
		}
		if !chatAvailable {
			// Text-only: a single LLM call completes the task in one quantum.
			items, err := e.executeWithLLMTextOnly(ctx, prompt, params)
			if err != nil {
				return nil, err
			}
			result.SetSuccess(items, "LLM recommendation completed")
			result.Duration = time.Since(start)
			e.emitCallback(&ares_callbacks.Context{Event: ares_callbacks.EventToolEnd, AgentID: agentID, Duration: time.Since(start)})
			e.emitSubTaskResult(ctx, task, result)
			return &StepOutcome{Done: true, Result: result}, nil
		}
		st = &chatStepState{
			SchemaVersion: stepSchemaVersion,
			TaskID:        task.TaskID,
			MaxRounds:     e.toolRounds(),
			Prompt:        prompt,
			Params:        params,
			Messages:      []*core.LLMMessage{{Role: "user", Content: prompt}},
		}
	} else if !chatAvailable {
		// A resumed checkpoint needs the same executor shape that produced it.
		// Missing chat/tools wiring mid-task is a config error, not a retryable
		// LLM failure — surface it instead of silently restarting.
		return nil, fmt.Errorf("sub: resumed step requires chat executor (chat_client=%v tool_binder=%v)", e.chatClient != nil, e.toolBinder != nil)
	}

	// One ReAct round — or a text-only degradation when the round budget is
	// already spent (the previous quantum ran the last tool round).
	var items []*models.RecommendItem
	var done bool
	if st.Round >= st.MaxRounds {
		log.Warn("Chat API tool loop exceeded max rounds, degrading to text-only",
			"max_rounds", st.MaxRounds,
			"msg_count", len(st.Messages),
		)
		items, err = e.executeWithLLMTextOnly(ctx, st.Prompt, st.Params)
		done = true
	} else {
		items, done, err = e.chatStep(ctx, st)
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
	e.emitCallback(&ares_callbacks.Context{Event: ares_callbacks.EventToolEnd, AgentID: agentID, Duration: time.Since(start)})
	e.emitSubTaskResult(ctx, task, result)
	return &StepOutcome{Done: true, Result: result}, nil
}

// singleQuantum wraps a one-shot (non-yieldable) execution path — the
// type-specific fallback — in a completed StepOutcome with the same event and
// result shape as Execute's tail.
func (e *taskExecutor) singleQuantum(ctx context.Context, task *models.Task, result *models.TaskResult, start time.Time, agentID string, run func() ([]*models.RecommendItem, string, error)) (*StepOutcome, error) {
	items, reason, err := run()
	if err != nil {
		result.SetError(err.Error())
		e.emitCallback(&ares_callbacks.Context{
			Event:    ares_callbacks.EventToolError,
			AgentID:  agentID,
			Error:    err,
			Duration: time.Since(start),
		})
		e.emitSubTaskResult(ctx, task, result)
		return &StepOutcome{Done: true, Result: result}, nil
	}
	result.SetSuccess(items, reason)
	result.Duration = time.Since(start)
	e.emitCallback(&ares_callbacks.Context{
		Event:    ares_callbacks.EventToolEnd,
		AgentID:  agentID,
		Duration: time.Since(start),
	})
	e.emitSubTaskResult(ctx, task, result)
	return &StepOutcome{Done: true, Result: result}, nil
}

// decodeChatStepState restores a resumable chatStepState from a task's
// payload["checkpoint"] entry — what the scheduler writes on yield (fabric
// checkpoint → fabricTaskMeta.StepCheckpoint → payload["checkpoint"]) and
// surfaces on resume. It always decodes into a fresh copy via JSON so the
// fabric's stored checkpoint is never mutated across quanta (code_rules
// the checkpoint a quantum returns is final; the next quantum works on
// its own copy). Resume is refused when the checkpoint belongs to another task
// (resume identity mismatch) or carries an unknown schema version
// (checkpoint protocol guards, see taskfabric/checkpoint_schema.go).
//
// Returns:
//
//	(nil, false, nil)  — no checkpoint entry; the caller starts fresh
//	(&st, true, nil)   — a valid resumable checkpoint
//	(nil, _, err)      — the checkpoint is present but cannot be resumed
func (e *taskExecutor) decodeChatStepState(task *models.Task) (*chatStepState, bool, error) {
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
		return nil, false, fmt.Errorf("sub: step checkpoint schema version %d unsupported (want %d)", st.SchemaVersion, stepSchemaVersion)
	}
	if st.TaskID != task.TaskID {
		return nil, false, fmt.Errorf("sub: step checkpoint for task %q does not match task %q, refusing resume", st.TaskID, task.TaskID)
	}
	return &st, true, nil
}

// toolRounds returns the configured tool-loop round budget (0 → default 5).
func (e *taskExecutor) toolRounds() int {
	if e.maxToolRounds <= 0 {
		return defaultMaxToolRounds
	}
	return e.maxToolRounds
}

// profileFromTask extracts the UserProfile from a task's direct field or its
// payload (the "profile" entry), matching Execute's lookup so the quantum path
// takes the same LLM-vs-fallback decision as the full-run path.
func profileFromTask(task *models.Task) *models.UserProfile {
	if task.UserProfile != nil {
		return task.UserProfile
	}
	if task.Payload != nil {
		if p, ok := task.Payload["profile"].(*models.UserProfile); ok {
			return p
		}
	}
	return nil
}

func (e *taskExecutor) executeWithLLM(ctx context.Context, task *models.Task, profile *models.UserProfile) ([]*models.RecommendItem, error) {
	var lastErr error
	for attempt := 0; attempt < e.maxRetries; attempt++ {
		if attempt > 0 {
			if nonIdempotent := e.listNonIdempotentTools(); len(nonIdempotent) > 0 {
				log.Error("LLM retry blocked: non-idempotent tools may have been called",
					"attempt", attempt+1,
					"max_retries", e.maxRetries,
					"tools", nonIdempotent,
				)
				return nil, errors.Wrap(lastErr, "retry aborted: non-idempotent tools may have been called")
			}
		}

		items, err := e.executeWithLLMSingle(ctx, task, profile)
		if err != nil {
			lastErr = err
			log.Error("LLM call failed", "attempt", attempt+1, "error", err)
			continue
		}

		// Validate results using validator
		if e.validator != nil {
			if err := e.validator.ValidateRecommendResult(&models.RecommendResult{Items: items}); err != nil {
				log.Debug("Validation failed", "error", err)
				// Retry if enabled and not already at max retries
				if e.retryOnFail && attempt < e.maxRetries-1 {
					log.Debug("Will retry LLM call", "next_attempt", attempt+2, "max_retries", e.maxRetries)
					continue
				}
				// Strict mode: return error
				if e.strictMode {
					return nil, errors.Wrap(err, "validation failed")
				}
				// Non-strict mode: log and continue with whatever we got
				log.Debug("Continuing with unvalidated result", "strict_mode", false)
			} else {
				log.Debug("Validation passed")
			}
		}

		log.Info("Got items from LLM", "count", len(items))
		return items, nil
	}

	return nil, errors.Wrap(lastErr, "all retries failed")
}

// activeRoleInstructions returns the system instructions of the role that the
// leader switched to via Handoff (see agents.GetFromContext), or an empty
// string when no role is active in the context.
func activeRoleInstructions(ctx context.Context) string {
	profile := agents.GetFromContext(ctx)
	if profile == nil {
		return ""
	}
	return profile.Instructions
}

// executeWithLLMSingle renders the prompt for a single LLM call and routes it
// through the Chat+tools path (when a chat client and tools are available) or
// the text-only path. It is the full-run (Execute) entry of the LLM pipeline;
// ExecuteStep shares the same rendering via renderPromptAndParams.
func (e *taskExecutor) executeWithLLMSingle(ctx context.Context, task *models.Task, profile *models.UserProfile) ([]*models.RecommendItem, error) {
	prompt, params, err := e.renderPromptAndParams(ctx, task, profile)
	if err != nil {
		return nil, err
	}

	// Try Chat API with tool support when chatClient is available and the
	// executor has registered tools. The gate is the FULL registered tool set
	// (ListTools), not the LLM-advertised subset (GetToolSchemas): the
	// active-tools filter (progressive disclosure) only narrows which schemas
	// reach the model — it must not flip the executor into text-only mode and
	// leave plan_tasks with available_tools=0 (run log
	// scheduler_trace_with_logs.log). executeWithChatAndTools degrades
	// internally if the advertised set is empty.
	if e.chatClient != nil && e.toolBinder != nil && len(e.toolBinder.ListTools()) > 0 {
		return e.executeWithChatAndTools(ctx, prompt, params)
	}

	// Fall back to text-only generation.
	return e.executeWithLLMTextOnly(ctx, prompt, params)
}

// renderPromptAndParams renders the worker prompt (with the active evolution
// strategy's template/param overrides and the active role's instructions) and
// the per-call LLM params. Shared by the full-run path (executeWithLLMSingle)
// and the quantum path (ExecuteStep) so a first quantum renders exactly what a
// full run would.
func (e *taskExecutor) renderPromptAndParams(ctx context.Context, task *models.Task, profile *models.UserProfile) (string, map[string]any, error) {
	tpl := e.promptTpl
	params := map[string]any{}
	if st := e.activeStrategy(ctx); st != nil {
		if st.Prompt != "" {
			tpl = st.Prompt
		}
		for k, v := range st.Params {
			params[k] = v
		}
	}

	// C5: overlay node-level ToolStep attributes (tools/budget/prior) from the
	// task payload onto the global strategy params, with NODE OVER GLOBAL
	// priority (§8.5). Mirrors chat_cognition.go so both peer executors honour
	// the same node-level tool-selection knob.
	params = agents.MergeNodeParams(params, task.Payload)

	prompt, err := e.renderPrompt(tpl, task, profile)
	if err != nil {
		return "", nil, err
	}

	// C5: prior is prompt-only — biases tool choice, never restricts the
	// advertised tool set (mirrors chat_cognition.go).
	prompt = agents.ApplyPriorHint(prompt, agents.PriorHintFromParams(params))

	// P0-3: prepend the active role's system instructions when the leader
	// switched this execution to a specialized role via Handoff (Ch.10
	// multi-stage role transition: same runtime, different role instructions).
	if instructions := activeRoleInstructions(ctx); instructions != "" {
		prompt = instructions + "\n\n" + prompt
	}
	log.Debug("Generated prompt", "preview", prompt[:min(200, len(prompt))])
	return prompt, params, nil
}

// renderPrompt renders the worker prompt template with the task and profile
// data. It fails fast on an empty result: an empty user message is rejected
// by OpenAI-compatible providers with a 400 ("message content cannot be
// empty"), which used to trip the failover chain and burn the cooldown (20s)
// on every worker call. A missing template or a template that references
// unset keys is a wiring error, not a retryable LLM failure.
func (e *taskExecutor) renderPrompt(tpl string, task *models.Task, profile *models.UserProfile) (string, error) {
	// Render prompt - support generic profile fields.
	// Use lowercase keys to match template's {{index . "key"}} syntax.
	promptData := map[string]any{
		"Category": string(task.AgentType), // Uppercase to match template
	}

	// Carry the original task input into the template as {{.input}}: the
	// planner writes it to the task payload as task_desc, so a config that
	// omits prompts.recommendation (or a template that references .input)
	// still renders a prompt that contains the actual task. Without this the
	// rendered prompt was empty and every worker LLM call 400'd on empty
	// user content (see DefaultRecommendationPrompt).
	if task.Payload != nil {
		if desc, ok := task.Payload["task_desc"].(string); ok && desc != "" {
			promptData["input"] = desc
		}
	}
	if _, ok := promptData["input"]; !ok {
		promptData["input"] = string(task.AgentType)
	}

	// Check if this is a travel request - use Preferences map
	if profile != nil && len(profile.Preferences) > 0 {
		// Copy all preferences to promptData (lowercase keys)
		for k, v := range profile.Preferences {
			promptData[k] = v
		}
	}

	// Include budget from profile.Budget for backward compatibility.
	if profile != nil && profile.Budget != nil {
		promptData["budget"] = formatBudget(profile.Budget)
	}

	// Also set style from profile
	if profile != nil && len(profile.Style) > 0 {
		promptData["style"] = profile.Style
	}

	prompt, err := e.template.Render(tpl, promptData)
	if err != nil {
		return "", errors.Wrap(err, "render prompt")
	}

	// Fail fast on an empty prompt (see doc comment above).
	if strings.TrimSpace(prompt) == "" {
		return "", fmt.Errorf("render prompt: empty prompt (template=%q)", tpl)
	}
	return prompt, nil
}

// activeStrategy fetches the currently-deployed evolution strategy, if any.
// Errors are logged and ignored so a missing store never breaks execution.
func (e *taskExecutor) activeStrategy(ctx context.Context) *agents.ActiveStrategy {
	if e.strategySource == nil {
		return nil
	}
	st, err := e.strategySource.GetActiveStrategy(ctx)
	if err != nil {
		log.Warn("failed to read active strategy", "error", err)
		return nil
	}
	return st
}

// executeWithChatAndTools uses the Chat API with native tool calling. It runs
// the agentic loop to completion in-process: LLM → tool_calls → execute →
// result → LLM → final answer. It drives the same chatStep primitive as the
// quantum entry (ExecuteStep) — the only difference is how many rounds run in
// one call: all of them here, exactly one per fabric quantum there
// (code_rules: one loop body, two entry points).
func (e *taskExecutor) executeWithChatAndTools(ctx context.Context, prompt string, params map[string]any) ([]*models.RecommendItem, error) {
	maxRounds := e.toolRounds()
	st := &chatStepState{
		SchemaVersion: stepSchemaVersion,
		Round:         0,
		MaxRounds:     maxRounds,
		Prompt:        prompt,
		Params:        params,
		Messages:      []*core.LLMMessage{{Role: "user", Content: prompt}},
	}
	// Run rounds while the budget lasts; once spent without a final answer,
	// degrade to a plain text-only call below.
	for st.Round < st.MaxRounds {
		items, done, err := e.chatStep(ctx, st)
		if err != nil {
			return nil, err
		}
		if done {
			return items, nil
		}
	}

	// The agentic loop did not converge on a final text answer. Degrade
	// gracefully to a plain text-only call with the original prompt instead
	// of failing the task: tool-calling loops that never terminate are a
	// model-behavior hazard (the worker exposes ~20 tools), not evidence the
	// task cannot be answered. The text-only path reuses the same prompt and
	// params, so the model produces a direct answer without tool pressure.
	log.Warn("Chat API tool loop exceeded max rounds, degrading to text-only",
		"max_rounds", maxRounds,
		"msg_count", len(st.Messages),
	)
	if e.llmAdapter == nil {
		return nil, fmt.Errorf("exceeded max tool rounds (%d) without final answer and no text-only adapter", maxRounds)
	}
	return e.executeWithLLMTextOnly(ctx, prompt, params)
}

// chatStep advances a tool-calling execution by exactly one ReAct round: one
// Chat API call, every requested tool call executed, and the resulting
// messages appended to the state. It is the single loop body of the executor
// code_rules: the full-run path and the quantum path both drive it.
//
// Contract: the caller has verified st.Round < st.MaxRounds (the round budget
// is enforced by the loop, not by this method).
//
// Returns:
//
//	items, true, nil  — the LLM answered with a final text result (done)
//	nil, false, nil   — tool calls were executed; st carries the resumable
//	                    state for the next round / quantum
//	nil, false, err   — a hard failure (LLM or tool execution error)
func (e *taskExecutor) chatStep(ctx context.Context, st *chatStepState) ([]*models.RecommendItem, bool, error) {
	// Snapshot agent ID for thread-safe event emission (D9: data-race fix).
	e.mu.RLock()
	agentID := e.agentID
	e.mu.RUnlock()

	schemas := e.toolBinder.GetToolSchemas()

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
		// set rather than degrade to zero tools (see chat_cognition.go for the
		// full rationale).
		if len(filtered) == 0 {
			// Package logger, not e.logger: the executor is constructible with a
			// nil logger (quantum-path fixtures do exactly that), and a guard
			// must never be the thing that panics.
			log.Warn("tool whitelist matched no registered tools; falling back to full set",
				"whitelist", whitelist, "registered", len(schemas))
			filtered = schemas
		}
		schemas = filtered
	}

	// C5 budget gate: mirrors agentfabric/chat_cognition.go — tools whose
	// per-session budget is spent are removed from the advertised set, with the
	// same "never advertise zero tools" fallback.
	if budget := agents.ToolBudgetFromParams(st.Params); budget > 0 {
		allowed := make([]resources.ToolSchema, 0, len(schemas))
		for _, s := range schemas {
			if agents.ToolAllowedByBudget(s.Name, st.ToolUses, budget) {
				allowed = append(allowed, s)
			}
		}
		if len(allowed) == 0 {
			log.Warn("tool budget exhausted for every advertised tool; falling back to full set",
				"budget", budget, "uses", st.ToolUses, "advertised", len(schemas))
		} else {
			schemas = allowed
		}
	}

	llmTools := make([]core.Tool, 0, len(schemas))
	for _, s := range schemas {
		llmTools = append(llmTools, resources.ToolSchemaToLLMTool(s))
	}

	e.emitEvent(ctx, ares_events.EventLLMCall, map[string]any{
		KeyAgentID:   agentID,
		"round":      st.Round + 1,
		"max_rounds": st.MaxRounds,
		"tool_count": len(llmTools),
		"msg_count":  len(st.Messages),
	})

	resp, err := e.chatClient.Chat(ctx, st.Messages, llmTools, st.Params)
	if err != nil {
		return nil, false, errors.Wrap(err, "chat API call failed")
	}

	// No tool calls: LLM gave a final text answer.
	if len(resp.ToolCalls) == 0 {
		log.Debug("Chat API returned final text", "round", st.Round+1, "content_len", len(resp.Content))
		items, err := e.parseRecommendResult(resp.Content)
		return items, true, err
	}

	// Append the assistant message with its tool calls, then execute each call
	// and append its observation as a tool message — the conversation grows
	// exactly as the pre-quantum implementation did, so a resumed round sees
	// the accumulated context.
	log.Debug("Chat API returned tool calls", "round", st.Round+1, "count", len(resp.ToolCalls))
	st.Messages = append(st.Messages, &core.LLMMessage{
		Role:      "assistant",
		Content:   resp.Content,
		ToolCalls: resp.ToolCalls,
	})
	for seq, tc := range resp.ToolCalls {
		// C5 intra-round enforcement (mirrors agentfabric/chat_cognition.go):
		// the schema gate above runs once per round, but a single round can
		// carry N calls to the same tool. An over-budget call is skipped —
		// never executed, never counted — with the skip reported as its tool
		// observation (paired reply preserved) and no tool events emitted
		// (the call never ran: no success/failure signal exists to project
		// or score).
		if budget := agents.ToolBudgetFromParams(st.Params); budget > 0 &&
			!agents.ToolAllowedByBudget(tc.Function.Name, st.ToolUses, budget) {
			st.Messages = append(st.Messages, &core.LLMMessage{
				Role:       "tool",
				Content:    fmt.Sprintf("tool %s skipped: per-session budget (%d) exhausted", tc.Function.Name, budget),
				ToolCallID: tc.ID,
			})
			continue
		}
		// C5: count the call against the node budget BEFORE executing, so a
		// failing tool still spends its budget (mirrors chat_cognition.go).
		if st.ToolUses == nil {
			st.ToolUses = map[string]int{}
		}
		st.ToolUses[tc.Function.Name]++

		e.emitEvent(ctx, ares_events.EventToolCallStarted, map[string]any{
			ares_events.EventKeyAgentID:    agentID,
			ares_events.EventKeyToolName:   tc.Function.Name,
			ares_events.EventKeyToolCallID: tc.ID,
			ares_events.EventKeyRound:      st.Round,
			ares_events.EventKeySeq:        seq,
		})

		result, err := e.executeToolCall(ctx, tc, agentID)
		success := err == nil
		if err != nil {
			log.Warn("tool execution failed", "tool", tc.Function.Name, "error", err)
			result = fmt.Sprintf("error: %s", err.Error())
		}

		// C1: unified tool-call completed contract (round/seq/success/error/
		// arg_shape). Mirrors chat_cognition.go so the trajectory projection
		// (Y1 C2) reads the same keys from both production executors.
		e.emitEvent(ctx, ares_events.EventToolCallCompleted, ares_events.ToolCompletedPayload{
			AgentID:     agentID,
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
			Role:       "tool",
			Content:    result,
			ToolCallID: tc.ID,
		})
	}
	st.Round++
	return nil, false, nil
}

// executeToolCall parses arguments and calls the tool via toolBinder.
func (e *taskExecutor) executeToolCall(ctx context.Context, tc core.ToolCall, agentID string) (string, error) {
	var args map[string]any
	if tc.Function.Arguments != "" {
		if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
			return "", errors.Wrap(err, "parse tool arguments")
		}
	}

	// Stamp the caller identity into the tool context BEFORE invoking the
	// tool, so Kernel syscalls (agentsyscall) can enforce provenance
	// (Task.Origin / ParentID) from the context, never from LLM args.
	result, err := e.toolBinder.CallTool(kctx.WithCallerID(ctx, agentID), tc.Function.Name, args)
	if err != nil {
		return "", err
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Sprintf("%v", result), nil
	}
	return string(resultJSON), nil
}

// executeWithLLMTextOnly performs a text-only LLM generation (original behavior).
// params carries optional per-call LLM overrides from the active strategy.
func (e *taskExecutor) executeWithLLMTextOnly(ctx context.Context, prompt string, params map[string]any) ([]*models.RecommendItem, error) {
	// Snapshot agent ID for thread-safe event emission (D9: data-race fix).
	e.mu.RLock()
	agentID := e.agentID
	e.mu.RUnlock()

	e.emitEvent(ctx, ares_events.EventLLMCall, map[string]any{
		KeyAgentID: agentID,
		"prompt":   prompt[:min(200, len(prompt))],
	})
	response, err := e.llmAdapter.GenerateWithParams(ctx, prompt, params)
	if err != nil {
		e.emitEvent(ctx, ares_events.EventLLMCall, map[string]any{
			KeyAgentID: agentID,
			KeyError:   err.Error(),
			KeyStatus:  "failed",
		})
		return nil, errors.Wrap(err, "LLM call failed")
	}
	log.Debug("LLM response", "preview", response[:min(500, len(response))])

	return e.parseRecommendResult(response)
}

// parseRecommendResult parses the LLM text response into RecommendItems.
func (e *taskExecutor) parseRecommendResult(response string) ([]*models.RecommendItem, error) {
	parser := output.NewParser()
	result, err := parser.ParseRecommendResult(response)
	if err == nil && result != nil && result.Items != nil {
		log.Info("Parsed result items", "count", len(result.Items))
		return result.Items, nil
	}

	// Strict parse failed: the model answered in prose (general-purpose
	// tasks: architecture analysis, code review, …). Wrap the text in a
	// single RecommendItem (Content carries it) so a real answer becomes a
	// real result instead of a "invalid JSON" task failure — the last no-op
	// gap in the serve path. Only a truly empty answer is an error.
	if trimmed := strings.TrimSpace(response); trimmed != "" {
		log.Debug("Strict parse failed, wrapping prose", "error", err)
		e.mu.RLock()
		agentID := e.agentID
		e.mu.RUnlock()
		item := &models.RecommendItem{
			ItemID:      fmt.Sprintf("prose-%s-%d", agentID, time.Now().UnixNano()),
			Category:    "general",
			Name:        "Agent output",
			Description: trimmed[:min(500, len(trimmed))],
			Content:     trimmed,
		}
		return []*models.RecommendItem{item}, nil
	}

	return nil, errors.Wrap(err, "parse result")
}

func formatBudget(budget *models.PriceRange) string {
	if budget == nil {
		return "0 - 10000"
	}
	return fmt.Sprintf("%.0f - %.0f", budget.Min, budget.Max)
}

// listNonIdempotentTools returns names of non-idempotent tools bound to this executor.
func (e *taskExecutor) listNonIdempotentTools() []string {
	var names []string
	if e.toolBinder == nil {
		return nil
	}
	all := e.toolBinder.ListTools()
	for _, n := range all {
		if !e.toolBinder.IsToolIdempotent(n) {
			names = append(names, n)
		}
	}
	return names
}

// executeByType dispatches to type-specific handlers.
// If no handler is registered for the agent type, returns an empty result
// with a warning (graceful degradation instead of hard error).
func (e *taskExecutor) executeByType(ctx context.Context, task *models.Task) ([]*models.RecommendItem, string, error) {
	e.mu.RLock()
	handler, ok := e.fallbackHandlers[task.AgentType]
	e.mu.RUnlock()
	if ok {
		log.Debug("executeByType: using registered fallback", "agent_type", task.AgentType)
		return handler(ctx, task)
	}
	log.Warn("executeByType: no fallback handler registered",
		"agent_type", task.AgentType,
		"task_id", task.TaskID,
	)
	return []*models.RecommendItem{}, "fallback: empty result (no handler)", nil
}
