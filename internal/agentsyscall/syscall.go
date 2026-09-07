package agentsyscall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/core/models"
	kctx "github.com/Timwood0x10/ares/internal/kernel/ctx"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// SpawnAgentTool is the tool name exposed to the LLM for spawning peer agents.
const SpawnAgentTool = "spawn_agent"

// CreateTaskTool is the tool name exposed to the LLM for creating sub-tasks.
const CreateTaskTool = "create_task"

// AskAgentTool is the tool name exposed to the LLM for asking a target agent a
// question (Step Y.2-ACT). Unlike spawn_agent (create a new peer) and
// create_task (decompose into the task fabric), ask_agent turns "which agent to
// ask" into an agent-visible, changeable decision — the ACT half of the
// collaboration channel. The Kernel forwards it to a collabatation IPC
// primitive injected at serve time (ipc.Send), which lands in the existing
// "collaboration" feedback source via the CollaborationObserver.
const AskAgentTool = "ask_agent"

// goconst: reuse the field keys across syscall schemas. The values originated
// in the schema objects below; hoisting them keeps the ≥3-repetition rule of
// code_rules_v2 §8.1 satisfied.
const (
	paramTopic   = "topic"
	paramTo      = "to"
	paramPayload = "payload"
)

// goconst: these strings appear ≥3 times in ToolSchemas,
const (
	paramType        = "type"
	paramTypeString  = "string"
	paramTypeObject  = "object"
	paramTypeArray   = "array"
	paramTypeInteger = "integer"
	paramDescription = "description"
	paramCapability  = "capability"

	// Schema object keys reused across tool schemas (goconst).
	paramProperties = "properties"
	paramItems      = "items"
	paramRequired   = "required"
)

// ExecutorFactory creates a sub.Agent executor for a dynamically spawned agent.
// The factory is injected by the serve wiring so spawned agents get the same
// LLM + tool capabilities as configured agents.
type ExecutorFactory func(agentID, capability string) Executor

// Executor is the minimal contract a spawned agent's executor must satisfy.
// In production this is sub.Agent; the interface keeps this package decoupled
// from the sub package (code_rules: interface at the consumer).
type Executor interface {
	ID() string
	Type() models.AgentType
	ExecuteStep(ctx context.Context, task *models.Task) (*StepOutcome, error)
}

// StepOutcome mirrors sub.StepOutcome to avoid a circular dependency.
type StepOutcome struct {
	Done       bool
	Checkpoint any
	Result     *models.TaskResult
}

// cognitionFunc adapts an Executor to the agentfabric.Cognition contract
// (aresos-agentos-plan C1: spawn 的 agent 带执行体). It converts the syscall
// StepOutcome shape to the fabric one — the underlying quantum is the same
// executor, so semantics are preserved by construction.
func cognitionFunc(executor Executor) agentfabric.Cognition {
	return agentfabric.CognitionFunc(func(ctx context.Context, task *models.Task) (*agentfabric.StepOutcome, error) {
		out, err := executor.ExecuteStep(ctx, task)
		if err != nil {
			return nil, err
		}
		return &agentfabric.StepOutcome{
			Done:       out.Done,
			Checkpoint: out.Checkpoint,
			Result:     out.Result,
		}, nil
	})
}

// RegisterExecutorFn registers a dynamically spawned executor into the
// scheduler so it can be selected for task execution. This is the same
// method as kernelScheduler.RegisterExecutor.
type RegisterExecutorFn func(agentID string, executor Executor)

// AskAgentFn is the injected collaboration primitive behind the ask_agent tool.
// It must send a cross-agent request to target on the given topic and deliver
// the caller's payload. In production this is wired at serve time to
// aresrecovery.EvolutionAwareIPC.Send, which routes through the agentipc Bus —
// so the request lands in the existing "collaboration" feedback source via the
// CollaborationObserver (Step Y.2), reusing the already-closed OBSERVE half.
// Declared as a function (not an interface) so this package stays decoupled
// from agentipc/aresrecovery; the function is set at the consumer (code_rules
// §5.2).
type AskAgentFn func(ctx context.Context, from, to, topic string, payload any) error

// Kernel is the ensemble of fabric-level subsystems the syscalls operate on.
// It is the "Kernel enforces" surface: the syscalls validate against the
// agent fabric (quota/capability) and the task fabric (task creation).
type Kernel struct {
	agents   *agentfabric.Fabric
	fabric   *taskfabric.Fabric
	factory  ExecutorFactory
	register RegisterExecutorFn
	// askAgent is the collaboration primitive behind ask_agent (Step Y.2-ACT).
	// nil means the tool is not wired and ask_agent fails loudly rather than
	// pretend to collaborate.
	askAgent AskAgentFn
	// loopCtx is the lifetime context for plan loops started via the
	// create_plan loop option. A syscall Kernel is a long-lived managed
	// object (it backs every agent's tool binder for the whole serve
	// lifetime), so holding a context here is the sanctioned exception to
	// the "no ctx in struct" rule (code_rules §4.5); it is injected at
	// assembly with WithLoopLifetime and bounds every loop goroutine.
	loopCtx context.Context
	// idSeq generates unique agent IDs for auto-named spawns.
	idSeq atomic.Int64
	// maxPlanLoops caps concurrently live plan loops. create_plan is an
	// LLM-callable syscall, so an unbounded loop count is an unbounded
	// goroutine count; the cap is the plan-loop analogue of the spawn quota.
	maxPlanLoops int
	// loopMu guards planLoops.
	loopMu sync.Mutex
	// planLoops tracks live plan loops by plan ID so their errors have a
	// reader, they can be stopped individually, and the cap is enforceable.
	planLoops map[string]*taskfabric.PlanLoop
}

// KernelOption configures a syscall Kernel at construction time.
type KernelOption func(*Kernel)

// WithLoopLifetime injects the context that bounds plan-loop goroutines
// started through the create_plan loop option. Without it, a create_plan
// call carrying a loop spec fails loudly instead of leaking an unmanaged
// goroutine (production: the serve lifetime ctx, wired in peer_mode).
func WithLoopLifetime(ctx context.Context) KernelOption {
	return func(k *Kernel) { k.loopCtx = ctx }
}

// WithMaxPlanLoops overrides the cap on concurrently live plan loops started
// through create_plan. Non-positive values are ignored so the default cap can
// never be disabled by a bad config value.
func WithMaxPlanLoops(n int) KernelOption {
	return func(k *Kernel) {
		if n > 0 {
			k.maxPlanLoops = n
		}
	}
}

// WithAskAgent injects the collaboration primitive behind ask_agent (Step
// Y.2-ACT). Passed as a func so the Kernel stays decoupled from
// agentipc/aresrecovery. Without it, ask_agent fails loudly at call time
// (code_rules: no silent no-op for a deliberately offered action).
func WithAskAgent(fn AskAgentFn) KernelOption {
	return func(k *Kernel) { k.askAgent = fn }
}

// SetAskAgent replaces the collaboration primitive at runtime. Used at serve
// time to inject ipc.Send AFTER the Kernel is constructed but the IPC bridge
// is only built later (setupPeerRegistry). Thread-safe for the one writer /
// many tool-call readers pattern: askAgent is read on every tool call, and the
// injection happens once during serve assembly before any task runs.
func (k *Kernel) SetAskAgent(fn AskAgentFn) {
	k.askAgent = fn
}

// NewKernel creates a syscall Kernel over the given fabrics. The factory and
// register function are optional: without them, spawn creates the agent in
// the fabric but cannot register it as an executor (the agent exists but
// cannot execute tasks — useful for provenance-only spawns). In production
// both are wired so a spawned agent immediately becomes schedulable.
func NewKernel(
	agents *agentfabric.Fabric,
	fabric *taskfabric.Fabric,
	factory ExecutorFactory,
	register RegisterExecutorFn,
	opts ...KernelOption,
) *Kernel {
	k := &Kernel{
		agents:       agents,
		fabric:       fabric,
		factory:      factory,
		register:     register,
		maxPlanLoops: defaultMaxPlanLoops,
		planLoops:    make(map[string]*taskfabric.PlanLoop),
	}
	for _, opt := range opts {
		opt(k)
	}
	return k
}

// SpawnAgentArgs carries the LLM-provided arguments for the spawn_agent tool.
// The LLM decides the capability, the task context, and optionally the
// resource hints. The Kernel validates them.
type SpawnAgentArgs struct {
	// Capability is the declared capability of the new agent (e.g. "coder",
	// "reviewer"). Required — the scheduler matches tasks to agents by this.
	Capability string `json:"capability"`
	// TaskContext is the shared task state the new agent starts with. This
	// is the parent's projection of the task goal/constraints — never the
	// parent's private reasoning state.
	TaskContext map[string]any `json:"task_context,omitempty"`
	// Resources are optional resource hints for quota validation.
	Resources map[string]any `json:"resources,omitempty"`
	// ParentID is the spawning agent's ID (for provenance). Kernel-enforced:
	// when the tool context carries a caller (kctx.CallerID), that ID
	// wins and this field is ignored, so an LLM can never forge parentage;
	// the value is used only for direct/Kernel-internal calls without a
	// context identity. Empty parent = root spawn.
	ParentID string `json:"parent_id,omitempty"`
}

// SpawnAgentResult is the return value of the spawn_agent tool — what the
// LLM sees after the Kernel processes its spawn request.
type SpawnAgentResult struct {
	// AgentID is the identity of the newly created agent.
	AgentID string `json:"agent_id"`
	// Capability confirms the declared capability.
	Capability string `json:"capability"`
	// Registered reports whether the agent was registered as a scheduler
	// executor (false when no factory/register was wired).
	Registered bool `json:"registered"`
}

// SpawnAgent is the Kernel syscall behind the spawn_agent tool. It:
//  1. Validates the spec (non-empty capability, quota check via agentfabric).
//  2. Creates the Agent in the agent fabric (provenance link recorded).
//  3. If a factory + register function are wired, creates an executor and
//     registers it so the scheduler can drive tasks to the new agent.
//  4. Optionally creates a Task Fabric task if CreateTaskArgs is non-nil.
//
// The LLM calls this via the tool binder; the Kernel enforces safety.
func (k *Kernel) SpawnAgent(ctx context.Context, args SpawnAgentArgs) (*SpawnAgentResult, error) {
	if k.agents == nil {
		return nil, errors.New("agentsyscall: agent fabric not wired")
	}
	if args.Capability == "" {
		return nil, errors.New("agentsyscall: capability is required")
	}

	// Generate a unique agent ID when the LLM does not provide one.
	agentID := fmt.Sprintf("spawned-%s-%d", args.Capability, k.idSeq.Add(1))

	// C1: when an executor factory is wired, create the executor BEFORE spawn
	// and inject it as the agent's CognitionFactory so the spawned agent is a
	// REAL executable body (not just a provenance record) from birth. The
	// factory is called exactly once — the same executor instance is reused
	// for the scheduler registration below (code_rules: no second
	// executor copy).
	var executor Executor
	if k.factory != nil {
		executor = k.factory(agentID, args.Capability)
	}

	// Kernel-enforced provenance: the tool-context caller wins over any
	// LLM-supplied ParentID, so parentage can never be forged by a spawned
	// agent's arguments. The fallback keeps direct/Kernel-internal calls
	// (bootstrap wiring, existing syscall tests) working with no context
	// identity.
	parentID := args.ParentID
	if caller := kctx.CallerID(ctx); caller != "" {
		parentID = caller
	}

	spec := agentfabric.SpawnSpec{
		Identity:     agentID,
		Capabilities: []string{args.Capability},
		ParentID:     parentID,
		TaskContext:  args.TaskContext,
		Resources:    args.Resources,
	}
	if executor != nil {
		spec.CognitionFactory = func([]string) agentfabric.Cognition {
			return cognitionFunc(executor)
		}
	}

	agent, err := k.agents.Spawn(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("agentsyscall: spawn failed: %w", err)
	}

	registered := false
	if executor != nil && k.register != nil {
		k.register(agent.Identity, executor)
		registered = true
		log.Info("agentsyscall: spawned agent registered as executor", "agent", agent.Identity, "capability", args.Capability)
	}

	if !registered {
		log.Warn("agentsyscall: spawned agent not registered (no factory)", "agent", agent.Identity, "capability", args.Capability)
	}

	return &SpawnAgentResult{
		AgentID:    agent.Identity,
		Capability: args.Capability,
		Registered: registered,
	}, nil
}

// CreateTaskArgs carries the LLM-provided arguments for the create_task tool.
// The LLM decides the task's capability, priority, and dependencies — this
// is the cognition layer's decomposition output.
type CreateTaskArgs struct {
	// Capability is the required capability for this task. The scheduler
	// scores agents against this.
	Capability string `json:"capability"`
	// Priority drives preemption (higher wins).
	Priority int `json:"priority,omitempty"`
	// Dependencies lists prerequisite task IDs (DAG gate).
	Dependencies []string `json:"dependencies,omitempty"`
	// Payload carries opaque task data (e.g. task_desc, profile).
	Payload map[string]any `json:"payload,omitempty"`
	// NOTE: there is deliberately no "creator" argument. The Kernel stamps
	// Task.Origin from the tool context (kctx.CallerID) so provenance
	// is enforced by the Kernel — an LLM cannot forge a creator via params.
}

// CreateTaskResult is the return value of the create_task tool.
type CreateTaskResult struct {
	TaskID string `json:"task_id"`
	State  string `json:"state"`
}

// CreateTask is the Kernel syscall behind the create_task tool. It creates
// a real Task Fabric task (Create → READY) so the scheduler can pick it up
// and execute it via the normal Schedule → Acquire → RunQuantum path.
func (k *Kernel) CreateTask(ctx context.Context, args CreateTaskArgs) (*CreateTaskResult, error) {
	if k.fabric == nil {
		return nil, errors.New("agentsyscall: task fabric not wired")
	}
	if args.Capability == "" {
		return nil, errors.New("agentsyscall: capability is required")
	}

	taskID := fmt.Sprintf("task-%s-%d", args.Capability, k.idSeq.Add(1))

	task := &taskfabric.Task{
		ID:           taskID,
		Capability:   args.Capability,
		Priority:     args.Priority,
		Dependencies: args.Dependencies,
		RetryPolicy:  taskfabric.RetryPolicy{MaxRetries: 2},
		// Origin is Kernel-enforced: stamped from the tool context caller
		// (kctx.CallerID), never from LLM-supplied arguments. Empty =
		// root call (no agent caller in context).
		Origin: kctx.CallerID(ctx),
		Checkpoint: &taskfabric.CheckpointEnvelope{
			Payload: args.Payload,
		},
	}

	if err := k.fabric.Create(task); err != nil {
		return nil, fmt.Errorf("agentsyscall: create task failed: %w", err)
	}

	log.Info("agentsyscall: created task → READY", "task_id", taskID, "capability", args.Capability)

	return &CreateTaskResult{
		TaskID: taskID,
		State:  string(taskfabric.StateReady),
	}, nil
}

// AskAgentArgs carries the LLM-provided arguments for the ask_agent tool.
// The LLM decides which agent to ask and on what topic; the Kernel validates
// and forwards it to the injected collaboration primitive (ipc.Send in
// production).
type AskAgentArgs struct {
	// To is the target agent ID. Required.
	To string `json:"to"`
	// Topic is the collaboration subject.
	Topic string `json:"topic"`
	// Payload is the question body (JSON-serializable).
	Payload map[string]any `json:"payload,omitempty"`
}

// AskAgentResult is the return value of the ask_agent tool.
type AskAgentResult struct {
	// Accepted reports that the request was handed to the collaboration
	// primitive (a fire-and-forget send — acceptance is not an answer).
	Accepted bool `json:"accepted"`
}

// AskAgent is the Kernel syscall behind the ask_agent tool (Step Y.2-ACT).
// It forwards a cross-agent request to the injected AskAgentFn, which in
// production is ipc.Send — so the attempt produces a collaboration receipt
// in the existing "collaboration" feedback source, attributed to the active
// strategy. The Kernel enforces:
//
//   - a non-empty target is required;
//   - the primitive MUST be wired (fail-loud); a nil primitive would make the
//     tool a silent no-op, which is exactly the open-loop the plan removes.
func (k *Kernel) AskAgent(ctx context.Context, a AskAgentArgs) (*AskAgentResult, error) {
	if a.To == "" {
		return nil, errors.New("agentsyscall: ask_agent requires a target agent")
	}
	if k.askAgent == nil {
		return nil, errors.New("agentsyscall: ask_agent not wired (no collaboration IPC) — the agent cannot ask until serve injects it")
	}
	from := kctx.CallerID(ctx)
	if err := k.askAgent(ctx, from, a.To, a.Topic, a.Payload); err != nil {
		return nil, fmt.Errorf("agentsyscall: ask_agent to %s failed: %w", a.To, err)
	}
	return &AskAgentResult{Accepted: true}, nil
}

// BindTools registers the spawn_agent and create_task tools on the given
// tool binder. The binder is the same sub.ToolBinder the production LLM
// executor uses for all its tools, so the LLM sees spawn_agent alongside
// web_search, file_read, etc. — the Agent's cognition treats spawning and
// task creation as first-class tool calls.
//
// The toolBinder interface matches sub.ToolBinder.BindTool exactly, so this
// function accepts any binder that implements that method.
func BindTools(binder ToolBinder, kernel *Kernel) {
	if binder == nil || kernel == nil {
		return
	}
	binder.BindTool(SpawnAgentTool, func(ctx context.Context, args map[string]any) (any, error) {
		var sa SpawnAgentArgs
		if v, ok := args[paramCapability].(string); ok {
			sa.Capability = v
		}
		if v, ok := args["parent_id"].(string); ok {
			sa.ParentID = v
		}
		if v, ok := args["task_context"].(map[string]any); ok {
			sa.TaskContext = v
		}
		if v, ok := args["resources"].(map[string]any); ok {
			sa.Resources = v
		}
		return kernel.SpawnAgent(ctx, sa)
	})

	binder.BindTool(CreateTaskTool, func(ctx context.Context, args map[string]any) (any, error) {
		var ct CreateTaskArgs
		if v, ok := args[paramCapability].(string); ok {
			ct.Capability = v
		}
		if deps, ok := args["dependencies"].([]any); ok {
			for _, d := range deps {
				if s, ok := d.(string); ok {
					ct.Dependencies = append(ct.Dependencies, s)
				}
			}
		}
		if v, ok := args[paramPayload].(map[string]any); ok {
			ct.Payload = v
		}
		return kernel.CreateTask(ctx, ct)
	})
	// W9: the whole-DAG planning entry. See plan.go. JSON round-trip keeps
	// the parse strict: type mismatches surface as errors instead of silently
	// dropping fields (e.g. a string "3" for priority).
	binder.BindTool(AskAgentTool, func(ctx context.Context, args map[string]any) (any, error) {
		var aa AskAgentArgs
		if v, ok := args[paramTo].(string); ok {
			aa.To = v
		}
		if v, ok := args[paramTopic].(string); ok {
			aa.Topic = v
		}
		if v, ok := args[paramPayload].(map[string]any); ok {
			aa.Payload = v
		}
		return kernel.AskAgent(ctx, aa)
	})

	binder.BindTool(CreatePlanTool, func(ctx context.Context, args map[string]any) (any, error) {
		raw, err := json.Marshal(args)
		if err != nil {
			return nil, fmt.Errorf("agentsyscall: create_plan re-marshal: %w", err)
		}
		var cp CreatePlanArgs
		if err := json.Unmarshal(raw, &cp); err != nil {
			return nil, fmt.Errorf("agentsyscall: create_plan args: %w", err)
		}
		if len(cp.Steps) == 0 {
			return nil, errors.New("agentsyscall: create_plan requires a non-empty steps array")
		}
		return kernel.CreatePlan(ctx, cp)
	})
}

// ToolBinder is the minimal interface BindTools needs. It matches
// sub.ToolBinder.BindTool so the production binder satisfies it without
// importing this package.
type ToolBinder interface {
	BindTool(name string, toolFunc func(ctx context.Context, args map[string]any) (any, error))
}

// ToolSchemas returns the LLM-facing tool schemas for spawn_agent and
// create_task. These are injected into the resources.Registry so the LLM
// Chat API receives them alongside the built-in tools.
func ToolSchemas() []ToolSchema {
	return []ToolSchema{
		{
			Name:        SpawnAgentTool,
			Description: "Spawn a new peer agent with a declared capability. The Kernel validates quota and registers the agent as a schedulable executor. Use this when you decide a task should be split and worked on by another agent.",
			Parameters: map[string]any{
				paramType: paramTypeObject,
				paramProperties: map[string]any{
					paramCapability: map[string]any{
						paramType:        paramTypeString,
						paramDescription: "The declared capability of the new agent (e.g. 'coder', 'reviewer', 'researcher').",
					},
					"parent_id": map[string]any{
						paramType:        paramTypeString,
						paramDescription: "The spawning agent's ID (for provenance). Leave empty for root agents.",
					},
					"task_context": map[string]any{
						paramType:        paramTypeObject,
						paramDescription: "Shared task state to pass to the new agent (goal, constraints). Never include private reasoning.",
					},
				},
				paramRequired: []string{paramCapability},
			},
		},
		{
			Name:        CreateTaskTool,
			Description: "Create a new task in the Task Fabric. The task enters READY state and the scheduler will assign it to a capable agent. Use this to decompose work into sub-tasks.",
			Parameters: map[string]any{
				paramType: paramTypeObject,
				paramProperties: map[string]any{
					paramCapability: map[string]any{
						paramType:        paramTypeString,
						paramDescription: "The required capability for this task (e.g. 'coder', 'reviewer').",
					},
					"dependencies": map[string]any{
						paramType:        paramTypeArray,
						paramItems:       map[string]any{paramType: paramTypeString},
						paramDescription: "Prerequisite task IDs that must complete before this task runs.",
					},
					paramPayload: map[string]any{
						paramType:        paramTypeObject,
						paramDescription: "Opaque task data (e.g. task_desc, parameters).",
					},
				},
				paramRequired: []string{paramCapability},
			},
		},
		{
			Name:        AskAgentTool,
			Description: "Ask a specific target agent a question on a topic. The request is delivered to the target's collaboration handler. Use this when you know WHICH agent to ask, rather than spawning a new one (spawn_agent) or decomposing into tasks (create_task).",
			Parameters: map[string]any{
				paramType: paramTypeObject,
				paramProperties: map[string]any{
					paramTo: map[string]any{
						paramType:        paramTypeString,
						paramDescription: "The target agent ID to ask.",
					},
					paramTopic: map[string]any{
						paramType:        paramTypeString,
						paramDescription: "The collaboration subject (e.g. 'delegate-task').",
					},
					paramPayload: map[string]any{
						paramType:        paramTypeObject,
						paramDescription: "The question body (JSON-serializable).",
					},
				},
				paramRequired: []string{paramTo},
			},
		},
		CreatePlanToolSchema(),
	}
}

// ToolSchema is the minimal schema struct this package produces. It matches
// resources.ToolSchema's Name/Description/Parameters fields so the caller
// can convert without importing resources here.
type ToolSchema struct {
	Name        string
	Description string
	Parameters  map[string]any
}
