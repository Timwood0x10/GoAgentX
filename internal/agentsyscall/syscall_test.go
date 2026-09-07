package agentsyscall

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/core/models"
	kctx "github.com/Timwood0x10/ares/internal/kernel/ctx"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// stubExecutor is a minimal Executor for testing.
type stubExecutor struct {
	id  string
	typ models.AgentType
}

func (e *stubExecutor) ID() string             { return e.id }
func (e *stubExecutor) Type() models.AgentType { return e.typ }
func (e *stubExecutor) ExecuteStep(_ context.Context, _ *models.Task) (*StepOutcome, error) {
	return &StepOutcome{Done: true, Result: models.NewTaskResult("t", e.typ)}, nil
}

// stubBinder records bound tools for assertion.
type stubBinder struct {
	mu    sync.Mutex
	tools map[string]func(ctx context.Context, args map[string]any) (any, error)
}

func (b *stubBinder) BindTool(name string, fn func(ctx context.Context, args map[string]any) (any, error)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tools == nil {
		b.tools = make(map[string]func(ctx context.Context, args map[string]any) (any, error))
	}
	b.tools[name] = fn
}

func (b *stubBinder) call(ctx context.Context, name string, args map[string]any) (any, error) {
	b.mu.Lock()
	fn, ok := b.tools[name]
	b.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("tool %q not bound", name)
	}
	return fn(ctx, args)
}

// TestSpawnAgentCreatesAgentInFabric verifies the spawn_agent syscall creates
// a real agent in the agent fabric with the declared capability and a
// provenance link to the parent.
func TestSpawnAgentCreatesAgentInFabric(t *testing.T) {
	agents := agentfabric.NewFabric()
	kernel := NewKernel(agents, nil, nil, nil)

	result, err := kernel.SpawnAgent(context.Background(), SpawnAgentArgs{
		Capability: "coder",
		ParentID:   "agent-A",
	})
	if err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if result.AgentID == "" {
		t.Fatal("agent ID must not be empty")
	}
	if result.Capability != "coder" {
		t.Fatalf("capability = %q, want coder", result.Capability)
	}
	if result.Registered {
		t.Fatal("must not be registered without factory")
	}

	agent, err := agents.Get(result.AgentID)
	if err != nil {
		t.Fatalf("Get spawned agent: %v", err)
	}
	if agent.Parent != "agent-A" {
		t.Fatalf("parent = %q, want agent-A", agent.Parent)
	}
	// Provenance link exists.
	kids := agents.Children("agent-A")
	if len(kids) != 1 || kids[0] != result.AgentID {
		t.Fatalf("children = %v, want [%s]", kids, result.AgentID)
	}
}

// TestSpawnAgentInjectsExecutableCognition verifies the C1 upgrade
// (aresos-agentos-plan C1: spawn 的 agent 带执行体): when an executor factory
// is wired, the agent spawned by the syscall carries a real Cognition from
// birth — Agent.Executable() reports true and a quantum can be executed
// through the fabric — not just a provenance record. The same executor
// instance is registered for scheduling (factory called exactly once).
func TestSpawnAgentInjectsExecutableCognition(t *testing.T) {
	agents := agentfabric.NewFabric()
	execCalls := 0
	var registeredExec Executor
	factory := func(agentID, capability string) Executor {
		execCalls++
		return &stubExecutor{id: agentID, typ: models.AgentType(capability)}
	}
	register := func(agentID string, executor Executor) {
		registeredExec = executor
	}
	kernel := NewKernel(agents, nil, factory, register)

	result, err := kernel.SpawnAgent(context.Background(), SpawnAgentArgs{Capability: "coder"})
	if err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if !result.Registered {
		t.Fatal("spawned agent must be registered when factory + register are wired")
	}
	if execCalls != 1 {
		t.Fatalf("executor factory must be called exactly once, got %d", execCalls)
	}

	agent, err := agents.Get(result.AgentID)
	if err != nil {
		t.Fatalf("Get spawned agent: %v", err)
	}
	if !agent.Executable() {
		t.Fatal("C1: spawned agent must be executable (Cognition injected), not a phantom")
	}
	out, err := agent.ExecuteStep(context.Background(), models.NewTask("t-c1", models.AgentType("coder"), nil))
	if err != nil {
		t.Fatalf("ExecuteStep: %v", err)
	}
	if !out.Done {
		t.Fatal("spawned agent must complete a quantum")
	}
	if registeredExec == nil {
		t.Fatal("executor must be registered for scheduling")
	}
}

// TestSpawnAgentRegistersExecutor verifies that when a factory and register
// function are wired, the spawned agent is registered as a scheduler executor.
func TestSpawnAgentRegistersExecutor(t *testing.T) {
	agents := agentfabric.NewFabric()
	var registeredID string
	var registeredExec Executor
	factory := func(agentID, capability string) Executor {
		return &stubExecutor{id: agentID, typ: models.AgentType(capability)}
	}
	register := func(agentID string, executor Executor) {
		registeredID = agentID
		registeredExec = executor
	}
	kernel := NewKernel(agents, nil, factory, register)

	result, err := kernel.SpawnAgent(context.Background(), SpawnAgentArgs{
		Capability: "reviewer",
	})
	if err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if !result.Registered {
		t.Fatal("must be registered with factory + register")
	}
	if registeredID != result.AgentID {
		t.Fatalf("registered ID = %q, want %q", registeredID, result.AgentID)
	}
	if registeredExec == nil {
		t.Fatal("executor must not be nil")
	}
	if registeredExec.ID() != result.AgentID {
		t.Fatalf("executor ID = %q, want %q", registeredExec.ID(), result.AgentID)
	}
}

// TestSpawnAgentRejectsEmptyCapability verifies the Kernel enforces
// non-empty capability — a spawn without a declared capability is rejected.
func TestSpawnAgentRejectsEmptyCapability(t *testing.T) {
	kernel := NewKernel(agentfabric.NewFabric(), nil, nil, nil)
	_, err := kernel.SpawnAgent(context.Background(), SpawnAgentArgs{})
	if err == nil {
		t.Fatal("must reject empty capability")
	}
}

// TestCreateTaskCreatesTaskInFabric verifies the create_task syscall creates
// a real Task Fabric task in READY state.
func TestCreateTaskCreatesTaskInFabric(t *testing.T) {
	fabric := taskfabric.NewFabric()
	kernel := NewKernel(nil, fabric, nil, nil)

	result, err := kernel.CreateTask(context.Background(), CreateTaskArgs{
		Capability: "coder",
		Payload:    map[string]any{"task_desc": "write tests"},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if result.TaskID == "" {
		t.Fatal("task ID must not be empty")
	}
	if result.State != string(taskfabric.StateReady) {
		t.Fatalf("state = %q, want READY", result.State)
	}

	task, err := fabric.Task(result.TaskID)
	if err != nil {
		t.Fatalf("Get task: %v", err)
	}
	if task.Capability != "coder" {
		t.Fatalf("capability = %q, want coder", task.Capability)
	}
	if task.State != taskfabric.StateReady {
		t.Fatalf("state = %s, want READY", task.State)
	}
	// Root call (no caller in context): Origin must be empty — no agent
	// creator to attribute.
	if task.Origin != "" {
		t.Fatalf("origin = %q, want \"\" for a root call without context caller", task.Origin)
	}
}

// TestCreateTaskStampsCallerOrigin verifies the create_task syscall records
// the CALLER from the tool context (kctx.CallerID) as Task.Origin — the
// Kernel-enforced provenance, not an LLM-supplied argument.
func TestCreateTaskStampsCallerOrigin(t *testing.T) {
	fabric := taskfabric.NewFabric()
	kernel := NewKernel(nil, fabric, nil, nil)

	ctx := kctx.WithCallerID(context.Background(), "agent-A")
	result, err := kernel.CreateTask(ctx, CreateTaskArgs{
		Capability: "coder",
		Payload:    map[string]any{"task_desc": "write tests"},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	task, err := fabric.Task(result.TaskID)
	if err != nil {
		t.Fatalf("Get task: %v", err)
	}
	if task.Origin != "agent-A" {
		t.Fatalf("origin = %q, want agent-A (stamped from tool context)", task.Origin)
	}
}

// TestCreateTaskRejectsEmptyCapability verifies the Kernel rejects a task
// without a declared capability.
func TestCreateTaskRejectsEmptyCapability(t *testing.T) {
	kernel := NewKernel(nil, taskfabric.NewFabric(), nil, nil)
	_, err := kernel.CreateTask(context.Background(), CreateTaskArgs{})
	if err == nil {
		t.Fatal("must reject empty capability")
	}
}

// TestSpawnAgentEnforcesContextCaller verifies the Kernel overrides any
// LLM-supplied ParentID with the tool-context caller — parentage can never be
// forged by a spawned agent's arguments (plan D1-5: provenance is enforced by
// the Kernel, not trusted from LLM params).
func TestSpawnAgentEnforcesContextCaller(t *testing.T) {
	agents := agentfabric.NewFabric()
	kernel := NewKernel(agents, nil, nil, nil)

	// LLM claims parent "spoofed-parent"; the context proves the real caller
	// is agent-A. The Kernel must trust the context.
	ctx := kctx.WithCallerID(context.Background(), "agent-A")
	result, err := kernel.SpawnAgent(ctx, SpawnAgentArgs{
		Capability: "coder",
		ParentID:   "spoofed-parent",
	})
	if err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	agent, err := agents.Get(result.AgentID)
	if err != nil {
		t.Fatalf("Get spawned agent: %v", err)
	}
	if agent.Parent != "agent-A" {
		t.Fatalf("parent = %q, want agent-A (Kernel-enforced from context)", agent.Parent)
	}
	if kids := agents.Children("spoofed-parent"); len(kids) != 0 {
		t.Fatalf("spoofed parent must have no children, got %v", kids)
	}
}

// TestBindToolsRegistersBothTools verifies BindTools registers spawn_agent
// and create_task on the binder, and the bound functions call the Kernel.
func TestBindToolsRegistersBothTools(t *testing.T) {
	agents := agentfabric.NewFabric()
	fabric := taskfabric.NewFabric()
	kernel := NewKernel(agents, fabric, nil, nil)
	binder := &stubBinder{}

	BindTools(binder, kernel)

	ctx := context.Background()

	// spawn_agent
	spawnResult, err := binder.call(ctx, SpawnAgentTool, map[string]any{
		"capability": "coder",
		"parent_id":  "root",
	})
	if err != nil {
		t.Fatalf("call spawn_agent: %v", err)
	}
	sr, ok := spawnResult.(*SpawnAgentResult)
	if !ok {
		t.Fatalf("spawn result type = %T, want *SpawnAgentResult", spawnResult)
	}
	if sr.Capability != "coder" {
		t.Fatalf("capability = %q, want coder", sr.Capability)
	}

	// create_task — carry the caller in the context exactly as the tool
	// execution bodies do (sub executor / chat cognition / agentloop
	// engine), and verify the Kernel stamps it as Task.Origin.
	taskResult, err := binder.call(kctx.WithCallerID(ctx, "agent-A"), CreateTaskTool, map[string]any{
		"capability": "coder",
		"payload":    map[string]any{"task_desc": "review code"},
	})
	if err != nil {
		t.Fatalf("call create_task: %v", err)
	}
	tr, ok := taskResult.(*CreateTaskResult)
	if !ok {
		t.Fatalf("task result type = %T, want *CreateTaskResult", taskResult)
	}
	if tr.TaskID == "" {
		t.Fatal("task ID must not be empty")
	}
	tk, err := fabric.Task(tr.TaskID)
	if err != nil {
		t.Fatalf("Get task: %v", err)
	}
	if tk.Origin != "agent-A" {
		t.Fatalf("origin = %q, want agent-A (context caller survives the tool path)", tk.Origin)
	}
}

// TestSpawnedAgentIDsAreUnique verifies multiple spawns produce unique IDs.
func TestSpawnedAgentIDsAreUnique(t *testing.T) {
	kernel := NewKernel(agentfabric.NewFabric(), nil, nil, nil)
	seen := make(map[string]bool)
	for i := 0; i < 20; i++ {
		result, err := kernel.SpawnAgent(context.Background(), SpawnAgentArgs{
			Capability: "coder",
		})
		if err != nil {
			t.Fatalf("spawn %d: %v", i, err)
		}
		if seen[result.AgentID] {
			t.Fatalf("duplicate agent ID: %s", result.AgentID)
		}
		seen[result.AgentID] = true
	}
}

// TestToolSchemasReturnsBoth verifies ToolSchemas returns all the peer
// syscall schemas (spawn_agent, create_task, ask_agent, create_plan).
func TestToolSchemasReturnsBoth(t *testing.T) {
	schemas := ToolSchemas()
	if len(schemas) != 4 { // spawn_agent, create_task, ask_agent, create_plan
		t.Fatalf("expected 4 schemas, got %d", len(schemas))
	}
	names := make(map[string]bool)
	for _, s := range schemas {
		names[s.Name] = true
		if s.Description == "" {
			t.Fatalf("schema %q has empty description", s.Name)
		}
		if s.Parameters == nil {
			t.Fatalf("schema %q has nil parameters", s.Name)
		}
	}
	if !names[SpawnAgentTool] {
		t.Fatalf("missing %s schema", SpawnAgentTool)
	}
	if !names[CreateTaskTool] {
		t.Fatalf("missing %s schema", CreateTaskTool)
	}
	if !names[AskAgentTool] {
		t.Fatalf("missing %s schema", AskAgentTool)
	}
}

// TestAskAgent_NotWiredFailsLoud verifies that ask_agent without an injected
// collaboration primitive (SetAskAgent/WithAskAgent) returns an error rather
// than silently doing nothing — a nil primitive would make the tool a no-op
// and leave the collaboration loop open.
func TestAskAgent_NotWiredFailsLoud(t *testing.T) {
	_, err := NewKernel(nil, nil, nil, nil).AskAgent(
		kctx.WithCallerID(context.Background(), "agent-A"),
		AskAgentArgs{To: "agent-B", Topic: "delegate-task"},
	)
	if err == nil {
		t.Fatal("ask_agent must fail when no collaboration primitive is wired")
	}
}

// TestAskAgent_EmptyTargetRejected verifies the Kernel enforces a non-empty
// target even when the primitive is wired.
func TestAskAgent_EmptyTargetRejected(t *testing.T) {
	kernel := NewKernel(nil, nil, nil, nil, WithAskAgent(func(ctx context.Context, from, to, topic string, payload any) error {
		return nil
	}))
	if _, err := kernel.AskAgent(kctx.WithCallerID(context.Background(), "agent-A"), AskAgentArgs{To: ""}); err == nil {
		t.Fatal("ask_agent with empty target must be rejected")
	}
}

// TestAskAgent_ForwardsToPrimitive verifies that ask_agent forwards the caller,
// target, topic and payload to the injected primitive — the primitive call is
// the observable event that a real collaboration receipt is written from.
func TestAskAgent_ForwardsToPrimitive(t *testing.T) {
	var gotFrom, gotTo, gotTopic string
	var gotPayload map[string]any
	kernel := NewKernel(nil, nil, nil, nil, WithAskAgent(func(ctx context.Context, from, to, topic string, payload any) error {
		gotFrom, gotTo, gotTopic = from, to, topic
		gotPayload = payload.(map[string]any)
		return nil
	}))

	res, err := kernel.AskAgent(kctx.WithCallerID(context.Background(), "agent-A"), AskAgentArgs{
		To:      "agent-B",
		Topic:   "delegate-task",
		Payload: map[string]any{"task_desc": "please review"},
	})
	if err != nil {
		t.Fatalf("ask_agent: %v", err)
	}
	if !res.Accepted {
		t.Fatal("ask_agent should report accepted after a successful send")
	}
	if gotFrom != "agent-A" {
		t.Errorf("from = %q, want the context caller agent-A", gotFrom)
	}
	if gotTo != "agent-B" || gotTopic != "delegate-task" {
		t.Errorf("forwarded to=%q topic=%q, want agent-B / delegate-task", gotTo, gotTopic)
	}
	if gotPayload["task_desc"] != "please review" {
		t.Errorf("payload not forwarded verbatim: %v", gotPayload)
	}
}

// TestAskAgent_BoundToBinder verifies the ask_agent tool is registered on the
// binder and decodes its args (to/topic/payload) correctly.
func TestAskAgent_BoundToBinder(t *testing.T) {
	binder := &stubBinder{}
	kernel := NewKernel(nil, nil, nil, nil, WithAskAgent(func(ctx context.Context, from, to, topic string, payload any) error {
		return nil
	}))
	BindTools(binder, kernel)

	res, err := binder.call(
		kctx.WithCallerID(context.Background(), "agent-A"),
		AskAgentTool,
		map[string]any{"to": "agent-B", "topic": "pipeline-stage", "payload": map[string]any{"x": 1}},
	)
	if err != nil {
		t.Fatalf("call ask_agent: %v", err)
	}
	if ar, ok := res.(*AskAgentResult); !ok || !ar.Accepted {
		t.Fatalf("ask_agent result = %#v, want accepted *AskAgentResult", res)
	}
}

// TestCreatePlanCompilesBatchIntoFabric verifies the W9 create_plan syscall:
// a valid multi-step plan compiles into an all-READY batch with dependencies
// intact and Kernel-stamped origins.
func TestCreatePlanCompilesBatchIntoFabric(t *testing.T) {
	fabric := taskfabric.NewFabric()
	kernel := NewKernel(nil, fabric, nil, nil)

	result, err := kernel.CreatePlan(context.Background(), CreatePlanArgs{
		Steps: []PlanStepArgs{
			{ID: "plan-a", Capability: "coder"},
			{ID: "plan-b", Capability: "reviewer", DependsOn: []string{"plan-a"}},
		},
	})
	if err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
	if result.Count != 2 || len(result.TaskIDs) != 2 {
		t.Fatalf("batch = %v, want 2 tasks", result.TaskIDs)
	}
	b, terr := fabric.Task("plan-b")
	if terr != nil {
		t.Fatalf("task plan-b: %v", terr)
	}
	if len(b.Dependencies) != 1 || b.Dependencies[0] != "plan-a" {
		t.Fatalf("plan-b deps = %v, want [plan-a]", b.Dependencies)
	}
	if b.State != taskfabric.StateReady {
		t.Fatalf("state = %s, want READY", b.State)
	}
}

// TestCreatePlanRejectsEmptySteps verifies the empty-batch gate.
func TestCreatePlanRejectsEmptySteps(t *testing.T) {
	kernel := NewKernel(nil, taskfabric.NewFabric(), nil, nil)
	if _, err := kernel.CreatePlan(context.Background(), CreatePlanArgs{}); err == nil {
		t.Fatal("empty plan must be rejected")
	}
}

// TestCreatePlanRejectsMissingCapability verifies per-step validation.
func TestCreatePlanRejectsMissingCapability(t *testing.T) {
	kernel := NewKernel(nil, taskfabric.NewFabric(), nil, nil)
	_, err := kernel.CreatePlan(context.Background(), CreatePlanArgs{
		Steps: []PlanStepArgs{{ID: "s1"}},
	})
	if err == nil {
		t.Fatal("missing capability must be rejected")
	}
}

// TestCreatePlanAtomicRejectsCycle verifies a cyclic plan creates nothing.
func TestCreatePlanAtomicRejectsCycle(t *testing.T) {
	fabric := taskfabric.NewFabric()
	kernel := NewKernel(nil, fabric, nil, nil)
	_, err := kernel.CreatePlan(context.Background(), CreatePlanArgs{
		Steps: []PlanStepArgs{
			{ID: "x", Capability: "coder", DependsOn: []string{"y"}},
			{ID: "y", Capability: "coder", DependsOn: []string{"x"}},
		},
	})
	if err == nil {
		t.Fatal("cyclic plan must be rejected")
	}
	for _, id := range []string{"x", "y"} {
		if _, terr := fabric.Task(id); terr == nil {
			t.Fatalf("task %q must not exist after failed plan", id)
		}
	}
}
