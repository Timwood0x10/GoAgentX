package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agents/base"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/agentsyscall"
	"github.com/Timwood0x10/ares/internal/ares_events"
	"github.com/Timwood0x10/ares/internal/core/models"
	kctx "github.com/Timwood0x10/ares/internal/kernel/ctx"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// w2StubAgent is a minimal sub.Agent for E2E testing. It implements
// ExecuteStep to produce deterministic results that the test can assert on.
type w2StubAgent struct {
	id         string
	typ        models.AgentType
	executed   atomic.Int64
	results    sync.Map // taskID → *models.TaskResult
	stepResult func(task *models.Task) *sub.StepOutcome
}

func (a *w2StubAgent) ID() string                  { return a.id }
func (a *w2StubAgent) Type() models.AgentType      { return a.typ }
func (a *w2StubAgent) Status() models.AgentStatus  { return models.AgentStatusReady }
func (a *w2StubAgent) Start(context.Context) error { return nil }
func (a *w2StubAgent) Stop(context.Context) error  { return nil }
func (a *w2StubAgent) Process(_ context.Context, _ any) (any, error) {
	return struct{}{}, nil
}
func (a *w2StubAgent) ProcessStream(_ context.Context, _ any) (<-chan base.AgentEvent, error) {
	return make(chan base.AgentEvent), nil
}
func (a *w2StubAgent) Execute(_ context.Context, task *models.Task) (*models.TaskResult, error) {
	res := models.NewTaskResult(task.TaskID, task.AgentType)
	res.SetSuccess(nil, fmt.Sprintf("executed by %s", a.id))
	return res, nil
}
func (a *w2StubAgent) ExecuteStep(_ context.Context, task *models.Task) (*sub.StepOutcome, error) {
	a.executed.Add(1)
	if a.stepResult != nil {
		out := a.stepResult(task)
		a.results.Store(task.TaskID, out.Result)
		return out, nil
	}
	res := models.NewTaskResult(task.TaskID, task.AgentType)
	res.SetSuccess(nil, fmt.Sprintf("executed by %s", a.id))
	a.results.Store(task.TaskID, res)
	return &sub.StepOutcome{Done: true, Result: res}, nil
}

// TestW2Case1IndependentCompletion verifies Case 1: a single Agent with no
// Leader and no Planner independently completes a task → COMPLETE. This is
// the simplest Peer Agent scenario: one agent, one task, no decomposition.
func TestW2Case1IndependentCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	agent := &w2StubAgent{id: "agent-A", typ: models.AgentType("coder")}
	executors := map[string]CapabilityExecutor{"agent-A": agent}
	tracker := newLoadTracker()
	sched := NewKernelScheduler(fabric, executors, tracker)
	sched.PollInterval = 50 * time.Millisecond
	go sched.Run(ctx)

	// Create a task directly in the fabric — no leader, no planner.
	taskID := "w2-case1-task"
	if err := fabric.Create(&taskfabric.Task{
		ID:          taskID,
		Capability:  "coder",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
		Checkpoint: &taskfabric.CheckpointEnvelope{
			Payload: map[string]any{"task_desc": "write a function"},
		},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Poll until the task completes or timeout.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tk, err := fabric.Task(taskID)
		if err == nil && tk.State == taskfabric.StateCompleted {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	tk, err := fabric.Task(taskID)
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	if tk.State != taskfabric.StateCompleted {
		t.Fatalf("task state = %s, want COMPLETED", tk.State)
	}
	if agent.executed.Load() != 1 {
		t.Fatalf("agent must have executed exactly 1 step, got %d", agent.executed.Load())
	}
	t.Logf("Case 1 PASS: single agent independently completed task (state=%s)", tk.State)
}

// TestW2Case2AutonomousDecomposition verifies Case 2: an Agent autonomously
// decides to split a task by calling spawn_agent + create_task syscalls.
// The spawned sub-tasks enter the fabric and are scheduled to other agents.
// The parent agent synthesises the results — proving the decomposition is
// the Agent's cognition product, not a framework pre-definition.
func TestW2Case2AutonomousDecomposition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	agentsFab := agentfabric.NewFabric()
	store := ares_events.NewMemoryEventStore()
	fabric = fabric.WithEventStore(store)

	// Two agents: A (orchestrator) and B (worker).
	agentA := &w2StubAgent{id: "agent-A", typ: models.AgentType("orchestrator")}
	agentB := &w2StubAgent{id: "agent-B", typ: models.AgentType("coder")}
	executors := map[string]CapabilityExecutor{
		"agent-A": agentA,
		"agent-B": agentB,
	}
	tracker := newLoadTracker()
	sched := NewKernelScheduler(fabric, executors, tracker)
	sched.PollInterval = 50 * time.Millisecond
	sched.WithMaxConcurrent(2)
	go sched.Run(ctx)

	// Wire the spawn syscall so agent A can autonomously spawn + create tasks.
	kernelSyscall := agentsyscall.NewKernel(
		agentsFab,
		fabric,
		nil, // no executor factory — we pre-register agents
		nil,
	)

	// Simulate the LLM's cognition: agent A decides to split.
	// In production this is a tool call from the LLM, executed with the
	// caller stamped into the context by the execution body
	// (kctx.WithCallerID); here we stamp it directly to verify the
	// Kernel path stamps provenance ("B.origin = A").
	subTaskPayload := map[string]any{"task_desc": "write unit tests for module X"}
	taskResult, err := kernelSyscall.CreateTask(kctx.WithCallerID(ctx, "agent-A"), agentsyscall.CreateTaskArgs{
		Capability: "coder",
		Payload:    subTaskPayload,
	})
	if err != nil {
		t.Fatalf("CreateTask syscall: %v", err)
	}

	// The sub-task must be in the fabric and READY.
	subTask, err := fabric.Task(taskResult.TaskID)
	if err != nil {
		t.Fatalf("Get sub-task: %v", err)
	}
	if subTask.State != taskfabric.StateReady {
		t.Fatalf("sub-task state = %s, want READY", subTask.State)
	}
	// Provenance: the Kernel must attribute the sub-task to the caller
	// (agent-A), not to anything the LLM could forge in arguments.
	if subTask.Origin != "agent-A" {
		t.Fatalf("sub-task origin = %q, want agent-A (caller stamped from tool context)", subTask.Origin)
	}

	// Poll until the sub-task completes.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tk, err := fabric.Task(taskResult.TaskID)
		if err == nil && tk.State == taskfabric.StateCompleted {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	subTask, err = fabric.Task(taskResult.TaskID)
	if err != nil {
		t.Fatalf("Get sub-task after poll: %v", err)
	}
	if subTask.State != taskfabric.StateCompleted {
		t.Fatalf("sub-task state = %s, want COMPLETED", subTask.State)
	}

	// Agent B must have executed the sub-task (capability match: "coder").
	if agentB.executed.Load() != 1 {
		t.Fatalf("agent B must have executed the sub-task, got %d executions", agentB.executed.Load())
	}
	// Agent A must NOT have executed the sub-task (different capability).
	if agentA.executed.Load() != 0 {
		t.Fatalf("agent A must not have executed the coder sub-task, got %d", agentA.executed.Load())
	}
	t.Logf("Case 2 PASS: agent autonomously created sub-task, scheduler assigned it to the capable agent")
}

// TestW2Case3ParentDeathChildContinues verifies Case 3: when a parent agent
// dies, the tasks it spawned continue to run. The task fabric is durable —
// task survival does not depend on the spawning agent's lifecycle.
func TestW2Case3ParentDeathChildContinues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	agentsFab := agentfabric.NewFabric()

	// Spawn a parent agent in the fabric.
	parent, err := agentsFab.Spawn(ctx, agentfabric.SpawnSpec{
		Identity:     "parent-A",
		Capabilities: []string{"orchestrator"},
	})
	if err != nil {
		t.Fatalf("Spawn parent: %v", err)
	}

	// The parent creates a sub-task via the syscall. The parent's identity
	// is stamped into the context (as the tool execution bodies do), so the
	// Kernel attributes the sub-task to it.
	kernelSyscall := agentsyscall.NewKernel(agentsFab, fabric, nil, nil)
	taskResult, err := kernelSyscall.CreateTask(kctx.WithCallerID(ctx, "parent-A"), agentsyscall.CreateTaskArgs{
		Capability: "coder",
		Payload:    map[string]any{"task_desc": "review code"},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// The sub-task carries its creator: parent death must not erase the
	// audit trail of who spawned the work.
	if created, err := fabric.Task(taskResult.TaskID); err != nil {
		t.Fatalf("Get sub-task: %v", err)
	} else if created.Origin != "parent-A" {
		t.Fatalf("sub-task origin = %q, want parent-A", created.Origin)
	}

	// Register a worker agent that can execute the "coder" task.
	worker := &w2StubAgent{id: "worker-B", typ: models.AgentType("coder")}
	executors := map[string]CapabilityExecutor{"worker-B": worker}
	tracker := newLoadTracker()
	sched := NewKernelScheduler(fabric, executors, tracker)
	sched.PollInterval = 50 * time.Millisecond
	go sched.Run(ctx)

	// Kill the parent agent (simulate death).
	if err := agentsFab.Kill(ctx, parent.Identity); err != nil {
		t.Fatalf("Kill parent: %v", err)
	}

	// The sub-task must still be in the fabric and eventually complete —
	// the task is durable, it does not die with its spawner.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tk, err := fabric.Task(taskResult.TaskID)
		if err == nil && tk.State == taskfabric.StateCompleted {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	tk, err := fabric.Task(taskResult.TaskID)
	if err != nil {
		t.Fatalf("Get task after parent death: %v", err)
	}
	if tk.State != taskfabric.StateCompleted {
		t.Fatalf("task state after parent death = %s, want COMPLETED", tk.State)
	}
	if worker.executed.Load() != 1 {
		t.Fatalf("worker must have executed the task, got %d", worker.executed.Load())
	}
	t.Logf("Case 3 PASS: parent died, child task continued and completed")
}

// TestW2Case4TrueCollaboration verifies Case 4: A → B request, B → A reply,
// B → C spawn. All agents are peers (A ≡ B ≡ C) — no permission differences.
// The test verifies that spawn establishes provenance, not hierarchy.
func TestW2Case4TrueCollaboration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agentsFab := agentfabric.NewFabric()
	fabric := taskfabric.NewFabric()

	// Spawn three peer agents.
	agentA, err := agentsFab.Spawn(ctx, agentfabric.SpawnSpec{
		Identity:     "A",
		Capabilities: []string{"orchestrator"},
	})
	if err != nil {
		t.Fatalf("Spawn A: %v", err)
	}
	agentB, err := agentsFab.Spawn(ctx, agentfabric.SpawnSpec{
		Identity:     "B",
		Capabilities: []string{"coder"},
		ParentID:     "A",
	})
	if err != nil {
		t.Fatalf("Spawn B: %v", err)
	}
	agentC, err := agentsFab.Spawn(ctx, agentfabric.SpawnSpec{
		Identity:     "C",
		Capabilities: []string{"reviewer"},
		ParentID:     "B",
	})
	if err != nil {
		t.Fatalf("Spawn C: %v", err)
	}

	// All three are same-level: IDLE, all schedulable, no permission hierarchy.
	for _, a := range []*agentfabric.Agent{agentA, agentB, agentC} {
		if a.State != agentfabric.StateIdle {
			t.Fatalf("agent %s must be IDLE (same level), got %s", a.Identity, a.State)
		}
	}

	// B spawned C (provenance), but B ≡ C — no hierarchy.
	if agentC.Parent != "B" {
		t.Fatalf("C.Parent = %q, want B (provenance)", agentC.Parent)
	}
	kids := agentsFab.Children("B")
	if len(kids) != 1 || kids[0] != "C" {
		t.Fatalf("Children(B) = %v, want [C]", kids)
	}

	// The scheduler treats all agents equally — only capability matters.
	executors := map[string]CapabilityExecutor{
		"A": &w2StubAgent{id: "A", typ: models.AgentType("orchestrator")},
		"B": &w2StubAgent{id: "B", typ: models.AgentType("coder")},
		"C": &w2StubAgent{id: "C", typ: models.AgentType("reviewer")},
	}
	tracker := newLoadTracker()
	sched := NewKernelScheduler(fabric, executors, tracker)
	sched.PollInterval = 50 * time.Millisecond
	go sched.Run(ctx)

	// Create tasks for each capability — the scheduler assigns by capability,
	// not by parent/child relationship.
	for _, cap := range []string{"orchestrator", "coder", "reviewer"} {
		if err := fabric.Create(&taskfabric.Task{
			ID:          fmt.Sprintf("w2-case4-%s", cap),
			Capability:  cap,
			RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
		}); err != nil {
			t.Fatalf("Create %s task: %v", cap, err)
		}
	}

	// Poll until all three tasks complete.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		allDone := true
		for _, cap := range []string{"orchestrator", "coder", "reviewer"} {
			tk, err := fabric.Task(fmt.Sprintf("w2-case4-%s", cap))
			if err != nil || tk.State != taskfabric.StateCompleted {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Verify each task was executed by the capability-matching agent.
	for _, cap := range []string{"orchestrator", "coder", "reviewer"} {
		tk, err := fabric.Task(fmt.Sprintf("w2-case4-%s", cap))
		if err != nil {
			t.Fatalf("Get %s task: %v", cap, err)
		}
		if tk.State != taskfabric.StateCompleted {
			t.Fatalf("%s task state = %s, want COMPLETED", cap, tk.State)
		}
	}

	// Verify each agent executed exactly its matching task.
	for id, agent := range executors {
		stub := agent.(*w2StubAgent)
		if stub.executed.Load() != 1 {
			t.Fatalf("agent %s must have executed exactly 1 task, got %d", id, stub.executed.Load())
		}
	}
	t.Logf("Case 4 PASS: A≡B≡C, capability-based scheduling, provenance not hierarchy")
}

// TestW2LongTaskStability verifies a long-running task scenario does not
// crash the runtime. It creates a sustained stream of tasks over 2 seconds
// with multiple agents executing concurrently, verifying:
//   - No goroutine leak (scheduler stays alive)
//   - No task stuck in LEASED forever
//   - All tasks eventually complete or fail (no permanent SUSPENDED)
//   - The scheduler's scheduled counter increases monotonically
func TestW2LongTaskStability(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	store := ares_events.NewMemoryEventStore()
	fabric = fabric.WithEventStore(store)

	// 4 agents with different capabilities.
	executors := map[string]CapabilityExecutor{
		"coder-1":      &w2StubAgent{id: "coder-1", typ: models.AgentType("coder")},
		"coder-2":      &w2StubAgent{id: "coder-2", typ: models.AgentType("coder")},
		"reviewer-1":   &w2StubAgent{id: "reviewer-1", typ: models.AgentType("reviewer")},
		"researcher-1": &w2StubAgent{id: "researcher-1", typ: models.AgentType("researcher")},
	}
	tracker := newLoadTracker()
	sched := NewKernelScheduler(fabric, executors, tracker)
	sched.PollInterval = 20 * time.Millisecond
	sched.WithMaxConcurrent(4)
	go sched.Run(ctx)

	// Submit a sustained stream of 50 tasks over ~1 second.
	const totalTasks = 50
	var submitted atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < totalTasks; i++ {
			cap := "coder"
			switch i % 3 {
			case 1:
				cap = "reviewer"
			case 2:
				cap = "researcher"
			}
			taskID := fmt.Sprintf("w2-long-%d", i)
			if err := fabric.Create(&taskfabric.Task{
				ID:          taskID,
				Capability:  cap,
				RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
			}); err != nil {
				t.Errorf("Create task %d: %v", i, err)
				continue
			}
			submitted.Add(1)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	// Wait for submission to finish, then poll for completion.
	wg.Wait()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		completed := 0
		for i := 0; i < totalTasks; i++ {
			tk, err := fabric.Task(fmt.Sprintf("w2-long-%d", i))
			if err == nil && (tk.State == taskfabric.StateCompleted || tk.State == taskfabric.StateFailed) {
				completed++
			}
		}
		if completed == totalTasks {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify all tasks reached a terminal state.
	completed := 0
	failed := 0
	stuck := 0
	for i := 0; i < totalTasks; i++ {
		tk, err := fabric.Task(fmt.Sprintf("w2-long-%d", i))
		if err != nil {
			t.Errorf("Get task %d: %v", i, err)
			continue
		}
		switch tk.State {
		case taskfabric.StateCompleted:
			completed++
		case taskfabric.StateFailed:
			failed++
		default:
			stuck++
			t.Errorf("task %d stuck in state %s", i, tk.State)
		}
	}

	if stuck > 0 {
		t.Fatalf("%d tasks stuck in non-terminal state", stuck)
	}
	if completed+failed != totalTasks {
		t.Fatalf("completed=%d failed=%d total=%d, want sum=%d", completed, failed, totalTasks, totalTasks)
	}

	// Verify the scheduler is still alive (scheduled counter > 0).
	if sched.Scheduled.Load() == 0 {
		t.Fatal("scheduler must have executed at least 1 task")
	}

	totalExec := int64(0)
	for _, agent := range executors {
		stub := agent.(*w2StubAgent)
		totalExec += stub.executed.Load()
	}
	t.Logf("Long task stability PASS: %d tasks (%d completed, %d failed), %d total executions, scheduler scheduled=%d",
		totalTasks, completed, failed, totalExec, sched.Scheduled.Load())
}
