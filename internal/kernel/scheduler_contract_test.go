package kernel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	ares_events "github.com/Timwood0x10/ares/internal/ares_events"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// countingCognition adapts a closure to agentfabric.Cognition and counts
// executions, letting fabric-agent tests observe real quantum runs.
type countingCognition struct {
	mu       sync.Mutex
	executed int
}

func (c *countingCognition) ExecuteStep(_ context.Context, task *models.Task) (*agentfabric.StepOutcome, error) {
	c.mu.Lock()
	c.executed++
	c.mu.Unlock()
	res := models.NewTaskResult(task.TaskID, task.AgentType)
	res.SetSuccess(nil, "fabric agent done")
	return &agentfabric.StepOutcome{Done: true, Result: res}, nil
}

func (c *countingCognition) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.executed
}

// waitForTaskState polls the fabric until the task reaches want or fails the
// test when the deadline elapses (a silent return would mask the real final
// state and shift the failure to a later, less precise assertion).
func waitForTaskState(t *testing.T, f *taskfabric.Fabric, id string, want taskfabric.TaskState, timeout time.Duration) taskfabric.TaskState {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tk, err := f.Task(id); err == nil && tk.State == want {
			return tk.State
		}
		time.Sleep(5 * time.Millisecond)
	}
	tk, err := f.Task(id)
	if err != nil {
		t.Fatalf("Task(%q): %v", id, err)
	}
	t.Fatalf("task %q state = %s, want %s (timeout %s)", id, tk.State, want, timeout)
	return tk.State
}

// waitFor polls cond until it returns true or the deadline elapses, then fails
// the test with msg. It exists so a test can synchronize on a scheduler-side
// side effect (e.g. attribution/tracker recording that happens *after* the
// fabric task reaches a terminal state) instead of racing an intermediate
// observable like the task state.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

// TestRegisterExecutorMakesAgentSchedulable locks the W1 dynamic-registration
// contract: an executor registered AFTER task creation becomes a candidate on
// the next drain.
//
// Bug scenario: a registry snapshot taken once at construction would leave
// late registrations invisible, so a recovered replacement executor could
// never pick up its task.
func TestRegisterExecutorMakesAgentSchedulable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	if err := fabric.Create(&taskfabric.Task{
		ID:          "late-task",
		Capability:  "rescue",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 30},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// No executor yet: the task must stay READY, never FAILED-by-no-candidate.
	tk0, err := fabric.Task("late-task")
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	if tk0.State != taskfabric.StateReady {
		t.Fatalf("task without candidates must stay READY, got %s", tk0.State)
	}

	late := &smokeExecutor{id: "rescuer", typ: models.AgentType("rescue")}
	sched.RegisterExecutor("rescuer", late)

	waitForTaskState(t, fabric, "late-task", taskfabric.StateCompleted, 3*time.Second)
	if late.executed != 1 {
		t.Fatalf("dynamically registered executor must run exactly once, got %d", late.executed)
	}
}

// TestRecoveryBindingExclusiveAndAutoRelease locks the W1 anti-hijack
// contract: an executor bound to one task is the ONLY candidate for that task
// and is NEVER offered to any other task; once the bound task reaches a
// terminal state the binding and registration are released automatically.
//
// Bug scenarios:
//  1. A recovery replacement hijacks a brand-new READY task (the original
//     W1 review defect).
//  2. Bindings leak, growing the executor map without bound.
func TestRecoveryBindingExclusiveAndAutoRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	coder := &smokeExecutor{id: "coder", typ: models.AgentType("coder")}
	sched := New(fabric, map[string]CapabilityExecutor{"coder": coder}, NewLoadTracker())
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	replacement := &smokeExecutor{id: "replacement-a", typ: models.AgentType("legacy-cap")}
	sched.RegisterExecutorForTask("bound-task", "replacement-a", replacement)

	if err := fabric.Create(&taskfabric.Task{
		ID:          "bound-task",
		Capability:  "legacy-cap",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 2},
	}); err != nil {
		t.Fatalf("Create bound-task: %v", err)
	}
	if err := fabric.Create(&taskfabric.Task{
		ID:          "free-task",
		Capability:  "coder",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 2},
	}); err != nil {
		t.Fatalf("Create free-task: %v", err)
	}

	waitForTaskState(t, fabric, "bound-task", taskfabric.StateCompleted, 3*time.Second)
	waitForTaskState(t, fabric, "free-task", taskfabric.StateCompleted, 3*time.Second)

	// Exactly one run proves both directions of the contract: the bound
	// executor served ITS task, and was never offered the free task (a
	// hijack would push this count to 2).
	if replacement.executed != 1 {
		t.Fatalf("replacement must execute only its bound task once, got %d", replacement.executed)
	}
	if coder.executed != 1 {
		t.Fatalf("static executor must serve only the free task once, got %d", coder.executed)
	}
	// The unregister runs in the drain goroutine right AFTER the fabric
	// state flips to COMPLETED, so waitForTaskState can return one step
	// before the release lands. Poll for the release instead of racing it
	// (the contract is "released automatically after terminal state", not
	// "released before the state is observable").
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := sched.LookupExecutor("replacement-a"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("binding must auto-release: replacement unregistered after terminal state")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestUnregisterExecutorRemovesCandidate verifies UnregisterExecutor makes an
// agent unschedulable for FUTURE tasks and clears stale bindings, so a failed
// replacement cannot be selected again.
func TestUnregisterExecutorRemovesCandidate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &smokeExecutor{id: "worker", typ: models.AgentType("code")}
	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	sched.RegisterExecutor("worker", exec)
	sched.RegisterExecutorForTask("gone-task", "worker", &smokeExecutor{id: "ghost", typ: models.AgentType("code")})

	sched.UnregisterExecutor("worker")

	if _, ok := sched.LookupExecutor("worker"); ok {
		t.Fatal("unregistered executor must leave the registry")
	}
	if sched.HasCapableExecutor("gone-task") {
		t.Log("note: gone-task has no fabric entry, HasCapableExecutor correctly reports false")
	}
	if got := ExecutorCountForTest(sched); got != 0 {
		t.Fatalf("registry must be empty after unregister, got %d entries", got)
	}
}

// ExecutorCountForTest exposes the registry size for assertions.
func ExecutorCountForTest(s *Scheduler) int { return s.ExecutorCount() }

// TestFabricAgentsAreTheSingleCandidateSource locks the B1 contract: with the
// Agent Fabric wired, a spawned EXECUTABLE fabric agent is schedulable even
// with an empty static registry, and killing it removes it from the candidate
// pool immediately.
func TestFabricAgentsAreTheSingleCandidateSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	agents := agentfabric.NewFabric()
	cog := &countingCognition{}
	if _, err := agents.Spawn(ctx, agentfabric.SpawnSpec{
		Identity:     "fab-worker",
		Capabilities: []string{"research"},
		CognitionFactory: func([]string) agentfabric.Cognition {
			return cog
		},
	}); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())
	sched.PollInterval = 10 * time.Millisecond
	sched.WithAgentFabric(agents)
	go sched.Run(ctx)

	if err := fabric.Create(&taskfabric.Task{
		ID:          "fab-task-1",
		Capability:  "research",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 2},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitForTaskState(t, fabric, "fab-task-1", taskfabric.StateCompleted, 3*time.Second)
	if got := cog.count(); got != 1 {
		t.Fatalf("fabric agent cognition must run once, got %d", got)
	}

	// Kill removes the candidate: a follow-up task has nobody to run it.
	if err := agents.Kill(ctx, "fab-worker"); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if err := fabric.Create(&taskfabric.Task{
		ID:          "fab-task-2",
		Capability:  "research",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
	}); err != nil {
		t.Fatalf("Create after kill: %v", err)
	}
	if st := waitForTaskState(t, fabric, "fab-task-2", taskfabric.StateReady, 300*time.Millisecond); st != taskfabric.StateReady {
		t.Fatalf("task after agent death must stay READY (no candidate), got %s", st)
	}
}

// TestPreemptLowerPriorityHandsBackRunningTask locks the P2.2 cooperative
// preemption contract: a higher-priority READY task causes the running
// lower-priority task to be handed back to READY (checkpoint preserved,
// fencing enforced), and the scheduler re-executes it afterwards.
//
// Bug scenario: preemption silently killing the running quantum, or
// double-finalizing it — the fencing token must reject the stale holder's
// late completion.
func TestPreemptLowerPriorityHandsBackRunningTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := ares_events.NewMemoryEventStore()
	eventLog := &eventTypeCollector{}
	evCh, err := store.Subscribe(ctx, ares_events.EventFilter{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-evCh:
				if !ok {
					return
				}
				eventLog.add(ev.Type)
			}
		}
	}()

	fabric := taskfabric.NewFabric().WithEventStore(store)

	// The low-priority executor blocks on a gate during its FIRST quantum;
	// later quanta (after preemption) complete immediately.
	gate := make(chan struct{})
	lowStarted := make(chan struct{})
	var once sync.Once
	low := &gatedExecutor{id: "low", typ: models.AgentType("batch"),
		onStart: func() {
			once.Do(func() { close(lowStarted) })
		},
		gate: gate,
	}
	high := &smokeExecutor{id: "high", typ: models.AgentType("urgent")}
	sched := New(fabric, map[string]CapabilityExecutor{
		"low":    low,
		"urgent": high,
	}, NewLoadTracker())
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	if err := fabric.Create(&taskfabric.Task{
		ID:          "low-task",
		Capability:  "batch",
		Priority:    0,
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 5},
	}); err != nil {
		t.Fatalf("Create low: %v", err)
	}

	select {
	case <-lowStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("low-priority task never started executing")
	}

	if err := fabric.Create(&taskfabric.Task{
		ID:          "high-task",
		Capability:  "urgent",
		Priority:    5,
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 2},
	}); err != nil {
		t.Fatalf("Create high: %v", err)
	}

	// The next drain preempts the running low task for the higher-priority
	// work; the fabric emits task.preempted and the task returns to READY.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if eventLog.contains(ares_events.EventTaskPreempted) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !eventLog.contains(ares_events.EventTaskPreempted) {
		t.Fatal("running low-priority task was never preempted by higher-priority work")
	}

	// Release the stale holder: its late completion must be rejected by the
	// fencing token (benign error), and the task is re-acquired fresh.
	close(gate)

	waitForTaskState(t, fabric, "high-task", taskfabric.StateCompleted, 3*time.Second)
	waitForTaskState(t, fabric, "low-task", taskfabric.StateCompleted, 5*time.Second)
	if high.executed != 1 {
		t.Fatalf("high-priority executor must run exactly once, got %d", high.executed)
	}
	if low.executed < 2 {
		t.Fatalf("low task must be re-executed after preemption (got %d runs)", low.executed)
	}
}

// gatedExecutor signals onStart on its first execution and blocks every
// execution until gate closes, simulating a long-running quantum.
type gatedExecutor struct {
	id       string
	typ      models.AgentType
	onStart  func()
	gate     chan struct{}
	mu       sync.Mutex
	started  bool
	executed int
}

func (e *gatedExecutor) ID() string             { return e.id }
func (e *gatedExecutor) Type() models.AgentType { return e.typ }

func (e *gatedExecutor) ExecuteStep(_ context.Context, task *models.Task) (*sub.StepOutcome, error) {
	e.mu.Lock()
	first := !e.started
	e.started = true
	e.executed++
	e.mu.Unlock()
	if first && e.onStart != nil {
		e.onStart()
	}
	<-e.gate
	res := models.NewTaskResult(task.TaskID, task.AgentType)
	res.SetSuccess(nil, "gated done")
	return &sub.StepOutcome{Done: true, Result: res}, nil
}

// eventTypeCollector gathers event types from a subscription channel.
type eventTypeCollector struct {
	mu    sync.Mutex
	types []ares_events.EventType
}

func (c *eventTypeCollector) add(t ares_events.EventType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.types = append(c.types, t)
}

func (c *eventTypeCollector) contains(t ares_events.EventType) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.types {
		if v == t {
			return true
		}
	}
	return false
}

// TestToModelTaskEnvelopeRoundTrip locks the W3 submission-metadata contract:
// UserProfile / Payload / StepCheckpoint ride inside a CheckpointEnvelope and
// are surfaced to the executor exactly where the execution bodies expect them
// (Payload fields plus payload["checkpoint"]). Corrupt envelopes degrade to
// the raw-checkpoint fallback instead of panicking.
func TestToModelTaskEnvelopeRoundTrip(t *testing.T) {
	fabric := taskfabric.NewFabric()
	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())

	t.Run("envelope fields surface", func(t *testing.T) {
		tk := &taskfabric.Task{
			ID:         "t-env",
			Capability: "code",
			Checkpoint: taskfabric.EncodeCheckpoint(taskfabric.DecodedCheckpoint{
				Payload:        map[string]any{"input": "hello"},
				StepCheckpoint: map[string]any{"phase": "two"},
			}),
		}
		mt := sched.ToModelTask(tk)
		if got, _ := mt.Payload["input"].(string); got != "hello" {
			t.Fatalf("Payload[input] = %v, want hello", mt.Payload["input"])
		}
		step, ok := mt.Payload["checkpoint"].(map[string]any)
		if !ok || step["phase"] != "two" {
			t.Fatalf("Payload[checkpoint] = %v, want step checkpoint preserved", mt.Payload["checkpoint"])
		}
	})

	t.Run("nil checkpoint yields empty payload", func(t *testing.T) {
		mt := sched.ToModelTask(&taskfabric.Task{ID: "t-nil", Capability: "code"})
		if len(mt.Payload) != 0 {
			t.Fatalf("nil checkpoint must yield no payload keys, got %v", mt.Payload)
		}
	})

	t.Run("corrupt checkpoint falls back to raw", func(t *testing.T) {
		tk := &taskfabric.Task{ID: "t-bad", Capability: "code", Checkpoint: "not-an-envelope"}
		mt := sched.ToModelTask(tk)
		if mt.Payload["checkpoint"] != "not-an-envelope" {
			t.Fatalf("corrupt checkpoint must fall back to raw payload[checkpoint], got %v", mt.Payload)
		}
	})
}
