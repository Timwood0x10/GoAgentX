package kernel

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// fakeShadowHook records OnTaskFinalized deliveries.
type fakeShadowHook struct {
	mu    sync.Mutex
	tasks []*models.Task
}

func (h *fakeShadowHook) OnTaskFinalized(task *models.Task) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tasks = append(h.tasks, task)
}

func (h *fakeShadowHook) delivered() []*models.Task {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*models.Task(nil), h.tasks...)
}

// failingDecideExecutor fails the quantum so the finalize path takes the
// error branch.
type failingDecideExecutor struct{ id string }

func (e *failingDecideExecutor) ID() string             { return e.id }
func (e *failingDecideExecutor) Type() models.AgentType { return "code" }
func (e *failingDecideExecutor) ExecuteStep(context.Context, *models.Task) (*sub.StepOutcome, error) {
	return nil, errors.New("boom")
}

// TestShadowHook_InvokedOnFinalizedTask locks the Step 4 (closure plan N-1)
// capture contract: a successfully finalized task is handed to the hook as
// the scheduler's models.Task view (identity + capability mapping intact).
func TestShadowHook_InvokedOnFinalizedTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &fakeDecideExecutor{id: "coder-1", cap: "code"}
	hook := &fakeShadowHook{}
	sched := New(fabric, map[string]CapabilityExecutor{"coder-1": exec}, NewLoadTracker())
	sched.WithShadowExecutionHook(hook)

	if err := fabric.Create(&taskfabric.Task{ID: "task-1", Capability: "code"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := sched.executeUnbound(ctx, "task-1"); err != nil {
		t.Fatalf("execute: %v", err)
	}

	got := hook.delivered()
	if len(got) != 1 {
		t.Fatalf("expected exactly one delivered task, got %d", len(got))
	}
	if got[0].TaskID != "task-1" {
		t.Fatalf("delivered task id = %q, want task-1", got[0].TaskID)
	}
	if got[0].AgentType != models.AgentType("code") {
		t.Fatalf("delivered agent type = %q, want code", got[0].AgentType)
	}
}

// TestShadowHook_SkippedOnFailedQuantum locks the sampling restriction: a
// failed quantum says nothing about how a candidate would have run the task,
// so the hook must not fire.
func TestShadowHook_SkippedOnFailedQuantum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &failingDecideExecutor{id: "coder-1"}
	hook := &fakeShadowHook{}
	sched := New(fabric, map[string]CapabilityExecutor{"coder-1": exec}, NewLoadTracker())
	sched.WithShadowExecutionHook(hook)

	if err := fabric.Create(&taskfabric.Task{ID: "task-bad", Capability: "code"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := sched.executeUnbound(ctx, "task-bad"); err == nil {
		t.Fatal("expected the failing quantum to surface an error")
	}
	if got := len(hook.delivered()); got != 0 {
		t.Fatalf("hook fired %d times on a failed quantum, want 0", got)
	}
}

// TestShadowHook_NilHookSafe locks backward compatibility: without a hook the
// finalize path behaves exactly as before.
func TestShadowHook_NilHookSafe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &fakeDecideExecutor{id: "coder-1", cap: "code"}
	sched := New(fabric, map[string]CapabilityExecutor{"coder-1": exec}, NewLoadTracker())

	if err := fabric.Create(&taskfabric.Task{ID: "task-plain", Capability: "code"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := sched.executeUnbound(ctx, "task-plain"); err != nil {
		t.Fatalf("execute: %v", err)
	}
}
