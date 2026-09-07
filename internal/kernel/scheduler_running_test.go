package kernel

import (
	"context"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// TestSchedulerRunningFlag covers the K5 readiness contract: Running is
// false before Run, true while the drain loop is alive, and false after the
// loop exits on context cancellation — the System Runtime Adopt gate polls
// exactly this flag.
func TestSchedulerRunningFlag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &smokeExecutor{id: "coder", typ: models.AgentType("code")}
	sched := New(fabric, map[string]CapabilityExecutor{"coder": exec}, nil)
	sched.PollInterval = 10 * time.Millisecond

	if sched.Running() {
		t.Fatal("Running must be false before Run")
	}
	go sched.Run(ctx)

	// Poll for the flag with a deadline — the goroutine start is async.
	deadline := time.Now().Add(2 * time.Second)
	for !sched.Running() {
		if time.Now().After(deadline) {
			t.Fatal("Running never became true after Run started")
		}
		time.Sleep(2 * time.Millisecond)
	}

	cancel()
	deadline = time.Now().Add(2 * time.Second)
	for sched.Running() {
		if time.Now().After(deadline) {
			t.Fatal("Running never became false after context cancellation")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestSchedulerRunningFalseOnNilFabric covers the guard: a scheduler with no
// fabric must never report Running (Run returns before the flag is set).
func TestSchedulerRunningFalseOnNilFabric(t *testing.T) {
	sched := New(nil, map[string]CapabilityExecutor{"coder": nil}, nil)
	sched.Run(context.Background()) // synchronous early return
	if sched.Running() {
		t.Fatal("nil-fabric scheduler must not report Running")
	}
}

// TestSchedulerExecutesThenStops verifies the full managed-loop lifecycle:
// the drain loop runs (Running=true), completes a task, and exits cleanly
// with Running=false after cancellation — the same sequence the cmd/ares
// adopt path drives.
func TestSchedulerExecutesThenStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &smokeExecutor{id: "coder", typ: models.AgentType("code")}
	sched := New(fabric, map[string]CapabilityExecutor{"coder": exec}, nil)
	sched.PollInterval = 10 * time.Millisecond
	done := make(chan struct{})
	go func() {
		defer close(done)
		sched.Run(ctx)
	}()

	if err := fabric.Create(&taskfabric.Task{
		ID:          "t-running",
		Capability:  "code",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		tk, err := fabric.Task("t-running")
		if err == nil && tk.State == taskfabric.StateCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task not completed in time")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not exit after cancellation")
	}
	if sched.Running() {
		t.Fatal("Running must be false after Run exits")
	}
}
