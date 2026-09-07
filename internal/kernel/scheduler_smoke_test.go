package kernel

import (
	"context"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/ares_events"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// smokeExecutor completes every task in one quantum and records the count.
type smokeExecutor struct {
	id       string
	typ      models.AgentType
	executed int
}

func (e *smokeExecutor) ID() string { return e.id }
func (e *smokeExecutor) Type() models.AgentType {
	return e.typ
}
func (e *smokeExecutor) ExecuteStep(_ context.Context, task *models.Task) (*sub.StepOutcome, error) {
	e.executed++
	res := models.NewTaskResult(task.TaskID, task.AgentType)
	res.SetSuccess(nil, "done by "+e.id)
	return &sub.StepOutcome{Done: true, Result: res}, nil
}

// TestSchedulerExecutesReadyTask is the package-level smoke acceptance: a
// task created in the fabric is picked up by the shared scheduler and driven
// to COMPLETED through Schedule → Acquire → RunQuantum → finalize — the
// engine that both cmd/ares and the SDK drive.
func TestSchedulerExecutesReadyTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &smokeExecutor{id: "coder", typ: models.AgentType("code")}
	sched := New(fabric, map[string]CapabilityExecutor{"coder": exec}, nil)
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	if err := fabric.Create(&taskfabric.Task{
		ID:          "t-smoke",
		Capability:  "code",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tk, err := fabric.Task("t-smoke")
		if err == nil && tk.State == taskfabric.StateCompleted {
			if exec.executed != 1 {
				t.Fatalf("executor must run exactly once, got %d", exec.executed)
			}
			// Scheduled is incremented in executeWithCandidates *after*
			// RunQuantum returns COMPLETED, so it lags the task state by a
			// scheduler hop. Poll for it instead of reading it at the instant
			// the task turns COMPLETED (otherwise the assertion races the
			// counter increment and flakes as 0).
			waitFor(t, 2*time.Second, func() bool {
				return sched.Scheduled.Load() == 1
			}, "scheduler must count exactly one scheduled task")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task must complete, got state %v (executed=%d)", fabricTaskState(t, fabric), exec.executed)
}

// fabricTaskState is a test helper returning the current task state.
func fabricTaskState(t *testing.T, f *taskfabric.Fabric) taskfabric.TaskState {
	t.Helper()
	tk, err := f.Task("t-smoke")
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	return tk.State
}

// TestSchedulerSkippedWithoutCandidate verifies ErrNoCapableCandidate keeps a
// task waiting (not failed) when no executor matches the capability.
func TestSchedulerSkippedWithoutCandidate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	sched := New(fabric, map[string]CapabilityExecutor{"coder": &smokeExecutor{id: "coder", typ: models.AgentType("code")}}, nil)
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	if err := fabric.Create(&taskfabric.Task{
		ID:          "t-nocap",
		Capability:  "audit",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// The task stays READY — the scheduler never fails it for lacking a
	// capable candidate; it waits for one to appear.
	time.Sleep(150 * time.Millisecond)
	tk, err := fabric.Task("t-nocap")
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	if tk.State != taskfabric.StateReady && tk.State != taskfabric.StateSuspended {
		t.Fatalf("task without a capable candidate must keep waiting (READY/SUSPENDED), got %s", tk.State)
	}
}

// closedChannelEventStore is a test-only EventStore whose Subscribe returns a
// caller-owned channel (which the test can close to simulate the store closing
// a subscription). All other methods are inert.
type closedChannelEventStore struct {
	ch chan *ares_events.Event
}

func (s *closedChannelEventStore) Append(context.Context, string, []*ares_events.Event, int64) error {
	return nil
}
func (s *closedChannelEventStore) Read(context.Context, string, ares_events.ReadOptions) ([]*ares_events.Event, error) {
	return nil, nil
}
func (s *closedChannelEventStore) ReadAll(context.Context, ares_events.ReadOptions) ([]*ares_events.Event, error) {
	return nil, nil
}
func (s *closedChannelEventStore) Subscribe(context.Context, ares_events.EventFilter) (<-chan *ares_events.Event, error) {
	return s.ch, nil
}
func (s *closedChannelEventStore) StreamVersion(context.Context, string) (int64, error) {
	return 0, nil
}

// TestSchedulerClosedEventChannelFallsBackToPolling locks the N5 contract: a
// subscription channel closed by the event store must disable the event case
// and fall back to pure polling — the loop keeps draining on the poll ticker
// instead of busy-spinning on the closed channel.
func TestSchedulerClosedEventChannelFallsBackToPolling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &smokeExecutor{id: "coder", typ: models.AgentType("code")}
	sched := New(fabric, map[string]CapabilityExecutor{"coder": exec}, nil)
	sched.PollInterval = 20 * time.Millisecond

	ch := make(chan *ares_events.Event)
	sched.eventStore = &closedChannelEventStore{ch: ch}
	go sched.Run(ctx)

	// Close the subscription channel while the loop is running.
	close(ch)
	// Let the loop observe the close and switch to polling.
	time.Sleep(50 * time.Millisecond)

	// The scheduler must still be alive and executing via polling: a task
	// created after the channel closed must still complete.
	if err := fabric.Create(&taskfabric.Task{
		ID:          "t-after-close",
		Capability:  "code",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		tk, err := fabric.Task("t-after-close")
		return err == nil && tk.State == taskfabric.StateCompleted
	}, "task must complete via the polling fallback after the event channel closed")
}
