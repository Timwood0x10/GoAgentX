package kernel

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// TestSchedulerRenewsLeaseDuringLongQuantum pins the lease-heartbeat contract:
// a quantum that outlives the lease TTL must complete exactly ONCE — the
// heartbeat renews the lease so CheckExpiredLeases never requeues a live
// runner. Without renewal this test saw two concurrent executions of the same
// task (duplicate spend) and the first result discarded.
func TestSchedulerRenewsLeaseDuringLongQuantum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	var executions atomic.Int32
	exec := &countingSlow{id: "coder", typ: models.AgentType("code"), dur: 700 * time.Millisecond, count: &executions}
	sched := New(fabric, map[string]CapabilityExecutor{"coder": exec}, NewLoadTracker())
	sched.PollInterval = 20 * time.Millisecond
	sched.WithTTL(300 * time.Millisecond) // step (700ms) outlives the TTL 2x
	go sched.Run(ctx)

	if err := fabric.Create(&taskfabric.Task{
		ID:          "t-long",
		Capability:  "code",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	waitForTaskState(t, fabric, "t-long", taskfabric.StateCompleted, 5*time.Second)

	if got := executions.Load(); got != 1 {
		t.Fatalf("long quantum must execute exactly once (no expiry duplicate), got %d", got)
	}
}

// countingSlow records how many times the step actually ran.
type countingSlow struct {
	id    string
	typ   models.AgentType
	dur   time.Duration
	count *atomic.Int32
}

func (e *countingSlow) ID() string { return e.id }
func (e *countingSlow) Type() models.AgentType {
	return e.typ
}
func (e *countingSlow) ExecuteStep(ctx context.Context, task *models.Task) (*sub.StepOutcome, error) {
	e.count.Add(1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(e.dur):
	}
	res := models.NewTaskResult(task.TaskID, task.AgentType)
	res.SetSuccess(nil, "done")
	return &sub.StepOutcome{Done: true, Result: res}, nil
}
