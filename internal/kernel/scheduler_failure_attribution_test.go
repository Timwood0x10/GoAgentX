package kernel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/aresrecovery"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// failingExecutor always fails the step; it exists to pin the outcome
// attribution contract: a failed quantum must be recorded as a FAILURE by the
// load tracker and the W4 attribution — never as a success.
type failingExecutor struct {
	id  string
	typ models.AgentType
}

func (e *failingExecutor) ID() string { return e.id }
func (e *failingExecutor) Type() models.AgentType {
	return e.typ
}
func (e *failingExecutor) ExecuteStep(_ context.Context, _ *models.Task) (*sub.StepOutcome, error) {
	return nil, errors.New("executor exploded")
}

// panickingExecutor panics on every call; it pins the panic-path contract:
// the winner's LoadTracker slot must be released so the agent stays
// schedulable after a panic instead of scoring 0 forever.
type panickingExecutor struct {
	id  string
	typ models.AgentType
}

func (e *panickingExecutor) ID() string { return e.id }
func (e *panickingExecutor) Type() models.AgentType {
	return e.typ
}
func (e *panickingExecutor) ExecuteStep(_ context.Context, _ *models.Task) (*sub.StepOutcome, error) {
	panic("boom inside executor")
}

// TestSchedulerAttributesFailureAsFailure drives a FAILING executor through
// Schedule → Acquire → RunQuantum → Fail and asserts that neither the load
// tracker nor the W4 attribution records a success. Regression for the
// RunQuantum error-swallowing bug: endQuantumOutcome previously received a nil
// error for every failed quantum, inflating agent confidence toward 1.0 and
// hiding every task failure from the logs.
func TestSchedulerAttributesFailureAsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &failingExecutor{id: "coder", typ: models.AgentType("code")}
	tracker := NewLoadTracker()
	sched := New(fabric, map[string]CapabilityExecutor{"coder": exec}, tracker)
	sched.PollInterval = 10 * time.Millisecond

	attribution := aresrecovery.NewExecutionAttribution()
	sched.WithAttribution(attribution)

	go sched.Run(ctx)

	if err := fabric.Create(&taskfabric.Task{
		ID:          "t-fail",
		Capability:  "code",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1}, // single attempt → terminal FAILED
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	waitForTaskState(t, fabric, "t-fail", taskfabric.StateFailed, 3*time.Second)

	// The fabric task reaches FAILED *inside* RunQuantum (fabric.Fail), but the
	// scheduler records the outcome into attribution/tracker in
	// endQuantumOutcome, which runs *after* RunQuantum returns. Synchronizing on
	// StateFailed alone therefore races that recording: the assertion can read
	// the neutral prior (1.0) before the failure is attributed. Poll for the
	// recorded outcome so the test pins the contract (failed quantum → failure
	// attribution) without depending on that intra-scheduler ordering window.
	waitFor(t, 2*time.Second, func() bool {
		return attribution.CapabilityConfidence("coder", "code") == 0 &&
			tracker.Confidence("coder") == 0
	}, "failed quantum must be attributed as failure (confidence 0)")
	if got := tracker.Load("coder"); got != 0 {
		t.Fatalf("busy slot must be released after the failed quantum, load=%v", got)
	}
	if sched.Scheduled.Load() != 0 {
		t.Fatalf("Scheduled counter must not count failed quanta, got %d", sched.Scheduled.Load())
	}
}

// TestSchedulerRetryAttribution_RecordsActualRetries verifies the B-1 fix:
// a task that retries (fails, requeues, fails again, terminal) must have its
// retry count attributed as the LIVE post-quantum RetryPolicy.Attempts, not
// the pre-quantum snapshot value. The old code sampled Attempts before
// RunQuantum, so a failing quantum's retry increment was never seen by
// RecordWithMetrics — the "starting budget" was attributed instead of the
// "actual retries this round consumed".
func TestSchedulerRetryAttribution_RecordsActualRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &failingExecutor{id: "coder", typ: models.AgentType("code")}
	attribution := aresrecovery.NewExecutionAttribution()
	sched := New(fabric, map[string]CapabilityExecutor{"coder": exec}, NewLoadTracker())
	sched.PollInterval = 10 * time.Millisecond
	sched.WithAttribution(attribution)

	go sched.Run(ctx)

	// MaxRetries=2 means the task can fail twice before terminal FAILED.
	// The first failure requeues (Attempts 0→1, CanRetry), the second
	// failure reaches terminal (Attempts 1→2, !CanRetry).
	if err := fabric.Create(&taskfabric.Task{
		ID:          "t-b1",
		Capability:  "code",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 2},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Wait for the task to reach terminal FAILED (both attempts consumed).
	waitForTaskState(t, fabric, "t-b1", taskfabric.StateFailed, 5*time.Second)

	// B-1 contract: both quanta must be attributed with the DELTA increment
	// (post-quantum − pre-quantum), not the cumulative Attempts. Quantum1:
	// Attempts 0→1, delta=1. Quantum2: Attempts 1→2, delta=1.
	// totalRetries = 1+1 = 2 over 2 records → AvgRetries = 1.0.
	waitFor(t, 2*time.Second, func() bool {
		snap := attribution.Snapshot()
		for _, cr := range snap.PerCapability {
			if cr.AgentID == "coder" && cr.Capability == "code" {
				return cr.Fail == 2 && cr.AvgRetries == 1.0
			}
		}
		return false
	}, "B-1: failed-quantum retries must reflect the delta (1+1=2 total, avg 1.0)")
}

// TestSchedulerPanicReleasesLoadSlot drives a PANICKING executor through the
// quantum path and asserts the agent's load slot is released: after the panic
// the agent must still be schedulable (Load back to 0). Regression for the
// leaked-slot bug where one panic permanently zeroed an agent's Score.
func TestSchedulerPanicReleasesLoadSlot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	exec := &panickingExecutor{id: "coder", typ: models.AgentType("code")}
	tracker := NewLoadTracker()
	sched := New(fabric, map[string]CapabilityExecutor{"coder": exec}, tracker)
	sched.PollInterval = 10 * time.Millisecond
	go sched.Run(ctx)

	if err := fabric.Create(&taskfabric.Task{
		ID:          "t-panic",
		Capability:  "code",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Wait for the executor to be acquired (task leaves READY — the panic is
	// recovered by the drain goroutine; the task stays leased and recovery
	// requeues it later), then allow the deferred release to run.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if tk, err := fabric.Task("t-panic"); err == nil && tk.State != taskfabric.StateReady {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	if got := tracker.Load("coder"); got != 0 {
		t.Fatalf("panic must release the LoadTracker slot, load=%v", got)
	}
}
