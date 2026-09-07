package kernel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// stubExecutor is a minimal CapabilityExecutor for testing.
type stubExecutor struct {
	id  string
	typ models.AgentType
}

func (e *stubExecutor) ID() string             { return e.id }
func (e *stubExecutor) Type() models.AgentType { return e.typ }
func (e *stubExecutor) ExecuteStep(_ context.Context, _ *models.Task) (*sub.StepOutcome, error) {
	return &sub.StepOutcome{Done: true}, nil
}

// TestStaleWinnerReleasedWhenReplacementExists verifies that when a stale
// winner (selected by Schedule but no longer executable) has another capable
// executor available, the scheduler releases the task so the next drain
// re-schedules it within one poll interval (EDGE-4: 5-minute stall).
//
// Before the fix the task stays LEASED for the full TTL (5 min); after the
// fix it is released to READY when another capable executor exists.
func TestStaleWinnerReleasedWhenReplacementExists(t *testing.T) {
	ctx := context.Background()
	fab := taskfabric.NewFabric()
	// "live" executor is capable of the task's capability.
	execs := map[string]CapabilityExecutor{
		"live": &stubExecutor{id: "live", typ: "code"},
	}
	sched := New(fab, execs, nil)
	sched.ttl = time.Minute // short enough for test, not used in stale path

	if err := fab.Create(&taskfabric.Task{
		ID:          "t1",
		Capability:  "code",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 0}, // no retries
	}); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	// Hand-craft a candidate list with a "ghost" winner that does not exist
	// in the executor registry. Schedule picks it (only candidate), then
	// executor lookup fails → stale-winner path.
	cands := []taskfabric.Candidate{
		{AgentID: "ghost", Capabilities: []string{"code"}, Confidence: 1.0},
	}
	if err := sched.executeWithCandidates(ctx, "t1", cands); err != nil {
		// stale-winner path returns nil; any other error is unexpected.
		t.Fatalf("executeWithCandidates: %v", err)
	}

	tk, err := fab.Task("t1")
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	// After fix: another capable executor ("live") exists → task released to READY.
	if tk.State != taskfabric.StateReady {
		t.Fatalf("expected task to be READY (released for re-schedule), got %s", tk.State)
	}
}

// TestStaleWinnerReleasedAndNominatedWhenRecoveryWired is the B1 contract for
// the production path: no capable executor is left, but a recovery loop is
// wired, so the task is released to READY *and* nominated so recovery gives it
// a replacement execution body promptly.
//
// Why B1 exists: the pre-B1 code kept the lease here unconditionally, letting
// TTL expiry drive recovery. That made recovery depend on wall-clock time
// advancing on its own — under a controlled clock the task stalled forever
// (a 1-in-20 failure of TestE2E_GrandLoop_RealSchedulerChaosRecovery under
// -race -coverprofile), and in production it burned the full 5-minute default
// TTL doing nothing.
func TestStaleWinnerReleasedAndNominatedWhenRecoveryWired(t *testing.T) {
	ctx := context.Background()
	fab := taskfabric.NewFabric()
	// No capable executors registered.
	sched := New(fab, nil, nil)
	sched.ttl = time.Minute

	var mu sync.Mutex
	var nominated []string
	sched.WithRecoveryHint(func(taskID string) {
		mu.Lock()
		defer mu.Unlock()
		nominated = append(nominated, taskID)
	})

	if err := fab.Create(&taskfabric.Task{
		ID:          "t1",
		Capability:  "code",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 0},
		Checkpoint:  map[string]any{"phase": "investigation-done"},
	}); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	cands := []taskfabric.Candidate{
		{AgentID: "ghost", Capabilities: []string{"code"}, Confidence: 1.0},
	}
	if err := sched.executeWithCandidates(ctx, "t1", cands); err != nil {
		t.Fatalf("executeWithCandidates: %v", err)
	}

	tk, err := fab.Task("t1")
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	if tk.State != taskfabric.StateReady {
		t.Fatalf("expected task released to READY (B1), got %s", tk.State)
	}
	if tk.Owner != "" {
		t.Fatalf("released task must have no owner, got %q", tk.Owner)
	}
	if tk.Lease != nil {
		t.Fatal("released task must have no lease")
	}
	// The checkpoint is what makes "resume, don't restart" possible — Release
	// must not clear it.
	if tk.Checkpoint == nil {
		t.Fatal("Release must preserve the checkpoint (E1 resume contract)")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(nominated) != 1 || nominated[0] != "t1" {
		t.Fatalf("recovery must be nominated exactly once with the task id, got %v", nominated)
	}
}

// TestStaleWinnerKeepsLeasedWithoutRecoveryHint locks the ordering constraint
// that makes B1 safe: releasing clears the lease, which also removes the task
// from CheckExpiredLeases' scope. With no capable executor AND no recovery loop
// to nominate, releasing would strand the task permanently — strictly worse
// than the TTL stall. So those paths (leader, SDK, chaos/sandbox) must keep the
// lease, since TTL expiry is their only remaining recovery trigger.
func TestStaleWinnerKeepsLeasedWithoutRecoveryHint(t *testing.T) {
	ctx := context.Background()
	fab := taskfabric.NewFabric()
	sched := New(fab, nil, nil) // no executors, no hint
	sched.ttl = time.Minute

	if err := fab.Create(&taskfabric.Task{
		ID:         "t1",
		Capability: "code",
	}); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	cands := []taskfabric.Candidate{
		{AgentID: "ghost", Capabilities: []string{"code"}, Confidence: 1.0},
	}
	if err := sched.executeWithCandidates(ctx, "t1", cands); err != nil {
		t.Fatalf("executeWithCandidates: %v", err)
	}

	tk, err := fab.Task("t1")
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	// Must stay LEASED so CheckExpiredLeases can still find it at TTL.
	if tk.State != taskfabric.StateLeased {
		t.Fatalf("with no recovery consumer the lease must be kept for TTL expiry, got %s", tk.State)
	}
	if tk.Lease == nil {
		t.Fatal("kept task must retain its lease (it is what CheckExpiredLeases scans)")
	}
}

// TestStaleWinnerWithReplacementSkipsRecoveryHint keeps the stale-winner
// branches distinct: when another capable executor can take the task, the
// release alone resolves it within one poll interval and there is nothing for
// recovery to do. Nominating here would wake the recovery loop on every
// ordinary agent churn.
func TestStaleWinnerWithReplacementSkipsRecoveryHint(t *testing.T) {
	ctx := context.Background()
	fab := taskfabric.NewFabric()
	execs := map[string]CapabilityExecutor{
		"live": &stubExecutor{id: "live", typ: "code"},
	}
	sched := New(fab, execs, nil)
	sched.ttl = time.Minute

	var mu sync.Mutex
	hintCalls := 0
	sched.WithRecoveryHint(func(string) {
		mu.Lock()
		defer mu.Unlock()
		hintCalls++
	})

	if err := fab.Create(&taskfabric.Task{
		ID:         "t1",
		Capability: "code",
	}); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	cands := []taskfabric.Candidate{
		{AgentID: "ghost", Capabilities: []string{"code"}, Confidence: 1.0},
	}
	if err := sched.executeWithCandidates(ctx, "t1", cands); err != nil {
		t.Fatalf("executeWithCandidates: %v", err)
	}

	tk, err := fab.Task("t1")
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	if tk.State != taskfabric.StateReady {
		t.Fatalf("a capable replacement exists; task must be released, got %s", tk.State)
	}

	mu.Lock()
	defer mu.Unlock()
	if hintCalls != 0 {
		t.Fatalf("a capable replacement exists; recovery must not be nominated, got %d calls", hintCalls)
	}
}
