package sdk

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/aresrecovery"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// yieldExecutor is the H2 phase-1 execution body: the first ExecuteStep yields
// (Done=false + checkpoint), so the task enters SUSPENDED. It must be removed
// or killed for the chain to continue (the chaos kill in the test).
type yieldExecutor struct {
	id   string
	typ  string
	done atomic.Bool
}

var _ kernel.CapabilityExecutor = (*yieldExecutor)(nil)

func (e *yieldExecutor) ID() string             { return e.id }
func (e *yieldExecutor) Type() models.AgentType { return models.AgentType(e.typ) }

func (e *yieldExecutor) ExecuteStep(_ context.Context, task *models.Task) (*sub.StepOutcome, error) {
	e.done.Store(true)
	return &sub.StepOutcome{
		Done:       false,
		Checkpoint: map[string]any{"phase": "investigation-done", "step": 1},
	}, nil
}

// resumeExecutor is the H2 phase-2 replacement: it RESUMES from the preserved
// checkpoint (does not restart) and completes the task. Its resumedFrom field
// captures the checkpoint so the test can verify continuity.
type resumeExecutor struct {
	id          string
	typ         string
	mu          sync.Mutex
	resumedFrom any
}

var _ kernel.CapabilityExecutor = (*resumeExecutor)(nil)

func (e *resumeExecutor) ID() string             { return e.id }
func (e *resumeExecutor) Type() models.AgentType { return models.AgentType(e.typ) }

func (e *resumeExecutor) ExecuteStep(_ context.Context, task *models.Task) (*sub.StepOutcome, error) {
	e.mu.Lock()
	e.resumedFrom = task.Payload["checkpoint"]
	e.mu.Unlock()
	res := models.NewTaskResult(task.TaskID, task.AgentType)
	res.SetSuccess(nil, "recovered")
	return &sub.StepOutcome{Done: true, Result: res}, nil
}

func (e *resumeExecutor) resumed() any {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.resumedFrom
}

// TestSDKH2_ChaosRecoveryChain is the H2 acceptance (review: no
// SDK→scheduler→chaos→recovery whole-chain existed — only fabric-level tests
// did). It exercises the full loop through the SDK runtime:
//
//	Submit (SDK entry) → fabric.Create → scheduler Schedule→Acquire→RunQuantum
//	→ yield → SUSPENDED + checkpoint preserved → chaos kill executor → lease
//	expiry → aresrecovery.RequeueExpiredLeases → replacement executor resumes
//	from the preserved checkpoint (not restarts) → COMPLETED.
//
// The SDK's shared scheduler (kernel.Scheduler) and fabric
// (taskfabric.Fabric) are the SAME instances the production kernel would use;
// the aresrecovery recovery runs on them too. Only a controllable clock is
// injected so the lease ages deterministically without real sleeping.
func TestSDKH2_ChaosRecoveryChain(t *testing.T) {
	var mu sync.Mutex
	now := time.Now()
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}

	rt := NewRuntime(WithOllama("llama3.2"), WithTrace(false))
	defer rt.Close()
	// The yield/resume stubs below never invoke the LLM, but llmSvc must be
	// non-nil for NewAgent wiring; a mock keeps the test hermetic.
	rt.llmSvc = &mockLLMSvc{responses: []*core.GenerateResponse{
		{Content: "unused", Usage: core.TokenUsage{PromptTokens: 1, CompletionTokens: 1}},
	}}

	rt.RegisterAgent("coder") // capability = "coder"

	// ensureScheduler starts the shared scheduler goroutine and creates the
	// fabrics. After this, rt.sdkFabric, rt.sched, rt.agentsFabric are live.
	rt.ensureScheduler()

	// Inject a controllable clock so we can age the lease without sleeping.
	rt.sdkFabric.WithClock(clock)

	// Replace the default agent executor with a yield-checkpoint stub.
	// P1-1: route through sched.RegisterExecutor so the write is
	// execMu-guarded (no cross-lock race with the scheduler's reads).
	rt.sched.RegisterExecutor("coder", &yieldExecutor{id: "coder", typ: "coder"})

	// Submit the task in a goroutine (it blocks until COMPLETED).
	//
	// The task ID is explicit. It used to be left empty and the assertion below
	// looked up the hardcoded "sdk-task-1", but sdkTaskSeq is a package-level
	// counter: from the second run of the binary onward (`-count=2` and up) the
	// generated ID is sdk-task-2, sdk-task-3, … so the lookup never matched,
	// the test failed with "task never SUSPENDED", and this goroutine was left
	// blocked in Submit forever (goleak reports it).
	const h2TaskID = "sdk-h2-chain"
	var submitErr error
	submitDone := make(chan struct{})
	go func() {
		defer close(submitDone)
		_, submitErr = rt.Submit(context.Background(), Task{ID: h2TaskID, Capability: "coder", Input: "h2-chain"})
	}()

	// Wait for the task to reach SUSPENDED (quantum 1 yield).
	deadline := time.Now().Add(3 * time.Second)
	var taskID string
	for time.Now().Before(deadline) {
		tk, err := rt.sdkFabric.Task(h2TaskID)
		if err == nil && tk.State == taskfabric.StateSuspended {
			taskID = tk.ID
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if taskID == "" {
		t.Fatal("task never SUSPENDED")
	}

	// Verify the checkpoint was preserved after the yield.
	tk, _ := rt.sdkFabric.Task(taskID)
	if tk.Checkpoint == nil {
		t.Fatal("H2: checkpoint must be preserved after quantum 1 yield")
	}

	// ── Chaos kill: remove the executor from the scheduler's static pool.
	// P1-1: use sched.UnregisterExecutor so the write is execMu-guarded.
	rt.sched.UnregisterExecutor("coder")

	// Age the lease past the kernel scheduler's 5-minute TTL.
	advance(7 * time.Minute)

	// Recovery: requeue the expired-lease task.
	rec := aresrecovery.New(rt.sdkFabric, rt.agentsFabric, aresrecovery.DefaultRestartPolicy())
	requeued := rec.RequeueExpiredLeases()
	if len(requeued) != 1 {
		t.Fatalf("recovery must requeue exactly 1 task, got %d", len(requeued))
	}

	// Register a replacement executor that resumes from the preserved checkpoint.
	resume := &resumeExecutor{id: "coder-replacement", typ: "coder"}
	rt.sched.RegisterExecutorForTask(taskID, "coder-replacement", resume)

	// Wait for the Submit goroutine to complete (replacement runs → COMPLETED).
	select {
	case <-submitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Submit did not return after recovery")
	}

	if submitErr != nil {
		t.Fatalf("Submit error: %v", submitErr)
	}
	if resumed := resume.resumed(); resumed == nil {
		t.Fatal("replacement executor must RESUME from the preserved checkpoint (not restart)")
	}
	t.Logf("H2 chain PASS: Submit→yield→SUSPENDED→kill→lease→recovery→resume→COMPLETED")
}
