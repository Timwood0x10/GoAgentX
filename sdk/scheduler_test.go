package sdk

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/kernel"
)

// countingExecutor wraps a sdkAgentExecutor and counts scheduler-driven
// executions. It also proves a CapabilityExecutor-compatible adapter can be
// swapped in at the SDK boundary (the scheduler consumes the interface).
type countingExecutor struct {
	inner    *sdkAgentExecutor
	executed atomic.Int64
}

var _ kernel.CapabilityExecutor = (*countingExecutor)(nil)

func (c *countingExecutor) ID() string { return c.inner.ID() }

func (c *countingExecutor) Type() models.AgentType { return c.inner.Type() }

func (c *countingExecutor) ExecuteStep(ctx context.Context, task *models.Task) (*sub.StepOutcome, error) {
	c.executed.Add(1)
	return c.inner.ExecuteStep(ctx, task)
}

// TestSubmitGoesThroughFabricScheduler is the H1/H2 acceptance
// (aresos-agentos-plan H1/H2: sdk.Runtime.Submit 经过 Task Fabric →
// kernelScheduler 调度，而不是直接找 agent 跑): the shared scheduler drives
// the executor once per submitted task — a task is created in the runtime's
// Task Fabric and reaches COMPLETED through the scheduler's
// Schedule→Acquire→RunQuantum path, and the returned result carries the
// agent's real output.
func TestSubmitGoesThroughFabricScheduler(t *testing.T) {
	rt := NewRuntime(WithOllama("llama3.2"), WithTrace(false))
	defer rt.Close()
	rt.llmSvc = &mockLLMSvc{responses: []*core.GenerateResponse{
		{Content: "scheduled result", Usage: core.TokenUsage{PromptTokens: 2, CompletionTokens: 4}},
	}}

	rt.RegisterAgent("coder")
	// Replace the registered executor with a probe that counts scheduler
	// drives. Under the merged path, Submit creates ONE fabric task and the
	// scheduler runs the executor exactly once. P1-1: route through
	// sched.RegisterExecutor so the write is execMu-guarded.
	rt.ensureScheduler()
	counter := &countingExecutor{inner: &sdkAgentExecutor{agent: rt.agentByCapability["coder"]}}
	rt.sched.RegisterExecutor("coder", counter)

	res, err := rt.Submit(context.Background(), Task{Capability: "coder", Input: "refactor"})
	if err != nil {
		t.Fatalf("Submit error: %v", err)
	}
	if res.Output != "scheduled result" {
		t.Fatalf("Output = %q, want the agent's real output", res.Output)
	}
	if got := counter.executed.Load(); got != 1 {
		t.Fatalf("scheduler must drive the executor exactly once per submit, got %d", got)
	}
}

// TestSubmitConcurrentThroughScheduler verifies the merged path is safe for
// concurrent submits: each task is independently scheduled and completed by
// the shared scheduler.
func TestSubmitConcurrentThroughScheduler(t *testing.T) {
	rt := NewRuntime(WithOllama("llama3.2"), WithTrace(false))
	defer rt.Close()
	rt.llmSvc = &mockLLMSvc{responses: []*core.GenerateResponse{
		{Content: "r1"}, {Content: "r2"}, {Content: "r3"},
	}}
	rt.RegisterAgent("coder")

	results := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() {
			_, err := rt.Submit(context.Background(), Task{Capability: "coder", Input: "task"})
			results <- err
		}()
	}
	for i := 0; i < 3; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent submit %d: %v", i, err)
		}
	}
}
