package sdk

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// This file implements the H1/H2 merge (aresos-agentos-plan H1/H2: merge SDK
// and kernel two paths — sdk.Runtime.Submit goes through the Task Fabric and the
// shared kernelscheduler, not a divergent direct-run path). The SDK is a
// peer-runtime facade over the SAME scheduling engine the kernel uses:
//
//	Submit → fabric.Create → kernel.Scheduler (Schedule → Acquire →
//	RunQuantum via the registered sdk agent executor) → COMPLETED → result.
//
// A sdk agent is an executor like any other: it runs its ReAct loop inside one
// quantum (agentloop.Engine) and the scheduler owns capability matching,
// concurrency, retries and outcome bookkeeping — no second scheduling loop.

// sdkAgentExecutor adapts a sdk.Agent to the shared scheduler's
// CapabilityExecutor contract. One agent = one capability executor, matching
// the kernel's flat peer pool. Execution runs the agent's full ReAct loop in a
// single quantum (the agentloop engine iterates internally), so Done is always
// true; the fabric's retry policy still bounds failures.
type sdkAgentExecutor struct {
	agent *Agent
	// typ overrides the scheduler-facing capability. Normally empty (Type
	// falls back to the agent name); spawn_agent sets it to the declared
	// capability so a spawned peer can match create_task sub-tasks.
	typ models.AgentType
}

var _ kernel.CapabilityExecutor = (*sdkAgentExecutor)(nil)

func (e *sdkAgentExecutor) ID() string { return e.agent.name }

func (e *sdkAgentExecutor) Type() models.AgentType {
	if e.typ != "" {
		return e.typ
	}
	return models.AgentType(e.agent.name)
}

func (e *sdkAgentExecutor) ExecuteStep(ctx context.Context, task *models.Task) (*sub.StepOutcome, error) {
	input, _ := task.Payload["input"].(string)
	res, err := e.agent.Run(ctx, input)
	if err != nil {
		return nil, err
	}
	out := &sub.StepOutcome{Done: true}
	if res != nil {
		tr := models.NewTaskResult(task.TaskID, task.AgentType)
		tr.SetSuccess(nil, res.Output)
		// Carry the full sdk.Result back through the quantum checkpoint so
		// Submit can restore Output/ToolCalls/TokenUsage/Duration exactly.
		tr.Metadata = map[string]any{sdkResultKey: res}
		out.Result = tr
	}
	return out, nil
}

// sdkResultKey is the metadata key under which the sdk.Result rides through
// the fabric checkpoint (same-process reference — no JSON round-trip).
const sdkResultKey = "sdk_result"

// sdkTaskSeq assigns monotonic fabric task ids for submitted tasks.
var sdkTaskSeq atomic.Int64

// ensureScheduler lazily starts the shared scheduler over the runtime's own
// Task Fabric. It runs exactly once; subsequent calls reuse the started
// scheduler. The scheduler goroutine lives until Runtime.Close cancels
// schedCtx.
func (r *Runtime) ensureScheduler() {
	r.schedOnce.Do(func() {
		r.sdkFabric = taskfabric.NewFabric()
		r.schedCtx, r.schedCancel = context.WithCancel(context.Background())
		r.sched = kernel.New(r.sdkFabric, r.sdkExecutors, nil)
		r.sched.PollInterval = 20 * time.Millisecond
		go r.sched.Run(r.schedCtx)
		// D1: the SDK is a peer-runtime facade — wire the same kernel
		// syscalls (spawn_agent/create_task) into the tool registry so SDK
		// users can autonomously decompose tasks. Registered after sched
		// exists because the syscall Kernel needs the shared fabric + sched.
		r.wireSyscalls()
	})
}

// submitThroughScheduler creates the task in the fabric and waits for the
// scheduler to drive it to a terminal state, then restores the result. It is
// the H1/H2 merged dispatch path: the shared scheduler (not a direct agent
// call) owns capability matching and execution.
func (r *Runtime) submitThroughScheduler(ctx context.Context, t Task) (*Result, error) {
	r.ensureScheduler()

	// Resolve the executor first: a capability with no registered agent
	// auto-creates one (the runtime never refuses a well-formed task).
	executor := r.ensureExecutor(t.Capability)

	taskID := fmt.Sprintf("sdk-task-%d", sdkTaskSeq.Add(1))
	if t.ID != "" {
		taskID = t.ID
	}
	if err := r.sdkFabric.Create(&taskfabric.Task{
		ID:         taskID,
		Capability: string(executor.Type()),
		// Origin stays "" — SDK submissions are root tasks (the caller is
		// the SDK user, not an agent), so no agent creator is stamped.
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 1},
		Checkpoint: &taskfabric.CheckpointEnvelope{
			Payload: map[string]any{"input": t.Input},
		},
	}); err != nil {
		return nil, fmt.Errorf("sdk submit: %w", err)
	}
	// B32: reclaim the task on EVERY exit path. The two terminal branches
	// below delete explicitly (to read the result first); this defer covers
	// timeout / ctx cancellation / unexpected returns so a long-lived SDK
	// session cannot leak abandoned tasks in the fabric.
	defer func() {
		_ = r.sdkFabric.Delete(taskID)
	}()

	// Wait for a terminal state. A timeout, when set (> 0), bounds the
	// whole wait (and the execution — the executor receives the same
	// context). When <=0, no deadline is applied beyond the caller's ctx.
	// The wait context propagates DeadlineExceeded so a timed-out Submit
	// surfaces a deadline-exceeded cause, never a generic error.
	var waitCtx context.Context
	if t.Timeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, t.Timeout)
		defer cancel()
	} else {
		waitCtx = ctx
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-waitCtx.Done():
			if waitCtx.Err() == context.DeadlineExceeded {
				return nil, fmt.Errorf("sdk submit: task %s timed out after %s: %w", taskID, t.Timeout, context.DeadlineExceeded)
			}
			return nil, waitCtx.Err()
		case <-ticker.C:
			tk, err := r.sdkFabric.Task(taskID)
			if err != nil {
				return nil, fmt.Errorf("sdk submit: %w", err)
			}
			switch tk.State {
			case taskfabric.StateCompleted:
				// B32: the deferred Delete reclaims the task on every path.
				return r.resultFromFabric(tk)
			case taskfabric.StateFailed:
				return nil, fmt.Errorf("sdk submit: task %s failed", taskID)
			}
		}
	}
}

// resultFromFabric restores the sdk.Result from a completed fabric task's
// checkpoint. The full sdk.Result rides in the quantum metadata (same-process
// reference); the checkpoint's reason is the fallback output.
func (r *Runtime) resultFromFabric(tk *taskfabric.Task) (*Result, error) {
	dc, err := taskfabric.DecodeCheckpoint(tk.Checkpoint)
	if err != nil {
		return nil, fmt.Errorf("sdk submit: decode result: %w", err)
	}
	step, ok := dc.StepCheckpoint.(map[string]any)
	if ok {
		if md, ok := step["metadata"].(map[string]any); ok {
			if raw, present := md[sdkResultKey]; present {
				if res, ok := raw.(*Result); ok && res != nil {
					return res, nil
				}
				// The result rode as a same-process *Result pointer (see
				// sdkResultKey). A non-nil value that is NOT *Result means the
				// checkpoint crossed a boundary that dropped the concrete type
				// (e.g. a JSON round-trip once the fabric persists) — do NOT
				// silently return an empty Result and lose the output. Surface
				// it so the caller sees a real error instead of a blank
				// success (code_rules: no silent degradation).
				return nil, fmt.Errorf("sdk submit: result checkpoint has unexpected type %T (expected *Result); output lost across a serialization boundary", raw)
			}
		}
		if reason, ok := step["reason"].(string); ok && reason != "" {
			return &Result{Output: reason}, nil
		}
	}
	return &Result{}, nil
}

// ensureExecutor returns the executor for the capability, creating and
// registering a capability-named agent on demand when none is registered.
// The registration is protected by agentMu (same lock as RegisterAgent). It
// returns the interface so a caller-provided adapter (e.g. a test probe) is
// preserved.
func (r *Runtime) ensureExecutor(capability string) kernel.CapabilityExecutor {
	if capability == "" {
		capability = "agent"
	}
	// P1-1: check the scheduler's registry first (execMu-guarded). An agent
	// added via AddNode (not RegisterAgent) is registered here by
	// registerGraphAgents before the round starts, so we must NOT skip this
	// check even when agentByCapability has no entry.
	if ex, found := r.sched.LookupExecutor(capability); found {
		return ex
	}
	// Auto-create on demand: a runtime never refuses a well-formed task.
	agent := r.NewAgent(capability)
	ex := &sdkAgentExecutor{agent: agent}
	// P1-1: register through sched.RegisterExecutorIfAbsent so the check
	// (already registered?) and the set happen atomically under the single
	// execMu write lock. Two concurrent Submits for the same unregistered
	// capability otherwise both miss LookupExecutor and double-write, silently
	// discarding one agent; if-absent makes the first writer win and the
	// second reuse it.
	winner, _ := r.sched.RegisterExecutorIfAbsent(capability, ex)
	return winner
}
