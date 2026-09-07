// Package kernel provides the "no leader" execution engine
// (ares-runtime: Agents are not orchestrated. They are scheduled).
//
// The Scheduler repeatedly drains the Task Fabric's ReadyTasks — the work
// source — and for each ready task runs:
//
//	Schedule (capability-aware) → Acquire (lease + fencing) → RunQuantum (one
//	agent step) → finalize (COMPLETED / FAILED / SUSPENDED).
//
// It is shared by both entry points:
//
//   - cmd/ares (serve/kernel): the production kernel scheduler.
//   - sdk.Runtime (Submit): the peer-runtime dispatch path — a submitted task
//     is created in the fabric and driven through the same scheduler, so the
//     SDK and the kernel do not maintain two divergent scheduling paths
//     (aresos-agentos-plan H1/H2: 合并 SDK 和 kernel 两条路径).
package kernel

import (
	"context"

	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/core/models"
)

// CapabilityExecutor is the minimal contract the Kernel scheduler needs from an
// agent executor (W2-5: 调度以 capability 为核心). It decouples the scheduler
// from the full sub.Agent interface — the scheduler only cares about identity,
// declared capability (via Type()), and single-quantum execution. Any type that
// implements these three methods is a schedulable executor, regardless of whether
// it is a sub.Agent, a recovery replacement, or a dynamically spawned peer.
//
// sub.Agent already satisfies this interface (it has ID, Type, and ExecuteStep),
// so all existing executors — production sub-agents, test stubs, replacement
// executors — are CapabilityExecutors without any adapter. The interface lives
// at the consumer (code_rules: interface at the consumer).
type CapabilityExecutor interface {
	// ID returns the executor's unique identity (used for lease ownership,
	// load tracking, and provenance).
	ID() string
	// Type returns the executor's declared capability. The scheduler's
	// candidate scorer matches this against the task's required capability.
	Type() models.AgentType
	// ExecuteStep runs exactly one execution quantum (plan P1.1 Execution
	// Quantum). Done=false carries a resumable checkpoint; Done=true carries
	// the finalized task result.
	ExecuteStep(ctx context.Context, task *models.Task) (*sub.StepOutcome, error)
}
