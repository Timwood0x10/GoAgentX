package taskfabric

import "time"

// Task is the durable-intent object (design §3 of docs/zh/architecture/ares-runtime.md).
// Agents are disposable; a Task survives its owner via lease expiry and
// preserved checkpoints.
type Task struct {
	// ID is the stable task identifier.
	ID string
	// Capability is the required capability (e.g. "rust/unsafe-analysis");
	// the capability-aware scheduler scores agents against it.
	Capability string
	// State is the current lifecycle state.
	State TaskState
	// Priority drives preemption decisions (higher wins).
	Priority int
	// Owner is the current lease holder ("" when unowned).
	Owner string
	// Lease is the current TaskLease (nil when unowned).
	Lease *Lease
	// Checkpoint is durable progress preserved across preemption/requeue.
	Checkpoint any
	// Dependencies are prerequisite task IDs; is_ready = all completed.
	Dependencies []string
	// Deadline is the latest acceptable completion time.
	Deadline time.Time
	// RetryPolicy carries the retry budget.
	RetryPolicy RetryPolicy
	// Origin is the agent ID that created the task ("" = root: user-submitted
	// or system-bootstrapped, no agent caller). It is Kernel-validated: the
	// create_task syscall stamps the caller from the tool context
	// (kernel.CallerID), never from LLM-supplied arguments, so provenance
	// such as "B.origin = A" is auditable end-to-end (plan D1-5).
	Origin string
	// Quantum counts how many execution quanta (agent steps) this task has
	// run across ALL lease holders (accumulated across yield→resume cycles,
	// preemptions and chaos-recovery replacements). It is the "semantic step"
	// number the observability UI shows as Quantum #N (dashboard.md §4:
	// "Agent 正在执行第 18 个 semantic quantum"). Guarded by f.mu like every
	// other Task field.
	Quantum int
	// CreatedAt is when the task entered the fabric; UpdatedAt is the last
	// state/lease mutation (both injectable-clock aware). Used by the panel
	// for age/recency display.
	CreatedAt time.Time
	// UpdatedAt is the last state transition or quantum boundary time.
	UpdatedAt time.Time
}

// RetryPolicy bounds re-queueing after failures.
type RetryPolicy struct {
	// MaxRetries is the total attempts allowed (0 = no retries).
	MaxRetries int
	// Attempts counts executions so far.
	Attempts int
}

// CanRetry reports whether another attempt is allowed.
func (t *Task) CanRetry() bool {
	return t.RetryPolicy.MaxRetries <= 0 || t.RetryPolicy.Attempts < t.RetryPolicy.MaxRetries
}

// transition moves the task to a new state, rejecting illegal transitions
// (see canTransition in state.go).
func (t *Task) transition(to TaskState) error {
	if !canTransition(t.State, to) {
		return ErrIllegalState
	}
	t.State = to
	return nil
}
