package taskfabric

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Timwood0x10/ares/internal/ares_events"
)

var (
	// ErrTaskNotFound: the task id is unknown.
	ErrTaskNotFound = errors.New("taskfabric: task not found")
	// ErrNotOwner: the agent does not hold this task's lease.
	ErrNotOwner        = errors.New("taskfabric: agent does not own this task")
	ErrTaskUndeletable = errors.New("taskfabric: task is not deletable in its current state")
	// ErrEpochMismatch: the operation carried a stale fencing token (lease
	// epoch) — the task is now owned by a newer lease holder. This is the
	// guard against "A lease expired → B acquire → A late release" killing
	// B's ownership.
	ErrEpochMismatch = errors.New("taskfabric: lease epoch mismatch")
	// ErrIllegalState: the requested state transition is not allowed.
	ErrIllegalState = errors.New("taskfabric: illegal state transition")
	// ErrTaskNotReady: the task cannot be acquired in its current state.
	ErrTaskNotReady = errors.New("taskfabric: task not ready for acquire")
	// ErrTaskExists: a task with this id already exists.
	ErrTaskExists = errors.New("taskfabric: task already exists")
	// ErrNoCapableCandidate: no candidate scored > 0 for the task's required
	// capability, so Schedule could not pick an executor.
	ErrNoCapableCandidate = errors.New("taskfabric: no capable candidate")
	// ErrTaskIDRequired: Create was called without an id.
	ErrTaskIDRequired = errors.New("taskfabric: task id required")
	// ErrAgentIDRequired: Acquire was called without an owning agent id.
	ErrAgentIDRequired = errors.New("taskfabric: agent id required")
	// ErrInvalidTTL: a lease operation was called with a non-positive TTL.
	ErrInvalidTTL = errors.New("taskfabric: lease ttl must be positive")
	// ErrTaskNotMutable: an operation that rewrites a task's compiled shape
	// (SetDependencies / UpdatePayload) was attempted on a task in a state
	// that forbids it. Unlike ErrTaskUndeletable this is a *soft* refusal:
	// the caller (incremental compiler) records it and moves on, because the
	// graph change is still real — only its projection onto this one task
	// has to wait for the task to reach a mutable state.
	ErrTaskNotMutable = errors.New("taskfabric: task is not mutable in its current state")
)

// Fabric owns Tasks and their leases (design §6 of ares-runtime.md:
// Acquire / Release / Yield / Checkpoint). It is the scheduler's substrate:
// agents compete for tasks via CAS ownership, never via a leader's dispatch.
// Every ownership-carrying operation is fenced by the lease epoch (fencing
// token) so a stale holder can never act on a task it no longer owns.
// maxInMemoryEvents bounds the in-memory lifecycle log (N8: unbounded growth).
// The log is compacted to this size only when it reaches 2× the bound, so the
// amortized cost of the cap is O(1) per append and the resident log stays
// within 2× the bound. The durable event store (when attached) keeps the FULL
// history; the in-memory log is a bounded, convenience view for replay.
const maxInMemoryEvents = 10000

type Fabric struct {
	mu         sync.Mutex
	tasks      map[string]*Task
	events     []TaskEvent
	store      ares_events.EventStore // optional persistent event sink (P2-C); guarded by mu
	confidence ConfidenceSource       // experience-derived confidence (§8 Skill-first); guarded by mu
	now        func() time.Time       // injectable clock for lease tests
	epoch      uint64
	// strategyStamp is the submission-time attribution source (E1): called
	// once per Create to stamp the checkpoint envelope's StrategyID. Guarded
	// by mu; nil means "no strategy deployed / wiring absent", which reads as
	// the active-strategy fallback downstream.
	strategyStamp func() string

	// flushSeq/flushedSeq gate durable appends into strict causal order (N7:
	// concurrent flushAppends must not land out of order in the store's
	// version sequence). flushSeq is assigned under f.mu in recordLocked —
	// the same lock that serializes every state transition — so the sequence
	// order IS the causal order. flushCond waits until all earlier sequences
	// have been flushed, making store.Append calls across goroutines land in
	// record order regardless of which goroutine reaches flushAppends first.
	flushCond  *sync.Cond
	flushSeq   uint64 // next sequence; guarded by mu
	flushedSeq uint64 // last sequence durably appended; guarded by flushCond.L
}

// NewFabric creates an empty Task Fabric.
func NewFabric() *Fabric {
	f := &Fabric{tasks: make(map[string]*Task), now: time.Now}
	f.flushCond = sync.NewCond(&sync.Mutex{})
	return f
}

// WithClock injects a controllable clock for deterministic lease-expiry tests.
// Cross-package callers (e.g. aresrecovery) use this to advance time without
// real sleeping. Nil falls back to time.Now.
func (f *Fabric) WithClock(now func() time.Time) *Fabric {
	f.mu.Lock()
	defer f.mu.Unlock()
	if now != nil {
		f.now = now
	}
	return f
}

// WithConfidenceSource wires the experience-derived confidence (design §8:
// Skill-first — Score's Confidence comes from ares_skills.Experience
// BestMatch SuccessRate). Schedule fills candidates that do not declare a
// confidence with the provider's prior. Nil detaches. Guarded by mu.
//
// Args:
//   - src: the confidence provider (may be nil to detach).
//
// Returns:
//   - *Fabric: the fabric for chaining.
func (f *Fabric) WithConfidenceSource(src ConfidenceSource) *Fabric {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confidence = src
	return f
}

// WithStrategyStamp wires the submission-time attribution source (evolution
// loop closure E1). The fabric calls it once per Create to stamp the task's
// checkpoint envelope with the strategy that was active at submission, so
// runtime fitness samples stay attributed to the strategy that actually
// produced them — even when a promote happens mid-flight. It must be cheap
// and non-blocking (it runs on the submission path); returning "" means "no
// strategy deployed", which downstream reads as the active-strategy fallback.
// Nil detaches. Guarded by mu.
//
// Args:
//   - fn: the attribution source (may be nil to detach).
//
// Returns:
//   - *Fabric: the fabric for chaining.
func (f *Fabric) WithStrategyStamp(fn func() string) *Fabric {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.strategyStamp = fn
	return f
}

// strategyStampID samples the attribution source once. Callers that create
// task BATCHES (CompilePlan) call it a single time so every task of the batch
// carries the same attribution even if the active strategy changes
// mid-compilation. Returns "" when no stamp source is wired.
func (f *Fabric) strategyStampID() string {
	f.mu.Lock()
	fn := f.strategyStamp
	f.mu.Unlock()
	if fn == nil {
		return ""
	}
	return fn()
}

// stampStrategyAttribution stamps strategyID onto a freshly built task's
// checkpoint envelope. Only *CheckpointEnvelope checkpoints can carry the
// attribution (the versioned protocol); other shapes are left untouched so a
// raw progress checkpoint keeps its meaning. An explicit caller-provided
// StrategyID wins — the fabric only fills an EMPTY field, so batch creators
// that sampled the stamp once (CompilePlan) keep their per-batch consistency.
// The envelope is shallow-copied before stamping: the caller's envelope is
// never mutated through the fabric's back door.
func stampStrategyAttribution(t *Task, strategyID string) {
	if strategyID == "" {
		return
	}
	switch env := t.Checkpoint.(type) {
	case nil:
		t.Checkpoint = &CheckpointEnvelope{
			SchemaVersion: CurrentCheckpointSchemaVersion,
			StrategyID:    strategyID,
		}
	case *CheckpointEnvelope:
		if env == nil || env.StrategyID != "" {
			return
		}
		cp := *env
		cp.StrategyID = strategyID
		t.Checkpoint = &cp
	}
}

// WithEventStore attaches a persistent event sink (ares-runtime P2-C): every
// task lifecycle transition is appended to the store on the task's stream, in
// addition to the in-memory log, so scheduler/task/lease state can be rebuilt
// across restarts. Nil detaches. Guarded by mu.
//
// Args:
//   - store: the event store to publish task.* events to.
//
// Returns:
//   - *Fabric: the fabric for chaining.
func (f *Fabric) WithEventStore(store ares_events.EventStore) *Fabric {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.store = store
	return f
}

// Create registers a new READY task. The task is unowned and available for
// acquire. Idempotency: an existing id returns ErrTaskExists.
//
// Args:
//   - t: the task to register (ID must be non-empty).
//
// Returns:
//   - error: ErrTaskExists, or an error for an empty id.
func (f *Fabric) Create(t *Task) error {
	// Sample the attribution source BEFORE taking f.mu: the stamp fn talks to
	// the strategy control plane and must never run while the fabric state
	// machine is locked (deadlock avoidance), and it must be cheap.
	strategyID := f.strategyStampID()
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	if t.ID == "" {
		return ErrTaskIDRequired
	}
	if _, exists := f.tasks[t.ID]; exists {
		return ErrTaskExists
	}
	// P1-7: copy the caller's *Task so the fabric owns an isolated instance —
	// the caller keeping (or reusing) its *t cannot then race the fabric's
	// snapshot/state reads. `cp := *t` isolates every scalar field; the
	// Dependencies slice is a reference type, so it is copied explicitly below
	// (otherwise cp.Dependencies still aliases the caller's backing array and
	// LeaseSnapshot/TaskSnapshot reading it would race a caller mutation).
	// Checkpoint (any) is intentionally NOT deep-copied: it may hold arbitrary
	// types with no generic clone, and a freshly-created task's checkpoint is
	// nil in practice — callers must not mutate a checkpoint they have handed
	// to Create.
	cp := *t
	if len(t.Dependencies) > 0 {
		cp.Dependencies = append([]string(nil), t.Dependencies...)
	}
	cp.State = StateReady
	cp.Owner = ""
	cp.Lease = nil
	cp.CreatedAt = f.now()
	cp.UpdatedAt = cp.CreatedAt
	// E1: stamp the submission-time strategy attribution onto the task's
	// checkpoint envelope (once per Create; a pre-stamped envelope wins).
	stampStrategyAttribution(&cp, strategyID)
	f.tasks[t.ID] = &cp
	pending = append(pending, f.recordLocked(&cp, EventTaskCreated))
	return nil
}

// Acquire is the CAS ownership claim. Only an unowned READY (or SUSPENDED —
// checkpoint preserved, cooperative re-acquisition) task can be leased; a
// concurrent or repeated acquire is rejected, so two agents competing for the
// same task see exactly one winner.
//
// Args:
//   - id: the task id.
//   - agentID: the acquiring agent.
//   - ttl: the lease TTL.
//
// Returns:
//   - uint64: the fencing token (lease epoch) the agent must present on every
//     subsequent ownership-carrying operation.
//   - error: ErrTaskNotFound / ErrTaskNotReady.
func (f *Fabric) Acquire(id, agentID string, ttl time.Duration) (uint64, error) {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	t, ok := f.tasks[id]
	if !ok {
		return 0, ErrTaskNotFound
	}
	if agentID == "" {
		return 0, ErrAgentIDRequired
	}
	if t.State != StateReady && t.State != StateSuspended {
		return 0, ErrTaskNotReady
	}
	f.epoch++
	// Build the lease on the FABRIC's clock (f.now), not wall time: expiry is
	// evaluated against f.now (CheckExpiredLeases), so a mixed clock pair made
	// every lease born-expired whenever a test/fixture advanced the fabric
	// clock past real time — recovery then requeued live runners mid-quantum.
	lease := Lease{
		Owner:     agentID,
		ExpiresAt: f.now().Add(ttl),
		Epoch:     f.epoch,
	}
	if err := t.transition(StateLeased); err != nil {
		return 0, err
	}
	t.Owner = agentID
	t.Lease = &lease
	pending = append(pending, f.recordLocked(t, EventTaskAcquired))
	return lease.Epoch, nil
}

// Start moves a LEASED task owned by agentID (at the fenced epoch) to RUNNING.
func (f *Fabric) Start(id, agentID string, epoch uint64) error {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	t, err := f.ownerLocked(id, agentID, epoch)
	if err != nil {
		return err
	}
	if err := t.transition(StateRunning); err != nil {
		return err
	}
	pending = append(pending, f.recordLocked(t, EventTaskStarted))
	return nil
}

// Yield is the quantum-boundary primitive (design §4 correction 2): it
// hands execution back to the Runtime at a checkpoint. The state after yield
// is decided by the Scheduler (continue/suspend/preempt/handoff/complete);
// P0's default transition is SUSPENDED with the checkpoint preserved.
func (f *Fabric) Yield(id, agentID string, epoch uint64, checkpoint any) error {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	t, err := f.ownerLocked(id, agentID, epoch)
	if err != nil {
		return err
	}
	if err := t.transition(StateSuspended); err != nil {
		return err
	}
	t.Checkpoint = checkpoint
	pending = append(pending, f.recordLocked(t, EventTaskYielded))
	if checkpoint != nil {
		pending = append(pending, f.recordLocked(t, EventTaskCheckpointed))
	}
	return nil
}

// Complete finalizes a RUNNING task owned by agentID (at the fenced epoch) as
// COMPLETED. The task's Checkpoint is preserved as-is: a quantum may have
// written progress (or a worker result) into it before completing.
func (f *Fabric) Complete(id, agentID string, epoch uint64) error {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	t, err := f.ownerLocked(id, agentID, epoch)
	if err != nil {
		return err
	}
	if err := t.transition(StateCompleted); err != nil {
		return err
	}
	pending = append(pending, f.recordLocked(t, EventTaskCompleted))
	return nil
}

// CompleteWithCheckpoint finalizes a RUNNING task as COMPLETED while storing
// the quantum's output in the task Checkpoint. The plain Complete keeps
// whatever checkpoint was already on the task; this variant overwrites it
// with the caller-supplied result so a worker outcome survives completion
// (the kernel dispatch reads it back from the completed task — the serve
// result-reflux fix). The scheduler calls this instead of Complete when the
// step's quantum produced a real result.
func (f *Fabric) CompleteWithCheckpoint(id, agentID string, epoch uint64, checkpoint any) error {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	t, err := f.ownerLocked(id, agentID, epoch)
	if err != nil {
		return err
	}
	t.Checkpoint = checkpoint
	if err := t.transition(StateCompleted); err != nil {
		return err
	}
	pending = append(pending, f.recordLocked(t, EventTaskCompleted))
	return nil
}

// Fail marks a RUNNING task FAILED, or requeues it to READY when the retry
// policy allows another attempt (Agent 死亡 ≠ Task 死亡).
func (f *Fabric) Fail(id, agentID string, epoch uint64) error {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	t, err := f.ownerLocked(id, agentID, epoch)
	if err != nil {
		return err
	}
	t.RetryPolicy.Attempts++
	if t.CanRetry() {
		if err := t.transition(StateReady); err != nil {
			return err
		}
		// N8: record the failure while the failing agent is still attached —
		// the terminal/requeue event must not lose the actor. Ownership is
		// cleared only after the event is captured, so the following
		// task.ready event reflects the unowned task.
		pending = append(pending, f.recordLocked(t, EventTaskFailed))
		t.Owner = ""
		t.Lease = nil
		pending = append(pending, f.recordLocked(t, EventTaskReady))
		return nil
	}
	if err := t.transition(StateFailed); err != nil {
		return err
	}
	pending = append(pending, f.recordLocked(t, EventTaskFailed))
	return nil
}

// Renew extends the lease of a LEASED/RUNNING/SUSPENDED task owned by
// agentID at the fenced epoch. It is the heartbeat a long-running quantum
// sends so its own lease does not expire mid-execution: without renewal, any
// step longer than the TTL was requeued by CheckExpiredLeases while the
// original holder was still executing — duplicate concurrent execution of the
// same task (state stayed fenced-correct, but work and side effects doubled).
//
// Renewal fails (and callers must stop heartbeating) when the caller no
// longer owns the task: it was preempted, requeued after expiry, or finalized.
func (f *Fabric) Renew(id, agentID string, epoch uint64, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.ownerLocked(id, agentID, epoch)
	if err != nil {
		return err
	}
	if t.Lease == nil {
		return ErrTaskNotFound
	}
	t.Lease.ExpiresAt = f.now().Add(ttl)
	return nil
}

// Release returns a LEASED/RUNNING/SUSPENDED task to READY, clearing owner
// and lease so another agent can acquire it. The epoch fencing guarantees a
// stale holder (whose lease expired and was re-acquired by another agent)
// cannot release the task out from under the new owner.
func (f *Fabric) Release(id, agentID string, epoch uint64) error {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	t, err := f.ownerLocked(id, agentID, epoch)
	if err != nil {
		return err
	}
	if err := t.transition(StateReady); err != nil {
		return err
	}
	// N8: record the released event while the releasing agent is still
	// attached (provenance), then clear ownership so the task is unowned.
	pending = append(pending, f.recordLocked(t, EventTaskReleased))
	t.Owner = ""
	t.Lease = nil
	return nil
}

// CheckExpiredLeases requeues every task whose lease expired without renewal.
// This is the crash-recovery primitive: a dead agent's tasks return to READY
// and become acquirable again. Returns the ids of every requeued task so the
// recovery path can act on exactly the tasks that expired — not on all READY
// tasks (a task that is READY for the first time, or was released/steal-
// requeued, is not a recovery candidate and must not be treated as one).
func (f *Fabric) CheckExpiredLeases() []string {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	now := f.now()
	var requeued []string
	for _, t := range f.tasks {
		if t.Lease == nil || !t.Lease.IsExpired(now) {
			continue
		}
		// LEASED/RUNNING/SUSPENDED tasks with an expired lease are requeued
		// to READY. SUSPENDED is included: a dead agent's suspended task
		// (checkpoint preserved) must return to READY so another agent can
		// acquire and resume it (Agent 死亡 ≠ Task 死亡).
		if t.State != StateLeased && t.State != StateRunning && t.State != StateSuspended {
			continue
		}
		if err := t.transition(StateReady); err != nil {
			continue
		}
		// N8: record the expiry while the dead agent is still attached — the
		// terminal event must identify whose lease expired. Ownership is
		// cleared only after the event is captured.
		pending = append(pending, f.recordLocked(t, EventTaskExpired))
		t.Owner = ""
		t.Lease = nil
		requeued = append(requeued, t.ID)
	}
	return requeued
}

// Schedule picks the best capable candidate for a task and acquires it on its
// behalf (design §8: capability-aware scheduling — "who is the best executor",
// not merely "who is idle"). D2 (2026-08-16): the Scheduler orchestrates
// uniformly — ReadyTasks → Schedule → execute; idle agents Steal → Acquire.
// The scoring (capability overlap × (1-load) × confidence) comes from
// scheduler.go; Experience supplies confidence.
//
// Args:
//   - taskID: the task id.
//   - candidates: the agents competing to execute the task.
//   - ttl: the lease TTL granted to the winner.
//
// Returns:
//   - string: the winning agent id.
//   - uint64: the fencing token (lease epoch) the winner must present on
//     subsequent ownership-carrying operations.
//   - error: ErrNoCapableCandidate / ErrTaskNotFound / ErrTaskNotReady.
func (f *Fabric) Schedule(taskID string, candidates []Candidate, ttl time.Duration) (string, uint64, error) {
	t, err := f.Task(taskID)
	if err != nil {
		return "", 0, err
	}
	// Design §8 (Skill-first): the experience prior supplies confidence for
	// candidates that do not declare one — Score's Confidence comes from the
	// wired ConfidenceSource (ares_skills.Experience BestMatch SuccessRate).
	f.mu.Lock()
	src := f.confidence
	f.mu.Unlock()
	if src != nil {
		if conf := src.Confidence(t.Capability); conf > 0 {
			for i := range candidates {
				if candidates[i].Confidence <= 0 {
					candidates[i].Confidence = conf
				}
			}
		}
	}
	best := Pick(t.Capability, candidates)
	if best == nil {
		return "", 0, ErrNoCapableCandidate
	}
	epoch, err := f.Acquire(taskID, best.AgentID, ttl)
	if err != nil {
		return "", 0, err
	}
	return best.AgentID, epoch, nil
}

// Preempt cooperatively preempts a RUNNING task at a quantum boundary
// (architecture invariant #9: cooperative — never OS-style hard preemption).
// The task returns to READY with its checkpoint preserved, so another agent
// can acquire and resume it. The priority comparison itself is the caller's
// (Scheduler's) decision — Preempt is the primitive that hands the task back
// at the boundary; the fencing token ensures only the current holder can
// preempt its own task.
//
// Args:
//   - taskID: the task id.
//   - agentID: the preempting agent (must hold the lease).
//   - epoch: the fencing token returned by Acquire.
//   - reason: debug reason for the preemption (recorded in the event).
//
// Returns:
//   - error: ErrNotOwner / ErrEpochMismatch / ErrIllegalState.
func (f *Fabric) Preempt(taskID, agentID string, epoch uint64, reason string) error {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	t, err := f.ownerLocked(taskID, agentID, epoch)
	if err != nil {
		return err
	}
	if err := t.transition(StateReady); err != nil {
		return err
	}
	t.Owner = ""
	t.Lease = nil
	pending = append(pending, f.recordLocked(t, EventTaskPreempted))
	return nil
}

// RunningTask is a snapshot of one currently-RUNNING task, for the
// scheduler's preemption decision (the Scheduler decides WHO is preempted;
// Preempt is the primitive that hands the task back).
type RunningTask struct {
	// ID is the task id.
	ID string
	// Owner is the current lease holder (must be the preempting agent).
	Owner string
	// Epoch is the fencing token the holder must present to Preempt.
	Epoch uint64
	// Priority is the task's scheduling priority (higher wins).
	Priority int
}

// RunningTasks returns a snapshot of every currently-RUNNING task. It feeds
// the scheduler's priority-preemption decision (v0.3.0 review: Preempt was
// production-unused); the caller must not hold any fabric lock while calling
// Preempt with the returned epochs.
func (f *Fabric) RunningTasks() []RunningTask {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]RunningTask, 0, len(f.tasks))
	for _, t := range f.tasks {
		if t.State != StateRunning || t.Lease == nil {
			continue
		}
		out = append(out, RunningTask{
			ID:       t.ID,
			Owner:    t.Owner,
			Epoch:    t.Lease.Epoch,
			Priority: t.Priority,
		})
	}
	return out
}

// Task returns a copy of a task (ErrTaskNotFound when unknown). It returns a
// snapshot, never the internal pointer: callers may read the returned task
// freely while the fabric mutates the live task (state transitions under the
// fabric lock), so a caller that holds the result across its own reads cannot
// race with the fabric's writes.
//
// `snap := *t` only isolates the scalar fields; the reference-typed fields
// (Lease pointer, Dependencies slice) would still alias the live task, so a
// caller reading snap.Lease.ExpiresAt off-lock would race Renew/Acquire
// writing the SAME *Lease under f.mu. Both reference fields are therefore
// copied explicitly so the returned snapshot shares no mutable memory with
// the fabric. Checkpoint (any) is intentionally left aliased: it may hold
// arbitrary un-cloneable types, and the fabric only ever replaces the whole
// Checkpoint pointer (never mutates through it), so reading the old value
// off-lock stays safe.
func (f *Fabric) Task(id string) (*Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	snap := *t
	if t.Lease != nil {
		l := *t.Lease
		snap.Lease = &l
	}
	if len(t.Dependencies) > 0 {
		snap.Dependencies = append([]string(nil), t.Dependencies...)
	}
	return &snap, nil
}

// Events returns a copy of the lifecycle event log — the state-rebuild source.
func (f *Fabric) Events() []TaskEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]TaskEvent, len(f.events))
	copy(out, f.events)
	return out
}

// ownerLocked returns the task and verifies agentID holds its lease at the
// fenced epoch. A mismatch between the presented epoch and the current lease
// epoch returns ErrEpochMismatch — the fencing token guard.
func (f *Fabric) ownerLocked(id, agentID string, epoch uint64) (*Task, error) {
	t, ok := f.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if t.Owner == "" || t.Owner != agentID {
		return nil, ErrNotOwner
	}
	if t.Lease == nil || t.Lease.Epoch != epoch {
		return nil, ErrEpochMismatch
	}
	return t, nil
}

// pendingAppend is one durable-store write deferred until after f.mu is
// released. recordLocked builds it under the lock (cheap, in-memory only);
// flushAppends performs the actual store.Append I/O off-lock so a slow or
// blocking event store never stalls the fabric's CAS/state-machine mutex.
type pendingAppend struct {
	store  ares_events.EventStore // captured under lock — never read via f.store off-lock
	typ    EventType
	taskID string
	event  *ares_events.Event
	// seq is the fabric-wide monotonic sequence assigned under f.mu at record
	// time (N7). flushAppends waits for seq contiguity so durable appends from
	// concurrent fabric calls land in causal order.
	seq uint64
}

// recordLocked appends one lifecycle event to the in-memory log (the only
// part that needs f.mu) and, when a store is attached, returns the durable
// write to be flushed AFTER the lock is released. It never performs I/O.
// Callers MUST be holding f.mu and MUST flush the returned value (via a
// deferred flushAppends) once unlocked. Returns nil when there is nothing to
// persist (no store, or an unmapped event type).
func (f *Fabric) recordLocked(t *Task, typ EventType) *pendingAppend {
	ev := TaskEvent{
		Type:       typ,
		TaskID:     t.ID,
		AgentID:    t.Owner,
		Origin:     t.Origin,
		State:      t.State,
		Checkpoint: t.Checkpoint,
		At:         f.now(),
	}
	// Every recorded event is a task mutation (state transition, lease change
	// or quantum boundary), so stamp UpdatedAt here to keep it the single
	// source of "last change" the Tasks page renders — otherwise terminal
	// transitions (Complete/Fail) would leave UpdatedAt frozen at the last
	// quantum. Matches the event's own timestamp so the two never drift.
	t.UpdatedAt = ev.At
	f.events = append(f.events, ev)
	// Cap the in-memory event log (N8: unbounded growth). Only compact when
	// the log exceeds 2×max so the amortized cost is O(1) per append.
	if max := maxInMemoryEvents; max > 0 && len(f.events) > 2*max {
		copy(f.events, f.events[len(f.events)-max:])
		f.events = f.events[:max]
	}
	if f.store == nil {
		return nil
	}
	et := taskEventType(typ)
	if et == "" {
		return nil
	}
	f.flushSeq++
	// Rebuild payload (release-readiness T2): must-persist events carry every
	// field RestoreFromStore needs to fold the task back (capability,
	// priority, dependencies, deadline, retry budget, creation time and the
	// versioned checkpoint JSON). Observability-only events keep the minimal
	// provenance payload.
	payload := map[string]any{
		restoreKeyTaskID:  t.ID,
		restoreKeyAgentID: t.Owner,
		restoreKeyOrigin:  t.Origin,
		restoreKeyState:   string(t.State),
		// The fencing epoch rides on EVERY persisted event, not just the
		// must-persist ones: Acquire bumps f.epoch and records the
		// observability-only task.acquired, so restricting the epoch to
		// must-persist events would lose every token granted after the last
		// checkpoint — the rebuilt fabric would then RE-ISSUE those tokens
		// and a stale pre-crash holder would pass ownerLocked's epoch check.
		restoreKeyEpoch: f.epoch,
	}
	// E1: the strategy attribution key also rides on EVERY persisted event,
	// same reasoning as the epoch — the observability-only task.acquired/
	// task.completed events are the ones RuntimeObserver subscribes to, so
	// restricting the key to must-persist events would leave the observation
	// side reading nothing and every sample would fall back to "the strategy
	// active at fold time". The value comes from the checkpoint envelope;
	// decode is pure in-memory (safe under f.mu) and failure degrades to ""
	// (no key — the observer's activeID fallback), never an error.
	if sid := strategyIDFromCheckpoint(t.Checkpoint); sid != "" {
		payload[restoreKeyStrategyID] = sid
	}
	if isMustPersistEvent(typ) {
		payload[restoreKeyCapability] = t.Capability
		payload[restoreKeyPriority] = t.Priority
		if len(t.Dependencies) > 0 {
			deps := make([]string, len(t.Dependencies))
			copy(deps, t.Dependencies)
			payload[restoreKeyDependencies] = deps
		}
		if !t.Deadline.IsZero() {
			payload[restoreKeyDeadline] = t.Deadline.Format(time.RFC3339)
		}
		payload[restoreKeyRetryAttempts] = t.RetryPolicy.Attempts
		payload[restoreKeyRetryMax] = t.RetryPolicy.MaxRetries
		payload[restoreKeyCreatedAt] = t.CreatedAt.Format(time.RFC3339)
		if t.Checkpoint != nil {
			if b, err := MarshalCheckpoint(t.Checkpoint); err == nil {
				payload[restoreKeyCheckpointJSON] = string(b)
			} else {
				// The rebuilt task will resume without this checkpoint. Log
				// so the divergence is detectable; do not fail the transition.
				log.Error("taskfabric: checkpoint marshal failed (restore will lose progress)", "task_id", t.ID, "error", err)
			}
		}
	}
	return &pendingAppend{
		store:  f.store,
		typ:    typ,
		taskID: t.ID,
		event: &ares_events.Event{
			Type:       et,
			StreamID:   t.ID,
			ModuleName: "taskfabric",
			Payload:    payload,
			Timestamp:  ev.At,
		},
		seq: f.flushSeq,
	}
}

// flushAppends performs the deferred durable writes off-lock. It is registered
// with `defer f.flushAppends(&pending)` BEFORE `defer f.mu.Unlock()` so, by
// LIFO defer order, the unlock runs first and this flush runs immediately
// after — still within the same call (so W3 divergence logging stays
// synchronous with the mutating method) but with f.mu already released (so the
// store I/O never blocks other fabric operations). Takes a pointer so it reads
// the slice's final value populated during the method body.
//
// W3 Durability: must-persist events (TaskCreated, TaskCheckpointed,
// TaskCompleted, TaskFailed, TaskExpired) carry state the runtime relies on
// for recovery and replay. A failed append for these events is not silently
// swallowed — it is logged so a durable-state divergence (in-memory vs event
// log) is detectable. The in-memory state machine stays authoritative within a
// process (the append failure does not roll back the transition). Observability
// events remain best-effort and silent on failure.
func (f *Fabric) flushAppends(pending *[]*pendingAppend) {
	for _, p := range *pending {
		if p == nil {
			continue
		}
		// N7: wait until every earlier-recorded durable event has been
		// appended, so concurrent fabric calls flush in causal (record) order
		// and the store's per-stream version sequence never inverts.
		f.flushCond.L.Lock()
		for p.seq > f.flushedSeq+1 {
			f.flushCond.Wait()
		}
		var appendErr error
		if p.store != nil {
			appendErr = p.store.Append(context.Background(), p.taskID, []*ares_events.Event{p.event}, 0)
		}
		f.flushedSeq++
		f.flushCond.L.Unlock()
		f.flushCond.Broadcast()
		if appendErr != nil {
			if isMustPersistEvent(p.typ) {
				log.Error("taskfabric: must-persist event append failed (durable log diverges from memory)", "event_type", p.typ, "task_id", p.taskID, "error", appendErr)
			}
		}
	}
}

// isMustPersistEvent reports whether a lifecycle event is a must-persist
// transition (W3): the runtime's recovery/replay correctness depends on these
// events being in the durable log. Other events (Ready, Acquired, Started,
// Yielded, Preempted, Released, Stolen) are observability-only: they enrich
// the trace but are not required for state rebuild.
func isMustPersistEvent(typ EventType) bool {
	switch typ {
	case EventTaskCreated, EventTaskCheckpointed, EventTaskCompleted,
		EventTaskFailed, EventTaskExpired:
		return true
	default:
		return false
	}
}

// taskEventType maps the fabric's internal event type to the ares_events
// task.* event type. Unknown types map to "" and are never published.
func taskEventType(typ EventType) ares_events.EventType {
	switch typ {
	case EventTaskCreated:
		return ares_events.EventTaskCreated
	case EventTaskReady:
		return ares_events.EventTaskReady
	case EventTaskAcquired:
		return ares_events.EventTaskAcquired
	case EventTaskStarted:
		return ares_events.EventTaskStarted
	case EventTaskYielded:
		return ares_events.EventTaskYielded
	case EventTaskCheckpointed:
		return ares_events.EventTaskCheckpointed
	case EventTaskPreempted:
		return ares_events.EventTaskPreempted
	case EventTaskReleased:
		return ares_events.EventTaskReleased
	case EventTaskCompleted:
		return ares_events.EventTaskCompleted
	case EventTaskFailed:
		return ares_events.EventTaskFailed
	case EventTaskExpired:
		return ares_events.EventTaskExpired
	case EventTaskStolen:
		return ares_events.EventTaskStolen
	default:
		return ""
	}
}

// SetDependencies replaces a task's dependency list in place. It is the
// incremental-compile primitive behind runtime graph growth
// (TOOL_DAG_MAINLINE_DESIGN §4.1): an AddEdge/RemoveEdge on the live
// MutableDAG must move ONE task's scheduling shape, not rebuild the whole
// compiled batch.
//
// Only a READY task may be rewired. A LEASED/RUNNING/SUSPENDED task has an
// owner whose quantum was admitted against the dependency posture it read at
// acquire time — rewriting under it would let a task run before a dependency
// it never knew about. A terminal task's dependency list is a historical
// fact. Neither case is silent: the caller gets ErrTaskNotMutable (wrapped
// with the offending state) and must account for it.
//
// The rewrite is recorded as an observability-only event (EventTaskUpdated);
// see that constant for why it is deliberately not persisted.
//
// Args:
//   - id: the task to rewire.
//   - deps: the new dependency IDs; copied, so the caller's slice stays its
//     own (same isolation contract as Create).
//
// Returns:
//   - error: ErrTaskNotFound, or ErrTaskNotMutable when the state forbids it.
func (f *Fabric) SetDependencies(id string, deps []string) error {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	t, ok := f.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}
	if t.State != StateReady {
		return fmt.Errorf("%w: task %q is %s", ErrTaskNotMutable, id, t.State)
	}
	t.Dependencies = append([]string(nil), deps...)
	pending = append(pending, f.recordLocked(t, EventTaskUpdated))
	return nil
}

// UpdatePayload replaces the Payload inside a task's checkpoint envelope
// without recreating the task. It is the incremental-compile action behind a
// metadata-only graph change (SetNodeMetadata): a pure attribute patch must
// not cost a task rebuild, and must not reset the task's CreatedAt or its
// submission-time strategy attribution.
//
// Refused only while the task is RUNNING — a running quantum is already
// reading its payload. READY / LEASED / SUSPENDED / terminal tasks are all
// writable: nothing has committed to the payload yet, or it is history.
//
// The envelope's other fields (UserProfile, StepCheckpoint, UsedExperienceID,
// StrategyID) are preserved verbatim; the payload is copied so the caller
// cannot later mutate fabric-owned state.
//
// Args:
//   - id: the task whose payload to replace.
//   - payload: the new payload map (may be nil to clear it).
//
// Returns:
//   - error: ErrTaskNotFound, ErrCheckpointSchemaVersion, or
//     ErrTaskNotMutable when the task is RUNNING.
func (f *Fabric) UpdatePayload(id string, payload map[string]any) error {
	pending := make([]*pendingAppend, 0, 1)
	f.mu.Lock()
	defer f.flushAppends(&pending)
	defer f.mu.Unlock()
	t, ok := f.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}
	if t.State == StateRunning {
		return fmt.Errorf("%w: task %q is %s", ErrTaskNotMutable, id, t.State)
	}
	if t.Checkpoint == nil && len(payload) == 0 {
		// Nothing stored and nothing to store: do not invent an envelope
		// (that would flip TaskView.HasCheckpoint for no reason).
		return nil
	}
	pcopy := make(map[string]any, len(payload))
	for k, v := range payload {
		pcopy[k] = v
	}
	// Decode → replace → re-encode keeps every field the envelope already
	// carried (notably StrategyID, the submission-time attribution) intact
	// and keeps this the single decode path for checkpoints (W3).
	dc, err := DecodeCheckpoint(t.Checkpoint)
	if err != nil {
		return err
	}
	dc.Payload = pcopy
	t.Checkpoint = EncodeCheckpoint(dc)
	pending = append(pending, f.recordLocked(t, EventTaskUpdated))
	return nil
}

// Dependents returns the ids of every task that lists id in its Dependencies
// — the reverse-edge index the incremental compiler needs to migrate
// successors onto a replacement node (TOOL_DAG_MAINLINE_DESIGN §4.1,
// ChangeReplaceNode). Sorted for deterministic callers.
//
// Args:
//   - id: the dependency to look up (need not itself exist as a task).
//
// Returns:
//   - []string: the dependent task ids (empty when there are none).
func (f *Fabric) Dependents(id string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, 1)
	for taskID, t := range f.tasks {
		for _, dep := range t.Dependencies {
			if dep == id {
				out = append(out, taskID)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// Delete removes a task from the fabric entirely (fusion plan C4 review #2:
// submitted collaboration graphs are EPHEMERAL — results are harvested by the
// caller before deletion, so long-running kernels must not accumulate zombie
// entries from failed/timed-out graphs).
//
// Allowed only from states with no in-flight or resumable execution: READY,
// COMPLETED, FAILED. LEASED/RUNNING/SUSPENDED are refused with
// ErrTaskUndeletable — their quanta must finish or expire through the normal
// paths; callers retry deletion afterwards if needed.
//
// Deletion emits NO event on purpose: it is housekeeping for graphs whose
// results were already harvested, not a durable-state transition. The memory
// store therefore cannot replay these tasks after a restart — accepted,
// because replay value of harvested ephemeral work is nil.
func (f *Fabric) Delete(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}
	switch t.State {
	case StateReady, StateCompleted, StateFailed:
		delete(f.tasks, id)
		return nil
	default:
		return ErrTaskUndeletable
	}
}

// IDs returns a snapshot of every task id in the fabric (any state). Used by
// housekeeping sweeps — e.g. the collaboration-graph janitor that deletes
// stale terminal tasks from previous submissions.
func (f *Fabric) IDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.tasks))
	for id := range f.tasks {
		out = append(out, id)
	}
	return out
}

// LeaseEntry is one non-terminal task's scheduling-relevant state — the
// read-only view the runtime introspection panel consumes (monitoring.md
// Domain B: which tasks hold leases, how long until expiry, checkpoint
// progress, dependency posture).
type LeaseEntry struct {
	// TaskID is the fabric task identifier.
	TaskID string `json:"taskID"`
	// Capability is the required capability.
	Capability string `json:"capability"`
	// State is the current lifecycle state (never terminal in a snapshot).
	State TaskState `json:"state"`
	// Priority drives preemption decisions.
	Priority int `json:"priority"`
	// Owner is the lease-holding agent; empty when the task is unowned.
	Owner string `json:"owner"`
	// Epoch is the lease acquisition counter (stale-renew observability).
	Epoch uint64 `json:"epoch"`
	// ExpiresAt is the lease expiry; zero when unowned.
	ExpiresAt time.Time `json:"expiresAt"`
	// HasCheckpoint reports whether durable progress exists.
	HasCheckpoint bool `json:"hasCheckpoint"`
	// Dependencies are the task's prerequisite IDs (copied).
	Dependencies []string `json:"dependencies"`
}

// LeaseSnapshot returns a point-in-time copy of every non-terminal task,
// ordered by TaskID for stable rendering. Terminal tasks (COMPLETED/FAILED)
// are excluded so the snapshot stays bounded by live work rather than
// accumulating history. Purely read-only: everything is copied under f.mu and
// no transition/renew side effects can fire (unlike CheckExpiredLeases,
// which is a WRITE path and must never be used for observation).
func (f *Fabric) LeaseSnapshot() []LeaseEntry {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]LeaseEntry, 0, len(f.tasks))
	for id, t := range f.tasks {
		if t.State == StateCompleted || t.State == StateFailed {
			continue
		}
		e := LeaseEntry{
			TaskID:        id,
			Capability:    t.Capability,
			State:         t.State,
			Priority:      t.Priority,
			Owner:         t.Owner,
			HasCheckpoint: t.Checkpoint != nil,
		}
		if t.Lease != nil {
			e.Epoch = t.Lease.Epoch
			e.ExpiresAt = t.Lease.ExpiresAt
		}
		if len(t.Dependencies) > 0 {
			e.Dependencies = append([]string(nil), t.Dependencies...)
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TaskID < out[j].TaskID })
	return out
}

// TaskView is the full task row for the Tasks page (dashboard.md §5): unlike
// LeaseEntry it includes terminal states, the accumulated quantum count, and
// the owner across the whole lifecycle — so the UI can render the task board
// (Ready/Running/Done) and the DAG from one source.
type TaskView struct {
	// TaskID is the fabric task identifier.
	TaskID string `json:"taskID"`
	// Capability is the required capability.
	Capability string `json:"capability"`
	// State is the current lifecycle state (including terminal).
	State TaskState `json:"state"`
	// Priority drives preemption decisions.
	Priority int `json:"priority"`
	// Owner is the current lease holder ("" when unowned/terminal).
	Owner string `json:"owner"`
	// Quantum is the total execution quanta across all holders.
	Quantum int `json:"quantum"`
	// HasCheckpoint reports whether durable progress exists.
	HasCheckpoint bool `json:"hasCheckpoint"`
	// Dependencies are the task's prerequisite IDs (copied).
	Dependencies []string `json:"dependencies"`
	// Origin is the creating agent id ("" = root/user-submitted).
	Origin string `json:"origin"`
	// CreatedAt / UpdatedAt are the lifecycle timestamps.
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// TaskSnapshot returns a point-in-time copy of EVERY task, including terminal
// ones, ordered by TaskID. It powers the Tasks page board + DAG (dashboard.md
// §5: "看真正的任务依赖关系"). Terminal tasks are included so the Done column
// and the dependency closure render correctly. Purely read-only: everything
// is copied under f.mu, no write path fires.
func (f *Fabric) TaskSnapshot() []TaskView {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]TaskView, 0, len(f.tasks))
	for id, t := range f.tasks {
		v := TaskView{
			TaskID:        id,
			Capability:    t.Capability,
			State:         t.State,
			Priority:      t.Priority,
			Owner:         t.Owner,
			Quantum:       t.Quantum,
			HasCheckpoint: t.Checkpoint != nil,
			Origin:        t.Origin,
			CreatedAt:     t.CreatedAt,
			UpdatedAt:     t.UpdatedAt,
		}
		if len(t.Dependencies) > 0 {
			v.Dependencies = append([]string(nil), t.Dependencies...)
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TaskID < out[j].TaskID })
	return out
}
