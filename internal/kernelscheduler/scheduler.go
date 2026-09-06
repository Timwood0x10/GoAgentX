package kernelscheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/ares_events"
	"github.com/Timwood0x10/ares/internal/aresrecovery"
	"github.com/Timwood0x10/ares/internal/core/models"
	apperrors "github.com/Timwood0x10/ares/internal/errors"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// Scheduler is the "no leader" execution engine (ares-runtime.md:
// "Agents are not orchestrated. They are scheduled."). It repeatedly drains
// the fabric's ReadyTasks — the work source — and for each ready task:
//
//	Schedule (capability-aware) → Acquire (lease + fencing) → RunQuantum (one
//	agent step) → finalize (COMPLETED / FAILED / SUSPENDED).
//
// No leader decides "B is done, now run C"; the fabric's dependency-completed
// states make C ready. The scheduler is only a consumer of ReadyTasks.
//
// Failure policy: a scheduling or execution failure for one task is logged and
// the loop continues — one bad task must never take down the scheduler.
//
// Dynamic executor registration (W1): RegisterExecutor / UnregisterExecutor
// let the recovery loop inject a replacement agent at runtime so a recovered
// task is executed by a real executor, not a phantom agent. execMu guards the
// executor map for concurrent register/unregister/lookup from drain goroutines.
type Scheduler struct {
	fabric    *taskfabric.Fabric
	executors map[string]CapabilityExecutor
	// execMu guards the executors map for dynamic register/unregister (W1).
	// A separate lock avoids reentrancy with the fabric mutex during drain.
	execMu sync.RWMutex
	// tracker supplies real per-agent Load/Confidence to Schedule
	// (v0.3.0 GAP4: real load tracking instead of a static placeholder).
	tracker *LoadTracker
	// PollInterval is how often ReadyTasks is drained (default 500ms).
	PollInterval time.Duration
	// ttl is the lease granted to each winning agent.
	ttl time.Duration
	// eventStore is the shared EventStore the Task Fabric publishes lifecycle
	// events to. When set, the scheduler subscribes to dependency-relevant
	// task events (completed/failed/ready/… ) and drains immediately on each,
	// so a task whose DAG dependencies have just completed runs without
	// waiting for the next poll tick (GAP 6: event-driven DAG completion).
	// Nil keeps pure 500ms polling (backward compatible).
	eventStore ares_events.EventStore
	// maxConcurrent caps how many ready tasks run in parallel during one
	// drain (work stealing: multiple agents pick up tasks concurrently).
	// <= 0 falls back to the executor count (bounded by 32).
	maxConcurrent int
	// scheduled counts successfully executed tasks (for observability).
	// atomic: incremented from concurrent drain goroutines (work stealing).
	Scheduled atomic.Int64
	// noCandidateMu guards lastNoCandidateLog for throttling unschedulable-task
	// logs.
	noCandidateMu      sync.Mutex
	lastNoCandidateLog time.Time
	// governance is the P3 cognitive-execution budget provider (agentfabric:
	// token/tool budgets + deadline). Nil skips enforcement (backward
	// compatible). When set, execute() checks the budget at each quantum
	// boundary (before CheckResource / after ConsumeResource+Deadline) so a
	// budget-exhausted agent yields the task back instead of burning tokens it
	// cannot afford (aresos-plan.md P3: cooperative yield, not hard preempt).
	governance *agentfabric.Fabric
	// boundExecutors maps taskID → executorID for W1 recovery executors. A
	// recovery executor is bound to exactly one task: execute() only offers it
	// as a candidate for that task, never for another READY task, so a
	// replacement spawned for a recovered task cannot hijack new tasks.
	// Guarded by execMu.
	boundExecutors map[string]string
	// attribution is the optional W4 execution-outcome source. When wired,
	// execute() records every finalized outcome (agent, capability, success)
	// so the evolution feedback loop can read attribution and push derived
	// confidence into the tracker. Nil skips recording (backward compatible).
	attribution *aresrecovery.ExecutionAttribution
	// agents is the optional agentfabric.Fabric whose live IDLE agents are
	// schedulable candidates (aresos-agentos-plan B1: the scheduler's candidate
	// pool comes from the agentfabric dynamic population). Every drain re-queries the fabric, so a spawned
	// agent becomes schedulable immediately and a killed one disappears — no
	// explicit registry sync. Nil keeps the static executor registry only
	// (backward compatible with tests and minimal wiring).
	agents *agentfabric.Fabric
	// decisions records every scheduling decision (candidate pool + scores +
	// winner) for the Scheduling Observatory (dashboard.md §7). It is written
	// in executeWithCandidates and read via DecisionsSnapshot — the panel
	// explains WHY a task went to a particular agent.
	decisions *DecisionRecorder
	// quantumHook is the optional observational extension at the quantum
	// boundary (see quantum_hook.go). Nil = no hook (backward compatible).
	// Guarded by execMu alongside the executor registry so runtime
	// registration races with the drain loop stay safe.
	quantumHook QuantumHook
	// shadowHook is the optional real-execution shadow A/B capture point
	// (closure plan Step 4 / N-1). When wired, every successfully finalized
	// task is handed to the hook so the evolution layer can buffer it and
	// later execute a candidate strategy on it in isolation. The hook fires
	// on the drain path and MUST NOT block (see shadow.go). Nil = no shadow
	// capture (backward compatible).
	shadowHook ShadowExecutionHook
	// running reports whether the drain loop is actually running (K5: the
	// System Runtime readiness gate must mean "drain loop alive", not
	// "object exists"). Set at Run entry, cleared on exit.
	running atomic.Bool
	// recoveryHint, when wired, is invoked at the stale-winner boundary: the
	// winner died between candidate build and executor lookup and no capable
	// replacement exists yet, so waiting for the lease TTL is the only other
	// way out. The hint asks the recovery loop to sweep NOW (B1).
	//
	// It must be non-blocking — the caller is a drain goroutine on the hot
	// path. The wiring side (cmd/ares) satisfies this with a capacity-1
	// channel and a drop-on-full send, matching the sweep semaphore's own
	// drop semantics. Nil keeps the pre-B1 behavior for the leader/SDK paths
	// that have no recovery loop.
	//
	// Deliberately a callback rather than a recovery dependency: the
	// architecture red line forbids kernelscheduler importing runtime
	// (TestSchedulerMustNotImportRuntime).
	recoveryHint func(taskID string)
}

// WithRecoveryHint wires the stale-winner recovery trigger (B1). fn is called
// when a leased task's winner has died and no capable replacement executor
// exists, so the task would otherwise stall for the full lease TTL. fn MUST
// NOT block: it runs on a drain goroutine.
//
// Args:
//   - fn: the non-blocking sweep trigger; nil disables the hint.
//
// Returns:
//   - *Scheduler: the receiver, for chaining.
func (s *Scheduler) WithRecoveryHint(fn func(taskID string)) *Scheduler {
	s.execMu.Lock()
	defer s.execMu.Unlock()
	s.recoveryHint = fn
	return s
}

// notifyRecovery fires the recovery hint under a read lock (the hint may be
// re-wired at runtime). Safe when no hint is wired.
func (s *Scheduler) notifyRecovery(taskID string) {
	s.execMu.RLock()
	hint := s.recoveryHint
	s.execMu.RUnlock()
	if hint != nil {
		hint(taskID)
	}
}

// hasRecoveryHint reports whether a recovery loop is wired to receive
// stale-winner nominations. The stale-winner path needs this BEFORE releasing:
// releasing clears the lease and thereby removes the task from
// CheckExpiredLeases' scope, so a release with no recovery consumer would
// strand the task instead of merely delaying it.
func (s *Scheduler) hasRecoveryHint() bool {
	s.execMu.RLock()
	defer s.execMu.RUnlock()
	return s.recoveryHint != nil
}

// Running reports whether the scheduler's drain loop is currently running.
// K5 readiness semantics: a constructed-but-not-running scheduler must never
// report Ready — the System Runtime gate polls this before adoption.
func (s *Scheduler) Running() bool { return s.running.Load() }

// noCandidateLogInterval throttles "no capable candidate" logs to one per
// window — the condition is a waiting state, not an error worth per-poll noise.
const noCandidateLogInterval = 5 * time.Second

// WithGovernance attaches the P3 budget provider (agentfabric.Fabric). It is
// wired by the kernel lifecycle once the agent fabric exists; without it the
// scheduler enforces nothing (backward compatible with tests and minimal
// wiring). The provider is read-only here — the scheduler checks and consumes,
// it never mutates budgets.
func (s *Scheduler) WithGovernance(g *agentfabric.Fabric) *Scheduler {
	s.governance = g
	return s
}

// budgetOK reports whether the winning agent may start a new quantum. It is
// the P3 pre-quantum gate: deadline first (a deadline-expired agent is dead
// weight), then the tool budget for this quantum's expected 1 tool round. A
// denial is a cooperative yield — the scheduler returns the task to READY
// instead of burning a quantum the agent cannot afford.
func (s *Scheduler) budgetOK(winner string) bool {
	if s.governance == nil {
		return true
	}
	if over, err := s.governance.DeadlineExceeded(winner); err == nil && over {
		return false
	}
	ok, err := s.governance.CheckResource(winner, 0, 1)
	if err != nil {
		return true // unknown agent (not spawned via fabric) → don't block
	}
	return ok
}

// consumeBudget records the winning agent's quantum consumption (1 tool round)
// after a completed quantum. Errors (budget exceeded mid-quantum) are logged,
// not fatal — the task already ran; the next quantum's gate stops further work.
func (s *Scheduler) consumeBudget(winner string) {
	if s.governance == nil {
		return
	}
	if err := s.governance.ConsumeResource(winner, 0, 1); err != nil {
		log.Warn("kernel scheduler: agent budget consumption", "agent", winner, "error", err)
	}
}

// New creates a scheduler over a fabric with the given
// executors (agentID → CapabilityExecutor). A nil tracker allocates a private
// one; pass a shared tracker to keep Load/Confidence consistent with the fabric
// dispatch path (executeFabricTask).
//
// Args:
//   - fabric: the Task Fabric backing this scheduler.
//   - executors: the agent registry (agentID → CapabilityExecutor).
//   - tracker: per-agent load/confidence source; nil creates a private one.
//
// Returns:
//   - *Scheduler: ready to Run.
func New(fabric *taskfabric.Fabric, executors map[string]CapabilityExecutor, tracker *LoadTracker) *Scheduler {
	if tracker == nil {
		tracker = NewLoadTracker()
	}
	// P1-1: copy the initial executor map so the scheduler owns its own
	// map. The caller's map and the scheduler's map are now independent —
	// the caller must use RegisterExecutor/UnregisterExecutor to mutate
	// the live registry. Without this copy, both sides hold the same map
	// reference guarded by DIFFERENT mutexes (caller's lock vs execMu),
	// and concurrent read/write is a fatal race.
	ownExecutors := make(map[string]CapabilityExecutor, len(executors))
	for k, v := range executors {
		ownExecutors[k] = v
	}
	return &Scheduler{
		fabric:         fabric,
		executors:      ownExecutors,
		boundExecutors: make(map[string]string),
		tracker:        tracker,
		PollInterval:   500 * time.Millisecond,
		ttl:            5 * time.Minute,
		decisions:      newDecisionRecorder(),
	}
}

// WithAttribution attaches the W4 execution-outcome source (aresrecovery.
// ExecutionAttribution). When set, execute() records every finalized outcome
// after the quantum. Returns the scheduler for chaining.
func (s *Scheduler) WithAttribution(a *aresrecovery.ExecutionAttribution) *Scheduler {
	s.attribution = a
	return s
}

// WithAgentFabric attaches the agent lifecycle fabric so every live, IDLE,
// executable fabric agent is a schedulable candidate (B1: single scheduling
// loop — the scheduler recognizes only the unified Agent). It is wired by the kernel lifecycle once the
// fabric exists; nil keeps the static executor registry only. Returns the
// scheduler for chaining.
func (s *Scheduler) WithAgentFabric(f *agentfabric.Fabric) *Scheduler {
	s.agents = f
	return s
}

// WithMaxConcurrent caps how many ready tasks run in parallel per drain
// (work stealing). <= 0 falls back to the executor count. Returns the
// scheduler for chaining.
func (s *Scheduler) WithMaxConcurrent(n int) *Scheduler {
	s.maxConcurrent = n
	return s
}

// WithTTL overrides the lease duration granted on Acquire (default 5 minutes).
// The scheduler heartbeats Renew at ttl/3 (minimum 5s) while a quantum runs,
// so steps longer than ttl are not requeued by lease expiry. Returns the
// scheduler for chaining.
func (s *Scheduler) WithTTL(ttl time.Duration) *Scheduler {
	if ttl > 0 {
		s.ttl = ttl
	}
	return s
}

// Run drains ReadyTasks until ctx is cancelled or the fabric becomes nil.
// It runs synchronously; callers start it in a goroutine. Panics from one
// task's execution are recovered so a single bad step cannot kill the loop.
//
// When an event store is wired (WithEventStore), the scheduler also drains
// immediately on dependency-relevant task events (completed / failed /
// ready / created), so a task whose DAG dependencies just finished runs
// without waiting for the next poll tick (GAP 6). The periodic poll remains
// as a safety net for transitions that do not publish events.
//
// Args:
//   - ctx: lifetime of the scheduling loop.
func (s *Scheduler) Run(ctx context.Context) {
	if s.fabric == nil {
		log.Warn("kernel scheduler: fabric nil, scheduler disabled")
		return
	}
	// K5: the readiness flag goes up before the loop blocks so an Adopt-time
	// Ready gate observes "drain loop alive" as soon as Run begins, and goes
	// down when the loop exits so a crashed scheduler never keeps reporting
	// Ready.
	s.running.Store(true)
	defer s.running.Store(false)
	// Guard against zero or negative PollInterval which would panic
	// in time.NewTicker (B34). Fall back to preemptInterval which
	// applies the same safe default.
	pollInterval := s.PollInterval
	if pollInterval <= 0 {
		pollInterval = s.preemptInterval()
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	// Cooperative-preemption watcher (BUG-KSCHED-001): drain() blocks on
	// wg.Wait() until every dispatched quantum finishes, so preemption
	// checked only at drain entry could never observe a RUNNING task — the
	// branch was unreachable through the production loop. This managed worker
	// (deterministic exit on ctx.Done, per-sweep recover per code_rules)
	// scans independently of the blocking drain. Preemption stays
	// cooperative: it only mutates durable state; the stale holder's late
	// completion is rejected by the fencing token.
	preemptTicker := time.NewTicker(s.preemptInterval())
	defer preemptTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-preemptTicker.C:
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Error("kernel scheduler: panic in preemption sweep, continuing", "panic", r)
						}
					}()
					s.PreemptLowerPriority(s.fabric.ResumableTasks())
				}()
			}
		}
	}()

	// Subscribe to dependency-relevant task events when a store is wired.
	// The channel is nil (and the select case inert) when eventStore is nil,
	// preserving the pure-polling path.
	var events <-chan *ares_events.Event
	if s.eventStore != nil {
		ch, err := s.eventStore.Subscribe(ctx, ares_events.EventFilter{
			Types: []ares_events.EventType{
				ares_events.EventTaskCreated,
				ares_events.EventTaskReady,
				ares_events.EventTaskCompleted,
				ares_events.EventTaskFailed,
				// A yielded task (SUSPENDED) resumes on the next drain; draining
				// on the yield event skips the poll interval between quanta.
				ares_events.EventTaskYielded,
			},
		})
		if err != nil {
			log.Warn("kernel scheduler: event subscribe failed, polling only", "error", err)
		} else {
			events = ch
			log.Info("kernel scheduler: event-driven drain enabled (task lifecycle events)")
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.safeDrain(ctx)
		case _, ok := <-events:
			// A dependency-relevant task event arrived: drain now instead of
			// waiting up to one poll interval. When the subscription channel
			// is closed, disable the case (nil channel blocks forever) so the
			// loop falls back to pure polling instead of busy-spinning on a
			// closed channel (N5: closed-events spin).
			if !ok {
				log.Info("kernel scheduler: event subscription closed, polling only")
				events = nil
				continue
			}
			s.safeDrain(ctx)
		}
	}
}

// preemptInterval returns the preemption sweep period, guarding against a
// zero/negative PollInterval (time.NewTicker panics on a non-positive tick).
func (s *Scheduler) preemptInterval() time.Duration {
	if s.PollInterval > 0 {
		return s.PollInterval
	}
	return 500 * time.Millisecond
}

// safeDrain recovers a panic from one drain so the scheduling loop survives a
// single bad drain (M2: kernel loops must not crash the process). Per-task
// panics are already recovered inside drain; this guards the drain itself
// (e.g. a panic inside ReadyTasks).
func (s *Scheduler) safeDrain(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("kernel scheduler: panic in drain, continuing", "panic", r)
		}
	}()
	s.drain(ctx)
}

// WithEventStore wires the shared EventStore so the scheduler drains on task
// lifecycle events (GAP 6: event-driven DAG completion) instead of waiting
// for the next poll tick. Returns the scheduler for chaining.
func (s *Scheduler) WithEventStore(store ares_events.EventStore) *Scheduler {
	s.eventStore = store
	return s
}

// drain executes every currently ready task. When the scheduler is configured
// for concurrency (WithMaxConcurrent), ready tasks run in parallel (bounded by
// maxConcurrent) so multiple agents pick up work at the same time — the
// work-stealing substrate at the scheduler side. Panics from one task's
// execution are recovered so a single bad step cannot kill the loop.
// TODO(tech-debt): the per-agent local ready-queue design
// (taskfabric.AgentQueue/Steal) was removed as unused
// (v0.3.0 review P1: Steal idled unused — wire it or delete it); the shared ReadyTasks()
// queue drained concurrently by bounded goroutines IS the stealing substrate.
// Re-introduce per-agent queues only if profiling shows contention.
func (s *Scheduler) drain(ctx context.Context) {
	// Zombie-executor reconciliation: a fabric agent killed via chaos/
	// governance disappears from the fabric, but its STATIC registration (the
	// configured peers' executors and every spawned agent's executor) stayed
	// in the map forever — the stale-winner lookup could then execute a task
	// on a dead agent's registration, and the registry grew unboundedly with
	// each spawn. Every drain drops registrations whose fabric entry is gone,
	// EXCEPT recovery-bound replacements (they are deliberately outside the
	// fabric; they are unregistered at terminal state).
	s.reconcileFabricDeaths()

	// Work source: READY tasks (new work) plus SUSPENDED tasks (a yielded
	// quantum the scheduler continues via re-acquire — the SUSPENDED
	// semantics lock: "Continue is the Scheduler's decision via re-acquire").
	tasks := s.fabric.ResumableTasks()
	if len(tasks) == 0 {
		return
	}
	// Priority preemption (v0.3.0 review: fabric.Preempt was production-
	// unused): if a READY task outranks a task that is RUNNING from a
	// previous drain, cooperatively preempt the lower one so a capable
	// executor can pick up the higher-priority work. Preempt hands the task
	// back to READY with its checkpoint preserved (it resumes later), and the
	// fencing token guarantees only the current holder is affected. This runs
	// BEFORE this drain spawns its own goroutines — between quanta — so a
	// quantum is never interrupted mid-step.
	s.PreemptLowerPriority(tasks)
	limit := s.maxConcurrent
	if limit <= 0 {
		limit = s.ExecutorCount()
	}
	if limit <= 0 {
		limit = 1
	}
	if limit > 32 {
		limit = 32 // sanity cap: a drain never spawns unbounded goroutines
	}

	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for _, taskID := range tasks {
		select {
		case <-ctx.Done():
			return
		default:
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(id string) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if recover() != nil {
					log.Error("kernel scheduler: panic executing task, continuing", "task_id", id)
				}
			}()
			if err := s.execute(ctx, id); err != nil {
				s.logFailure(id, err)
			}
		}(taskID)
	}
	wg.Wait()
}

// preemptLowerPriority cooperatively preempts any RUNNING task whose priority
// is lower than the highest-priority READY task in this drain, so the next
// drain can hand the executor to the higher-priority work. No-op when no
// priority information exists (all zeros) — the scheduler never churns a
// running task on a tie or on unset priorities. The preempted task keeps its
// checkpoint and returns to READY for a later quantum.
func (s *Scheduler) PreemptLowerPriority(ready []string) {
	// B11: guard must also check fabric agents, not just static executors.
	// In production mode (agent fabric wired), the static executor count may
	// be 0 while fabric agents are the real candidate source.
	hasCandidates := s.ExecutorCount() > 0
	if !hasCandidates && s.agents != nil {
		for _, id := range s.agents.Agents() {
			if s.agents.IsIdle(id) {
				if a, err := s.agents.Get(id); err == nil && a != nil && a.Executable() {
					hasCandidates = true
					break
				}
			}
		}
	}
	if !hasCandidates || len(ready) == 0 {
		return
	}
	maxReady := 0
	for _, id := range ready {
		if tk, err := s.fabric.Task(id); err == nil && tk.Priority > maxReady {
			maxReady = tk.Priority
		}
	}
	if maxReady <= 0 {
		return
	}
	for _, rt := range s.fabric.RunningTasks() {
		if rt.Priority >= maxReady {
			continue
		}
		if err := s.fabric.Preempt(rt.ID, rt.Owner, rt.Epoch, "higher-priority task arrived"); err != nil {
			// A concurrently-finalized task (already COMPLETED/FAILED) or a
			// stale epoch is a benign race, not worth log spam.
			continue
		}
		log.Info("kernel scheduler: preempted for higher-priority work", "task_id", rt.ID, "priority", rt.Priority, "max_ready", maxReady)
	}
}

// logFailure logs a task failure, throttling ErrNoCapableCandidate: an
// unschedulable task is a legitimate "waiting for a capable agent" state that
// the scheduler re-polls every interval, so it must not spam the log. Other
// errors are logged every time (they are transient and need attention).
func (s *Scheduler) logFailure(taskID string, err error) {
	if err == taskfabric.ErrNoCapableCandidate {
		now := time.Now()
		s.noCandidateMu.Lock()
		defer s.noCandidateMu.Unlock()
		if now.Sub(s.lastNoCandidateLog) < noCandidateLogInterval {
			return
		}
		s.lastNoCandidateLog = now
	}
	log.Error("kernel scheduler: execute task failed", "task_id", taskID, "error", err)
}

// Submission-time metadata (UserProfile + Payload + UsedExperienceID) rides in
// the task's Checkpoint slot inside a *taskfabric.CheckpointEnvelope (W3
// schema, unversioned-v0 → versioned-v1 migration). Without the envelope the
// executor saw profile==nil and degraded to an empty executeByType fallback —
// a silent no-op that still reported success (the serve result-reflux bug
// chain). The scheduler re-wraps EVERY quantum's returned checkpoint (yield
// AND done) back into an envelope (EncodeCheckpoint), so the submission
// metadata survives a yield: RunQuantum overwrites the task Checkpoint with
// the step's checkpoint, and re-wrapping it inside the envelope means the next
// quantum's toModelTask can still restore UserProfile/Payload (v0.3.0 review
// Bug 3: yield→resume otherwise lost the profile and degraded to
// executeByType). nil before the first quantum runs.

// execute runs the full fabric path for one task: Schedule → Acquire →
// RunQuantum (delegating the actual work to the winning sub-agent) →
// finalize. Errors are returned to the caller for logging; the fabric
// state machine (RetryPolicy) decides requeue vs. final failure.
func (s *Scheduler) execute(ctx context.Context, taskID string) error {
	// Build the candidate list from the registered executors so scheduling is
	// always consistent with what can actually run. Each candidate declares its
	// OWN capabilities (from the agent's Type), NOT the task's — the scorer
	// compares the task's required capability against what the agent can do.
	// Load/Confidence come from the live tracker (v0.3.0 GAP4): real busy
	// fraction and historical success rate, not static placeholders.
	//
	// W1 recovery binding: a recovery executor bound to THIS task is the only
	// candidate (the replacement must run the task it was spawned for). Bound
	// executors of OTHER tasks are excluded so a replacement can never hijack
	// a different READY task.
	execs := s.allExecutors()
	if boundID, bound := s.boundFor(taskID); bound {
		cands := make([]taskfabric.Candidate, 0, 1)
		if agent, ok := execs[boundID]; ok && agent != nil {
			cands = append(cands, taskfabric.Candidate{
				AgentID:      boundID,
				Capabilities: []string{string(agent.Type())},
				Load:         s.tracker.Load(boundID),
				Confidence:   s.tracker.Confidence(boundID),
				Priority:     s.tracker.Priority(boundID),
			})
		}
		if len(cands) == 0 {
			// The bound executor is gone (already unregistered) — fall through
			// to the normal pool so the task is not stranded.
			return s.executeUnbound(ctx, taskID)
		}
		return s.executeWithCandidates(ctx, taskID, cands)
	}
	return s.executeUnbound(ctx, taskID)
}

// executeUnbound runs the fabric path for a task with no recovery binding:
// the candidate pool is every registered, unbound executor whose capability
// overlaps the task, plus every live IDLE fabric agent (B1).
func (s *Scheduler) executeUnbound(ctx context.Context, taskID string) error {
	execs := s.allExecutors()
	cands := make([]taskfabric.Candidate, 0, len(execs))
	for agentID, agent := range execs {
		if agent == nil {
			continue
		}
		if s.isBoundToAnyTask(agentID) {
			continue
		}
		// C1: when the fabric is wired (peer mode), the fabric's live
		// population is the SINGLE candidate source — the static registrations
		// of the configured sub-agents have a managed copy in the fabric, so a
		// chaos kill takes effect on the next drain. Only recovery-bound
		// executors (reserved for their task) stay in the static pool.
		// (isBoundToAnyTask already checked above, so this is just s.agents != nil)
		if s.agents != nil {
			continue
		}
		cands = append(cands, taskfabric.Candidate{
			AgentID:      agentID,
			Capabilities: []string{string(agent.Type())},
			Load:         s.tracker.Load(agentID),
			Confidence:   s.tracker.Confidence(agentID),
			Priority:     s.tracker.Priority(agentID),
		})
	}
	// B1: live fabric agents are candidates too. Every drain re-queries the
	// fabric, so a freshly spawned IDLE agent becomes schedulable immediately
	// and a killed one disappears (spawn/kill reflected in the candidate set immediately).
	cands = s.appendFabricCandidates(cands, execs)
	return s.executeWithCandidates(ctx, taskID, cands)
}

// executeWithCandidates runs the shared Schedule → Acquire → RunQuantum →
// finalize path for a prebuilt candidate list. The task capability is read
// for W4 attribution at the outcome boundary.
func (s *Scheduler) executeWithCandidates(ctx context.Context, taskID string, cands []taskfabric.Candidate) error {
	tk, err := s.fabric.Task(taskID)
	if err != nil {
		return err
	}
	if len(cands) == 0 {
		// KernelError carries task attribution while keeping the sentinel on the
		// chain, so errors.Is(err, taskfabric.ErrNoCapableCandidate) still matches.
		return apperrors.Kernel("schedule", "no_capable_candidate", taskID, "", taskfabric.ErrNoCapableCandidate)
	}
	// W4: capability-specific confidence. The candidate builders only know
	// agentID; the task capability is available here, so re-resolve each
	// candidate's confidence against (agentID, task capability) before
	// Schedule scores them. Without a capability override this falls back to
	// the agent-level value (design-fix: per-capability feedback is consumed).
	for i := range cands {
		cands[i].Confidence = s.tracker.ConfidenceFor(cands[i].AgentID, tk.Capability)
	}
	winner, epoch, err := s.fabric.Schedule(taskID, cands, s.ttl)
	// Record the scheduling decision for the Observatory (dashboard.md §7):
	// candidate breakdown + winner. Recorded even on failure (e.g. no capable
	// candidate) so the panel explains why a task stayed unscheduled.
	if s.decisions != nil {
		d := ScheduleDecision{
			TaskID:     taskID,
			Capability: tk.Capability,
			Candidates: scoreCandidates(tk.Capability, cands),
			Time:       time.Now(),
		}
		if err == nil {
			d.Winner = winner
			d.Epoch = epoch
		} else {
			d.Err = err.Error()
		}
		s.decisions.Record(d)
	}
	if err != nil {
		return err
	}
	// C1: when the fabric is wired, resolve the winner through the fabric
	// FIRST — the fabric copy is the live, lifecycle-managed agent (kill/
	// recovery affect it), so a same-id static registration must not shadow
	// it. Only when the fabric has no live agent for the winner (legacy
	// mode, or a recovery-bound static executor) fall back to the registry.
	var executor CapabilityExecutor
	var ok bool
	if s.agents != nil {
		executor = s.fabricExecutor(winner)
	}
	if executor == nil {
		executor, ok = s.LookupExecutor(winner)
		if !ok || executor == nil {
			// The candidate snapshot is stale: the winner died (or became
			// non-executable) between candidate build and executor lookup.
			//
			// The task must only be released to someone who can actually pick
			// it up. Releasing clears the lease, which also removes the task
			// from CheckExpiredLeases' scope — so releasing into an empty
			// world would strand it permanently, which is strictly worse than
			// the TTL stall. Hence three cases, in order of preference:
			//
			//  1. Another capable executor exists → release; the next drain
			//     re-schedules within one poll interval (EDGE-4).
			//  2. No capable executor, but a recovery loop is wired → release
			//     AND nominate the task to it (B1). Recovery gives the task a
			//     replacement execution body promptly, instead of the task
			//     waiting out the full lease TTL. This is the production path
			//     (cmd/ares peer mode).
			//  3. Neither → keep the lease. TTL expiry is then the ONLY
			//     recovery trigger available, and keeping the lease is what
			//     makes the task visible to CheckExpiredLeases. Leader/SDK/
			//     chaos-sandbox paths land here.
			//
			// Release is epoch-fenced (only the current holder can release)
			// and PRESERVES the checkpoint, so E1's "resume, don't restart"
			// contract holds in cases 1 and 2.
			if s.HasCapableExecutor(taskID) {
				if releaseErr := s.fabric.Release(taskID, winner, epoch); releaseErr != nil {
					log.Error("kernel scheduler: release for stale winner failed", "task_id", taskID, "winner", winner, "error", releaseErr)
				}
				return nil
			}
			if s.hasRecoveryHint() {
				if releaseErr := s.fabric.Release(taskID, winner, epoch); releaseErr != nil {
					log.Error("kernel scheduler: release for stale winner failed", "task_id", taskID, "winner", winner, "error", releaseErr)
					return nil
				}
				log.Warn("kernel scheduler: winner no longer executable; released to READY and nominated for recovery", "winner", winner, "task_id", taskID)
				s.notifyRecovery(taskID)
				return nil
			}
			log.Warn("kernel scheduler: winner no longer executable and no capable replacement or recovery loop exists; task stays leased until TTL expiry", "winner", winner, "task_id", taskID)
			return nil
		}
	}
	// Track the busy slot while the quantum runs so the next Schedule sees the
	// real load (v0.3.0 GAP4); end records the outcome for confidence.
	// Preserve the submission metadata across the quantum: the task's current
	// checkpoint is the meta envelope written by submitFabricTask or by a
	// previous quantum (yield/done re-wraps below). Capturing it here — before
	// RunQuantum overwrites the task Checkpoint — is what keeps UserProfile
	// alive through an arbitrary number of yield→resume cycles.
	meta, decodeErr := taskfabric.DecodeCheckpoint(tk.Checkpoint)
	if decodeErr != nil {
		log.Warn("kernel scheduler: decode checkpoint failed", "task_id", taskID, "error", decodeErr)
	}
	// P3 pre-quantum gate: if the winner's budget/deadline is exhausted, yield
	// the task back (release the lease) so another capable agent (or a later
	// quantum after ResetResource) can pick it up. This closes the P3 loop at
	// the scheduler boundary — the fabric's state machine (Release→READY)
	// drives the requeue, matching the plan's "budget.exceeded → yield()".
	if !s.budgetOK(winner) {
		if releaseErr := s.fabric.Release(taskID, winner, epoch); releaseErr != nil {
			log.Error("kernel scheduler: release for budget-exhausted failed", "task_id", taskID, "winner", winner, "error", releaseErr)
		}
		return nil
	}
	s.tracker.Begin(winner)
	// Quantum boundary hooks (observational): before the quantum runs and
	// after it finalizes. See quantum_hook.go for the contract.
	s.beforeQuantum(ctx, taskID, winner)
	// Lease heartbeat: renew the winner's lease while the quantum runs so a
	// long step (> TTL) is not requeued by lease expiry and executed a second
	// time concurrently. The heartbeat goroutine is managed by an errgroup
	// (code_rules #8) and stops when the quantum ends, the scheduler context
	// is cancelled, or renewal fails (ownership lost — preemption/expiry).
	renewStop := make(chan struct{})
	qg, qgCtx := errgroup.WithContext(ctx)
	qg.Go(func() error {
		interval := s.ttl / 3
		if interval < 5*time.Second {
			interval = 5 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewStop:
				return nil
			case <-qgCtx.Done():
				return nil
			case <-ticker.C:
				if rerr := s.fabric.Renew(taskID, winner, epoch, s.ttl); rerr != nil {
					log.Error("kernel scheduler: lease renew failed, stopping heartbeat", "task_id", taskID, "winner", winner, "error", rerr)
					return nil
				}
			}
		}
	})
	// stopHeartbeat closes renewStop and waits for the heartbeat goroutine
	// EXACTLY ONCE. sync.Once removes the double-close hazard: the normal
	// path and the panic-recovery defer both call it, and a panic occurring
	// AFTER the normal close (e.g. inside qg.Wait or endQuantumOutcome) must
	// not close an already-closed channel (that panic would itself unwind,
	// skip EndNeutral, and leak the load slot — the very bug this guards).
	var stopOnce sync.Once
	stopHeartbeat := func() {
		stopOnce.Do(func() {
			close(renewStop)
			_ = qg.Wait()
		})
	}
	// Panic guard: a panic inside the executor unwinds through RunQuantum and
	// would skip endQuantumOutcome, leaking the winner's LoadTracker slot
	// forever (load never decrements → Score multiplies by (1-clamp01(load))
	// = 0 → the agent is permanently unschedulable). The deferred release only
	// fires on the panic path: the normal path clears the flag right after
	// endQuantumOutcome, so there is no double-release.
	slotReleased := false
	defer func() {
		if r := recover(); r != nil {
			stopHeartbeat()
			if !slotReleased {
				// Log BEFORE releasing: the stack trace is the only forensic
				// trail for an executor panic, and the load slot is released
				// so the agent stays schedulable (the fabric's expired-lease
				// requeue reclaims the stuck task separately).
				log.Error("kernel scheduler: panic in executor, releasing load slot", "task_id", taskID, "agent", winner, "panic", r)
				s.tracker.EndNeutral(winner)
				slotReleased = true
			}
		}
	}()
	// C2.1: capture the quantum's wall-clock duration before RunQuantum so
	// endQuantumOutcome can attribute real latency to the deterministic scorer
	// (C2.2). The old Record() path passed 0,0,0, which made the
	// latency/retry/recover weights dead and collapsed every score to
	// 0.70×successRate+0.30 (no added information).
	//
	// B-1 (review P0-2): the retry count is DERIVED from the RunQuantum error
	// via quantumRetries — never read from RetryPolicy.Attempts (cumulative,
	// would over-attribute) and never re-read from the task (races another
	// drain). See quantumRetries for the full rationale.
	quantumStart := time.Now()
	err = s.fabric.RunQuantum(taskID, winner, epoch, s.buildQuantumStep(ctx, executor, tk, meta))
	quantumLatency := time.Since(quantumStart)
	retries := quantumRetries(err)
	// Release the busy slot and attribute the outcome (see endQuantumOutcome).
	stopHeartbeat()
	s.afterQuantum(ctx, taskID, winner, err)
	s.endQuantumOutcome(winner, tk.Capability, taskID, err, quantumLatency, retries)
	slotReleased = true
	// Step 4 (closure plan N-1): hand the finalized task to the shadow A/B
	// executor so a candidate strategy can be executed on it in isolation.
	// Contract: the hook buffers and returns — it never blocks the drain
	// path. Only successful finalizations are sampled; a failed quantum says
	// nothing about how a candidate would have run the task.
	if s.shadowHook != nil && err == nil {
		s.shadowHook.OnTaskFinalized(s.ToModelTask(tk))
	}
	// P3 post-quantum bookkeeping: record the quantum's consumption (1 tool
	// round) so the next gate sees the new balance. Runs even on step errors —
	// the quantum did execute (or partially execute) and spent budget.
	if s.governance != nil {
		s.consumeBudget(winner)
	}
	if err == nil {
		s.Scheduled.Add(1)
	}
	s.unbindRecoveryExecutorAfterTerminal(taskID)
	return err
}

// unbindRecoveryExecutorAfterTerminal unregisters the recovery executor bound
// to taskID once the task reaches a terminal state, so the executor map does
// not grow unboundedly and the replacement is not offered as a candidate for
// other tasks. No-op while the task can still run again (READY/RUNNING/
// SUSPENDED).
func (s *Scheduler) unbindRecoveryExecutorAfterTerminal(taskID string) {
	tk2, tkErr := s.fabric.Task(taskID)
	if tkErr != nil {
		return
	}
	if tk2.State != taskfabric.StateCompleted && tk2.State != taskfabric.StateFailed {
		return
	}
	if boundID := s.unbindFor(taskID); boundID != "" {
		s.UnregisterExecutor(boundID)
		log.Info("kernel scheduler: unregistered recovery executor after task reached state", "executor", boundID, "task_id", taskID, "state", tk2.State)
	}
}

// reconcileFabricDeaths drops static executor registrations whose agent has
// disappeared from the wired Agent Fabric (kill/retire). Recovery-bound
// executors are skipped — they intentionally live outside the fabric and are
// cleaned up by the terminal-state unbind. No-op when no fabric is wired.
func (s *Scheduler) reconcileFabricDeaths() {
	if s.agents == nil {
		return
	}
	for id := range s.allExecutors() {
		if s.isBoundToAnyTask(id) {
			continue // recovery replacements are unregistered at terminal state
		}
		if _, err := s.agents.Get(id); err != nil {
			log.Info("kernel scheduler: unregistering executor — agent no longer in fabric (killed or retired)", "executor", id)
			s.UnregisterExecutor(id)
		}
	}
}

// buildQuantumStep constructs the QuantumStep closure RunQuantum executes:
// it runs the executor's step and translates the outcome into fabric state
// transitions (error → Fail, !Done → Yield with checkpoint, Done → Complete
// with the worker result riding in the checkpoint envelope).
//
// Args:
//   - ctx: the quantum's execution context (cancelled on scheduler shutdown).
//   - executor: the winning agent running this step.
//   - tk: the fabric task snapshot taken at acquire time.
//   - meta: the submission metadata decoded from the task checkpoint; it is
//     re-wrapped around EVERY quantum output so UserProfile/Payload survive
//     arbitrary yield→resume cycles.
func (s *Scheduler) buildQuantumStep(
	ctx context.Context,
	executor CapabilityExecutor,
	tk *taskfabric.Task,
	meta taskfabric.DecodedCheckpoint,
) taskfabric.QuantumStep {
	return func() (any, bool, error) {
		out, stepErr := executor.ExecuteStep(ctx, s.ToModelTask(tk))
		if stepErr != nil {
			// A step error flows to fabric.Fail, which requeues (retry budget)
			// or finalizes FAILED — the fabric owns the retry policy.
			return nil, false, stepErr
		}
		if out == nil {
			return nil, false, apperrors.Kernel("run_quantum", "nil_step_outcome", tk.ID, executor.ID(), ErrNilStepOutcome)
		}
		if out.Result != nil && out.Result.Error != "" {
			return nil, false, apperrors.Kernel("run_quantum", "step_error", tk.ID, executor.ID(), errors.New(out.Result.Error))
		}
		if !out.Done {
			// Yield (P1.1 Execution Quantum): the quantum made progress but the
			// task is not complete. RunQuantum's not-done branch SUSPENDEDs the
			// task with this checkpoint preserved; the next drain re-acquires
			// it and the next quantum resumes from this PCB.
			return taskfabric.EncodeCheckpoint(taskfabric.DecodedCheckpoint{
				UserProfile:      meta.UserProfile,
				Payload:          meta.Payload,
				UsedExperienceID: meta.UsedExperienceID,
				StrategyID:       meta.StrategyID,
				SessionID:        meta.SessionID,
				StepCheckpoint:   out.Checkpoint,
			}), false, nil
		}
		// Done: carry the worker's real output back through the fabric so the
		// dispatch layer can surface actual items instead of an "ok"
		// placeholder. The result rides in the quantum checkpoint:
		// RunQuantum's done branch stores it via CompleteWithCheckpoint, and
		// the dispatcher reads it back after polling the task to COMPLETED.
		outMap := map[string]any{"result": "ok"}
		if res := out.Result; res != nil {
			if items := res.Items; len(items) > 0 {
				outMap["items"] = items
			}
			if res.Reason != "" {
				outMap["reason"] = res.Reason
			}
			if len(res.Metadata) > 0 {
				outMap["metadata"] = res.Metadata
			}
		}
		// Re-wrap the step output in the metadata envelope so the dispatcher's
		// outcomeFromFabric unwraps it on COMPLETED (same as pre-quantum).
		return taskfabric.EncodeCheckpoint(taskfabric.DecodedCheckpoint{
			UserProfile:      meta.UserProfile,
			Payload:          meta.Payload,
			UsedExperienceID: meta.UsedExperienceID,
			StrategyID:       meta.StrategyID,
			StepCheckpoint:   outMap,
		}), true, nil
	}
}

// quantumRetries derives the number of retries THIS quantum consumed from the
// RunQuantum error. RunQuantum calls fabric.Fail at most ONCE per quantum —
// only on a step error — and Fail is the sole incrementer of
// RetryPolicy.Attempts (a monotonically increasing LIFETIME counter that never
// resets). Therefore:
//
//   - 1 when the step failed (Fail ran, the task consumed one retry);
//   - 0 otherwise (completed/yielded, or the quantum never started).
//
// Start-stage sentinels (fencing ErrNotOwner/ErrEpochMismatch, ErrIllegalState,
// ErrTaskNotFound) are NOT step failures — Fail never ran — so they contribute
// 0. Executor step errors cannot equal taskfabric's sentinels, so errors.Is
// cleanly separates the two.
//
// This derivation is used instead of reading RetryPolicy.Attempts (cumulative,
// over-attributes later quanta and inflates the deterministic scorer's retry
// component with retry depth) or re-reading the task after RunQuantum (races
// another drain that may have re-acquired and re-failed our requeued task,
// attributing ITS retry to US). See review P0-2.
func quantumRetries(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, taskfabric.ErrNotOwner) ||
		errors.Is(err, taskfabric.ErrEpochMismatch) ||
		errors.Is(err, taskfabric.ErrIllegalState) ||
		errors.Is(err, taskfabric.ErrTaskNotFound) {
		return 0
	}
	return 1
}

// endQuantumOutcome releases the winner's busy slot and attributes the
// quantum outcome to W4 feedback.
//
// A benign fencing rejection (cooperative preemption handed the task back
// while the stale holder was still mid-step) is NOT the executor's failure:
// recording it as one would poison the agent's success rate toward 0, and
// Score's confidence factor would make the preempted task permanently
// unschedulable. Such rejections end NEUTRAL — load is released but no
// success/failure enters the history, and W4 attribution is skipped.
func (s *Scheduler) endQuantumOutcome(winner, capability, taskID string, err error, latency time.Duration, retries int) {
	if errors.Is(err, taskfabric.ErrNotOwner) || errors.Is(err, taskfabric.ErrEpochMismatch) {
		s.tracker.EndNeutral(winner)
		log.Debug("kernel scheduler: quantum ended by preemption fencing (benign); outcome not attributed", "task_id", taskID)
		return
	}
	s.tracker.End(winner, err == nil)
	// W4 evolution feedback: record the outcome for the feedback loop. The
	// attribution is read by the EvolutionFeedbackAdapter and pushed back into
	// the tracker's confidence override (SetAgentConfidence) so the next
	// Schedule sees the evolution-derived confidence.
	//
	// C2.1: RecordWithMetrics carries the real quantum latency and retry
	// budget so the deterministic scorer (C2.2) has non-degenerate evidence.
	// Recovery count stays 0 (normal quantum: no replacement was needed).
	if s.attribution != nil {
		s.attribution.RecordWithMetrics(winner, capability, err == nil, latency, retries, 0)
	}
}

// toModelTask maps a fabric Task back to the models.Task shape the sub-agent
// executor expects. The submission-time metadata (UserProfile + Payload +
// UsedExperienceID) rides in the fabric Checkpoint slot inside a
// *taskfabric.CheckpointEnvelope (W3 schema); restoring it here is what lets
// the executor take the real LLM path instead of degrading to an empty
// fallback result (profile==nil → executeByType). A genuine progress
// checkpoint (plain map, written by RunQuantum) is preserved in the payload so
// a resumed quantum can observe where the previous step left off. Decode goes
// through the single shared protocol (taskfabric.DecodeCheckpoint) — the same
// path recovery and every other consumer use.
func (s *Scheduler) ToModelTask(tk *taskfabric.Task) *models.Task {
	t := models.NewTask(tk.ID, models.AgentType(tk.Capability), nil)
	if tk.Checkpoint == nil {
		return t
	}
	dc, err := taskfabric.DecodeCheckpoint(tk.Checkpoint)
	if err != nil {
		log.Warn("kernel scheduler: toModelTask decode checkpoint failed", "task_id", tk.ID, "error", err)
		t.Payload = map[string]any{"checkpoint": tk.Checkpoint}
		return t
	}
	t.UserProfile = reifyUserProfile(dc.UserProfile)
	t.Payload = dc.Payload
	t.UsedExperienceID = dc.UsedExperienceID
	// E1: the submission-time strategy attribution rides to the executor so
	// the sub-agent's task.completed/failed events carry the same key the
	// fabric's own events do — RuntimeObserver attributes fitness samples by
	// it, and a promote mid-task must not re-credit the new strategy.
	t.StrategyID = dc.StrategyID
	// M2: SessionID rides to the executor so the plannerCognition can look
	// up the per-session L2 graph registry.
	t.SessionID = dc.SessionID
	// A resumed quantum observes where the previous step left off (Bug 3):
	// the step checkpoint is surfaced to the executor as payload["checkpoint"].
	if dc.StepCheckpoint != nil {
		if t.Payload == nil {
			t.Payload = make(map[string]any)
		}
		t.Payload["checkpoint"] = dc.StepCheckpoint
	}
	return t
}

// reifyUserProfile converts a decoded envelope UserProfile (typed pointer, or
// a raw map after a JSON round-trip) back into the *models.UserProfile the
// executor expects. A value that cannot be reified yields nil (the executor
// then degrades exactly as before).
func reifyUserProfile(v any) *models.UserProfile {
	switch up := v.(type) {
	case *models.UserProfile:
		return up
	case nil:
		return nil
	default:
		if buf, err := json.Marshal(up); err == nil {
			var p models.UserProfile
			if err := json.Unmarshal(buf, &p); err == nil {
				return &p
			}
		}
		return nil
	}
}

// SchedulerSnapshot is a point-in-time, read-only view of the scheduler's
// observable state — the runtime introspection panel's Domain A read-model
// (monitoring.md §2.2: queue depth, preemption cadence, executor inventory,
// budget/governance wiring, per-agent load). Every field is a copy taken
// under the appropriate lock; callers never touch scheduling internals.
type SchedulerSnapshot struct {
	// PollInterval is the drain cadence; PreemptInterval the preemption sweep
	// cadence (both as configured / defaulted).
	PollInterval    time.Duration `json:"pollInterval"`
	PreemptInterval time.Duration `json:"preemptInterval"`
	// TTL is the lease granted to each winning agent.
	TTL time.Duration `json:"ttl"`
	// MaxConcurrent is the per-drain parallelism cap (after defaulting).
	MaxConcurrent int `json:"maxConcurrent"`
	// EventDriven reports whether an event store subscription accelerates
	// dependency completion (GAP 6) on top of polling.
	EventDriven bool `json:"eventDriven"`
	// Executors is the static + spawned executor count; BoundExecutors the
	// recovery-bound one-task-one-executor subset (W1).
	Executors      int `json:"executors"`
	BoundExecutors int `json:"boundExecutors"`
	// Scheduled is the total successfully executed task count.
	Scheduled int64 `json:"scheduled"`
	// ReadyTasks is the fabric's current resumable (ready/suspended) depth —
	// the queue-depth signal for the panel's queue gauge.
	ReadyTasks int `json:"readyTasks"`
	// GovernanceWired / AgentFabricWired report optional subsystem wiring so
	// the panel can annotate whether budgets and dynamic population are live.
	GovernanceWired  bool `json:"governanceWired"`
	AgentFabricWired bool `json:"agentFabricWired"`
	// Load is the per-agent load/confidence snapshot from the tracker.
	Load LoadTrackerSnapshot `json:"load"`
}

// DecisionsSnapshot returns the recorded scheduling decisions (newest first)
// for the Scheduling Observatory (dashboard.md §7). Purely read-only: the
// recorder's lock is internal; no scheduling write path is touched.
func (s *Scheduler) DecisionsSnapshot() []ScheduleDecision {
	if s.decisions == nil {
		return nil
	}
	return s.decisions.Snapshot()
}

// Snapshot returns the read-only view. It acquires only reader locks
// (execMu.RLock, tracker/fabric internal locks), never the drain write path,
// and is safe to call concurrently with Run (monitoring.md Phase 0:
// "纯只读、持读锁拷贝、返回不可变副本").
func (s *Scheduler) Snapshot() SchedulerSnapshot {
	s.execMu.RLock()
	execN := len(s.executors)
	boundN := len(s.boundExecutors)
	s.execMu.RUnlock()

	snap := SchedulerSnapshot{
		PollInterval:     s.preemptInterval(), // same guard/default as both tickers
		PreemptInterval:  s.preemptInterval(),
		TTL:              s.ttl,
		MaxConcurrent:    s.maxConcurrent,
		EventDriven:      s.eventStore != nil,
		Executors:        execN,
		BoundExecutors:   boundN,
		Scheduled:        s.Scheduled.Load(),
		GovernanceWired:  s.governance != nil,
		AgentFabricWired: s.agents != nil,
	}
	if snap.MaxConcurrent <= 0 {
		snap.MaxConcurrent = execN // mirror drain's default: executor count
	}
	if snap.MaxConcurrent <= 0 {
		snap.MaxConcurrent = 1
	}
	if snap.MaxConcurrent > 32 {
		snap.MaxConcurrent = 32 // same sanity cap as drain
	}
	if s.fabric != nil {
		snap.ReadyTasks = len(s.fabric.ResumableTasks())
	}
	if s.tracker != nil {
		snap.Load = s.tracker.Snapshot()
	}
	return snap
}
