package kernel

import (
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// RegisterExecutor dynamically registers an executor under agentID so the
// scheduler can execute tasks assigned to it (W1: production-grade recovery
// 闭环). The recovery loop calls this after spawning a replacement agent so
// the new agent is a real executor, not a phantom. Safe for concurrent use
// with drain goroutines: execMu guards the map.
//
// Args:
//   - agentID: the replacement agent's identity.
//   - executor: the CapabilityExecutor (must be non-nil).
func (s *Scheduler) RegisterExecutor(agentID string, executor CapabilityExecutor) {
	if agentID == "" || executor == nil {
		return
	}
	s.execMu.Lock()
	defer s.execMu.Unlock()
	s.executors[agentID] = executor
	log.Info("kernel scheduler: registered replacement executor", "executor", agentID)
}

// RegisterExecutorIfAbsent atomically registers executor under agentID only
// when no executor is registered there yet, and reports whether it stored the
// argument (true) or an existing one already occupied the slot (false),
// returning the winning executor either way. It closes the check-then-act race
// in the on-demand executor creation path (sdk ensureExecutor): two concurrent
// Submits for the same unregistered capability would each LookupExecutor
// (miss), build an agent, and double-write, silently discarding one. Doing the
// check and the set under a single execMu write lock makes the first writer
// win and the second reuse it.
//
// Args:
//   - agentID: the capability/agent identity to register under.
//   - executor: the candidate executor (must be non-nil).
//
// Returns:
//   - CapabilityExecutor: the executor now occupying the slot (existing or new).
//   - bool: true if the argument was stored, false if an existing one won.
func (s *Scheduler) RegisterExecutorIfAbsent(agentID string, executor CapabilityExecutor) (CapabilityExecutor, bool) {
	if agentID == "" || executor == nil {
		return nil, false
	}
	s.execMu.Lock()
	defer s.execMu.Unlock()
	if existing, ok := s.executors[agentID]; ok && existing != nil {
		return existing, false
	}
	s.executors[agentID] = executor
	log.Info("kernel scheduler: registered executor (if-absent)", "executor", agentID)
	return executor, true
}

// RegisterExecutorForTask registers an executor bound to exactly one task
// (W1 recovery). The executor is only ever offered as a candidate for taskID
// — execute() filters it out for every other READY task, so a replacement
// spawned for a recovered task can never hijack a brand-new task. When the
// task reaches a terminal state (COMPLETED / FAILED) execute() unregisters
// the bound executor automatically.
//
// Args:
//   - taskID: the recovered task the executor is bound to.
//   - agentID: the replacement executor's identity.
//   - executor: the CapabilityExecutor (must be non-nil).
func (s *Scheduler) RegisterExecutorForTask(taskID, agentID string, executor CapabilityExecutor) {
	if taskID == "" || agentID == "" || executor == nil {
		return
	}
	s.execMu.Lock()
	defer s.execMu.Unlock()
	s.executors[agentID] = executor
	s.boundExecutors[taskID] = agentID
	log.Info("kernel scheduler: registered recovery executor bound to task", "executor", agentID, "task_id", taskID)
}

// boundFor returns the executor id bound to taskID, if any. Safe for
// concurrent use.
func (s *Scheduler) boundFor(taskID string) (string, bool) {
	s.execMu.RLock()
	defer s.execMu.RUnlock()
	id, ok := s.boundExecutors[taskID]
	return id, ok
}

// unbindFor removes the executor binding for taskID and returns the bound
// executor id ("" when none). Callers use it to unregister a recovery
// executor once its task reaches a terminal state.
func (s *Scheduler) unbindFor(taskID string) string {
	s.execMu.Lock()
	defer s.execMu.Unlock()
	id := s.boundExecutors[taskID]
	delete(s.boundExecutors, taskID)
	return id
}

// UnregisterExecutor removes an executor from the registry and clears any
// task binding it had. The recovery loop calls this when a replacement agent
// itself fails, so stale executors are not selected for scheduling. Safe for
// concurrent use.
//
// Args:
//   - agentID: the agent to remove.
func (s *Scheduler) UnregisterExecutor(agentID string) {
	if agentID == "" {
		return
	}
	s.execMu.Lock()
	defer s.execMu.Unlock()
	delete(s.executors, agentID)
	for taskID, boundID := range s.boundExecutors {
		if boundID == agentID {
			delete(s.boundExecutors, taskID)
		}
	}
}

// HasCapableExecutor reports whether a registered, unbound executor can
// execute taskID (capability overlap > 0). The recovery loop calls this for
// each requeued task to decide whether a replacement executor is needed:
// when an existing executor can already resume the task, no spawn is
// required and the task simply returns to READY.
func (s *Scheduler) HasCapableExecutor(taskID string) bool {
	tk, err := s.fabric.Task(taskID)
	if err != nil {
		return false
	}
	if _, ok := s.boundFor(taskID); ok {
		return true
	}
	execs := s.allExecutors()
	for agentID, agent := range execs {
		if agent == nil {
			continue
		}
		if s.isBoundToAnyTask(agentID) {
			continue
		}
		// C1: same single-source rule as executeUnbound — with the fabric
		// wired, unbound static registrations are managed fabric agents.
		// (isBoundToAnyTask already checked above, so this is just s.agents != nil)
		if s.agents != nil {
			continue
		}
		cand := taskfabric.Candidate{
			Capabilities: []string{string(agent.Type())},
			Confidence:   s.tracker.ConfidenceFor(agentID, tk.Capability),
			Load:         s.tracker.Load(agentID),
		}
		if taskfabric.Score(tk.Capability, cand) > 0 {
			return true
		}
	}
	// B1: a live, IDLE, executable fabric agent can also resume the task.
	if s.agents != nil {
		for _, id := range s.agents.Agents() {
			if _, ok := execs[id]; ok {
				continue
			}
			if !s.agents.IsIdle(id) {
				continue
			}
			a, err := s.agents.Get(id)
			if err != nil || a == nil || !a.Executable() {
				continue
			}
			cand := taskfabric.Candidate{Capabilities: a.Capabilities}
			if taskfabric.Score(tk.Capability, cand) > 0 {
				return true
			}
		}
	}
	return false
}

// isBoundToAnyTask reports whether agentID is reserved as a recovery executor
// for ANY task. When the fabric is the single candidate source (peer mode,
// s.agents != nil), unbound static registrations are managed as fabric agents
// and must not be offered as candidates — only these reserved executors stay
// in the static pool. It also serves the executeUnbound / HasCapableExecutor
// exclusion: a recovery executor bound to any task must not be offered for a
// different task (the bound branch already reserved it for its own task), so
// the two callers share one predicate.
func (s *Scheduler) isBoundToAnyTask(agentID string) bool {
	s.execMu.RLock()
	defer s.execMu.RUnlock()
	for _, boundID := range s.boundExecutors {
		if boundID == agentID {
			return true
		}
	}
	return false
}

// executorCount returns the current number of registered executors under a
// read lock. Used by drain to compute the concurrency limit.
func (s *Scheduler) ExecutorCount() int {
	s.execMu.RLock()
	defer s.execMu.RUnlock()
	return len(s.executors)
}

// lookupExecutor safely retrieves an executor under a read lock.
func (s *Scheduler) LookupExecutor(agentID string) (CapabilityExecutor, bool) {
	s.execMu.RLock()
	defer s.execMu.RUnlock()
	e, ok := s.executors[agentID]
	return e, ok && e != nil
}

// allExecutors returns a snapshot of the executor map under a read lock. The
// drain goroutines iterate this snapshot to build the candidate list.
func (s *Scheduler) allExecutors() map[string]CapabilityExecutor {
	s.execMu.RLock()
	defer s.execMu.RUnlock()
	out := make(map[string]CapabilityExecutor, len(s.executors))
	for k, v := range s.executors {
		out[k] = v
	}
	return out
}

// Capabilities lists the distinct executor types across the registry. The
// graph-submission endpoint uses it to reject requests no peer can serve.
func (s *Scheduler) Capabilities() []string {
	s.execMu.RLock()
	defer s.execMu.RUnlock()
	set := make(map[string]bool, len(s.executors))
	out := make([]string, 0, len(s.executors))
	for _, ex := range s.executors {
		if ex == nil {
			continue
		}
		c := string(ex.Type())
		if !set[c] {
			set[c] = true
			out = append(out, c)
		}
	}
	return out
}
