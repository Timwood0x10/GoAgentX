package taskfabric

import (
	"strings"
	"time"
)

// Reaper periodically harvests terminal fabric tasks so the in-memory task
// map does not grow monotonically across a server lifetime (M2-⑤, §9:
// "fabric 不自动回收终态任务"). The reaper is a housekeeping loop, NOT a
// correctness mechanism — terminal tasks are garbage once their results have
// been read by the caller.
//
// Harvesting rules:
//   - Only COMPLETED and FAILED tasks are eligible (READY/LEASED/RUNNING/
//     SUSPENDED are live or resumable and must survive).
//   - A session prefix filter ("sess/<id>/") limits harvesting to L2 session
//     tasks; non-session tasks (collab-*, peer-task-*) are invisible to this
//     reaper. The collab GC loop (cmd/ares/collab_graph.go) already handles
//     its own prefix.
//   - A grace period prevents harvesting a task that just completed — the
//     planner/answer path may still be reading its envelope when the state
//     transition lands. The default grace is 30s.
//
// The reaper runs as a managed background loop (like runCollabGCLoop), exited
// by closing its done channel. It is NOT on the scheduler drain path.
//
// TODO(tech-debt): no production caller wires this reaper yet, so terminal L2
// session tasks still accumulate. Wiring it needs the keep-set from
// agentfabric.SessionRegistry.SessionIDs (harvest only tasks whose session is
// gone) rather than the wall-clock grace below — a session that outlives the
// grace period would otherwise have its early predecessors harvested while the
// planner still reads their envelopes for context assembly (decision C).
type Reaper struct {
	fabric      *Fabric
	prefix      string
	gracePeriod time.Duration
}

// NewReaper creates a terminal-task reaper scoped to the given session
// prefix. The prefix is the node-ID stem of L2 session tasks
// ("sess/<sessionID>/"); non-matching tasks are skipped. A gracePeriod of 0
// defaults to 30s.
func NewReaper(fabric *Fabric, prefix string, gracePeriod time.Duration) *Reaper {
	if gracePeriod <= 0 {
		gracePeriod = 30 * time.Second
	}
	return &Reaper{fabric: fabric, prefix: prefix, gracePeriod: gracePeriod}
}

// Sweep performs one harvesting pass: every terminal task whose ID starts
// with the reaper's prefix and whose UpdatedAt is older than the grace
// period is deleted. Returns the number of tasks harvested.
//
// In-flight tasks (LEASED/RUNNING/SUSPENDED) are refused by Delete's guard
// and skipped — they finish naturally and become harvestable on the next
// sweep.
func (r *Reaper) Sweep() int {
	if r == nil || r.fabric == nil {
		return 0
	}
	removed := 0
	now := time.Now()
	for _, id := range r.fabric.IDs() {
		if r.prefix != "" && !strings.HasPrefix(id, r.prefix) {
			continue
		}
		tk, err := r.fabric.Task(id)
		if err != nil {
			continue
		}
		if tk.State != StateCompleted && tk.State != StateFailed {
			continue
		}
		// Grace period: a task that just transitioned to terminal may
		// still be read by the session's answer path.
		if now.Sub(tk.UpdatedAt) < r.gracePeriod {
			continue
		}
		if derr := r.fabric.Delete(id); derr == nil {
			removed++
		}
	}
	return removed
}

// Run starts the periodic harvesting loop. It exits when done is closed. The
// interval controls the sweep cadence; a shorter interval reclaims memory
// faster but scans the task map more often. The default (when interval <= 0)
// is 60s — matching the collab GC cadence.
//
// Each sweep that harvested anything is logged: deleting production tasks
// without a trace is exactly the silent action this codebase forbids.
func (r *Reaper) Run(done <-chan struct{}, interval time.Duration) {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if n := r.Sweep(); n > 0 {
				log.Info("taskfabric: reaper harvested terminal tasks",
					"count", n, "prefix", r.prefix)
			}
		}
	}
}
