// Package introspect implements the runtime introspection panel read-model
// (monitoring.md): it periodically PULLS point-in-time snapshots from the
// kernel scheduler, task fabric and agent fabric, keeps only the LATEST one
// (bounded memory), and serves both a JSON API (/api/v1/introspect/*) and an
// embedded single-page UI. It is strictly read-only — sources expose snapshot
// methods, never write paths.
package introspect

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// Snapshot is one full panel refresh: everything the UI renders in a frame.
type Snapshot struct {
	// TS is when the collector produced this snapshot.
	TS time.Time `json:"ts"`
	// Seq is a monotonically increasing frame counter.
	Seq uint64 `json:"seq"`
	// Kernel is the scheduler's Domain A view.
	Kernel kernel.SchedulerSnapshot `json:"kernel"`
	// Fabric is the task/lease Domain B view.
	Fabric []taskfabric.LeaseEntry `json:"fabric"`
	// Agents is the lifecycle Domain C view.
	Agents []agentfabric.AgentView `json:"agents"`
	// Chaos is the #12 Phase 3 chaos-subsystem status (shadow sandbox health
	// + live-injection state). Omitted when the chaos source is nil.
	Chaos *ChaosStatus `json:"chaos,omitempty"`
	// Collab is the agent collaboration graph (who handed work to whom).
	// Omitted when the collab source is nil.
	Collab *CollabSnapshot `json:"collab,omitempty"`
	// Tasks is the full task board (all states incl. terminal) + quantum
	// counts for the Tasks page (dashboard.md §5). Omitted when the source
	// is nil.
	Tasks []taskfabric.TaskView `json:"tasks,omitempty"`
	// Decisions is the scheduling-decision trail (candidates + scores +
	// winner) for the Scheduler page (dashboard.md §7). Omitted when the
	// source is nil.
	Decisions []kernel.ScheduleDecision `json:"decisions,omitempty"`
}

// Sources abstract the three subsystems so tests can fake them
// (code_rules: interfaces defined at the consumer).
type Sources struct {
	Kernel func() kernel.SchedulerSnapshot
	Fabric func() []taskfabric.LeaseEntry
	Agents func() []agentfabric.AgentView
	// Chaos reports the chaos-subsystem status (monitoring.md #12 Phase 3).
	// A nil Chaos source omits the field from the snapshot.
	Chaos func() ChaosStatus
	// Collab reports the agent collaboration graph. A nil Collab source omits
	// the field from the snapshot.
	Collab func() CollabSnapshot
	// Tasks reports the full task board (all states, quantum counts). A nil
	// Tasks source omits the field.
	Tasks func() []taskfabric.TaskView
	// Decisions reports the scheduling-decision trail. A nil Decisions source
	// omits the field.
	Decisions func() []kernel.ScheduleDecision
}

// Collector produces Snapshots from Sources.
type Collector struct {
	src Sources
	seq atomic.Uint64
}

// NewCollector builds a collector over the given sources.
func NewCollector(src Sources) *Collector {
	return &Collector{src: src}
}

// Collect assembles one Snapshot. A nil source function yields its zero value
// so a partially wired runtime still renders (missing domains show empty).
func (c *Collector) Collect() Snapshot {
	snap := Snapshot{TS: time.Now(), Seq: c.seq.Add(1)}
	if c.src.Kernel != nil {
		snap.Kernel = c.src.Kernel()
	}
	if c.src.Fabric != nil {
		snap.Fabric = c.src.Fabric()
	}
	if c.src.Agents != nil {
		snap.Agents = c.src.Agents()
	}
	if c.src.Chaos != nil {
		cs := c.src.Chaos()
		snap.Chaos = &cs
	}
	if c.src.Collab != nil {
		cs := c.src.Collab()
		snap.Collab = &cs
	}
	if c.src.Tasks != nil {
		snap.Tasks = c.src.Tasks()
	}
	if c.src.Decisions != nil {
		snap.Decisions = c.src.Decisions()
	}
	return snap
}

// Store holds the latest snapshot. Memory stays O(1) by design (monitoring.md
// §3.1 principle 3: bounded read-model) — history lives in the event log, not
// here.
type Store struct {
	latest atomic.Pointer[Snapshot]

	// eventsMu/events form the bounded activity ring (#panel feedback);
	// separate lock from the atomic latest pointer.
	eventsMu sync.Mutex
	events   []TimelineEntry
}

// Set publishes a new latest snapshot.
func (s *Store) Set(snap Snapshot) { s.latest.Store(&snap) }

// Latest returns the most recent snapshot, or nil before the first collect.
func (s *Store) Latest() *Snapshot { return s.latest.Load() }

// TimelineEntry is one understated activity-feed row (#panel feedback): who
// died, who took work, what got preempted. Terse text, color carried by Level.
type TimelineEntry struct {
	// TS is the event time.
	TS time.Time `json:"ts"`
	// Kind is "agent", "task" or "recovery".
	Kind string `json:"kind"`
	// Level drives the dot color: ok | info | warn | danger.
	Level string `json:"level"`
	// Type is the raw event type (e.g. agent.stopped).
	Type string `json:"type"`
	// Text is the terse human line (pre-formatted server-side).
	Text string `json:"text"`
	// AgentID / TaskID enable cross-highlighting in the UI.
	AgentID string `json:"agent_id,omitempty"`
	TaskID  string `json:"task_id,omitempty"`
}

// maxTimelineEntries bounds the activity ring (monitoring.md: bounded
// read-model — history beyond this lives in the event log/archive).
const maxTimelineEntries = 300

// PushEvent appends to the bounded activity ring (newest last), guarded by
// Store.eventsMu.
func (s *Store) PushEvent(e TimelineEntry) {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	s.events = append(s.events, e)
	if len(s.events) > maxTimelineEntries {
		s.events = s.events[len(s.events)-maxTimelineEntries:]
	}
}

// Events returns up to limit most-recent entries, newest FIRST (feed order).
func (s *Store) Events(limit int) []TimelineEntry {
	if limit <= 0 {
		limit = maxTimelineEntries
	}
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	n := len(s.events)
	if limit > n {
		limit = n
	}
	out := make([]TimelineEntry, 0, limit)
	for i := n - 1; i >= n-limit; i-- {
		out = append(out, s.events[i])
	}
	return out
}
