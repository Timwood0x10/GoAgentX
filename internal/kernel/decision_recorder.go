// Package kernel — scheduling-decision recorder.
//
// The Scheduling Observatory (dashboard.md §7) exists to explain WHY a task
// was assigned to a particular agent: the candidate pool, each candidate's
// capability-overlap / load / confidence / priority scores, and the final
// winner. This file records one decision per Schedule call into a bounded
// ring buffer so the panel can render the decision trail without the
// scheduler exposing internal mutation paths.
package kernel

import (
	"sort"
	"sync"
	"time"

	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// maxRecordedDecisions bounds the scheduling-decision ring. Decisions are
// appended on every Schedule; the cap keeps memory bounded while retaining a
// long-enough window for the panel's recent-decision view.
const maxRecordedDecisions = 200

// CandidateScore is one candidate's scheduling score breakdown — the
// "Capability Match" + "Scheduling Score" rows of dashboard.md §7.
type CandidateScore struct {
	// AgentID is the candidate executor.
	AgentID string `json:"agentId"`
	// Capabilities is the candidate's declared capability set.
	Capabilities []string `json:"capabilities"`
	// Overlap is the capability-overlap fraction [0,1].
	Overlap float64 `json:"overlap"`
	// Load is the candidate's current utilization [0,1].
	Load float64 `json:"load"`
	// Confidence is the experience-derived prior [0,1].
	Confidence float64 `json:"confidence"`
	// PriorityBoost is (1 + priority); 1 when priority is 0.
	PriorityBoost float64 `json:"priorityBoost"`
	// Score is the final candidate score (0 when capability does not match).
	Score float64 `json:"score"`
}

// ScheduleDecision is one immutable scheduling decision (dashboard.md §7:
// "Decision: TASK-184 ↓ agent-7f21 ↓ Acquire ↓ epoch = 42").
type ScheduleDecision struct {
	// TaskID is the scheduled task.
	TaskID string `json:"taskId"`
	// Capability is the task's required capability.
	Capability string `json:"capability"`
	// Candidates is every candidate considered, with its score breakdown,
	// ordered by score descending.
	Candidates []CandidateScore `json:"candidates"`
	// Winner is the chosen agent id.
	Winner string `json:"winner"`
	// Epoch is the fencing token assigned by Acquire.
	Epoch uint64 `json:"epoch"`
	// Time is when the decision was made.
	Time time.Time `json:"time"`
	// Err is set when Schedule failed (e.g. ErrNoCapableCandidate); Winner
	// and Epoch are then zero. Omitted on success.
	Err string `json:"err,omitempty"`
}

// DecisionRecorder is a bounded, lock-guarded ring of ScheduleDecision. A
// single instance is owned by the Scheduler; the panel reads it via Snapshot.
type DecisionRecorder struct {
	mu        sync.Mutex
	decisions []ScheduleDecision
}

// newDecisionRecorder builds an empty decision recorder.
func newDecisionRecorder() *DecisionRecorder { return &DecisionRecorder{} }

// Record appends one decision, evicting the oldest when the ring is full.
func (r *DecisionRecorder) Record(d ScheduleDecision) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.decisions = append(r.decisions, d)
	if len(r.decisions) > maxRecordedDecisions {
		r.decisions = r.decisions[len(r.decisions)-maxRecordedDecisions:]
	}
}

// Snapshot returns a copy of the recorded decisions, newest first.
func (r *DecisionRecorder) Snapshot() []ScheduleDecision {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ScheduleDecision, len(r.decisions))
	copy(out, r.decisions)
	// Newest first (reverse in place).
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// scoreCandidates computes the per-candidate score breakdown for a task's
// capability using the same Score formula the fabric's Pick applies. The
// breakdown is what the Scheduling Observatory renders — the scheduler does
// NOT mutate candidates, it only reads them.
func scoreCandidates(taskCapability string, cands []taskfabric.Candidate) []CandidateScore {
	out := make([]CandidateScore, 0, len(cands))
	for _, c := range cands {
		// One evaluation yields the score AND its decomposition, so the
		// breakdown the panel renders is exactly what the scheduler ranked
		// on — no factor is recomputed and the two cannot drift.
		parts := taskfabric.ScoreBreakdown(taskCapability, c)
		cs := CandidateScore{
			AgentID:       c.AgentID,
			Capabilities:  append([]string(nil), c.Capabilities...),
			Overlap:       parts.Overlap,
			Load:          parts.Load,
			Confidence:    parts.Confidence,
			PriorityBoost: parts.PriorityBoost,
			Score:         parts.Score,
		}
		out = append(out, cs)
	}
	// Highest score first; stable for equal scores (capability-overlap first).
	sort.SliceStable(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}
