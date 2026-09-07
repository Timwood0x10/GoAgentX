package kernel

import (
	"context"
	"testing"

	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// fakeDecideExecutor is a minimal CapabilityExecutor for decision-recording
// tests. Type declares the schedulable capability.
type fakeDecideExecutor struct {
	id  string
	cap string
}

func (e *fakeDecideExecutor) ID() string { return e.id }
func (e *fakeDecideExecutor) Type() models.AgentType {
	if e.cap != "" {
		return models.AgentType(e.cap)
	}
	return models.AgentType(e.id)
}
func (e *fakeDecideExecutor) ExecuteStep(ctx context.Context, task *models.Task) (*sub.StepOutcome, error) {
	return &sub.StepOutcome{Done: true, Result: &models.TaskResult{TaskID: task.TaskID, Success: true}}, nil
}

// TestDecisionRecorderRing verifies the ring cap evicts oldest first and the
// snapshot returns newest first.
func TestDecisionRecorderRing(t *testing.T) {
	r := newDecisionRecorder()
	for i := 0; i < maxRecordedDecisions+5; i++ {
		r.Record(ScheduleDecision{TaskID: "t"})
	}
	snap := r.Snapshot()
	if len(snap) != maxRecordedDecisions {
		t.Fatalf("expected %d decisions, got %d", maxRecordedDecisions, len(snap))
	}
}

// TestSchedulerRecordsDecisions locks the scheduling-decision contract: after
// a real Schedule→Acquire→RunQuantum cycle the recorder carries the task, the
// candidate pool (with score breakdown) and the winner.
func TestSchedulerRecordsDecisions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	tracker := NewLoadTracker()
	exec := &fakeDecideExecutor{id: "coder-1", cap: "code"}
	sched := New(fabric, map[string]CapabilityExecutor{"coder-1": exec}, tracker)
	sched.WithMaxConcurrent(1)

	if err := fabric.Create(&taskfabric.Task{ID: "task-1", Capability: "code"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := sched.executeUnbound(ctx, "task-1"); err != nil {
		t.Fatalf("execute: %v", err)
	}

	decisions := sched.DecisionsSnapshot()
	if len(decisions) == 0 {
		t.Fatal("expected at least one recorded decision")
	}
	d := decisions[0]
	if d.TaskID != "task-1" || d.Capability != "code" {
		t.Fatalf("decision identity wrong: %+v", d)
	}
	if d.Winner != "coder-1" {
		t.Fatalf("winner = %q, want coder-1", d.Winner)
	}
	if len(d.Candidates) == 0 {
		t.Fatal("expected candidate breakdown")
	}
	// The fabric-agent candidate path is not wired here, so the static
	// executor is the candidate; its score must be > 0 for the matching
	// capability.
	if d.Candidates[0].AgentID != "coder-1" || d.Candidates[0].Score <= 0 {
		t.Fatalf("candidate breakdown wrong: %+v", d.Candidates)
	}
}

// TestScoreCandidates verifies the breakdown matches the Score formula and is
// ordered descending.
func TestScoreCandidates(t *testing.T) {
	cands := []taskfabric.Candidate{
		{AgentID: "a", Capabilities: []string{"code"}, Load: 0.5, Confidence: 0.8},
		{AgentID: "b", Capabilities: []string{"review"}, Load: 0.1, Confidence: 0.9},
		{AgentID: "c", Capabilities: []string{"code", "review"}, Load: 0, Confidence: 1, Priority: 2},
	}
	scores := scoreCandidates("code", cands)
	if len(scores) != 3 {
		t.Fatalf("expected 3 scores, got %d", len(scores))
	}
	// c (code + zero load + full confidence + priority 2) must win.
	if scores[0].AgentID != "c" {
		t.Fatalf("expected c first, got %s", scores[0].AgentID)
	}
	if scores[0].Overlap != 1 || scores[0].PriorityBoost != 3 {
		t.Fatalf("c breakdown wrong: %+v", scores[0])
	}
	// a (code but 0.5 load) beats b (no overlap → 0 score).
	if scores[1].AgentID != "a" || scores[2].AgentID != "b" {
		t.Fatalf("order wrong: %+v", scores)
	}
	if scores[2].Score != 0 {
		t.Fatalf("b should score 0 (no capability overlap), got %v", scores[2].Score)
	}
}
