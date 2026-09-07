package kernel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// TestLoadTrackerSnapshot locks the read-model contract: every tracked agent
// appears exactly once with copied values, overrides are flagged by presence
// (0.0 is a valid override), capability overrides are grouped per agent, and
// the result is sorted by AgentID.
func TestLoadTrackerSnapshot(t *testing.T) {
	lt := NewLoadTracker()
	lt.Begin("a")     // load 1
	lt.End("a", true) // done 1, load back to 0
	lt.Begin("a")     // load 1 again — snapshot must show the in-flight quantum
	lt.SetPriority("b", 3)
	lt.SetAgentConfidence("c", 0.0) // 0.0 is a VALID override (presence matters)
	lt.SetCapabilityConfidence("a", "code", 0.9)

	snap := lt.Snapshot()

	if len(snap.Agents) != 3 {
		t.Fatalf("expected 3 agents, got %d: %+v", len(snap.Agents), snap.Agents)
	}
	if snap.Agents[0].AgentID != "a" || snap.Agents[1].AgentID != "b" || snap.Agents[2].AgentID != "c" {
		t.Fatalf("agents not sorted: %+v", snap.Agents)
	}

	a := snap.Agents[0]
	if a.Done != 1 || a.Ok != 1 {
		t.Errorf("agent a done/ok = %v/%v, want 1/1", a.Done, a.Ok)
	}
	if a.Load != 1 {
		t.Errorf("agent a load = %v (Begin without End), want 1", a.Load)
	}
	if v := a.CapabilityOverrides["code"]; v != 0.9 {
		t.Errorf("agent a capability override = %v, want 0.9", v)
	}

	c := snap.Agents[2]
	if !c.HasConfidenceOverride || c.ConfidenceOverride != 0.0 {
		t.Errorf("agent c override presence/value wrong: %+v", c)
	}
}

// TestSchedulerSnapshotReadModel verifies the Domain A fields against a wired
// scheduler: executor counts, wiring flags, queue depth from the fabric, and
// tracker passthrough.
func TestSchedulerSnapshotReadModel(t *testing.T) {
	fabric := taskfabric.NewFabric()
	agents := agentfabric.NewFabric()
	tracker := NewLoadTracker()

	s := New(fabric, map[string]CapabilityExecutor{
		"exec-1": &noopSnapshotExecutor{id: "exec-1"},
	}, tracker)
	s.WithAgentFabric(agents).WithGovernance(agents).WithTTL(time.Minute)

	if err := fabric.Create(&taskfabric.Task{ID: "t-ready", Capability: "cap"}); err != nil {
		t.Fatal(err)
	}
	tracker.Begin("exec-1")

	snap := s.Snapshot()

	if snap.Executors != 1 || snap.BoundExecutors != 0 {
		t.Errorf("executors/bound = %d/%d, want 1/0", snap.Executors, snap.BoundExecutors)
	}
	if !snap.GovernanceWired || !snap.AgentFabricWired {
		t.Error("wiring flags must reflect WithGovernance/WithAgentFabric")
	}
	if snap.EventDriven {
		t.Error("EventDriven must be false when no event store is wired")
	}
	if snap.ReadyTasks != 1 {
		t.Errorf("ready depth = %d, want 1", snap.ReadyTasks)
	}
	if snap.TTL != time.Minute {
		t.Errorf("ttl = %v", snap.TTL)
	}
	if len(snap.Load.Agents) != 1 || snap.Load.Agents[0].AgentID != "exec-1" {
		t.Errorf("tracker snapshot passthrough broken: %+v", snap.Load)
	}
}

// TestSchedulerSnapshotConcurrentWithRun is the monitoring.md Phase 0 safety
// gate: Snapshot must be safe to call concurrently with the running drain
// loop and task churn (go test -race is the real judge).
func TestSchedulerSnapshotConcurrentWithRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	tracker := NewLoadTracker()
	s := New(fabric, map[string]CapabilityExecutor{
		"exec": &noopSnapshotExecutor{id: "exec"},
	}, tracker)
	s.PollInterval = 5 * time.Millisecond

	go s.Run(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := time.Now().UnixNano() == 0 // silence unused n lint in expr below
			_ = id
			taskID := string(rune('a'+n%26)) + "-" + time.Now().Format("150405.000000000")
			_ = fabric.Create(&taskfabric.Task{ID: taskID, Capability: "cap"})
		}(i)
	}
	for i := 0; i < 200; i++ {
		_ = s.Snapshot()
	}
	wg.Wait()
	time.Sleep(30 * time.Millisecond) // let one drain tick pass before cancel
}

// noopSnapshotExecutor satisfies CapabilityExecutor doing nothing.
type noopSnapshotExecutor struct{ id string }

func (e *noopSnapshotExecutor) ID() string             { return e.id }
func (e *noopSnapshotExecutor) Type() models.AgentType { return "cap" }
func (e *noopSnapshotExecutor) ExecuteStep(ctx context.Context, task *models.Task) (*sub.StepOutcome, error) {
	return &sub.StepOutcome{Done: true, Result: &models.TaskResult{TaskID: task.TaskID, Success: true}}, nil
}
