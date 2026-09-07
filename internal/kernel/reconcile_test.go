package kernel

import (
	"context"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// reconcileProbe is a minimal executor for reconciliation tests.
type reconcileProbe struct {
	id  string
	typ models.AgentType
}

func (e *reconcileProbe) ID() string             { return e.id }
func (e *reconcileProbe) Type() models.AgentType { return e.typ }
func (e *reconcileProbe) ExecuteStep(_ context.Context, _ *models.Task) (*sub.StepOutcome, error) {
	return &sub.StepOutcome{Done: true}, nil
}

// TestReconcileFabricDeaths_UnregistersKilledAgent pins the zombie-cleanup
// contract: when an agent is killed (removed from the Agent Fabric), its
// static scheduler registration is dropped by reconciliation. Without this,
// the stale-winner lookup executed tasks on a dead agent's registration and
// the executor map grew unboundedly with every spawn.
func TestReconcileFabricDeaths_UnregistersKilledAgent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	agents := agentfabric.NewFabric()
	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())
	sched.WithAgentFabric(agents)
	sched.PollInterval = 10 * time.Millisecond

	sched.RegisterExecutor("victim-1", &reconcileProbe{id: "victim-1", typ: "code"})
	requireExecutorCount(t, sched, 1)

	// Spawn a matching fabric entry, then kill it — the shape chaos
	// /api/chaos/random-kill exercises in production.
	if _, err := agents.Spawn(ctx, agentfabric.SpawnSpec{Identity: "victim-1", Capabilities: []string{"code"}}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if err := agents.Kill(ctx, "victim-1"); err != nil {
		t.Fatalf("kill: %v", err)
	}

	sched.reconcileFabricDeaths()
	requireExecutorCount(t, sched, 0,
		"killed agent's static registration must be unregistered")
}

// TestReconcileFabricDeaths_KeepsLiveAndBound pins the two exemptions:
// live fabric agents keep their registrations, and recovery-bound executors
// (deliberately outside the fabric) are never swept by reconciliation.
func TestReconcileFabricDeaths_KeepsLiveAndBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fabric := taskfabric.NewFabric()
	agents := agentfabric.NewFabric()
	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())
	sched.WithAgentFabric(agents)

	sched.RegisterExecutor("live-1", &reconcileProbe{id: "live-1", typ: "code"})
	if _, err := agents.Spawn(ctx, agentfabric.SpawnSpec{Identity: "live-1", Capabilities: []string{"code"}}); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	boundExec := &reconcileProbe{id: "recovery-x", typ: "code"}
	sched.RegisterExecutor("recovery-x", boundExec)
	if err := fabric.Create(&taskfabric.Task{
		ID:          "t-recover",
		Capability:  "code",
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 2},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	sched.RegisterExecutorForTask("t-recover", "recovery-x", boundExec)

	sched.reconcileFabricDeaths()

	if _, ok := allIDs(sched)["live-1"]; !ok {
		t.Fatal("live fabric agent's registration must survive reconciliation")
	}
	if _, ok := allIDs(sched)["recovery-x"]; !ok {
		t.Fatal("recovery-bound replacement must survive reconciliation")
	}
}

// TestReconcileFabricDeaths_NilFabricNoOp pins the no-op guard for legacy
// deployments without an Agent Fabric.
func TestReconcileFabricDeaths_NilFabricNoOp(t *testing.T) {
	fabric := taskfabric.NewFabric()
	sched := New(fabric, map[string]CapabilityExecutor{}, NewLoadTracker())
	sched.RegisterExecutor("solo", &reconcileProbe{id: "solo", typ: "code"})

	sched.reconcileFabricDeaths() // must not panic

	requireExecutorCount(t, sched, 1)
}

// allIDs snapshots the executor registry under its lock.
func allIDs(s *Scheduler) map[string]CapabilityExecutor {
	return s.allExecutors()
}

// requireExecutorCount asserts the executor registry size with a helpful
// failure message.
func requireExecutorCount(t *testing.T, s *Scheduler, want int, msgAndArgs ...any) {
	t.Helper()
	if got := len(s.allExecutors()); got != want {
		t.Fatalf("executor count = %d, want %d %v", got, want, msgAndArgs)
	}
}
