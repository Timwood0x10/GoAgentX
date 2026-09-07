package kernel

import (
	"sync"
	"testing"

	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// capCode is the test capability shared by the confidence/score fixtures.
const capCode = "code"

// TestLoadTracker_BeginEndRelease verifies the F1 core invariant: Begin
// increments load, End decrements it, so after a quantum the agent is
// schedulable again. Without the load-- in End, this test fails (the bug
// that caused "later rounds get no capable candidate").
func TestLoadTracker_BeginEndRelease(t *testing.T) {
	tr := NewLoadTracker()
	agentID := "agent-1"

	// Before Begin: load == 0.
	if l := tr.Load(agentID); l != 0 {
		t.Fatalf("before Begin: Load = %v, want 0", l)
	}

	tr.Begin(agentID)

	// After Begin: load == 1 (busy slot held).
	if l := tr.Load(agentID); l != 1 {
		t.Fatalf("after Begin: Load = %v, want 1", l)
	}

	tr.End(agentID, true)

	// After End: load == 0 (busy slot released — the F1 fix).
	if l := tr.Load(agentID); l != 0 {
		t.Fatalf("after End: Load = %v, want 0", l)
	}
}

// TestLoadTracker_MultiRoundNoMonotonicClimb verifies that multiple Begin/End
// rounds do NOT cause load to climb monotonically — the bug that starved
// scheduling after the first round. This is the regression test for F1:
// if someone removes the load-- in End, this test must fail.
func TestLoadTracker_MultiRoundNoMonotonicClimb(t *testing.T) {
	tr := NewLoadTracker()
	agentID := "agent-multi"

	for i := 0; i < 10; i++ {
		tr.Begin(agentID)
		tr.End(agentID, true)
	}

	// After 10 rounds, load must still be 0 (not 10).
	if l := tr.Load(agentID); l != 0 {
		t.Fatalf("after 10 Begin/End rounds: Load = %v, want 0 (load must not climb monotonically — F1 starvation bug)", l)
	}
}

// TestLoadTracker_EndNoUnderflow verifies End does not underflow load below
// zero. When load is already 0, End must be a no-op (the `if t.load[agentID]
// > 0` guard).
func TestLoadTracker_EndNoUnderflow(t *testing.T) {
	tr := NewLoadTracker()
	agentID := "agent-underflow"

	// End without a preceding Begin: load is 0, must not go negative.
	tr.End(agentID, true)
	if l := tr.Load(agentID); l != 0 {
		t.Fatalf("End without Begin: Load = %v, want 0 (no underflow)", l)
	}

	// Begin once, End twice: the second End must not underflow.
	tr.Begin(agentID)
	tr.End(agentID, true)
	tr.End(agentID, true)
	if l := tr.Load(agentID); l != 0 {
		t.Fatalf("double End: Load = %v, want 0 (no underflow)", l)
	}
}

// TestLoadTracker_SetAgentConfidenceClearAndZero verifies:
//   - SetAgentConfidence(id, -1) clears the override → Confidence falls back
//     to historical success rate.
//   - SetAgentConfidence(id, 0.0) is a VALID override (0% success rate keeps
//     the agent at the bottom of the ranking — F1 GA contract).
func TestLoadTracker_SetAgentConfidenceClearAndZero(t *testing.T) {
	tr := NewLoadTracker()
	agentID := "agent-conf"

	// No history, no override: neutral prior 1.0.
	if c := tr.Confidence(agentID); c != 1.0 {
		t.Fatalf("no history: Confidence = %v, want 1.0", c)
	}

	// Set 0.0 — a valid override (0% success rate).
	tr.SetAgentConfidence(agentID, 0.0)
	if c := tr.Confidence(agentID); c != 0.0 {
		t.Fatalf("after SetAgentConfidence(0.0): Confidence = %v, want 0.0", c)
	}

	// Set -1 — clears the override → falls back to neutral prior 1.0.
	tr.SetAgentConfidence(agentID, -1)
	if c := tr.Confidence(agentID); c != 1.0 {
		t.Fatalf("after SetAgentConfidence(-1): Confidence = %v, want 1.0 (cleared override, neutral prior)", c)
	}

	// Now with history: do 5 tasks, 3 ok → historical rate = 0.6.
	for i := 0; i < 5; i++ {
		tr.Begin(agentID)
		tr.End(agentID, i < 3)
	}
	// Historical: 3/5 = 0.6.
	if c := tr.Confidence(agentID); c != 0.6 {
		t.Fatalf("historical Confidence = %v, want 0.6", c)
	}

	// Override to 0.2 — overrides history.
	tr.SetAgentConfidence(agentID, 0.2)
	if c := tr.Confidence(agentID); c != 0.2 {
		t.Fatalf("after override 0.2: Confidence = %v, want 0.2", c)
	}

	// Clear with -1 → falls back to historical 0.6.
	tr.SetAgentConfidence(agentID, -1)
	if c := tr.Confidence(agentID); c != 0.6 {
		t.Fatalf("after clear with -1: Confidence = %v, want 0.6 (historical fallback)", c)
	}
}

// TestLoadTracker_SetCapabilityConfidenceClearAndFallback verifies:
//   - SetCapabilityConfidence with a negative value clears the capability
//     override.
//   - ConfidenceFor falls back: capability > agent > historical > 1.0.
func TestLoadTracker_SetCapabilityConfidenceClearAndFallback(t *testing.T) {
	tr := NewLoadTracker()
	agentID := "agent-cap"
	cap := capCode

	// No history, no override: neutral prior 1.0.
	if c := tr.ConfidenceFor(agentID, cap); c != 1.0 {
		t.Fatalf("no data: ConfidenceFor = %v, want 1.0", c)
	}

	// Set capability override to 0.3.
	tr.SetCapabilityConfidence(agentID, cap, 0.3)
	if c := tr.ConfidenceFor(agentID, cap); c != 0.3 {
		t.Fatalf("after SetCapabilityConfidence(0.3): ConfidenceFor = %v, want 0.3", c)
	}

	// Set agent-level override to 0.5 — capability still wins.
	tr.SetAgentConfidence(agentID, 0.5)
	if c := tr.ConfidenceFor(agentID, cap); c != 0.3 {
		t.Fatalf("capability > agent: ConfidenceFor = %v, want 0.3 (capability wins)", c)
	}

	// Clear capability override → falls back to agent-level 0.5.
	tr.SetCapabilityConfidence(agentID, cap, -1)
	if c := tr.ConfidenceFor(agentID, cap); c != 0.5 {
		t.Fatalf("after clear cap: ConfidenceFor = %v, want 0.5 (agent fallback)", c)
	}

	// Clear agent override → falls back to neutral 1.0.
	tr.SetAgentConfidence(agentID, -1)
	if c := tr.ConfidenceFor(agentID, cap); c != 1.0 {
		t.Fatalf("after clear agent: ConfidenceFor = %v, want 1.0 (neutral prior)", c)
	}

	// Add history: 4 done, 2 ok → 0.5 historical.
	for i := 0; i < 4; i++ {
		tr.Begin(agentID)
		tr.End(agentID, i < 2)
	}
	if c := tr.ConfidenceFor(agentID, cap); c != 0.5 {
		t.Fatalf("historical: ConfidenceFor = %v, want 0.5", c)
	}

	// Agent override 0.8 → wins over history.
	tr.SetAgentConfidence(agentID, 0.8)
	if c := tr.ConfidenceFor(agentID, cap); c != 0.8 {
		t.Fatalf("agent override: ConfidenceFor = %v, want 0.8 (agent > historical)", c)
	}

	// Capability override 0.2 → wins over agent.
	tr.SetCapabilityConfidence(agentID, cap, 0.2)
	if c := tr.ConfidenceFor(agentID, cap); c != 0.2 {
		t.Fatalf("capability override: ConfidenceFor = %v, want 0.2 (capability > agent > historical)", c)
	}

	// Clear both → back to historical 0.5.
	tr.SetCapabilityConfidence(agentID, cap, -1)
	tr.SetAgentConfidence(agentID, -1)
	if c := tr.ConfidenceFor(agentID, cap); c != 0.5 {
		t.Fatalf("after clearing all overrides: ConfidenceFor = %v, want 0.5 (historical fallback)", c)
	}
}

// TestLoadTracker_ConcurrentNoRace verifies that concurrent Begin/End/
// SetAgentConfidence/SetCapabilityConfidence operations do not trigger
// data races (run with -race).
func TestLoadTracker_ConcurrentNoRace(t *testing.T) {
	tr := NewLoadTracker()
	agentID := "agent-race"
	cap := capCode

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			tr.Begin(agentID)
		}()
		go func() {
			defer wg.Done()
			tr.End(agentID, true)
		}()
		go func(i int) {
			defer wg.Done()
			tr.SetAgentConfidence(agentID, float64(i%5)*0.1)
		}(i)
		go func(i int) {
			defer wg.Done()
			tr.SetCapabilityConfidence(agentID, cap, float64(i%3)*0.2)
		}(i)
	}
	wg.Wait()

	// Final state: load should be roughly 0 (50 begins, 50 ends).
	// We don't assert exact value because the ordering is nondeterministic,
	// but it must be non-negative (no underflow under concurrency).
	if l := tr.Load(agentID); l < 0 {
		t.Fatalf("concurrent: Load = %v, must be >= 0", l)
	}
	// ConfidenceFor must stay in [0, 1]: every value injected above is in
	// that range (overrides [0, 0.4], historical rate [0, 1]), so anything
	// outside means the tracker corrupted its state.
	if c := tr.ConfidenceFor(agentID, cap); c < 0 || c > 1.0 {
		t.Fatalf("concurrent: ConfidenceFor = %v, want [0, 1]", c)
	}
}

// TestLoadTracker_ScoreStaysPositiveAfterMultipleRounds is the H1.2
// integration assertion: after multiple Begin/End rounds, the agent's
// taskfabric.Score must remain > 0 because load has been released. This
// reproduces the F1 scenario end-to-end: "later rounds get no capable
// candidate" must NOT happen.
func TestLoadTracker_ScoreStaysPositiveAfterMultipleRounds(t *testing.T) {
	tr := NewLoadTracker()
	agentID := "agent-score"
	capability := capCode

	// Simulate 5 quantum rounds: Begin → End → check Score.
	for round := 0; round < 5; round++ {
		tr.Begin(agentID)
		// During quantum: load == 1, Score should be 0 (agent is busy).
		cand := taskfabric.Candidate{
			AgentID:      agentID,
			Capabilities: []string{capability},
			Load:         tr.Load(agentID),
			Confidence:   tr.Confidence(agentID),
		}
		scoreDuring := taskfabric.Score(capability, cand)
		if scoreDuring != 0 {
			t.Fatalf("round %d: during quantum Score = %v, want 0 (agent busy, load=1)", round, scoreDuring)
		}

		tr.End(agentID, true)
		// After quantum: load == 0, Score must be > 0 (agent idle again).
		cand.Load = tr.Load(agentID)
		scoreAfter := taskfabric.Score(capability, cand)
		if scoreAfter <= 0 {
			t.Fatalf("round %d: after quantum Score = %v, want > 0 "+
				"(load released — F1 regression: 'no capable candidate')", round, scoreAfter)
		}
	}
}
