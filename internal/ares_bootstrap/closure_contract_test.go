// Package ares_bootstrap — Runtime Closure Contract Tests (Stage 0).
//
// These tests are designed to FAIL initially, exposing gaps documented in
// RUNTIME_COMPONENT_CLOSURE_PLAN_2026-08-04.md. Each test asserts the
// desired (target) behavior, not the current behavior. When Stage 1+
// implementation closes the gaps, these tests should turn green.
//
// Contract tests are tagged with build constraint "closure" so they can be
// selectively run via: go test -tags closure ./internal/ares_bootstrap/...
//
//go:build closure

package ares_bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/ares_config"
	ares_memory "github.com/Timwood0x10/ares/internal/ares_memory"
	"github.com/Timwood0x10/ares/internal/ares_runtime"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Contract Test 1: Memory.Enabled=false → Memory must not be constructed.
//
// Gap: Bootstrap always calls ProvideMemory regardless of cfg.Memory.Enabled.
// Target: When cfg.Memory.Enabled is false, comp.Memory must be nil and no
// memory goroutines or event subscriptions should be active.
// ---------------------------------------------------------------------------

func TestClosure_MemoryDisabled_NotConstructed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &ares_config.Config{
		LLM: ares_config.LLMConfig{
			Provider: "mock",
			Model:    "mock-model",
			APIKey:   "test-key",
			BaseURL:  "http://localhost:9999",
		},
		Memory: ares_config.MemoryConfig{
			Enabled: boolPtr(false), // Explicitly disable memory (*bool: false = opt out).
		},
	}

	comp, err := Bootstrap(ctx, cfg, nil)
	require.NoError(t, err, "Bootstrap should succeed with memory disabled")
	require.NotNil(t, comp)

	// Target behavior: Memory must be nil when disabled.
	// Current behavior: Memory is always constructed (F01).
	assert.Nil(t, comp.Memory,
		"Memory must be nil when cfg.Memory.Enabled=false; "+
			"currently Bootstrap always constructs Memory (F01: config gate bypass)")
}

// ---------------------------------------------------------------------------
// Contract Test 2: Evolution.Enabled=false → GA ticker must not run.
//
// Gap: Bootstrap always calls wireGAEvolution which starts a background
// ticker goroutine, regardless of cfg.Evolution.Enabled.
// Target: When cfg.Evolution.Enabled is false, no GA ticker goroutine
// should be active and comp.NewEvolution should be nil or Disabled.
// ---------------------------------------------------------------------------

func TestClosure_EvolutionDisabled_NoGATicker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &ares_config.Config{
		LLM: ares_config.LLMConfig{
			Provider: "mock",
			Model:    "mock-model",
			APIKey:   "test-key",
			BaseURL:  "http://localhost:9999",
		},
		Evolution: ares_config.EvolutionConfig{
			Enabled: false, // Explicitly disable evolution.
		},
	}

	comp, err := Bootstrap(ctx, cfg, nil)
	require.NoError(t, err, "Bootstrap should succeed with evolution disabled")
	require.NotNil(t, comp)

	// Target behavior: NewEvolution should not exist when evolution is disabled.
	// Current behavior: NewEvolution is always constructed (F02).
	if comp.NewEvolution != nil {
		t.Errorf("NewEvolution must be nil when cfg.Evolution.Enabled=false; " +
			"currently Bootstrap always constructs NewEvolution (F02: config gate bypass)")
	}

	// Target behavior: WaitBackground should return immediately (no goroutines).
	// Current behavior: GA ticker and distillation subscriber are always started.
	waitDone := make(chan struct{})
	go func() {
		comp.WaitBackground()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		// Good — no background goroutines running.
	case <-time.After(2 * time.Second):
		t.Errorf("WaitBackground did not return within 2s; " +
			"GA ticker is likely still running when cfg.Evolution.Enabled=false " +
			"(F02: config gate bypass)")
	}

	// Ensure we cancel the context to stop any leaked goroutines.
	cancel()
	comp.WaitBackground()
}

// ---------------------------------------------------------------------------
// Contract Test 3: Knowledge.RetrievalEnabled=true + write deps missing →
// must not report Ready.
//
// Gap: Bootstrap silently degrades when AKG write dependencies (embedding,
// experience repo) are missing. The system reports success despite the AKG
// loop being incomplete.
// Target: When knowledge.retrieval_enabled=true but write deps are missing,
// the system must report Degraded or Failed, not Ready.
// ---------------------------------------------------------------------------

func TestClosure_KnowledgeRetrievalEnabled_MissingWriteDeps_NotReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &ares_config.Config{
		LLM: ares_config.LLMConfig{
			Provider: "mock",
			Model:    "mock-model",
			APIKey:   "test-key",
			BaseURL:  "http://localhost:9999",
		},
		Knowledge: ares_config.KnowledgeConfig{
			RetrievalEnabled: true, // Enable AKG retrieval.
			TopK:             5,
			MinScore:         0.4,
		},
		// Storage and Embedding are NOT configured — write deps missing.
		Storage:   ares_config.StorageConfig{Enabled: false},
		Embedding: ares_config.EmbeddingConfig{Enabled: false},
	}

	comp, err := Bootstrap(ctx, cfg, nil)
	require.NoError(t, err, "Bootstrap should not error on missing deps (current behavior)")
	require.NotNil(t, comp)

	// Target behavior: When AKG retrieval is enabled but write deps are
	// missing, the system must not report Ready. It should be Degraded.
	// Current behavior: Bootstrap silently skips AKG wiring and reports success (F03).
	//
	// Since Runtime status API does not exist yet (Stage 1), we assert on
	// observable side effects:
	// 1. KnowledgeRetriever should not be wired (no retriever).
	// 2. AKGBridge should be nil (no write path).
	// 3. KnowledgeStore should be nil (no backing store).

	// F03 (Stage 9, hard assertion): the knowledge component must report
	// Degraded — NOT Ready — when AKG retrieval is enabled but the write-side
	// dependency is missing. The registry now carries a readiness hook for
	// this exact case (wireSystemRuntime registers Degraded mode + readyFn),
	// so the closure contract is asserted instead of skipped.
	if comp.AKGBridge != nil {
		t.Errorf("AKGBridge should be nil when write deps are missing; " +
			"got non-nil AKGBridge (F03: silent degradation)")
	}

	status, ok := comp.ComponentStatus(sysCompKnowledge)
	require.True(t, ok, "knowledge component must be registered in System Runtime")
	assert.Equal(t, kernel.StateDegraded, status.State,
		"knowledge component must report Degraded when AKG write deps are missing (F03)")
	assert.NotEmpty(t, status.Reason, "Degraded state must carry a reason")
}

// ---------------------------------------------------------------------------
// Contract Test 4: Runtime Ready → all GA executors must be bound to live
// targets.
//
// Gap: Bootstrap creates a synthetic 3-step DAG and registers it with the
// PatchRegistry. The live agent DAG is only wired after mgr.Start() in
// serve.go via wireEvolutionLiveDAGs.
// Target: Before Runtime reports Ready, all five genome executors
// (workflow, scheduler, recovery, memory, knowledge) must be bound to
// live targets, not synthetic placeholders.
// ---------------------------------------------------------------------------

func TestClosure_Ready_AllExecutorsBoundToLiveTargets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &ares_config.Config{
		LLM: ares_config.LLMConfig{
			Provider: "mock",
			Model:    "mock-model",
			APIKey:   "test-key",
			BaseURL:  "http://localhost:9999",
		},
		Evolution: ares_config.EvolutionConfig{Enabled: true},
	}

	comp, err := Bootstrap(ctx, cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, comp)
	require.NotNil(t, comp.NewEvolution, "NewEvolution must be wired for executor check")
	require.NotNil(t, comp.NewEvolution.PatchReg, "PatchRegistry must exist")

	// Check that the recovery executor is bound to a synthetic DAG, not a
	// live agent DAG. The recovery executor is created in ProvideNewEvolution
	// with a synthetic MutableDAG. Only wireEvolutionLiveDAGs (called post-
	// Start in serve.go) replaces it with the real agent DAG.
	//
	// F04: At Bootstrap time, executors are bound to synthetic targets.
	// This is the core bypass the plan calls out.

	// Verify the synthetic DAG exists (the 3-step placeholder) AND is isolated
	// to the "evolution" key: the leader's live DAG key must NOT be occupied by
	// the synthetic graph, so the serve entry (buildLeaderLiveDAG, pre-Start)
	// is the sole source of the production DAG. This is the F04 hard assertion:
	// synthetic placeholders never masquerade as a live agent DAG.
	if comp.NewEvolution != nil && comp.Runtime != nil {
		dag, ok := comp.Runtime.GetAgentDAG(ares_runtime.AgentDAGEvolutionKey)
		require.True(t, ok, "synthetic DAG must be registered under "+ares_runtime.AgentDAGEvolutionKey)
		require.NotNil(t, dag, "synthetic DAG must not be nil")

		leaderKey := ares_runtime.AgentDAGLiveKey
		if _, leaderOK := comp.Runtime.GetAgentDAG(leaderKey); leaderOK {
			t.Errorf("F04: live DAG key %q must not be populated at Bootstrap "+
				"(synthetic isolation violated)", leaderKey)
		}
	}

	// The live DAG supply chain (buildLeaderLiveDAG → RegisterAgentDAG →
	// wireEvolutionLiveDAGs) is verified at the serve entry in
	// cmd/ares/serve_live_dag_test.go; this Bootstrap-level test asserts the
	// isolation half of F04 (synthetic graph confined to the "evolution" key).
}

// ---------------------------------------------------------------------------
// Helper: minimal config for closure tests.
// ---------------------------------------------------------------------------

// newClosureTestConfig returns a minimal Config suitable for closure contract
// tests. All optional subsystems are disabled by default. Individual tests
// enable specific subsystems to test their config gates.
func newClosureTestConfig() *ares_config.Config {
	return &ares_config.Config{
		LLM: ares_config.LLMConfig{
			Provider: "mock",
			Model:    "mock-model",
			APIKey:   "test-key",
			BaseURL:  "http://localhost:9999",
		},
		Memory: ares_config.MemoryConfig{
			Enabled: boolPtr(true),
		},
		Evolution: ares_config.EvolutionConfig{
			Enabled: false,
		},
		Storage:   ares_config.StorageConfig{Enabled: false},
		Embedding: ares_config.EmbeddingConfig{Enabled: false},
		Knowledge: ares_config.KnowledgeConfig{RetrievalEnabled: false},
	}
}

// TestClosure_MinimalConfig_NoPanic verifies that a minimal config with all
// optional subsystems disabled does not panic during Bootstrap.
func TestClosure_MinimalConfig_NoPanic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := newClosureTestConfig()
	comp, err := Bootstrap(ctx, cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, comp)

	// Verify basic invariants.
	assert.NotNil(t, comp.EventStore, "EventStore must always be constructed")
	assert.NotNil(t, comp.Runtime, "Runtime must always be constructed")
	assert.NotNil(t, comp.LLM, "LLM must always be constructed")

	// Clean up.
	cancel()
	comp.WaitBackground()
}

// TestClosure_MemoryEnabled_Constructed verifies that when memory is enabled,
// MemoryManager is constructed and functional.
func TestClosure_MemoryEnabled_Constructed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := newClosureTestConfig()
	cfg.Memory.Enabled = boolPtr(true)

	comp, err := Bootstrap(ctx, cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, comp)

	// When enabled, Memory must be constructed.
	assert.NotNil(t, comp.Memory, "Memory must be constructed when enabled")

	// Verify it satisfies the MemoryManager interface.
	if comp.Memory != nil {
		_, ok := comp.Memory.(ares_memory.MemoryManager)
		assert.True(t, ok, "comp.Memory must implement MemoryManager interface")
	}

	cancel()
	comp.WaitBackground()
}
