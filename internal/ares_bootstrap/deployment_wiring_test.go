// Package ares_bootstrap — deployment staging tests (Stage 7).
//
// Verifies the shadow runtime no longer reports a constant passing score:
// Evaluate must return the real recent fitness mean (or coldStartScore when
// no evidence exists) so promotion only proceeds on observed performance.
//
// Step 2 fix: Evaluate now returns (shadow, baseline) per-strategy scores
// instead of one global mean. Tests cover per-strategy scoping and the
// cold-start fallback.
package ares_bootstrap

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	evolution "github.com/Timwood0x10/ares/internal/ares_evolution"
	"github.com/Timwood0x10/ares/internal/ares_evolution/mutation"
	aresmemory "github.com/Timwood0x10/ares/internal/ares_memory"
	"github.com/Timwood0x10/ares/internal/evidence"
	"github.com/Timwood0x10/ares/internal/evolution/patch"
)

// newStagingRuntime builds a deploymentStagingRuntime over the given evidence
// store with the zero-value cold-start score (0.0 — the pre-B6 behavior the
// original tests pinned). Production construction (bootstrap.go) wires the
// shared aggregator plus an explicit 0.5 cold-start score.
func newStagingRuntime(store evidence.Store, reg *patch.Registry) *deploymentStagingRuntime {
	return &deploymentStagingRuntime{
		reg: reg,
		agg: evolution.NewRuntimeFitnessAggregator(store, evolution.DefaultAggregatorConfig()),
	}
}

// newStagingRuntimeWithASM builds a deploymentStagingRuntime whose baseline is
// resolved live from an ActiveStrategyManager with the given strategy deployed.
// It mirrors production (bootstrap.go): baseline scoring must come from the
// ASM's Current(), not a frozen field.
func newStagingRuntimeWithASM(t *testing.T, store evidence.Store, reg *patch.Registry, strategyID string) *deploymentStagingRuntime {
	t.Helper()
	asmStore := evolution.NewMemoryStrategyStore(0)
	asm, err := evolution.NewActiveStrategyManager(asmStore, nil)
	require.NoError(t, err)
	require.NoError(t, asm.Deploy(context.Background(), &mutation.Strategy{ID: strategyID, Version: 1}))
	r := newStagingRuntime(store, reg)
	r.asm = asm
	return r
}

// TestDeploymentStaging_NoEvidence_ReturnsColdStart verifies that with no
// fitness evidence the shadow score falls back to coldStartScore (0.0 in
// the zero-value construction) — promotion is blocked instead of the old
// constant 1.0 pass.
func TestDeploymentStaging_NoEvidence_ReturnsColdStart(t *testing.T) {
	reg := patch.NewRegistry()
	r := newStagingRuntime(evidence.NewMemoryStore(), reg)

	shadow, baseline, err := r.Evaluate(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0.0, shadow,
		"shadow score must be cold-start (0.0) without evidence")
	assert.Equal(t, 0.0, baseline,
		"baseline score must be cold-start (0.0) without evidence")
}

// TestDeploymentStaging_NilEvidence_ReturnsColdStart covers the nil-store
// guard: the aggregator is wired but has no store, Window reports count 0,
// and the runtime falls back to the cold-start score.
func TestDeploymentStaging_NilEvidence_ReturnsColdStart(t *testing.T) {
	r := newStagingRuntime(nil, patch.NewRegistry())

	shadow, baseline, err := r.Evaluate(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0.0, shadow)
	assert.Equal(t, 0.0, baseline)
}

// TestDeploymentStaging_PerStrategyScores verifies that Evaluate returns
// different shadow scores for different strategy IDs. Seeds evidence for
// two strategies (A=0.9, B=0.3), applies each, and asserts the shadow
// score matches the strategy's own evidence — NOT a global mean.
func TestDeploymentStaging_PerStrategyScores(t *testing.T) {
	store := evidence.NewMemoryStore()
	// Register a real memory executor so the staging preflight (CanApply)
	// passes for Target "memory".
	memStore := buildMemoryManager()
	reg := patch.NewRegistry()
	require.NoError(t, reg.RegisterComponent(aresmemory.NewMemoryPatchExecutor(memStore)))
	r := newStagingRuntimeWithASM(t, store, reg, "active")

	// Seed strategy evidence: A=0.9 (10 samples), B=0.3 (10 samples).
	seedStrategyEvidence(t, store, "strategy-a", 0.9, 10)
	seedStrategyEvidence(t, store, "strategy-b", 0.3, 10)

	// Deploy patch for strategy A.
	pA := patch.RuntimePatch{Type: patch.PatchChangePlanner, Target: "memory", StrategyID: "strategy-a"}
	_, err := r.Apply(context.Background(), pA)
	require.NoError(t, err)
	shadowA, _, err := r.Evaluate(context.Background())
	require.NoError(t, err)
	assert.InDelta(t, 0.9, shadowA, 0.05,
		"shadow score for strategy A must reflect A's own evidence (0.9)")

	// Rollback and deploy patch for strategy B.
	require.NoError(t, r.Rollback(context.Background(), &pA))
	pB := patch.RuntimePatch{Type: patch.PatchChangePlanner, Target: "memory", StrategyID: "strategy-b"}
	_, err = r.Apply(context.Background(), pB)
	require.NoError(t, err)
	shadowB, _, err := r.Evaluate(context.Background())
	require.NoError(t, err)
	assert.InDelta(t, 0.3, shadowB, 0.05,
		"shadow score for strategy B must reflect B's own evidence (0.3)")

	// The two scores must differ — this is the core per-patch fix.
	delta := shadowA - shadowB
	if delta < 0 {
		delta = -delta
	}
	assert.True(t, delta > 0.1,
		"shadow scores for different strategies must differ by >0.1, got shadowA=%.3f shadowB=%.3f", shadowA, shadowB)
}

// TestDeploymentStaging_BaselineResolvedLiveFromASM pins the Step 6.2 fix:
// the baseline is resolved from the ActiveStrategyManager at Evaluate time, so
// promoting a different strategy mid-run is reflected in the comparison instead
// of measuring against a stale construction-time strategy ID. If the runtime
// cached the baseline, this test fails because switching the ASM would leave the
// baseline frozen.
func TestDeploymentStaging_BaselineResolvedLiveFromASM(t *testing.T) {
	ctx := context.Background()
	store := evidence.NewMemoryStore()
	memStore := buildMemoryManager()
	reg := patch.NewRegistry()
	require.NoError(t, reg.RegisterComponent(aresmemory.NewMemoryPatchExecutor(memStore)))

	r := newStagingRuntimeWithASM(t, store, reg, "active-a")
	r.coldStartScore = 0.5

	// Distinct baseline evidence so the switch is observable.
	seedStrategyEvidence(t, store, "active-a", 0.9, 10)
	seedStrategyEvidence(t, store, "active-b", 0.3, 10)

	_, err := r.Apply(ctx, patch.RuntimePatch{Type: patch.PatchChangePlanner, Target: "memory", StrategyID: "candidate"})
	require.NoError(t, err)

	// Baseline reflects the initially-active ASM strategy.
	_, baselineA, err := r.Evaluate(ctx)
	require.NoError(t, err)
	assert.InDelta(t, 0.9, baselineA, 0.05,
		"baseline must resolve actively-deployed strategy active-a")

	// Promote a DIFFERENT strategy without touching the staging runtime.
	asmStore := evolution.NewMemoryStrategyStore(0)
	asm, err := evolution.NewActiveStrategyManager(asmStore, nil)
	require.NoError(t, err)
	require.NoError(t, asm.Deploy(ctx, &mutation.Strategy{ID: "active-b", Version: 2}))
	r.asm = asm

	// Baseline must now reflect the switched active strategy, not a frozen ID.
	_, baselineB, err := r.Evaluate(ctx)
	require.NoError(t, err)
	assert.InDelta(t, 0.3, baselineB, 0.05,
		"baseline must reflect the ASM switch (active-b), proving live resolution")
}

// TestDeploymentStaging_ExplicitColdStartScore pins the B6 contract: when the
// construction site sets a cold-start score (bootstrap uses 0.5), a store
// with zero evidence returns that score instead of the universal 0.0 reject.
func TestDeploymentStaging_ExplicitColdStartScore(t *testing.T) {
	r := &deploymentStagingRuntime{
		reg:            patch.NewRegistry(),
		agg:            evolution.NewRuntimeFitnessAggregator(evidence.NewMemoryStore(), evolution.DefaultAggregatorConfig()),
		coldStartScore: 0.5,
	}
	shadow, baseline, err := r.Evaluate(context.Background())
	require.NoError(t, err)
	assert.InDelta(t, 0.5, shadow, 0.001,
		"cold-start shadow must receive the configured fallback score")
	assert.InDelta(t, 0.5, baseline, 0.001,
		"cold-start baseline must receive the configured fallback score")
}

// TestDeploymentStaging_UnattributedPatchIsNotMeasurable pins the attribution
// precondition: a patch with no StrategyID (or an unset active strategy) has
// no comparable pair of windows, so Evaluate must report both sides as
// cold-start — yielding delta 0, which any positive PromotionThreshold
// rejects. Regression guard: an earlier version backfilled the strategy key
// from Source/Target, producing a lookup that always missed while the score
// still looked like a measurement.
func TestDeploymentStaging_UnattributedPatchIsNotMeasurable(t *testing.T) {
	ctx := context.Background()
	store := evidence.NewMemoryStore()
	memStore := buildMemoryManager()
	reg := patch.NewRegistry()
	require.NoError(t, reg.RegisterComponent(aresmemory.NewMemoryPatchExecutor(memStore)))

	// Evidence exists and is strongly positive for the active strategy, so a
	// naive global-mean fallback would return a high, comparable-looking score.
	seedStrategyEvidence(t, store, "active", 0.9, 10)

	// Case 1: patch carries no StrategyID.
	r := newStagingRuntimeWithASM(t, store, reg, "active")
	r.coldStartScore = 0.5
	_, err := r.Apply(ctx, patch.RuntimePatch{Type: patch.PatchChangePlanner, Target: "memory", Source: "diff.memory"})
	require.NoError(t, err)
	assert.Equal(t, "", r.currentPatchStrategy,
		"Source must NOT be backfilled as a strategy ID")
	shadow, baseline, err := r.Evaluate(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0.5, shadow, "unattributed patch must report cold-start, not a global mean")
	assert.Equal(t, 0.5, baseline, "baseline must match shadow so delta is exactly 0")
	assert.Equal(t, 0.0, shadow-baseline, "unmeasurable pair must yield delta 0")

	// Case 2: patch is attributed but no active strategy is known.
	r2 := newStagingRuntime(store, reg)
	r2.coldStartScore = 0.5
	seedStrategyEvidence(t, store, "strategy-a", 0.9, 10)
	_, err = r2.Apply(ctx, patch.RuntimePatch{Type: patch.PatchChangePlanner, Target: "memory", StrategyID: "strategy-a"})
	require.NoError(t, err)
	shadow, baseline, err = r2.Evaluate(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0.0, shadow-baseline,
		"missing baseline attribution must not produce a positive delta")
}

// anchorRecordingStore captures Query filters for the E1 time-anchor test.
type anchorRecordingStore struct {
	inner   *evidence.MemoryStore
	mu      sync.Mutex
	filters []evidence.Filter
}

func (s *anchorRecordingStore) Append(ctx context.Context, e evidence.Evidence) error {
	return s.inner.Append(ctx, e)
}

func (s *anchorRecordingStore) Query(ctx context.Context, f evidence.Filter) ([]evidence.Evidence, error) {
	s.mu.Lock()
	s.filters = append(s.filters, f)
	s.mu.Unlock()
	return s.inner.Query(ctx, f)
}

func (s *anchorRecordingStore) Aggregate(ctx context.Context, f evidence.Filter, fn evidence.AggregateFn) (float64, error) {
	return s.inner.Aggregate(ctx, f, fn)
}

func (s *anchorRecordingStore) captured() []evidence.Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]evidence.Filter(nil), s.filters...)
}

// TestDeploymentStaging_EvaluateSharesSingleTimeAnchor locks E1: Evaluate
// samples shadow and baseline with the SAME non-zero [since, until] so
// concurrent evidence writes cannot skew the delta.
func TestDeploymentStaging_EvaluateSharesSingleTimeAnchor(t *testing.T) {
	ctx := context.Background()
	inner := evidence.NewMemoryStore()
	rec := &anchorRecordingStore{inner: inner}
	agg := evolution.NewRuntimeFitnessAggregator(rec, evolution.DefaultAggregatorConfig())
	memStore := buildMemoryManager()
	reg := patch.NewRegistry()
	require.NoError(t, reg.RegisterComponent(aresmemory.NewMemoryPatchExecutor(memStore)))
	r := &deploymentStagingRuntime{reg: reg, agg: agg, coldStartScore: 0.5}

	asmStore := evolution.NewMemoryStrategyStore(0)
	asm, err := evolution.NewActiveStrategyManager(asmStore, nil)
	require.NoError(t, err)
	require.NoError(t, asm.Deploy(ctx, &mutation.Strategy{ID: "active", Version: 1}))
	r.asm = asm

	seedStrategyEvidence(t, inner, "candidate", 0.9, 5)
	seedStrategyEvidence(t, inner, "active", 0.3, 5)
	_, err = r.Apply(ctx, patch.RuntimePatch{Type: patch.PatchChangePlanner, Target: "memory", StrategyID: "candidate"})
	require.NoError(t, err)
	_, _, err = r.Evaluate(ctx)
	require.NoError(t, err)

	filters := rec.captured()
	require.NotEmpty(t, filters, "Evaluate must query evidence with a time anchor")
	for _, f := range filters {
		assert.False(t, f.Since.IsZero(), "Since must be non-zero")
		assert.False(t, f.Until.IsZero(), "Until must be non-zero")
	}
	base := filters[0]
	for _, f := range filters[1:] {
		assert.True(t, f.Since.Equal(base.Since), "all queries share Since, got %v vs %v", f.Since, base.Since)
		assert.True(t, f.Until.Equal(base.Until), "all queries share Until, got %v vs %v", f.Until, base.Until)
	}
}

// seedStrategyEvidence writes n fitness evidence records for the given
// strategy ID with the given value into the "strategy" source.
func seedStrategyEvidence(t *testing.T, store *evidence.MemoryStore, strategyID string, value float64, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		payload, err := json.Marshal(map[string]any{
			"value":       value,
			"strategy_id": strategyID,
		})
		require.NoError(t, err)
		require.NoError(t, store.Append(ctx, evidence.Evidence{
			ID:        "test_strategy_" + strategyID + "_" + string(rune('a'+i)),
			Source:    "strategy",
			Kind:      evidence.KindFitness,
			Payload:   payload,
			Timestamp: time.Now().Add(-time.Duration(n-i) * time.Second),
		}))
	}
}

// TestDeploymentStaging_DoesNotMutateLiveRegistry pins the staging-isolation
// contract: a shadow Apply must not change the state the live registry's
// executors point at. Regression: staging previously called reg.Apply on the
// SAME registry the live runtime uses, so REJECTED patches had already
// mutated live memory config — and ID-bearing patches poisoned the shared
// idempotency map so the later promotion silently no-op'd.
func TestDeploymentStaging_DoesNotMutateLiveRegistry(t *testing.T) {
	ctx := context.Background()

	// The shared registry holds a REAL executor writing to a real config store.
	memStore := buildMemoryManager()
	reg := patch.NewRegistry()
	require.NoError(t, reg.RegisterComponent(aresmemory.NewMemoryPatchExecutor(memStore)))
	require.True(t, reg.CanApply("memory"), "memory patch component must be registered")

	r := newStagingRuntime(evidence.NewMemoryStore(), reg)

	p := patch.RuntimePatch{
		Type:       patch.PatchChangePlanner,
		Target:     "memory",
		StrategyID: "strategy-x",
		Value:      map[string]any{"max_history": 99},
		Reason:     "test: must never reach live state from staging",
	}
	_, err := r.Apply(ctx, p)
	require.NoError(t, err)

	cfg := memStore.GetConfig()
	require.NotNil(t, cfg)
	assert.NotEqual(t, 99, cfg.MaxHistory,
		"staging apply must NOT mutate live memory config")
	assert.Equal(t, 1, r.applyCount, "staging bookkeeping records the shadow apply")
	assert.Equal(t, "strategy-x", r.currentPatchStrategy,
		"staging must record the patch's StrategyID for per-patch Evaluate")

	// Rollback is a no-op (nothing was applied) and must not error;
	// it must also clear the per-patch strategy.
	require.NoError(t, r.Rollback(ctx, &p))
	assert.Equal(t, "", r.currentPatchStrategy,
		"rollback must clear the per-patch strategy")

	// A target with no registered executor is rejected by the preflight,
	// preserving the old "staging apply failed" rejection class.
	orphan := newStagingRuntime(evidence.NewMemoryStore(), patch.NewRegistry())
	_, err = orphan.Apply(ctx, patch.RuntimePatch{Type: patch.PatchChangePlanner, Target: "nope"})
	require.Error(t, err)
}
