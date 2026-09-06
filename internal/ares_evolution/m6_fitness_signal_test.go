package evolution

// m6_fitness_signal_test.go locks M6: per-(strategy, toolClass) tool_call
// evidence separates two strategies' success rates, and the signal reaches
// the aggregate once tool_weight > 0 — the L2 → L1 fitness path the GA
// promotion decision reads.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/evidence"
)

func seedToolCall(t *testing.T, store *evidence.MemoryStore, strategyID, toolStepID string, value float64, at time.Time) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"value": value, "strategy_id": strategyID, "tool_step_id": toolStepID,
	})
	require.NoError(t, err)
	require.NoError(t, store.Append(context.Background(), evidence.Evidence{
		ID:        strategyID + "_" + toolStepID + "_" + at.Format("150405.000000000"),
		Source:    toolCallEvidenceSource,
		Kind:      evidence.KindFitness,
		Payload:   payload,
		Timestamp: at,
	}))
}

func TestM6_ToolClassSuccessRateSeparatesStrategies(t *testing.T) {
	ctx := context.Background()
	store := evidence.NewMemoryStore()
	now := time.Now()
	const toolClass = "grep#limit,query"

	// Strategy A: 9/10 success. Strategy B: 2/10 success.
	for i := 0; i < 10; i++ {
		v := 0.0
		if i < 9 {
			v = 1.0
		}
		seedToolCall(t, store, "cand-a", toolClass, v, now.Add(-time.Duration(20-i)*time.Second))
	}
	for i := 0; i < 10; i++ {
		v := 0.0
		if i < 2 {
			v = 1.0
		}
		seedToolCall(t, store, "cand-b", toolClass, v, now.Add(-time.Duration(10-i)*time.Second))
	}

	cfg := DefaultAggregatorConfig()
	cfg.Weights.ToolCall = 0.15
	cfg.MinSamplesBeforeJudge = 5
	agg := NewRuntimeFitnessAggregator(store, cfg)

	resA := agg.WindowToolStep(ctx, "cand-a", toolClass)
	resB := agg.WindowToolStep(ctx, "cand-b", toolClass)
	require.True(t, resA.Ok && resB.Ok, "both sides need enough samples")
	assert.InDelta(t, 0.9, resA.Mean, 0.001)
	assert.InDelta(t, 0.2, resB.Mean, 0.001)

	// The signal reaches the aggregate: with tool_weight > 0 the high-success
	// strategy scores higher on the same evidence mix.
	winA := agg.Window(ctx, "cand-a")
	winB := agg.Window(ctx, "cand-b")
	assert.Greater(t, winA.Mean, winB.Mean,
		"tool success-rate signal must reach the aggregate (A=%.3f B=%.3f)", winA.Mean, winB.Mean)
}
