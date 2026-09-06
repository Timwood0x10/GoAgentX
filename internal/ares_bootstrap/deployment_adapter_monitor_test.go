package ares_bootstrap

// deployment_adapter_monitor_test.go locks E2: deploymentAdapter.Deploy chains
// MonitorAndRollback after a promotion — a post-promotion regression must flip
// the record to DeploymentRolledBack and restore the live executor instance.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/evolution/deployment"
	"github.com/Timwood0x10/ares/internal/evolution/patch"
)

// sequencedStaging returns one score pair per Evaluate call: first the
// promotion pair, then the regression pair.
type sequencedStaging struct {
	pairs [][2]float64
	calls int
}

func (s *sequencedStaging) Apply(_ context.Context, p patch.RuntimePatch) (*patch.RuntimePatch, error) {
	return &p, nil
}

func (s *sequencedStaging) Evaluate(_ context.Context) (float64, float64, error) {
	i := s.calls
	if i >= len(s.pairs) {
		i = len(s.pairs) - 1
	}
	s.calls++
	return s.pairs[i][0], s.pairs[i][1], nil
}

func (s *sequencedStaging) Rollback(_ context.Context, _ *patch.RuntimePatch) error {
	return nil
}

// pointerLive swaps the live executor on Apply and restores the exact old
// pointer on Rollback.
type pointerLive struct {
	current       *int
	old           *int
	applyCalls    int
	rollbackCalls int
}

func (l *pointerLive) Apply(_ context.Context, p patch.RuntimePatch) (*patch.RuntimePatch, error) {
	l.applyCalls++
	l.old = l.current
	fresh := new(int)
	*fresh = 2
	l.current = fresh
	return &patch.RuntimePatch{
		Type:   p.Type,
		Target: p.Target,
		Source: p.Source,
		Value:  l.old,
	}, nil
}

func (l *pointerLive) Rollback(_ context.Context, rb *patch.RuntimePatch) error {
	l.rollbackCalls++
	if rb != nil {
		if old, ok := rb.Value.(*int); ok {
			l.current = old
			return nil
		}
	}
	l.current = l.old
	return nil
}

func TestDeploymentAdapter_PromoteThenRollbackOnRegression(t *testing.T) {
	oldExecutor := new(int)
	*oldExecutor = 1
	live := &pointerLive{current: oldExecutor}
	staging := &sequencedStaging{pairs: [][2]float64{
		{0.90, 0.50},
		{0.30, 0.90},
	}}
	dp := deployment.NewDeploymentPipeline(deployment.DeploymentConfig{
		Enabled:            true,
		PromotionThreshold: 0.15,
		RollbackThreshold:  0.10,
		EvaluationTimeout:  0,
	}, staging, live)
	adapter := &deploymentAdapter{dp: dp}

	p := patch.RuntimePatch{Type: patch.PatchChangePlanner, Target: "memory", StrategyID: "cand-1"}
	err := adapter.Deploy(context.Background(), p)
	require.Error(t, err, "post-promotion regression must surface as an error")
	assert.Contains(t, err.Error(), "rolled back")
	assert.Equal(t, 1, live.rollbackCalls, "live rollback must run")
	assert.True(t, live.current == oldExecutor, "executor must be restored to the old instance (pointer equal)")
}
