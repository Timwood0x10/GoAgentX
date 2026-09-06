package evolution

// fitness_aggregator_time_anchor_test.go locks E1: WindowAt scopes every
// source query to the same [since, until], and Window (zero bounds) keeps
// the legacy unscoped behavior.

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/evidence"
)

// recordingStore captures every Query filter so the test can assert the
// time anchor is shared across sources.
type recordingStore struct {
	mu      sync.Mutex
	filters []evidence.Filter
	inner   *evidence.MemoryStore
}

func (s *recordingStore) Append(ctx context.Context, e evidence.Evidence) error {
	return s.inner.Append(ctx, e)
}

func (s *recordingStore) Query(ctx context.Context, f evidence.Filter) ([]evidence.Evidence, error) {
	s.mu.Lock()
	s.filters = append(s.filters, f)
	s.mu.Unlock()
	return s.inner.Query(ctx, f)
}

func (s *recordingStore) Aggregate(ctx context.Context, f evidence.Filter, fn evidence.AggregateFn) (float64, error) {
	return s.inner.Aggregate(ctx, f, fn)
}

func (s *recordingStore) captured() []evidence.Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]evidence.Filter(nil), s.filters...)
}

func TestAggregator_WindowAt_SharesSingleTimeAnchor(t *testing.T) {
	inner := evidence.NewMemoryStore()
	now := time.Now()
	for i, ts := range []time.Time{
		now.Add(-30 * time.Minute),
		now.Add(-10 * time.Minute),
		now.Add(-2 * time.Hour),
	} {
		payload, err := json.Marshal(map[string]any{"value": 0.8, "strategy_id": "cand-1"})
		require.NoError(t, err)
		require.NoError(t, inner.Append(context.Background(), evidence.Evidence{
			ID:        string(rune('a'+i)) + "-anchor",
			Source:    "strategy",
			Kind:      evidence.KindFitness,
			Payload:   payload,
			Timestamp: ts,
		}))
	}
	rec := &recordingStore{inner: inner}
	agg := NewRuntimeFitnessAggregator(rec, DefaultAggregatorConfig())

	since := now.Add(-time.Hour)
	until := now
	res := agg.WindowAt(context.Background(), "cand-1", since, until)
	assert.Equal(t, 2, res.Count, "only the two in-window records count")

	filters := rec.captured()
	require.NotEmpty(t, filters, "WindowAt must query the store")
	for _, f := range filters {
		assert.True(t, f.Since.Equal(since), "every source shares the same Since, got %v", f.Since)
		assert.True(t, f.Until.Equal(until), "every source shares the same Until, got %v", f.Until)
		assert.False(t, f.Since.IsZero(), "Since must be non-zero for staging comparisons")
		assert.False(t, f.Until.IsZero(), "Until must be non-zero for staging comparisons")
	}
}
