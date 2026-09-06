package evolution

// gate_eval_warn_test.go locks E3: every skip emits one structured warn naming
// the missing component and bumps SkippedCount, so a misconfigured G3 gate is
// operator-visible.

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/ares_eval"
	"github.com/Timwood0x10/ares/internal/ares_evolution/mutation"
)

func newWarnCapturingGate(t *testing.T, registry *ares_eval.EvaluatorRegistry, runner *ares_eval.AgentTestRunner, suite ares_eval.TestSuite) (*EvalGate, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	g := NewEvalGate(registry, runner, suite, DefaultEvalGateConfig(),
		WithEvalGateLogger(logger))
	return g, &buf
}

func TestEvalGate_SkipEmitsWarnPerMissingComponent(t *testing.T) {
	ctx := context.Background()

	t.Run("missing_registry_warns_and_counts", func(t *testing.T) {
		g, buf := newWarnCapturingGate(t, nil, nil, ares_eval.TestSuite{})
		pass, _, _ := g.Check(ctx, &mutation.Strategy{}, nil)
		assert.True(t, pass, "non-strict keeps the pass-through contract")
		assert.Equal(t, 1, g.SkippedCount())
		assert.Contains(t, buf.String(), "registry")
	})

	t.Run("missing_runner_warns_and_counts", func(t *testing.T) {
		runner, err := ares_eval.NewAgentTestRunner(&fakeExecutor{output: "ok"})
		require.NoError(t, err)
		registry := ares_eval.NewEvaluatorRegistry()
		g, buf := newWarnCapturingGate(t, registry, nil, ares_eval.TestSuite{TestCases: []ares_eval.TestCase{{ID: "c1", Input: "hi"}}})
		_, _ = runner, runner
		pass, _, _ := g.Check(ctx, &mutation.Strategy{}, nil)
		assert.True(t, pass)
		assert.Equal(t, 1, g.SkippedCount())
		assert.Contains(t, buf.String(), "runner")
	})

	t.Run("missing_suite_warns_and_counts", func(t *testing.T) {
		runner, err := ares_eval.NewAgentTestRunner(&fakeExecutor{output: "ok"})
		require.NoError(t, err)
		registry := ares_eval.NewEvaluatorRegistry()
		g, buf := newWarnCapturingGate(t, registry, runner, ares_eval.TestSuite{})
		pass, _, _ := g.Check(ctx, &mutation.Strategy{}, nil)
		assert.True(t, pass)
		assert.Equal(t, 1, g.SkippedCount())
		assert.Contains(t, buf.String(), "test suite")
	})
}
