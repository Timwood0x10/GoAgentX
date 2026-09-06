// gate_eval.go wraps the ares_eval evaluation framework into a VerifyGate
// so the StrategyLifecycle can run independent regression tests before
// promoting a candidate strategy (B5 fix: Eval was built but never
// participated in promote/rollback decisions).
//
// G3 (Eval Suite) is the third gate in the verify pipeline:
//
//	G1 Guardrail → G2 Shadow → G3 Eval Suite → G4 Deployment staging
//
// When no EvaluatorRegistry is wired, the gate is a pass-through (returns
// pass=true) so the pipeline degrades gracefully in environments without
// LLM-based evaluators.
package evolution

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/Timwood0x10/ares/internal/ares_eval"
	"github.com/Timwood0x10/ares/internal/ares_evolution/mutation"
)

// EvalGateConfig configures the G3 eval-suite verify gate.
type EvalGateConfig struct {
	// MinScore is the minimum weighted average score for a candidate to
	// pass. Default: 0.7.
	MinScore float64
	// EvaluatorName selects which registered evaluator to use. When empty,
	// the gate runs all registered evaluators and averages their scores.
	EvaluatorName string
	// StrictMode, when true, causes the gate to REJECT (return false)
	// when no eval infrastructure is wired instead of silently passing.
	// Production deployments should set this to true so a missing
	// registry does not quietly allow every candidate through.
	StrictMode bool
}

// DefaultEvalGateConfig returns sensible defaults.
func DefaultEvalGateConfig() EvalGateConfig {
	return EvalGateConfig{
		MinScore:   0.7,
		StrictMode: false, // preserves backward compatibility; prod sets true
	}
}

// EvalGate is the G3 verify gate. It wraps an ares_eval.EvaluatorRegistry
// and an optional AgentTestRunner to score candidate strategies against a
// fixed regression suite. The gate is pass-through when no registry is set
// (unless StrictMode is enabled, in which case it fails closed).
type EvalGate struct {
	registry *ares_eval.EvaluatorRegistry
	runner   *ares_eval.AgentTestRunner
	suite    ares_eval.TestSuite
	cfg      EvalGateConfig
	// skippedCount tracks how many times the gate was skipped due to
	// missing infrastructure. Exposed via SkippedCount for observability.
	skippedCount int
	// logger receives a structured warn on every skip (E3) so a
	// misconfigured G3 gate is operator-visible instead of a silent pass.
	logger *slog.Logger
	// beforeRun, when set, is invoked with the candidate right before the
	// suite runs — the seam that lets the executor run test cases THROUGH
	// the candidate strategy (e.g. apply its prompt template) so the score
	// actually discriminates candidates instead of measuring a fixed agent.
	beforeRun func(*mutation.Strategy)
}

// EvalGateOption configures an EvalGate.
type EvalGateOption func(*EvalGate)

// WithEvalGateBeforeRun registers a hook invoked with the candidate strategy
// immediately before the suite runs. Use it to push candidate state (prompt
// template, sampling params) into the executor behind the AgentTestRunner.
func WithEvalGateBeforeRun(fn func(*mutation.Strategy)) EvalGateOption {
	return func(g *EvalGate) {
		g.beforeRun = fn
	}
}

// WithEvalGateLogger overrides the skip-warning sink (E3). Default is
// slog.Default(); tests inject a buffered handler to assert the warn.
func WithEvalGateLogger(l *slog.Logger) EvalGateOption {
	return func(g *EvalGate) {
		if l != nil {
			g.logger = l
		}
	}
}

// NewEvalGate creates a G3 eval-suite gate. Any nil argument makes the gate
// a pass-through (always passes), so the pipeline degrades gracefully.
func NewEvalGate(
	registry *ares_eval.EvaluatorRegistry,
	runner *ares_eval.AgentTestRunner,
	suite ares_eval.TestSuite,
	cfg EvalGateConfig,
	opts ...EvalGateOption,
) *EvalGate {
	g := &EvalGate{
		registry: registry,
		runner:   runner,
		suite:    suite,
		cfg:      cfg,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Name returns the gate identifier.
func (g *EvalGate) Name() string {
	return "eval"
}

// SkippedCount returns the number of times the gate was skipped due to
// missing eval infrastructure (registry, runner, or empty suite). This
// counter lets operators detect a misconfigured G3 gate that would
// otherwise silently pass every candidate.
func (g *EvalGate) SkippedCount() int {
	return g.skippedCount
}

// Check runs the candidate through the eval suite and returns pass=true when
// the weighted average score meets or exceeds MinScore. When no registry or
// runner is wired, the gate is a pass-through — UNLESS StrictMode is enabled,
// in which case it returns false (fail-closed) so a missing registry does not
// silently allow every candidate through.
func (g *EvalGate) Check(ctx context.Context, cand *mutation.Strategy, _ *mutation.Strategy) (bool, float64, string) {
	if g.registry == nil || g.runner == nil || len(g.suite.TestCases) == 0 {
		g.skippedCount++
		// Identify the specific missing component for the log.
		var missing []string
		if g.registry == nil {
			missing = append(missing, "registry")
		}
		if g.runner == nil {
			missing = append(missing, "runner")
		}
		if len(g.suite.TestCases) == 0 {
			missing = append(missing, "test suite")
		}
		reason := fmt.Sprintf("eval suite not configured (missing: %s), skipping", strings.Join(missing, ", "))
		g.logger.WarnContext(ctx, "eval gate skipped: eval infrastructure missing",
			"missing", strings.Join(missing, ","),
			"strict_mode", g.cfg.StrictMode,
			"skipped_count", g.skippedCount)
		if g.cfg.StrictMode {
			return false, 0, fmt.Sprintf("strict mode: %s — rejected", reason)
		}
		return true, 0, reason
	}

	// Let the executor run the suite through THIS candidate (prompt template
	// / params), so the score reflects the candidate rather than a fixed
	// agent configuration.
	if g.beforeRun != nil {
		g.beforeRun(cand)
	}

	// Resolve evaluator: use the named one when configured, otherwise run
	// all registered evaluators and average their scores.
	if g.cfg.EvaluatorName != "" {
		results, scores, err := g.runner.RunAndEvaluate(ctx, g.suite, g.cfg.EvaluatorName)
		if err != nil {
			return false, 0, fmt.Sprintf("eval suite failed: %s", err)
		}
		score := averageScores(scores, len(results))
		if score >= g.cfg.MinScore {
			return true, score, fmt.Sprintf("eval score %.2f >= %.2f", score, g.cfg.MinScore)
		}
		return false, score, fmt.Sprintf("eval score %.2f < %.2f", score, g.cfg.MinScore)
	}

	// Run all registered evaluators and average.
	totalScore := 0.0
	evalCount := 0
	for _, name := range g.registry.Names() {
		results, scores, err := g.runner.RunAndEvaluate(ctx, g.suite, name)
		if err != nil {
			continue
		}
		totalScore += averageScores(scores, len(results))
		evalCount++
	}
	if evalCount == 0 {
		g.skippedCount++
		reason := "no evaluators produced results, skipping"
		g.logger.WarnContext(ctx, "eval gate skipped: no evaluator results",
			"strict_mode", g.cfg.StrictMode,
			"skipped_count", g.skippedCount)
		if g.cfg.StrictMode {
			return false, 0, fmt.Sprintf("strict mode: %s — rejected", reason)
		}
		return true, 0, reason
	}
	avgScore := totalScore / float64(evalCount)
	if avgScore >= g.cfg.MinScore {
		return true, avgScore, fmt.Sprintf("eval score %.2f >= %.2f", avgScore, g.cfg.MinScore)
	}
	return false, avgScore, fmt.Sprintf("eval score %.2f < %.2f", avgScore, g.cfg.MinScore)
}

// averageScores computes the mean score across all test case results.
func averageScores(scores [][]ares_eval.EvalScore, resultCount int) float64 {
	if len(scores) == 0 || resultCount == 0 {
		return 0
	}
	var total float64
	var count int
	for _, caseScores := range scores {
		for _, s := range caseScores {
			total += s.Score
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return total / float64(count)
}
