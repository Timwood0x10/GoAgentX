// eval_gate_wiring.go builds the PRODUCTION G3 eval-suite gate: an
// AgentTestRunner whose executor runs regression test cases THROUGH the
// candidate strategy (its prompt template), scored by the registered LLM
// evaluators (llm_judge). This closes review item B.2 — the gate used to be
// constructed with a nil runner and an empty suite, making it a permanent
// pass-through while the docs claimed a four-gate pipeline.
//
// Configuration contract (evolution.gates.eval_suite, a file path):
//   - unset → NO G3 gate is built (honest absence, not a fake pass-through);
//     the pipeline degrades to G2 shadow (+ G1 guardrails at the scheduler).
//   - set but unloadable → Bootstrap FAILS. An operator who configured a
//     verification gate must not get a silently weaker pipeline from a typo.
package ares_bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Timwood0x10/ares/internal/ares_eval"
	evolution "github.com/Timwood0x10/ares/internal/ares_evolution"
	"github.com/Timwood0x10/ares/internal/ares_evolution/mutation"
)

// llmEvalExecutor adapts an ares_eval.LLMClient into an ares_eval.AgentExecutor
// for the G3 gate. The candidate strategy's PromptTemplate is prepended to
// every test-case input, so candidate and active strategies produce genuinely
// different outputs and the judge scores discriminate between them.
//
// Sampling params (temperature/max_tokens) are NOT applied: the bare
// LLMClient.Generate interface carries no sampling options. The prompt
// template is the artifact the GA actually mutates, so it is the honest
// discriminator here. Thread-safe: lifecycle Submit can run concurrently.
type llmEvalExecutor struct {
	client ares_eval.LLMClient

	mu             sync.Mutex
	promptTemplate string
}

func newLLMEvalExecutor(client ares_eval.LLMClient) *llmEvalExecutor {
	return &llmEvalExecutor{client: client}
}

// setCandidate records the candidate whose prompt template the next Execute
// calls run through. Invoked by the gate's beforeRun hook per Submit.
func (e *llmEvalExecutor) setCandidate(cand *mutation.Strategy) {
	if cand == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.promptTemplate = cand.PromptTemplate
}

// Execute implements ares_eval.AgentExecutor.
func (e *llmEvalExecutor) Execute(ctx context.Context, input string) (string, []string, int, error) {
	e.mu.Lock()
	tmpl := e.promptTemplate
	e.mu.Unlock()

	prompt := input
	if tmpl != "" {
		prompt = strings.TrimSpace(tmpl) + "\n\n" + input
	}
	out, err := e.client.Generate(ctx, prompt)
	if err != nil {
		return "", nil, 0, fmt.Errorf("eval executor generate: %w", err)
	}
	return out, nil, 0, nil
}

// errEvalGateNotConfigured marks the INTENTIONAL absence of the G3 gate
// (no registry / no eval LLM client / no suite path configured). Callers
// must treat it as "pipeline runs without G3" — the documented degradation
// contract — and never as a wiring failure. A CONFIGURED suite that fails
// to load returns a real error instead (fail closed).
var errEvalGateNotConfigured = errors.New("eval gate not configured")

// buildEvalGate constructs the G3 verify gate from the evaluator registry,
// the eval LLM client, and the YAML-configured suite path. Returns
// (nil, errEvalGateNotConfigured) when the gate is intentionally absent and
// an error when a CONFIGURED suite cannot be loaded (fail closed — see the
// package comment).
func buildEvalGate(
	registry *ares_eval.EvaluatorRegistry,
	client ares_eval.LLMClient,
	suitePath string,
	minScore float64,
	strict bool,
) (*evolution.EvalGate, error) {
	if registry == nil || client == nil || strings.TrimSpace(suitePath) == "" {
		// Gate intentionally absent — the pipeline runs without G3. This is
		// the documented degradation contract, not a fake pass-through.
		return nil, errEvalGateNotConfigured
	}
	suite, err := ares_eval.NewLoader().Load(suitePath)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: load eval suite %q (evolution.gates.eval_suite): %w", suitePath, err)
	}
	if len(suite.TestCases) == 0 {
		return nil, fmt.Errorf("bootstrap: eval suite %q contains no test cases", suitePath)
	}

	exec := newLLMEvalExecutor(client)
	runner, err := ares_eval.NewAgentTestRunner(exec)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: create eval runner: %w", err)
	}
	runner.SetRegistry(registry)

	gateCfg := evolution.DefaultEvalGateConfig()
	if minScore > 0 {
		gateCfg.MinScore = minScore
	}
	// E3: production opts into fail-closed via evolution.gates.eval_strict.
	gateCfg.StrictMode = strict
	gate := evolution.NewEvalGate(registry, runner, *suite, gateCfg,
		evolution.WithEvalGateBeforeRun(exec.setCandidate),
	)
	return gate, nil
}
