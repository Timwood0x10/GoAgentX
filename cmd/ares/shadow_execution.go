// shadow_execution.go wires the real-execution shadow A/B path (closure plan
// Step 4 / N-1) into the peer-mode kernel: the scheduler buffers finalized
// real tasks, and each candidate judgment executes the candidate AND the
// active strategy on buffered task copies inside an isolated runner whose
// side effects are denied at the interface level.
package main

import (
	"context"
	"errors"
	"log"
	"sync/atomic"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agents"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/ares_bootstrap"
	"github.com/Timwood0x10/ares/internal/ares_config"
	evolution "github.com/Timwood0x10/ares/internal/ares_evolution"
	"github.com/Timwood0x10/ares/internal/ares_evolution/mutation"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/llm/output"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
)

// errShadowToolDenied is the sentinel every tool call hits inside a shadow
// execution. Shadow verdicts must measure the strategy, not side effects the
// strategy triggered: tool calls are intercepted BEFORE reaching the
// production binder — nothing is written, nothing is sent.
var errShadowToolDenied = errors.New("shadow-mode: tool calls disabled")

// shadowMaxToolRounds caps one isolated pass. A task that needs more rounds
// than this scores 0.0 for BOTH arms equally (they run under the same cap), so
// the paired comparison stays fair while the shadow cost stays bounded.
const shadowMaxToolRounds = 3

// shadowDenyBinder is the side-effect deny-list (closure plan Step 4: the
// hard constraint is enforced HERE, at the interface the shadow cognition
// uses — not by caller discipline). Tool calls are never delegated.
type shadowDenyBinder struct {
	// inner is retained ONLY as the test/audit surface: the interface-level
	// spy wraps it and asserts the production binder saw zero calls during
	// an entire shadow execution. Never invoked by this type.
	inner sub.ToolBinder
}

// CallTool implements agentfabric.ToolBinder. Always denied.
func (b *shadowDenyBinder) CallTool(context.Context, string, map[string]any) (any, error) {
	return nil, errShadowToolDenied
}

// ListTools implements agentfabric.ToolBinder. Returns nil so the shadow LLM
// is never even offered a tool to call (defense in depth on top of CallTool).
func (b *shadowDenyBinder) ListTools() []string { return nil }

// IsToolIdempotent implements agentfabric.ToolBinder. No tool is callable, so
// none is idempotent.
func (b *shadowDenyBinder) IsToolIdempotent(string) bool { return false }

// GetToolSchemas implements agentfabric.ToolBinder. Returns nil: the shadow
// arm advertises no tools.
func (b *shadowDenyBinder) GetToolSchemas() []resources.ToolSchema { return nil }

// fixedStrategySource pins one strategy for a shadow cognition so the A/B arm
// executes under the exact prompt + LLM params of ITS strategy — not whatever
// the ASM currently has active (that would make both arms the same strategy).
type fixedStrategySource struct{ st *agents.ActiveStrategy }

// GetActiveStrategy implements agents.StrategySource. Always returns the
// pinned strategy (nil only when constructed with one).
func (s *fixedStrategySource) GetActiveStrategy(context.Context) (*agents.ActiveStrategy, error) {
	return s.st, nil
}

// shadowCognitionFactory builds one isolated tool-loop cognition per A/B arm.
// It reuses the SAME LLM stack as the production peers so the shadow arm runs
// the real execution path — the only difference is the pinned strategy and
// the deny-list binder.
type shadowCognitionFactory struct {
	chatClient sub.ChatClient
	llmAdapter output.LLMAdapter
	deny       *shadowDenyBinder
	promptTmpl string
}

// For builds the cognition for one strategy arm.
func (f *shadowCognitionFactory) For(strategy *mutation.Strategy) (agentfabric.Cognition, error) {
	if strategy == nil {
		return nil, errors.New("shadow execution: nil strategy")
	}
	return agentfabric.NewChatCognition(agentfabric.ChatCognitionDeps{
		ChatClient: f.chatClient,
		LLMAdapter: f.llmAdapter,
		ToolBinder: f.deny,
		StrategySource: &fixedStrategySource{st: &agents.ActiveStrategy{
			ID:     strategy.ID,
			Prompt: strategy.PromptTemplate,
			Params: strategy.Params,
		}},
		Template:       output.NewTemplateEngine(),
		PromptTemplate: f.promptTmpl,
		// Shadow events must not enter the production event bus: the
		// RuntimeObserver attributes fitness by task events, and shadow
		// runs have their own evidence writer.
		EventStore: nil,
		AgentID:    "shadow-" + strategy.ID,
	})
}

// shadowQuantumRunner implements evolution.ShadowRunner: one isolated pass =
// up to shadowMaxToolRounds quanta of the chat tool-loop, re-wrapping the
// yield checkpoint into the payload exactly like the scheduler's
// buildQuantumStep resume path (the resumed quantum reads
// payload["checkpoint"]).
type shadowQuantumRunner struct {
	factory *shadowCognitionFactory
	// skipped counts buffered tasks the runner declined as out-of-scope
	// (M4-C1 below). Same observability rationale as the planner's
	// ForcedAnswers: a silently-skipped sample class would bias every
	// judgment built on the buffer.
	skipped atomic.Uint64
}

// Skipped reports how many buffered tasks were declined as out-of-scope for
// strategy-shadow (M4-C1: L2 session tasks).
func (r *shadowQuantumRunner) Skipped() uint64 {
	if r == nil {
		return 0
	}
	return r.skipped.Load()
}

// RunShadow implements evolution.ShadowRunner.
func (r *shadowQuantumRunner) RunShadow(ctx context.Context, task *models.Task, strategy *mutation.Strategy) (bool, error) {
	if task == nil {
		return false, errors.New("shadow execution: nil task")
	}
	if agentfabric.IsL2Capability(string(task.AgentType)) {
		// M4-C1: L2 session tasks are out of scope for strategy-shadow.
		// Strategies judge ReAct prompts; the planner does not consume
		// strategies, so an A/B verdict on an L2 task would be noise, not
		// signal — and running it through the chat body would be worse
		// (a plan task read as a recommendation request). L2 coverage
		// comes from the B1 dual-path comparison and B2 canary metrics,
		// not from here. The skip is neutral (false, nil): the same
		// non-outcome an over-budget pass reports, so neither arm gains.
		r.skipped.Add(1)
		log.Printf("shadow: skip L2 task %q (%s) — out of scope for strategy judgment", task.TaskID, task.AgentType)
		return false, nil
	}
	cog, err := r.factory.For(strategy)
	if err != nil {
		return false, err
	}
	for round := 0; round < shadowMaxToolRounds; round++ {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		out, err := cog.ExecuteStep(ctx, task)
		if err != nil {
			return false, err
		}
		if out.Done {
			return true, nil
		}
		if out.Checkpoint != nil {
			if task.Payload == nil {
				task.Payload = make(map[string]any)
			}
			task.Payload["checkpoint"] = out.Checkpoint
		}
	}
	return false, nil
}

// wireShadowExecution attaches the real-execution shadow A/B path to the
// peer kernel. It is a no-op unless evolution AND shadow execution are both
// enabled and the G2 shadow sampler is wired — every silent return keeps the
// pre-Step-4 behavior (replay-only shadow evidence).
//
// Attach points (structural, no import between the two layers):
//   - sched.WithShadowExecutionHook: finalized tasks land in the executor's
//     buffer (kernelscheduler.ShadowExecutionHook).
//   - sampler.SetExecutionFeeder: candidate judgments run the isolated A/B
//     pass before the replay fallback (evolution.ShadowExecutionFeeder).
func wireShadowExecution(
	cfg *ares_config.Config,
	comp *ares_bootstrap.Components,
	sched *kernelScheduler,
	llmAdapter output.LLMAdapter,
	chatClient sub.ChatClient,
	toolBinder sub.ToolBinder,
) {
	if cfg == nil || !cfg.Evolution.Enabled || !cfg.Evolution.ShadowExecution.Enabled {
		return
	}
	if comp == nil || comp.NewEvolution == nil || comp.NewEvolution.Lifecycle == nil {
		return
	}
	if comp.EvidenceStore == nil || sched == nil {
		return
	}
	sampler := comp.NewEvolution.Lifecycle.ShadowSampler()
	if sampler == nil {
		log.Print("peer mode: shadow execution disabled — no G2 shadow sampler wired")
		return
	}
	exec, err := evolution.NewShadowExecutor(
		comp.EvidenceStore,
		&shadowQuantumRunner{factory: &shadowCognitionFactory{
			chatClient: chatClient,
			llmAdapter: llmAdapter,
			deny:       &shadowDenyBinder{inner: toolBinder},
			promptTmpl: cfg.Prompts.Recommendation,
		}},
		cfg.Evolution.ShadowExecution.SampleSize,
	)
	if err != nil {
		log.Printf("peer mode: shadow execution disabled: %v", err)
		return
	}
	sched.WithShadowExecutionHook(exec)
	sampler.SetExecutionFeeder(exec)
	log.Printf("peer mode: shadow execution wired (sample_size=%d)", cfg.Evolution.ShadowExecution.SampleSize)
}
