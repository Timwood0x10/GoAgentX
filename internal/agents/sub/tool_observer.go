// tool_observer.go closes the second half of N-11 (closure plan Step Y.3): tool
// calls now produce an evolution feature, not only a distillation side note.
//
// Before this file a tool's success or failure reached `emitSubTaskResult` (the
// experience-distillation bypass) and stopped there. Nothing scored it, so the
// evolution loop could not learn "this strategy keeps calling a tool that always
// errors" — the tool channel, one of the three ways an agent perceives the
// outside world, was invisible to fitness.
//
// WHY A DECORATOR, NOT A HOOK INSIDE THE EXECUTOR: there are two production
// execution bodies — the sub executor's tool loop and agentfabric's
// ChatCognition — and both invoke tools through an injected ToolBinder. Wrapping
// the binder measures BOTH with one implementation and cannot be bypassed by a
// third execution path added later. Instrumenting either loop instead would
// leave the other blind, which is how the tool channel came to be a gap in the
// first place.
//
// The observer interface is declared here, at the consumer (code_rules), and
// this package never imports the evolution layer:
// ares_evolution.ChannelFeedbackRecorder satisfies it structurally.
package sub

import (
	"context"
	stderrors "errors"
	"sort"
	"strings"
	"time"

	"github.com/Timwood0x10/ares/internal/errors"
	"github.com/Timwood0x10/ares/internal/feedback"
	kctx "github.com/Timwood0x10/ares/internal/kernel/ctx"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
)

// ToolCallObserver receives one record per tool invocation. Implementations
// MUST NOT block: the observer runs inside the agent's tool call, so latency
// here is latency the agent's task pays.
type ToolCallObserver interface {
	// OnToolCall reports one tool invocation outcome.
	OnToolCall(out feedback.ToolCallOutcome)
}

// observedToolBinder wraps a ToolBinder and reports every CallTool outcome to
// the observer. Every other method delegates untouched — the decorator adds
// measurement, never behavior.
type observedToolBinder struct {
	// ToolBinder is embedded so the decorator automatically forwards the whole
	// surface (BindTool, ListTools, BridgeFromRegistry, …). Embedding rather
	// than hand-forwarding also means a future method added to the interface
	// cannot silently lose its implementation here.
	ToolBinder
	// obs receives one record per call. Never nil (ObserveToolCalls returns
	// the inner binder unchanged when no observer is supplied).
	obs ToolCallObserver
}

// ObserveToolCalls wraps a binder so each tool invocation is reported to the
// observer (Step Y.3). A nil binder or nil observer returns the binder
// unchanged: an unobserved binder is the pre-Step-Y behavior, and returning a
// wrapper with a nil observer would only add an indirection that measures
// nothing.
//
// Args:
//   - inner: the binder to wrap.
//   - obs: the non-blocking outcome observer.
//
// Returns:
//   - ToolBinder: the wrapped binder, or inner when there is nothing to do.
func ObserveToolCalls(inner ToolBinder, obs ToolCallObserver) ToolBinder {
	if inner == nil || obs == nil {
		return inner
	}
	return &observedToolBinder{ToolBinder: inner, obs: obs}
}

// CallTool invokes the wrapped binder and reports the outcome.
//
// Outcome mapping: a nil error is success; ErrToolNotFound is not_found (the
// STRATEGY asked for a tool that does not exist — an addressing mistake worth
// learning from, distinct from a tool that ran and failed); any other error is
// failure. A context cancellation is reported as unobserved: the caller walked
// away, so the tool earned neither credit nor blame.
func (b *observedToolBinder) CallTool(ctx context.Context, name string, args map[string]any) (any, error) {
	started := time.Now()
	result, err := b.ToolBinder.CallTool(ctx, name, args)
	b.obs.OnToolCall(feedback.ToolCallOutcome{
		Tool:       name,
		Caller:     kctx.CallerID(ctx),
		Outcome:    toolCallOutcome(ctx, err),
		Latency:    time.Since(started),
		ToolStepID: b.toolClassIDForCall(name, args),
	})
	return result, err
}

// toolClassIDForCall builds the M6 attribution key: toolName#schemaShape,
// where schemaShape comes from the tool's DECLARED schema (shared
// resources.ToolArgShape) — the same derivation the L1 graph builder and
// the planner constraint check use. A call that omits an optional parameter
// must still attribute to the L1 node, so an args-derived shape (which would
// miss the node and fall through to permissive) is only the fallback when
// the tool has no known schema.
func (b *observedToolBinder) toolClassIDForCall(name string, args map[string]any) string {
	if b.ToolBinder != nil {
		for _, s := range b.ToolBinder.GetToolSchemas() {
			if s.Name == name {
				shape := resources.ToolArgShape(s)
				if shape == "" {
					return name
				}
				return name + "#" + shape
			}
		}
	}
	return toolStepID(name, args)
}

// toolStepID builds the process-level attribution key (Y1 C3): toolName#argShape,
// where argShape is the sorted set of argument KEY names (values dropped). This
// mirrors ares_events.ToolArgShape but accepts a decoded map (the binder hands
// the observer a struct, not the raw JSON string), so the recorder and the
// projection layer attribute tool-call fitness at the same granularity.
func toolStepID(name string, args map[string]any) string {
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	shape := strings.Join(keys, ",")
	if shape == "" {
		return name // no args: attribute at the tool (not tool-step) granularity
	}
	return name + "#" + shape
}

// toolCallOutcome classifies one invocation result.
func toolCallOutcome(ctx context.Context, err error) feedback.Outcome {
	if err == nil {
		return feedback.OutcomeSuccess
	}
	if ctx.Err() != nil {
		// The call was abandoned from above; whatever the tool returned says
		// nothing about the tool.
		return feedback.OutcomeUnobserved
	}
	if stderrors.Is(err, errors.ErrToolNotFound) {
		return feedback.OutcomeNotFound
	}
	return feedback.OutcomeFailure
}

// GetToolSchemas delegates. Declared explicitly only to document that the
// decorator does NOT filter the advertised tool set: measuring tool outcomes
// must not change which tools the LLM is offered, or the measurement would be
// of a different system than the one running in production.
func (b *observedToolBinder) GetToolSchemas() []resources.ToolSchema {
	return b.ToolBinder.GetToolSchemas()
}
