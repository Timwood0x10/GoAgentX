package sub

import (
	"context"
	"errors"
	"testing"

	apperrors "github.com/Timwood0x10/ares/internal/errors"
	"github.com/Timwood0x10/ares/internal/feedback"
	kctx "github.com/Timwood0x10/ares/internal/kernel/ctx"
)

// recordingToolObserver captures tool outcomes for assertions.
type recordingToolObserver struct {
	got []feedback.ToolCallOutcome
}

func (o *recordingToolObserver) OnToolCall(out feedback.ToolCallOutcome) {
	o.got = append(o.got, out)
}

// TestObserveToolCalls_ClassifiesOutcomes is the Step Y.3 acceptance: every
// tool invocation produces one record, and the outcome distinguishes "the tool
// worked", "the tool ran and failed", and "the strategy asked for a tool that
// does not exist". The last one is the case worth separating — it is a decision
// error by the strategy, not a tool defect.
func TestObserveToolCalls_ClassifiesOutcomes(t *testing.T) {
	toolErr := errors.New("upstream 500")
	tests := []struct {
		name    string
		tool    string
		bind    func(b ToolBinder)
		want    feedback.Outcome
		wantErr bool
	}{
		{
			name: "success",
			tool: "search",
			bind: func(b ToolBinder) {
				b.BindTool("search", func(context.Context, map[string]any) (any, error) {
					return "hit", nil
				})
			},
			want: feedback.OutcomeSuccess,
		},
		{
			name: "tool ran and failed",
			tool: "search",
			bind: func(b ToolBinder) {
				b.BindTool("search", func(context.Context, map[string]any) (any, error) {
					return nil, toolErr
				})
			},
			want:    feedback.OutcomeFailure,
			wantErr: true,
		},
		{
			name:    "tool does not exist",
			tool:    "nonexistent",
			bind:    func(ToolBinder) {},
			want:    feedback.OutcomeNotFound,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inner := NewToolBinder()
			tc.bind(inner)
			obs := &recordingToolObserver{}
			binder := ObserveToolCalls(inner, obs)

			ctx := kctx.WithCallerID(context.Background(), "agent-7")
			_, err := binder.CallTool(ctx, tc.tool, nil)
			if tc.wantErr != (err != nil) {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}

			if len(obs.got) != 1 {
				t.Fatalf("want exactly 1 tool record, got %d", len(obs.got))
			}
			rec := obs.got[0]
			if rec.Outcome != tc.want {
				t.Errorf("outcome = %q, want %q", rec.Outcome, tc.want)
			}
			if rec.Tool != tc.tool {
				t.Errorf("tool = %q, want %q", rec.Tool, tc.tool)
			}
			// The caller identity must survive: without it evolution cannot tell
			// which agent's strategy made the call.
			if rec.Caller != "agent-7" {
				t.Errorf("caller = %q, want agent-7", rec.Caller)
			}
			if rec.Latency < 0 {
				t.Errorf("latency = %v, want >= 0", rec.Latency)
			}
		})
	}
}

// TestObserveToolCalls_CancellationIsUnobserved locks the exclusion: when the
// caller's context is already dead, the tool earned neither credit nor blame.
func TestObserveToolCalls_CancellationIsUnobserved(t *testing.T) {
	inner := NewToolBinder()
	inner.BindTool("slow", func(ctx context.Context, _ map[string]any) (any, error) {
		return nil, ctx.Err()
	})
	obs := &recordingToolObserver{}
	binder := ObserveToolCalls(inner, obs)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := binder.CallTool(ctx, "slow", nil); err == nil {
		t.Fatal("want an error from the cancelled call")
	}

	if len(obs.got) != 1 {
		t.Fatalf("want 1 record, got %d", len(obs.got))
	}
	if got := obs.got[0].Outcome; got != feedback.OutcomeUnobserved {
		t.Errorf("outcome = %q, want unobserved", got)
	}
	if obs.got[0].Outcome.Observable() {
		t.Error("an abandoned call must not be observable — it would become fitness evidence")
	}
}

// TestObserveToolCalls_PassesThroughBinderSurface locks that the decorator adds
// measurement WITHOUT changing behavior: the same tools are advertised, the same
// results come back, and idempotency metadata is preserved. If the wrapper
// filtered the tool set, the measured system would no longer be the production
// system.
func TestObserveToolCalls_PassesThroughBinderSurface(t *testing.T) {
	inner := NewToolBinder()
	inner.BindTool("plain", func(context.Context, map[string]any) (any, error) { return 1, nil })
	inner.(interface {
		BindIdempotentTool(string, func(context.Context, map[string]any) (any, error))
	}).BindIdempotentTool("safe", func(context.Context, map[string]any) (any, error) { return 2, nil })

	binder := ObserveToolCalls(inner, &recordingToolObserver{})

	if got, want := len(binder.ListTools()), len(inner.ListTools()); got != want {
		t.Errorf("ListTools count = %d, want %d", got, want)
	}
	if !binder.IsToolIdempotent("safe") {
		t.Error("idempotency metadata lost through the decorator")
	}
	if binder.IsToolIdempotent("plain") {
		t.Error("non-idempotent tool reported as idempotent")
	}
	res, err := binder.CallTool(context.Background(), "safe", nil)
	if err != nil || res != 2 {
		t.Errorf("CallTool = (%v, %v), want (2, nil)", res, err)
	}
	if got, want := len(binder.GetToolSchemas()), len(inner.GetToolSchemas()); got != want {
		t.Errorf("GetToolSchemas count = %d, want %d", got, want)
	}
}

// TestObserveToolCalls_NilObserverReturnsInner locks the default path: with no
// observer there is no wrapper, so the unarmed configuration adds not even an
// indirection.
func TestObserveToolCalls_NilObserverReturnsInner(t *testing.T) {
	inner := NewToolBinder()
	if got := ObserveToolCalls(inner, nil); got != inner {
		t.Error("a nil observer must return the inner binder unchanged")
	}
	if got := ObserveToolCalls(nil, &recordingToolObserver{}); got != nil {
		t.Error("a nil binder must stay nil")
	}
}

// TestToolCallOutcome_NotFoundIsErrToolNotFound guards the classification
// against a refactor of the binder's sentinel: if ErrToolNotFound stops
// propagating, "the strategy asked for a tool that does not exist" would silently
// be scored as a generic failure and the distinction would rot away unnoticed.
func TestToolCallOutcome_NotFoundIsErrToolNotFound(t *testing.T) {
	if got := toolCallOutcome(context.Background(), apperrors.ErrToolNotFound); got != feedback.OutcomeNotFound {
		t.Errorf("outcome = %q, want not_found", got)
	}
	wrapped := apperrors.Wrap(apperrors.ErrToolNotFound, "binder")
	if got := toolCallOutcome(context.Background(), wrapped); got != feedback.OutcomeNotFound {
		t.Errorf("wrapped outcome = %q, want not_found", got)
	}
	if got := toolCallOutcome(context.Background(), errors.New("other")); got != feedback.OutcomeFailure {
		t.Errorf("outcome = %q, want failure", got)
	}
}
