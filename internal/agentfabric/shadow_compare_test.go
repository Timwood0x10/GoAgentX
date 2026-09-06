package agentfabric

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/api/core"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
)

// scriptChat is a round-scripted ChatClient: round N (1-based) returns the
// Nth scripted response, then repeats the last one. It counts its own calls
// so the verdict's per-arm LLM accounting is pinned.
type scriptChat struct {
	mu        sync.Mutex
	calls     int
	responses []core.GenerateResponse
}

func toolCallResponse(id, name, args string) core.GenerateResponse {
	return core.GenerateResponse{
		ToolCalls: []core.ToolCall{{
			ID:   id,
			Type: "function",
			Function: core.FunctionCall{
				Name:      name,
				Arguments: args,
			},
		}},
	}
}

func (c *scriptChat) Chat(_ context.Context, _ []*core.LLMMessage, _ []core.Tool, _ map[string]any) (*core.GenerateResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	i := c.calls - 1
	if i >= len(c.responses) {
		i = len(c.responses) - 1
	}
	resp := c.responses[i]
	return &resp, nil
}

func (c *scriptChat) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// grepSchemas advertises one grep tool so both arms genuinely consider tools.
func grepSchemas() []resources.ToolSchema {
	return []resources.ToolSchema{{
		Name: "grep",
		Parameters: &resources.ParameterSchema{
			Properties: map[string]*resources.Parameter{
				"query": {Type: "string"},
			},
			Required: []string{"query"},
		},
	}}
}

// auditDenyBinder wraps the arm deny binder with a would-be production spy:
// the spy must stay at zero across a whole comparison, proving the shadow
// arms never reach the production tool surface.
type auditDenyBinder struct {
	*recordDenyBinder
	prodCalls *int
}

func (b *auditDenyBinder) CallTool(ctx context.Context, name string, args map[string]any) (any, error) {
	// Records + denies via the embedded arm binder; prod is never touched.
	return b.recordDenyBinder.CallTool(ctx, name, args)
}

// oneToolScript returns a script both arms agree on: one grep round, then
// the final answer.
func oneToolScript() []core.GenerateResponse {
	return []core.GenerateResponse{
		toolCallResponse("s-1", "grep", `{"query":"pattern"}`),
		{Content: "the answer is 42"},
	}
}

func shadowInput(session string, newChat func() ChatClient, prodCalls *int) DualPathInput {
	return DualPathInput{
		Prompt:    "find the answer",
		SessionID: session,
		NewChat:   newChat,
		NewBinder: func() ToolBinder {
			return &auditDenyBinder{recordDenyBinder: newRecordDenyBinder(grepSchemas()), prodCalls: prodCalls}
		},
		MaxRounds: 5,
		Archive:   newMemArchive(),
	}
}

// TestShadowCompare_MatchOnSameScript pins the B1 happy path: the same
// 1-tool-then-answer script on both arms yields identical tool sequences,
// equal LLM call counts, and no archived samples.
func TestShadowCompare_MatchOnSameScript(t *testing.T) {
	ctx := context.Background()
	var prodCalls int
	in := shadowInput("shadow-match", func() ChatClient {
		return &scriptChat{responses: oneToolScript()}
	}, &prodCalls)

	verdict, err := CompareDualPath(ctx, in)
	require.NoError(t, err)
	require.True(t, verdict.Match, "same script on both arms must agree")
	require.Equal(t, []string{"grep"}, verdict.LegacySeq)
	require.Equal(t, []string{"grep"}, verdict.DAGSeq)
	require.Equal(t, verdict.LegacyLLMCalls, verdict.DAGLLMCalls,
		"both arms run the same number of LLM rounds")
	require.Equal(t, 2, verdict.LegacyLLMCalls, "1 tool round + 1 answer round")
	require.Empty(t, verdict.Mismatches)
	require.Equal(t, 0, prodCalls, "shadow arms must never reach production tools")
	require.Empty(t, in.Archive.(*memArchive).Samples())
}

// TestShadowCompare_MismatchIsArchived pins the B1 triage path: arms that
// decide differently produce a non-match verdict AND a persisted sample
// carrying both sequences — never a dropped divergence.
func TestShadowCompare_MismatchIsArchived(t *testing.T) {
	ctx := context.Background()
	var prodCalls int
	arm := 0
	in := shadowInput("shadow-mismatch", func() ChatClient {
		arm++
		if arm == 1 {
			return &scriptChat{responses: oneToolScript()}
		}
		return &scriptChat{responses: []core.GenerateResponse{
			toolCallResponse("s-1", "read", `{"path":"f"}`),
			{Content: "other answer"},
		}}
	}, &prodCalls)
	// The DAG arm only knows grep; advertise read too so its decision is
	// real, not a schema artifact.
	in.NewBinder = func() ToolBinder {
		schemas := append(grepSchemas(), resources.ToolSchema{Name: "read"})
		return &auditDenyBinder{recordDenyBinder: newRecordDenyBinder(schemas), prodCalls: &prodCalls}
	}

	verdict, err := CompareDualPath(ctx, in)
	require.NoError(t, err)
	require.False(t, verdict.Match, "different decisions must not compare equal")
	require.Equal(t, []string{"grep"}, verdict.LegacySeq)
	require.Equal(t, []string{"read"}, verdict.DAGSeq)
	require.Len(t, verdict.Mismatches, 1)
	sample := verdict.Mismatches[0]
	require.Equal(t, "shadow-mismatch", sample.SessionID)
	require.Equal(t, "find the answer", sample.Prompt)
	require.Equal(t, "tool-sequence", sample.Reason)
	require.Equal(t, 1, len(in.Archive.(*memArchive).Samples()),
		"the sample must reach the archive, not just the verdict")
}

// TestShadowCompare_ZeroSideEffects pins the hard constraint: across a full
// comparison (both arms, all rounds) the production tool surface sees zero
// calls. The audit binder would forward nothing by construction; this test
// locks that construction in.
func TestShadowCompare_ZeroSideEffects(t *testing.T) {
	ctx := context.Background()
	var prodCalls int
	in := shadowInput("shadow-noeffect", func() ChatClient {
		return &scriptChat{responses: oneToolScript()}
	}, &prodCalls)

	_, err := CompareDualPath(ctx, in)
	require.NoError(t, err)
	require.Equal(t, 0, prodCalls)
}

// TestShadowCompare_RequiresInput pins fail-fast on unusable input: without
// a chat factory, binder factory, or session scope there is no fair
// comparison to run.
func TestShadowCompare_RequiresInput(t *testing.T) {
	ctx := context.Background()
	valid := shadowInput("shadow-input", func() ChatClient {
		return &scriptChat{responses: oneToolScript()}
	}, new(int))

	valid.NewChat = nil
	if _, err := CompareDualPath(ctx, valid); err == nil {
		t.Error("nil NewChat must fail, not run a one-armed comparison")
	}
	valid = shadowInput("shadow-input", func() ChatClient {
		return &scriptChat{responses: oneToolScript()}
	}, new(int))
	valid.NewBinder = nil
	if _, err := CompareDualPath(ctx, valid); err == nil {
		t.Error("nil NewBinder must fail, not run a one-armed comparison")
	}
	valid = shadowInput("", func() ChatClient {
		return &scriptChat{responses: oneToolScript()}
	}, new(int))
	if _, err := CompareDualPath(ctx, valid); err == nil {
		t.Error("empty SessionID must fail, not grow unscoped nodes")
	}
}

// memArchive is an in-memory MismatchArchive for tests and single-process
// triage. Safe for concurrent use.
type memArchive struct {
	mu      sync.Mutex
	samples []MismatchSample
}

func newMemArchive() *memArchive { return &memArchive{} }

// Archive implements MismatchArchive.
func (a *memArchive) Archive(s MismatchSample) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.samples = append(a.samples, s)
}

// Samples returns a copy of the archived samples.
func (a *memArchive) Samples() []MismatchSample {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]MismatchSample(nil), a.samples...)
}
