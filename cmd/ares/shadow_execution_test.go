package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/agents"
	evolution "github.com/Timwood0x10/ares/internal/ares_evolution"
	"github.com/Timwood0x10/ares/internal/ares_evolution/mutation"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/evidence"
	"github.com/Timwood0x10/ares/internal/llm/output"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
)

// shadowLLMAdapter is the text-only fallback the tool loop degrades to when
// the tool call is denied; it answers immediately so the pass terminates.
type shadowLLMAdapter struct{}

func (shadowLLMAdapter) Generate(context.Context, string) (string, error) {
	return "done", nil
}
func (shadowLLMAdapter) GenerateWithParams(_ context.Context, _ string, _ map[string]any) (string, error) {
	return "done", nil
}
func (shadowLLMAdapter) GenerateStructured(context.Context, string, string) (*models.RecommendResult, error) {
	return nil, errors.New("not used")
}
func (shadowLLMAdapter) GenerateStream(context.Context, string) (<-chan output.StreamChunk, error) {
	return nil, errors.New("not used")
}
func (shadowLLMAdapter) GetModel() string { return "shadow-fake" }

// spyToolBinder is the interface-level spy (closure plan Step 4): it counts
// every tool call that REACHES the production-side binder. During a shadow
// execution the deny binder sits in front of it, so the count must stay 0.
type spyToolBinder struct {
	mu       sync.Mutex
	callTool int
	list     int
}

func (b *spyToolBinder) BindTool(string, func(ctx context.Context, args map[string]any) (any, error)) {
}
func (b *spyToolBinder) CallTool(context.Context, string, map[string]any) (any, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.callTool++
	return nil, nil
}
func (b *spyToolBinder) ListTools() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.list++
	return []string{"production-tool"}
}
func (b *spyToolBinder) IsToolIdempotent(string) bool { return false }
func (b *spyToolBinder) ListIdempotentTools() []string {
	return nil
}
func (b *spyToolBinder) GetToolSchemas() []resources.ToolSchema {
	return []resources.ToolSchema{{Name: "production-tool"}}
}
func (b *spyToolBinder) BridgeFromRegistry(*resources.Registry) {}
func (b *spyToolBinder) WithPlannerBridge(bridge interface {
	Execute(ctx context.Context, toolName string, params map[string]any, userRequest string) (resources.Result, error)
}) {
}

func (b *spyToolBinder) calls() (int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.callTool, b.list
}

// shadowChatClient scripts the LLM: first call demands a tool call (the arm
// MUST be denied), later calls finish the task.
type shadowChatClient struct {
	mu    sync.Mutex
	calls int
	tools int // number of tools the LLM was last offered
}

func (c *shadowChatClient) Chat(_ context.Context, _ []*core.LLMMessage, tools []core.Tool, _ map[string]any) (*core.GenerateResponse, error) {
	c.mu.Lock()
	c.calls++
	c.tools = len(tools)
	c.mu.Unlock()
	return &core.GenerateResponse{
		ToolCalls: []core.ToolCall{{
			ID:       "call-1",
			Type:     "function",
			Function: core.FunctionCall{Name: "production-tool", Arguments: `{}`},
		}},
		Content: "trying the tool",
	}, nil
}

// TestShadowDenyBinder_InterceptsAllToolCalls locks the Step 4 isolation
// contract: an entire shadow execution that tries to call tools reaches the
// production binder ZERO times — no writes, no requests, no schemas leaked.
func TestShadowDenyBinder_InterceptsAllToolCalls(t *testing.T) {
	ctx := context.Background()
	spy := &spyToolBinder{}
	deny := &shadowDenyBinder{inner: spy}
	runner := &shadowQuantumRunner{factory: &shadowCognitionFactory{
		chatClient: &shadowChatClient{},
		llmAdapter: shadowLLMAdapter{},
		deny:       deny,
		promptTmpl: "do {{.task_desc}}",
	}}

	task := models.NewTask("t1#shadow", "code", nil)
	task.Payload = map[string]any{"task_desc": "work"}
	completed, err := runner.RunShadow(ctx, task, &mutation.Strategy{ID: "cand-1", PromptTemplate: "p"})
	if err != nil {
		t.Fatalf("RunShadow: %v", err)
	}
	// The scripted LLM keeps demanding a tool; every demand is denied and the
	// arm degrades to the text-only path. A denied tool must not break the
	// pass — completion is still a real outcome the comparison can use.
	if !completed {
		t.Fatal("a tool-denied pass must still complete via the text-only fallback")
	}

	calls, lists := spy.calls()
	if calls != 0 {
		t.Fatalf("production binder saw %d tool calls during shadow execution, want 0", calls)
	}
	if lists != 0 {
		t.Fatalf("production binder ListTools called %d times, want 0", lists)
	}
	if tools := deny.ListTools(); len(tools) != 0 {
		t.Fatalf("shadow arm must be offered no tools, got %v", tools)
	}
	if _, err := deny.CallTool(ctx, "production-tool", nil); !errors.Is(err, errShadowToolDenied) {
		t.Fatalf("CallTool must return the shadow sentinel, got %v", err)
	}
	if len(deny.GetToolSchemas()) != 0 {
		t.Fatal("shadow arm must leak no tool schemas")
	}
}

// TestFixedStrategySource_PinsArm locks the A/B pinning: each arm executes
// under ITS strategy's prompt + params, not the currently active one.
func TestFixedStrategySource_PinsArm(t *testing.T) {
	src := &fixedStrategySource{st: &agents.ActiveStrategy{ID: "cand-1", Prompt: "p1", Params: map[string]any{"k": "v"}}}
	st, err := src.GetActiveStrategy(context.Background())
	if err != nil {
		t.Fatalf("GetActiveStrategy: %v", err)
	}
	if st == nil || st.ID != "cand-1" || st.Prompt != "p1" || st.Params["k"] != "v" {
		t.Fatalf("pinned strategy wrong: %+v", st)
	}
}

// TestShadowExecutionEndToEnd is the Step 4 acceptance in miniature: buffer a
// finalized real task, run the candidate judgment, and observe shadow-marked
// evidence for BOTH arms attributed to their own strategy IDs — with the
// production binder untouched.
func TestShadowExecutionEndToEnd(t *testing.T) {
	spy := &spyToolBinder{}
	store := evidence.NewMemoryStore()
	runner := &shadowQuantumRunner{factory: &shadowCognitionFactory{
		chatClient: &shadowChatClient{},
		llmAdapter: shadowLLMAdapter{},
		deny:       &shadowDenyBinder{inner: spy},
		promptTmpl: "do {{.task_desc}}",
	}}
	exec, err := evolution.NewShadowExecutor(store, runner, 3)
	if err != nil {
		t.Fatalf("NewShadowExecutor: %v", err)
	}

	// The scheduler-side capture: a finalized real task lands in the buffer.
	exec.OnTaskFinalized(func() *models.Task {
		tk := models.NewTask("t1", "code", nil)
		tk.Payload = map[string]any{"task_desc": "work"}
		return tk
	}())

	pairs := exec.Feed(context.Background(), &mutation.Strategy{ID: "cand-1"}, &mutation.Strategy{ID: "active-1"})
	if len(pairs) != 1 {
		t.Fatalf("expected 1 paired comparison, got %d", len(pairs))
	}

	evs, err := store.Query(context.Background(), evidence.Filter{
		Source: "strategy_shadow",
		Kind:   evidence.KindFitness,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	seen := map[string]bool{}
	for _, ev := range evs {
		var fe struct {
			StrategyID string `json:"strategy_id"`
			Shadow     bool   `json:"shadow"`
		}
		if err := json.Unmarshal(ev.Payload, &fe); err != nil {
			t.Fatalf("payload: %v", err)
		}
		if !fe.Shadow {
			t.Fatalf("record %s missing shadow marker", ev.ID)
		}
		seen[fe.StrategyID] = true
	}
	if !seen["cand-1"] || !seen["active-1"] {
		t.Fatalf("both arms must leave shadow evidence, saw %v", seen)
	}
	if calls, _ := spy.calls(); calls != 0 {
		t.Fatalf("production binder saw %d calls, want 0", calls)
	}
}

// TestWireShadowExecution_DisabledByDefault locks the default-off guarantee:
// without the config flags the wiring is a no-op and nothing panics on nils.
func TestWireShadowExecution_DisabledByDefault(t *testing.T) {
	wireShadowExecution(nil, nil, nil, nil, nil, nil)
}

// TestShadowRunner_SkipsL2Tasks pins M4-C1: L2 session tasks are out of scope
// for strategy-shadow — the planner does not consume strategies, so an A/B
// verdict would be noise, and running a plan task through the chat body would
// read it as a recommendation request. The skip is neutral (false, nil): the
// same non-outcome an over-budget pass reports, so neither arm gains.
func TestShadowRunner_SkipsL2Tasks(t *testing.T) {
	ctx := context.Background()
	runner := &shadowQuantumRunner{factory: &shadowCognitionFactory{
		chatClient: &shadowChatClient{},
		llmAdapter: shadowLLMAdapter{},
		deny:       &shadowDenyBinder{},
		promptTmpl: "do {{.task_desc}}",
	}}

	for _, capability := range []string{"ares/plan", "ares/root", "ares/answer", "tool/grep"} {
		task := models.NewTask("shadow-l2", models.AgentType(capability), nil)
		completed, err := runner.RunShadow(ctx, task, &mutation.Strategy{ID: "cand-1"})
		if err != nil {
			t.Fatalf("RunShadow(%s) error = %v, want neutral skip", capability, err)
		}
		if completed {
			t.Errorf("RunShadow(%s) = true, want false (skipped, not completed)", capability)
		}
	}
	if got := runner.Skipped(); got != 4 {
		t.Errorf("Skipped() = %d, want 4 (one per declined task)", got)
	}

	// Legacy capabilities still run the chat arm.
	legacy := models.NewTask("shadow-legacy", "code", nil)
	legacy.Payload = map[string]any{"task_desc": "work"}
	if _, err := runner.RunShadow(ctx, legacy, &mutation.Strategy{ID: "cand-1"}); err != nil {
		t.Fatalf("legacy task must still run: %v", err)
	}
	if got := runner.Skipped(); got != 4 {
		t.Errorf("Skipped() = %d after legacy run, want still 4", got)
	}
}

// TestShadowRunner_NilTaskFailsFast pins fail-loud on unusable input: a nil
// task previously panicked on payload access; now it errors.
func TestShadowRunner_NilTaskFailsFast(t *testing.T) {
	runner := &shadowQuantumRunner{}
	if _, err := runner.RunShadow(context.Background(), nil, &mutation.Strategy{ID: "cand-1"}); err == nil {
		t.Error("RunShadow(nil task) must fail, not panic or pass")
	}
	if got := runner.Skipped(); got != 0 {
		t.Errorf("Skipped() = %d after input error, want 0 (errors are not skips)", got)
	}
}
