//go:build e2e

// Live LLM canary for the L2 execution path (M4-B2): the SAME prompt runs
// through the legacy ReAct body and the FULL L2 stack (planner → subscription
// → real scheduler → router → echo tools) against a REAL model, and the test
// asserts both arms chose the same tool sequence.
//
// Build tag: e2e — needs a REAL LLM key and runs locally only
// (go test -tags=e2e -run TestCanaryLiveLLM ./cmd/ares/). Deliberately NOT
// part of the default suite or CI, so CI stays hermetic and no money burns
// by accident. Cost is a handful of flash-model calls with tiny prompts.
//
// Config: base ./ares.yaml, LLM block overridden by configs/ares.local.yaml
// (the live working credentials; both files are gitignored — secrets never
// touch the repo). Tools are echo-only (zero side effects): the LLM genuinely
// decides, the scheduler genuinely executes, nothing real happens.
//
// NOTE: this is intentionally NOT CompareDualPath. That harness runs the DAG
// arm without executing tool tasks (scripted clients answer on schedule, so
// holes never matter). A live model plans from history — unreadable
// predecessors would degenerate every later round — so the live canary runs
// the full stack with the scheduler executing each grown node.
package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/ares_bootstrap"
	"github.com/Timwood0x10/ares/internal/ares_config"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/llm"
	"github.com/Timwood0x10/ares/internal/planprojection"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// liveLLMClient loads the real model: base config from ./ares.yaml with the
// LLM block overridden by configs/ares.local.yaml (live credentials), then
// the production ProvideLLM constructor — the same client serve wires in.
// It finishes with one cheap preflight ping so a dead key fails fast instead
// of burning a confusing full run.
func liveLLMClient(t *testing.T) *llm.Client {
	t.Helper()

	cfgPath := findRootConfig(t)
	if cfgPath == "" {
		t.Fatal("live canary needs ./ares.yaml (run from the repo so the config resolves)")
	}
	raw, err := os.ReadFile(cfgPath)
	require.NoError(t, err, "read base config")
	var cfg ares_config.Config
	require.NoError(t, yaml.Unmarshal(raw, &cfg), "parse base config")

	localPath := ""
	for _, p := range []string{"configs/ares.local.yaml", "../configs/ares.local.yaml", "../../configs/ares.local.yaml"} {
		if _, err := os.Stat(p); err == nil {
			localPath = p
			break
		}
	}
	require.NotEmpty(t, localPath, "live canary needs configs/ares.local.yaml (live credentials)")
	localRaw, err := os.ReadFile(localPath)
	require.NoError(t, err, "read local llm override")
	var local struct {
		LLM ares_config.LLMConfig `yaml:"llm"`
	}
	require.NoError(t, yaml.Unmarshal(localRaw, &local), "parse local llm override")
	if local.LLM.Model != "" || local.LLM.APIKey != "" {
		cfg.LLM = local.LLM
	}

	comp, err := ares_bootstrap.ProvideLLM(cfg.LLM)
	require.NoError(t, err, "build production LLM client")
	client, ok := comp.Client.(*llm.Client)
	require.True(t, ok, "ProvideLLM must yield *llm.Client, got %T", comp.Client)

	// Preflight: one tiny call so a dead key/endpoint/model fails here,
	// loudly. A real user message: the client rejects empty message lists
	// before touching the network.
	pingCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ping := []*core.LLMMessage{{Role: "user", Content: "Reply with exactly: OK"}}
	if _, err := client.Chat(pingCtx, ping, nil, nil); err != nil {
		t.Fatalf("live LLM preflight failed (key/endpoint/model): %v", err)
	}
	return client
}

// TestCanaryLiveLLM runs one pinned prompt through both execution bodies
// against the real model and asserts tool-sequence parity. The prompt pins a
// single grep call so the verdict is non-vacuous: a model that answers
// without tools on both arms would "match" on empty sequences, and the len
// guard below turns that into a failure (prompt needs work), not a pass.
func TestCanaryLiveLLM(t *testing.T) {
	client := liveLLMClient(t)
	const prompt = "Use the grep tool once with query 'hello' to search, then answer with the tool result in one sentence."
	const sessionID = "canary-live-1"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	start := time.Now()

	// ── Legacy arm: chat loop to Done, echo tools really execute. ──
	legacyBinder := &canaryBinder{}
	legacyCog, err := agentfabric.NewChatCognition(agentfabric.ChatCognitionDeps{
		ChatClient:     client,
		ToolBinder:     legacyBinder,
		PromptTemplate: "{{.input}}",
		MaxToolRounds:  5,
		AgentID:        "live-legacy",
	})
	require.NoError(t, err)
	legacyTask := models.NewTask("live-legacy-task", "worker", nil)
	legacyTask.Payload = map[string]any{"task_desc": prompt}
	legacyDone := false
	for round := 0; round < 5; round++ {
		out, err := legacyCog.ExecuteStep(ctx, legacyTask)
		require.NoError(t, err, "legacy arm must not error")
		if out.Done {
			legacyDone = true
			break
		}
		if out.Checkpoint != nil {
			legacyTask.Payload["checkpoint"] = out.Checkpoint
		}
	}
	require.True(t, legacyDone, "legacy arm must terminate")
	legacyBinder.mu.Lock()
	var legacySeq []string
	for _, c := range legacyBinder.calls {
		legacySeq = append(legacySeq, c.tool)
	}
	legacyBinder.mu.Unlock()

	// ── L2 arm: full stack, scheduler executes each grown node. ──
	fabric := taskfabric.NewFabric()
	coord := planprojection.NewCompileCoordinator(fabric, nil)
	reg := agentfabric.NewSessionRegistry()
	liveBinder := &canaryBinder{}
	planner, err := agentfabric.NewPlannerCognition(agentfabric.PlannerDeps{
		ChatClient: client,
		ToolBinder: liveBinder,
		Sessions:   reg,
		Fabric:     fabric,
		Logger:     nil,
	})
	require.NoError(t, err)

	agents := agentfabric.NewFabric()
	_, err = agents.Spawn(ctx, agentfabric.SpawnSpec{
		Identity:     "live-agent",
		Capabilities: []string{"ares/root", "ares/plan", "ares/answer", "tool/grep", "tool/read", "tool/echo"},
		CognitionFactory: func([]string) agentfabric.Cognition {
			return agentfabric.NewRouterCognitionWithPlanner(liveBinder, planner, reg, nil)
		},
	})
	require.NoError(t, err)

	sched := NewKernelScheduler(fabric, map[string]CapabilityExecutor{}, newLoadTracker())
	sched.WithAgentFabric(agents)
	sched.PollInterval = 50 * time.Millisecond
	go sched.Run(ctx)

	g, err := reg.InitSession(ctx, sessionID, prompt, nil,
		func(subCtx context.Context, dag *engine.MutableDAG) (stop func()) {
			return coord.SubscribeGraphEvents(subCtx, dag)
		})
	require.NoError(t, err)
	canaryAdmitRoot(t, ctx, fabric, g)
	require.NoError(t, fabric.Create(&taskfabric.Task{
		ID:         agentfabric.SessionNodeID(sessionID, 0, "plan", 0),
		Capability: "ares/plan",
		Checkpoint: &taskfabric.CheckpointEnvelope{
			SessionID: sessionID,
			Payload:   map[string]any{"input": prompt},
		},
	}))

	answerID := waitForCanaryAnswer(t, fabric, sessionID, 4*time.Minute)
	answerBody := canaryAnswerContent(t, fabric, answerID)
	require.NotEmpty(t, answerBody, "terminal answer must carry model-produced content")
	elapsed := time.Since(start)

	// ── Verdict. ──
	steps := g.DAG().StepIndex()
	order, err := g.DAG().GetExecutionOrder()
	require.NoError(t, err)
	var dagSeq []string
	for _, id := range order {
		if s, ok := steps[id]; ok {
			if name, ok := strings.CutPrefix(string(s.AgentType), "tool/"); ok {
				dagSeq = append(dagSeq, name)
			}
		}
	}
	t.Logf("live canary: legacy_seq=%q dag_seq=%q elapsed=%v", legacySeq, dagSeq, elapsed.Round(time.Second))

	require.NotEmpty(t, legacySeq,
		"vacuous run: model used no tools on the legacy arm, prompt needs work")
	require.Equal(t, legacySeq, dagSeq,
		"live model chose different tools per body (run again to rule out sampling noise)")
	if planner, ok := planner.(interface{ ForcedAnswers() uint64 }); ok {
		require.Equal(t, uint64(0), planner.ForcedAnswers(), "live session must not hit the depth guard")
	}
}

// canaryAnswerContent reads the terminal answer body from its envelope.
// The live model's exact wording is unpredictable, so callers assert
// non-emptiness (the session produced an answer), not equality.
func canaryAnswerContent(t *testing.T, f *taskfabric.Fabric, id string) string {
	t.Helper()
	tk, err := f.Task(id)
	require.NoError(t, err)
	dc, err := taskfabric.DecodeCheckpoint(tk.Checkpoint)
	require.NoError(t, err)
	sc, ok := dc.StepCheckpoint.(map[string]any)
	require.True(t, ok, "answer checkpoint carries a {result,items,...} map")
	raw, ok := sc["items"]
	require.True(t, ok, "envelope carries items")
	items, ok := raw.([]*models.RecommendItem)
	require.True(t, ok, "items decode as recommend items, got %T", raw)
	require.Len(t, items, 1)
	return items[0].Content
}
