package agentfabric

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
)

// DualPathInput is one shadow comparison between the legacy ReAct execution
// body (chatCognition) and the L2 session-graph body (plannerCognition) for
// the same request (M4-B1).
//
// Both arms decide from the SAME script: NewChat is called twice to produce
// one fresh client per arm, so per-arm call counts stay attributable while
// the decision logic is identical. NewBinder is likewise called twice: each
// arm gets a binder that ADVERTISES tool schemas (so both arms genuinely
// consider tools) but DENIES every call (so the comparison has zero side
// effects — nothing is written, nothing is sent).
type DualPathInput struct {
	// Prompt is the user request both arms decide on.
	Prompt string
	// SessionID scopes the L2 graph the planner arm grows. Must be unique
	// per comparison; node IDs embed it.
	SessionID string
	// NewChat builds one ChatClient per arm (called exactly twice).
	NewChat func() ChatClient
	// NewBinder builds one recording deny binder per arm (called exactly
	// twice). Denied calls are recorded, never delegated.
	NewBinder func() ToolBinder
	// MaxRounds bounds each arm (0 = default cap). The arms run under the
	// same cap so the paired comparison stays fair.
	MaxRounds int
	// Archive persists mismatch samples for triage (nil = verdict only).
	Archive MismatchArchive
	// Logger receives warn-level mismatch reports (nil = slog.Default()).
	Logger *slog.Logger
}

// DualPathVerdict is the outcome of one shadow comparison.
type DualPathVerdict struct {
	// Match is true when both arms chose the same tool sequence.
	Match bool
	// LegacySeq is the tool-call sequence the chat arm attempted, in order.
	LegacySeq []string
	// DAGSeq is the tool-node sequence the planner arm grew, in order.
	DAGSeq []string
	// LegacyLLMCalls and DAGLLMCalls count Chat calls, one per arm-round.
	LegacyLLMCalls int
	DAGLLMCalls    int
	// Mismatches carries every archived sample (empty when Match).
	Mismatches []MismatchSample
}

// MismatchSample is one triage-ready record of a behavioral divergence
// between the two execution bodies (M4-B1: inconsistent samples are archived,
// never dropped).
type MismatchSample struct {
	// SessionID identifies the comparison that produced the sample.
	SessionID string
	// Prompt is the request both arms decided on.
	Prompt string
	// LegacySeq and DAGSeq are the two disagreeing tool sequences.
	LegacySeq []string
	DAGSeq    []string
	// Reason names the divergence class ("tool-sequence" today).
	Reason string
}

// MismatchArchive persists mismatch samples for offline triage.
type MismatchArchive interface {
	// Archive stores one mismatch sample. It must be safe for concurrent
	// use; the comparator calls it at most once per comparison.
	Archive(MismatchSample)
}

// defaultShadowRounds caps one arm when the input leaves MaxRounds unset.
const defaultShadowRounds = 5

// CompareDualPath runs the same request through the legacy ReAct body and
// the L2 graph body and compares the tool sequences they chose (M4-B1).
//
// Fairness: both arms get a fresh client from the same factory, both run
// under the same round cap, and neither arm executes a tool — the chat arm's
// attempts are denied-and-recorded by its binder, the planner arm grows
// nodes instead of calling. A denied tool surfaces as an "error: ..."
// observation on the chat arm, so its loop still advances to the scripted
// answer exactly as it would on a failing tool in production.
//
// A mismatch is archived (returned in the verdict AND forwarded to Archive)
// with both sequences attached — never logged-and-dropped.
//
// Boundary: the DAG arm grows nodes but never executes tool tasks, so
// predecessors stay unreadable and scripted clients (which answer on
// schedule) are the intended input. A LIVE model plans from history —
// holes would degenerate every later round — so live parity runs the full
// stack with the scheduler executing each grown node instead (see the
// canary_live_test.go e2e test, not this harness).
func CompareDualPath(ctx context.Context, in DualPathInput) (DualPathVerdict, error) {
	if in.NewChat == nil {
		return DualPathVerdict{}, fmt.Errorf("agentfabric: shadow compare requires NewChat")
	}
	if in.NewBinder == nil {
		return DualPathVerdict{}, fmt.Errorf("agentfabric: shadow compare requires NewBinder")
	}
	if strings.TrimSpace(in.SessionID) == "" {
		return DualPathVerdict{}, fmt.Errorf("agentfabric: shadow compare requires SessionID")
	}
	rounds := in.MaxRounds
	if rounds <= 0 {
		rounds = defaultShadowRounds
	}
	logger := in.Logger
	if logger == nil {
		logger = slog.Default()
	}

	legacySeq, legacyCalls, err := runLegacyArm(ctx, in, rounds)
	if err != nil {
		return DualPathVerdict{}, fmt.Errorf("agentfabric: shadow compare legacy arm: %w", err)
	}
	dagSeq, dagCalls, err := runDAGArm(ctx, in, rounds)
	if err != nil {
		return DualPathVerdict{}, fmt.Errorf("agentfabric: shadow compare dag arm: %w", err)
	}

	verdict := DualPathVerdict{
		Match:          equalSequences(legacySeq, dagSeq),
		LegacySeq:      legacySeq,
		DAGSeq:         dagSeq,
		LegacyLLMCalls: legacyCalls,
		DAGLLMCalls:    dagCalls,
	}
	if !verdict.Match {
		sample := MismatchSample{
			SessionID: in.SessionID,
			Prompt:    in.Prompt,
			LegacySeq: legacySeq,
			DAGSeq:    dagSeq,
			Reason:    "tool-sequence",
		}
		verdict.Mismatches = []MismatchSample{sample}
		if in.Archive != nil {
			in.Archive.Archive(sample)
		}
		logger.Warn("agentfabric: dual-path tool sequence diverged",
			"session", in.SessionID,
			"legacy_seq", strings.Join(legacySeq, ","),
			"dag_seq", strings.Join(dagSeq, ","))
	}
	return verdict, nil
}

// runLegacyArm drives the chat tool-loop to Done (bounded by rounds),
// resuming through the payload checkpoint exactly like the scheduler's
// buildQuantumStep resume path. Attempted tool names are recorded by the
// arm's binder in call order.
func runLegacyArm(ctx context.Context, in DualPathInput, rounds int) ([]string, int, error) {
	binder := in.NewBinder()
	chat := in.NewChat()
	cog, err := NewChatCognition(ChatCognitionDeps{
		ChatClient:     chat,
		ToolBinder:     binder,
		PromptTemplate: "{{.input}}",
		MaxToolRounds:  rounds,
		AgentID:        "shadow-legacy-" + in.SessionID,
	})
	if err != nil {
		return nil, 0, err
	}
	task := models.NewTask("shadow-chat-"+in.SessionID, models.AgentType("shadow"), nil)
	task.Payload = map[string]any{"task_desc": in.Prompt}
	for round := 0; round < rounds; round++ {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		out, err := cog.ExecuteStep(ctx, task)
		if err != nil {
			return nil, 0, err
		}
		if out.Done {
			break
		}
		if out.Checkpoint != nil {
			if task.Payload == nil {
				task.Payload = make(map[string]any)
			}
			task.Payload["checkpoint"] = out.Checkpoint
		}
	}
	seq, calls := binderSequence(binder, chat)
	return seq, calls, nil
}

// runDAGArm grows the L2 session graph to the answer node (bounded by
// rounds) and reads the grown tool-node names back in topological order —
// the DAG arm's chosen tool sequence. Tool tasks are never executed: the
// comparison is about what each arm CHOSE, and the scripted client answers
// from the grown structure regardless of tool outputs.
func runDAGArm(ctx context.Context, in DualPathInput, rounds int) ([]string, int, error) {
	fabric := taskfabric.NewFabric()
	reg := NewSessionRegistry()
	g, err := reg.InitSession(ctx, in.SessionID, in.Prompt, nil, nil)
	if err != nil {
		return nil, 0, err
	}
	chat := in.NewChat()
	planner, err := NewPlannerCognition(PlannerDeps{
		ChatClient: chat,
		ToolBinder: in.NewBinder(),
		Sessions:   reg,
		Fabric:     fabric,
		Logger:     in.Logger,
	})
	if err != nil {
		return nil, 0, err
	}
	planID := SessionNodeID(in.SessionID, 0, "plan", 0)
	for round := 0; round < rounds; round++ {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		planTask := models.NewTask(planID, models.AgentType(planAgentType), nil)
		planTask.SessionID = in.SessionID
		planTask.Payload = map[string]any{"input": in.Prompt, planMetadataKey: in.SessionID}
		out, err := planner.ExecuteStep(ctx, planTask)
		if err != nil {
			return nil, 0, err
		}
		if !out.Done {
			return nil, 0, fmt.Errorf("planner quantum did not complete")
		}
		if hasAnswerNode(g) {
			break
		}
		// The next quantum resumes from the plan node this round grew.
		planID = newestPlanNode(g, planID)
	}
	return dagToolSequence(g), chatCalls(chat), nil
}

// hasAnswerNode reports whether the graph already holds a terminal node.
func hasAnswerNode(g *L2Graph) bool {
	for _, s := range g.DAG().StepIndex() {
		if s.AgentType == answerAgentType {
			return true
		}
	}
	return false
}

// newestPlanNode finds the plan node grown after prev: the plan node whose
// predecessor chain passes through prev but which is not prev itself. Falls
// back to prev when the round grew no plan node (all tools skipped → the
// caller forces an answer node, which hasAnswerNode already caught).
func newestPlanNode(g *L2Graph, prev string) string {
	for id, s := range g.DAG().StepIndex() {
		if id == prev || s.AgentType != planAgentType {
			continue
		}
		for p := g.Predecessor(id); p != ""; p = g.Predecessor(p) {
			if p == prev {
				return id
			}
		}
	}
	return prev
}

// dagToolSequence reads tool-node names in topological order — the growth
// order, since tools grown in one round are chained sequentially.
func dagToolSequence(g *L2Graph) []string {
	order, err := g.DAG().GetExecutionOrder()
	if err != nil {
		return nil
	}
	steps := g.DAG().StepIndex()
	var seq []string
	for _, id := range order {
		if s, ok := steps[id]; ok {
			if name, ok := strings.CutPrefix(s.AgentType, "tool/"); ok {
				seq = append(seq, name)
			}
		}
	}
	return seq
}

// equalSequences reports whether two tool sequences chose the same tools in
// the same order. Nil and empty both mean "no tools chosen".
func equalSequences(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// binderSequence extracts the recorded tool attempts from a recording deny
// binder, plus the arm's LLM call count. A binder that does not record
// contributes no sequence — the comparison then rests on the DAG side alone,
// which is always recorded from the graph.
func binderSequence(binder ToolBinder, chat ChatClient) ([]string, int) {
	var seq []string
	if r, ok := binder.(sequenceRecorder); ok {
		seq = r.recordedSequence()
	}
	return seq, chatCalls(chat)
}

// sequenceRecorder is the read side of a recording deny binder.
type sequenceRecorder interface {
	recordedSequence() []string
}

// chatCalls reads the call count from a counting chat client. A client that
// does not count contributes zero — counts are diagnostic, the verdict rests
// on the sequences.
func chatCalls(chat ChatClient) int {
	type callCounter interface{ callCount() int }
	if c, ok := chat.(callCounter); ok {
		return c.callCount()
	}
	return 0
}

// recordDenyBinder is the arm binder for shadow comparison: it ADVERTISES
// the given tool schemas (both arms must genuinely consider tools) but
// DENIES every call with a sentinel error (zero side effects — nothing is
// written, nothing is sent). Attempts are recorded in call order: that record
// IS the legacy arm's chosen tool sequence.
type recordDenyBinder struct {
	schemas  []resources.ToolSchema
	attempts []string
}

// errShadowDenied marks a tool call refused by the shadow comparison arm.
var errShadowDenied = fmt.Errorf("agentfabric: shadow comparison denies tool calls")

// newRecordDenyBinder builds an arm binder over the given schemas.
func newRecordDenyBinder(schemas []resources.ToolSchema) *recordDenyBinder {
	return &recordDenyBinder{schemas: schemas}
}

// CallTool implements ToolBinder. It records the attempt and denies it —
// never delegates, so the production tool surface sees zero calls.
func (b *recordDenyBinder) CallTool(_ context.Context, name string, _ map[string]any) (any, error) {
	b.attempts = append(b.attempts, name)
	return nil, errShadowDenied
}

// ListTools implements ToolBinder.
func (b *recordDenyBinder) ListTools() []string {
	names := make([]string, 0, len(b.schemas))
	for _, s := range b.schemas {
		names = append(names, s.Name)
	}
	return names
}

// IsToolIdempotent implements ToolBinder. No tool is callable, so none is
// idempotent.
func (b *recordDenyBinder) IsToolIdempotent(string) bool { return false }

// GetToolSchemas implements ToolBinder.
func (b *recordDenyBinder) GetToolSchemas() []resources.ToolSchema { return b.schemas }

// recordedSequence implements sequenceRecorder.
func (b *recordDenyBinder) recordedSequence() []string {
	return append([]string(nil), b.attempts...)
}
