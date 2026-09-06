// Package ares_bootstrap — New evolution system provider (Genome + Diff + Coordinator).
package ares_bootstrap

//nolint: errcheck // best-effort operations: ResponseWriter writes, cleanup Close/Wait, deferred shutdown
import (
	"context"
	"errors"
	"fmt"

	apiembedding "github.com/Timwood0x10/ares/api/embedding"
	evolution "github.com/Timwood0x10/ares/internal/ares_evolution"
	aresmemory "github.com/Timwood0x10/ares/internal/ares_memory"
	"github.com/Timwood0x10/ares/internal/evidence"
	evoparent "github.com/Timwood0x10/ares/internal/evolution"
	"github.com/Timwood0x10/ares/internal/evolution/coordinator"
	"github.com/Timwood0x10/ares/internal/evolution/diff"
	"github.com/Timwood0x10/ares/internal/evolution/genome"
	"github.com/Timwood0x10/ares/internal/evolution/patch"
	"github.com/Timwood0x10/ares/internal/knowledge"
	"github.com/Timwood0x10/ares/internal/knowledge/pipeline"
	"github.com/Timwood0x10/ares/internal/knowledge/planner"
	"github.com/Timwood0x10/ares/internal/knowledge/provider"
	provider_code "github.com/Timwood0x10/ares/internal/knowledge/provider/code"
	provider_memory "github.com/Timwood0x10/ares/internal/knowledge/provider/memory"
	storeprovider "github.com/Timwood0x10/ares/internal/knowledge/provider/store"
	"github.com/Timwood0x10/ares/internal/knowledge/provider/vector"
	knowledgeruntime "github.com/Timwood0x10/ares/internal/knowledge/runtime"
	"github.com/Timwood0x10/ares/internal/storage"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
	wfgraph "github.com/Timwood0x10/ares/internal/workflow/graph"
)

// NewEvolutionComponents holds the new evolution system components.
type NewEvolutionComponents struct {
	EvidenceStore evidence.Store
	GenomeReg     *genome.Registry
	DiffReg       *diff.Registry
	PatchReg      *patch.Registry
	Coordinator   *coordinator.EvolutionCoordinator
	// LLMAdapter parses natural-language LLM suggestions into PatchProposals
	// that the Coordinator can evaluate alongside GA/Chaos/AKF/Human sources.
	// Wired into the Coordinator's suggestion pipeline in wireGAEvolution when
	// an LLM client is available (LLM → Parse → PatchProposal → Coordinate.Evaluate).
	LLMAdapter *evoparent.LLMAdapter
	// StrategyStore persists the best-evolved strategy deployed by the GA
	// engine so the live agent can consume it at runtime. Set by the
	// bootstrap bridge after the store is created.
	StrategyStore evolution.StrategyStore

	// GAGenerationActive reports whether a GA generation is currently in
	// flight (REVIEW #12 Phase 2: the live-chaos GA quiet-window probe). Nil when
	// no wired evolution system exists.
	GAGenerationActive func() bool

	// Lifecycle is the strategy lifecycle orchestrator (P2-2). When set,
	// serve wires it into the introspect control plane so
	// /api/evolution/lifecycle returns a state snapshot.
	Lifecycle *evolution.StrategyLifecycle

	// ActiveStrategyManager is the ASM the Lifecycle wraps (sole Deploy/
	// Rollback/RecordScore caller). Exposed for the §8 closure assertions:
	// Previous() / RollbackPolicy() are the acceptance surfaces for the
	// promote→rollback loop. Nil when no strategy store was wired.
	ActiveStrategyManager *evolution.ActiveStrategyManager

	// ShadowEvaluator is the G2 gate's data source. Exposed so the closure
	// tests (and a future task-level sampler) can feed shadow comparisons —
	// DreamCycle was the only feeder, and it is disabled in production.
	// Nil when shadow evaluation is disabled.
	ShadowEvaluator *evolution.ShadowEvaluator

	// ChannelFeedback records the two perception channels that were previously
	// invisible to evolution (closure plan Step Y.2/Y.3): cross-agent
	// collaboration receipts and tool-call outcomes. The wiring layer attaches
	// it to the IPC bus (agentipc.CollaborationObserver) and wraps the tool
	// binder with it (sub.ToolCallObserver). Nil when
	// evolution.channel_feedback arms neither channel — the default.
	ChannelFeedback *evolution.ChannelFeedbackRecorder

	// liveDAG holds the agent's live workflow DAG injected after bootstrap
	// so the evolution system's executors operate on real runtime state
	// instead of synthetic placeholders. Set via UpdateLiveDAG after agents
	// are created and their DAGs are registered with the runtime manager.
	liveDAG *engine.MutableDAG

	// toolClassDAG is the L1 capability graph (M5): one node per ToolClass
	// (toolName#argShape) with enabled/budget/prior metadata. Evolution
	// structure patches (SetNodeMetadata on L1) constrain L2 growth: the
	// plannerCognition reads enabled/budget before growing tool nodes.
	// Unlike liveDAG, this is NOT compiled into taskfabric — it is a
	// capability catalog, not an execution plan. Nil when no tools are
	// registered (L1 constraints default to permissive).
	toolClassDAG *engine.MutableDAG

	// graphExec is the GraphPatchExecutor created at bootstrap time.
	// UpdateLiveDAG calls SetGraph on it to swap in the live workflow graph,
	// since Register cannot overwrite an already-registered component key
	// (a naive re-register is a guaranteed-failure no-op).
	graphExec *wfgraph.GraphPatchExecutor

	// recoveryExec is the RecoveryPatchExecutor created at bootstrap time.
	// UpdateLiveDAG calls SetDAG on it to replace the fake DAG with the
	// live one, since Register cannot overwrite an already-registered key.
	recoveryExec *engine.RecoveryPatchExecutor

	// dagExec is the engine.DAGPatchExecutor that applies workflow structure
	// patches (insert/remove/replace node, add/remove edge) directly to the
	// live *MutableDAG. UpdateLiveDAG binds it to the live DAG and installs it
	// as the patch registry's fallback, so a structure patch whose target is a
	// dynamic node ID no longer dies on "no executor registered" — it reaches
	// the real runtime topology. The pointer stays put; SetDAG swaps the DAG it
	// operates on.
	dagExec *engine.DAGPatchExecutor

	// knowledgeExec is the KnowledgePatchExecutor created at bootstrap time.
	// UpdateLiveKnowledgeRuntime calls SetRuntime on it to swap in the agent's
	// live KnowledgeRuntime, since Register cannot overwrite an already
	// registered component key.
	knowledgeExec *knowledgeruntime.KnowledgePatchExecutor
}

// ProvideNewEvolution wires the new evolution system:
// Evidence Store → Genome Registry → Diff Registry → Patch Registry → Coordinator.
//
// Args:
//
//	dag - optional MutableDAG for WorkflowGenome and executors (may be nil).
//	rt  - optional KnowledgeRuntime for KnowledgePatchExecutor (may be nil).
//	memoryStore - optional MemoryConfigStore for MemoryPatchExecutor (may be nil).
//	evStore - optional persistent evidence store (may be nil). When nil, an
//	in-memory evidence store is used (default, dev/offline semantics).
//
// When dag, rt, or memoryStore is nil, their corresponding executors are skipped.
func ProvideNewEvolution(dag *engine.MutableDAG, rt *knowledgeruntime.KnowledgeRuntime, memoryStore aresmemory.MemoryConfigStore, evStore evidence.Store) (*NewEvolutionComponents, error) {
	// 1. Evidence Store — central logging for all runtime evidence.
	// T1 (evidence persistence): an explicit non-nil store (e.g. Postgres)
	// survives restarts; nil falls back to the in-memory store.
	if evStore == nil {
		evStore = evidence.NewMemoryStore()
	}

	// 2. Genome Registry — register all available genomes.
	genomeReg := genome.NewRegistry()
	if dag != nil {
		wfGenome := genome.NewWorkflowGenome(dag, genome.WorkflowGenomeConfig{
			MaxNodes:      20,
			InsertionRate: 0.3,
			PruneRate:     0.2,
			EvidenceStore: evStore,
		})
		if err := genomeReg.Register(wfGenome); err != nil {
			return nil, fmt.Errorf("register workflow genome: %w", err)
		}

		// TODO(tech-debt): the scheduler genome dimension was retired
		// (fusion plan §B1, 2026-08-22): sdk.Graph runs fully-parallel ready
		// batches, so ordering schedulers have no execution decision left.
		// Legacy PatchChangeScheduler appliers remain for persisted patches.
		// TODO(evolution-dim): candidate successor dimension — a concurrency
		// genome evolving sdk.Graph.MaxRoundConcurrency (the one scheduling
		// semantic that survived the retirement). Not scheduled for 0.3.x.

		recoveryGenome := genome.NewRecoveryGenome(
			&engine.RecoveryPolicy{Strategy: engine.RecoveryRetry, MaxAttempts: 3},
			genome.DefaultRecoveryGenomeConfig(),
		)
		if err := genomeReg.Register(recoveryGenome); err != nil {
			return nil, fmt.Errorf("register recovery genome: %w", err)
		}
	}

	// Always register the knowledge genome (it works with or without a runtime).
	knowledgeGenome := genome.NewKnowledgeGenome(nil, genome.KnowledgeGenomeConfig{
		MaxResults:      100,
		ReducerStrategy: "default",
		PlannerStrategy: "balanced",
		EvidenceStore:   evStore,
	})
	if err := genomeReg.Register(knowledgeGenome); err != nil {
		return nil, fmt.Errorf("register knowledge genome: %w", err)
	}

	// Memory genome — evolves memory management parameters.
	memoryGenome := genome.NewMemoryGenome(genome.MemoryGenomeConfig{
		MaxHistory:            10,
		MaxSessions:           100,
		MaxDistilledTasks:     5000,
		UseStructuredCleaning: false,
		EvidenceStore:         evStore,
	})
	if err := genomeReg.Register(memoryGenome); err != nil {
		return nil, fmt.Errorf("register memory genome: %w", err)
	}

	// 3. Diff Registry — register all differs.
	diffReg := diff.NewRegistry()
	for _, d := range []diff.Differ{
		diff.NewWorkflowDiffer(),
		diff.NewKnowledgeDiffer(),
		diff.NewRecoveryDiffer(),
		diff.NewMemoryDiffer(),
	} {
		if err := diffReg.Register(d); err != nil {
			return nil, fmt.Errorf("register differ %s: %w", d.Name(), err)
		}
	}

	// 4. Patch Registry — register all executors.
	patchReg := patch.NewRegistry()

	// Track the graph and recovery executors so UpdateLiveDAG can replace
	// their references in place later (Register cannot overwrite
	// already-registered keys).
	var graphExec *wfgraph.GraphPatchExecutor
	var recoveryExec *engine.RecoveryPatchExecutor

	if dag != nil {
		// Graph executor — for workflow and scheduler patches.
		g, gErr := wfgraph.NewGraph("evolution-workflow")
		if gErr != nil {
			return nil, fmt.Errorf("create evolution graph: %w", gErr)
		}
		for _, step := range dag.Steps() {
			fn, fErr := wfgraph.NewFuncNode(step.ID, func(_ context.Context, _ *wfgraph.State) error { return nil })
			if fErr != nil {
				return nil, fmt.Errorf("create func node %s: %w", step.ID, fErr)
			}
			if _, nErr := g.Node(step.ID, fn); nErr != nil {
				return nil, fmt.Errorf("add node %s: %w", step.ID, nErr)
			}
		}
		for _, step := range dag.Steps() {
			for _, dep := range step.DependsOn {
				if _, eErr := g.Edge(dep, step.ID); eErr != nil {
					return nil, fmt.Errorf("add edge %s→%s: %w", dep, step.ID, eErr)
				}
			}
		}
		if len(dag.Steps()) > 0 {
			if _, sErr := g.Start(dag.Steps()[0].ID); sErr != nil {
				return nil, fmt.Errorf("set start node: %w", sErr)
			}
		}

		graphExec = wfgraph.NewGraphPatchExecutor(g)
		_ = patchReg.RegisterComponent(graphExec)

		// Recovery executor.
		recoveryExec = engine.NewRecoveryPatchExecutor(dag)
		_ = patchReg.RegisterComponent(recoveryExec)
		_ = patchReg.Register("recovery.max_attempts", recoveryExec)
		_ = patchReg.Register("recovery.replacement_agent", recoveryExec)
		_ = patchReg.Register("recovery.max_retries", recoveryExec)
		// recovery.strategy is the target the RecoveryDiffer (and the LLM
		// adapter) emit for PatchChangeRecoveryStrategy. Without it every
		// recovery-strategy patch failed apply with "no executor registered".
		_ = patchReg.Register("recovery.strategy", recoveryExec)
	}

	// Knowledge executor — works with or without a real runtime.
	var knowledgeExec patch.RuntimeComponent
	var knowledgeExecTyped *knowledgeruntime.KnowledgePatchExecutor
	if rt != nil {
		// Wire the KnowledgeRuntime to the PatchRegistry and EvidenceStore
		// so that runtime patches can dynamically update knowledge config and
		// evidence emitted during AKG execution is recorded centrally.
		rt.WithPatchRegistry(patchReg).WithEvidenceStore(evStore)
		ke := knowledgeruntime.NewKnowledgePatchExecutor(rt)
		knowledgeExec = ke
		knowledgeExecTyped = ke
	} else {
		// No runtime available — use a no-op executor for knowledge patches.
		knowledgeExec = &noopKnowledgeExecutor{}
	}
	_ = patchReg.RegisterComponent(knowledgeExec)
	_ = patchReg.Register("knowledge.planner.max_results", knowledgeExec)
	_ = patchReg.Register("knowledge.planner.reducer", knowledgeExec)
	_ = patchReg.Register("knowledge.planner.strategy", knowledgeExec)
	_ = patchReg.Register("knowledge.planner.summarizer", knowledgeExec)

	// Memory executor — wraps a MemoryConfigStore as a RuntimeComponent.
	// Accepts patches for memory configuration (history depth, TTL, task limits).
	// When memoryStore is nil, the executor is skipped.
	if memoryStore != nil {
		memoryExec := aresmemory.NewMemoryPatchExecutor(memoryStore)
		_ = patchReg.RegisterComponent(memoryExec)
		_ = patchReg.Register("memory.config.max_history", memoryExec)
		_ = patchReg.Register("memory.config.max_tasks", memoryExec)
		_ = patchReg.Register("memory.config.max_distilled_tasks", memoryExec)
		_ = patchReg.Register("memory.config.session_ttl", memoryExec)
	}

	// 5. Coordinator — decision engine for all patches.
	coord := coordinator.NewEvolutionCoordinator(coordinator.DefaultPolicy(), patchReg)

	return &NewEvolutionComponents{
		EvidenceStore: evStore,
		GenomeReg:     genomeReg,
		DiffReg:       diffReg,
		PatchReg:      patchReg,
		Coordinator:   coord,
		LLMAdapter:    evoparent.NewLLMAdapter(),
		graphExec:     graphExec,
		recoveryExec:  recoveryExec,
		knowledgeExec: knowledgeExecTyped,
	}, nil
}

// UpdateLiveKnowledgeRuntime replaces the evolution system's isolated
// KnowledgeRuntime with the agent's live KnowledgeRuntime, so knowledge
// genome patches (ChangeBudget/ChangePlanner/ChangeReducer) are applied
// to the actual runtime used by the agent's knowledge tools.
//
// It swaps the runtime into the existing KnowledgePatchExecutor in place via
// SetRuntime. This is correct where re-registering would silently fail:
// patch.Registry.Register cannot overwrite an already-registered component
// key, so a naive RegisterComponent swap would be a no-op and knowledge
// patches would keep hitting the bootstrap (placeholder) runtime.
func (c *NewEvolutionComponents) UpdateLiveKnowledgeRuntime(rt *knowledgeruntime.KnowledgeRuntime) {
	if rt == nil {
		log.Warn("new evolution: UpdateLiveKnowledgeRuntime called with nil, keeping existing")
		return
	}
	// Wire the live runtime to the patch registry and evidence store so that
	// patches it proposes are recorded centrally.
	rt.WithPatchRegistry(c.PatchReg).WithEvidenceStore(c.EvidenceStore)

	// Common path: bootstrap created a real (typed) KnowledgePatchExecutor.
	// Swap the live runtime in place — no re-registration needed.
	if c.knowledgeExec != nil {
		c.knowledgeExec.SetRuntime(rt)
		log.Info("new evolution: live KnowledgeRuntime injected into executor")
		return
	}

	// Fallback path: bootstrap built a no-op executor because rt was nil at
	// bootstrap time. Replace the registrations so the live runtime takes
	// effect. Replace overwrites the existing keys instead of failing silently.
	liveExec := knowledgeruntime.NewKnowledgePatchExecutor(rt)
	if err := c.PatchReg.ReplaceComponent(liveExec); err != nil {
		log.Warn("new evolution: replace knowledge component failed", "error", err)
		return
	}
	for _, key := range []string{
		"knowledge.planner.max_results",
		"knowledge.planner.reducer",
		"knowledge.planner.strategy",
		"knowledge.planner.summarizer",
	} {
		if err := c.PatchReg.Replace(key, liveExec); err != nil {
			log.Warn("new evolution: replace knowledge key failed", "key", key, "error", err)
		}
	}
	log.Info("new evolution: live KnowledgeRuntime injected into executors (replaced no-op)")
}

// ── noopKnowledgeExecutor ─────────────────────

// UpdateLiveDAG injects a live agent workflow DAG into the evolution system's
// executors after bootstrap, replacing the synthetic placeholder DAG. This
// ensures that workflow/scheduler/recovery patches generated by the genome
// evolution system are applied to the real runtime DAG instead of synthetic
// executors. Must be called after agents are created and their DAGs are
// registered with the runtime manager.
//
// The DAG is used to rebuild the graph executor and recovery executor in the
// patch registry, and to repoint the WorkflowGenome at the live topology so
// evolution reasons over the real agent DAG instead of the bootstrap
// placeholder (previously the genome kept evolving the synthetic DAG while
// patches were applied to the live one — a cross-graph mismatch that silently
// no-op'd or errored). All three are updated in place: Register cannot
// overwrite already-registered keys, so SetDAG/SetGraph/SetDAG mirror that.
func (c *NewEvolutionComponents) UpdateLiveDAG(dag *engine.MutableDAG) error {
	if dag == nil {
		return errors.New("live DAG must not be nil")
	}
	c.liveDAG = dag

	// Repoint the WorkflowGenome at the live DAG so mutations and diffs are
	// computed against the topology patches will actually touch. A registry
	// Register cannot overwrite an already-registered genome, so we update the
	// existing instance in place via SetDAG. GenomeReg is nil when UpdateLiveDAG
	// runs outside a full bootstrap (e.g. tests that only register executors),
	// and no WorkflowGenome is registered when bootstrap ran with a nil DAG —
	// both are a no-op, since there is no cross-graph mismatch to fix.
	if c.GenomeReg != nil {
		if wfG, gErr := c.GenomeReg.Get(genome.WorkflowGenomeName); gErr == nil {
			if wf, ok := wfG.(*genome.WorkflowGenome); ok {
				wf.SetDAG(dag)
				log.Info("new evolution: WorkflowGenome repointed at live DAG",
					"steps", len(dag.Steps()))
			}
		}
	}

	// Rebuild graph executor with the live DAG's steps.
	g, gErr := wfgraph.NewGraph("evolution-workflow")
	if gErr != nil {
		return fmt.Errorf("create evolution graph from live DAG: %w", gErr)
	}
	for _, step := range dag.Steps() {
		fn, fErr := wfgraph.NewFuncNode(step.ID, func(_ context.Context, _ *wfgraph.State) error { return nil })
		if fErr != nil {
			return fmt.Errorf("create func node %s: %w", step.ID, fErr)
		}
		if _, nErr := g.Node(step.ID, fn); nErr != nil {
			return fmt.Errorf("add node %s: %w", step.ID, nErr)
		}
	}
	for _, step := range dag.Steps() {
		for _, dep := range step.DependsOn {
			if _, eErr := g.Edge(dep, step.ID); eErr != nil {
				return fmt.Errorf("add edge %s→%s: %w", dep, step.ID, eErr)
			}
		}
	}
	if len(dag.Steps()) > 0 {
		if _, sErr := g.Start(dag.Steps()[0].ID); sErr != nil {
			return fmt.Errorf("set start node: %w", sErr)
		}
	}

	// Rebuild the graph executor with the live DAG in place. Register cannot
	// overwrite an already-registered component key, so a naive
	// RegisterComponent/Register would always fail — the previous code returned
	// an error on every call. We instead SetGraph on the executor that
	// bootstrap registered, mirroring recoveryExec.SetDAG and
	// knowledgeExec.SetRuntime below.
	if c.graphExec != nil {
		c.graphExec.SetGraph(g)
	} else {
		// Fallback: create a new executor if no existing one was stored.
		graphExec := wfgraph.NewGraphPatchExecutor(g)
		_ = c.PatchReg.RegisterComponent(graphExec)
	}

	// Rebuild recovery executor with the live DAG.
	// Register fails on existing keys (bootstrap executors already registered),
	// so we use SetDAG to update the existing executor's DAG reference instead.
	if c.recoveryExec != nil {
		c.recoveryExec.SetDAG(dag)
	} else {
		// Fallback: create a new executor if no existing one was stored.
		recoveryExec := engine.NewRecoveryPatchExecutor(dag)
		_ = c.PatchReg.RegisterComponent(recoveryExec)
		_ = c.PatchReg.Register("recovery.max_attempts", recoveryExec)
		_ = c.PatchReg.Register("recovery.replacement_agent", recoveryExec)
		_ = c.PatchReg.Register("recovery.max_retries", recoveryExec)
		_ = c.PatchReg.Register("recovery.strategy", recoveryExec)
	}

	// Install the structure executor as the patch registry's fallback. The
	// WorkflowDiffer emits patches whose Target is a node ID (e.g. "wf-mut-1")
	// rather than a registered component key, so without a fallback they hit
	// "no executor registered for target". Binding the fallback to the live DAG
	// makes structure patches mutate the real runtime topology. SetDAG keeps
	// the same executor pointer (and thus the registered fallback slot) while
	// rebinding it to a refreshed DAG on later calls.
	if c.dagExec != nil {
		c.dagExec.SetDAG(dag)
	} else {
		c.dagExec = engine.NewDAGPatchExecutor(dag)
		c.PatchReg.SetFallback(c.dagExec)
	}

	log.Info("new evolution: live DAG injected into executors",
		"steps", len(dag.Steps()))
	return nil
}

// SetToolClassDAG injects the L1 capability graph (M5). The L1 graph is the
// evolution system's ToolClass action surface: its nodes are
// toolName#argShape, and its Metadata (enabled/budget/prior) constrains L2
// growth. Unlike the live DAG (UpdateLiveDAG), the L1 graph is NOT compiled
// into taskfabric and does NOT replace the recovery executor — it is a
// capability catalog, not an execution plan.
//
// The L1 graph is stored for the plannerCognition to read at growth time
// (§6 constraint point: "要不要长出这个节点"). Evolution structure patches
// (SetNodeMetadata) mutate L1 metadata; the planner reads the mutated values
// before growing each tool node. A nil dag clears the L1 graph (constraints
// default to permissive).
func (c *NewEvolutionComponents) SetToolClassDAG(dag *engine.MutableDAG) {
	c.toolClassDAG = dag
	if dag != nil {
		log.Info("new evolution: L1 ToolClass DAG injected",
			"nodes", len(dag.Steps()))
	}
}

// ToolClassDAG returns the L1 capability graph, or nil when no L1 graph was
// injected. The plannerCognition reads this to check enabled/budget/prior
// before growing tool nodes (§6 constraint point).
func (c *NewEvolutionComponents) ToolClassDAG() *engine.MutableDAG {
	return c.toolClassDAG
}

// used when no KnowledgeRuntime is available. It accepts all knowledge patches
// but does nothing — enabling the evolution pipeline to function without AKF.
type noopKnowledgeExecutor struct{}

func (e *noopKnowledgeExecutor) Name() string { return "knowledge.planner" }

func (e *noopKnowledgeExecutor) Snapshot(_ context.Context) (any, error) {
	return nil, errors.New("noop: no snapshot")
}

func (e *noopKnowledgeExecutor) Apply(_ context.Context, p patch.RuntimePatch) (*patch.RuntimePatch, error) {
	return &patch.RuntimePatch{
		Type:   p.Type,
		Reason: "rollback: mimic original config",
	}, nil
}

func (e *noopKnowledgeExecutor) CanApply(_ context.Context, p patch.RuntimePatch) error {
	switch p.Type {
	case patch.PatchChangeBudget, patch.PatchChangePlanner, patch.PatchChangeReducer:
		return nil
	default:
		return fmt.Errorf("knowledge noop executor: unsupported patch type %s", p.Type)
	}
}

// Ensure noopKnowledgeExecutor implements patch.RuntimeComponent.
var _ patch.RuntimeComponent = (*noopKnowledgeExecutor)(nil)

// BuildKnowledgeRuntime creates a KnowledgeRuntime for the evolution
// system with registered providers (memory, code, optional vector + store)
// that work without an external database. This enables the
// KnowledgePatchExecutor to process knowledge/planner patches meaningfully
// instead of being a no-op.
//
// The vector provider is registered only when both a VectorStore and an
// EmbeddingService are supplied (e.g. postgres pgvector + the shared embedding
// client). The AKG store provider is registered when both a KnowledgeStore
// and an EmbeddingService are supplied — this closes the AKG read loop so
// facts written by the DistillBridge are recalled through the runtime. When
// a dependency is nil the corresponding provider is skipped and the runtime
// keeps working with the memory/code providers — best-effort, not a hard
// dependency.
func BuildKnowledgeRuntime(
	vecStore storage.VectorStore,
	emb apiembedding.EmbeddingService,
	store knowledge.KnowledgeStore,
) *knowledgeruntime.KnowledgeRuntime {
	knowPipe := knowledge.NewKnowledgePipeline(
		[]knowledge.Normalizer{&pipeline.DefaultNormalizer{MaxRawBytes: 10240}},
		[]knowledge.EntityMatcher{&pipeline.DefaultEntityMatcher{MatchThreshold: 0.6}},
		[]knowledge.Validator{&pipeline.DefaultValidator{}},
		[]knowledge.Summarizer{&pipeline.DefaultSummarizer{MaxSummaryLen: 200}},
	)

	reg := provider.NewProviderRegistry()
	// Register lightweight providers that work without an external database.
	// Memory provider — stores knowledge objects in-memory for the current session.
	if err := reg.Register(provider_memory.New("memory-default", nil)); err != nil {
		log.Warn("bootstrap: register memory provider for knowledge runtime", "error", err)
	}
	// Code provider — extracts knowledge from the local codebase (functions, types, etc.).
	if cp, err := provider_code.New("codebase", "."); err == nil {
		if err := reg.Register(cp); err != nil {
			log.Warn("bootstrap: register code provider for knowledge runtime", "error", err)
		}
	} else {
		log.Warn("bootstrap: create code provider for knowledge runtime", "error", err)
	}
	// Vector provider — semantic search over embedded documents (best-effort).
	// Needs both a VectorStore (pgvector) and an EmbeddingService; without
	// either there is no corpus to search and no query embedding to produce.
	if vecStore != nil && emb != nil {
		vp, err := vector.NewVectorProvider(vecStore, vector.Config{
			Name:            "knowledge-vectors",
			Namespace:       fitnessSourceKnowledge,
			Collection:      tableKnowledgeChunks,
			IntentTags:      []string{fitnessSourceKnowledge, "doc", "guide"},
			VectorDimension: 1024,
			Embedder:        emb,
		})
		if err != nil {
			log.Warn("bootstrap: create vector provider for knowledge runtime", "error", err)
		} else if err := reg.Register(vp); err != nil {
			log.Warn("bootstrap: register vector provider for knowledge runtime", "error", err)
		} else {
			log.Info("bootstrap: vector provider wired for knowledge runtime",
				"collection", "knowledge_chunks_1024",
				"embedding_model", emb.GetModel())
		}
	}

	// AKG store provider — recalls AKG-distilled facts written by the
	// DistillBridge (write side of the AKG closed loop). Needs both a
	// KnowledgeStore and an EmbeddingService; without either there is no
	// corpus to search and no query embedding to produce. The provider is
	// skipped with a warning when AKG is not enabled (store nil).
	if store != nil && emb != nil {
		sp := storeprovider.New("akg_store", store, emb, akgModelName(emb), akgNamespace)
		if err := reg.Register(sp); err != nil {
			log.Warn("bootstrap: register AKG store provider for knowledge runtime", "error", err)
		} else {
			log.Info("bootstrap: AKG store provider wired for knowledge runtime",
				"namespace", akgNamespace, "model", akgModelName(emb))
		}
	}

	knowDiscovery := planner.NewSourceDiscovery(
		reg,
		planner.NewQueryPlanner(),
	)
	return knowledgeruntime.New(
		planner.NewKnowledgePlanner(),
		knowDiscovery,
		reg,
		knowPipe,
		[]knowledgeruntime.Linker{&knowledgeruntime.DefaultLinker{}},
		[]knowledgeruntime.Reducer{&knowledgeruntime.DefaultReducer{}},
	)
}

// buildMemoryManager creates a lightweight ProductionMemoryManager for the
// evolution system that works without a database pool. The MemoryPatchExecutor
// only needs the config field — it reads/writes memory configuration values
// (max_history, max_tasks, session_ttl, etc.) without touching the database.
func buildMemoryManager() *aresmemory.ProductionMemoryManager {
	return aresmemory.NewMinimalMemoryManager()
}
