// Package ares_bootstrap orchestrates component assembly.
package ares_bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"golang.org/x/sync/errgroup"

	apiembed "github.com/Timwood0x10/ares/api/embedding"
	"github.com/Timwood0x10/ares/internal/ares_callbacks"
	"github.com/Timwood0x10/ares/internal/ares_config"
	"github.com/Timwood0x10/ares/internal/ares_eval"
	"github.com/Timwood0x10/ares/internal/ares_events"
	evolution "github.com/Timwood0x10/ares/internal/ares_evolution"
	aresexp "github.com/Timwood0x10/ares/internal/ares_experience"
	flight "github.com/Timwood0x10/ares/internal/ares_flight"
	"github.com/Timwood0x10/ares/internal/ares_mcp"
	ares_memory "github.com/Timwood0x10/ares/internal/ares_memory"
	"github.com/Timwood0x10/ares/internal/ares_observability"
	"github.com/Timwood0x10/ares/internal/ares_runtime"
	ares_skills "github.com/Timwood0x10/ares/internal/ares_skills"
	"github.com/Timwood0x10/ares/internal/aresrecovery"
	"github.com/Timwood0x10/ares/internal/evidence"
	"github.com/Timwood0x10/ares/internal/evolution/deployment"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/Timwood0x10/ares/internal/knowledge"
	"github.com/Timwood0x10/ares/internal/knowledge/adapter"
	knowledgeruntime "github.com/Timwood0x10/ares/internal/knowledge/runtime"
	"github.com/Timwood0x10/ares/internal/knowledge/skills"
	"github.com/Timwood0x10/ares/internal/storage"
	"github.com/Timwood0x10/ares/internal/storage/postgres"
	"github.com/Timwood0x10/ares/internal/storage/postgres/repositories"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// DAG step identifiers used in the minimal evolution graph.
const dagStepProcess = "process"

// Components holds all assembled system components.
type Components struct {
	MCP          *ares_mcp.MCPManager
	Dashboard    *ObservabilityProviders
	LLM          *LLMComponents
	Evolution    *EvolutionComponents
	NewEvolution *NewEvolutionComponents
	Runtime      *ares_runtime.Manager
	Memory       ares_memory.MemoryManager
	EventStore   ares_events.EventStore
	Distillation *aresexp.DistillationService
	// SkillsRegistry is the progressive-disclosure skill index seeded by
	// wireSkills (REVIEW #11 closure). It powers two readers: the memory
	// manager's resident "Available skills" block (attached via
	// SetSkillsRegistry) and the environment-capability searcher (envcap) in
	// serve, which exposes skills as searchable tool capabilities. Nil when
	// memory is disabled or skill wiring was skipped.
	SkillsRegistry *skills.Registry
	// SkillCatalog is the live catalog handle (W8): serve registers its
	// CatalogTools into the agent tool registry and wires the experience
	// confidence source from it. Nil when skills are disabled.
	SkillCatalog *ares_skills.Catalog
	// Discovery holds the optional service discovery engine. It is nil when
	// cfg.Discovery.Enabled is false (the default), preserving prior behavior.
	Discovery *DiscoveryComponents
	// KnowledgeRuntime is the shared knowledge runtime used by the evolution
	// system's KnowledgePatchExecutor and the agent's AKF tools. It is
	// created once during bootstrap and reused so that knowledge genome
	// patches (ChangeBudget/ChangePlanner/ChangeReducer) affect the actual
	// runtime used by the agent's knowledge tools.
	KnowledgeRuntime *knowledgeruntime.KnowledgeRuntime
	// VectorStore backs the knowledge runtime's VectorProvider (semantic
	// search over embedded documents). It is nil when distillation/vector
	// storage is not wired, in which case the runtime skips the vector
	// provider entirely.
	VectorStore storage.VectorStore
	// KnowledgeStore backs the AKG read/write loop: the DistillBridge
	// (write side) persists AKG facts here and the knowledge runtime's
	// StoreProvider / the leader's KnowledgeRetriever (read side) recall
	// them. In-memory by default; PostgreSQL when storage is configured.
	// Nil when AKG is not enabled (cfg.Knowledge.RetrievalEnabled).
	KnowledgeStore knowledge.KnowledgeStore
	// AKGBridge distills conversations into KnowledgeStore on task
	// lifecycle events (write side of the AKG loop). Nil when AKG or its
	// write dependencies (embedding client, experience repo) are
	// unavailable.
	AKGBridge *adapter.DistillBridge
	// FlightRecorder is the single shared flight recorder (collector
	// subscribes to comp.EventStore and emits workflow/scheduler/recovery
	// fitness evidence into the shared evidence store). It is created and
	// started by Bootstrap independently of ProvideEvolution so the fitness
	// write loop works even when the legacy evolution deps (ExpRepo) are
	// absent; ProvideEvolution and the serve launcher reuse it instead of
	// building their own. Nil when the event store is unavailable.
	FlightRecorder *flight.FlightRecorder
	// ExpRepo is the experience repository used by distillation writes
	// (Track A) and — in the Agent Fabric runtime — as the G1 spawn-prior
	// source (aresos-agentos-plan G1: distillation output → experience repo query → spawn injection).
	// It is the deps.ExpRepo when provided, or the repository created by
	// wireDistillation when PostgreSQL distillation is enabled; nil otherwise
	// (callers treat nil as "no prior", never as an error).
	ExpRepo repositories.ExperienceRepositoryInterface
	// EvidenceStore is the shared evidence store used by the flight recorder
	// and (when enabled) the GA genomes. Always set, even when evolution is
	// disabled, so downstream consumers (cmd/ares serve, tests) can
	// reference it without nil guards.
	EvidenceStore evidence.Store
	// SystemRuntime is the system-level control plane (Stage 1): an
	// orchestrator that observes the assembled component graph and provides
	// lifecycle states, a shared root context, and status snapshots. It is
	// created at the end of Bootstrap; nil when wiring is skipped on failure.
	SystemRuntime *kernel.Orchestrator
	// SystemRegistry backs SystemRuntime with one entry per constructed
	// component, enabling dependency-aware lookup and snapshot queries.
	SystemRegistry *kernel.Registry
	// Observability holds the shared v0.3.0 M3/M4 observability components:
	// the evolution trajectory tracer, the human-feedback store, and the
	// cross-Fabric tracer. All three are created together once in Bootstrap
	// and shared by the dashboard (read side) and the runtime write hooks
	// (GA generation recording, task/agent lifecycle tracing), so the
	// dashboard endpoints show live data. Non-nil whenever Bootstrap
	// completed; nil only when wiring never ran.
	Observability *ObservabilityComponents
	// ExpiryCleaners lists repositories that own TTL/decay purges (REVIEW #7).
	// Subsystems append entries when they construct a repo with retention
	// columns; startExpiryCleanupWorker purges them hourly on bgGroup. Empty
	// by default (no cleaners wired = no worker goroutine).
	ExpiryCleaners []NamedExpiryCleaner
	// bgGroup manages all Bootstrap background goroutines (distillation
	// subscriber, GA evolution ticker, LLM suggestion ticker) via errgroup
	// (F06: no bare goroutines). WaitBackground blocks on it during shutdown.
	bgGroup errgroup.Group
}

// ObservabilityComponents groups the shared v0.3.0 M3/M4 observability
// surfaces. They are constructed together as one subsystem — Bootstrap creates
// all three unconditionally and the dashboard reads them via provider
// adapters, so the flat Components struct stays scannable.
type ObservabilityComponents struct {
	// EvolutionTracer is the shared evolution trajectory tracer (v0.3.0
	// M3-1). Shared by the dashboard (read side: /evolution/trajectory) and
	// the GA wiring (write side: Record after each generation).
	EvolutionTracer *aresrecovery.EvolutionTracer
	// FeedbackStore is the shared human-feedback store (v0.3.0 M3-2). Written
	// by POST /evolution/feedback; read by the evolution scoring path.
	FeedbackStore *aresrecovery.FeedbackStore
	// GlobalTracer is the shared cross-Fabric tracer (v0.3.0 M4-1). It is
	// shared by the dashboard (read side: /observability/spans) and the
	// kernel wiring (write side: task/agent lifecycle hooks). Nil when the
	// dashboard observability wiring is skipped.
	GlobalTracer *aresrecovery.GlobalTracer
}

// GoBackground runs fn as an errgroup-managed background goroutine on the
// Bootstrap group (F06: no bare goroutines) with a panic-recover boundary so
// one panicking tick logs and returns instead of killing the process (M2).
// The goroutine runs until WaitBackground; fn should exit promptly when its
// ctx is cancelled.
func (c *Components) GoBackground(ctx context.Context, name string, fn func(ctx context.Context) error) {
	c.bgGroup.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("bootstrap: background goroutine panicked",
					"name", name, "panic", r)
				err = fmt.Errorf("background %s panicked: %v", name, r)
			}
		}()
		return fn(ctx)
	})
}

// WaitBackground blocks until all background goroutines started by Bootstrap
// (distillation event subscriber, GA evolution ticker, LLM suggestion ticker)
// have exited. It must be called after the bootstrap context is cancelled;
// each goroutine exits on ctx.Done() and this ensures no goroutine is left
// running across a graceful shutdown.
func (c *Components) WaitBackground() {
	if c == nil {
		return
	}
	if err := c.bgGroup.Wait(); err != nil {
		log.Warn("bootstrap: background group error during shutdown", "error", err)
	}
}

// Snapshot returns the system-level component status snapshot (Stage 1
// observability). It returns an empty snapshot when the System Runtime
// registry is not wired (Bootstrap failed before wiring completed), so
// callers can always consume a valid value without nil guards.
func (c *Components) Snapshot() kernel.Snapshot {
	if c == nil || c.SystemRegistry == nil {
		return kernel.Snapshot{}
	}
	return c.SystemRegistry.Snapshot()
}

// ComponentStatus returns the status of one managed component by name.
// The bool is false when the component is not registered.
func (c *Components) ComponentStatus(name string) (kernel.ComponentStatus, bool) {
	if c == nil || c.SystemRegistry == nil {
		return kernel.ComponentStatus{}, false
	}
	return c.SystemRegistry.GetStatus(name)
}

// IsSystemReady reports whether all Required components reached Ready and no
// component is Failed. Returns false when the registry is not wired.
func (c *Components) IsSystemReady() bool {
	if c == nil || c.SystemRegistry == nil {
		return false
	}
	return c.SystemRegistry.IsReady()
}

// LLMComponents holds LLM client and callback registry.
type LLMComponents struct {
	Client      interface{}
	CallbackReg *ares_callbacks.Registry
	// CostDashboard is the W1 cost surface served at
	// /api/v1/observability/cost*; fed by the LLM client's MetricsTracer.
	CostDashboard *ares_observability.CostDashboard
}

// BootstrapDeps holds optional external dependencies for full wiring.
type BootstrapDeps struct {
	EventStore ares_events.EventStore
	ExpRepo    repositories.ExperienceRepositoryInterface
	LLMClient  ares_eval.LLMClient
}

// Bootstrap assembles all components from config and optional dependencies.
// It is the single wiring hub — used by cmd/ares serve, and tests.
// On partial failure, already-created components are cleaned up in reverse
// order before returning the error.
// extracted for each major component group (wireMemory, wireNewEvolution, etc.)
// and the remaining complexity is inherent to the assembly orchestration.
//
//nolint:gocyclo // Bootstrap is a complex wiring hub; sub-functions are
func Bootstrap(ctx context.Context, cfg *ares_config.Config, deps *BootstrapDeps) (*Components, error) {
	var comp Components

	if deps == nil {
		deps = &BootstrapDeps{}
	}

	// Track cleanup functions for components created during bootstrap.
	// On error, they are executed in reverse order of creation.
	var cleanups []func()

	// runCleanups executes all cleanup functions in reverse order.
	runCleanups := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	// 1. EventStore — from deps or create in-memory default
	if deps.EventStore != nil {
		comp.EventStore = deps.EventStore
	} else {
		comp.EventStore = ares_events.NewMemoryEventStore()
	}

	// 2. Runtime — always created (accepts nil eventStore)
	rt, err := ProvideRuntime(comp.EventStore)
	if err != nil {
		runCleanups()
		return nil, err
	}
	comp.Runtime = rt

	// 3. Memory — only construct when cfg.Memory.IsEnabled() is true.
	// Stage 2 fix (F01): respect the config gate so disabled = no goroutine,
	// no event subscription, no store writes.
	mem, memErr := wireMemory(cfg, comp.EventStore)
	if memErr != nil {
		runCleanups()
		return nil, memErr
	}
	comp.Memory = mem

	// 4. MCP
	mcp, err := ProvideMCP(ctx, cfg.MCP)
	if err != nil {
		runCleanups()
		return nil, err
	}
	comp.MCP = mcp
	cleanups = append(cleanups, func() {
		if err := mcp.Stop(ctx); err != nil {
			log.Warn("bootstrap: cleanup MCP stop error", "error", err)
		}
	})

	// 4b. SKILLS progressive disclosure (REVIEW #11 closure): assemble the
	// skill catalog once and seed it into the memory manager so the resident
	// "Available skills" block is populated in serve (previously only the
	// `ares status` CLI constructed the catalog). The seeded registry is also
	// stored on comp.SkillsRegistry so the serve launcher can feed it to the
	// environment-capability searcher (envcap), completing the second half of
	// progressive disclosure: skills become searchable tool capabilities, not
	// just a resident prompt block. Best-effort: skipped when memory is
	// disabled or the manager does not expose SetSkillsRegistry.
	if comp.Memory != nil {
		if catalog, reg := wireSkills(ctx, comp.Memory, mcp); catalog != nil {
			comp.SkillsRegistry = reg
			comp.SkillCatalog = catalog
			cleanups = append(cleanups, func() {
				if err := catalog.Close(); err != nil {
					log.Warn("bootstrap: cleanup skills catalog close error", "error", err)
				}
			})
			// W8: start the skill outcome recorder so skill usage/results are
			// observed from the EventSubTaskResult stream (emitted by the sub
			// executor on task completion) and persisted for experience-guided
			// selection. Best-effort: a nil catalog/store is a no-op.
			recorder := ares_skills.NewSkillOutcomeRecorder(catalog)
			if serr := recorder.Start(ctx, comp.EventStore); serr != nil {
				log.Warn("bootstrap: skill outcome recorder start failed", "error", serr)
			} else {
				log.Info("bootstrap: skill outcome recorder started (EventSubTaskResult)")
			}
		}
	}

	// 5. LLM — from config (for backward compat) or from deps
	if deps.LLMClient != nil {
		comp.LLM = &LLMComponents{Client: deps.LLMClient}
	} else {
		llm, err := ProvideLLM(cfg.LLM)
		if err != nil {
			runCleanups()
			return nil, err
		}
		comp.LLM = llm
	}

	// 5b + 5c. Experience distillation + auto-distill on task completion
	// (Track A). Wired conditionally (PG + embedding); failures are non-fatal.
	// embClient is reused by wireRetrievers to build the MemoryRetriever, so
	// the distillation and RAG retrieval paths share one embedding client.
	guidanceProvider, embClient := wireDistillation(ctx, cfg, &comp, deps, &cleanups)
	// G1: expose the experience repository (deps-provided or distillation-
	// created) so consumers can query distilled experiences — e.g. the Agent
	// Fabric spawn path injects the latest experience as the spawn prior.
	comp.ExpRepo = deps.ExpRepo

	// AKG closed loop (0.2.9): build the KnowledgeStore (in-memory default,
	// PG optional) and the write-side DistillBridge, gated on
	// cfg.Knowledge.RetrievalEnabled. Best-effort: when AKG or its deps are
	// unavailable the loop is skipped with a warning, leaving the system
	// fully functional (read-only mode keeps the store when write deps
	// are missing). The store is shared by the knowledge runtime's
	// StoreProvider (read side) and the leader's KnowledgeRetriever.
	knowStore, akgBridge := wireAKGLoop(cfg, deps, embClient)
	comp.KnowledgeStore = knowStore
	comp.AKGBridge = akgBridge

	subscribeDistillationEvents(ctx, &comp)

	// 6. Dashboard
	// The v0.3.0 M3/M4 observability components (trajectory tracer, feedback
	// store, global tracer) are created ONCE here and shared: the dashboard
	// reads them via the provider adapters, and the runtime write hooks (GA
	// generation recording, task/agent lifecycle tracing) write into the same
	// instances — so the dashboard endpoints show live data, not empty lists.
	comp.Observability = &ObservabilityComponents{
		EvolutionTracer: aresrecovery.NewEvolutionTracer(),
		FeedbackStore:   aresrecovery.NewFeedbackStore(),
		GlobalTracer:    aresrecovery.NewGlobalTracer(),
	}
	// Runtime observability providers (monitoring.md Phase 4): the M3/M4
	// surfaces now feed introspect.ControlServer directly; the standalone
	// :8090 dashboard server was removed.
	comp.Dashboard = ProvideObservability(
		comp.Observability.EvolutionTracer,
		comp.Observability.FeedbackStore,
		comp.Observability.GlobalTracer,
	)

	// 7+8. Evolution wiring order matters: ProvideNewEvolution (below) creates
	// the shared evidence store (newEvol.EvidenceStore); ProvideEvolution's
	// flight recorder must be built AFTER it so the flight collector's
	// workflow/scheduler/recovery fitness evidence lands in the same store
	// the GA genomes read (previously the recorder got a nil EvidenceStore
	// and those three fitness signals were silently dropped).

	// 8. New Evolution — runtime-evolution system (Genome + Diff + Coordinator).
	// Stage 2 fix (F02): only construct when cfg.Evolution.Enabled is true.
	// When disabled, no NewEvolution, no GA ticker, no LLM suggestion ticker.
	if !cfg.Evolution.Enabled {
		log.Info("bootstrap: evolution disabled (cfg.Evolution.Enabled=false), " +
			"skipping NewEvolution and background tickers")
	}
	dag, dagErr := buildEvolutionDAG(cfg.Evolution.Enabled)
	if dagErr != nil {
		runCleanups()
		return nil, dagErr
	}

	// Type-assert comp.Memory to MemoryConfigStore. Both *memoryManager and
	// *ProductionMemoryManager implement MemoryConfigStore. When Memory is
	// disabled (comp.Memory is nil), fall back to the minimal manager so
	// the evolution system still has a MemoryConfigStore to write patches to.
	liveMemoryStore := resolveLiveMemoryStore(comp.Memory)

	// Create the KnowledgeRuntime once and share it between the evolution
	// system and the agent's AKF tools so knowledge genome patches affect
	// the actual runtime used by the agent's knowledge tools. The vector
	// provider is registered when postgres vector storage + embedding are
	// wired (comp.VectorStore / embClient); otherwise the runtime uses only
	// the memory/code providers.
	// Convert nil *EmbeddingClient to nil EmbeddingService interface to avoid
	// the Go nil-interface-trap: a nil typed pointer wrapped in a non-nil
	// interface passes nil checks but panics on method calls (e.g. GetModel).
	var embForRuntime apiembed.EmbeddingService
	if embClient != nil {
		embForRuntime = embClient
	}
	knowRt := BuildKnowledgeRuntime(comp.VectorStore, embForRuntime, knowStore)
	comp.KnowledgeRuntime = knowRt

	// T1 (evidence persistence): when PostgreSQL is configured, use a
	// persistent evidence store instead of the default in-memory one.
	// Fail-loud: configured Postgres that cannot connect blocks startup.
	var evidenceStore evidence.Store
	if cfg.Storage.Enabled && cfg.Storage.Host != "" {
		pgCfg := &postgres.Config{
			Host:     cfg.Storage.Host,
			Port:     cfg.Storage.Port,
			User:     cfg.Storage.Username,
			Password: cfg.Storage.Password,
			Database: cfg.Storage.Database,
			SSLMode:  cfg.Storage.SSLMode,
		}
		pgPool, pgErr := postgres.NewPool(pgCfg)
		if pgErr != nil {
			runCleanups()
			return nil, fmt.Errorf("evidence: create postgres pool: %w", pgErr)
		}
		pgStore, storeErr := evidence.NewPostgresStore(pgPool)
		if storeErr != nil {
			runCleanups()
			return nil, fmt.Errorf("evidence: create postgres store: %w", storeErr)
		}
		evidenceStore = pgStore
		// N9 closure: register the evidence store with the maintenance worker
		// so TTL-expired rows are purged on the same hourly schedule as the
		// other retention-managed tables. Query already hides expired rows;
		// this stops the table growing unboundedly with dead ones.
		comp.ExpiryCleaners = append(comp.ExpiryCleaners,
			NamedExpiryCleaner{Name: "evidence_records", Cleaner: pgStore})
		cleanups = append(cleanups, func() {
			if cerr := pgPool.Close(); cerr != nil {
				log.Warn("bootstrap: close evidence postgres pool",
					"error", cerr)
			}
		})
	}

	newEvol, evStore, evErr := wireNewEvolution(cfg.Evolution.Enabled, dag, knowRt, liveMemoryStore, evidenceStore)
	if evErr != nil {
		runCleanups()
		return nil, evErr
	}
	comp.NewEvolution = newEvol
	comp.EvidenceStore = evStore

	// Single shared flight recorder — created and started here, independent
	// of the legacy evolution deps (ExpRepo). Its collector subscribes to
	// comp.EventStore and emits workflow/scheduler/recovery fitness evidence
	// into the shared evidence store (the same store the GA genomes read when
	// evolution is enabled), so the fitness write loop works on every
	// production path (ares serve / ares start) even when ProvideEvolution is
	// skipped. ProvideEvolution and the serve launcher reuse this instance.
	if comp.EventStore != nil {
		comp.FlightRecorder = flight.NewFlightRecorder(flight.FlightRecorderConfig{
			EventStore:    comp.EventStore,
			EvidenceStore: evStore,
		})
		if err := comp.FlightRecorder.Start(ctx); err != nil {
			log.WarnContext(ctx, "bootstrap: flight recorder start failed (fitness evidence disabled)",
				"error", err)
		}
		cleanups = append(cleanups, comp.FlightRecorder.Stop)
	}

	// 7. Evolution (legacy system) — only if all required deps are wired.
	// Built after the shared recorder so it reuses comp.FlightRecorder
	// (which shares the evidence store with the GA genomes) instead of
	// constructing a second recorder. Fully gated by cfg.Evolution.Enabled
	// (F02) so the legacy scheduler/dream cycle cannot start behind the
	// config's back.
	evol, err := wireLegacyEvolution(ctx, cfg, deps, &comp)
	if err != nil {
		runCleanups()
		return nil, err
	}
	comp.Evolution = evol
	// B4: ProvideEvolution calls scheduler.Register(), which subscribes to the
	// EventStore on a context.Background() of its own and parks a goroutine on
	// the event channel. Nothing used to call Shutdown(), so that goroutine —
	// plus the EventStore subscriber goroutine feeding it — outlived every
	// Runtime and Bootstrap for the life of the process. goleak surfaced it;
	// counting goroutines by hand never would have, which is why B4 exists.
	if evol != nil {
		if sched, ok := evol.Scheduler.(*evolution.EvolutionScheduler); ok && sched != nil {
			comp.bgGroup.Go(func() error {
				<-ctx.Done()
				sched.Shutdown()
				return nil
			})
		}
	}

	// Closed-loop wiring: inject MemoryRetriever (distilled experiences) and
	// KnowledgeRetriever (AKG entries) into the MemoryManager so every
	// BuildContext / BuildPromptMessages call augments the prompt with
	// retrieved context when config.EnableRAG is true. Best-effort: skips
	// retrievers whose dependencies (embedding client, experience repo, AKG
	// runtime) are unavailable, so minimal configs are unaffected.
	//
	// Runs after ProvideNewEvolution so the retriever can emit retrieval
	// evidence to the shared evidence store (Source "memory") consumed by the
	// GA MemoryGenome.
	wireRetrievers(ctx, cfg, comp.Memory, embClient, deps.ExpRepo, knowRt, knowStore, evStore)

	// Track C (C-Safe): wire the DeploymentPipeline into the Coordinator so
	// generated patches are safely promoted to the live runtime. Gated by
	// cfg.Evolution.Deployment.Enabled — when disabled, the Coordinator falls
	// back to applying patches directly (pre-deployment behavior). The live
	// runtime is the real executor registry, so memory patches are written to
	// the live comp.Memory; workflow/scheduler/recovery/knowledge patches hit
	// their (still synthetic) executors — closing those requires a live DAG
	// supply chain (Track C-Risky, deferred).
	if cfg.Evolution.Enabled && cfg.Evolution.Deployment.Enabled && comp.NewEvolution != nil {
		staging := &deploymentStagingRuntime{
			reg: comp.NewEvolution.PatchReg,
			// B6 fix: shared scoring backend (same weights/filter as the
			// lifecycle's rollback window) + explicit cold-start score so
			// patches without evidence get a conservative 0.5 instead of a
			// universal 0.0 reject.
			agg:            evolution.NewRuntimeFitnessAggregator(comp.EvidenceStore, evolution.DefaultAggregatorConfig()),
			coldStartScore: 0.5,
			// Baseline scoring resolves the active strategy live at Evaluate
			// time via the ASM, so a strategy switched mid-run is reflected
			// in the comparison instead of a stale construction-time ID.
			asm: comp.NewEvolution.ActiveStrategyManager,
		}
		dp := deployment.NewDeploymentPipeline(
			cfg.Evolution.Deployment,
			staging,
			&deploymentLiveRuntime{reg: comp.NewEvolution.PatchReg},
		)
		comp.NewEvolution.Coordinator.SetDeployer(&deploymentAdapter{dp: dp})
		log.Info("bootstrap: deployment pipeline wired into coordinator", "enabled", true)
	}

	// Register the minimal DAG with the runtime manager so the evolution
	// system can apply workflow patches to the live DAG (v0.5.0 DAG reflux).
	// When a real agent DAG is registered later, it replaces this minimal one.
	if comp.Runtime != nil && dag != nil {
		comp.Runtime.RegisterAgentDAG(ares_runtime.AgentDAGEvolutionKey, dag)
	}

	// Closure plan N-4 / Step 7.3: a standalone Bootstrap has no agent
	// population, so no live agent DAG exists at this point — evolution
	// verdicts are available but have no live topology to act on. Say so
	// explicitly instead of letting the synthetic graph silently take
	// promotions. The serve entry (buildLiveAgentDAG + UpdateLiveDAG) is the
	// only live-DAG supplier and supersedes this placeholder afterwards.
	if cfg.Evolution.Enabled {
		log.InfoContext(ctx, "bootstrap: evolution verdicts available but no live agent topology to act on",
			"live_dag_registered", false,
			"synthetic_dag_key", ares_runtime.AgentDAGEvolutionKey,
		)
	}

	// 9. Wire the GA population adapter, coordinator bridge, and background
	// evolution ticker (extracted to wireGAEvolution to keep Bootstrap's
	// cyclomatic complexity within lint limits).
	if cfg.Evolution.Enabled && comp.NewEvolution != nil {
		if err := wireGAEvolution(ctx, cfg, &comp, comp.NewEvolution, guidanceProvider); err != nil {
			runCleanups()
			return nil, err
		}
	}

	// 10. Optional service discovery (opt-in via config.Discovery.Enabled).
	// When disabled, ProvideDiscovery returns ErrDiscoveryDisabled and the
	// discovery packages remain unused, preserving prior behavior.
	discoveryComp, err := ProvideDiscovery(ctx, &cfg.Discovery, comp.EventStore)
	switch {
	case errors.Is(err, ErrDiscoveryDisabled):
		// Discovery is disabled — not an error, just no-op.
		comp.Discovery = nil
	case err != nil:
		runCleanups()
		return nil, fmt.Errorf("bootstrap: wire discovery: %w", err)
	default:
		comp.Discovery = discoveryComp
	}

	// 11. System Runtime (Stage 1): register the assembled component graph
	// with the system-level control plane so entry points observe a uniform
	// component list, lifecycle state, and readiness snapshot. Observational
	// only — construction and startup stay with Bootstrap.
	orch, sysReg, sysErr := wireSystemRuntime(ctx, cfg, &comp)
	if sysErr != nil {
		runCleanups()
		return nil, sysErr
	}
	comp.SystemRuntime = orch
	comp.SystemRegistry = sysReg

	// REVIEW #7 + N9 closure: purge expired/decayed rows on a schedule instead
	// of letting retention-managed tables grow unboundedly. Started last so it
	// sees every cleaner registered above (sessions, conversations, knowledge,
	// secrets, and the evidence store). No-op when no cleaners were wired
	// (e.g. storage disabled).
	startExpiryCleanupWorker(ctx, &comp)

	return &comp, nil
}

// wireMemory constructs the memory manager when cfg.Memory.IsEnabled() is true.
// Stage 2 fix (F01): disabled = no goroutine, no event subscription, no store
// writes, so the gate is honored here instead of constructing unconditionally.
// Stage 3 fix (B01): the event store is wired during construction, eliminating
// the post-Bootstrap SetEventStore bypass in serve.go. Returns nil when disabled.
//
//nolint:nilnil // nil manager + nil error is the documented "disabled" contract.
func wireMemory(cfg *ares_config.Config, eventStore ares_events.EventStore) (ares_memory.MemoryManager, error) {
	if !cfg.Memory.IsEnabled() {
		log.Info("bootstrap: memory disabled (cfg.Memory.IsEnabled()=false), skipping construction")
		return nil, nil
	}
	memCfg := ares_memory.DefaultMemoryConfig()
	if cfg.Memory.EnableRAG {
		memCfg.EnableRAG = true
		if cfg.Memory.RAGTopK > 0 {
			memCfg.RAGTopK = cfg.Memory.RAGTopK
		}
		if cfg.Memory.RAGMinScore > 0 {
			memCfg.RAGMinScore = cfg.Memory.RAGMinScore
		}
	}
	mem, err := ProvideMemory(memCfg)
	if err != nil {
		return nil, err
	}
	if eventStore != nil {
		mem.SetEventStore(eventStore, "memory")
	}
	return mem, nil
}

// buildEvolutionDAG builds the minimal mutable DAG used by the evolution system
// (workflow/scheduler/recovery genomes evolve against it). Returns nil when
// evolution is disabled so no graph is constructed behind the config's back.
//
//nolint:nilnil // nil DAG + nil error is the documented "disabled" contract.
func buildEvolutionDAG(enabled bool) (*engine.MutableDAG, error) {
	if !enabled {
		return nil, nil
	}
	dagSteps := []*engine.Step{
		{ID: "input", Name: "Input", AgentType: "parser", Input: "parse input"},
		{ID: dagStepProcess, Name: "Process", AgentType: "processor", Input: dagStepProcess, DependsOn: []string{"input"}},
		{ID: "output", Name: "Output", AgentType: "formatter", Input: "format", DependsOn: []string{dagStepProcess}},
	}
	dag, err := engine.NewMutableDAG(dagSteps)
	if err != nil {
		return nil, fmt.Errorf("create mutable dag: %w", err)
	}
	return dag, nil
}

// resolveLiveMemoryStore returns the live memory config store from the
// constructed memory manager. Both *memoryManager and *ProductionMemoryManager
// implement MemoryConfigStore; when memory is disabled or the type assertion
// fails, the minimal manager is used so evolution still has a config store.
func resolveLiveMemoryStore(mem ares_memory.MemoryManager) ares_memory.MemoryConfigStore {
	if mem != nil {
		if store, ok := mem.(ares_memory.MemoryConfigStore); ok {
			return store
		}
	}
	return buildMemoryManager()
}

// wireNewEvolution constructs the runtime evolution system (Genome + Diff +
// Coordinator) when evolution is enabled, and always returns the shared
// evidence store: when disabled, a standalone store keeps the flight recorder's
// fitness evidence flowing without a NewEvolution instance.
//
//nolint:nilnil // nil components + nil error is the documented "disabled" contract.
func wireNewEvolution(enabled bool, dag *engine.MutableDAG, rt *knowledgeruntime.KnowledgeRuntime, memoryStore ares_memory.MemoryConfigStore, evStore evidence.Store) (*NewEvolutionComponents, evidence.Store, error) {
	if !enabled {
		return nil, evidence.NewMemoryStore(), nil
	}
	newEvol, err := ProvideNewEvolution(dag, rt, memoryStore, evStore)
	if err != nil {
		return nil, nil, err
	}
	return newEvol, newEvol.EvidenceStore, nil
}

// wireLegacyEvolution wires the legacy evolution system when it is enabled and
// all required deps are present; otherwise it is skipped (nil), preserving
// prior behavior. Gated by cfg.Evolution.Enabled (F02) so the legacy scheduler
// cannot start behind the config's back.
//
//nolint:nilnil // nil components + nil error is the documented "disabled" contract.
func wireLegacyEvolution(ctx context.Context, cfg *ares_config.Config, deps *BootstrapDeps, comp *Components) (*EvolutionComponents, error) {
	if !cfg.Evolution.Enabled || deps.EventStore == nil || deps.ExpRepo == nil {
		return nil, nil
	}
	return ProvideEvolution(ctx, &cfg.Evolution,
		comp.EventStore, deps.ExpRepo,
		deps.LLMClient,
		comp.FlightRecorder,
	)
}
