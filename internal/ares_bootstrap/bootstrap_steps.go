package ares_bootstrap

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"

	"github.com/Timwood0x10/ares/internal/agents"
	"github.com/Timwood0x10/ares/internal/ares_config"
	"github.com/Timwood0x10/ares/internal/ares_events"
	evolution "github.com/Timwood0x10/ares/internal/ares_evolution"
	"github.com/Timwood0x10/ares/internal/ares_evolution/genome"
	"github.com/Timwood0x10/ares/internal/ares_evolution/mutation"
	evoService "github.com/Timwood0x10/ares/internal/ares_evolution/service"
	"github.com/Timwood0x10/ares/internal/ares_observability"
	"github.com/Timwood0x10/ares/internal/evidence"
	evoprovider "github.com/Timwood0x10/ares/internal/knowledge/provider/evolution"
	knowledgeruntime "github.com/Timwood0x10/ares/internal/knowledge/runtime"
	"github.com/Timwood0x10/ares/internal/storage/postgres"
	"github.com/Timwood0x10/ares/internal/storage/postgres/embedding"
	storage_models "github.com/Timwood0x10/ares/internal/storage/postgres/models"
	"github.com/Timwood0x10/ares/internal/storage/postgres/repositories"
)

// wireDistillation conditionally wires experience distillation (Track A) and
// returns a GuidanceProvider consumed by the GA, plus the embedding client
// used by the distillation pipeline. Both return values are nil when
// distillation is not configured/wired. Failures are non-fatal: they are
// logged and skipped, leaving the system running without distillation
// (graceful degradation). The returned embedding client is reused by
// wireRetrievers to build the MemoryRetriever, avoiding a second client.
func wireDistillation(ctx context.Context, cfg *ares_config.Config, comp *Components, deps *BootstrapDeps, cleanups *[]func()) (evolution.GuidanceProvider, *embedding.EmbeddingClient) {
	var guidanceProvider evolution.GuidanceProvider
	var embClient *embedding.EmbeddingClient
	// C1: honor memory.enable_distillation — tri-state gate (nil defaults to
	// true, P0-3 decision), so deployments relying on Storage+Embedding keep
	// distillation; only an explicit YAML false disables the wiring.
	if !cfg.Memory.DistillationEnabled() {
		return nil, nil
	}
	if cfg.Storage.Enabled && cfg.Storage.Type == storageTypePostgres && cfg.Embedding.Enabled {
		wiring, wireErr := provideDistillation(ctx, cfg, comp.LLM.Client)
		if wireErr != nil {
			log.Warn("bootstrap: experience distillation not wired", "error", wireErr)
		} else {
			pool, expRepo := wiring.pool, wiring.experienceRepo
			guidanceProvider = wiring.guidanceProvider
			embClient = wiring.embeddingClient
			comp.Distillation = wiring.service
			// Feed the experience repo into the old evolution system if present.
			if deps.ExpRepo == nil {
				deps.ExpRepo = expRepo
			}
			// REVIEW #7: register the repo's decay purge with the maintenance
			// worker so decayed experience rows are deleted, not just filtered
			// on read. The concrete *ExperienceRepository implements
			// CleanupExpired; the fat interface intentionally stays untouched.
			if cleaner, ok := expRepo.(ExpiryCleaner); ok {
				comp.ExpiryCleaners = append(comp.ExpiryCleaners,
					NamedExpiryCleaner{Name: storage_models.ExperiencesTable, Cleaner: cleaner})
			}
			// REVIEW #7 (remainder): register the other retention-managed
			// tables (sessions, conversations, secrets, knowledge_chunks) so
			// their expired/decayed rows are purged too, not just experiences.
			// They share the distillation pool (already open for the process
			// lifetime) instead of opening a second pool — minimal wiring.
			wireExpiryCleaners(comp, pool.GetDB(), cfg)
			// The embedding queue worker + reconciler consume pending tasks and
			// write vectors back to knowledge_chunks_1024 and experiences_1024
			// (both repos share the same pool). The producer side is wired in
			// provide_distillation: the distillation path persists an experience
			// row without a vector and enqueues a backfill task so the async
			// worker writes the vector back instead of blocking the event
			// subscriber loop on a synchronous embed. The LLM extraction call
			// (30s) still runs in the subscriber loop; only the embed was
			// deferred to the worker. The queue instance is shared with the
			// producer rather than rebuilt here.
			knowRepo := repositories.NewKnowledgeRepository(pool.GetDB(), pool.GetDB())
			var expConcreteRepo *repositories.ExperienceRepository
			if r, ok := expRepo.(*repositories.ExperienceRepository); ok {
				expConcreteRepo = r
			}
			wireEmbeddingWorker(ctx, comp, pool, embClient,
				wiring.embeddingQueue, wiring.embeddingConfig, knowRepo, expConcreteRepo)
			// Back the knowledge runtime's VectorProvider with the same PG
			// pool, so AKF vector search reads the same embedded corpus the
			// distillation path writes. Best-effort: nil embedding config uses
			// defaults.
			comp.VectorStore = postgres.NewVectorSearcher(pool, nil)
			// The postgres pool must be closed if bootstrap fails later.
			*cleanups = append(*cleanups, func() {
				if cerr := pool.Close(); cerr != nil {
					log.Warn("bootstrap: close distillation postgres pool",
						"error", cerr)
				}
			})
			log.Info("bootstrap: experience distillation wired",
				"embedding_model", cfg.Embedding.Model)
		}
	}
	return guidanceProvider, embClient
}

// subscribeDistillationEvents starts the background distillation loop that
// turns task-completed/failed events into experiences (experience
// distillation) and, when the AKG DistillBridge is wired, into AKG
// knowledge facts (write side of the AKG loop). It is a no-op when the
// experience distillation service or the event store is unavailable.
func subscribeDistillationEvents(ctx context.Context, comp *Components) {
	if comp.Distillation == nil || comp.EventStore == nil {
		return
	}
	comp.bgGroup.Go(func() error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ch, err := comp.EventStore.Subscribe(ctx, ares_events.EventFilter{
			Types: []ares_events.EventType{
				ares_events.EventTaskCompleted,
				ares_events.EventTaskFailed,
			},
		})
		if err != nil {
			log.Warn("bootstrap: distillation event subscription failed", "error", err)
			return nil
		}
		// akgEg runs AKG distillations off the subscriber loop so a slow
		// bridge call (LLM/embedding) cannot block experience distillation.
		akgEg, akgCtx := errgroup.WithContext(ctx)
		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					// Channel closed: stop the loop and join in-flight AKG
					// distillations so no goroutine is abandoned on shutdown.
					// Each distillation is bounded by akgBridgeTimeout (30s),
					// and cancel() makes them return promptly.
					cancel()
					if waitErr := akgEg.Wait(); waitErr != nil {
						log.Warn("bootstrap: AKG distillation group error during shutdown", "error", waitErr)
					}
					return nil
				}
				HandleTaskCompletedForDistillation(ctx, comp.Distillation, ev)
				if comp.AKGBridge != nil {
					triggerAKGBridge(akgCtx, akgEg, ev, comp.AKGBridge)
				}
			case <-ctx.Done():
				// Context cancelled: join in-flight AKG distillations before
				// exiting so the subscriber goroutine does not leak them.
				if waitErr := akgEg.Wait(); waitErr != nil {
					log.Warn("bootstrap: AKG distillation group error during shutdown", "error", waitErr)
				}
				return nil
			}
		}
	})
}

// Parameter keys used in evolution strategy configurations.
const (
	paramTemperature = "temperature"
	paramMaxTokens   = "max_tokens"
)

// maxShadowReplayHorizon is the widest total replay history (window span ×
// MinSamples) the shadow sampler is expected to walk backwards before the
// oldest comparison stops describing current behaviour. Purely advisory: the
// bootstrap warns above it and never clamps, because a low-traffic deployment
// may legitimately need a longer horizon to find any evidence at all.
const maxShadowReplayHorizon = 24 * time.Hour

// wireGAEvolution wires the GA population adapter (step 9 of Bootstrap): it
// builds the GA system, attaches the coordinator bridge to the population
// adapter, and starts the background evolution ticker. Extracted from Bootstrap
// to keep its cyclomatic complexity within lint limits.
//
//nolint:gocyclo // it is a complex wiring hub with many config fields.
func wireGAEvolution(ctx context.Context, cfg *ares_config.Config, comp *Components, newEvol *NewEvolutionComponents, guidanceProvider evolution.GuidanceProvider) error {
	// Create a persistent strategy store when PostgreSQL is configured,
	// falling back to the in-memory store when no database is available.
	// The PG store ensures evolution results survive process restarts.
	var memStore evolution.StrategyStore
	if cfg.Storage.Enabled && cfg.Storage.Type == storageTypePostgres && cfg.Storage.Host != "" {
		pgStore, err := newPGStrategyStore(cfg)
		if err != nil {
			log.WarnContext(ctx, "bootstrap: PG strategy store init failed, falling back to in-memory", "error", err)
			memStore = evolution.NewMemoryStrategyStore(0)
		} else {
			memStore = pgStore
			log.InfoContext(ctx, "bootstrap: PG strategy store wired (persistent)")
		}
	} else {
		memStore = evolution.NewMemoryStrategyStore(0)
	}
	newEvol.StrategyStore = memStore

	// Close the "evolution context in the knowledge graph" loop (#9): stream
	// active/historical strategies as decision-type knowledge objects so
	// server-side evolution queries can retrieve strategy decisions. The
	// StrategyStore only exists from this point on (the knowledge runtime is
	// built earlier, in BuildKnowledgeRuntime), hence the late registration.
	attachEvolutionKnowledgeProvider(ctx, comp.KnowledgeRuntime, memStore, comp.EvidenceStore)

	base := &mutation.Strategy{
		ID:     "bootstrap-root",
		Params: map[string]any{paramTemperature: 0.7, paramMaxTokens: 4096},
	}
	gaCfg := evolution.DefaultSystemConfig()
	gaCfg.EnableDreamCycle = false
	// B4 fix: EnableScheduler is now always true so the ticker path goes
	// through scheduler.Tick (which applies shouldEvolve + guardrails).
	// When the legacy scheduler exists, SetAdapter is still called so it
	// can drive event-triggered evolution, but the ticker no longer
	// bypasses the scheduler's throttling.
	gaCfg.EnableScheduler = true
	gaCfg.EventStore = comp.EventStore
	gaCfg.StrategyStore = memStore
	// B1 fix: rollback thresholds come from the evolution.rollback YAML
	// block (design doc §7); zero values fall back to the code defaults
	// that match the previous hardcoded wiring.
	// E2: Enabled is now tri-state (nil defaults true) instead of hardcoded:
	// an operator can disarm the rollback net, which (a) stops the watch loop
	// from triggering and (b) re-arms the G2 gate fail-closed — see
	// shadowGateMode below.
	rbCfg := cfg.Evolution.Rollback
	rollbackArmed := rbCfg.IsEnabled()
	gaCfg.RollbackPolicyConfig = evolution.RollbackPolicyConfig{
		Enabled:              rollbackArmed,
		DegradationThreshold: defaultFloat(rbCfg.DegradationThreshold, 0.15),
		WindowSize:           defaultInt(rbCfg.WindowSize, 5),
		MinSamples:           defaultInt(rbCfg.MinSamples, 3),
	}
	// B3 fix: enable shadow evaluation independently of DreamCycle.
	// Thresholds come from the evolution.shadow YAML block (design doc §7).
	shCfg := cfg.Evolution.Shadow
	gaCfg.ShadowEvalConfig = evolution.ShadowEvaluationConfig{
		Enabled:    true,
		MinSamples: defaultInt(shCfg.MinSamples, 20),
		MinWinRate: defaultFloat(shCfg.MinWinRate, 0.55),
	}
	// W2: the replay evidence window width is YAML-configurable (duration
	// string). Zero/unset keeps the scorer's 10-minute default; an invalid
	// string is ignored, never fatal — the operator's typo must not take down
	// the evolution plane.
	//
	// The per-window query limit (evolution.shadow.replay_query_limit) is NOT
	// carried here: the ReplayScorer is constructed in the serve layer
	// (cmd/ares/peer_mode.go), which reads the YAML directly. Duplicating it
	// into ShadowEvalConfig would create a second, unread copy of the same
	// knob (review P1).
	if raw := shCfg.ReplayWindowSpan; raw != "" {
		if d, perr := time.ParseDuration(raw); perr == nil && d > 0 {
			gaCfg.ShadowEvalConfig.ReplayWindowSpan = d
			// P2: the sampler walks MinSamples windows BACKWARDS from now, so
			// span × MinSamples is the total history horizon. A wide span with
			// the default query limit (200 records/window, no server-side
			// strategy filter) makes truncation likely and pushes the oldest
			// window far out of date — stale evidence judged as current. Warn
			// rather than clamp: the operator may genuinely want a long
			// horizon on a low-traffic deployment.
			if horizon := d * time.Duration(gaCfg.ShadowEvalConfig.MinSamples); horizon > maxShadowReplayHorizon {
				log.WarnContext(ctx, "bootstrap: evolution.shadow replay horizon is very wide — oldest comparison reads stale evidence and wide windows are more likely to hit replay_query_limit",
					"replay_window_span", d, "min_samples", gaCfg.ShadowEvalConfig.MinSamples,
					"horizon", horizon, "recommended_max", maxShadowReplayHorizon)
			}
		} else {
			log.WarnContext(ctx, "bootstrap: ignoring invalid evolution.shadow.replay_window_span", "value", raw, "error", perr)
		}
	}
	// Design doc §7: the lifecycle control plane (window/judge/gates/watch
	// interval) is YAML-configurable; the same config also feeds the G3
	// eval-gate MinScore further below.
	gaCfg.Lifecycle = lifecycleConfigFromYAML(cfg.Evolution.Lifecycle, cfg.Evolution.Gates, cfg.Evolution.ChannelFeedback)
	// E2: the lifecycle must know whether the rollback net is armed (it owns
	// the watch loop) — the same tri-state decision as the ASM wiring above.
	gaCfg.Lifecycle.RollbackArmed = rollbackArmed
	// P2-1: wire the shared Prometheus metrics into the GA system so the
	// lifecycle counters (promote/rollback/gate-reject) are actually
	// incremented in production instead of registered-but-never-updated.
	// NewPrometheusMetrics is idempotent (AlreadyRegisteredError returns the
	// cached instance created by provide_llm), so this reuses the same
	// collector the /metrics endpoint serves.
	if m, merr := ares_observability.NewPrometheusMetrics(); merr == nil {
		gaCfg.Metrics = m
	} else {
		log.WarnContext(ctx, "bootstrap: evolution metrics wiring skipped", "error", merr)
	}

	// W3: honor the YAML evolution tuning. Only fields with a matching
	// SystemConfig slot are wired; the rest of the YAML GA knobs
	// (TournamentSize/CrossoverType/SteadyState*) are registered as dead
	// config pending a SystemConfig slot.
	ec := &cfg.Evolution
	applyGATuning(&gaCfg, ec)
	// B2 (G1): construct the guardrails. Until now gaCfg.Guardrails was NEVER
	// assigned anywhere in this package, so WithAdapterGuardrails was skipped
	// and both runPreGuardrails and the legacy scheduler's checkGuardrails
	// short-circuited on nil — G1 was a gate that existed in code and did
	// nothing at runtime. The design doc's §5 known gap 1 understated this: it
	// described the adapter layer as G1's "only real defense", but the adapter
	// layer was inert too.
	gaCfg.Guardrails = buildEvolutionGuardrails(ctx, ec, gaCfg.Metrics)
	// Track A closure: feed distilled experiences back into the GA's
	// experience-guided mutation. guidanceProvider is non-nil only when
	// distillation was successfully wired above (PG + embedding configured).
	gaCfg.GuidanceProvider = guidanceProvider
	gaCfg.EnableExperienceGuidedMutation = guidanceProvider != nil

	// Track B closure: opt-in LLM-backed scorer. When enabled and an LLM
	// client is available, override the default constant baseline scorer
	// with the LLM scorer + deterministic heuristic fallback. When disabled
	// (the default), gaCfg.Scorer stays nil and buildAdapterOptions falls
	// back to ConstantScorer(50.0), preserving prior behavior.
	llmScorer, llmHeuristic, llmMaxCalls := wireLLMScorer(cfg, comp)
	if llmScorer != nil {
		gaCfg.Scorer = llmScorer
		gaCfg.HeuristicScorer = llmHeuristic
		if llmMaxCalls > 0 {
			gaCfg.MaxLLMCallsPerGeneration = llmMaxCalls
		}
		// A fixed seed forces the LLM to temperature 0 + prompt-embedded seed
		// → deterministic output. The sampler's comparisons are then identical
		// (MinSamples satisfied by repetition, not by independent evidence).
		// buildShadowEvaluator logs a warning when this is set.
		if cfg.Evolution.LLMScoring.Seed > 0 {
			gaCfg.ShadowEvalConfig.DeterministicScorer = true
		}
	}

	// C2.6: when LLM scoring is off (the default), the zero-LLM deterministic
	// scorer takes over as the independent evidence source. The scorer is
	// wired at runtime by the serve layer (peer_mode.go) once the
	// ExecutionAttribution is created, but the shadow gate posture must be
	// decided HERE (before NewWiredEvolutionSystem). Setting
	// DeterministicScorerEnabled=true makes hasScorer pass without an LLM,
	// so the G2 gate stays registered and can produce shadow comparison
	// evidence from execution attribution alone.
	if llmScorer == nil {
		gaCfg.DeterministicScorerEnabled = true
	}

	// E2: decide the G2 shadow-gate posture BEFORE wiring the system, so the
	// lifecycle is constructed with the gate already suppressed (or kept)
	// instead of un-registering it afterwards. The invariant: skipping
	// PRE-deployment verification is allowed only when POST-deployment
	// verification is armed; with neither, G2 stays fail-closed.
	// hasScorer ⇔ gaCfg.Scorer != nil: buildShadowEvaluator sets an
	// independent scorer on the evaluator exactly when cfg.Scorer is wired
	// (the heuristic-only TieredScorer is NOT independent evidence).
	// C2.6: when a deterministic (zero-LLM) scorer is wired, it counts as
	// independent evidence too — the attribution-derived score is a
	// legitimate comparison source. This breaks the "zero-token ⇒ no G2"
	// deadlock: with DeterministicScorerEnabled, the G2 gate stays
	// registered and produces shadow comparison evidence from execution
	// attribution alone, without any LLM call.
	hasScorer := gaCfg.Scorer != nil || gaCfg.DeterministicScorerEnabled
	register, gateReason, gateErr := shadowGateMode(hasScorer, rollbackArmed)
	if !register && errors.Is(gateErr, errShadowGateNotConfigured) {
		gaCfg.Lifecycle.DisableShadowGate = true
		gaCfg.Lifecycle.ShadowGateSkipReason = gateReason
		log.WarnContext(ctx, "evolution: pre-deployment shadow gate NOT registered",
			"reason", gateReason,
			"mitigation", "candidates promote directly; degradation triggers automatic rollback",
			"rollback_armed", rollbackArmed,
			"rollback_threshold", gaCfg.RollbackPolicyConfig.DegradationThreshold,
			"rollback_window", gaCfg.RollbackPolicyConfig.WindowSize,
			"rollback_min_samples", gaCfg.RollbackPolicyConfig.MinSamples,
		)
		// E6: the absence must be meterable, not only logged once.
		if gaCfg.Metrics != nil {
			gaCfg.Metrics.RecordEvolutionGateSkipped("shadow", gateReason)
		}
	}

	wired, wErr := evolution.NewWiredEvolutionSystem(base, gaCfg)
	if wErr != nil {
		return fmt.Errorf("wire GA population adapter: %w", wErr)
	}

	// Live-chaos GA quiet-window probe (#12 Phase 2): expose whether a generation
	// is mid-flight so serve's chaos loop can defer injections.
	newEvol.GAGenerationActive = wired.GenerationActive

	// Attach the coordinator bridge to the population adapter.
	popAdapter := wired.PopAdapter
	evolution.WithAdapterCoordinator(
		newEvol.Coordinator,
		newEvol.DiffReg,
		newEvol.GenomeReg,
	)(popAdapter)

	// P1-2 (B5 fix): wire the G3 eval-suite gate so independently-built
	// evaluators participate in the promote/rollback decision instead of
	// sitting idle. The gate is built ONLY when a regression suite is
	// configured (evolution.gates.eval_suite file path); otherwise NO gate
	// is registered — honest absence, not a permanent pass-through pretending
	// to be verification (review item B.2).
	if wired.Lifecycle != nil && comp.Evolution != nil {
		// MinScore flows from the evolution.gates.eval_min_score YAML knob
		// via gaCfg.Lifecycle (design doc §7); 0 falls back to the gate's
		// own 0.7 default.
		var minScore float64
		if wiredLifecycleCfg := gaCfg.Lifecycle; wiredLifecycleCfg != nil {
			minScore = wiredLifecycleCfg.Gates.EvalMinScore
		}
		suitePath := ""
		strict := false
		if gaCfg.Lifecycle != nil {
			suitePath = cfg.Evolution.Gates.EvalSuite
			strict = cfg.Evolution.Gates.EvalStrict
		}
		evalGate, gerr := buildEvalGate(
			comp.Evolution.EvaluatorRegistry,
			comp.Evolution.EvalLLMClient,
			suitePath,
			minScore,
			strict,
		)
		if gerr != nil && !errors.Is(gerr, errEvalGateNotConfigured) {
			// A CONFIGURED but broken suite fails bootstrap (fail closed);
			// an intentionally absent gate just skips G3.
			return gerr
		}
		if evalGate != nil {
			evolution.WithLifecycleGates(evalGate)(wired.Lifecycle)
			log.InfoContext(ctx, "bootstrap: G3 eval gate wired",
				"suite", suitePath, "min_score", minScore, "strict_mode", strict,
				"skipped_count", evalGate.SkippedCount())
		}
	}

	// Wire the lifecycle's evidence store and start its watch loop so
	// rollback detection runs against real runtime evidence (B1 fix).
	if wired.Lifecycle != nil {
		evolution.WithLifecycleEvidenceStore(newEvol.EvidenceStore)(wired.Lifecycle)
		wired.Lifecycle.Start(ctx)
		// K3 / release-plan T11: the watch goroutine must not outlive
		// bootstrap — stop it (and wait) when the bootstrap context ends.
		comp.bgGroup.Go(func() error {
			<-ctx.Done()
			wired.Lifecycle.Stop()
			return nil
		})
		// P2-2: expose the lifecycle for the introspect control plane
		// so /api/evolution/lifecycle returns a state snapshot.
		newEvol.Lifecycle = wired.Lifecycle
		// §8 closure-assertion surfaces: the ASM (Previous/RollbackPolicy)
		// and the G2 shadow evaluator (the comparison feeder).
		newEvol.ActiveStrategyManager = wired.ActiveStrategyManager
		newEvol.ShadowEvaluator = wired.ShadowEvaluator
	}

	// P2-3: wire the RuntimeObserver — the OBSERVE stage of the evolution
	// control plane. It converts task completed/failed events into
	// normalized [0,1] strategy samples and writes KindFitness evidence
	// (source "strategy"). Without it that source is empty, so the B1
	// rollback watch loop's Window() never reaches ok=true and the B6
	// staging score has no runtime fitness to read — the whole feedback
	// chain starves regardless of how well the decision side is wired.
	if comp.EventStore != nil && newEvol.EvidenceStore != nil {
		obsOpts := []evolution.ObserverOption{
			evolution.WithObserverEvidenceStore(newEvol.EvidenceStore),
		}
		if wired.ActiveStrategyManager != nil {
			obsOpts = append(obsOpts, evolution.WithObserverActiveIDFunc(activeStrategyIDFunc(wired.ActiveStrategyManager)))
		}
		observer := evolution.NewRuntimeObserver(comp.EventStore, obsOpts...)
		if err := observer.Start(ctx); err != nil {
			log.WarnContext(ctx, "bootstrap: runtime observer start failed", "error", err)
		} else {
			comp.bgGroup.Go(func() error {
				<-ctx.Done()
				observer.Stop()
				return nil
			})
		}
	}

	// Step Y.2/Y.3: the OBSERVE stage for the other two perception channels.
	// The recorder is only built when a channel is armed AND the strategy is
	// attributable — a recorder with no ASM would drop every record as
	// unattributable, i.e. dead wiring that looks live.
	newEvol.ChannelFeedback = startChannelFeedback(ctx, comp, newEvol, wired.ActiveStrategyManager, cfg.Evolution.ChannelFeedback)

	// M4-D0: the ToolStep projection worker is deleted with its package (default-disabled, zero production readers of
	// WindowToolStep). The per-(tool#arg_shape) fitness dimension is
	// superseded by the L1 ToolClass graph (M5) fed from L2 execution stats
	// (M6) — same key shape (toolName#argShape), live source instead of a
	// batch-projected one.
	// In the full configuration, attach the GA adapter to the existing
	// old-system scheduler; otherwise the GA system's own scheduler
	// (registered above on the LLM callback registry) drives it.
	//
	// B4 fix: remember which scheduler the background ticker will drive.
	// Two instances exist: the LEGACY one (created+Registered in
	// provide_evolution, so its score window receives task events) and the
	// wired one (created in NewWiredEvolutionSystem WITHOUT Register — its
	// score window is forever empty, making Tick's shouldEvolve a permanent
	// no-op, silently disabling ticker-triggered evolution). The ticker
	// prefers the legacy scheduler; only when it does not exist does it fall
	// back to the wired one, which must then be Registered here so Tick sees
	// real scores.
	var legacySched *evolution.EvolutionScheduler
	if comp.Evolution != nil {
		legacySched, _ = comp.Evolution.Scheduler.(*evolution.EvolutionScheduler)
	}
	if legacySched != nil {
		legacySched.SetAdapter(popAdapter)
	} else if wired.Scheduler != nil && comp.EventStore != nil {
		wired.Scheduler.Register()
		// B4: Register subscribes on its own context.Background() and parks a
		// goroutine on the event channel; without a matching Shutdown that
		// goroutine (and the EventStore subscriber feeding it) outlives the
		// bootstrap for the life of the process. goleak found this; a
		// goroutine count could not have.
		wiredSched := wired.Scheduler
		comp.bgGroup.Go(func() error {
			<-ctx.Done()
			wiredSched.Shutdown()
			return nil
		})
	}

	// Start a background ticker that triggers evolution via the unified
	// scheduler.Tick path (B4 fix). This replaces the old unconditional
	// popAdapter.Run(ctx) call so evolution timing is always gated by
	// shouldEvolve + checkGuardrails.
	comp.bgGroup.Go(func() error {
		// W3: honor evolution.min_interval from yaml (audit: the 5-minute
		// ticker was hardcoded, leaving MinInterval dead config).
		tick := 5 * time.Minute
		if raw := cfg.Evolution.MinInterval; raw != "" {
			if d, perr := time.ParseDuration(raw); perr == nil && d > 0 {
				tick = d
			}
		}
		evoTicker := time.NewTicker(tick)
		defer evoTicker.Stop()
		for {
			select {
			case <-evoTicker.C:
				// B4 fix: route through a scheduler Tick that actually sees
				// scores, so shouldEvolve + guardrails + MinInterval are
				// always applied. legacySched is preferred (it is Registered
				// and therefore receives score events); the wired scheduler
				// is Registered above when it is the only one. When neither
				// exists (no EventStore), keep the old unconditional Run so
				// minimal configs still evolve.
				switch {
				case legacySched != nil:
					legacySched.Tick(ctx)
				case wired.Scheduler != nil:
					wired.Scheduler.Tick(ctx)
				default:
					if err := popAdapter.Run(ctx); err != nil {
						log.WarnContext(ctx, "[bootstrap] ticker-triggered evolution failed",
							"error", err)
						continue
					}
				}
				// Record the generation trajectory into the shared tracer
				// (v0.3.0 M3-1) so /evolution/trajectory returns live data
				// instead of an empty list. wired.Population exposes the
				// per-generation Stats after a run.
				if comp.Observability != nil && comp.Observability.EvolutionTracer != nil && wired.Population != nil {
					stats := wired.Population.Stats()
					comp.Observability.EvolutionTracer.Record(stats.Generation, stats.BestScore, nil, nil)
				}
			case <-ctx.Done():
				return nil
			}
		}
	})

	// Wire the LLMAdapter into the Coordinator's suggestion pipeline.
	// When an LLM client is available, periodically generate and submit
	// evolution suggestions (LLM → Parse → PatchProposal → Coordinator.Evaluate).
	if newEvol.LLMAdapter != nil && comp.LLM != nil && comp.LLM.Client != nil {
		if llmClient, ok := comp.LLM.Client.(evoService.LLMClient); ok {
			comp.bgGroup.Go(func() error {
				suggestTicker := time.NewTicker(15 * time.Minute)
				defer suggestTicker.Stop()
				for {
					select {
					case <-suggestTicker.C:
						// Generate a suggestion prompt for the LLM based on
						// current evolution state and recent evidence.
						prompt := buildEvolutionSuggestionPrompt(ctx,
							newEvol.EvidenceStore, newEvol.StrategyStore)
						resp, err := llmClient.Generate(ctx, prompt)
						if err != nil {
							log.WarnContext(ctx, "[bootstrap] LLM suggestion generation failed",
								"error", err)
							continue
						}
						results, parseErr := newEvol.LLMAdapter.Parse(ctx, resp)
						if parseErr != nil {
							// Parsing failures are expected when the LLM response
							// doesn't match any known pattern — log and skip.
							log.DebugContext(ctx, "[bootstrap] LLM suggestion parse skipped",
								"error", parseErr)
							continue
						}
						for _, r := range results {
							newEvol.Coordinator.Submit(r.Proposal)
						}
						newEvol.Coordinator.Evaluate(ctx)
					case <-ctx.Done():
						return nil
					}
				}
			})
			log.InfoContext(ctx, "[bootstrap] LLM suggestion pipeline wired into Coordinator")
		}
	}
	return nil
}

// wireLLMScorer constructs the opt-in LLM-backed scorer for the GA evolution
// system (Track B from the closure plan). It returns non-nil scorer functions
// only when all of the following hold:
//   - cfg.Evolution.LLMScoring.Enabled is true,
//   - comp.LLM and comp.LLM.Client are non-nil,
//   - comp.LLM.Client satisfies the evoService.LLMClient interface,
//   - evoService.NewLLMScorer succeeds.
//
// On any failure (disabled, missing client, type mismatch, construction
// error), the function logs a warning and returns nil scorers with a zero
// budget. The caller then leaves gaCfg.Scorer unset, causing
// buildAdapterOptions to fall back to ConstantScorer(50.0). This keeps
// scoring best-effort: bootstrap never fails due to scorer wiring.
// applyGATuning copies operator tuning from the YAML evolution section onto the
// GA engine config, so operators can tune the GA without touching code
// (previously these fields were dead config).
//
// A field overrides the engine default only when it differs from the
// config-layer default (ares_config.DefaultEvolution*): cfg arrives from
// LoadConfig with setDefaults already applied, so every field is non-zero and a
// plain `> 0` guard would always fire, silently replacing the GA engine's own
// tuned defaults from DefaultSystemConfig (e.g. EliteCount 3 with the
// config-layer default 2). The `> 0` half of each guard keeps programmatic
// configs that skip setDefaults from clobbering the engine with zero values.
//
// Known tradeoff: an operator who explicitly sets a field equal to the
// config-layer default is indistinguishable from an unset field and keeps the
// GA engine default (locked by TestApplyGATuningExplicitDefaultValues).
//
// Args:
//
//	gaCfg - the GA engine config to mutate; starts from DefaultSystemConfig.
//	ec    - the YAML evolution section of the loaded config.
func applyGATuning(gaCfg *evolution.SystemConfig, ec *ares_config.EvolutionConfig) {
	if ec.PopulationSize > 0 && ec.PopulationSize != ares_config.DefaultEvolutionPopulationSize {
		gaCfg.PopulationSize = ec.PopulationSize
	}
	if ec.EliteCount > 0 && ec.EliteCount != ares_config.DefaultEvolutionEliteCount {
		gaCfg.EliteCount = ec.EliteCount
	}
	if ec.SurvivalRate > 0 && ec.SurvivalRate != ares_config.DefaultEvolutionSurvivalRate {
		gaCfg.SurvivalRate = ec.SurvivalRate
	}
	if ec.MutationRate > 0 && ec.MutationRate != ares_config.DefaultEvolutionMutationRate {
		gaCfg.MutationRate = ec.MutationRate
	}
	if ec.MinMutationRate > 0 && ec.MinMutationRate != ares_config.DefaultEvolutionMinMutationRate {
		gaCfg.MinMutationRate = ec.MinMutationRate
	}
	if ec.MaxMutationRate > 0 && ec.MaxMutationRate != ares_config.DefaultEvolutionMaxMutationRate {
		gaCfg.MaxMutationRate = ec.MaxMutationRate
	}
	if ec.BreedingPoolRatio > 0 && ec.BreedingPoolRatio != ares_config.DefaultEvolutionBreedingPoolRatio {
		gaCfg.BreedingPoolRatio = ec.BreedingPoolRatio
	}
	if ec.SelectionStrategy != "" && ec.SelectionStrategy != ares_config.DefaultEvolutionSelectionStrategy {
		gaCfg.SelectionStrategy = ec.SelectionStrategy
	}
	// ToolPool: wire the deployment-configured tool-whitelist pool into the
	// mutator (single source for tool mutation vocabulary). Empty keeps tool
	// mutation disabled (guided mutation may still produce choices from hints).
	if len(ec.ToolPool) > 0 {
		gaCfg.ToolPool = ec.ToolPool
	}
}

// targetFitnessScale converts the YAML evolution.target_fitness (documented as
// a 0-100 scale in ares_config) to the [0,1] scale EvolutionGuardrails compares
// against, since its BaselineScore is measured against the same values fed to
// PostEvolveCheckForSource.
const targetFitnessScale = 100.0

// findUnknownPoolTools cross-checks the deployment-configured mutation pool
// (evolution.tool_pool, each entry a comma-separated whitelist) against the
// registered-tool vocabulary (evolution.guardrails.known_tools). It returns
// the offending entries mapped to their unknown names, or nil when everything
// resolves (or when either side is empty — no vocabulary means no judgment,
// same opt-in rule as the unknown-name guard itself).
//
// Parsing goes through agents.ToolNamesFromParams — the same single parser the
// executors and the guardrail use — so "a,a," counts as one name here exactly
// as it does at selection and execution time.
func findUnknownPoolTools(pool, known []string) map[string][]string {
	if len(pool) == 0 || len(known) == 0 {
		return nil
	}
	knownSet := make(map[string]bool, len(known))
	for _, k := range known {
		if k = strings.TrimSpace(k); k != "" {
			knownSet[k] = true
		}
	}
	if len(knownSet) == 0 {
		return nil
	}
	var bad map[string][]string
	for _, entry := range pool {
		names := agents.ToolNamesFromParams(map[string]any{agents.ParamKeyTools: entry})
		var unknown []string
		for _, name := range names {
			if !knownSet[name] {
				unknown = append(unknown, name)
			}
		}
		if len(unknown) > 0 {
			if bad == nil {
				bad = make(map[string][]string)
			}
			bad[entry] = unknown
		}
	}
	return bad
}

// buildEvolutionGuardrails constructs the G1 population-level guardrails from
// the YAML evolution section (B2).
//
// Before B2 this construction did not exist: gaCfg.Guardrails was never
// assigned, so GenomePopulationAdapter.runPreGuardrails / runPostGuardrails and
// EvolutionScheduler.checkGuardrails all short-circuited on nil and passed
// unconditionally. G1 was structurally present and operationally absent.
//
// Two YAML knobs that were previously dead config now drive it:
//   - Generations → MaxStagnantGenerations. Semantics line up: both bound "how
//     many generations may pass without progress".
//   - TargetFitness (0-100) → BaselineScore, rescaled to [0,1]. Left unset when
//     zero so the guardrail keeps its adaptive behavior (baseline = the best
//     score seen so far for that source).
//
// MaxLineageShare keeps the constructor default (0.8); no YAML key exists for
// it and inventing one would create fresh dead config.
//
// IMPORTANT — each caller must own its OWN instance, never share one.
// EvolutionGuardrails carries mutable state (stagnantCount, bestBySource) and
// the two driving paths (legacy scheduler ticker vs adapter population layer)
// run on different generation counters and score scales. Sharing an instance
// would let one path's stagnation count and baseline pollute the other's. The
// bestBySource source-keying inside guardrails.go exists for the same reason.
//
// Args:
//   - ctx: for the degradation log only.
//   - ec: the YAML evolution section (nil returns nil).
//   - metrics: optional Prometheus sink; when non-nil every guardrail event
//     increments ARES_evolution_guardrail_total{code}.
//
// Returns:
//   - *evolution.EvolutionGuardrails: nil on construction failure, which
//     degrades to the pre-B2 behavior (all checks pass) rather than blocking
//     bootstrap.
func buildEvolutionGuardrails(
	ctx context.Context,
	ec *ares_config.EvolutionConfig,
	metrics *ares_observability.PrometheusMetrics,
) *evolution.EvolutionGuardrails {
	if ec == nil {
		return nil
	}
	opts := []evolution.GuardrailOption{
		evolution.WithMaxStagnantGenerations(
			defaultInt(ec.Generations, ares_config.DefaultEvolutionGenerations),
		),
	}
	if ec.TargetFitness > 0 {
		opts = append(opts, evolution.WithBaselineScore(ec.TargetFitness/targetFitnessScale))
	}
	// C6: tool-set selection guardrails from the evolution.guardrails YAML block.
	// All three are opt-in — zero-value disables, preserving prior behavior.
	if gr := ec.Guardrails; gr.MaxToolsEnabled > 0 {
		opts = append(opts, evolution.WithMaxToolsEnabled(gr.MaxToolsEnabled))
	}
	if ec.Guardrails.RequireAnyTool {
		opts = append(opts, evolution.WithRequireAnyTool(true))
	}
	if len(ec.Guardrails.KnownTools) > 0 {
		opts = append(opts, evolution.WithKnownTools(ec.Guardrails.KnownTools))
	}
	// P2b: loud misconfiguration check — a tool_pool entry naming tools
	// outside the known vocabulary silently jails every candidate it produces
	// (unknown-name guard), so the generation burns with no promotion and
	// evolution looks stalled rather than misconfigured. Warn, don't
	// fail-closed: the pool only feeds the elite/random mutation path
	// (guided mutation still works), and blocking bootstrap on a soft
	// evolution knob would violate graceful degradation.
	for entry, unknown := range findUnknownPoolTools(ec.ToolPool, ec.Guardrails.KnownTools) {
		log.WarnContext(ctx, "bootstrap: evolution tool_pool entry names unregistered tools; candidates from this entry will be jailed by the tool-set guardrail",
			"entry", entry, "unknown", unknown, "known_count", len(ec.Guardrails.KnownTools))
	}
	if metrics != nil {
		opts = append(opts, evolution.WithGuardrailEventHandler(func(evt evolution.GuardrailEvent) {
			metrics.RecordEvolutionGuardrail(string(evt.ErrorCode))
		}))
	}
	g, err := evolution.NewEvolutionGuardrails(opts...)
	if err != nil {
		// C3.1: construction failure is fail-closed, not silent pass-through.
		// Previously this returned nil, which caused checkGuardrails to
		// short-circuit to true (all checks pass) — a guardrail that does
		// not exist cannot block anything. Now we return a guardrail that
		// always blocks (ShouldStop=true) so a broken G1 cannot silently
		// let bad candidates through.
		log.ErrorContext(ctx, "bootstrap: evolution guardrails construction failed, using fail-closed guardrail",
			"error", err)
		return evolution.NewFailClosedGuardrails()
	}
	return g
}

func wireLLMScorer(cfg *ares_config.Config, comp *Components) (genome.ScorerFunc, genome.ScorerFunc, int) {
	if cfg == nil || !cfg.Evolution.LLMScoring.Enabled {
		return nil, nil, 0
	}

	if comp == nil || comp.LLM == nil || comp.LLM.Client == nil {
		log.Warn("bootstrap: LLM scoring enabled but LLM client is nil, falling back to baseline scorer")
		return nil, nil, 0
	}

	llmClient, ok := comp.LLM.Client.(evoService.LLMClient)
	if !ok {
		log.Warn("bootstrap: LLM client does not satisfy LLMClient interface, falling back to baseline scorer",
			"client_type", fmt.Sprintf("%T", comp.LLM.Client))
		return nil, nil, 0
	}

	llmScorer, err := evoService.NewLLMScorer(evoService.LLMScorerConfig{
		Client:   llmClient,
		Seed:     cfg.Evolution.LLMScoring.Seed,
		Fallback: evoService.DeterministicScore,
	})
	if err != nil {
		log.Warn("bootstrap: failed to create LLM scorer, falling back to baseline scorer", "error", err)
		return nil, nil, 0
	}

	llmScorerFn := llmScorer.AsScorerFunc()
	scorer := genome.ScorerFunc(func(agent *mutation.Strategy) float64 {
		return llmScorerFn(evoService.ToAPIStrategy(agent))
	})
	heuristic := genome.ScorerFunc(func(agent *mutation.Strategy) float64 {
		return evoService.DeterministicScore(evoService.ToAPIStrategy(agent))
	})

	log.Info("bootstrap: LLM-backed scorer wired into GA evolution",
		"seed", cfg.Evolution.LLMScoring.Seed,
		"max_calls_per_generation", cfg.Evolution.LLMScoring.MaxCallsPerGeneration)

	return scorer, heuristic, cfg.Evolution.LLMScoring.MaxCallsPerGeneration
}

// attachEvolutionKnowledgeProvider registers the evolution StrategyStore as a
// knowledge graph provider on rt (best-effort: nil rt or a registration
// failure degrades to a warn log, never blocks bootstrap). Kept as its own
// function so the closure contract is directly testable without the full
// wireGAEvolution fixture.
//
// E3: the evidence store (comp.EvidenceStore — wired long before this point,
// so the call-site timing is unaffected) lets the provider also stream the
// promote/rollback decision trail, closing the "P2-3 wrote evidence nobody
// consumed" gap. A nil evStore degrades to lineage-only output.
func attachEvolutionKnowledgeProvider(
	ctx context.Context,
	rt *knowledgeruntime.KnowledgeRuntime,
	store evolution.StrategyStore,
	evStore evidence.Store,
) {
	if rt == nil || store == nil {
		return
	}
	evoProv := evoprovider.New("evolution", store)
	if evStore != nil {
		evoProv.WithEvidenceStore(evStore)
	}
	if err := rt.RegisterProvider(evoProv); err != nil {
		log.WarnContext(ctx, "bootstrap: register evolution provider for knowledge runtime", "error", err)
		return
	}
	log.InfoContext(ctx, "bootstrap: evolution provider wired for knowledge runtime",
		"decision_trail", evStore != nil)
}

// newPGStrategyStore creates a PostgreSQL-backed strategy store from config.
// Returns nil when the database connection cannot be established, so callers
// can fall back to the in-memory store gracefully.
func newPGStrategyStore(cfg *ares_config.Config) (evolution.StrategyStore, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Storage.Host, cfg.Storage.Port, cfg.Storage.Username,
		cfg.Storage.Password, cfg.Storage.Database, cfg.Storage.SSLMode)
	db, err := sql.Open(storageTypePostgres, dsn)
	if err != nil {
		return nil, fmt.Errorf("pg strategy store: open db: %w", err)
	}
	// Verify the connection is alive.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Warn("pg strategy store: close db after ping failure", "error", closeErr)
		}
		return nil, fmt.Errorf("pg strategy store: ping: %w", err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	store, err := evolution.NewPGStrategyStore(db, "evolution_strategies", 100)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Warn("pg strategy store: close db after init failure", "error", closeErr)
		}
		return nil, fmt.Errorf("pg strategy store: init: %w", err)
	}
	return store, nil
}

// fitnessSourceKnowledge is the AKG genome source name used in fitness
// evidence summaries (shared with the knowledge runtime vector provider).
const fitnessSourceKnowledge = "knowledge"

// fitnessSourceMemory is the memory genome source name used in fitness
// evidence summaries (shared with the memory retriever emitter).
const fitnessSourceMemory = "memory"

// fitnessSourceOrder is the stable ordering of GA genome sources whose recent
// fitness evidence is summarized into the LLM suggestion prompt.
var fitnessSourceOrder = []string{"workflow", "scheduler", "recovery", fitnessSourceMemory, fitnessSourceKnowledge}

// buildEvolutionSuggestionPrompt builds an LLM suggestion prompt grounded in
// the current evolution state: the mean fitness value of the most recent
// evidence per genome source plus the currently deployed strategy. When no
// evidence or strategy exists yet, it falls back to the generic prompt so the
// LLM still has the instruction it needs. Returns the prompt string.
//
// The summary makes the LLM's suggestions state-aware instead of blind: it can
// see which genome has low fitness (and thus deserves a patch) and which
// strategy is live (and thus should be mutated with care).
func buildEvolutionSuggestionPrompt(
	ctx context.Context,
	evStore evidence.Store,
	strategyStore evolution.StrategyStore,
) string {
	base := "Examine the current system state and suggest one evolution improvement. " +
		"Use one of: insert node, remove node, replace node, add edge, remove edge, " +
		"change scheduler, change topk, change reducer, change planner, change recovery."

	var sb strings.Builder
	sb.WriteString(base)

	if evStore != nil {
		var lines []string
		for _, src := range fitnessSourceOrder {
			mean, count, ok := recentFitnessSummary(ctx, evStore, src, fitnessWindowSize)
			if !ok {
				continue
			}
			lines = append(lines, fmt.Sprintf("- %s: mean fitness %.2f over %d evidence records", src, mean, count))
		}
		if len(lines) > 0 {
			sb.WriteString("\n\nCurrent evolution state (recent fitness evidence):\n")
			sb.WriteString(strings.Join(lines, "\n"))
		}
	}

	if strategyStore != nil {
		if st, err := strategyStore.GetActive(ctx); err == nil && st != nil {
			sb.WriteString("\n\nCurrently deployed strategy: ")
			fmt.Fprintf(&sb, "id=%s version=%d", st.ID, st.Version)
			if st.Score >= 0 {
				fmt.Fprintf(&sb, " score=%.2f", st.Score)
			}
			if st.MutationDesc != "" {
				fmt.Fprintf(&sb, " mutation=%q", st.MutationDesc)
			}
		}
	}

	sb.WriteString("\n\nRespond with exactly one suggestion in the allowed format.")
	return sb.String()
}

// fitnessWindowSize bounds how many evidence records are summarized per genome
// source so a long-running process does not read the whole store each cycle.
const fitnessWindowSize = 50

// recentFitnessSummary computes the mean fitness value over the most recent
// fitness evidence records for one genome source. It returns ok=false when
// the store is nil or no usable numeric record exists in the window.
func recentFitnessSummary(ctx context.Context, store evidence.Store, source string, limit int) (mean float64, count int, ok bool) {
	if store == nil {
		return 0, 0, false
	}
	evs, err := store.Query(ctx, evidence.Filter{
		Source: source,
		Kind:   evidence.KindFitness,
		Limit:  limit,
	})
	if err != nil {
		return 0, 0, false
	}
	var sum float64
	for _, ev := range evs {
		if len(ev.Payload) == 0 {
			continue
		}
		var fe struct {
			Value float64 `json:"value"`
		}
		if err := json.Unmarshal(ev.Payload, &fe); err != nil {
			continue
		}
		if fe.Value < 0 || fe.Value > 1 {
			continue
		}
		sum += fe.Value
		count++
	}
	if count == 0 {
		return 0, 0, false
	}
	return sum / float64(count), count, true
}
