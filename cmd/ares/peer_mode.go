package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agents"
	"github.com/Timwood0x10/ares/internal/agents/base"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/agentsyscall"
	"github.com/Timwood0x10/ares/internal/ares_bootstrap"
	"github.com/Timwood0x10/ares/internal/ares_config"
	"github.com/Timwood0x10/ares/internal/ares_events"
	evolution "github.com/Timwood0x10/ares/internal/ares_evolution"
	"github.com/Timwood0x10/ares/internal/ares_skills"
	"github.com/Timwood0x10/ares/internal/aresrecovery"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/llm/output"
	"github.com/Timwood0x10/ares/internal/storage/postgres/repositories"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// peerTaskSeq is a monotonic sequence for peer-mode task IDs (the old tracker
// counter hack is gone: the shared LoadTracker is scheduler-internal now).
var peerTaskSeq atomic.Int64

// normalizedPeers resolves the C1 flat peer population from config. The
// agents.peers structure is the DEFAULT; when it is empty (pre-C1 config),
// the legacy agents.sub entries are normalized into peers (each sub's single
// Type becomes its only capability). Returns an empty slice when neither is
// configured (the caller reports it as an error).
func normalizedPeers(cfg *ares_config.Config) []ares_config.PeerAgentConfig {
	if len(cfg.Agents.Peers) > 0 {
		return cfg.Agents.Peers
	}
	peers := make([]ares_config.PeerAgentConfig, 0, len(cfg.Agents.Sub))
	for _, s := range cfg.Agents.Sub {
		peers = append(peers, ares_config.PeerAgentConfig{
			ID:            s.ID,
			Capabilities:  []string{s.Type},
			Priority:      s.Priority,
			MaxToolRounds: s.MaxToolRounds,
		})
	}
	return peers
}

// createPeerAgents builds a set of peer agents WITHOUT a Leader. Each agent
// registers directly with the Kernel scheduler via the Task Fabric. This is
// the W2 "Leader OFF" startup mode (aresos-plan.md §6.3.6): a group of
// equal agents competes for tasks via capability-based scheduling, with no
// privileged orchestrator.
//
// The spawn_agent / create_task syscalls are wired into the shared ToolBinder
// so every agent can autonomously decide to decompose work and spawn peers.
// The Kernel enforces quota/capability validation on every spawn.
// createPeerAgents builds a set of peer agents WITHOUT a Leader (C1 flat
// capability agents registered into the agentfabric dynamic population).
// Each configured sub-agent is
// spawned into the Agent Fabric WITH its execution body (ChatCognition) and
// its distilled experience prior (G1), so the scheduler's candidate pool —
// queried live from the fabric (B1) — is exactly the set of real, executable
// agents. There is no second registration table to keep in sync: spawn/kill
// take effect on the next scheduler drain.
//
//nolint:gocyclo // createPeerAgents is a wiring hub (like runServe): it assembles the peer-mode kernel from Task Fabric, Agent Fabric, scheduler, evolution feedback, syscalls, recovery and the lifecycle in one function. Each branch is a distinct wiring step; splitting it would spread one assembly across helpers without reducing the decisions.
func createPeerAgents(
	ctx context.Context,
	cfg *ares_config.Config,
	comp *ares_bootstrap.Components,
	llmAdapter output.LLMAdapter,
	chatClient sub.ChatClient,
	toolBinder sub.ToolBinder,
	store ares_events.EventStore,
	strategySrc agents.StrategySource,
	expRepo repositories.ExperienceRepositoryInterface,
) ([]sub.Agent, *kernelHandle, error) {
	kernel := &kernelHandle{}

	// C1: the flat Peers structure is the DEFAULT agent source; the legacy
	// Sub structure remains as the fallback so pre-C1 configs keep working.
	peers := normalizedPeers(cfg)

	// Build sub-agents from the flat peer population (each agent gets the
	// full LLM + tool stack).
	subAgents := createPeerSubAgents(cfg, peers, llmAdapter, chatClient, toolBinder, store, strategySrc)

	// W4: index the configured role by agent id so both the static executor
	// (createExecutor, inside createPeerSubAgents) and the fabric execution
	// body (newPeerChatCognition, in the spawn loop below) resolve the SAME
	// role for an agent — the fabric body is the production path, so a
	// config-pinned role must reach it too .
	roleByID := make(map[string]string, len(peers))
	for _, p := range peers {
		roleByID[p.ID] = p.Role
	}
	if len(subAgents) == 0 {
		return nil, nil, errors.New("peer mode: no peer agents configured (agents.peers or agents.sub)")
	}

	// Assemble the Kernel: Task Fabric + Agent Fabric + scheduler. This
	// mirrors flipKernelToTaskFabric but runs directly at startup (no
	// legacy path to flip from).
	kernel.fabric = taskfabric.NewFabric()
	// E1 (evolution loop closure): stamp every submitted task with the
	// strategy that was active at submission time, so runtime fitness samples
	// stay attributed to the strategy that produced them across promotes.
	// Cheap + non-blocking: one store read per Create on the submission path.
	if strategySrc != nil {
		kernel.fabric = kernel.fabric.WithStrategyStamp(func() string {
			stampCtx, stampCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer stampCancel()
			st, err := strategySrc.GetActiveStrategy(stampCtx)
			if err != nil || st == nil {
				return ""
			}
			return st.ID
		})
	}
	if store != nil {
		kernel.fabric = kernel.fabric.WithEventStore(store)
		// Rebuild in-memory tasks from the durable task.* log BEFORE the
		// scheduler starts draining: restoring after the first Acquire would
		// reset tasks created in this process lifetime. Fail-loud — silently
		// continuing would drop tasks the log says exist (T2).
		if err := kernel.fabric.RestoreFromStore(ctx); err != nil {
			return nil, nil, fmt.Errorf("peer mode: restore task fabric from event store: %w", err)
		}
	}
	// W8: experience-derived confidence prior — recorded skill/task outcomes
	// sharpen scheduling when the same pattern recurs. Nil (skills disabled)
	// keeps declared confidences.
	if expSrc := resolveExperienceConfidence(comp); expSrc != nil {
		kernel.fabric = kernel.fabric.WithConfidenceSource(expSrc)
	}

	// M4-C3: the static sub.Agent executor pool is gone. Its entries were
	// dead in peer mode — the scheduler skips static registrations whenever
	// the agent fabric is wired (the fabric's live population is the single
	// candidate source) and recovery-bound tasks resolve through
	// RegisterExecutorForTask instead. The map stays non-nil because the
	// scheduler copies it at construction; an empty pool simply means
	// "fabric only", which the drain path was designed for.
	kernel.executors = make(map[string]CapabilityExecutor, len(subAgents))

	// Build the candidate list for the fabric dispatcher. The full declared
	// capability set (Caps) is offered to the scorer so a task matching ANY
	// capability is schedulable to the peer.
	subCaps := make([]subAgentCapability, 0, len(peers))
	for _, p := range peers {
		typ := ""
		if len(p.Capabilities) > 0 {
			typ = p.Capabilities[0]
		}
		subCaps = append(subCaps, subAgentCapability{ID: p.ID, Type: typ, Caps: append([]string(nil), p.Capabilities...)})
	}

	// Assemble the kernel dispatcher with the Task Fabric path as the active
	// path (no legacy leader track: the flag starts at PolicyTaskFabric).
	kernelDispatcher, kernelFlag := wireKernelDispatcher(subCaps)
	kernel.dual = kernelDispatcher
	kernel.flag = kernelFlag

	// One shared load tracker for the scheduler.
	tracker := newLoadTracker()
	kernel.tracker = tracker

	// Enable real Task Fabric execution (not scoring mode).
	enableKernelExecution(kernel.dual, kernel.fabric)

	// Start the scheduler.
	sched := NewKernelScheduler(kernel.fabric, kernel.executors, tracker)
	if store != nil {
		sched.WithEventStore(store)
	}
	sched.WithMaxConcurrent(0)
	// W7: honor the YAML kernel.poll_interval. Previously the config field was
	// never injected — the scheduler always drained on the 500ms default.
	if d := parseKernelPollInterval(cfg.Kernel.PollInterval); d > 0 {
		sched.PollInterval = d
	}
	// Optional snappier leases for chaos/recovery demos (#panel): a dead
	// agent's tasks requeue after lease_ttl instead of the 5-minute default.
	if ttl := parseKernelLoopConfig(cfg).LeaseTTL; ttl > 0 {
		sched.WithTTL(ttl)
	}
	kernel.scheduler = sched
	kernel.flipped = true

	// Step 4 (closure plan N-1): real-execution shadow A/B for candidate
	// strategies. Default off; when enabled the scheduler buffers finalized
	// tasks and the G2 gate judges candidates on isolated executions (with a
	// side-effect deny-list) of those buffered tasks instead of replay-only
	// evidence. See shadow_execution.go.
	wireShadowExecution(cfg, comp, sched, llmAdapter, chatClient, toolBinder)

	// W4 evolution feedback loop: record execution outcomes per agent +
	// capability, and periodically push the derived confidence back into the
	// tracker so the next Schedule prefers historically-successful executors.
	// C2.3: the loop now also writes the zero-LLM deterministic score back
	// to the active strategy's Score field via the StrategyStore, so the
	// GA's fitness signal tracks real execution outcomes without any LLM
	// call.
	attribution := aresrecovery.NewExecutionAttribution()
	sched.WithAttribution(attribution)
	feedback := aresrecovery.NewEvolutionFeedbackAdapter(attribution, tracker)

	// C2.4: wire the zero-LLM score provider into the EvolutionScheduler so
	// task.completed/failed events feed the deterministic aggregate score
	// (from attribution) instead of the constant 1.0/0.0. The provider reads
	// the same attribution that the feedback loop writes to, so the score
	// window reflects real execution quality (latency, retries, recovery).
	if comp.Evolution != nil {
		if sched, ok := comp.Evolution.Scheduler.(*evolution.EvolutionScheduler); ok && sched != nil {
			sched.SetScoreProvider(
				aresrecovery.NewAttributionScoreProvider(attribution),
			)
		}
	}

	// C3.2 (loop closure): make the "independent scorer wired" G2 promise real.
	// bootstrap_steps.go set DeterministicScorerEnabled=true so hasScorer passed
	// and the G2 shadow gate was registered as "independent scorer wired" — but
	// buildShadowEvaluator only sets a shadow scorer when an LLM scorer exists.
	// With llmScorer==nil the evaluator's scorer stayed nil, the ShadowSampler
	// no-op'd, and G2 rejected every candidate fail-closed forever (a gate that
	// claims evidence but never gathers it).
	//
	// The scorer must DISCRIMINATE per strategy, otherwise the defect only
	// moves: one global attribution score returns the same number for the
	// candidate and the active strategy, every comparison is an exact tie
	// (ShadowWon requires shadow > active), the win rate is 0.0 and G2 still
	// rejects everything. So the evidence source is the ReplayScorer: each
	// strategy is scored by the mean of ITS OWN KindFitness records that the
	// RuntimeObserver already writes per finished task, read over a distinct
	// time window per comparison — real per-strategy evidence, zero LLM calls.
	// The attribution-derived deterministic score (C2.2) supplies the
	// cold-start prior for a strategy with no history in a window, so the same
	// execution quality the GA rewards also anchors the shadow comparison.
	if comp.NewEvolution != nil && comp.NewEvolution.ShadowEvaluator != nil {
		det := aresrecovery.NewDeterministicScorer()
		// W2: the replay query limit is configurable (evolution.shadow.
		// replay_query_limit). Zero keeps the default (200) — a config that
		// never mentions it behaves exactly as before.
		replay := evolution.NewReplayScorer(comp.EvidenceStore, func() float64 {
			return det.ScoreAttribution(attribution)
		}, evolution.WithReplayQueryLimit(cfg.Evolution.Shadow.ReplayQueryLimit))
		// Without an evidence store replay degrades to prior-vs-prior, i.e.
		// the tie deadlock above. Leave the scorer unset in that case so G2
		// stays honestly fail-closed instead of judging on ties.
		if replay.HasStore() {
			comp.NewEvolution.ShadowEvaluator.SetShadowScorer(replay.Score)
		}
	}

	// C2.3: wrap the confidence-injection adapter with score write-back.
	// The strategyScoreAdapter bridges to evolution.StrategyStore without
	// creating a circular import (aresrecovery cannot import evolution).
	var scoreWriter aresrecovery.StrategyScoreWriter
	if comp.NewEvolution != nil {
		scoreWriter = newStrategyScoreAdapter(comp.NewEvolution.StrategyStore)
	}
	scoredFeedback := aresrecovery.NewScoredFeedbackAdapter(feedback, nil, scoreWriter)
	runBackground(ctx, comp, "evolution-feedback", func(loopCtx context.Context) error {
		aresrecovery.RunScoredFeedbackLoop(loopCtx, scoredFeedback, 10*time.Second)
		return nil
	})

	// Collaboration-graph janitor: reclaim terminal residue left by fail-fast
	// / timeout submissions off the hot path (per-submission cleanup handles
	// the common case; this catches siblings that were in-flight then).
	runBackground(ctx, comp, "collab-gc", func(loopCtx context.Context) error {
		runCollabGCLoop(loopCtx, kernel.fabric, 60*time.Second)
		return nil
	})

	// Assemble the Lifecycle pillar (agentfabric + aresrecovery).
	// Wire the agent-fabric lifecycle sink into the shared event bus (#panel
	// feedback): deaths/spawns/suspensions must reach the introspection feed
	// the moment they happen, not only via lease-expiry downstream. Mapping to
	// existing bus types keeps consumers uniform (spawned/resumed → started;
	// killed/suspended/retired → stopped with reason).
	agentBus := &fabricEventSink{store: store}
	agents := agentfabric.NewFabric().WithEventSink(agentBus)
	if len(cfg.Kernel.Resources) > 0 {
		agents = agents.WithResourceBudget(cfg.Kernel.Resources)
	}
	kernel.agents = agents

	// The DAG execution gate (M4-A1: kernel.dag_execution in config).
	// Zero/absent config = legacy ReAct behavior (chat cognition for every
	// peer, L2 machinery test-only).
	//
	// M3: when Enabled, peers advertise the FULL capability set the L2
	// router needs (ares/root, ares/plan, ares/answer, tool/<name> for
	// every bound tool) instead of a single primary type. The session
	// registry is wired per-peer so the plannerCognition can look up its
	// L2 graph. Both prerequisites (M2 session wiring + M3 capability
	// advertisement) now land here.
	peerDAGExecution := resolveDAGExecution(cfg.Kernel.DAGExecution)
	// The router body carries the planner when the gate is open; the
	// planner needs session-scoped dependencies (registry, fabric reader)
	// that are constructed here.
	var peerRouter agentfabric.Cognition
	var sessionReg *agentfabric.SessionRegistry
	if peerDAGExecution.Enabled {
		sessionReg = agentfabric.NewSessionRegistry()

		// M5: read the L1 ToolClass DAG from the evolution components so
		// the planner can check enabled/budget/prior before growing L2
		// tool nodes. Nil when no tools are registered (permissive).
		var l1DAG *engine.MutableDAG
		if comp.NewEvolution != nil {
			l1DAG = comp.NewEvolution.ToolClassDAG()
		}

		planner, err := agentfabric.NewPlannerCognition(agentfabric.PlannerDeps{
			ChatClient: chatClient, // sub.ChatClient satisfies agentfabric.ChatClient
			ToolBinder: toolBinder, // sub.ToolBinder satisfies agentfabric.ToolBinder
			Sessions:   sessionReg,
			Fabric:     kernel.fabric,
			L1DAG:      l1DAG,
			// M4-A2: operator-tunable growth-depth guard (0/absent = default).
			MaxDepth: resolveMaxPlanDepth(cfg.Kernel.DAGExecution),
			Logger:   slog.Default(),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("peer mode: create planner cognition: %w", err)
		}
		peerRouter = agentfabric.NewRouterCognitionWithPlanner(toolBinder, planner, sessionReg, slog.Default())
	}

	// M4-B2: retain the registry on the kernel so the submission path can
	// admit sessions. Nil when the gate is off — legacy behavior unchanged.
	kernel.sessionReg = sessionReg

	// P0-1: terminal-task reaper for L2 session tasks. Every grown node is
	// a fabric task and the fabric never self-harvests, so without this
	// loop the in-memory task map grows monotonically across a long-lived
	// serve (§9's named cost). The registry is the keep-set authority: a
	// live session's tasks are its readable history (decision C) and are
	// never harvested; only tasks of released sessions die, after the
	// configured grace window. Gate off = no registry = nothing to reap.
	if sessionReg != nil {
		sessionReaper := taskfabric.NewReaperWithKeep(kernel.fabric, "sess/",
			resolveReaperGrace(cfg.Kernel.DAGExecution), sessionKeepSet(sessionReg))
		runBackground(ctx, comp, "l2-reaper", func(loopCtx context.Context) error {
			sessionReaper.Run(loopCtx.Done(), time.Minute)
			return nil
		})
		log.Printf("peer mode: L2 session task reaper wired (grace %s)",
			sessionReaper.GracePeriod())
	}

	// M3: the L2 capability set is built per-peer below via
	// peerCapabilities (canary partition: legacy primary-type tasks match
	// only gate-off peers, ares/* tasks only gate-on peers).

	// C1: configured sub-agents ARE the fabric's dynamic population — each is
	// spawned WITH its execution body (ChatCognition) and its distilled
	// experience prior (G1), instead of living only in the static executor
	// registry. The scheduler queries the fabric on every drain (B1), so this
	// is the single registration point: a future kill/retire immediately
	// removes the candidate, and the recovery/chaos loops manage the SAME
	// population they recover.
	for _, sa := range subAgents {
		if sa == nil {
			continue
		}
		sa := sa // capture for the closure (spawn is synchronous, but keep the
		// loop-scoped binding local for the CognitionFactory below)
		// N10: build the cognition upfront so a nil-CognitionFactory error
		// surfaces at spawn time, not silently swallowed in a closure.
		cog, err := newPeerChatCognition(sa.ID(), llmAdapter, chatClient, toolBinder, cfg, strategySrc, roleByID[sa.ID()])
		if err != nil {
			return nil, nil, fmt.Errorf("peer mode: create chat cognition for %q: %w", sa.ID(), err)
		}
		caps := []string{string(sa.Type())}
		if peerDAGExecution.Enabled {
			caps = peerCapabilities(string(sa.Type()), true, toolBinder.ListTools())
		}
		if _, err := agents.Spawn(ctx, agentfabric.SpawnSpec{
			Identity:     sa.ID(),
			Capabilities: caps,
			// A1.4: the default execution body is the tool-loop Cognition
			// moved down into agentfabric — a fabric agent is fully
			// self-contained (LLM + tools), no sub.Agent wrapper.
			CognitionFactory: func([]string) agentfabric.Cognition {
				return peerDAGExecution.Select(cog, peerRouter)
			},
			ExperiencePrior: loadExperiencePrior(ctx, expRepo, sa.ID()),
		}); err != nil {
			return nil, nil, fmt.Errorf("peer mode: spawn agent %q into fabric: %w", sa.ID(), err)
		}
	}

	policy := aresrecovery.DefaultRestartPolicy()
	if cfg.Kernel.MaxRestarts > 0 {
		policy.MaxRestarts = cfg.Kernel.MaxRestarts
	}
	kernel.recovery = aresrecovery.New(kernel.fabric, agents, policy)
	sched.WithGovernance(agents)
	// B1: the scheduler's candidate pool includes every live, IDLE, executable
	// fabric agent — the configured peers spawned above, plus any spawned via
	// the spawn_agent syscall. Static registered executors (recovery-bound)
	// still win by skip logic in appendFabricCandidates.
	sched.WithAgentFabric(agents)

	// Wire the spawn_agent / create_task syscalls into the shared ToolBinder.
	// Every agent's LLM executor sees these tools alongside the built-in
	// tools, so it can autonomously decide to spawn peers and create tasks.
	kernelSyscall := agentsyscall.NewKernel(
		agents,
		kernel.fabric,
		func(agentID, capability string) agentsyscall.Executor {
			agent := newPeerExecutor(agentID, models.AgentType(capability), llmAdapter, chatClient, toolBinder, cfg, strategySrc)
			return &peerExecutorAdapter{agent: agent}
		},
		// M4-C3: no scheduler registration here. The static pool is
		// skipped whenever the agent fabric is wired, so registering
		// syscall-spawned agents was a no-op for normal drains — and the
		// agentsyscall.Executor half (the factory return above, which
		// powers spawn_agent/create_task/ask_agent) is untouched.
		func(string, agentsyscall.Executor) {},
		// GAP-2: plan loops started via the create_plan loop option must be
		// bounded by the serve lifetime, not the individual tool call.
		agentsyscall.WithLoopLifetime(ctx),
	)
	agentsyscall.BindTools(toolBinder, kernelSyscall)
	// Retain the syscall Kernel on the kernel handle so the collaboration IPC
	// bridge (built later in setupPeerRegistry) can inject ipc.Send into
	// ask_agent (Step Y.2-ACT).
	kernel.syscalls = kernelSyscall
	log.Printf("peer mode: spawn_agent / create_task / ask_agent syscalls wired into tool binder")

	// Inject agent priorities into the tracker (B2: thread priority).
	for _, p := range peers {
		if p.Priority > 0 {
			tracker.SetPriority(p.ID, p.Priority)
		}
	}

	// Start the scheduler and recovery loop. The recovery loop wires a REAL
	// executor factory (newPeerExecutor — full sub.Agent with LLM + tools) and
	// binds each replacement to exactly the task it was spawned for
	// (RegisterExecutorForTask), so a dead agent's task is resumed by a real
	// cognitive process — not a canned-success stub, and never at the expense
	// of a brand-new task.
	// Runtime plugin ecosystem closure: the PluginBus hooks the scheduler's
	// quantum boundary (observer/checkpoint/tool plugins observe every
	// Schedule→Acquire→RunQuantum). The adapter lives in runtime_bridge.go —
	// the kernel stays free of any runtime import (§0.3 dependency rule).
	// The loop knobs are parsed ONCE here and shared with the recovery loop
	// below (a second parse would waste work and risk drift).
	kernelLoopCfg := parseKernelLoopConfig(cfg)
	kernel.pluginBus = startPluginBus(ctx, store, sched, kernelLoopCfg)

	// K2/K3: the scheduler drain loop and the recovery loop run as managed
	// background loops and hand their lifecycle to the System Runtime
	// adapter (stop = cancel, wait = join the goroutine). The loop context
	// is pre-derived from the serve ctx — NOT from the context runBackground
	// passes — so the adopt-time Stop hook owns a cancel that works
	// independently of which managed pool ended up running the goroutine.
	schedCtx, schedCancel := context.WithCancel(ctx)
	schedDone := make(chan struct{})
	runBackground(ctx, comp, sysCompScheduler, func(context.Context) error {
		defer close(schedDone)
		sched.Run(schedCtx)
		return nil
	})
	kernel.schedulerStop = schedCancel
	kernel.schedulerDone = schedDone

	recCtx, recCancel := context.WithCancel(ctx)
	recDone := make(chan struct{})
	// B1: bind the scheduler's stale-winner hint to this recovery loop. When a
	// leased task's winner dies with no capable replacement, the scheduler
	// releases the task and kicks a sweep here, so the replacement execution
	// body is bound within one drain instead of one full lease TTL.
	recoveryKick, recoveryHint := newRecoveryKick()
	recoveryLoopCfg := kernelLoopCfg
	recoveryLoopCfg.RecoveryKick = recoveryKick
	sched.WithRecoveryHint(recoveryHint)
	runBackground(ctx, comp, sysCompRecovery, func(context.Context) error {
		defer close(recDone)
		runKernelRecoveryLoop(recCtx, store, kernel.recovery, recoveryLoopCfg,
			func(taskID, agentID string, executor CapabilityExecutor) {
				sched.RegisterExecutorForTask(taskID, agentID, executor)
			},
			func(agentID, capability string) CapabilityExecutor {
				// M4-C3 remainder: recovery-bound tasks bypass the candidate
				// pool, so the canary partition cannot isolate them — dispatch
				// per task. L2 session tasks resume on the gate-selected
				// router; everything else keeps the legacy ReAct executor.
				if body := selectRecoveryBody(peerDAGExecution, peerRouter, capability); body != nil {
					exec, err := newCognitionExecutor(agentID, models.AgentType(capability), body)
					if err == nil {
						return exec
					}
					log.Printf("peer mode: recovery executor L2 dispatch failed, falling back to legacy: %v", err)
				}
				return newPeerExecutor(agentID, models.AgentType(capability), llmAdapter, chatClient, toolBinder, cfg, strategySrc)
			},
			sched.HasCapableExecutor,
		)
		return nil
	})
	kernel.recoveryStop = recCancel
	kernel.recoveryDone = recDone

	log.Printf("peer mode: %d peer agents registered, Kernel scheduler started (no leader)", len(subAgents))
	return subAgents, kernel, nil
}

// newPeerChatCognition constructs the A1.4 default tool-loop Cognition for a
// peer agent. It carries the same LLM + tool stack as the configured agents
// (the createExecutor wiring), but as a self-contained agentfabric execution
// body — no sub.Agent wrapper. This is the production default execution body
// of the peer runtime (aresos-agentos-plan A1.4: tool-loop sunk into agentfabric).
func newPeerChatCognition(
	agentID string,
	llmAdapter output.LLMAdapter,
	chatClient sub.ChatClient,
	toolBinder sub.ToolBinder,
	cfg *ares_config.Config,
	strategySrc agents.StrategySource,
	role string,
) (agentfabric.Cognition, error) {
	return agentfabric.NewChatCognition(agentfabric.ChatCognitionDeps{
		ChatClient:     chatClient, // sub.ChatClient satisfies agentfabric.ChatClient
		LLMAdapter:     llmAdapter,
		ToolBinder:     toolBinder, // sub.ToolBinder satisfies agentfabric.ToolBinder
		StrategySource: strategySrc,
		Template:       output.NewTemplateEngine(),
		PromptTemplate: cfg.Prompts.Recommendation,
		EventStore:     nil, // the fabric publishes via its own event store wiring
		AgentID:        agentID,
		// W4 write side: pin the configured role so the fabric execution body
		// (the actual production path) carries the role instructions, matched to
		// the static createExecutor. Unknown role → nil → roleless peer.
		Profile: resolveRoleProfile(role),
	})
}

// newPeerExecutor creates a full sub.Agent executor for a dynamically spawned
// peer agent. The executor carries the same LLM + tool stack as the configured
// agents, so a spawned agent is a real cognitive process — not a stub.
func newPeerExecutor(
	agentID string,
	capability models.AgentType,
	llmAdapter output.LLMAdapter,
	chatClient sub.ChatClient,
	toolBinder sub.ToolBinder,
	cfg *ares_config.Config,
	strategySrc agents.StrategySource,
) sub.Agent {
	// Build a sub-agent config from the capability.
	subCfg := ares_config.SubAgentConfig{
		ID:         agentID,
		Type:       string(capability),
		MaxRetries: 3,
		Timeout:    60,
	}
	executor := createExecutor(llmAdapter, chatClient, toolBinder, cfg, subCfg, strategySrc, "")
	handler := sub.NewMessageHandler(agentID)
	agent := sub.New(
		agentID,
		capability,
		executor,
		handler,
		nil,
		nil,
		&sub.SubAgentConfig{
			Config: base.Config{
				ID:   agentID,
				Type: capability,
			},
			EnableTools: true,
		},
	)
	return agent
}

// loadExperiencePrior loads the most recent distilled experience for the
// agent and returns it as the G1 spawn prior (aresos-agentos-plan G1: Memory
// Distill onto the agent lifecycle — async distillation feeds an experience
// store queried at spawn time and injected as a prior).
// The prior is injected as SpawnSpec.ExperiencePrior so the agent starts with
// reusable distilled experience as its cognitive context instead of a blank
// slate. Returns nil when the repo is unavailable, the agent has no distilled
// experience yet, or the query fails — a nil prior is the zero-value
// contract, never a startup error.
func loadExperiencePrior(ctx context.Context, expRepo repositories.ExperienceRepositoryInterface, agentID string) any {
	if expRepo == nil {
		return nil
	}
	exps, err := expRepo.ListByAgent(ctx, agentID, ares_events.DefaultTenantID, 1)
	if err != nil || len(exps) == 0 {
		return nil
	}
	exp := exps[0]
	return map[string]any{
		"type":        exp.Type,
		"problem":     exp.Problem,
		"solution":    exp.Solution,
		"constraints": exp.GetConstraints(),
	}
}

// submitPeerTask creates a task directly in the Task Fabric for the peer-agent
// runtime (no leader dispatch). This is the entry point for user-submitted
// work in Leader OFF mode: the task enters READY and the Kernel scheduler
// picks it up via the normal Schedule → Acquire → RunQuantum path.
//
// It is exposed as POST /api/tasks on the serve HTTP layer (actionHandler),
// closing the user-submission loop: a request reaches the fabric and the
// scheduler executes it — no leader and no autopilot involved.
func submitPeerTask(ctx context.Context, kernel *kernelHandle, capability string, payload map[string]any) (string, error) {
	if kernel == nil || kernel.fabric == nil {
		return "", errors.New("peer mode: kernel fabric not wired")
	}
	// M4-B2: a session-scoped submission admits its L2 session first, so the
	// planner's first quantum finds a live graph. Fail-fast comes before any
	// task is created: an unadmittable session must not leave an unrunnable
	// task behind. Session-less submissions skip this entirely (legacy path).
	var sessionID, prompt string
	if payload != nil {
		sessionID, _ = payload["session_id"].(string)
		prompt, _ = payload["input"].(string)
	}
	if err := ensureSessionAdmission(ctx, kernel, sessionID, prompt); err != nil {
		return "", err
	}
	taskID := fmt.Sprintf("peer-task-%s-%d", capability, peerTaskSeq.Add(1))

	env := &taskfabric.CheckpointEnvelope{
		Payload: payload,
	}
	// M2: stamp SessionID onto the envelope so the plannerCognition can
	// look up the per-session L2 graph registry. A session-less task
	// (no "session_id" in payload) leaves SessionID empty — the legacy
	// behavior is unchanged.
	if sid, ok := payload["session_id"].(string); ok && sid != "" {
		env.SessionID = sid
	}
	task := &taskfabric.Task{
		ID:         taskID,
		Capability: capability,
		// Origin stays "" — this is a root task (user-submitted work), no
		// agent caller. Agent-created tasks get their Origin from the
		// create_task syscall's tool context (kernel.CallerID).
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 2},
		Checkpoint:  env,
	}
	if err := kernel.fabric.Create(task); err != nil {
		return "", fmt.Errorf("peer mode: create task: %w", err)
	}
	log.Printf("peer mode: submitted task %q (%s) → READY", taskID, capability)
	return taskID, nil
}

// ── Kernel-path chaos (P1: unified lifecycle) ───────────────────────────
//
// The /api/chaos/* endpoints previously killed agents through the legacy
// ares_runtime manager pool, which has its own resurrection semantics — a
// SECOND lifecycle next to the kernel's agentfabric + aresrecovery pair.
// These helpers retarget chaos at the kernel fabric so an injected death
// exercises the REAL recovery chain: agent.killed → lease expiry → requeue →
// replacement executor resumes from checkpoint. The legacy mgr path remains
// only as a fallback for non-peer deployments.

// chaosKillRandomFabric kills one uniformly-chosen LIVE agent in the kernel's
// Agent Fabric and returns its id. Agents already dead (killed earlier) are
// skipped; killing an empty fabric is a caller-visible error.
func chaosKillRandomFabric(ctx context.Context, k *kernelHandle) (string, error) {
	if k == nil || k.agents == nil {
		return "", errors.New("peer mode: kernel agent fabric not wired")
	}
	live := liveFabricAgents(k.agents)
	if len(live) == 0 {
		return "", errors.New("peer mode: no live agents in the fabric")
	}
	target := live[rand.Intn(len(live))]
	if err := k.agents.Kill(ctx, target); err != nil {
		return "", fmt.Errorf("peer mode: kill %s: %w", target, err)
	}
	log.Printf("peer mode: chaos killed agent %q — lease expiry + replacement recovery will follow", target)
	return target, nil
}

// chaosKillAllFabric kills every LIVE agent in the kernel's Agent Fabric.
// It returns separate killed/failed lists because chaos engineering cares
// precisely about what did NOT die: a per-agent Kill error is logged AND
// surfaced instead of being silently skipped. err != nil is reserved for
// "the fabric itself is not wired", mirroring chaosKillRandomFabric.
func chaosKillAllFabric(ctx context.Context, k *kernelHandle) (killed, failed []string, err error) {
	if k == nil || k.agents == nil {
		return nil, nil, errors.New("peer mode: kernel agent fabric not wired")
	}
	killed = make([]string, 0)
	failed = make([]string, 0)
	for _, id := range liveFabricAgents(k.agents) {
		if kerr := k.agents.Kill(ctx, id); kerr != nil {
			log.Printf("peer mode: chaos kill-all failed for %q: %v", id, kerr)
			failed = append(failed, id)
			continue
		}
		killed = append(killed, id)
	}
	return killed, failed, nil
}

// chaosRecoverSweep forces one recovery sweep over the kernel's task fabric:
// every expired-lease task is requeued to READY so the scheduler (and, when
// no capable executor remains, the replacement factory) can pick it up.
//
// The two outcomes are deliberately distinct: an unwired recovery subsystem
// is an ERROR (operators must never see success with zero work done when the
// sweeper does not exist), while an empty result is a NORMAL response
// meaning nothing had expired. Returns the requeued task ids — the kernel
// recovers TASKS, not agents, because agents are disposable cognition and
// tasks are durable intent.
func chaosRecoverSweep(k *kernelHandle) ([]string, error) {
	if k == nil || k.recovery == nil {
		return nil, errors.New("peer mode: kernel recovery not wired")
	}
	requeued := k.recovery.RequeueExpiredLeases()
	if len(requeued) > 0 {
		log.Printf("peer mode: chaos recover sweep requeued %d expired task(s)", len(requeued))
	}
	return requeued, nil
}

// liveFabricAgents lists fabric ids that still resolve to a live agent
// (Get errors after Kill).
func liveFabricAgents(agents *agentfabric.Fabric) []string {
	live := make([]string, 0)
	for _, id := range agents.Agents() {
		if _, err := agents.Get(id); err == nil {
			live = append(live, id)
		}
	}
	return live
}

// peerExecutorAdapter wraps a sub.Agent to satisfy the agentsyscall.Executor
// interface. The adapter translates sub.StepOutcome to agentsyscall.StepOutcome
// so the syscall package stays decoupled from the sub package
// (interface defined at the consumer, code_rules_v2 5.2).
type peerExecutorAdapter struct {
	agent sub.Agent
}

// ID returns the wrapped agent's ID.
func (a *peerExecutorAdapter) ID() string { return a.agent.ID() }

// Type returns the wrapped agent's type.
func (a *peerExecutorAdapter) Type() models.AgentType { return a.agent.Type() }
func (a *peerExecutorAdapter) ExecuteStep(ctx context.Context, task *models.Task) (*agentsyscall.StepOutcome, error) {
	out, err := a.agent.ExecuteStep(ctx, task)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return &agentsyscall.StepOutcome{}, nil
	}
	return &agentsyscall.StepOutcome{
		Done:       out.Done,
		Checkpoint: out.Checkpoint,
		Result:     out.Result,
	}, nil
}

// fabricEventSink forwards agentfabric lifecycle records onto the shared
// ares_events bus so observability consumers (introspection feed, archive)
// see agent deaths and revivals in real time.
type fabricEventSink struct {
	store ares_events.EventStore
}

// Emit implements agentfabric.EventSink.
func (f *fabricEventSink) Emit(ctx context.Context, ev agentfabric.AgentEvent) error {
	if f == nil || f.store == nil {
		return nil
	}
	busType := ares_events.EventAgentStarted
	reason := string(ev.Type)
	switch ev.Type {
	case agentfabric.EventAgentSpawned, agentfabric.EventAgentResumed:
		reason = ""
	case agentfabric.EventAgentSuspended, agentfabric.EventAgentRetired,
		agentfabric.EventAgentKilled:
		busType = ares_events.EventAgentStopped
	}
	payload := map[string]any{
		"agent_id": ev.AgentID,
	}
	if reason != "" {
		payload["reason"] = reason
	}
	if ev.ParentID != "" {
		payload["parent"] = ev.ParentID
	}
	return f.store.Append(ctx, ev.AgentID, []*ares_events.Event{{
		Type:       busType,
		ModuleName: "agentfabric",
		Payload:    payload,
		Timestamp:  ev.At,
	}}, 0)
}

// resolveExperienceConfidence wires the skill catalog's experience store as
// the task fabric's confidence prior (W8 second half). A nil catalog (skills
// disabled) keeps the fabric's declared confidences untouched.
//
// Args:
//   - comp: the bootstrap components carrying the live skill catalog.
//
// Returns:
//   - taskfabric.ConfidenceSource: the catalog-backed prior, or nil.
func resolveExperienceConfidence(comp *ares_bootstrap.Components) taskfabric.ConfidenceSource {
	if comp == nil || comp.SkillCatalog == nil {
		return nil
	}
	return ares_skills.NewExperienceConfidenceSource(comp.SkillCatalog.Experience())
}
