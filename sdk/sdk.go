// Package ares provides the top-level, unified entry point for the ARES
// agent runtime. It wraps all internal components behind a simple,
// production-friendly API.
//
// Quick start:
//
//	import (
//	    "context"
//
//	    "github.com/Timwood0x10/ares/sdk"
//	)
//
//	func main() {
//	    ctx := context.Background()
//	    rt := sdk.NewRuntime(sdk.WithOpenAI("gpt-4o-mini"))
//	    defer rt.Close()
//
//	    agent := rt.NewAgent("assistant",
//	        sdk.WithInstruction("You are a helpful assistant."),
//	    )
//	    result, err := agent.Run(ctx, "Hello!")
//	    _ = result
//	    _ = err
//	}
package sdk

//nolint: errcheck // best-effort operations: ResponseWriter writes, cleanup Close/Wait, deferred shutdown
import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Timwood0x10/ares/api/core"
	apiembed "github.com/Timwood0x10/ares/api/embedding"
	"github.com/Timwood0x10/ares/api/mcp"
	"github.com/Timwood0x10/ares/api/service/llm"
	"github.com/Timwood0x10/ares/api/tools"
	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agentloop"
	"github.com/Timwood0x10/ares/internal/agentsyscall"
	ares_bootstrap "github.com/Timwood0x10/ares/internal/ares_bootstrap"
	ares_events "github.com/Timwood0x10/ares/internal/ares_events"
	aresexp "github.com/Timwood0x10/ares/internal/ares_experience"
	memory "github.com/Timwood0x10/ares/internal/ares_memory"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/Timwood0x10/ares/internal/knowledge"
	"github.com/Timwood0x10/ares/internal/knowledge/adapter"
	khruntime "github.com/Timwood0x10/ares/internal/knowledge/runtime"
	"github.com/Timwood0x10/ares/internal/storage/postgres"
	"github.com/Timwood0x10/ares/internal/storage/postgres/repositories"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

const strategyPriority = "priority"

// akgNamespace is the default namespace assigned to AKG-distilled
// KnowledgeObjects and used to filter recall in the StoreProvider. It matches
// ares_events.DefaultTenantID so AKG facts are visible to the same
// single-tenant consumers that read distilled experiences.
const akgNamespace = "default"

// ---- public types ----

// Role constants for LLM messages.
const (
	roleSystem    = "system"
	roleUser      = "user"
	roleAssistant = "assistant"
	roleTool      = "tool"
	// sslModeDisable is the SDK convention for local/dev PostgreSQL when no
	// ssl_mode is configured (empty means "disable").
	sslModeDisable = "disable"
)

// llmService is the subset of the LLM service the sdk uses. It is an
// unexported interface so tests can inject a mock LLM (see sdk_test.go)
// without spinning up a real provider. *llm.Service satisfies it; the field
// is assigned the concrete service in New().
type llmService interface {
	Generate(ctx context.Context, req *core.GenerateRequest) (*core.GenerateResponse, error)
	GetProvider() core.LLMProvider
	GetModel() string
	Close()
}

// Runtime is the top-level container for an ARES agent system (a "new ARES runtime").
//
// It owns and manages:
//   - LLM client (OpenAI, Ollama, Anthropic, OpenRouter, or custom)
//   - Tool registry (built-in, custom, MCP-discovered, AKF tools)
//   - Memory & distillation engine (session history, experience distillation, RAG)
//   - AKG / AKF Knowledge Fabric (knowledge graph compilation + retrieval)
//   - Strategy evolution (GA-based optimisation of agent behaviour)
//   - MCP server connections (stdio-based external tools)
//   - Event-driven distillation (TaskCompleted → auto-distill pipeline)
//
// Create one with NewRuntime or New, then call NewAgent to build
// agents. Close must be called once when the Runtime is no longer needed to
// release LLM connections, stop background goroutines, and close MCP clients.
//
// Quick start:
//
//	cfg, _ := sdk.LoadConfigFile("ares.yaml")
//	opts, _ := cfg.ToOptions()
//	ares := sdk.NewRuntime(opts...)       // ares = new ARES runtime
//	defer ares.Close()
//
//	// H1: 极简闭环 — 注册平等 capability agent，按 capability 提交任务。
//	ares.RegisterAgent("coder", sdk.WithInstruction("You fix code."))
//	result, _ := ares.Submit(ctx, sdk.Task{Capability: "coder", Input: "hello"})
//
//	// 或直接用 agent 运行（Agent.Run 保留为细粒度入口）。
//	agent := ares.NewAgent("assistant", sdk.WithInstruction("You are helpful."))
//	result, _ = agent.Run(ctx, "hello")
type Runtime struct {
	llmSvc           llmService
	toolReg          *tools.Registry
	memMgr           memory.MemoryManager
	distillCleanup   func()
	memEnabled       bool
	evoEnabled       bool
	knowledgeEnabled bool
	knowledgeRT      *khruntime.KnowledgeRuntime
	knowledgeStore   knowledge.KnowledgeStore
	evolutionStore   *memStrategyStore
	// evoComponents holds the new evolution system (genome/diff/patch/coordinator)
	// wired to the live KnowledgeRuntime so evolution patches can affect the
	// running knowledge engine. Nil when evolution or knowledge is disabled.
	evoComponents *ares_bootstrap.NewEvolutionComponents
	eventStore    ares_events.EventStore
	mcpClients    []*mcp.Client
	trace         bool
	// bootstrap holds the Bootstrap-assembled core components (Stage 8): when
	// non-nil, the SDK reuses the same EventStore / NewEvolution / System
	// Runtime instances as serve and start instead of a parallel graph, and
	// Close drains Bootstrap's background goroutines via WaitBackground.
	bootstrap *ares_bootstrap.Components
	// bootstrapCancel cancels the Bootstrap lifecycle context; stored so Close
	// stops Bootstrap's background goroutines before WaitBackground drains them.
	bootstrapCancel context.CancelFunc
	// evidencePool, when non-nil, is a PostgreSQL pool created for the
	// evidence store (T1.3). Closed in Close() to prevent connection leaks.
	// Typed as *postgres.Pool (not io.Closer) so a nil pool stays a nil
	// pointer — assigning a nil *postgres.Pool to an interface would make
	// the interface non-nil and Close() would dereference a nil db.
	evidencePool *postgres.Pool
	// ctx governs the lifetime of background goroutines (event-driven
	// distillation subscriber). Cancelled in Close so subscribers exit cleanly.
	ctx context.Context
	// cancel stops background goroutines started in New.
	cancel context.CancelFunc
	// eg tracks background goroutines so Close can wait for in-flight work.
	eg *errgroup.Group
	// distillSvc consumes TaskCompleted events and distills them into long-term
	// experiences. Nil when distillation is disabled or its deps are unavailable.
	distillSvc *aresexp.DistillationService
	// akgBridge distills conversations into AKG KnowledgeObjects and persists
	// them through the quality gate into the knowledge store. Triggered
	// best-effort from the event subscriber alongside distillSvc. Nil when the
	// AKG distiller or knowledge store is unavailable.
	akgBridge *adapter.DistillBridge
	// agentByCapability maps a capability to the agent registered to handle it
	// (H1: 极简 SDK 调度面 — RegisterAgent/Submit). Guarded by agentMu.
	agentByCapability map[string]*Agent
	agentMu           sync.Mutex
	// ---- shared scheduler (H1/H2 merge) ----
	// sdkExecutors maps capability → the shared-scheduler executor wrapping
	// the registered agent. Guarded by agentMu (same lock as
	// agentByCapability). The map is passed BY REFERENCE to the shared
	// scheduler, so late RegisterAgent calls are visible to the next drain.
	sdkExecutors map[string]kernel.CapabilityExecutor
	// sdkFabric is the runtime's own Task Fabric; sched is the shared
	// kernel.Scheduler driving submitted tasks (the SAME engine the
	// kernel uses). Lazily started on the first Submit; schedOnce guards it.
	sdkFabric   *taskfabric.Fabric
	sched       *kernel.Scheduler
	schedOnce   sync.Once
	schedCtx    context.Context
	schedCancel context.CancelFunc
	// agentsFabric is the runtime's Agent Fabric, backing spawn_agent syscalls
	// (D1: SDK now wires the same kernel syscalls as peer mode). Created in
	// ensureScheduler alongside sdkFabric; nil until the first Submit.
	agentsFabric *agentfabric.Fabric
	// syscallTools are the LLM-facing spawn_agent/create_task definitions
	// appended to every agent's tool list so SDK users can autonomously
	// decompose tasks. Populated by wireSyscalls; nil before the first
	// Submit.
	syscallTools []core.Tool
	// syscallKernel is the agentsyscall kernel built by wireSyscalls, kept so
	// the loop lifetime (WithLoopLifetime) wiring is observable and plan-loop
	// control (LivePlanLoops/StopPlanLoop) is reachable from the runtime. Nil
	// before the first Submit.
	syscallKernel *agentsyscall.Kernel
}

// ---- constructors ----

// NewRuntime creates and returns a new ARES Runtime — the top-level container that
// owns the LLM client, tool registry, memory/distillation engine, AKG knowledge
// fabric, evolution system, and MCP connections.
//
// It panics on error so it is safe for quickstart / prototyping code.
// Use New for production code that wants to handle errors gracefully.
//
// Quick start:
//
//	ares := sdk.NewRuntime(sdk.WithConfigFromEnv())
//	defer ares.Close()
//	agent := ares.NewAgent("assistant")
//	result, _ := agent.Run(ctx, "hello")
func NewRuntime(opts ...Option) *Runtime {
	r, err := New(opts...)
	if err != nil {
		panic("ares: " + err.Error())
	}
	return r
}

// New creates and returns a new ARES Runtime. It wires the LLM client, tool
// registry, memory/distillation engine, RAG retrievers, AKG knowledge fabric,
// MCP connections, evolution system, and event-driven distillation.
//
// Returns an error when a required option (e.g. an LLM provider) cannot be
// initialised. Use NewRuntime for quickstart code that panics on error instead.
func New(opts ...Option) (*Runtime, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, fmt.Errorf("option: %w", err)
		}
	}
	// Early misconfiguration signal: a hosted provider without a key only
	// surfaced as a provider-side 401 on the first Run, far from the option
	// call that caused it. Warn here so the gap is visible at construction;
	// construction itself stays non-fatal (key-less gateways are legitimate).
	if hint := providerKeyHint(cfg.llmCfg.Provider, cfg.llmCfg.APIKey); hint != "" {
		slog.Warn(hint)
	}

	// ---- LLM ----
	llmCfg := &llm.Config{
		BaseConfig: cfg.baseCfg,
		LLMConfig:  cfg.llmCfg,
		Fallbacks:  cfg.fallbacks,
	}
	llmSvc, err := llm.NewService(llmCfg)
	if err != nil {
		return nil, agentloop.FriendlyErr("llm", cfg.llmCfg.Provider, err)
	}

	toolReg := tools.NewRegistry()

	// ---- Stage 8: assemble the core component graph through the single
	// Bootstrap kernel so the SDK reuses the same EventStore / NewEvolution /
	// System Runtime instances as serve and start. Falls back to SDK wiring
	// when the config is not Bootstrap-capable (sqlite/extra providers) or
	// assembly fails, preserving prior behavior.
	// The bootstrap ctx is cancelled in Close so Bootstrap's background
	// goroutines exit before WaitBackground drains them. Ownership is
	// transferred to the Runtime on the success path; on any error path the
	// deferred cancel prevents a context leak (vet lostcancel).
	bootstrapCtx, bootstrapCancel := context.WithCancel(context.Background())
	bootstrapCancelTaken := false
	// mcpClients and bootstrapComp are declared here (before the cleanup defer)
	// so the deferred cleanup can reference them; variables referenced by a
	// defer must already be in scope at the defer statement.
	var mcpClients []*mcp.Client
	var bootstrapComp *ares_bootstrap.Components
	defer func() {
		if !bootstrapCancelTaken {
			// Error path: release everything created so far. The success path
			// sets bootstrapCancelTaken and hands ownership to Runtime.Close().
			bootstrapCancel()
			// Drain Bootstrap background goroutines (they exit on ctx.Done()) so
			// none outlives the failed construction, mirroring Runtime.Close().
			if bootstrapComp != nil {
				bootstrapComp.WaitBackground()
			}
			llmSvc.Close()
			for _, c := range mcpClients {
				_ = c.Close()
			}
		}
	}()
	bootstrapComp = newBootstrapCore(bootstrapCtx, cfg)

	// ---- Memory (production MemoryManager: compression + RAG + distillation) ----
	var memMgr memory.MemoryManager
	var distillCleanup func()
	var embClient apiembed.EmbeddingService
	var expRepo repositories.ExperienceRepositoryInterface
	var distillSvc *aresexp.DistillationService
	var akgDistiller adapter.ConversationDistiller
	if cfg.memCfg.Enabled {
		w, err := wireMemory(context.Background(), cfg)
		if err != nil {
			return nil, fmt.Errorf("memory: %w", err)
		}
		memMgr = w.mgr
		embClient = w.embClient
		expRepo = w.expRepo
		distillCleanup = w.cleanup
		distillSvc = w.distillSvc
		akgDistiller = w.akgDistiller
	}

	// ---- MCP ----
	mcpClients, err = wireMCPClients(cfg, toolReg)
	if err != nil {
		return nil, err
	}

	// ---- AKF Knowledge Fabric ----
	embModelForAKG := resolveAKGEmbeddingModel(cfg)
	kw, err := wireKnowledge(cfg, memMgr, embClient, embModelForAKG)
	if err != nil {
		return nil, err
	}

	// ---- Stage 9 (SDK unification): keep the SDK's own KnowledgeRuntime
	// (its providers carry the live memSearcher/embedding backends) and bind
	// the Bootstrap NewEvolution's KnowledgePatchExecutor to THAT instance via
	// UpdateLiveKnowledgeRuntime. This satisfies §5.2 (KnowledgePatchExecutor
	// and AKF tools share one runtime) without replacing the SDK runtime with
	// the Bootstrap one, whose memory provider has no searcher.
	if bootstrapComp != nil && bootstrapComp.NewEvolution != nil && kw.rt != nil {
		bootstrapComp.NewEvolution.UpdateLiveKnowledgeRuntime(kw.rt)
	}

	// ---- AKF knowledge tools (auto-registered so the agent can call them) ----
	if cfg.knlCfg.Enabled && kw.rt != nil {
		if err := registerAKFTools(toolReg, kw.rt); err != nil {
			return nil, fmt.Errorf("akf tools: %w", err)
		}
	}

	// ---- Evolution hot-update + evidence store ----
	// Stage 8: reuse the Bootstrap-assembled NewEvolution when available;
	// otherwise keep the SDK dual-track wiring as a compatibility fallback.
	// (wireSDKEvolution owns the T1.3 evidence-persistence gating.)
	evoComponents, pgPool, err := wireSDKEvolution(cfg, kw, bootstrapComp)
	if err != nil {
		return nil, err
	}

	// ---- RAG retriever wiring (best-effort, non-fatal) ----
	if cfg.memCfg.EnableRAG && memMgr != nil {
		wireSDKRetrievers(context.Background(), cfg, memMgr, embClient, expRepo,
			kw.rt, kw.store, embModelForAKG)
	}

	// ---- AKG DistillBridge (write loop: conversations → knowledge store) ----
	akgBridge := buildAKGBridge(cfg, akgDistiller, kw.store, embClient, embModelForAKG)

	// ---- Event backend ----
	// Stage 8: when the Bootstrap core is available, subscribe distillation to
	// Bootstrap's shared EventStore (single store across entry points) instead
	// of a private SDK store; otherwise fall back to the SDK event backend.
	rtCtx, rtCancel, eg, eventStore := wireSDKEventBackend(bootstrapComp, distillSvc, akgBridge)

	runtime := &Runtime{
		llmSvc:            llmSvc,
		toolReg:           toolReg,
		memMgr:            memMgr,
		distillCleanup:    distillCleanup,
		memEnabled:        cfg.memCfg.Enabled,
		evoEnabled:        cfg.evoCfg.Enabled,
		knowledgeEnabled:  cfg.knlCfg.Enabled,
		knowledgeRT:       kw.rt,
		knowledgeStore:    kw.store,
		evolutionStore:    kw.evolutionStore,
		evoComponents:     evoComponents,
		eventStore:        eventStore,
		mcpClients:        mcpClients,
		trace:             cfg.trace,
		bootstrap:         bootstrapComp,
		bootstrapCancel:   bootstrapCancel,
		evidencePool:      pgPool,
		ctx:               rtCtx,
		cancel:            rtCancel,
		eg:                eg,
		distillSvc:        distillSvc,
		akgBridge:         akgBridge,
		agentByCapability: make(map[string]*Agent),
		sdkExecutors:      make(map[string]kernel.CapabilityExecutor),
	}
	// Transfer Bootstrap ctx ownership to the Runtime on the success path so
	// the deferred cancel above does not fire; Close owns cancellation now.
	bootstrapCancelTaken = true
	return runtime, nil
}

// Close releases all resources held by the Runtime (LLM connections, memory
// store, MCP connections). Call once when the Runtime is no longer needed.
func (r *Runtime) Close() {
	// Stop the shared scheduler's drain loop first (H1/H2 merge): it runs on
	// its own context so a Submit in flight is cancelled before the executor
	// agents and stores it depends on are torn down.
	if r.schedCancel != nil {
		r.schedCancel()
	}
	// Stop background goroutines (event-driven distillation subscriber) first
	// and wait for in-flight work, so the subscriber stops accepting new events
	// before the stores/clients it depends on are torn down. Best-effort: the
	// subscriber returns nil on ctx cancellation.
	if r.cancel != nil {
		r.cancel()
	}
	if r.eg != nil {
		_ = r.eg.Wait()
	}
	// Stage 8 (SDK unification): when the Runtime is backed by the Bootstrap
	// core, cancel its lifecycle context FIRST (so Bootstrap's background
	// goroutines — distillation subscriber, GA ticker, LLM suggestion ticker —
	// exit on ctx.Done()), then drain them through the SAME lifecycle kernel as
	// serve/start — WaitBackground — so no goroutine outlives Close. Fallback
	// SDK wiring (sqlite/extra providers) has no Bootstrap core and is skipped.
	if r.bootstrap != nil {
		if r.bootstrapCancel != nil {
			r.bootstrapCancel()
		}
		r.bootstrap.WaitBackground()
	}
	// Close the evidence PostgreSQL pool to prevent connection leaks (T1.3).
	// The pool is nil when no Postgres was configured, so this is a safe no-op.
	if r.evidencePool != nil {
		_ = r.evidencePool.Close()
	}
	r.llmSvc.Close()
	if r.memMgr != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		_ = r.memMgr.Stop(stopCtx)
	}
	if r.distillCleanup != nil {
		r.distillCleanup()
	}
	for _, c := range r.mcpClients {
		_ = c.Close()
	}
}

// Snapshot returns the system-level component status snapshot from the
// Bootstrap core (Stage 1 observability). Returns an empty snapshot when
// the Runtime is not backed by Bootstrap (SDK-only options) or when
// Bootstrap failed before wiring completed — callers can always consume
// a valid value without nil guards.
func (r *Runtime) Snapshot() kernel.Snapshot {
	if r.bootstrap == nil {
		return kernel.Snapshot{}
	}
	return r.bootstrap.Snapshot()
}

// ToolRegistry returns the internal tool registry. Use this to register custom
// tools before creating agents.
func (r *Runtime) ToolRegistry() *tools.Registry {
	return r.toolReg
}

// GetModel returns the LLM model name used by this Runtime.
func (r *Runtime) GetModel() string {
	return r.llmSvc.GetModel()
}

// GetProvider returns the LLM provider name used by this Runtime.
func (r *Runtime) GetProvider() string {
	return string(r.llmSvc.GetProvider())
}

// KnowledgeStore returns the knowledge store, or nil if knowledge is not
// enabled. The concrete type depends on the SDK options used: in-memory by
// default, SQLite via WithSQLiteKnowledgeStore, or PostgreSQL via
// WithPostgres. Use this to save and query KnowledgeObjects directly.
func (r *Runtime) KnowledgeStore() knowledge.KnowledgeStore {
	return r.knowledgeStore
}

// NewAgent creates a new Agent bound to this Runtime. The agent carries a name,
// an optional system instruction, and an optional set of tools.
func (r *Runtime) NewAgent(name string, opts ...AgentOption) *Agent {
	ac := defaultAgentConfig()
	for _, o := range opts {
		o(ac)
	}
	return &Agent{
		name:        name,
		instruction: ac.instruction,
		tools:       ac.tools,
		runtime:     r,
		humanInput:  ac.humanInput,
		maxIter:     ac.maxIter,
		maxTokens:   ac.maxTokens,
		timeout:     ac.timeout,
		discovery:   ac.discovery,
		toolSource:  ac.toolSource,
		selector:    ac.selector,
	}
}

// wireMCPClients connects to each configured MCP server, lists its tools, and
// registers them into the SDK tool registry. Extracted from New() to keep the
// constructor under the 100-line limit.
//
// Args:
//
//	cfg     - fully applied SDK config; mcpConns is read.
//	toolReg - the SDK tool registry; MCP tools are registered by name.
//
// Returns:
//
//	[]*mcp.Client - one client per configured MCP connection (empty when none).
//	error         - wrapped with context if a connection, list, or register fails.
func wireMCPClients(cfg *config, toolReg *tools.Registry) ([]*mcp.Client, error) {
	var mcpClients []*mcp.Client
	// On any failure, close every client already connected so a partial
	// connection is not leaked when New() returns the error.
	closeAll := func() {
		for _, c := range mcpClients {
			_ = c.Close()
		}
	}
	for _, conn := range cfg.mcpConns {
		connectCtx, connectCancel := context.WithTimeout(context.Background(), 30*time.Second)
		client, err := mcp.ConnectStdio(connectCtx, conn.Name, conn.Command, conn.Args)
		connectCancel()
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("mcp %q: %w", conn.Name, err)
		}
		listCtx, listCancel := context.WithTimeout(context.Background(), 30*time.Second)
		mcpTools, listErr := client.ListTools(listCtx)
		listCancel()
		if listErr != nil {
			_ = client.Close()
			closeAll()
			return nil, fmt.Errorf("mcp %q list tools: %w", conn.Name, listErr)
		}
		for _, t := range mcpTools {
			if err := toolReg.Register(mcpToolAdapter{
				name:   t.Name,
				desc:   t.Description,
				client: client,
			}); err != nil {
				_ = client.Close()
				closeAll()
				return nil, fmt.Errorf("mcp %q register %s: %w", conn.Name, t.Name, err)
			}
		}
		mcpClients = append(mcpClients, client)
	}
	return mcpClients, nil
}
