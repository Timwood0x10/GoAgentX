// Package introspect — one-line dashboard runtime.
//
// NewDashboard assembles a complete observable ARES peer runtime behind a
// single handle: real LLM agents, kernel scheduler, task/agent fabrics, the
// introspection collector/sink, and the HTTP panel — then Run starts it and
// Submit dispatches tasks. Callers never touch the internal plumbing; the
// dashboard owns data collection and serving.
package introspect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	api_tools "github.com/Timwood0x10/ares/api/tools"
	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agentipc"
	"github.com/Timwood0x10/ares/internal/agents/sub"
	"github.com/Timwood0x10/ares/internal/ares_config"
	"github.com/Timwood0x10/ares/internal/ares_events"
	"github.com/Timwood0x10/ares/internal/aresrecovery"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/Timwood0x10/ares/internal/llm"
	"github.com/Timwood0x10/ares/internal/llm/output"
	"github.com/Timwood0x10/ares/internal/logger"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// AgentSpec describes one real-LLM peer agent the dashboard spawns.
type AgentSpec struct {
	// ID is the agent's identity (e.g. "coder-1").
	ID string
	// Capabilities the agent declares; the scheduler scores tasks against
	// these.
	Capabilities []string
	// Instruction is the system prompt shaping how the agent behaves.
	Instruction string
}

// DashboardConfig controls NewDashboard.
type DashboardConfig struct {
	// ConfigPath points at an ares.yaml with live LLM credentials. Empty
	// falls back to a minimal in-memory demo LLM so the panel renders even
	// without a key.
	ConfigPath string
	// Addr is the HTTP listen address for the panel (default ":5606").
	Addr string
	// Agents is the peer population to spawn. When empty, the dashboard
	// spawns no agents (the panel still renders an empty runtime).
	Agents []AgentSpec
	// MaxConcurrent caps how many tasks run in parallel (default 3).
	MaxConcurrent int
	// LeaseTTL is the scheduler lease duration (default 60s).
	LeaseTTL time.Duration
	// Workload, when set, runs a workload driver (submit tasks / exercise
	// chaos) after Run starts. The dashboard passes itself so the callback can
	// Submit tasks.
	Workload func(ctx context.Context, d *Dashboard)
}

// Dashboard is a self-contained, observable ARES runtime: it owns the task
// fabric, agent fabric, scheduler, LLM wiring, the introspection collector
// and the HTTP panel. Create with NewDashboard, start with Run, dispatch with
// Submit.
type Dashboard struct {
	mu sync.Mutex

	cfg    DashboardConfig
	store  ares_events.EventStore
	tasks  *taskfabric.Fabric
	agents *agentfabric.Fabric
	sched  *kernel.Scheduler
	panel  *Store
	bus    *agentipc.Bus
	collab *CollabReporter
	chaos  *ChaosReporter

	collector *Collector
	sink      *Sink

	taskSeq  int
	httpSrv  *http.Server
	started  bool
	peerIDs  []string
	workload func(ctx context.Context, d *Dashboard)
}

// NewDashboard assembles the full runtime from config. All plumbing — LLM
// adapter, failover client, tool registry, fabrics, scheduler, chaos observer,
// panel collector and HTTP handler — is created here; Run starts it.
func NewDashboard(ctx context.Context, cfg DashboardConfig) (*Dashboard, error) {
	cfg = applyDashboardDefaults(cfg)

	d := &Dashboard{
		cfg: cfg,
		// WithLogger: handler panics are contained at the bus goroutine
		// boundary (P1-3); the bus itself never prints (code_rules §9.1), so
		// without a logger a contained panic would be invisible here too.
		bus:    agentipc.NewBus().WithLogger(logger.Module("introspect")),
		collab: NewCollabReporter(),
		chaos:  NewChaosReporter(),
	}

	// Event store + fabrics (the single data plane).
	d.store = ares_events.NewMemoryEventStore()
	d.tasks = taskfabric.NewFabric().WithEventStore(d.store)
	d.agents = agentfabric.NewFabric().WithEventSink(&fabricEventSink{store: d.store})

	// LLM wiring. ConfigPath is required — the dashboard needs real LLM
	// credentials to create agents that produce real work. A missing config
	// or adapter failure is a startup error, not a silent fallback.
	var llmAdapter output.LLMAdapter
	var chatClient sub.ChatClient
	if cfg.ConfigPath == "" {
		return nil, errors.New("dashboard: ConfigPath is required (point at an ares.yaml with live LLM credentials)")
	}
	acfg, err := ares_config.Load(cfg.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("dashboard: load config %s: %w", cfg.ConfigPath, err)
	}
	_ = ares_config.LoadFromEnv(acfg)
	factory := output.NewFactory()
	llmAdapter, err = factory.Create(acfg.LLM.Provider, &output.Config{
		Provider:  acfg.LLM.Provider,
		APIKey:    acfg.LLM.APIKey,
		BaseURL:   acfg.LLM.BaseURL,
		Model:     acfg.LLM.Model,
		Timeout:   acfg.LLM.Timeout,
		MaxTokens: acfg.LLM.MaxTokens,
	})
	if err != nil {
		return nil, fmt.Errorf("dashboard: LLM adapter: %w", err)
	}
	chatClient = buildFailoverClient(acfg)
	if chatClient == nil {
		return nil, errors.New("dashboard: failover client creation failed")
	}

	// Tool registry (built-in tools) + binder.
	toolReg := api_tools.NewRegistry()
	coreReg, err := toolReg.CoreRegistry()
	if err != nil {
		return nil, fmt.Errorf("dashboard: core registry: %w", err)
	}
	toolBinder := sub.NewToolBinder()
	toolBinder.BridgeFromRegistry(coreReg)

	// Spawn the peer population (real LLM when available, demo otherwise).
	for _, spec := range cfg.Agents {
		if _, err := d.spawnAgent(ctx, spec, llmAdapter, chatClient, toolBinder); err != nil {
			log.Error("dashboard: spawn agent failed", "agent_id", spec.ID, "error", err)
			continue
		}
		d.peerIDs = append(d.peerIDs, spec.ID)
	}

	// Scheduler.
	tracker := kernel.NewLoadTracker()
	sched := kernel.New(d.tasks, map[string]kernel.CapabilityExecutor{}, tracker)
	sched.WithEventStore(d.store).
		WithAgentFabric(d.agents).
		WithGovernance(d.agents).
		WithMaxConcurrent(cfg.MaxConcurrent).
		WithTTL(cfg.LeaseTTL)
	d.sched = sched

	// Panel read-model.
	d.panel = &Store{}
	collector := NewCollector(Sources{
		Kernel:    sched.Snapshot,
		Fabric:    d.tasks.LeaseSnapshot,
		Agents:    d.agents.AgentsView,
		Chaos:     d.chaos.Snapshot,
		Collab:    d.collab.Snapshot,
		Tasks:     d.tasks.TaskSnapshot,
		Decisions: sched.DecisionsSnapshot,
	})
	handler := NewHandler(d.panel).WithEventStore(d.store)
	d.sink = NewSink(d.panel)
	d.collector = collector

	// HTTP server.
	addr := cfg.Addr
	if addr == "" {
		addr = ":5606"
	}
	mux := http.NewServeMux()
	mux.Handle("/introspect", handler)
	mux.Handle("/introspect/", handler)
	mux.Handle("/api/v1/introspect/", handler)
	d.httpSrv = &http.Server{Addr: addr, Handler: mux, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second}

	// Workload driver (if any) + chaos shadow sandbox (defaults on, live off).
	d.workload = cfg.Workload
	d.chaos.SetConfig(true, "shadow")

	return d, nil
}

func applyDashboardDefaults(cfg DashboardConfig) DashboardConfig {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 3
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 60 * time.Second
	}
	return cfg
}

// buildFailoverClient assembles the LLM chat client with fallback chain.
func buildFailoverClient(cfg *ares_config.Config) sub.ChatClient {
	configs := make([]*llm.Config, 0, 1+len(cfg.LLM.Fallbacks))
	configs = append(configs, &llm.Config{
		Provider: cfg.LLM.Provider, APIKey: cfg.LLM.APIKey, BaseURL: cfg.LLM.BaseURL,
		Model: cfg.LLM.Model, Timeout: cfg.LLM.Timeout, MaxTokens: cfg.LLM.MaxTokens,
	})
	for _, fb := range cfg.LLM.Fallbacks {
		prov := fb.Provider
		if prov == "" {
			prov = "openai"
		}
		configs = append(configs, &llm.Config{
			Provider: prov, APIKey: fb.APIKey, BaseURL: fb.BaseURL,
			Model: fb.Model, Timeout: fb.Timeout, MaxTokens: fb.MaxTokens,
		})
	}
	timeout := time.Duration(cfg.LLM.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	client, err := llm.NewFailoverClient(configs, timeout, cfg.LLM.ScorerAPIRate, cfg.LLM.ScorerAPIBurst)
	if err != nil {
		log.Error("dashboard: failover client init failed", "error", err)
		return nil
	}
	return client
}

// spawnAgent creates one peer agent with the real LLM ChatCognition. The
// agent registers on the IPC bus and spawns into the fabric as a schedulable
// candidate. The instruction (system prompt) shapes how the agent behaves;
// when empty, a neutral default is used.
func (d *Dashboard) spawnAgent(ctx context.Context, spec AgentSpec, llmAdapter output.LLMAdapter, chatClient sub.ChatClient, toolBinder sub.ToolBinder) (*agentfabric.Agent, error) {
	agentID := spec.ID
	if err := d.bus.Register(agentID, func(ctx context.Context, msg *agentipc.Message) (*agentipc.Message, error) {
		return &agentipc.Message{From: agentID, To: msg.From, Topic: "ack", Payload: map[string]any{"status": "ok"}}, nil
	}); err != nil {
		// B6: duplicate agent ID registration must not be silently
		// swallowed; log and skip so the caller can handle the conflict.
		log.Error("introspect: bus register failed, skipping agent", "agent_id", agentID, "error", err)
		return nil, fmt.Errorf("register agent %s on bus: %w", agentID, err)
	}

	prompt := spec.Instruction
	if prompt == "" {
		prompt = defaultAgentPrompt
	}
	cog, err := agentfabric.NewChatCognition(agentfabric.ChatCognitionDeps{
		ChatClient:     chatClient,
		LLMAdapter:     llmAdapter,
		ToolBinder:     toolBinder,
		Template:       output.NewTemplateEngine(),
		PromptTemplate: prompt,
		EventStore:     d.store,
		AgentID:        agentID,
	})
	if err != nil {
		return nil, fmt.Errorf("chat cognition: %w", err)
	}

	return d.agents.Spawn(ctx, agentfabric.SpawnSpec{
		Identity:         agentID,
		Capabilities:     spec.Capabilities,
		CognitionFactory: func([]string) agentfabric.Cognition { return cog },
	})
}

// defaultAgentPrompt is the neutral system prompt used when an agent spec
// provides no Instruction.
const defaultAgentPrompt = "You are a capable peer agent. Complete the task you are given, using tools when useful, and answer concisely."

// Run starts the runtime loops (scheduler, panel collector/sink, HTTP server)
// and blocks until ctx is cancelled. Errors during startup are returned;
// runtime errors are logged (the dashboard is best-effort).
func (d *Dashboard) Run(ctx context.Context) error {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return errors.New("dashboard: already started")
	}
	d.started = true
	d.mu.Unlock()

	go d.sched.Run(ctx)
	go d.runCollector(ctx)
	go d.runSink(ctx)
	go d.runShadowVerification(ctx)
	go func() {
		if err := d.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("dashboard: http server error", "error", err)
		}
	}()

	if d.workload != nil {
		go d.workload(ctx, d)
	}

	<-ctx.Done()
	_ = d.httpSrv.Close()
	return nil
}

func (d *Dashboard) runCollector(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.panel.Set(d.collector.Collect())
		}
	}
}

func (d *Dashboard) runSink(ctx context.Context) {
	if err := d.sink.Run(ctx, d.store); err != nil {
		log.Error("dashboard: sink run failed", "error", err)
	}
}

// runShadowVerification periodically re-runs the canonical kill→recover chain
// on a SCRATCH fabric and records the real outcome to the panel's chaos
// source. Production agents are never touched — the sandbox is independent.
func (d *Dashboard) runShadowVerification(ctx context.Context) {
	ticker := time.NewTicker(shadowVerifyInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.chaos.RecordShadow(runShadowSandbox(ctx))
		}
	}
}

// shadowVerifyInterval is how often the shadow sandbox re-verifies recovery.
const shadowVerifyInterval = 10 * time.Second

// Shadow-sandbox script identities (goconst: reused across the replay events).
const (
	shadowAgentID = "shadow-agent-1"
	shadowTaskID  = "shadow-task-1"
)

// runShadowSandbox builds a scratch Task+Agent fabric, replays the canonical
// agent-kill → lease-expire → recovery scenario and returns the outcome.
// RecoverFromAgentDeath re-acquires the requeued task for a replacement agent,
// so the reliable recovered signal is the recover.all outcome's count, not the
// task state.
func runShadowSandbox(ctx context.Context) ShadowResult {
	scratchTasks := taskfabric.NewFabric()
	scratchAgents := agentfabric.NewFabric()
	recovery := aresrecovery.New(scratchTasks, scratchAgents, aresrecovery.DefaultRestartPolicy())
	sandbox := aresrecovery.NewSandbox(scratchTasks, scratchAgents, recovery)

	events := []aresrecovery.SandboxEvent{
		{Type: aresrecovery.SandboxEventAgentSpawn, AgentID: shadowAgentID},
		{Type: aresrecovery.SandboxEventTaskCreate, TaskID: shadowTaskID},
		{Type: aresrecovery.SandboxEventTaskAcquire, TaskID: shadowTaskID, AgentID: shadowAgentID},
		{Type: aresrecovery.SandboxEventAgentKill, AgentID: shadowAgentID},
		{Type: aresrecovery.SandboxEventLeaseExpire, TaskID: shadowTaskID},
		{Type: aresrecovery.SandboxEventRecoverAll},
	}

	outcomes, err := sandbox.Replay(ctx, events)
	if err != nil {
		return ShadowResult{LastRun: time.Now(), Events: len(events), Errored: true}
	}
	if len(outcomes) == 0 {
		return ShadowResult{LastRun: time.Now(), Events: len(events), Errored: true}
	}
	last := outcomes[len(outcomes)-1]
	recovered, _ := last.Detail["recovered"].(int)
	return ShadowResult{
		LastRun:   time.Now(),
		Events:    len(outcomes),
		Recovered: recovered > 0,
	}
}

// Submit dispatches a new task into the fabric. The scheduler picks a capable
// agent and drives it to completion. Returns the assigned task id.
func (d *Dashboard) Submit(capability, input string) (string, error) {
	d.mu.Lock()
	d.taskSeq++
	seq := d.taskSeq
	d.mu.Unlock()

	taskID := fmt.Sprintf("task-%03d", seq)
	err := d.tasks.Create(&taskfabric.Task{
		ID:          taskID,
		Capability:  capability,
		RetryPolicy: taskfabric.RetryPolicy{MaxRetries: 0},
		Checkpoint:  &taskfabric.CheckpointEnvelope{Payload: map[string]any{"input": input}},
	})
	if err != nil {
		return "", fmt.Errorf("submit: %w", err)
	}
	log.Info("dashboard: submitted task", "task_id", taskID, "capability", capability, "input", input)
	return taskID, nil
}

// Peers returns the spawned agent ids.
func (d *Dashboard) Peers() []string {
	return append([]string(nil), d.peerIDs...)
}

// Scheduler returns the underlying kernel scheduler (read-only access for
// observability consumers).
func (d *Dashboard) Scheduler() *kernel.Scheduler { return d.sched }

// fabricEventSink forwards agentfabric lifecycle records onto the shared event
// bus so the panel's activity feed sees agent deaths/revivals immediately.
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
	payload := map[string]any{"agent_id": ev.AgentID}
	if reason != "" {
		payload["reason"] = reason
	}
	return f.store.Append(ctx, ev.AgentID, []*ares_events.Event{{
		Type: busType, ModuleName: "agentfabric", Payload: payload, Timestamp: ev.At,
	}}, 0)
}
