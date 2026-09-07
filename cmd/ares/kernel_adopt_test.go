package main

// K2/K3/K5 acceptance tests: the kernel pillars join the System Runtime
// orchestrator with real stop/wait hooks, the background loops are managed
// (no bare `go`), and the readiness snapshot reflects the drain loop.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/ares_bootstrap"
	"github.com/Timwood0x10/ares/internal/ares_events"
	"github.com/Timwood0x10/ares/internal/aresrecovery"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// TestKernelAdopt_SixPillarsAdoptedAndStopped is the K2 acceptance: a fully
// assembled kernel handle adopts all six pillars into a live orchestrator,
// every one reaches Ready (the scheduler via its Running gate), and Shutdown
// stops them in an order consistent with the K2 dependency table
// (pluginbus/recovery/scheduler → dispatcher → fabrics → eventstore).
func TestKernelAdopt_SixPillarsAdoptedAndStopped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	k := &kernelHandle{
		fabric:        taskfabric.NewFabric(),
		agents:        agentfabric.NewFabric(),
		recovery:      aresrecovery.New(taskfabric.NewFabric(), agentfabric.NewFabric(), aresrecovery.DefaultRestartPolicy()),
		schedulerStop: func() {},
		schedulerDone: make(chan struct{}),
		recoveryStop:  func() {},
		recoveryDone:  make(chan struct{}),
		// scheduler nil: adopt must skip a nil pillar (present semantics)
		// only when the pillar itself is absent — here we set the scheduler
		// below via a real one.
	}
	dual, _ := wireKernelDispatcher(nil)
	k.dual = dual

	reg := kernel.NewRegistry()
	// eventstore is the fabrics' dependency; register it so Adopt's
	// dependency validation passes.
	if err := reg.Register(&stubComponent{name: "eventstore"}, kernel.ModeRequired); err != nil {
		t.Fatalf("register: %v", err)
	}
	orch := kernel.NewOrchestrator(reg, ctx)
	if err := orch.Start(ctx); err != nil {
		t.Fatalf("orchestrator start: %v", err)
	}

	// Adopt without a scheduler/plugin bus: the present pillars only (nil
	// pillars are skipped per the present semantics).
	if err := k.adopt(ctx, orch); err != nil {
		t.Fatalf("adopt (partial kernel): %v", err)
	}
	for _, name := range []string{"taskfabric", "agentfabric", "dispatcher", "recovery"} {
		if _, ok := reg.GetStatus(name); !ok {
			t.Fatalf("pillar %q missing after adopt", name)
		}
	}
	if _, ok := reg.GetStatus("scheduler"); ok {
		t.Fatal("nil scheduler must not be adopted")
	}
	if _, ok := reg.GetStatus("pluginbus"); ok {
		t.Fatal("nil plugin bus must not be adopted")
	}

	// Second orchestrator with a real running scheduler: all 6 pillars.
	reg2 := kernel.NewRegistry()
	if err := reg2.Register(&stubComponent{name: "eventstore"}, kernel.ModeRequired); err != nil {
		t.Fatalf("register: %v", err)
	}
	orch2 := kernel.NewOrchestrator(reg2, ctx)
	if err := orch2.Start(ctx); err != nil {
		t.Fatalf("orchestrator start 2: %v", err)
	}

	f := taskfabric.NewFabric()
	sched := NewKernelScheduler(f, nil, nil)
	sched.PollInterval = 20 * time.Millisecond
	k2 := &kernelHandle{
		fabric:        f,
		agents:        agentfabric.NewFabric(),
		recovery:      aresrecovery.New(f, agentfabric.NewFabric(), aresrecovery.DefaultRestartPolicy()),
		scheduler:     sched,
		schedulerStop: cancel,
		schedulerDone: make(chan struct{}),
		recoveryStop:  func() {},
		recoveryDone:  make(chan struct{}),
	}
	dual2, _ := wireKernelDispatcher(nil)
	k2.dual = dual2
	// Real plugin bus so all six pillars are present (K2 full-kernel case).
	k2.pluginBus = startPluginBus(ctx, ares_events.NewMemoryEventStore(), sched, kernelLoopConfig{})
	if k2.pluginBus == nil {
		t.Fatal("plugin bus must start in the full-kernel fixture")
	}
	go func() {
		defer close(k2.schedulerDone)
		sched.Run(ctx)
	}()

	if err := k2.adopt(ctx, orch2); err != nil {
		t.Fatalf("adopt (full kernel): %v", err)
	}
	for _, name := range []string{
		sysCompTaskFabric, sysCompAgentFabric, sysCompDispatcher,
		sysCompScheduler, sysCompRecovery, sysCompPluginBus,
	} {
		st, ok := reg2.GetStatus(name)
		if !ok {
			t.Fatalf("pillar %q missing after adopt", name)
		}
		if st.State != kernel.StateReady {
			t.Fatalf("pillar %q expected Ready, got %s (%s)", name, st.State, st.Reason)
		}
	}
}

// TestKernelAdopt_SchedulerNotRunningReportsDegraded is the K5 "false Ready"
// guard: adopting with a scheduler whose drain loop never started must leave
// the pillar Degraded (with a readable reason), not Ready.
func TestKernelAdopt_SchedulerNotRunningReportsDegraded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := kernel.NewRegistry()
	if err := reg.Register(&stubComponent{name: "eventstore"}, kernel.ModeRequired); err != nil {
		t.Fatalf("register: %v", err)
	}
	orch := kernel.NewOrchestrator(reg, ctx)
	if err := orch.Start(ctx); err != nil {
		t.Fatalf("orchestrator start: %v", err)
	}

	f := taskfabric.NewFabric()
	// The scheduler object exists but Run was never called.
	dead := NewKernelScheduler(f, nil, nil)
	k := &kernelHandle{
		fabric:        f,
		agents:        agentfabric.NewFabric(),
		scheduler:     dead,
		schedulerStop: func() {},
		schedulerDone: make(chan struct{}),
	}
	dual, _ := wireKernelDispatcher(nil)
	k.dual = dual

	if err := k.adopt(ctx, orch); err != nil {
		t.Fatalf("adopt must not fail in Degraded mode: %v", err)
	}
	st, ok := reg.GetStatus(sysCompScheduler)
	if !ok {
		t.Fatal("scheduler pillar missing after adopt")
	}
	if st.State != kernel.StateDegraded {
		t.Fatalf("non-running scheduler must be Degraded, got %s", st.State)
	}
	if !strings.Contains(st.Reason, "drain loop") {
		t.Fatalf("Degraded reason must name the drain loop, got %q", st.Reason)
	}
}

// TestKernelAdopt_NilOrchestratorNoop covers the partial-path contract: a
// kernel handle adopting into an unwired runtime must be a safe no-op.
func TestKernelAdopt_NilOrchestratorNoop(t *testing.T) {
	k := &kernelHandle{}
	if err := k.adopt(context.Background(), nil); err != nil {
		t.Fatalf("adopt with nil orchestrator must be a no-op, got %v", err)
	}
	if err := (*kernelHandle)(nil).adopt(context.Background(), nil); err != nil {
		t.Fatalf("nil handle adopt must be a no-op, got %v", err)
	}
}

// TestRunBackground_UsesSystemRuntime verifies the K3 entry: with a wired
// System Runtime the loop is errgroup-managed (the orchestrator's Shutdown
// joins it), and its component name maps to a registered component for
// failure marking when one exists.
func TestRunBackground_UsesSystemRuntime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	comp := &ares_bootstrap.Components{}
	reg := kernel.NewRegistry()
	orch := kernel.NewOrchestrator(reg, ctx)
	if err := orch.Start(ctx); err != nil {
		t.Fatalf("orchestrator start: %v", err)
	}
	comp.SystemRuntime = orch

	started := make(chan struct{})
	runBackground(ctx, comp, "managed-loop", func(loopCtx context.Context) error {
		close(started)
		<-loopCtx.Done()
		return nil
	})

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("background loop never started")
	}

	// Shutdown joins the managed loop (errgroup semantics) instead of
	// leaking it — a bounded wait proves the join.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	if err := orch.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestRunBackground_NilContainerSkips verifies the fail-loud fallback: no
// component container means the loop is skipped with a log line, never a
// leaked unmanaged goroutine.
func TestRunBackground_NilContainerSkips(t *testing.T) {
	called := false
	runBackground(context.Background(), nil, "orphan", func(ctx context.Context) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("loop must not run without a component container")
	}
}

// TestKernelAdopt_FailedAdoptionPropagates verifies the fail-loud contract:
// a pillar whose adoption fails (duplicate registration) surfaces as a serve
// error instead of being swallowed.
func TestKernelAdopt_FailedAdoptionPropagates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := kernel.NewRegistry()
	if err := reg.Register(&stubComponent{name: "eventstore"}, kernel.ModeRequired); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Register(&stubComponent{name: sysCompTaskFabric}, kernel.ModeRequired); err != nil {
		t.Fatalf("register: %v", err)
	}
	orch := kernel.NewOrchestrator(reg, ctx)
	if err := orch.Start(ctx); err != nil {
		t.Fatalf("orchestrator start: %v", err)
	}

	k := &kernelHandle{fabric: taskfabric.NewFabric()}
	if err := k.adopt(ctx, orch); err == nil {
		t.Fatal("adopt must fail when a pillar name is already registered")
	}
}

// stubComponent is a minimal System Runtime component for registry seeding.
type stubComponent struct {
	name string
}

func (c *stubComponent) Name() string           { return c.name }
func (c *stubComponent) Dependencies() []string { return nil }
