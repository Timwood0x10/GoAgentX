package main

// System Runtime adoption of the six kernel pillars (K2, 0.3.1 readiness
// plan): the kernel pillars are assembled LATER than the Bootstrap
// components, so they join the orchestrator through Orchestrator.Adopt
// instead of the startup-time Register. This file owns the component names,
// the dependency edges (which decide the reverse-topological shutdown
// order), the stop/wait hooks, and the unified background-loop entry.

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Timwood0x10/ares/internal/ares_bootstrap"
	"github.com/Timwood0x10/ares/internal/kernel"
)

// System Runtime registry names of the six kernel pillars (K2). The
// dependency edges below turn these names into the shutdown order
// pluginbus/recovery → scheduler → dispatcher → fabrics → eventstore.
const (
	sysCompScheduler   = "scheduler"
	sysCompTaskFabric  = "taskfabric"
	sysCompAgentFabric = "agentfabric"
	sysCompRecovery    = "recovery"
	sysCompDispatcher  = "dispatcher"
	sysCompPluginBus   = "pluginbus"
)

// adoptReadyPollInterval is the polling cadence of the scheduler readiness
// gate; adoptReadyPollBudget bounds the total wait.
const (
	adoptReadyPollInterval = 50 * time.Millisecond
	adoptReadyPollBudget   = 2 * time.Second
)

// kernelComponent adapts one kernel pillar to the System Runtime Component
// contract. Identity + dependency metadata drive the registry ordering;
// optional ready/stop/wait hooks let Adopt verify real readiness (K5: the
// scheduler must be draining to count as Ready) and let Shutdown drive real
// teardown (K2: cancel + wait the loop's goroutine). Nil hooks are safe
// no-ops, so passive pillars (the fabrics, the dispatcher — pure in-memory
// state machines with no goroutine of their own) still join the graph and
// the shutdown order.
type kernelComponent struct {
	name    string
	deps    []string
	mode    kernel.Mode
	readyFn func(ctx context.Context) error
	stopFn  func(ctx context.Context) error
	waitFn  func() error
}

// Name returns the stable component identifier.
func (a *kernelComponent) Name() string { return a.name }

// Dependencies returns the names of components that must exist (and not be
// Failed) before this one is adopted; they also decide the shutdown order.
func (a *kernelComponent) Dependencies() []string { return a.deps }

// Ready delegates to the optional readiness hook; nil means Ready by
// construction.
func (a *kernelComponent) Ready(ctx context.Context) error {
	if a.readyFn == nil {
		return nil
	}
	return a.readyFn(ctx)
}

// Stop delegates to the optional teardown hook; nil is a no-op.
func (a *kernelComponent) Stop(ctx context.Context) error {
	if a.stopFn == nil {
		return nil
	}
	return a.stopFn(ctx)
}

// Wait delegates to the optional wait hook; nil is a no-op.
func (a *kernelComponent) Wait() error {
	if a.waitFn == nil {
		return nil
	}
	return a.waitFn()
}

// schedulerReady verifies the drain loop is actually running (K5): it polls
// Scheduler.Running with a bounded budget so the natural delay between
// `go sched.Run(...)` and adoption does not produce a false Degraded, while
// a genuinely dead loop still reports a readable reason instead of Ready.
func schedulerReady(sched *kernelScheduler) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if sched.Running() {
			return nil
		}
		deadline := time.Now().Add(adoptReadyPollBudget)
		ticker := time.NewTicker(adoptReadyPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("scheduler drain loop not running (readiness check aborted: %v)", ctx.Err())
			case <-ticker.C:
				if sched.Running() {
					return nil
				}
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("scheduler drain loop not running after %s", adoptReadyPollBudget)
			}
		}
	}
}

// adopt registers the six kernel pillars with the System Runtime orchestrator
// (K2). Nil pillars are skipped (present semantics, mirroring
// registerSystemComponent): partial kernels (SDK-adjacent paths, tests) keep
// working. A non-nil error from Adopt fails the serve startup loudly — a
// kernel pillar that cannot join the managed graph would otherwise recreate
// the "false Ready" blind spot the K group exists to close.
func (k *kernelHandle) adopt(ctx context.Context, orch *kernel.Orchestrator) error {
	if k == nil {
		return nil
	}
	if orch == nil {
		log.Printf("serve: system runtime not wired; kernel components not adopted (unmanaged lifecycle)")
		return nil
	}

	// eventstore is the Bootstrap-registered dependency edge for both
	// fabrics; it decides that the fabrics stop BEFORE the store.
	eventstore := ares_bootstrap.SysCompEventStore

	components := []*kernelComponent{
		{
			// Passive state machine — no goroutine, nothing to stop. Its
			// presence in the graph orders the dispatcher/scheduler before
			// the event store at shutdown.
			name: sysCompTaskFabric,
			deps: []string{eventstore},
		},
		{
			// Passive population registry — same passive-stop semantics.
			name: sysCompAgentFabric,
			deps: []string{eventstore},
		},
		{
			// Dispatch is synchronous through the fabrics; stopping the
			// fabrics first (reverse topo) already halts new dispatch.
			name: sysCompDispatcher,
			deps: []string{sysCompTaskFabric, sysCompAgentFabric},
		},
		{
			// The scheduler owns the drain loop goroutine: Stop cancels its
			// context, Wait joins it. Degraded mode (K5): a loop that is not
			// running reports Degraded + reason, never a false Ready.
			name: sysCompScheduler,
			deps: []string{sysCompTaskFabric, sysCompAgentFabric, sysCompDispatcher},
			mode: kernel.ModeDegraded,
			readyFn: func(ctx context.Context) error {
				if k.scheduler == nil {
					return nil
				}
				return schedulerReady(k.scheduler)(ctx)
			},
			stopFn: func(ctx context.Context) error {
				if k.schedulerStop != nil {
					k.schedulerStop()
				}
				return nil
			},
			waitFn: func() error {
				if k.schedulerDone != nil {
					<-k.schedulerDone
				}
				return nil
			},
		},
		{
			// Recovery owns the requeue/restart loop goroutine.
			name: sysCompRecovery,
			deps: []string{sysCompTaskFabric, sysCompAgentFabric},
			stopFn: func(ctx context.Context) error {
				if k.recoveryStop != nil {
					k.recoveryStop()
				}
				return nil
			},
			waitFn: func() error {
				if k.recoveryDone != nil {
					<-k.recoveryDone
				}
				return nil
			},
		},
		{
			// PluginBus has a real Stop (plugin reverse-order teardown).
			name: sysCompPluginBus,
			deps: []string{sysCompScheduler},
			stopFn: func(ctx context.Context) error {
				if k.pluginBus == nil {
					return nil
				}
				return k.pluginBus.Stop(ctx)
			},
		},
	}

	for _, c := range components {
		if !k.componentPresent(c.name) {
			continue
		}
		mode := c.mode
		if mode == 0 {
			mode = kernel.ModeRequired
		}
		if err := orch.Adopt(ctx, c, mode); err != nil {
			return fmt.Errorf("serve: adopt kernel component %q: %w", c.name, err)
		}
	}
	log.Printf("serve: kernel components adopted into system runtime (scheduler/taskfabric/agentfabric/recovery/dispatcher/pluginbus)")
	return nil
}

// componentPresent reports whether the named pillar exists on this kernel
// handle (present semantics: nil pillars are skipped, not an error).
func (k *kernelHandle) componentPresent(name string) bool {
	switch name {
	case sysCompTaskFabric:
		return k.fabric != nil
	case sysCompAgentFabric:
		return k.agents != nil
	case sysCompDispatcher:
		return k.dual != nil
	case sysCompScheduler:
		return k.scheduler != nil
	case sysCompRecovery:
		return k.recovery != nil
	case sysCompPluginBus:
		return k.pluginBus != nil
	default:
		return false
	}
}

// runBackground starts a managed background loop (K3: no bare `go` on the
// serve path). With a wired System Runtime the loop joins the orchestrator's
// errgroup under the given name — a panic marks the component Failed and is
// recorded on the event sink — otherwise it falls back to the Bootstrap
// errgroup (same recover guarantees, no component marking).
//
// The fn receives the effective loop context: the orchestrator's managed
// root context on the adopted path, the caller's ctx on the fallback path.
// comp must be non-nil (every serve-path caller holds the Bootstrap
// container); a nil comp skips the loop loudly instead of leaking an
// unmanaged goroutine.
func runBackground(ctx context.Context, comp *ares_bootstrap.Components, name string, fn func(ctx context.Context) error) {
	if comp == nil {
		log.Printf("serve: background loop %q skipped (no component container)", name)
		return
	}
	if comp.SystemRuntime != nil {
		comp.SystemRuntime.GoBackground(name, fn)
		return
	}
	comp.GoBackground(ctx, name, fn)
}
