// Package ares_bootstrap — System Runtime wiring (Stage 1).
//
// This file bridges the system-level control plane (internal/kernel)
// into the Bootstrap assembly: after all components are constructed, they are
// registered with the System Runtime registry so entry points (serve, start,
// SDK) observe a uniform component graph, lifecycle state, and readiness
// snapshot. Registration is observational: Bootstrap keeps owning construction
// and startup; the orchestrator records component states and provides the
// shared root context and status snapshot API.
package ares_bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/Timwood0x10/ares/internal/ares_config"
	"github.com/Timwood0x10/ares/internal/kernel"
)

// System Runtime component names — stable identifiers used by the registry.
const (
	sysCompEventStore     = "eventstore"
	sysCompRuntime        = "runtime"
	sysCompMemory         = "memory"
	sysCompMCP            = "mcp"
	sysCompLLM            = "llm"
	sysCompEvidenceStore  = "evidence"
	sysCompFlightRecorder = "flight"
	sysCompKnowledge      = "knowledge"
	sysCompNewEvolution   = "newevolution"
	sysCompDiscovery      = "discovery"
)

// SysCompEventStore is the System Runtime registry name of the shared event
// store. Exported for the kernel-side adoption (cmd/ares K2): the kernel
// pillar adapters declare it as their dependency edge so Shutdown stops them
// before the store.
const SysCompEventStore = sysCompEventStore

// runtimeComponentAdapter adapts an already-constructed Bootstrap component
// to the System Runtime Component interface. Identity and dependency metadata
// drive registry ordering; optional stop/wait hooks let the orchestrator's
// Shutdown drive real teardown in reverse topological order (Stage 9) instead
// of leaving teardown only to entry-point shutdown managers. Nil hooks are
// safe no-ops, so components without a dedicated teardown still transition.
// An optional readyFn lets a Degraded-mode component report a missing
// capability instead of silently claiming Ready (F03, Stage 9).
type runtimeComponentAdapter struct {
	name    string
	deps    []string
	stopFn  func(ctx context.Context) error
	waitFn  func() error
	readyFn func(ctx context.Context) error
}

// Name returns the stable component identifier.
func (a *runtimeComponentAdapter) Name() string { return a.name }

// Dependencies returns the names of components that must be Ready first.
func (a *runtimeComponentAdapter) Dependencies() []string { return a.deps }

// Stop delegates to the optional teardown hook; nil hook is a no-op.
func (a *runtimeComponentAdapter) Stop(ctx context.Context) error {
	if a.stopFn == nil {
		return nil
	}
	return a.stopFn(ctx)
}

// Wait delegates to the optional wait hook; nil hook is a no-op.
func (a *runtimeComponentAdapter) Wait() error {
	if a.waitFn == nil {
		return nil
	}
	return a.waitFn()
}

// Ready reports whether the component is fully operational. A nil readyFn
// means no readiness constraint (component is Ready by construction). A
// non-nil readyFn returning an error signals a missing capability, which the
// orchestrator records as Degraded for Degraded-mode components (F03).
func (a *runtimeComponentAdapter) Ready(ctx context.Context) error {
	if a.readyFn == nil {
		return nil
	}
	return a.readyFn(ctx)
}

// registerSystemComponent registers one component when it was actually
// constructed (present == true), attaching optional teardown and readiness
// hooks so the orchestrator drives real Stop/Wait and Degraded reporting.
// Registration failures are logged, never fatal: the registry is observational
// and a metadata problem must not block Bootstrap on an otherwise healthy
// assembly.
func registerSystemComponent(reg *kernel.Registry, name string, present bool, deps []string, mode kernel.Mode, stopFn func(ctx context.Context) error, waitFn func() error, readyFn func(ctx context.Context) error) {
	if !present {
		return
	}
	adapter := &runtimeComponentAdapter{name: name, deps: deps, stopFn: stopFn, waitFn: waitFn, readyFn: readyFn}
	if err := reg.Register(adapter, mode); err != nil {
		log.Warn("kernel: component registration skipped",
			"component", name, "error", err)
	}
}

// wireSystemRuntime registers every constructed component with the System
// Runtime registry and creates the orchestrator that observes their states.
// It runs after construction completes so the full component graph is known.
// Teardown hooks (Stage 9) let Orchestrator.Shutdown own real Stop/Wait in
// reverse topological order, so entry points no longer duplicate teardown.
//
// Args:
// ctx - bootstrap context used as the orchestrator's root context.
// cfg - resolved configuration (used for future per-component mode mapping).
// comp - the fully assembled Components instance.
//
// Returns:
// orch - the System Runtime orchestrator, or nil on error.
// reg - the backing registry (same instance the orchestrator observes).
// err - error when the orchestrator fails to observe startup.
func wireSystemRuntime(ctx context.Context, cfg *ares_config.Config, comp *Components) (*kernel.Orchestrator, *kernel.Registry, error) {
	reg := kernel.NewRegistry()

	registerSystemComponent(reg, sysCompEventStore, comp.EventStore != nil, nil, kernel.ModeRequired, nil, nil, nil)
	registerSystemComponent(reg, sysCompRuntime, comp.Runtime != nil, []string{sysCompEventStore}, kernel.ModeRequired,
		func(ctx context.Context) error { return comp.Runtime.Stop() }, nil, nil)
	registerSystemComponent(reg, sysCompMemory, comp.Memory != nil, []string{sysCompEventStore}, kernel.ModeRequired,
		func(ctx context.Context) error { return comp.Memory.Stop(ctx) }, nil, nil)
	registerSystemComponent(reg, sysCompMCP, comp.MCP != nil, nil, kernel.ModeRequired,
		func(ctx context.Context) error { return comp.MCP.Stop(ctx) }, nil, nil)
	registerSystemComponent(reg, sysCompLLM, comp.LLM != nil, nil, kernel.ModeRequired, nil, nil, nil)
	registerSystemComponent(reg, sysCompEvidenceStore, comp.EvidenceStore != nil, nil, kernel.ModeRequired, nil, nil, nil)
	registerSystemComponent(reg, sysCompFlightRecorder, comp.FlightRecorder != nil, []string{sysCompEventStore, sysCompEvidenceStore}, kernel.ModeRequired,
		func(ctx context.Context) error { comp.FlightRecorder.Stop(); return nil }, nil, nil)

	// Knowledge component: when AKG retrieval is enabled but the write-side
	// dependency (DistillBridge) is missing, the component must NOT silently
	// claim Ready — it registers as Degraded with a readiness error (F03).
	// Otherwise it is a normal Required component.
	knowledgeMode := kernel.ModeRequired
	var knowledgeReady func(ctx context.Context) error
	if cfg.Knowledge.RetrievalEnabled && comp.AKGBridge == nil {
		knowledgeMode = kernel.ModeDegraded
		knowledgeReady = func(ctx context.Context) error {
			return errors.New("knowledge: AKG retrieval enabled but write deps missing (AKGBridge nil)")
		}
	}
	registerSystemComponent(reg, sysCompKnowledge, comp.KnowledgeRuntime != nil, nil, knowledgeMode, nil, nil, knowledgeReady)

	registerSystemComponent(reg, sysCompNewEvolution, comp.NewEvolution != nil, []string{sysCompEvidenceStore}, kernel.ModeRequired, nil, nil, nil)
	registerSystemComponent(reg, sysCompDiscovery, comp.Discovery != nil, nil, kernel.ModeRequired, nil, nil, nil)

	orch := kernel.NewOrchestrator(reg, ctx)
	// K3: background component failures are recorded on the shared event
	// store so the flight recorder timeline (which subscribes to the whole
	// stream) shows them. Best-effort: a nil store only disables the record.
	orch.SetEventSink(comp.EventStore)
	if err := orch.Start(ctx); err != nil {
		return orch, reg, fmt.Errorf("kernel: observe startup: %w", err)
	}
	return orch, reg, nil
}
