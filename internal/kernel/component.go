// Package kernel provides the system-level control plane that
// unifies component assembly, dependency resolution, lifecycle orchestration,
// and shutdown coordination across all entry points (serve, start, SDK).
//
// The System Runtime is distinct from ares_runtime.Manager, which remains
// the Agent lifecycle subsystem. System Runtime owns the broader component
// graph: EventStore, Memory, MCP, Flight, Evolution, Tools, HTTP, etc.
package kernel

import "context"

// Component is the base interface every managed component must implement.
// It provides identity and dependency metadata for topological ordering.
type Component interface {
	// Name returns the unique identifier of this component.
	Name() string

	// Dependencies returns the names of components that must be Ready
	// before this component can Bind/Start. The orchestrator uses this
	// to determine topological order. An empty slice means no dependencies.
	Dependencies() []string
}

// Binder wires dependencies into a component after construction but
// before Start. This is where live references (DAG, KnowledgeRuntime,
// StrategyStore, etc.) are injected so that Start operates on real targets.
type Binder interface {
	Bind(ctx context.Context, deps Resolver) error
}

// Starter begins the component's active lifecycle (goroutines, network
// connections, tickers). Construct must not start anything; only Start does.
type Starter interface {
	Start(ctx context.Context) error
}

// ReadinessChecker verifies that a started component is fully operational.
// If Ready returns an error, the component enters Degraded or Failed state.
type ReadinessChecker interface {
	Ready(ctx context.Context) error
}

// Stopper gracefully shuts down a component. Called in reverse topological
// order during shutdown. Must be idempotent (safe to call multiple times).
type Stopper interface {
	Stop(ctx context.Context) error
}

// Waiter blocks until all background work owned by the component has
// completed. Called after Stop in the shutdown sequence.
type Waiter interface {
	Wait() error
}

// Resolver provides access to already-constructed components by name.
// Bind implementations use it to obtain references to their dependencies.
type Resolver interface {
	// Get returns the component instance by name, or nil if not found.
	Get(name string) any
}

// Mode declares how a component's failure affects the overall system.
type Mode int

const (
	// ModeRequired means the component must reach Ready for the system
	// to report Ready. Failure to Bind/Start/Ready is a hard failure.
	ModeRequired Mode = iota

	// ModeOptional means the component is not constructed when disabled.
	// When enabled, it behaves as Required.
	ModeOptional

	// ModeDegraded means the component may operate in a reduced capacity.
	// It must report its degraded state and the missing capability.
	ModeDegraded
)

// String returns a human-readable mode name.
func (m Mode) String() string {
	switch m {
	case ModeRequired:
		return "required"
	case ModeOptional:
		return "optional"
	case ModeDegraded:
		return "degraded"
	default:
		return "unknown"
	}
}
