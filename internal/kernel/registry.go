// Package kernel — component registry and resolver implementation.
package kernel

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// entry wraps a registered component with its metadata and runtime status.
type entry struct {
	component Component
	mode      Mode
	status    ComponentStatus
}

// Registry holds all declared components and provides dependency-aware
// lookup. It implements the Resolver interface so Bind implementations can
// obtain references to their dependencies.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*entry
	order   []string // registration order for deterministic iteration
}

// NewRegistry creates an empty component registry.
func NewRegistry() *Registry {
	return &Registry{
		entries: make(map[string]*entry),
	}
}

// Register declares a component with its mode. Returns an error if a
// component with the same name is already registered, the component is nil
// (including a typed-nil pointer wrapped in the interface), the name is empty,
// or the mode is not one of the declared Mode values.
func (r *Registry) Register(c Component, mode Mode) error {
	if c == nil || isNilComponent(c) {
		return errors.New("system_runtime: cannot register nil component")
	}
	if mode < ModeRequired || mode > ModeDegraded {
		return fmt.Errorf("system_runtime: invalid mode %d for component %q", mode, c.Name())
	}
	name := c.Name()
	if name == "" {
		return errors.New("system_runtime: component name must not be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.entries[name]; exists {
		return fmt.Errorf("system_runtime: component %q already registered", name)
	}

	r.entries[name] = &entry{
		component: c,
		mode:      mode,
		status: ComponentStatus{
			Name:  name,
			Mode:  mode,
			State: StateConstructed, // registration hands over a constructed instance
		},
	}
	r.order = append(r.order, name)
	return nil
}

// isNilComponent detects a nil interface as well as a typed-nil pointer
// wrapped in the Component interface (the classic Go nil-interface trap).
func isNilComponent(c Component) bool {
	if c == nil {
		return true
	}
	v := reflect.ValueOf(c)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

// Get returns the component instance by name, or nil if not found.
// Implements the Resolver interface.
func (r *Registry) Get(name string) any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.entries[name]; ok {
		return e.component
	}
	return nil
}

// GetComponent returns the raw Component interface for orchestration.
func (r *Registry) GetComponent(name string) Component {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.entries[name]; ok {
		return e.component
	}
	return nil
}

// GetMode returns the mode for a component.
func (r *Registry) GetMode(name string) (Mode, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.entries[name]; ok {
		return e.mode, true
	}
	return ModeRequired, false
}

// GetStatus returns a copy of the current status for a component.
func (r *Registry) GetStatus(name string) (ComponentStatus, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.entries[name]; ok {
		return e.status, true
	}
	return ComponentStatus{}, false
}

// SetStatus updates the status for a component. Used by the orchestrator
// during lifecycle transitions.
func (r *Registry) SetStatus(name string, status ComponentStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[name]; ok {
		e.status = status
	}
}

// AllStatuses returns a snapshot of all component statuses.
func (r *Registry) AllStatuses() []ComponentStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	statuses := make([]ComponentStatus, 0, len(r.order))
	for _, name := range r.order {
		if e, ok := r.entries[name]; ok {
			statuses = append(statuses, e.status)
		}
	}
	return statuses
}

// Names returns all registered component names in registration order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]string, len(r.order))
	copy(result, r.order)
	return result
}

// TopologicalOrder returns component names in dependency order:
// dependencies appear before their dependents. Returns an error if
// a cycle is detected.
func (r *Registry) TopologicalOrder() ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Build adjacency: for each component, list its dependencies.
	deps := make(map[string][]string)
	for _, name := range r.order {
		e := r.entries[name]
		if e != nil {
			deps[name] = e.component.Dependencies()
		} else {
			deps[name] = nil
		}
	}

	// Kahn's algorithm for topological sort.
	inDegree := make(map[string]int)
	graph := make(map[string][]string) // dep -> dependents

	for name := range deps {
		inDegree[name] = 0
	}
	for name, dlist := range deps {
		for _, d := range dlist {
			if _, exists := inDegree[d]; !exists {
				// Fail-loud: a declared dependency must be registered.
				// Treating it as an external dep would silently hide
				// typos, missing registrations, and config mistakes.
				return nil, fmt.Errorf(
					"system_runtime: component %q declares unregistered dependency %q", name, d)
			}
			graph[d] = append(graph[d], name)
			inDegree[name]++
		}
	}

	// Find all nodes with in-degree 0.
	var queue []string
	for _, name := range r.order {
		if inDegree[name] == 0 {
			queue = append(queue, name)
		}
	}

	var result []string
	for len(queue) > 0 {
		// Pop from front (BFS preserves registration order for ties).
		name := queue[0]
		queue = queue[1:]
		result = append(result, name)

		for _, dependent := range graph[name] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	if len(result) != len(r.entries) {
		// Cycle detected — find the cycle members for the error message.
		remaining := make(map[string]bool)
		for name, deg := range inDegree {
			if deg > 0 {
				remaining[name] = true
			}
		}
		return nil, fmt.Errorf(
			"system_runtime: dependency cycle detected among %d components: %v",
			len(remaining), keysOf(remaining),
		)
	}

	return result, nil
}

// keysOf returns the keys of a string set map for error messages.
func keysOf(m map[string]bool) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}
