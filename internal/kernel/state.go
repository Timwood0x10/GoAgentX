// Package kernel — state machine types for component lifecycle.
package kernel

import (
	"fmt"
	"time"
)

// State represents the lifecycle state of a managed component.
type State int

const (
	// StateDeclared means the component is registered but not yet
	// constructed.
	StateDeclared State = iota

	// StateConstructed means the component instance is created but
	// dependencies are not yet bound.
	StateConstructed

	// StateBound means Bind() has completed successfully.
	StateBound

	// StateStarted means Start() has completed successfully.
	StateStarted

	// StateReady means Ready() has completed successfully.
	StateReady

	// StateDegraded means the component is operating with reduced
	// capability. Only valid when explicitly allowed by config.
	StateDegraded

	// StateFailed means the component encountered an unrecoverable
	// error during Bind/Start/Ready.
	StateFailed

	// StateStopping means Stop() has been called but not yet completed.
	StateStopping

	// StateStopped means Stop() and Wait() have completed.
	StateStopped

	// StateDisabled means the component was not constructed because
	// its config gate was false.
	StateDisabled
)

// String returns a human-readable state name.
func (s State) String() string {
	switch s {
	case StateDeclared:
		return "declared"
	case StateConstructed:
		return "constructed"
	case StateBound:
		return "bound"
	case StateStarted:
		return "started"
	case StateReady:
		return "ready"
	case StateDegraded:
		return "degraded"
	case StateFailed:
		return "failed"
	case StateStopping:
		return "stopping"
	case StateStopped:
		return "stopped"
	case StateDisabled:
		return "disabled"
	default:
		return "unknown"
	}
}

// IsTerminal returns true if the state is a terminal state
// (no further transitions are expected).
func (s State) IsTerminal() bool {
	switch s {
	case StateStopped, StateDisabled, StateFailed:
		return true
	default:
		return false
	}
}

// IsHealthy returns true if the component is in a healthy state
// (Ready or Degraded with explicit config).
func (s State) IsHealthy() bool {
	return s == StateReady || s == StateDegraded
}

// ComponentStatus holds the full status of a managed component.
// This is the data plane for the status snapshot API.
type ComponentStatus struct {
	Name       string    `json:"name"`
	Mode       Mode      `json:"mode"`
	State      State     `json:"state"`
	Reason     string    `json:"reason,omitempty"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	InstanceID string    `json:"instance_id,omitempty"`
}

// String returns a human-readable status line.
func (s ComponentStatus) String() string {
	return fmt.Sprintf(
		"component %s: mode=%s state=%s reason=%q",
		s.Name, s.Mode, s.State, s.Reason,
	)
}
