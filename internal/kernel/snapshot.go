// Package kernel — status snapshot API.
package kernel

import (
	"encoding/json"
	"time"
)

// Snapshot is a point-in-time view of all component statuses.
// It is safe to marshal to JSON for diagnostic/monitoring use.
type Snapshot struct {
	TakenAt    time.Time         `json:"taken_at"`
	Components []ComponentStatus `json:"components"`
	Summary    SnapshotSummary   `json:"summary"`
}

// SnapshotSummary aggregates component counts by state.
type SnapshotSummary struct {
	Total    int `json:"total"`
	Ready    int `json:"ready"`
	Degraded int `json:"degraded"`
	Failed   int `json:"failed"`
	Disabled int `json:"disabled"`
	Stopped  int `json:"stopped"`
}

// Snapshot returns a full status snapshot of all managed components.
func (r *Registry) Snapshot() Snapshot {
	statuses := r.AllStatuses()
	summary := SnapshotSummary{Total: len(statuses)}
	for _, s := range statuses {
		switch s.State {
		case StateReady:
			summary.Ready++
		case StateDegraded:
			summary.Degraded++
		case StateFailed:
			summary.Failed++
		case StateDisabled:
			summary.Disabled++
		case StateStopped:
			summary.Stopped++
		}
	}
	return Snapshot{
		TakenAt:    time.Now(),
		Components: statuses,
		Summary:    summary,
	}
}

// IsReady returns true if all Required components are Ready or Degraded
// and no component is in Failed state.
func (r *Registry) IsReady() bool {
	statuses := r.AllStatuses()
	for _, s := range statuses {
		if s.Mode == ModeRequired && !s.State.IsHealthy() {
			return false
		}
		if s.State == StateFailed {
			return false
		}
	}
	return true
}

// JSON marshals the snapshot to JSON for diagnostic output.
func (s Snapshot) JSON() ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}
