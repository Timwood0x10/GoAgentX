// Package ares_bootstrap — System Runtime wiring tests (Stage 1).
package ares_bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/ares_config"
	"github.com/Timwood0x10/ares/internal/kernel"
)

// newBootstrapForSnapshot builds a minimal Components instance through the real
// Bootstrap path with Memory and Evolution disabled (default gates), which
// still registers the always-constructed components (eventstore, runtime, mcp,
// llm, dashboard, evidence).
func newBootstrapForSnapshot(t *testing.T) *Components {
	t.Helper()
	cfg := &ares_config.Config{
		LLM:    ares_config.LLMConfig{Provider: "ollama", Model: "llama3.2"},
		Memory: ares_config.MemoryConfig{Enabled: boolPtr(false)},
		MCP:    ares_config.MCPConfig{Servers: []ares_config.MCPServerEntry{}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	comp, err := Bootstrap(ctx, cfg, nil)
	require.NoError(t, err, "Bootstrap must succeed with minimal config")
	require.NotNil(t, comp.SystemRuntime, "SystemRuntime must be wired")
	require.NotNil(t, comp.SystemRegistry, "SystemRegistry must be wired")
	return comp
}

// TestSnapshot_ContainsCoreComponents verifies the snapshot reports the
// always-constructed components after a minimal Bootstrap.
func TestSnapshot_ContainsCoreComponents(t *testing.T) {
	comp := newBootstrapForSnapshot(t)

	snap := comp.Snapshot()
	require.NotZero(t, snap.TakenAt, "snapshot must carry a timestamp")
	assert.GreaterOrEqual(t, snap.Summary.Total, 4,
		"at least eventstore/runtime/mcp/llm/dashboard/evidence registered")

	names := make(map[string]bool, len(snap.Components))
	for _, s := range snap.Components {
		names[s.Name] = true
	}
	for _, want := range []string{sysCompEventStore, sysCompRuntime, sysCompMCP, sysCompLLM, sysCompEvidenceStore} {
		assert.True(t, names[want], "snapshot must include component %q", want)
	}
	// Memory and NewEvolution are disabled by default, so they must NOT appear.
	assert.False(t, names[sysCompMemory], "disabled memory must not be registered")
	assert.False(t, names[sysCompNewEvolution], "disabled evolution must not be registered")
}

// TestComponentStatus_RegisteredAndUnknown verifies per-component status lookup.
func TestComponentStatus_RegisteredAndUnknown(t *testing.T) {
	comp := newBootstrapForSnapshot(t)

	status, ok := comp.ComponentStatus(sysCompEventStore)
	assert.True(t, ok, "eventstore must be registered")
	assert.Equal(t, sysCompEventStore, status.Name)
	assert.Equal(t, kernel.ModeRequired, status.Mode)

	_, ok = comp.ComponentStatus("does-not-exist")
	assert.False(t, ok, "unknown component must not be found")
}

// TestIsSystemReady_TrueWithMinimalConfig verifies all Required components are
// Ready after a successful minimal Bootstrap (adapters have no Ready check, so
// the orchestrator marks them Ready on Start).
func TestIsSystemReady_TrueWithMinimalConfig(t *testing.T) {
	comp := newBootstrapForSnapshot(t)
	assert.True(t, comp.IsSystemReady(),
		"all registered Required components must be Ready after Bootstrap")
}

// TestSnapshot_NilComponents covers the nil-receiver / unwired-registry guards:
// a nil Components and a Components without a registry must both return valid
// empty snapshots instead of panicking.
func TestSnapshot_NilComponents(t *testing.T) {
	var nilComp *Components
	snap := nilComp.Snapshot()
	assert.Empty(t, snap.Components, "nil receiver must yield empty snapshot")
	assert.False(t, nilComp.IsSystemReady(), "nil receiver must not report ready")

	emptyComp := &Components{}
	snap = emptyComp.Snapshot()
	assert.Empty(t, snap.Components, "unwired registry must yield empty snapshot")

	_, ok := emptyComp.ComponentStatus(sysCompEventStore)
	assert.False(t, ok, "unwired registry must not find components")
	assert.False(t, emptyComp.IsSystemReady(), "unwired registry must not report ready")
}

// TestSnapshot_JSONSerializable verifies the snapshot marshals to JSON, which
// entry points use for diagnostic/monitoring output.
func TestSnapshot_JSONSerializable(t *testing.T) {
	comp := newBootstrapForSnapshot(t)

	snap := comp.Snapshot()
	data, err := snap.JSON()
	require.NoError(t, err, "snapshot must marshal to JSON")
	assert.Contains(t, string(data), sysCompEventStore)
}

// TestWaitBackground_EmptyGroup verifies WaitBackground returns promptly when
// no background goroutines are running (nil receiver and empty group).
func TestWaitBackground_EmptyGroup(t *testing.T) {
	var nilComp *Components
	nilComp.WaitBackground() // must not panic

	comp := &Components{}
	done := make(chan struct{})
	go func() {
		comp.WaitBackground()
		close(done)
	}()
	select {
	case <-done:
		// Returned promptly.
	case <-time.After(2 * time.Second):
		t.Fatal("WaitBackground must return promptly with an empty group")
	}
}
