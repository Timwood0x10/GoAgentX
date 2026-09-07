// Package ares_bootstrap — Entry Component Graph Equivalence Tests (Stage 6).
//
// Verifies that building the same config through the serve-style entry path
// (explicit EventStore via BootstrapDeps) and the start-style path (nil deps,
// Bootstrap creates its own store) produces the identical System Runtime
// component graph: same names, modes, and dependency edges. This locks the
// "entry equivalence" contract — a single assembly kernel (Bootstrap) driving all
// entry points instead of per-entry wiring drifting apart.
//
//go:build closure

package ares_bootstrap

import (
	"context"
	"sort"
	"testing"

	"github.com/Timwood0x10/ares/internal/ares_config"
	"github.com/Timwood0x10/ares/internal/ares_events"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// componentEdge captures one graph edge: a component name, its mode, and its
// sorted dependency list. Two graphs are equivalent iff their edge sets match.
type componentEdge struct {
	name string
	mode kernel.Mode
	deps []string
}

// graphEdges extracts the full component graph from a Bootstrap result.
func graphEdges(t *testing.T, comp *Components) []componentEdge {
	t.Helper()
	require.NotNil(t, comp.SystemRegistry, "SystemRegistry must be wired")

	var edges []componentEdge
	for _, name := range comp.SystemRegistry.Names() {
		mode, ok := comp.SystemRegistry.GetMode(name)
		require.True(t, ok, "mode must exist for %q", name)
		compIface := comp.SystemRegistry.GetComponent(name)
		require.NotNil(t, compIface, "component %q must be registered", name)
		deps := append([]string(nil), compIface.Dependencies()...)
		sort.Strings(deps)
		edges = append(edges, componentEdge{name: name, mode: mode, deps: deps})
	}
	// Deterministic comparison order.
	sort.Slice(edges, func(i, j int) bool { return edges[i].name < edges[j].name })
	return edges
}

// TestClosure_EntryComponentGraphEquivalence_ServeVsStart verifies the serve
// entry (explicit EventStore dep) and the start entry (nil deps) produce the
// identical component graph for the same config (Stage 6: entry equivalence).
func TestClosure_EntryComponentGraphEquivalence_ServeVsStart(t *testing.T) {
	cfg := &ares_config.Config{
		LLM: ares_config.LLMConfig{
			Provider: "mock",
			Model:    "mock-model",
			APIKey:   "test-key",
			BaseURL:  "http://localhost:9999",
		},
		Memory:    ares_config.MemoryConfig{Enabled: boolPtr(true)},
		Evolution: ares_config.EvolutionConfig{Enabled: true},
	}

	// serve-style: explicit EventStore supplied by the entry.
	serveCtx, serveCancel := context.WithCancel(context.Background())
	serveComp, err := Bootstrap(serveCtx, cfg, &BootstrapDeps{
		EventStore: ares_events.NewMemoryEventStore(),
	})
	require.NoError(t, err, "serve-style Bootstrap must succeed")
	serveEdges := graphEdges(t, serveComp)
	serveCancel()
	serveComp.WaitBackground()

	// start-style: nil deps — Bootstrap constructs its own in-memory store.
	startCtx, startCancel := context.WithCancel(context.Background())
	startComp, err := Bootstrap(startCtx, cfg, nil)
	require.NoError(t, err, "start-style Bootstrap must succeed")
	startEdges := graphEdges(t, startComp)
	startCancel()
	startComp.WaitBackground()

	// The core component graph must be identical across both entry styles.
	require.Equal(t, len(serveEdges), len(startEdges),
		"component count must match across entries")
	for i := range serveEdges {
		assert.Equal(t, serveEdges[i], startEdges[i],
			"component graph edge %q must be identical across entries", serveEdges[i].name)
	}

	// Both entries must report system Ready with the same Required set.
	assert.True(t, serveComp.IsSystemReady(), "serve-style system must be Ready")
	assert.True(t, startComp.IsSystemReady(), "start-style system must be Ready")
}

// TestClosure_EntryGraph_DisabledComponentsAbsent verifies that config gates
// (F01/F02) apply identically in both entry styles: a disabled component is
// absent from the graph, never half-wired.
func TestClosure_EntryGraph_DisabledComponentsAbsent(t *testing.T) {
	cfg := &ares_config.Config{
		LLM: ares_config.LLMConfig{
			Provider: "mock",
			Model:    "mock-model",
			APIKey:   "test-key",
			BaseURL:  "http://localhost:9999",
		},
		Memory:    ares_config.MemoryConfig{Enabled: boolPtr(false)},
		Evolution: ares_config.EvolutionConfig{Enabled: false},
	}

	ctx, cancel := context.WithCancel(context.Background())
	comp, err := Bootstrap(ctx, cfg, nil)
	require.NoError(t, err)
	defer cancel()
	defer comp.WaitBackground()

	edges := graphEdges(t, comp)
	names := make(map[string]bool, len(edges))
	for _, e := range edges {
		names[e.name] = true
	}
	assert.False(t, names[sysCompMemory], "disabled memory must not appear in the graph")
	assert.False(t, names[sysCompNewEvolution], "disabled evolution must not appear in the graph")
	assert.True(t, names[sysCompEventStore], "eventstore is always required")
	assert.True(t, names[sysCompRuntime], "runtime is always required")
}
