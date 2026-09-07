// Package sdk — entry component graph equivalence tests (Stage 6/9, SDK side).
//
// Verifies the SDK, when backed by the Bootstrap core (Stage 8), assembles the
// SAME component graph as a direct Bootstrap call with the equivalent config:
// identical component names, modes, and dependency edges. This locks the
// "three-entry equivalence" contract on the SDK entry — no drifting dual-track graph.
package sdk

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/ares_bootstrap"
	"github.com/Timwood0x10/ares/internal/kernel"
)

// sdkGraphEdge captures one graph edge (name, mode, sorted deps) for
// equivalence comparison.
type sdkGraphEdge struct {
	name string
	mode kernel.Mode
	deps []string
}

// sdkGraphEdges extracts the full component graph from a Bootstrap result.
func sdkGraphEdges(t *testing.T, comp *ares_bootstrap.Components) []sdkGraphEdge {
	t.Helper()
	require.NotNil(t, comp.SystemRegistry, "SystemRegistry must be wired")

	var edges []sdkGraphEdge
	for _, name := range comp.SystemRegistry.Names() {
		mode, ok := comp.SystemRegistry.GetMode(name)
		require.True(t, ok, "mode must exist for %q", name)
		compIface := comp.SystemRegistry.GetComponent(name)
		require.NotNil(t, compIface, "component %q must be registered", name)
		deps := append([]string(nil), compIface.Dependencies()...)
		sort.Strings(deps)
		edges = append(edges, sdkGraphEdge{name: name, mode: mode, deps: deps})
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].name < edges[j].name })
	return edges
}

// TestSDK_ComponentGraph_EquivalentToBootstrap verifies the SDK's Bootstrap
// core assembles the identical component graph as a direct Bootstrap call with
// the equivalent config (three-entry equivalence on the SDK entry).
func TestSDK_ComponentGraph_EquivalentToBootstrap(t *testing.T) {
	// Build the SDK Runtime backed by the Bootstrap core.
	rt, err := New(
		WithOllama("llama3.2"),
		WithDefaultMemory(),
		WithEvolution(),
		WithTrace(false),
	)
	require.NoError(t, err, "SDK New() must succeed")
	defer rt.Close()

	require.NotNil(t, rt.bootstrap, "Runtime must be backed by the Bootstrap core")
	require.NotNil(t, rt.bootstrap.SystemRegistry, "Bootstrap core must wire SystemRegistry")

	// Rebuild the same config through a direct Bootstrap call and compare.
	cfg := defaultConfig()
	require.NoError(t, WithOllama("llama3.2")(cfg))
	require.NoError(t, WithDefaultMemory()(cfg))
	require.NoError(t, WithEvolution()(cfg))
	require.NoError(t, WithTrace(false)(cfg))

	ctx, cancel := context.WithCancel(context.Background())
	direct, err := ares_bootstrap.Bootstrap(ctx, buildBootstrapConfig(cfg), nil)
	require.NoError(t, err, "direct Bootstrap must succeed")
	directEdges := sdkGraphEdges(t, direct)
	cancel()
	direct.WaitBackground()

	sdkEdges := sdkGraphEdges(t, rt.bootstrap)

	require.Equal(t, len(directEdges), len(sdkEdges),
		"component count must match between SDK Bootstrap core and direct Bootstrap")
	for i := range directEdges {
		assert.Equal(t, directEdges[i], sdkEdges[i],
			"component graph edge %q must be identical (SDK vs direct Bootstrap)", directEdges[i].name)
	}
}

// TestSDK_Graph_DisabledComponentsAbsent verifies config gates apply on the SDK
// path: disabling evolution removes NewEvolution from the SDK's graph.
func TestSDK_Graph_DisabledComponentsAbsent(t *testing.T) {
	rt, err := New(
		WithOllama("llama3.2"),
		WithTrace(false),
	)
	require.NoError(t, err, "SDK New() must succeed")
	defer rt.Close()

	if rt.bootstrap == nil {
		t.Log("Runtime not Bootstrap-backed (SDK-only options); graph check skipped")
		return
	}

	names := make(map[string]bool)
	for _, e := range sdkGraphEdges(t, rt.bootstrap) {
		names[e.name] = true
	}
	assert.False(t, names["newevolution"],
		"disabled evolution must not appear in the SDK component graph")
	assert.True(t, names["eventstore"], "eventstore is always required")
}
