package main

import (
	"errors"
	"fmt"

	"github.com/Timwood0x10/ares/internal/ares_config"
	resources "github.com/Timwood0x10/ares/internal/tools/resources/core"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// errNoLiveAgentDAG is returned when no peers are configured: the caller
// keeps the bootstrap placeholder rather than injecting an empty graph.
var errNoLiveAgentDAG = errors.New("no peer agents configured for a live DAG")

// errNoToolSchemas is returned when the tool binder exposes no tool schemas:
// the L1 capability graph would be empty, so the caller keeps the bootstrap
// placeholder instead of injecting an empty graph.
var errNoToolSchemas = errors.New("no tool schemas available for a ToolClass DAG")

// buildLiveAgentDAG materializes the configured agent population as a real
// MutableDAG: one node per peer (AgentType = primary capability), dependency
// edges from the legacy agents.sub entries' Dependencies when present.
//
// This is the live topology the evolution system's structure patches act on.
// Historically serve never called UpdateLiveDAG, so workflow/recovery
// patches mutated the synthetic input→process→output bootstrap DAG forever —
// the "live runtime" promotion affected nothing observable. The returned DAG
// is registered on the runtime manager AND injected into the evolution
// executors so graph/recovery patches land on the agent graph actually shown
// in the runtime snapshot.
//
// Returns (nil, errNoLiveAgentDAG) when no peers are configured — the caller
// matches on that sentinel and keeps the bootstrap placeholder rather than
// injecting an empty graph.
func buildLiveAgentDAG(cfg *ares_config.Config) (*engine.MutableDAG, error) {
	peers := normalizedPeers(cfg)
	if len(peers) == 0 {
		return nil, errNoLiveAgentDAG
	}

	// Legacy sub entries may declare Dependencies between agents; carry them
	// over so pre-C1 configs keep their declared topology.
	legacyDeps := make(map[string][]string, len(cfg.Agents.Sub))
	for _, s := range cfg.Agents.Sub {
		if len(s.Dependencies) > 0 {
			legacyDeps[s.ID] = append([]string(nil), s.Dependencies...)
		}
	}

	steps := make([]*engine.Step, 0, len(peers))
	seen := make(map[string]bool, len(peers))
	for _, p := range peers {
		if p.ID == "" || seen[p.ID] {
			continue // defensive: NewMutableDAG rejects duplicate ids anyway
		}
		seen[p.ID] = true
		typ := ""
		if len(p.Capabilities) > 0 {
			typ = p.Capabilities[0]
		}
		step := &engine.Step{
			ID:        p.ID,
			Name:      p.ID,
			AgentType: typ,
			Input:     fmt.Sprintf("capability:%s", typ),
		}
		if deps, ok := legacyDeps[p.ID]; ok {
			step.DependsOn = deps
		}
		steps = append(steps, step)
	}
	if len(steps) == 0 {
		return nil, errNoLiveAgentDAG
	}

	dag, err := engine.NewMutableDAG(steps)
	if err != nil {
		return nil, fmt.Errorf("build live agent DAG: %w", err)
	}
	return dag, nil
}

// L1 metadata keys for ToolClass evolution constraints (§6). The L1 graph's
// Metadata is string-only (engine.Step.Metadata), so budget/prior are stored
// as their string representations.
const (
	l1MetaEnabled = "enabled"
	l1MetaBudget  = "budget"
	l1MetaPrior   = "prior"
)

// buildToolClassDAG constructs the L1 capability graph (M5): one node per
// ToolClass (toolName + "#" + argShape), with Metadata carrying the evolution
// constraints enabled/budget/prior. The argShape is the sorted set of
// parameter key names from the tool's schema — this normalizes by type
// signature, not by value, so "read_file(path=foo.txt)" and
// "read_file(path=bar.txt)" collapse into one ToolClass node (§1 L1).
//
// The L1 graph is the evolution system's stable action surface: genome
// patches mutate enabled/budget/prior on L1 nodes, the planner reads them
// before growing L2 tool nodes (§6 constraint point), and L2 execution
// statistics flow back as fitness (M6). The L1 graph is NOT compiled into
// taskfabric — it is a capability catalog, not an execution plan.
//
// Returns (nil, errNoToolSchemas) when the binder exposes no tools.
func buildToolClassDAG(schemas []resources.ToolSchema) (*engine.MutableDAG, error) {
	if len(schemas) == 0 {
		return nil, errNoToolSchemas
	}

	steps := make([]*engine.Step, 0, len(schemas))
	seen := make(map[string]bool, len(schemas))
	for _, s := range schemas {
		if s.Name == "" {
			continue
		}
		nodeID := resources.ToolClassID(s.Name, resources.ToolArgShape(s))
		if seen[nodeID] {
			continue // defensive: same tool+shape deduplicated
		}
		seen[nodeID] = true
		step := &engine.Step{
			ID:        nodeID,
			Name:      s.Name,
			AgentType: "tool/" + s.Name,
			Input:     s.Description,
			Metadata: map[string]string{
				l1MetaEnabled: "true",
				l1MetaBudget:  "0", // 0 = unlimited
				l1MetaPrior:   "",
			},
		}
		steps = append(steps, step)
	}
	if len(steps) == 0 {
		return nil, errNoToolSchemas
	}

	dag, err := engine.NewMutableDAG(steps)
	if err != nil {
		return nil, fmt.Errorf("build toolclass DAG: %w", err)
	}
	return dag, nil
}
