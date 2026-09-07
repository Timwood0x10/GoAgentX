// Package fabric is the unified orchestration layer for ARES AgentOS.
//
// It consolidates three existing packages into a single fabric:
//
//   - agentfabric (agent lifecycle: Spawn/Suspend/Resume/Retire/Kill)
//   - taskfabric (task projection: state machine, Lease+Epoch fencing, DAG)
//   - workflow/engine (MutableDAG: the single task graph + L1 evolution surface)
//   - planprojection (graph→task incremental compilation)
//
// Phase 2a creates placeholder sub-packages only; the actual code migration
// happens in Phase 2b (after M4 completes). Until then, the original packages
// remain in place and production code must not import from internal/fabric.
package fabric
