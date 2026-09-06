package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// cloneMetadata deep-copies a Step.Metadata map; nil stays nil. Used wherever a
// DAGNode.Metadata snapshot is taken from a Step so the node never aliases the
// step's live map (code_rules: no shared mutable state without ownership).
func cloneMetadata(md map[string]string) map[string]string {
	if md == nil {
		return nil
	}
	out := make(map[string]string, len(md))
	for k, v := range md {
		out[k] = v
	}
	return out
}

// Sentinel errors for MutableDAG operations.
var (
	ErrNodeNotFound      = errors.New("node not found")
	ErrNodeHasDependents = errors.New("node has dependents")
	ErrDuplicateEdge     = errors.New("duplicate edge")
	ErrEdgeNotFound      = errors.New("edge not found")
)

// MutableDAG extends DAG with thread-safe mutation operations.
type MutableDAG struct {
	mu            sync.RWMutex
	dag           *DAG
	steps         map[string]*Step
	version       uint64
	hub           *GraphEventHub
	SchedulerType string // active scheduler type, set by genome evolution patches
}

// NewMutableDAG creates a MutableDAG from initial steps.
func NewMutableDAG(steps []*Step) (*MutableDAG, error) {
	dag, err := NewDAG(steps)
	if err != nil {
		return nil, err
	}

	stepsMap := make(map[string]*Step, len(steps))
	for _, s := range steps {
		stepsMap[s.ID] = s
	}

	return &MutableDAG{
		dag:   dag,
		steps: stepsMap,
		hub:   NewGraphEventHub(),
	}, nil
}

// ReadDeps returns a copy of the dependency list for a step, safe for use
// outside the DAG's mutex. This replaces direct access to m.mu/step.DependsOn
// from external goroutines (e.g. the executor), which violates encapsulation.
func (m *MutableDAG) ReadDeps(stepID string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	step, ok := m.steps[stepID]
	if !ok {
		return nil
	}
	deps := make([]string, len(step.DependsOn))
	copy(deps, step.DependsOn)
	return deps
}

// AddNode adds a step as a new node. Validates dependencies exist, checks for cycles.
func (m *MutableDAG) AddNode(ctx context.Context, step *Step) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if step == nil {
		return errors.New("step must not be nil")
	}

	// Normalize: trim spaces from step ID.
	id := strings.TrimSpace(step.ID)
	if id == "" {
		return errors.New("step ID must not be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.dag.Nodes[id]; exists {
		return ErrDuplicateID
	}

	// Track added edges for rollback on cycle detection.
	type addedEdge struct {
		from string
		to   string
	}
	var addedEdges []addedEdge

	// Add the node.
	m.dag.Nodes[id] = &DAGNode{
		StepID:   id,
		Metadata: cloneMetadata(step.Metadata),
	}

	// Process dependencies, deduplicating DependsOn.
	seen := make(map[string]bool, len(step.DependsOn))
	for _, dep := range step.DependsOn {
		dep = strings.TrimSpace(dep)
		if dep == "" || seen[dep] {
			continue
		}
		seen[dep] = true

		if _, exists := m.dag.Nodes[dep]; !exists {
			// Rollback: remove any edges added so far, then remove the node.
			// Node deletion must happen AFTER edge rollback because
			// addedEdges[].to is always id, and decrementing InDegree on
			// a deleted node would nil-deref.
			for _, e := range addedEdges {
				m.removeEdgeFromSlice(e.from, e.to)
				m.dag.Nodes[e.from].OutDegree--
				m.dag.Nodes[e.to].InDegree--
			}
			delete(m.dag.Nodes, id)
			return ErrInvalidDependency
		}

		// Check for cycle before adding edge.
		if m.wouldCreateCycle(dep, id) {
			// Rollback.
			for _, e := range addedEdges {
				m.removeEdgeFromSlice(e.from, e.to)
				m.dag.Nodes[e.from].OutDegree--
				m.dag.Nodes[e.to].InDegree--
			}
			delete(m.dag.Nodes, id)
			return ErrCycleDetected
		}

		m.dag.Edges[dep] = append(m.dag.Edges[dep], id)
		m.dag.Nodes[id].InDegree++
		m.dag.Nodes[dep].OutDegree++
		addedEdges = append(addedEdges, addedEdge{from: dep, to: id})
	}

	m.steps[id] = step
	m.version++

	m.hub.Publish(GraphEvent{
		Change: GraphChange{
			Type:      ChangeAddNode,
			NodeID:    id,
			Step:      step,
			Timestamp: time.Now(),
		},
		Success: true,
	})

	return nil
}

// RemoveNode removes a node and its edges. Fails if other nodes depend on it.
func (m *MutableDAG) RemoveNode(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.dag.Nodes[id]; !exists {
		return ErrNodeNotFound
	}

	// Check if any other node depends on this node.
	for _, step := range m.steps {
		if step.ID == id {
			continue
		}
		for _, dep := range step.DependsOn {
			if dep == id {
				m.hub.Publish(GraphEvent{
					Change: GraphChange{
						Type:      ChangeRemoveNode,
						NodeID:    id,
						Timestamp: time.Now(),
					},
					Success: false,
					Error:   ErrNodeHasDependents,
				})
				return ErrNodeHasDependents
			}
		}
	}

	// Remove all edges where this node is source.
	for _, target := range m.dag.Edges[id] {
		m.dag.Nodes[target].InDegree--
	}
	delete(m.dag.Edges, id)

	// Remove all edges where this node is target.
	for src, targets := range m.dag.Edges {
		newTargets := make([]string, 0, len(targets))
		for _, t := range targets {
			if t != id {
				newTargets = append(newTargets, t)
			} else {
				m.dag.Nodes[src].OutDegree--
			}
		}
		m.dag.Edges[src] = newTargets
	}

	delete(m.dag.Nodes, id)
	delete(m.steps, id)
	m.version++

	m.hub.Publish(GraphEvent{
		Change: GraphChange{
			Type:      ChangeRemoveNode,
			NodeID:    id,
			Timestamp: time.Now(),
		},
		Success: true,
	})

	return nil
}

// AddEdge adds a directed edge. Checks for cycles incrementally.
func (m *MutableDAG) AddEdge(ctx context.Context, from, to string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.dag.Nodes[from]; !exists {
		return ErrNodeNotFound
	}
	if _, exists := m.dag.Nodes[to]; !exists {
		return ErrNodeNotFound
	}

	// Check for duplicate edge.
	for _, target := range m.dag.Edges[from] {
		if target == to {
			return ErrDuplicateEdge
		}
	}

	if m.wouldCreateCycle(from, to) {
		m.hub.Publish(GraphEvent{
			Change: GraphChange{
				Type:      ChangeAddEdge,
				FromID:    from,
				ToID:      to,
				Timestamp: time.Now(),
			},
			Success: false,
			Error:   ErrCycleDetected,
		})
		return ErrCycleDetected
	}

	m.dag.Edges[from] = append(m.dag.Edges[from], to)
	m.dag.Nodes[to].InDegree++
	m.dag.Nodes[from].OutDegree++
	m.version++

	// Update the step's DependsOn list.
	if step, exists := m.steps[to]; exists {
		step.DependsOn = append(step.DependsOn, from)
	}

	m.hub.Publish(GraphEvent{
		Change: GraphChange{
			Type:      ChangeAddEdge,
			FromID:    from,
			ToID:      to,
			Timestamp: time.Now(),
		},
		Success: true,
	})

	return nil
}

// RemoveEdge removes a directed edge.
func (m *MutableDAG) RemoveEdge(ctx context.Context, from, to string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.dag.Nodes[from]; !exists {
		return ErrNodeNotFound
	}
	if _, exists := m.dag.Nodes[to]; !exists {
		return ErrNodeNotFound
	}

	found := m.removeEdgeFromSlice(from, to)
	if !found {
		return ErrEdgeNotFound
	}

	m.dag.Nodes[to].InDegree--
	m.dag.Nodes[from].OutDegree--
	m.version++

	// Update the step's DependsOn list.
	if step, exists := m.steps[to]; exists {
		newDeps := make([]string, 0, len(step.DependsOn))
		for _, dep := range step.DependsOn {
			if dep != from {
				newDeps = append(newDeps, dep)
			}
		}
		step.DependsOn = newDeps
	}

	m.hub.Publish(GraphEvent{
		Change: GraphChange{
			Type:      ChangeRemoveEdge,
			FromID:    from,
			ToID:      to,
			Timestamp: time.Now(),
		},
		Success: true,
	})

	return nil
}

// GetExecutionOrder returns topological sort under read lock.
// The ordering strategy is determined by SchedulerType:
//   - "" or "*graph.DefaultScheduler": FIFO topological order (default)
//   - other: random shuffle of ready nodes at each step
func (m *MutableDAG) GetExecutionOrder() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inDegree := make(map[string]int)
	for node := range m.dag.Nodes {
		inDegree[node] = m.dag.Nodes[node].InDegree
	}

	queue := make([]string, 0)
	for node, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}

	// When the scheduler type is not the default, shuffle the ready queue
	// at each step to produce a different execution order. This is how
	// genome evolution of scheduler config actually affects the agent's
	// runtime behavior — the PatchChangeScheduler sets SchedulerType on
	// the live DAG, and GetExecutionOrder reads it here.
	useRandom := m.SchedulerType != "" && m.SchedulerType != "*graph.DefaultScheduler"

	result := make([]string, 0, len(m.dag.Nodes))
	for len(queue) > 0 {
		var node string
		if useRandom && len(queue) > 1 {
			// Non-default scheduler: randomize selection order.
			idx := int(time.Now().UnixNano()) % len(queue)
			if idx < 0 {
				idx = -idx
			}
			node = queue[idx]
			queue = append(queue[:idx], queue[idx+1:]...)
		} else {
			node = queue[0]
			queue = queue[1:]
		}
		result = append(result, node)

		for _, neighbor := range m.dag.Edges[node] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(result) != len(m.dag.Nodes) {
		return nil, ErrCycleDetected
	}

	return result, nil
}

// Snapshot returns a deep copy of the current DAG.
func (m *MutableDAG) Snapshot() *DAG {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.snapshotDAGLocked()
}

// SnapshotWithSteps returns a copy of the current DAG topology (deep copy) and step
// references (shallow copy — same Step pointers) atomically under a single read lock.
// Callers must treat the returned Steps as read-only to avoid corrupting internal state.
func (m *MutableDAG) SnapshotWithSteps() (*DAG, map[string]*Step) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dagCopy := m.snapshotDAGLocked()

	stepsCopy := make(map[string]*Step, len(m.steps))
	for id, step := range m.steps {
		stepsCopy[id] = step
	}

	return dagCopy, stepsCopy
}

// snapshotDAGLocked returns a deep copy of the DAG. Must be called with m.mu held.
func (m *MutableDAG) snapshotDAGLocked() *DAG {
	nodesCopy := make(map[string]*DAGNode, len(m.dag.Nodes))
	for id, node := range m.dag.Nodes {
		nodeCopy := *node
		nodeCopy.Metadata = cloneMetadata(node.Metadata)
		nodesCopy[id] = &nodeCopy
	}

	edgesCopy := make(map[string][]string, len(m.dag.Edges))
	for src, targets := range m.dag.Edges {
		targetsCopy := make([]string, len(targets))
		copy(targetsCopy, targets)
		edgesCopy[src] = targetsCopy
	}

	return &DAG{
		Nodes: nodesCopy,
		Edges: edgesCopy,
	}
}

// SetNodeMetadata replaces a node's Metadata map in place, preserving topology
// and the *MutableDAG identity. It updates BOTH the owning Step (so the patch
// survives a snapshot/restore, which is driven by steps) and the DAGNode's
// metadata snapshot (so WorkflowDiffer sees the metadata-only change as a
// patch). This is the C4 mutation target for a ToolStep node's enabled/budget/
// prior attributes.
func (m *MutableDAG) SetNodeMetadata(nodeID string, md map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, ok := m.dag.Nodes[nodeID]
	if !ok {
		return ErrNodeNotFound
	}
	node.Metadata = cloneMetadata(md)
	if step, ok := m.steps[nodeID]; ok {
		step.Metadata = cloneMetadata(md)
	}
	m.version++
	m.hub.Publish(GraphEvent{
		Change: GraphChange{
			Type:      ChangeSetNodeMetadata,
			NodeID:    nodeID,
			Step:      m.steps[nodeID],
			Timestamp: time.Now(),
		},
		Success: true,
	})
	return nil
}

// Version returns the current mutation counter.
func (m *MutableDAG) Version() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.version
}

// Steps returns the current step list under read lock.
func (m *MutableDAG) Steps() []*Step {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*Step, 0, len(m.steps))
	for _, s := range m.steps {
		result = append(result, s)
	}
	return result
}

// StepIndex returns a copy of the step index map under read lock.
func (m *MutableDAG) StepIndex() map[string]*Step {
	m.mu.RLock()
	defer m.mu.RUnlock()

	idx := make(map[string]*Step, len(m.steps))
	for id, s := range m.steps {
		idx[id] = s
	}
	return idx
}

// Subscribe returns a channel for graph change events.
// The returned channel is never closed unless Unsubscribe is called with
// the corresponding subscription ID. Prefer SubscribeWithID when the caller
// needs to clean up the subscription.
func (m *MutableDAG) Subscribe() <-chan GraphEvent {
	_, ch := m.hub.Subscribe()
	return ch
}

// SubscribeWithID returns a subscription ID and a channel for graph change
// events. The caller must call Unsubscribe(id) to close the channel and
// free resources when the subscription is no longer needed.
func (m *MutableDAG) SubscribeWithID() (string, <-chan GraphEvent) {
	return m.hub.Subscribe()
}

// Unsubscribe removes a subscriber and closes its channel, allowing any
// goroutine blocked on range over the channel to exit.
func (m *MutableDAG) Unsubscribe(id string) {
	m.hub.Unsubscribe(id)
}

// DroppedEvents returns how many graph events the subscriber missed because
// its buffer was full. The incremental compiler polls it to catch drops at
// the tail of a burst (no later event arrives to reveal the sequence gap) and
// runs a full reconcile when the counter moves.
func (m *MutableDAG) DroppedEvents(subID string) uint64 {
	return m.hub.Dropped(subID)
}

// NodeCount returns the number of nodes.
func (m *MutableDAG) NodeCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.dag.Nodes)
}

// ResetFromSteps rebuilds the DAG in place from the given steps, preserving the
// *MutableDAG identity. This is what makes rollback safe when multiple holders
// (runtime manager, WorkflowGenome, patch executors) share the same pointer —
// the pointer stays valid while its topology is restored to a prior snapshot.
func (m *MutableDAG) ResetFromSteps(steps []*Step) error {
	if len(steps) == 0 {
		return errors.New("reset: at least one step is required")
	}
	newDAG, err := NewDAG(steps)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	newSteps := make(map[string]*Step, len(steps))
	for _, s := range steps {
		newSteps[s.ID] = s
	}
	m.dag = newDAG
	m.steps = newSteps
	m.version++
	return nil
}

// EdgeCount returns the total number of edges.
func (m *MutableDAG) EdgeCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, targets := range m.dag.Edges {
		count += len(targets)
	}
	return count
}

// wouldCreateCycle checks if adding an edge from->to would create a cycle.
// BFS from `to` following outgoing edges. If `from` is reachable, it creates a cycle.
// Must be called with m.mu held.
func (m *MutableDAG) wouldCreateCycle(from, to string) bool {
	visited := make(map[string]bool)
	queue := []string{to}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current == from {
			return true
		}

		if visited[current] {
			continue
		}
		visited[current] = true

		for _, neighbor := range m.dag.Edges[current] {
			if !visited[neighbor] {
				queue = append(queue, neighbor)
			}
		}
	}

	return false
}

// removeEdgeFromSlice removes the edge from->to from the Edges slice.
// Returns true if the edge was found and removed.
// Must be called with m.mu held.
func (m *MutableDAG) removeEdgeFromSlice(from, to string) bool {
	targets := m.dag.Edges[from]
	for i, t := range targets {
		if t == to {
			m.dag.Edges[from] = append(targets[:i], targets[i+1:]...)
			return true
		}
	}
	return false
}

// hasCycleInAdjList returns true if the directed graph represented by the
// adjacency list contains a cycle. Uses three-color DFS marking.
func hasCycleInAdjList(adjList map[string][]string) bool {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(adjList))
	for node := range adjList {
		color[node] = white
	}

	var dfs func(node string) bool
	dfs = func(node string) bool {
		color[node] = gray
		for _, neighbor := range adjList[node] {
			switch color[neighbor] {
			case gray:
				return true
			case white:
				if dfs(neighbor) {
					return true
				}
			}
		}
		color[node] = black
		return false
	}

	for node := range adjList {
		if color[node] == white {
			if dfs(node) {
				return true
			}
		}
	}
	return false
}

// recalculateDegrees recomputes InDegree and OutDegree for all DAGNodes
// from the current Edges map. Called after structural mutations that affect
// multiple edges at once. Must be called with m.mu held.
func (m *MutableDAG) recalculateDegrees() {
	for _, node := range m.dag.Nodes {
		node.InDegree = 0
		node.OutDegree = 0
	}
	for src, targets := range m.dag.Edges {
		if _, ok := m.dag.Nodes[src]; !ok {
			continue
		}
		m.dag.Nodes[src].OutDegree = len(targets)
		for _, tgt := range targets {
			if node, ok := m.dag.Nodes[tgt]; ok {
				node.InDegree++
			}
		}
	}
}

// ReplaceNode atomically replaces the node identified by oldID with newStep,
// migrating all incoming and outgoing edges to the new node.
//
// Behavior depends on whether the ID changes:
//   - Same ID (newStep.ID == oldID): in-place update, new DependsOn edges are added.
//   - Different ID: all incoming edges are redirected to newStep.ID, all outgoing
//     edges are moved from oldID to newStep.ID, new DependsOn edges are added,
//     then the old node is removed.
//
// Cycle detection is performed on a simulated adjacency list of the post-replacement
// graph before any mutation is applied, so the operation is atomic with respect to
// consistency — no rollback logic is needed.
//
//nolint:gocyclo // Complex DAG node replacement with dependency updates
//nolint:gocyclo
func (m *MutableDAG) ReplaceNode(ctx context.Context, oldID string, newStep *Step) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if newStep == nil {
		return errors.New("step must not be nil")
	}
	if newStep.ID == "" {
		return errors.New("step ID must not be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.dag.Nodes[oldID]; !exists {
		return ErrNodeNotFound
	}

	if newStep.ID != oldID {
		if _, exists := m.dag.Nodes[newStep.ID]; exists {
			return ErrDuplicateID
		}
	}

	// Self-loop check must come before DependsOn existence check:
	// a DependsOn referencing the new ID is a self-loop (since the target
	// is the node being introduced), but the node does not yet exist.
	for _, dep := range newStep.DependsOn {
		if dep == newStep.ID {
			return ErrCycleDetected
		}
	}

	for _, dep := range newStep.DependsOn {
		if _, exists := m.dag.Nodes[dep]; !exists {
			return ErrInvalidDependency
		}
	}

	// Build simulated adjacency list for cycle detection.
	adjList := make(map[string][]string)
	for nodeID := range m.dag.Nodes {
		if nodeID == oldID && newStep.ID != oldID {
			continue
		}
		adjList[nodeID] = nil
	}
	if newStep.ID != oldID {
		adjList[newStep.ID] = nil
	}
	for src, targets := range m.dag.Edges {
		effSrc := src
		if src == oldID && newStep.ID != oldID {
			effSrc = newStep.ID
		}
		if _, ok := adjList[effSrc]; !ok {
			continue
		}
		for _, t := range targets {
			effTgt := t
			if t == oldID && newStep.ID != oldID {
				effTgt = newStep.ID
			}
			if _, ok := adjList[effTgt]; !ok {
				continue
			}
			adjList[effSrc] = append(adjList[effSrc], effTgt)
		}
	}
	for _, dep := range newStep.DependsOn {
		adjList[dep] = append(adjList[dep], newStep.ID)
	}

	if hasCycleInAdjList(adjList) {
		return ErrCycleDetected
	}

	// Apply mutation.
	if newStep.ID != oldID {
		m.dag.Nodes[newStep.ID] = &DAGNode{StepID: newStep.ID, Metadata: cloneMetadata(newStep.Metadata)}
		m.dag.Edges[newStep.ID] = m.dag.Edges[oldID]
		delete(m.dag.Edges, oldID)
		for src, targets := range m.dag.Edges {
			for i, t := range targets {
				if t == oldID {
					m.dag.Edges[src][i] = newStep.ID
				}
			}
		}
		// Update downstream steps' DependsOn to reflect the edge migration.
		for _, step := range m.steps {
			for i, dep := range step.DependsOn {
				if dep == oldID {
					step.DependsOn[i] = newStep.ID
				}
			}
		}
		delete(m.dag.Nodes, oldID)
		delete(m.steps, oldID)
		m.steps[newStep.ID] = newStep
	} else {
		// Same-ID replacement (#31): edges contributed by the OLD step's
		// DependsOn that are absent from the new step's DependsOn must be
		// removed, otherwise the node silently keeps stale dependencies.
		if oldStep, ok := m.steps[oldID]; ok {
			newDeps := make(map[string]bool, len(newStep.DependsOn))
			for _, dep := range newStep.DependsOn {
				newDeps[dep] = true
			}
			for _, oldDep := range oldStep.DependsOn {
				if newDeps[oldDep] {
					continue
				}
				targets := m.dag.Edges[oldDep]
				for i, t := range targets {
					if t == oldID {
						m.dag.Edges[oldDep] = append(targets[:i], targets[i+1:]...)
						break
					}
				}
			}
		}

		m.steps[oldID] = newStep
		// Same-ID replace: refresh the node's Metadata snapshot from the new
		// step so a metadata-only replace is visible to WorkflowDiffer (C4).
		m.dag.Nodes[oldID].Metadata = cloneMetadata(newStep.Metadata)
		// Add new DependsOn edges, checking for duplicates.
		for _, dep := range newStep.DependsOn {
			duplicate := false
			for _, target := range m.dag.Edges[dep] {
				if target == oldID {
					duplicate = true
					break
				}
			}
			if !duplicate {
				m.dag.Edges[dep] = append(m.dag.Edges[dep], oldID)
			}
		}
	}

	m.recalculateDegrees()
	m.version++

	m.hub.Publish(GraphEvent{
		Change: GraphChange{
			Type:      ChangeReplaceNode,
			NodeID:    newStep.ID,
			OldNodeID: oldID,
			Step:      newStep,
			Timestamp: time.Now(),
		},
		Success: true,
	})

	return nil
}
