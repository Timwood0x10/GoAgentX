package agentfabric

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// SessionRegistry is the per-session L2 graph lifecycle manager (M2-②).
// It owns one L2Graph per SessionID, created on first reference and released
// when the session terminates. The registry solves the wiring gap the design
// document calls out: CognitionFactory (executor.go:53) is called once at
// spawn time without a task, so the L2Graph handle cannot ride a closure —
// the plannerCognition must look it up by the SessionID it reads from the
// task's checkpoint envelope.
//
// The registry is the single owner of every L2Graph in a peer runtime: the
// CompileCoordinator subscribes to the graph's MutableDAG for incremental
// compilation, the plannerCognition grows tool nodes into it, and the reaper
// harvests terminal fabric tasks when the session is released. The graph
// holds topology + Metadata only (§2.2 decision C: execution facts live in
// the fabric task envelopes).
//
// Thread safety: every method is safe for concurrent use. The internal map
// is guarded by a mutex; each L2Graph carries its own RWMutex for graph
// mutations.
type SessionRegistry struct {
	mu       sync.RWMutex // guards sessions
	sessions map[string]*sessionEntry
}

// sessionEntry is one session's L2 graph plus its incremental-compile
// coordinator subscription stop function.
type sessionEntry struct {
	graph   *L2Graph
	stopSub func() // stops the CompileCoordinator's graph-event subscription
}

// ErrSessionNotFound is returned by GetSession/ReleaseSession when no L2
// graph is registered for the requested session. It signals the
// plannerCognition that the session has ended (or was never admitted), so
// it can surface the error instead of silently growing into a phantom graph.
var ErrSessionNotFound = errors.New("agentfabric: session not found")

// ErrSessionAlreadyExists is returned by InitSession when the session was
// admitted before. Concurrent admitters use errors.Is to tell "someone else
// won the race, the session is usable" apart from a real failure.
var ErrSessionAlreadyExists = errors.New("agentfabric: session already initialized")

// NewSessionRegistry creates an empty per-session L2 graph registry.
func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{
		sessions: make(map[string]*sessionEntry),
	}
}

// InitSession creates a new L2 graph for the given session and registers it.
// It is called when the session root task is first admitted — the root is
// compiled as a fabric task and its prompt rides the envelope. The
// compileCoord function wires the graph into the incremental compilation
// pipeline (SubscribeGraphEvents) so every subsequent AddToolNode is projected
// to a fabric task. Returns the graph handle so the caller (session admission
// path) can grow the initial plan node.
//
// Args:
//   - sessionID: the session identifier (must be non-empty).
//   - prompt: the session-invariant prompt stored on the root node.
//   - params: the session-invariant params flattened onto root Metadata.
//   - ctx: bounds the incremental-compile subscription's lifetime.
//   - compileCoord: wires the graph into the incremental compiler; called
//     once under the registry lock. Nil = no incremental compilation (test
//     path only). The returned stop function is called on Release.
//
// Returns:
//   - *L2Graph: the session's execution plan.
//   - error: when sessionID is empty, a graph already exists for this
//     session, or graph creation fails.
func (r *SessionRegistry) InitSession(
	ctx context.Context,
	sessionID, prompt string,
	params map[string]any,
	compileCoord func(ctx context.Context, dag *engine.MutableDAG) (stop func()),
) (*L2Graph, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("agentfabric: session registry: session id is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sessions[sessionID]; exists {
		return nil, fmt.Errorf("%w: %q", ErrSessionAlreadyExists, sessionID)
	}

	rootID := SessionRootID(sessionID)
	g, err := NewL2Graph(rootID, prompt, params)
	if err != nil {
		return nil, fmt.Errorf("agentfabric: session registry: init session %q: %w", sessionID, err)
	}

	entry := &sessionEntry{graph: g}
	if compileCoord != nil {
		// ctx is the caller's, not Background: ReleaseSession is the normal
		// stop, but a session that is never released (abandoned, crash on the
		// admission path) must still die with its owner's context instead of
		// leaking the compile subscription for the process lifetime.
		entry.stopSub = compileCoord(ctx, g.DAG())
	}

	r.sessions[sessionID] = entry
	return g, nil
}

// GetSession returns the L2 graph for the given session, or
// ErrSessionNotFound when no session is registered. The plannerCognition
// calls this on every ExecuteStep to look up the graph it grows nodes into.
func (r *SessionRegistry) GetSession(sessionID string) (*L2Graph, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrSessionNotFound, sessionID)
	}
	return entry.graph, nil
}

// ReleaseSession tears down a session's L2 graph: stops the incremental
// compile subscription and removes the entry so future references fail
// fast. Called when the session's answer node completes (the terminal
// event) or the session is abandoned. The fabric tasks are NOT deleted here
// — that is the reaper's job (M2-⑤); Release only drops the graph handle so
// no new nodes can be grown into a finished session.
func (r *SessionRegistry) ReleaseSession(sessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.sessions[sessionID]
	if !ok {
		return fmt.Errorf("%w: %q", ErrSessionNotFound, sessionID)
	}
	if entry.stopSub != nil {
		entry.stopSub()
	}
	delete(r.sessions, sessionID)
	return nil
}

// SessionIDs returns a snapshot of every LIVE session ID — released sessions
// are removed from the registry, so they never appear here. A task reaper uses
// it as the keep-set: a session task whose session is absent belongs to a
// finished session and is harvestable.
func (r *SessionRegistry) SessionIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.sessions))
	for id := range r.sessions {
		out = append(out, id)
	}
	return out
}

// SessionRootID builds the deterministic root node ID for a session's L2
// graph. The root ID is stable across rebuilds (the same sessionID always
// yields the same root ID) so a recompiled graph's root task is a 1:1 match
// to the original — the rebuild-idempotency test pins this property.
func SessionRootID(sessionID string) string {
	return sessionIDPrefix + sessionID + "/root"
}

// SessionNodeID builds a deterministic instance node ID for a tool execution
// in a session's L2 graph. The ID encodes the session, depth, tool name, and
// sequence number so it is unique within the session and stable across
// rebuilds.
func SessionNodeID(sessionID string, depth int, tool string, seq int) string {
	return fmt.Sprintf("sess/%s/d%d/%s#%d", sessionID, depth, tool, seq)
}

// sessionIDPrefix is the shared stem of every L2 session node/task ID
// (SessionRootID / SessionNodeID). The terminal-task reaper filters on it.
const sessionIDPrefix = "sess/"

// SessionIDFromNode extracts the owning session ID from an L2 node/task ID
// ("sess/<sessionID>/…" → sessionID). It is the inverse of the ID builders
// above, so the reaper's keep-set can map a fabric task ID back to the
// registry key without re-deriving the format. Returns ok=false for any ID
// that is not a session-scoped node.
func SessionIDFromNode(nodeID string) (string, bool) {
	rest := strings.TrimPrefix(nodeID, sessionIDPrefix)
	if rest == nodeID { // no prefix
		return "", false
	}
	sid, _, found := strings.Cut(rest, "/")
	if !found || sid == "" {
		return "", false
	}
	return sid, true
}
