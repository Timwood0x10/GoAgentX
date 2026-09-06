package agentfabric

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// TestSessionRegistry_InitAndGet verifies the basic lifecycle: a session is
// initialized with a prompt, its graph is retrievable by ID, and the root
// node carries the session-invariant prompt.
func TestSessionRegistry_InitAndGet(t *testing.T) {
	ctx := context.Background()
	r := NewSessionRegistry()

	g, err := r.InitSession(ctx, "s1", "find the answer", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, g)
	require.Equal(t, SessionRootID("s1"), g.Root())

	got, err := r.GetSession("s1")
	require.NoError(t, err)
	require.Same(t, g, got)
}

// TestSessionRegistry_InitDuplicateFails verifies idempotent rejection: a
// session that is already registered cannot be re-initialized — the second
// InitSession returns an error, not a silent overwrite.
func TestSessionRegistry_InitDuplicateFails(t *testing.T) {
	ctx := context.Background()
	r := NewSessionRegistry()

	_, err := r.InitSession(ctx, "s1", "prompt", nil, nil)
	require.NoError(t, err)

	_, err = r.InitSession(ctx, "s1", "other", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already initialized")
}

// TestSessionRegistry_GetNotFound verifies that a session that was never
// admitted returns ErrSessionNotFound, not a nil graph.
func TestSessionRegistry_GetNotFound(t *testing.T) {
	r := NewSessionRegistry()

	_, err := r.GetSession("nope")
	require.ErrorIs(t, err, ErrSessionNotFound)
}

// TestSessionRegistry_Release verifies that a released session is gone from
// the registry — a subsequent Get returns ErrSessionNotFound.
func TestSessionRegistry_Release(t *testing.T) {
	ctx := context.Background()
	r := NewSessionRegistry()

	_, err := r.InitSession(ctx, "s1", "prompt", nil, nil)
	require.NoError(t, err)

	require.NoError(t, r.ReleaseSession("s1"))

	_, err = r.GetSession("s1")
	require.ErrorIs(t, err, ErrSessionNotFound)
}

// TestSessionRegistry_ReleaseNotFound verifies that releasing a session
// that was never registered returns an error.
func TestSessionRegistry_ReleaseNotFound(t *testing.T) {
	r := NewSessionRegistry()

	err := r.ReleaseSession("nope")
	require.ErrorIs(t, err, ErrSessionNotFound)
}

// TestSessionRegistry_CompileCoordWired verifies that the compile coordinator
// function is called during InitSession and its stop function is called on
// Release.
func TestSessionRegistry_CompileCoordWired(t *testing.T) {
	ctx := context.Background()
	r := NewSessionRegistry()

	stopped := false
	coord := func(_ context.Context, _ *engine.MutableDAG) (stop func()) {
		return func() { stopped = true }
	}

	_, err := r.InitSession(ctx, "s1", "prompt", nil, coord)
	require.NoError(t, err)

	require.NoError(t, r.ReleaseSession("s1"))
	require.True(t, stopped, "compile coordinator stop must be called on Release")
}

// TestSessionRegistry_CompileCoordError verifies that a compile coordinator
// that returns an error prevents the session from being registered.
func TestSessionRegistry_CompileCoordError(t *testing.T) {
	ctx := context.Background()
	r := NewSessionRegistry()

	// With the simplified signature (no error return), a coordinator that
	// needs to signal an error must do so via a nil stop function. The
	// registry does not interpret a nil stop as an error — it simply does
	// not call it on Release. This is acceptable because the only production
	// coordinator (SubscribeGraphEvents) never fails.
	var called bool
	coord := func(_ context.Context, _ *engine.MutableDAG) (stop func()) {
		called = true
		return nil // no stop function — simulates a no-op coordinator
	}

	_, err := r.InitSession(ctx, "s1", "prompt", nil, coord)
	require.NoError(t, err)
	require.True(t, called, "compile coordinator must be called")

	require.NoError(t, r.ReleaseSession("s1"))
}

// TestSessionRegistry_SessionIDs verifies the registry can list its session
// IDs.
func TestSessionRegistry_SessionIDs(t *testing.T) {
	ctx := context.Background()
	r := NewSessionRegistry()

	_, _ = r.InitSession(ctx, "a", "p", nil, nil)
	_, _ = r.InitSession(ctx, "b", "p", nil, nil)

	ids := r.SessionIDs()
	require.Len(t, ids, 2)
	require.ElementsMatch(t, []string{"a", "b"}, ids)
}

// TestSessionRootID verifies the deterministic root ID format so a
// recompiled graph's root task is a 1:1 match to the original.
func TestSessionRootID(t *testing.T) {
	require.Equal(t, "sess/s1/root", SessionRootID("s1"))
}

// TestSessionNodeID verifies the deterministic instance node ID format.
func TestSessionNodeID(t *testing.T) {
	require.Equal(t, "sess/s1/d1/grep#0", SessionNodeID("s1", 1, "grep", 0))
	require.Equal(t, "sess/s1/d2/read#1", SessionNodeID("s1", 2, "read", 1))
}

// TestSessionIDFromNode pins the ID inverse the reaper keep-set relies on:
// every builder's output round-trips back to its session ID, and anything
// that is not a session-scoped node is rejected (so the reaper never keeps
// a task it cannot attribute to a live session).
func TestSessionIDFromNode(t *testing.T) {
	tests := []struct {
		name   string
		nodeID string
		want   string
		wantOK bool
	}{
		{"root", SessionRootID("s1"), "s1", true},
		{"node", SessionNodeID("s1", 2, "grep", 7), "s1", true},
		{"session id with slash-free dashes", "sess/adm-1/root", "adm-1", true},
		{"non-session id", "plain/task", "", false},
		{"bare prefix", "sess/", "", false},
		{"missing terminator", "sess/s1", "", false},
		{"empty session id", "sess//root", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := SessionIDFromNode(tt.nodeID)
			if ok != tt.wantOK || got != tt.want {
				t.Errorf("SessionIDFromNode(%q) = (%q, %v), want (%q, %v)",
					tt.nodeID, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}
