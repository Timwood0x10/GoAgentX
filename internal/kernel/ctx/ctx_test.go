package ctx

import (
	"context"
	"testing"
)

// TestWithCallerIDRoundTrip verifies the caller identity survives a context
// round-trip.
func TestWithCallerIDRoundTrip(t *testing.T) {
	ctx := WithCallerID(context.Background(), "agent-A")
	if got := CallerID(ctx); got != "agent-A" {
		t.Fatalf("CallerID = %q, want agent-A", got)
	}
}

// TestWithCallerIDEmptyNoValue verifies an empty caller writes nothing into
// the context — a root call stays indistinguishable from a bare context.
func TestWithCallerIDEmptyNoValue(t *testing.T) {
	base := context.WithValue(context.Background(), callerIDKey{}, "ghost")
	ctx := WithCallerID(base, "")
	if got := CallerID(ctx); got != "ghost" {
		t.Fatalf("CallerID = %q, want the pre-existing value untouched (ghost)", got)
	}
}

// TestCallerIDAbsentIsEmpty verifies a context without a stamped caller reads
// as "" (root call).
func TestCallerIDAbsentIsEmpty(t *testing.T) {
	if got := CallerID(context.Background()); got != "" {
		t.Fatalf("CallerID = %q, want \"\" for a root context", got)
	}
}
