// Package ctx carries Kernel-validated caller identity through tool execution
// contexts. The execution bodies (sub executor, agentfabric chat cognition,
// agentloop engine) stamp the calling agent's ID into the context before
// invoking a tool; the Kernel syscalls (agentsyscall) read it back so
// provenance (Task.Origin) is enforced by the Kernel, never trusted from
// LLM-supplied arguments.
package ctx

import "context"

type callerIDKey struct{}

// WithCallerID returns a context carrying the calling agent's identity.
// When agentID is empty, the original context is returned unchanged (no
// value written), so a root / user-submitted call does not poison the
// context with a zero-value identity.
func WithCallerID(ctx context.Context, agentID string) context.Context {
	if agentID == "" {
		return ctx
	}
	return context.WithValue(ctx, callerIDKey{}, agentID)
}

// CallerID extracts the calling agent's identity from the context.
// Returns "" when absent — a root call (e.g. user-submitted task, system
// bootstrap) where no agent is the caller.
func CallerID(ctx context.Context) string {
	id, _ := ctx.Value(callerIDKey{}).(string)
	return id
}
