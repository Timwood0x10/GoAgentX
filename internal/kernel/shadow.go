package kernel

import "github.com/Timwood0x10/ares/internal/core/models"

// ShadowExecutionHook observes finalized real tasks so a shadow A/B executor
// can buffer them for isolated candidate-strategy execution (closure plan
// Step 4 / N-1: real-execution A/B). The interface is defined at the consumer
// (code_rules) — the scheduler knows nothing about the evolution layer, and
// implementers satisfy it structurally without importing this package back.
//
// The hook fires on the drain path, so implementations MUST NOT block:
// buffer-and-return, dropping work when the buffer is full. Real shadow
// execution happens later, outside the scheduling hot path.
type ShadowExecutionHook interface {
	// OnTaskFinalized receives the scheduler's models.Task view of a task
	// whose quantum finalized successfully. Implementations must not mutate
	// it — clone before storing.
	OnTaskFinalized(task *models.Task)
}

// WithShadowExecutionHook attaches the shadow execution hook. Nil disables
// shadow capture (backward compatible).
//
// Args:
//   - h: the non-blocking capture hook; nil clears it.
//
// Returns:
//   - *Scheduler: the receiver, for chaining.
func (s *Scheduler) WithShadowExecutionHook(h ShadowExecutionHook) *Scheduler {
	s.shadowHook = h
	return s
}
