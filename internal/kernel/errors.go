package kernel

import "errors"

// Sentinel errors for the scheduling path. They stay plain so errors.Is can
// match them; call sites wrap with internal/errors.Kernel when structured
// task/agent/op/code attribution is needed.
var (
	// ErrNilStepOutcome reports that the executor returned a nil quantum step outcome.
	ErrNilStepOutcome = errors.New("kernel: executor returned a nil step outcome")
	// ErrNoCandidate reports that no capable candidate was available.
	ErrNoCandidate = errors.New("kernel: no capable candidate")
)
