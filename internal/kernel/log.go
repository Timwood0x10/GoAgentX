package kernel

import (
	"log/slog"

	"github.com/Timwood0x10/ares/internal/logger"
)

// log is the module-scoped structured logger for the kernel scheduler.
// Production code in this package must use this instead of the standard
// library log package (code_rules_v2 §9.1: library code does not print
// directly; output goes through an injected logger or event sink).
var log *slog.Logger = logger.Module("kernel")
