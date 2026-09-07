package kernel

// Architecture red line (agent-os-loop-wiring plan gate 6.3): the kernel
// scheduler must never import internal/ares_runtime. The dependency direction
// is one-way — cmd/ares adapts the runtime plugin ecosystem INTO the
// scheduler via the QuantumHook interface — so the engine stays free of the
// runtime plugin graph. If this test fails, the plugin bus leaked into the
// scheduling core; route the dependency through a cmd-layer adapter instead.

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSchedulerMustNotImportRuntime(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test file path")
	}
	dir := filepath.Dir(thisFile)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	// C1.2: the banned list is a slice so it can grow. Adding
	// internal/workflow/engine prevents the kernel scheduler from
	// importing the planner package — the projection runs in the cmd
	// layer, never inside the kernel. This is a regression guard: the
	// current import count is 0; the test ensures it stays 0.
	banned := []string{
		"internal/ares_runtime",
		"internal/workflow/engine",
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		if name == filepath.Base(thisFile) {
			// Skip the gate itself: its documentation quotes the banned
			// import paths and would otherwise trip its own scan.
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			for _, b := range banned {
				if strings.Contains(line, b) {
					t.Fatalf("%s:%d: kernel must not import %s (dependency direction: cmd/ares adapters inject the runtime INTO the kernel, never the reverse)",
						name, i+1, b)
				}
			}
		}
	}
}
