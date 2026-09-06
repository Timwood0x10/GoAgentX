package main

import (
	"context"
	"testing"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/planprojection"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// admissionKernel builds a kernelHandle with the L2 session path wired
// (registry + shared compile coordinator on one fabric).
func admissionKernel() (*kernelHandle, *taskfabric.Fabric) {
	fabric := taskfabric.NewFabric()
	return &kernelHandle{
		fabric:       fabric,
		sessionReg:   agentfabric.NewSessionRegistry(),
		compileCoord: planprojection.NewCompileCoordinator(fabric, nil),
	}, fabric
}

// TestSessionKeepSet pins the reaper keep predicate (P0-1): the session
// registry is the single authority — tasks of live sessions are kept, tasks
// of released sessions are harvestable, and IDs that are not session-scoped
// are never kept (the reaper's prefix filter plus grace handle those).
func TestSessionKeepSet(t *testing.T) {
	ctx := context.Background()
	reg := agentfabric.NewSessionRegistry()
	if _, err := reg.InitSession(ctx, "keep-1", "p", nil, nil); err != nil {
		t.Fatalf("InitSession: %v", err)
	}
	keep := sessionKeepSet(reg)

	if !keep("sess/keep-1/d0/alpha#1") {
		t.Error("live session task must be kept")
	}
	if !keep(agentfabric.SessionRootID("keep-1")) {
		t.Error("live session root must be kept")
	}
	if keep("sess/gone/d0/alpha#1") {
		t.Error("task of an unknown session must not be kept")
	}
	if keep("plain/task") {
		t.Error("non-session id must not be kept")
	}

	if err := reg.ReleaseSession("keep-1"); err != nil {
		t.Fatalf("ReleaseSession: %v", err)
	}
	if keep("sess/keep-1/d0/alpha#1") {
		t.Error("released session task must become harvestable")
	}
}

// TestSubmitPeerTask_AdmitsSessionFirst pins M4-B2 admission: a session-scoped
// submission registers the session and compiles its root BEFORE the user
// task is created, so the planner's first quantum finds a live graph.
func TestSubmitPeerTask_AdmitsSessionFirst(t *testing.T) {
	ctx := context.Background()
	kernel, fabric := admissionKernel()

	taskID, err := submitPeerTask(ctx, kernel, "ares/plan", map[string]any{
		"session_id": "adm-1",
		"input":      "find the answer",
	})
	if err != nil {
		t.Fatalf("submitPeerTask with session payload error = %v", err)
	}
	if taskID == "" {
		t.Fatal("submitPeerTask must return the created task id")
	}
	got, err := kernel.sessionReg.GetSession("adm-1")
	if err != nil {
		t.Fatalf("session must exist after submission: %v", err)
	}
	if got.Root() != agentfabric.SessionRootID("adm-1") {
		t.Errorf("session root = %q, want deterministic root id", got.Root())
	}
	if _, err := fabric.Task(got.Root()); err != nil {
		t.Errorf("session root task must be compiled at admission: %v", err)
	}
	if _, err := fabric.Task(taskID); err != nil {
		t.Errorf("submitted task must exist: %v", err)
	}
}

// TestSubmitPeerTask_ResubmitReusesSession pins admission idempotency: a
// second submission into a live session is a continuation — no error, no
// duplicate root, exactly one root task.
func TestSubmitPeerTask_ResubmitReusesSession(t *testing.T) {
	ctx := context.Background()
	kernel, fabric := admissionKernel()

	payload := map[string]any{"session_id": "adm-2", "input": "prompt"}
	if _, err := submitPeerTask(ctx, kernel, "ares/plan", payload); err != nil {
		t.Fatalf("first submission error = %v", err)
	}
	if _, err := submitPeerTask(ctx, kernel, "ares/plan", payload); err != nil {
		t.Fatalf("resubmission into a live session must not fail: %v", err)
	}
	root := agentfabric.SessionRootID("adm-2")
	seen := 0
	for _, id := range fabric.IDs() {
		if id == root {
			seen++
		}
	}
	if seen != 1 {
		t.Errorf("root task count = %d, want exactly 1 (no duplicate admission)", seen)
	}
}

// TestSubmitPeerTask_SessionlessUnchanged pins the legacy path: without a
// session_id the submission behaves exactly like today — no session, no
// root, just the task.
func TestSubmitPeerTask_SessionlessUnchanged(t *testing.T) {
	ctx := context.Background()
	kernel, fabric := admissionKernel()

	taskID, err := submitPeerTask(ctx, kernel, "worker", map[string]any{"q": "x"})
	if err != nil {
		t.Fatalf("sessionless submission error = %v", err)
	}
	if _, err := fabric.Task(taskID); err != nil {
		t.Fatalf("submitted task must exist: %v", err)
	}
	if n := len(kernel.sessionReg.SessionIDs()); n != 0 {
		t.Errorf("sessionless submission created %d sessions, want 0", n)
	}
}

// TestSubmitPeerTask_GateOffIgnoresSession pins the default: with no registry
// wired (gate off) a session_id payload is envelope-only — no admission, no
// error, legacy behavior byte-for-byte.
func TestSubmitPeerTask_GateOffIgnoresSession(t *testing.T) {
	ctx := context.Background()
	kernel := &kernelHandle{fabric: taskfabric.NewFabric()}

	taskID, err := submitPeerTask(ctx, kernel, "worker", map[string]any{
		"session_id": "adm-off",
		"input":      "prompt",
	})
	if err != nil {
		t.Fatalf("gate-off submission error = %v", err)
	}
	if _, err := kernel.fabric.Task(taskID); err != nil {
		t.Fatalf("submitted task must exist: %v", err)
	}
}

// TestSubmitPeerTask_AdmissionFailureCreatesNothing pins fail-fast: when the
// compile coordinator is missing, the submission errors AND creates nothing —
// no unrunnable task, no half-admitted session.
func TestSubmitPeerTask_AdmissionFailureCreatesNothing(t *testing.T) {
	ctx := context.Background()
	fabric := taskfabric.NewFabric()
	kernel := &kernelHandle{
		fabric:     fabric,
		sessionReg: agentfabric.NewSessionRegistry(),
		// compileCoord nil: admission cannot project grown nodes.
	}

	if _, err := submitPeerTask(ctx, kernel, "ares/plan", map[string]any{
		"session_id": "adm-fail",
		"input":      "prompt",
	}); err == nil {
		t.Fatal("submission without compile coordinator must fail, not degrade silently")
	}
	if len(fabric.IDs()) != 0 {
		t.Errorf("failed admission left %d tasks behind, want 0", len(fabric.IDs()))
	}
	if _, err := kernel.sessionReg.GetSession("adm-fail"); err == nil {
		t.Error("failed admission must not leave a half-admitted session")
	}
}
