// Package system_runtime — K1/K3/K4 acceptance tests: Orchestrator.Adopt
// (late registration), GoBackground (managed background loops) and the
// Shutdown not-stopped report.
package kernel

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOrchestrator_Adopt_HappyPath(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "eventstore"}, ModeRequired))
	var adoptedStop int32
	adopted := &lifecycleComp{name: "late", deps: []string{"eventstore"}, stopCalls: &adoptedStop}

	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := o.Adopt(context.Background(), adopted, ModeRequired); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	st, ok := reg.GetStatus("late")
	if !ok {
		t.Fatal("adopted component missing from registry")
	}
	if st.State != StateReady {
		t.Fatalf("adopted component expected Ready, got %s", st.State)
	}
	if st.StartedAt.IsZero() {
		t.Fatal("adopted component must carry a StartedAt timestamp")
	}

	// The adopted component must join the reverse-topological shutdown.
	if err := o.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if atomic.LoadInt32(&adoptedStop) != 1 {
		t.Fatalf("adopted component Stop must run during Shutdown, got %d calls", adoptedStop)
	}
}

func TestOrchestrator_Adopt_MissingDependencyRejected(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	err := o.Adopt(context.Background(), &lifecycleComp{name: "late", deps: []string{"ghost"}}, ModeRequired)
	if err == nil {
		t.Fatal("Adopt with an unregistered dependency must fail")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("error must name the missing dependency, got %v", err)
	}
	// A rejected component must not pollute the registry.
	if _, ok := reg.GetStatus("late"); ok {
		t.Fatal("rejected component must not be registered")
	}
}

func TestOrchestrator_Adopt_FailedDependencyRejected(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	failed := &lifecycleComp{name: "dep", startErr: errSentinel}
	requireNoErr(t, reg.Register(failed, ModeRequired))
	o := NewOrchestrator(reg, rootCtx)
	// Start fails on dep; the registry keeps dep in Failed state.
	_ = o.Start(context.Background())

	err := o.Adopt(context.Background(), &lifecycleComp{name: "late", deps: []string{"dep"}}, ModeRequired)
	if err == nil {
		t.Fatal("Adopt with a Failed dependency must fail")
	}
	if _, ok := reg.GetStatus("late"); ok {
		t.Fatal("rejected component must not be registered")
	}
}

func TestOrchestrator_Adopt_DuplicateRejected(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "late"}, ModeRequired))
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// A second component with the same name must be rejected and must not
	// overwrite the existing entry's state.
	before, _ := reg.GetStatus("late")
	err := o.Adopt(context.Background(), &lifecycleComp{name: "late", readyErr: errSentinel}, ModeRequired)
	if err == nil {
		t.Fatal("duplicate Adopt must fail")
	}
	after, _ := reg.GetStatus("late")
	if after.State != before.State {
		t.Fatalf("duplicate Adopt must not overwrite existing state: before=%s after=%s",
			before.State, after.State)
	}
}

func TestOrchestrator_Adopt_DuringShutdownReturnsErrShuttingDown(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "a"}, ModeRequired))
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := o.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := o.Adopt(context.Background(), &lifecycleComp{name: "late"}, ModeRequired); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("Adopt after Shutdown must return ErrShuttingDown, got %v", err)
	}
}

func TestOrchestrator_Adopt_RequiredReadyFailureIsFailed(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	err := o.Adopt(context.Background(), &lifecycleComp{name: "late", readyErr: errSentinel}, ModeRequired)
	if err == nil {
		t.Fatal("Required-mode Ready failure must fail the adoption")
	}
	st, ok := reg.GetStatus("late")
	if !ok || st.State != StateFailed {
		t.Fatalf("component must be visible as Failed, got %+v (ok=%v)", st, ok)
	}
	if !strings.Contains(st.Reason, "sentinel") {
		t.Fatalf("Failed reason must carry the readiness error, got %q", st.Reason)
	}
}

func TestOrchestrator_Adopt_DegradedReadyFailureIsDegraded(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := o.Adopt(context.Background(), &lifecycleComp{name: "late", readyErr: errSentinel}, ModeDegraded); err != nil {
		t.Fatalf("Degraded-mode Ready failure must not fail adoption, got %v", err)
	}
	st, _ := reg.GetStatus("late")
	if st.State != StateDegraded {
		t.Fatalf("component must be Degraded, got %s", st.State)
	}
	if st.Reason == "" {
		t.Fatal("Degraded state must carry a readable reason")
	}
}

// TestOrchestrator_GoBackground_PanicMarksComponentFailed covers the K3
// contract: a panicking background loop is contained (the process — here the
// test — survives), the named component is marked Failed with a readable
// reason, and the failure is not propagated into the errgroup.
func TestOrchestrator_GoBackground_PanicMarksComponentFailed(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "loop-owner"}, ModeRequired))
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	o.GoBackground("loop-owner", func(ctx context.Context) error {
		panic("boom")
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		st, ok := reg.GetStatus("loop-owner")
		if ok && st.State == StateFailed && strings.Contains(st.Reason, "boom") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("component not marked Failed with panic reason; status=%+v ok=%v", st, ok)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A healthy loop must keep the component untouched by the failed one.
	o.GoBackground("unregistered-loop", func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})
}

// TestOrchestrator_GoBackground_ErrorReturnMarksFailed covers the "a dead
// loop must never keep showing Ready" rule: an error return after a normal
// (non-shutdown) run marks the component Failed with the exit reason.
func TestOrchestrator_GoBackground_ErrorReturnMarksFailed(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "loop-owner"}, ModeRequired))
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	o.GoBackground("loop-owner", func(ctx context.Context) error {
		return errSentinel
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		st, ok := reg.GetStatus("loop-owner")
		if ok && st.State == StateFailed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("component not marked Failed after error return; status=%+v ok=%v", st, ok)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestOrchestrator_GoBackground_ShutdownExitKeepsStopped covers the teardown
// rule: loops exiting on the cancelled root context during Shutdown must NOT
// be marked Failed (the final snapshot must stay clean).
func TestOrchestrator_GoBackground_ShutdownExitKeepsStopped(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "loop-owner"}, ModeRequired))
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	o.GoBackground("loop-owner", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	// Give the loop a moment to block on ctx.Done before shutting down.
	// (Polling, not a fixed sleep, per code_rules 7.3.)
	deadline := time.Now().Add(time.Second)
	for o.registry.Snapshot().Components[0].State != StateReady {
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if err := o.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	st, _ := reg.GetStatus("loop-owner")
	if st.State == StateFailed {
		t.Fatalf("loop exiting on shutdown must not be marked Failed, got %s (%s)", st.State, st.Reason)
	}
}

// TestOrchestrator_Shutdown_ReportsNonStoppedComponents covers K4: a
// component whose Stop fails stays visible as not-stopped and is named in
// the Shutdown error.
func TestOrchestrator_Shutdown_ReportsNonStoppedComponents(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "bad", stopErr: errSentinel}, ModeRequired))
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	err := o.Shutdown(context.Background())
	if err == nil {
		t.Fatal("Shutdown must report components that did not reach Stopped")
	}
	if !strings.Contains(err.Error(), "bad") {
		t.Fatalf("Shutdown error must name the unstopped component, got %v", err)
	}
}

// TestOrchestrator_Shutdown_HappyPathNoReport guards against a regression:
// a clean teardown must not start reporting unstopped components (the K4
// report only fires when something actually failed to stop).
func TestOrchestrator_Shutdown_HappyPathNoReport(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "a"}, ModeRequired))
	requireNoErr(t, reg.Register(&lifecycleComp{name: "b", deps: []string{"a"}}, ModeRequired))
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := o.Shutdown(context.Background()); err != nil {
		t.Fatalf("clean Shutdown must return nil, got %v", err)
	}
}

// TestOrchestrator_Snapshot_IncludesAdopted covers K5: the orchestrator
// snapshot lists adopted kernel components with their states.
func TestOrchestrator_Snapshot_IncludesAdopted(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "a"}, ModeRequired))
	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := o.Adopt(context.Background(), &lifecycleComp{name: "scheduler"}, ModeDegraded); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	snap := o.Snapshot()
	found := map[string]State{}
	for _, c := range snap.Components {
		found[c.Name] = c.State
	}
	if found["a"] != StateReady || found["scheduler"] != StateReady {
		t.Fatalf("snapshot missing expected states: %v", found)
	}
}
