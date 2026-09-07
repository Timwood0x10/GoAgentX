// Package system_runtime — direct unit tests for Orchestrator lifecycle.
package kernel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// errSentinel is a distinct error used to assert specific failure propagation.
var errSentinel = errors.New("sentinel failure")

// lifecycleComp is a controllable component exposing explicit lifecycle hooks
// and call counters so tests can assert ordering and rollback behavior.
type lifecycleComp struct {
	name string
	deps []string

	// errors injected per phase; nil means the phase succeeds.
	bindErr  error
	startErr error
	readyErr error
	stopErr  error
	waitErr  error

	// blockWait makes Wait block forever (no context), simulating a
	// misbehaving component that ignores shutdown.
	blockWait bool

	// failReady marks the component as started then fails Ready, simulating
	// a partial start that must be cleaned up by rollback.
	failReady bool

	bindCalls  *int32
	startCalls *int32
	stopCalls  *int32
}

func (c *lifecycleComp) Name() string { return c.name }
func (c *lifecycleComp) Dependencies() []string {
	return c.deps
}

func (c *lifecycleComp) Bind(_ context.Context, _ Resolver) error {
	if c.bindCalls != nil {
		atomic.AddInt32(c.bindCalls, 1)
	}
	return c.bindErr
}

func (c *lifecycleComp) Start(_ context.Context) error {
	if c.startCalls != nil {
		atomic.AddInt32(c.startCalls, 1)
	}
	return c.startErr
}

func (c *lifecycleComp) Ready(_ context.Context) error {
	if c.failReady {
		return errSentinel
	}
	return c.readyErr
}

func (c *lifecycleComp) Stop(_ context.Context) error {
	if c.stopCalls != nil {
		atomic.AddInt32(c.stopCalls, 1)
	}
	return c.stopErr
}

func (c *lifecycleComp) Wait() error {
	if c.blockWait {
		<-make(chan struct{}) // block forever, ignoring cancellation
	}
	return c.waitErr
}

func TestOrchestrator_Start_HappyPath(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	// b depends on a; c depends on b. Expect order a, b, c.
	requireNoErr(t, reg.Register(&lifecycleComp{name: "a"}, ModeRequired))
	requireNoErr(t, reg.Register(&lifecycleComp{name: "b", deps: []string{"a"}}, ModeRequired))
	requireNoErr(t, reg.Register(&lifecycleComp{name: "c", deps: []string{"b"}}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for _, name := range []string{"a", "b", "c"} {
		st, ok := reg.GetStatus(name)
		if !ok {
			t.Fatalf("no status for %q", name)
		}
		if st.State != StateReady {
			t.Fatalf("component %q expected Ready, got %s", name, st.State)
		}
	}
}

func TestOrchestrator_Start_ReadyFailureTriggersRollback(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	var aStop, aStart, bStart, bStop int32
	requireNoErr(t, reg.Register(&lifecycleComp{
		name: "a", startCalls: &aStart, stopCalls: &aStop,
	}, ModeRequired))
	requireNoErr(t, reg.Register(&lifecycleComp{
		name: "b", deps: []string{"a"}, startCalls: &bStart, stopCalls: &bStop,
		// b starts successfully but fails Ready — a partial start.
		failReady: true,
	}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	err := o.Start(context.Background())
	if err == nil {
		t.Fatal("expected Start to fail when Ready fails")
	}
	if atomic.LoadInt32(&bStart) != 1 {
		t.Fatalf("b should have started once, got %d", atomic.LoadInt32(&bStart))
	}
	// The failing component's Stop must be attempted (cleanup of partial start).
	if atomic.LoadInt32(&bStop) == 0 {
		t.Fatal("failing component b must be cleaned up via Stop during rollback")
	}
	// Previously started a must be stopped in rollback.
	if atomic.LoadInt32(&aStart) != 1 || atomic.LoadInt32(&aStop) != 1 {
		t.Fatalf("a must start once and stop once: start=%d stop=%d",
			atomic.LoadInt32(&aStart), atomic.LoadInt32(&aStop))
	}
}

func TestOrchestrator_Start_StartFailureTriggersCleanup(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	var aStart, aStop, bStop int32
	requireNoErr(t, reg.Register(&lifecycleComp{
		name: "a", startCalls: &aStart, stopCalls: &aStop,
	}, ModeRequired))
	requireNoErr(t, reg.Register(&lifecycleComp{
		name: "b", deps: []string{"a"}, startErr: errSentinel, stopCalls: &bStop,
	}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail when Start phase fails")
	}
	// b failed during Start (before Ready), so it must be cleaned up.
	if atomic.LoadInt32(&bStop) == 0 {
		t.Fatal("failed component b must be cleaned up via Stop")
	}
	// a rolled back.
	if atomic.LoadInt32(&aStart) != 1 || atomic.LoadInt32(&aStop) != 1 {
		t.Fatalf("a must start and stop once: start=%d stop=%d",
			atomic.LoadInt32(&aStart), atomic.LoadInt32(&aStop))
	}
}

func TestOrchestrator_Start_BindFailureNoStart(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	var aStart, aStop, bStart int32
	requireNoErr(t, reg.Register(&lifecycleComp{
		name: "a", startCalls: &aStart, stopCalls: &aStop,
	}, ModeRequired))
	requireNoErr(t, reg.Register(&lifecycleComp{
		name: "b", deps: []string{"a"}, bindErr: errSentinel, startCalls: &bStart,
	}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail on Bind failure")
	}
	if atomic.LoadInt32(&bStart) != 0 {
		t.Fatalf("b must not start when Bind fails, got %d", atomic.LoadInt32(&bStart))
	}
	// a rolled back.
	if atomic.LoadInt32(&aStart) != 1 || atomic.LoadInt32(&aStop) != 1 {
		t.Fatalf("a must start and stop once: start=%d stop=%d",
			atomic.LoadInt32(&aStart), atomic.LoadInt32(&aStop))
	}
}

func TestOrchestrator_Start_MissingDependencyFails(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "a", deps: []string{"ghost"}}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail on unregistered dependency")
	}
}

func TestOrchestrator_Start_NotIdempotent(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "a"}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := o.Start(context.Background()); err == nil {
		t.Fatal("expected second Start to return an error (not idempotent)")
	}
}

func TestOrchestrator_Shutdown_CancelsRootContext(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "a"}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	// A managed goroutine waiting on the root context must be released.
	released := make(chan struct{})
	o.Go(func() error {
		<-o.RootContext().Done()
		close(released)
		return nil
	})

	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := o.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("root context was not cancelled; managed goroutine not released")
	}
}

// orderingComp wraps a lifecycleComp with an onStop callback to record the
// order in which Stop is invoked during shutdown.
type orderingComp struct {
	*lifecycleComp
	onStop func(name string)
}

func (c *orderingComp) Stop(_ context.Context) error {
	if c.onStop != nil {
		c.onStop(c.name)
	}
	return c.stopErr
}

func TestOrchestrator_Shutdown_StopsInReverseOrder(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	var mu sync.Mutex
	var stopOrder []string
	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		stopOrder = append(stopOrder, name)
	}

	// c depends on b; b depends on a. Start order a, b, c. Shutdown c, b, a.
	requireNoErr(t, reg.Register(&orderingComp{
		lifecycleComp: &lifecycleComp{name: "a"}, onStop: record,
	}, ModeRequired))
	requireNoErr(t, reg.Register(&orderingComp{
		lifecycleComp: &lifecycleComp{name: "b", deps: []string{"a"}}, onStop: record,
	}, ModeRequired))
	requireNoErr(t, reg.Register(&orderingComp{
		lifecycleComp: &lifecycleComp{name: "c", deps: []string{"b"}}, onStop: record,
	}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := o.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	mu.Lock()
	got := append([]string(nil), stopOrder...)
	mu.Unlock()
	want := []string{"c", "b", "a"}
	if len(got) != len(want) {
		t.Fatalf("expected stop order %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected stop order %v, got %v", want, got)
		}
	}
}

func TestOrchestrator_Shutdown_IdempotentAndConcurrent(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	var aStop int32
	requireNoErr(t, reg.Register(&lifecycleComp{
		name: "a", stopCalls: &aStop,
	}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = o.Shutdown(context.Background())
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&aStop) != 1 {
		t.Fatalf("Stop must run exactly once under concurrent Shutdown, got %d",
			atomic.LoadInt32(&aStop))
	}
}

func TestOrchestrator_Shutdown_AggregatesErrors(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	// b fails Stop; a's managed goroutine returns an error.
	requireNoErr(t, reg.Register(&lifecycleComp{
		name: "a", stopErr: nil,
	}, ModeRequired))
	requireNoErr(t, reg.Register(&lifecycleComp{
		name: "b", deps: []string{"a"}, stopErr: errSentinel,
	}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Managed goroutine that returns an error → errgroup.Wait reports it.
	o.Go(func() error {
		return errSentinel
	})

	err := o.Shutdown(context.Background())
	if err == nil {
		t.Fatal("expected Shutdown to aggregate stop + errgroup errors")
	}
	if !errors.Is(err, errSentinel) {
		t.Fatalf("expected sentinel error in aggregate, got %v", err)
	}
}

func TestOrchestrator_Shutdown_BlockingWaiterTimesOut(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{
		name: "a", blockWait: true,
	}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	start := time.Now()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	// The blocking Waiter has no context; Shutdown must respect the caller's
	// 5s deadline instead of the 30s stopTimeout. Allow a small margin. The
	// Wait aborts gracefully via the shutdown context (a caller-imposed
	// deadline is not a component error, so Shutdown may return nil).
	if err := o.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 7*time.Second {
		t.Fatalf("Shutdown blocked too long with a blocking Waiter: %v", elapsed)
	}
}

func TestOrchestrator_CancelSignalsRootContext(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	requireNoErr(t, reg.Register(&lifecycleComp{name: "a"}, ModeRequired))

	o := NewOrchestrator(reg, rootCtx)
	select {
	case <-o.RootContext().Done():
		t.Fatal("root context must not be cancelled before Cancel()")
	default:
	}
	o.Cancel()
	select {
	case <-o.RootContext().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel() must cancel the managed root context")
	}
}
