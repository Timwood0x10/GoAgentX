// Package kernel — lifecycle orchestrator.
package kernel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Timwood0x10/ares/internal/ares_events"
)

// stopTimeout bounds a single component's Stop/Wait during shutdown.
const stopTimeout = 30 * time.Second

// waitTimeout bounds the errgroup drain during Shutdown. Components that
// ignore the cancelled root context are reported instead of blocking forever.
const waitTimeout = 30 * time.Second

// overallShutdownTimeout bounds the WHOLE Shutdown sequence when the caller
// provides no deadline of its own (K4). Per-component timeouts already bound
// each Stop/Wait; this cap keeps the total teardown finite even when many
// components are registered.
const overallShutdownTimeout = 30 * time.Second

// ErrShuttingDown is returned by Adopt when the orchestrator has already
// begun (or completed) its shutdown sequence: late registration must fail
// loudly instead of silently joining a graph that is being torn down.
var ErrShuttingDown = errors.New("kernel: orchestrator is shutting down")

// Orchestrator drives the component lifecycle: Construct → Bind → Start → Ready
// on startup, and Stop → Wait → Close on shutdown, in topological order.
type Orchestrator struct {
	registry *Registry
	rootCtx  context.Context
	cancel   context.CancelFunc
	eg       *errgroup.Group
	// events is the optional event sink for background-loop failure records
	// (K3). Nil disables event emission; the loop panic is always logged and
	// reflected in the component status regardless.
	events ares_events.EventStore

	mu      sync.Mutex
	started bool
	stopped bool
}

// NewOrchestrator creates a new orchestrator with the given registry.
// The root context is derived from the provided context and will be
// used as the parent for all component lifecycle operations.
func NewOrchestrator(reg *Registry, rootCtx context.Context) *Orchestrator {
	ctx, cancel := context.WithCancel(rootCtx)
	eg, egCtx := errgroup.WithContext(ctx)
	// The errgroup-derived context is the managed root context: it is
	// cancelled both by Cancel() and whenever any managed goroutine fails,
	// so components that select on RootContext().Done() are signalled in
	// both paths.
	return &Orchestrator{
		registry: reg,
		rootCtx:  egCtx,
		cancel:   cancel,
		eg:       eg,
	}
}

// Start executes the full startup sequence for all registered components
// in topological order: Construct (already done by registration) →
// Bind → Start → Ready. On failure, the failing component is cleaned up
// if it had started, then all previously started components are stopped
// in reverse order (rollback). Start is not idempotent: calling it twice
// returns an error.
func (o *Orchestrator) Start(ctx context.Context) error {
	o.mu.Lock()
	if o.started || o.stopped {
		o.mu.Unlock()
		return errors.New("kernel: Start called after startup already began or completed")
	}
	o.started = true
	o.mu.Unlock()

	order, err := o.registry.TopologicalOrder()
	if err != nil {
		return fmt.Errorf("kernel: startup: %w", err)
	}

	var started []string

	for _, name := range order {
		if err := o.startComponent(ctx, name); err != nil {
			// The failing component may have partially started (Start()
			// succeeded, Ready() failed, or Start() returned error after
			// spawning goroutines). Clean it up before rolling back the
			// previously started components so nothing leaks.
			if status, ok := o.registry.GetStatus(name); ok && status.State == StateFailed {
				o.cleanupComponent(ctx, name)
			}
			o.rollback(ctx, started)
			return fmt.Errorf("kernel: startup failed at %q: %w", name, err)
		}
		started = append(started, name)
	}

	log.Info("kernel: all components started",
		"count", len(started))
	return nil
}

// startComponent executes Bind → Start → Ready for one component.
// Skips Disabled components. Updates the registry status on each transition.
func (o *Orchestrator) startComponent(ctx context.Context, name string) error {
	comp := o.registry.GetComponent(name)
	if comp == nil {
		return fmt.Errorf("component %q not found", name)
	}

	mode, _ := o.registry.GetMode(name)

	// Check if component is disabled (by config gate in Stage 2).
	status, _ := o.registry.GetStatus(name)
	if status.State == StateDisabled {
		log.Info("kernel: skipping disabled component",
			"component", name)
		return nil
	}

	// Bind phase.
	if binder, ok := comp.(Binder); ok {
		if err := binder.Bind(ctx, o.registry); err != nil {
			o.setStatus(name, StateFailed, err.Error())
			return fmt.Errorf("bind: %w", err)
		}
	}
	o.setStatus(name, StateBound, "")

	// Start phase.
	if starter, ok := comp.(Starter); ok {
		if err := starter.Start(ctx); err != nil {
			o.setStatus(name, StateFailed, err.Error())
			return fmt.Errorf("start: %w", err)
		}
	}
	o.setStatusStarted(name, StateStarted)

	// Ready phase.
	if checker, ok := comp.(ReadinessChecker); ok {
		if err := checker.Ready(ctx); err != nil {
			// For Degraded mode, a Ready failure enters Degraded state.
			if mode == ModeDegraded {
				o.setStatus(name, StateDegraded, err.Error())
				log.Warn("kernel: component degraded",
					"component", name, "reason", err)
			} else {
				o.setStatus(name, StateFailed, err.Error())
				return fmt.Errorf("ready: %w", err)
			}
		}
	}

	// If we get here without entering Degraded, mark as Ready.
	currentStatus, _ := o.registry.GetStatus(name)
	if currentStatus.State != StateDegraded {
		o.setStatus(name, StateReady, "")
	}

	log.Info("kernel: component ready",
		"component", name, "state", currentStatus.State)
	return nil
}

// Shutdown executes the full shutdown sequence: it cancels the managed root
// context first (so goroutines waiting on RootContext().Done() exit), then
// stops all started components in reverse topological order, then drains the
// errgroup with a bounded timeout. Stop/Wait/errgroup errors are aggregated
// and returned. Shutdown is idempotent and safe to call concurrently: only
// the first call runs the sequence.
//
// K4: the whole sequence runs under an overall budget — the caller's context
// deadline when it has one, otherwise overallShutdownTimeout. Components that
// did not reach Stopped when the budget expires are named in the returned
// error (and logged) instead of blocking the process exit forever.
func (o *Orchestrator) Shutdown(ctx context.Context) error {
	o.mu.Lock()
	if o.stopped {
		o.mu.Unlock()
		return nil
	}
	o.stopped = true
	o.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	// Apply the overall cap only when the caller did not impose a deadline:
	// an operator-provided budget (e.g. the serve graceful-shutdown window)
	// is the tighter contract and must win.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, overallShutdownTimeout)
		defer cancel()
	}

	// Signal all managed goroutines first so they stop accepting work
	// before we Stop the components that feed them.
	o.cancel()

	var errs []error

	order, err := o.registry.TopologicalOrder()
	if err != nil {
		// On error, just stop in registration order.
		order = o.registry.Names()
	}

	// Reverse order for shutdown.
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]
		if ctx.Err() != nil {
			errs = append(errs, fmt.Errorf(
				"system_runtime: shutdown budget expired before stopping %q", name))
			break
		}
		if err := o.stopComponent(ctx, name); err != nil {
			errs = append(errs, fmt.Errorf("kernel: stop %q: %w", name, err))
		}
	}

	// Report every component that did not reach a terminal stopped state so
	// a truncated teardown is visible instead of silent (K4).
	var notStopped []string
	for _, st := range o.registry.AllStatuses() {
		if st.State != StateStopped && st.State != StateDisabled {
			notStopped = append(notStopped, fmt.Sprintf("%s=%s", st.Name, st.State))
		}
	}
	if len(notStopped) > 0 {
		errs = append(errs, fmt.Errorf(
			"system_runtime: components did not reach Stopped: %s", strings.Join(notStopped, ", ")))
		log.Warn("kernel: shutdown left components unstopped",
			"components", strings.Join(notStopped, ", "))
	}

	// Wait for all errgroup goroutines, bounded so a misbehaving component
	// cannot hang shutdown forever.
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- o.eg.Wait()
	}()
	select {
	case waitErr := <-waitCh:
		if waitErr != nil {
			errs = append(errs, fmt.Errorf("kernel: errgroup wait: %w", waitErr))
		}
	case <-time.After(waitTimeout):
		errs = append(errs, fmt.Errorf("kernel: errgroup wait timed out after %s", waitTimeout))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	log.Info("kernel: shutdown complete")
	return nil
}

// stopComponent executes Stop → Wait for one component.
func (o *Orchestrator) stopComponent(ctx context.Context, name string) error {
	comp := o.registry.GetComponent(name)
	if comp == nil {
		return nil
	}

	status, _ := o.registry.GetStatus(name)
	if status.State == StateDisabled || status.State == StateStopped {
		return nil
	}

	o.setStatus(name, StateStopping, "")

	if stopper, ok := comp.(Stopper); ok {
		stopCtx, stopCancel := context.WithTimeout(ctx, stopTimeout)
		defer stopCancel()
		if err := stopper.Stop(stopCtx); err != nil {
			o.setStatus(name, StateFailed, err.Error())
			return err
		}
	}

	if waiter, ok := comp.(Waiter); ok {
		// Wait() has no context, so bound it both by stopTimeout and by the
		// caller's context. A shorter Shutdown deadline must not be held up
		// by a component that ignores cancellation.
		waitCh := make(chan error, 1)
		go func() {
			waitCh <- waiter.Wait()
		}()
		select {
		case waitErr := <-waitCh:
			if waitErr != nil {
				log.Warn("kernel: wait error",
					"component", name, "error", waitErr)
			}
		case <-time.After(stopTimeout):
			log.Warn("kernel: wait timed out (goroutine leaked)",
				"component", name, "timeout", stopTimeout)
		case <-ctx.Done():
			log.Warn("kernel: wait aborted by shutdown context (goroutine leaked)",
				"component", name)
		}
	}

	o.setStatus(name, StateStopped, "")
	return nil
}

// cleanupComponent best-effort Stops and Waits a component that failed
// startup after it had already started. Unlike stopComponent it does not
// touch the registry status, so the Failed state remains observable.
func (o *Orchestrator) cleanupComponent(ctx context.Context, name string) {
	comp := o.registry.GetComponent(name)
	if comp == nil {
		return
	}
	if stopper, ok := comp.(Stopper); ok {
		stopCtx, stopCancel := context.WithTimeout(ctx, stopTimeout)
		defer stopCancel()
		if err := stopper.Stop(stopCtx); err != nil {
			log.Warn("kernel: cleanup stop error",
				"component", name, "error", err)
		}
	}
	if waiter, ok := comp.(Waiter); ok {
		waitCh := make(chan error, 1)
		go func() {
			waitCh <- waiter.Wait()
		}()
		select {
		case waitErr := <-waitCh:
			if waitErr != nil {
				log.Warn("kernel: cleanup wait error",
					"component", name, "error", waitErr)
			}
		case <-time.After(stopTimeout):
			log.Warn("kernel: cleanup wait timed out (goroutine leaked)",
				"component", name, "timeout", stopTimeout)
		case <-ctx.Done():
			log.Warn("kernel: cleanup wait aborted by context (goroutine leaked)",
				"component", name)
		}
	}
}

// rollback stops already-started components in reverse order after a
// startup failure. This ensures no goroutine or resource leaks.
func (o *Orchestrator) rollback(ctx context.Context, started []string) {
	log.Warn("kernel: rolling back startup",
		"started_count", len(started))
	for i := len(started) - 1; i >= 0; i-- {
		name := started[i]
		if err := o.stopComponent(ctx, name); err != nil {
			log.Warn("kernel: rollback stop error",
				"component", name, "error", err)
		}
	}
}

// Go submits a background goroutine to the orchestrator's errgroup.
// Use this instead of bare `go` for all managed background work.
func (o *Orchestrator) Go(fn func() error) {
	o.eg.Go(fn)
}

// Adopt registers a component AFTER Start has already run (K1: late
// admission, e.g. the kernel pillars assembled later in the serve flow) and
// drives its startup state record so the component joins the status snapshot
// and the reverse-topological Shutdown sequence.
//
// Adopt does NOT call Start: the component's active work (goroutines,
// tickers) is owned by the adopter and must already be running. Adopt runs
// Bind (for components that need wiring) and the Ready gate:
//   - Required mode: a Ready failure marks the component Failed and returns
//     an error — a late component that claims readiness falsely must fail
//     loudly, never silently degrade the graph (the "false Ready" trap).
//   - Degraded mode: a Ready failure marks the component Degraded with the
//     reason and returns nil.
//
// Validation runs BEFORE registration so a rejected component never
// pollutes the graph: every declared dependency must already be registered
// and not Failed; a duplicate name is rejected (existing state is never
// overwritten). Adopt during or after Shutdown returns ErrShuttingDown —
// the teardown always wins the race.
func (o *Orchestrator) Adopt(ctx context.Context, c Component, mode Mode) error {
	o.mu.Lock()
	stopped := o.stopped
	o.mu.Unlock()
	if stopped {
		return ErrShuttingDown
	}
	// The errgroup root context is cancelled at the top of Shutdown, so it
	// is the authoritative in-flight signal for a shutdown that started
	// between the flag check above and now.
	select {
	case <-o.rootCtx.Done():
		return ErrShuttingDown
	default:
	}

	if c == nil || isNilComponent(c) {
		return errors.New("kernel: cannot adopt nil component")
	}
	name := c.Name()
	// Fail-loud dependency validation before registration: a missing or
	// failed dependency means the adopted component would join the graph in
	// a state that can never become Ready.
	for _, dep := range c.Dependencies() {
		st, ok := o.registry.GetStatus(dep)
		if !ok {
			return fmt.Errorf("kernel: adopt %q: dependency %q is not registered", name, dep)
		}
		if st.State == StateFailed {
			return fmt.Errorf("kernel: adopt %q: dependency %q is Failed: %s",
				name, dep, st.Reason)
		}
	}
	if err := o.registry.Register(c, mode); err != nil {
		return err
	}

	comp := o.registry.GetComponent(name)
	// Bind phase: wiring-only; adopters own their own Start.
	if binder, ok := comp.(Binder); ok {
		if err := binder.Bind(ctx, o.registry); err != nil {
			o.setStatus(name, StateFailed, err.Error())
			return fmt.Errorf("kernel: adopt %q bind: %w", name, err)
		}
	}
	// The component is already running (adoption implies started work);
	// record the started timestamp so Snapshot shows a real instance.
	o.setStatusStarted(name, StateStarted)

	if checker, ok := comp.(ReadinessChecker); ok {
		if err := checker.Ready(ctx); err != nil {
			if mode == ModeDegraded {
				o.setStatus(name, StateDegraded, err.Error())
				log.Warn("kernel: adopted component degraded",
					"component", name, "reason", err)
				return nil
			}
			o.setStatus(name, StateFailed, err.Error())
			return fmt.Errorf("kernel: adopt %q ready: %w", name, err)
		}
	}
	current, _ := o.registry.GetStatus(name)
	if current.State != StateDegraded {
		o.setStatus(name, StateReady, "")
	}
	final, _ := o.registry.GetStatus(name)
	log.Info("kernel: component adopted",
		"component", name, "state", final.State)
	return nil
}

// GoBackground runs fn as an errgroup-managed background loop (K3: the
// unified entry for cmd/ares long-lived loops, replacing bare `go`).
//
// The name identifies the owning component: a loop that panics is recovered,
// logged, recorded on the event sink (FlightRecorder timeline) and marks
// that component Failed — the process survives, the damage is visible. A
// normal error return records the exit reason and marks the component Failed
// too, because a dead loop must never keep showing Ready. Neither a panic
// nor an error is propagated into the errgroup: one misbehaving loop must
// not cancel every other managed goroutine (the process-wide teardown is
// driven by Shutdown, not by a single loop's death).
//
// Status marking is skipped while the orchestrator is shutting down: loops
// exiting on the cancelled root context is the normal teardown path, not a
// failure. Loops whose name matches no registered component still get the
// panic recovery and logging — only the status mark is skipped.
func (o *Orchestrator) GoBackground(name string, fn func(ctx context.Context) error) {
	o.mu.Lock()
	stopped := o.stopped
	o.mu.Unlock()
	if stopped {
		log.Warn("kernel: background loop not started (orchestrator shutting down)",
			"component", name)
		return
	}
	o.eg.Go(func() error {
		err := o.runBackground(o.rootCtx, name, fn)
		select {
		case <-o.rootCtx.Done():
			// Teardown exit: the shutdown report covers the component, and
			// marking Failed here would corrupt the final snapshot.
		default:
			if err != nil {
				log.Warn("kernel: background loop exited with error",
					"component", name, "error", err)
				o.markBackgroundFailed(name, err.Error())
			} else {
				log.Info("kernel: background loop exited",
					"component", name)
			}
		}
		return nil
	})
}

// runBackground invokes fn with a recover boundary so a panicking loop is
// contained: logged, recorded, marked Failed, never propagated.
func (o *Orchestrator) runBackground(ctx context.Context, name string, fn func(ctx context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("system_runtime: background loop panicked",
				"component", name, "panic", r)
			o.markBackgroundFailed(name, fmt.Sprintf("panic: %v", r))
			err = nil // contained: do not crash the process (K3)
		}
	}()
	return fn(ctx)
}

// markBackgroundFailed marks the named component Failed with the given
// reason and emits a component.failed event on the configured sink (K3/K5:
// the FlightRecorder subscribes to the whole event stream, so the panic
// lands on the flight timeline). Best-effort: an unwired sink or missing
// component only skips that part of the record; the log line is the
// always-present trace.
func (o *Orchestrator) markBackgroundFailed(name, reason string) {
	select {
	case <-o.rootCtx.Done():
		return
	default:
	}
	if st, ok := o.registry.GetStatus(name); ok {
		st.State = StateFailed
		st.Reason = reason
		o.registry.SetStatus(name, st)
	}
	if o.events == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	evt := &ares_events.Event{
		Type:       ares_events.EventComponentFailed,
		ModuleName: "system_runtime",
		Payload:    map[string]any{"component": name, "reason": reason},
		Timestamp:  time.Now(),
	}
	if err := o.events.Append(ctx, "system_runtime/"+name, []*ares_events.Event{evt}, 0); err != nil {
		log.Warn("kernel: component failure event not recorded",
			"component", name, "error", err)
	}
}

// SetEventSink attaches the optional event store used to record background
// component failures (K3). Nil (the default) disables event emission.
func (o *Orchestrator) SetEventSink(store ares_events.EventStore) {
	o.events = store
}

// Snapshot returns a point-in-time status view of all managed components
// (K5: the introspect panel and startup logs consume this instead of
// reaching into the registry).
func (o *Orchestrator) Snapshot() Snapshot {
	return o.registry.Snapshot()
}

// RootContext returns the managed root context. Components should use
// this context (or a derived one) for goroutines that need to respect
// the orchestrator's lifecycle. It is cancelled by Cancel(), by Shutdown,
// and when any managed goroutine returns an error.
func (o *Orchestrator) RootContext() context.Context {
	return o.rootCtx
}

// Cancel cancels the root context, signalling all managed goroutines.
func (o *Orchestrator) Cancel() {
	o.cancel()
}

// setStatus updates the component status in the registry.
func (o *Orchestrator) setStatus(name string, state State, reason string) {
	status, ok := o.registry.GetStatus(name)
	if !ok {
		return
	}
	status.State = state
	status.Reason = reason
	o.registry.SetStatus(name, status)
}

// setStatusStarted updates status with a timestamp when a component starts.
func (o *Orchestrator) setStatusStarted(name string, state State) {
	status, ok := o.registry.GetStatus(name)
	if !ok {
		return
	}
	status.State = state
	status.StartedAt = time.Now()
	status.InstanceID = fmt.Sprintf("%s-%d", name, status.StartedAt.UnixNano())
	o.registry.SetStatus(name, status)
}
