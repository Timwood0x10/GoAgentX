// Package system_runtime — direct unit tests for Registry.
package kernel

import (
	"context"
	"testing"
)

// testComp is a controllable Component for lifecycle tests. Optional
// lifecycle hooks are populated per-test.
type testComp struct {
	name string
	deps []string

	bindErr    error
	startErr   error
	readyErr   error
	stopErr    error
	waitErr    error
	blockWait  bool // when true, Wait blocks until ctx-free channel (never completes)
	startCalls *int
	stopCalls  *int
}

func (c *testComp) Name() string                         { return c.name }
func (c *testComp) Dependencies() []string               { return c.deps }
func (c *testComp) Bind(context.Context, Resolver) error { return c.bindErr }

func (c *testComp) Start(ctx context.Context) error {
	if c.startCalls != nil {
		*c.startCalls++
	}
	return c.startErr
}

func (c *testComp) Ready(ctx context.Context) error { return c.readyErr }

func (c *testComp) Stop(ctx context.Context) error {
	if c.stopCalls != nil {
		*c.stopCalls++
	}
	return c.stopErr
}

func (c *testComp) Wait() error {
	if c.blockWait {
		<-make(chan struct{}) // block forever
	}
	return c.waitErr
}

// typedNilComp is a struct whose nil pointer is wrapped in the Component
// interface — the classic Go nil-interface trap.
type typedNilComp struct{}

func (c *typedNilComp) Name() string           { return "typed-nil" }
func (c *typedNilComp) Dependencies() []string { return nil }

func TestRegistry_Register_RejectsNil(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(nil, ModeRequired); err == nil {
		t.Fatal("expected error registering nil component")
	}
	var tnc *typedNilComp
	if err := r.Register(tnc, ModeRequired); err == nil {
		t.Fatal("expected error registering typed-nil component (nil-interface trap)")
	}
}

func TestRegistry_Register_RejectsInvalidMode(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(&testComp{name: "c"}, Mode(99)); err == nil {
		t.Fatal("expected error for invalid mode")
	}
}

func TestRegistry_Register_DuplicateName(t *testing.T) {
	r := NewRegistry()
	requireNoErr(t, r.Register(&testComp{name: "c"}, ModeRequired))
	if err := r.Register(&testComp{name: "c"}, ModeOptional); err == nil {
		t.Fatal("expected error for duplicate registration")
	}
}

func TestRegistry_TopologicalOrder_HappyPath(t *testing.T) {
	r := NewRegistry()
	// a depends on b; b depends on c → order c, b, a
	requireNoErr(t, r.Register(&testComp{name: "a", deps: []string{"b"}}, ModeRequired))
	requireNoErr(t, r.Register(&testComp{name: "b", deps: []string{"c"}}, ModeRequired))
	requireNoErr(t, r.Register(&testComp{name: "c"}, ModeRequired))

	order, err := r.TopologicalOrder()
	requireNoErr(t, err)
	if len(order) != 3 {
		t.Fatalf("expected 3 components, got %d: %v", len(order), order)
	}
	pos := map[string]int{}
	for i, name := range order {
		pos[name] = i
	}
	if pos["a"] < pos["b"] || pos["b"] < pos["c"] {
		t.Fatalf("dependencies must come before dependents, order=%v", order)
	}
}

func TestRegistry_TopologicalOrder_MissingDependency(t *testing.T) {
	r := NewRegistry()
	requireNoErr(t, r.Register(&testComp{name: "a", deps: []string{"ghost"}}, ModeRequired))
	_, err := r.TopologicalOrder()
	if err == nil {
		t.Fatal("expected error for unregistered dependency (fail-loud), got nil")
	}
}

func TestRegistry_TopologicalOrder_Cycle(t *testing.T) {
	r := NewRegistry()
	requireNoErr(t, r.Register(&testComp{name: "a", deps: []string{"b"}}, ModeRequired))
	requireNoErr(t, r.Register(&testComp{name: "b", deps: []string{"a"}}, ModeRequired))
	_, err := r.TopologicalOrder()
	if err == nil {
		t.Fatal("expected error for dependency cycle")
	}
}

func TestRegistry_IsReady_RequiredMustBeHealthy(t *testing.T) {
	r := NewRegistry()
	requireNoErr(t, r.Register(&testComp{name: "a"}, ModeRequired))
	if r.IsReady() {
		t.Fatal("required component in Declared state must not report Ready")
	}
	st, _ := r.GetStatus("a")
	st.State = StateReady
	r.SetStatus("a", st)
	if !r.IsReady() {
		t.Fatal("all required components Ready must report Ready")
	}
	st.State = StateFailed
	r.SetStatus("a", st)
	if r.IsReady() {
		t.Fatal("failed component must not report Ready")
	}
}

func requireNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
