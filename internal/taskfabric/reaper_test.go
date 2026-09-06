package taskfabric

import (
	"strings"
	"testing"
	"time"
)

// seedCompletedTask creates a task and drives it to Completed through the
// legal state machine (Acquire → Start → Complete).
func seedCompletedTask(t *testing.T, f *Fabric, id string) {
	t.Helper()
	if err := f.Create(&Task{ID: id}); err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
	epoch, err := f.Acquire(id, "w1", 5*time.Second)
	if err != nil {
		t.Fatalf("acquire %s: %v", id, err)
	}
	if err := f.Start(id, "w1", epoch); err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	if err := f.Complete(id, "w1", epoch); err != nil {
		t.Fatalf("complete %s: %v", id, err)
	}
}

// TestReaper_KeepSetProtectsLiveSession locks the P0-1 keep-set semantics:
// a task whose owning session is still live is NEVER harvested, no matter
// how far past the grace window it is — the wall-clock grace must not eat
// a long session's readable history (decision C). Only released sessions
// become harvestable.
func TestReaper_KeepSetProtectsLiveSession(t *testing.T) {
	f := NewFabric()
	seedCompletedTask(t, f, "sess/s1/d0/alpha#1")
	seedCompletedTask(t, f, "sess/s1/d1/beta#2")
	seedCompletedTask(t, f, "sess/s2/d0/alpha#1")
	seedCompletedTask(t, f, "plain/task") // outside the session prefix

	live := map[string]bool{"s1": true}
	keep := func(taskID string) bool {
		rest := strings.TrimPrefix(taskID, "sess/")
		if rest == taskID {
			return false
		}
		sid, _, ok := strings.Cut(rest, "/")
		return ok && live[sid]
	}

	// Grace of 1ns: every seeded task is past the window immediately, so
	// the only thing protecting s1's tasks is the keep predicate.
	r := NewReaperWithKeep(f, "sess/", time.Nanosecond, keep)
	time.Sleep(2 * time.Millisecond)

	if n := r.Sweep(); n != 1 {
		t.Fatalf("sweep harvested %d, want 1 (only the released session)", n)
	}
	for _, id := range []string{"sess/s1/d0/alpha#1", "sess/s1/d1/beta#2"} {
		if _, err := f.Task(id); err != nil {
			t.Fatalf("live session task %s was harvested: %v", id, err)
		}
	}
	if _, err := f.Task("sess/s2/d0/alpha#1"); err == nil {
		t.Fatal("released session task survived the sweep")
	}
	if _, err := f.Task("plain/task"); err != nil {
		t.Fatalf("non-session task touched by the reaper: %v", err)
	}

	// Release s1: its history becomes harvestable on the next sweep.
	live["s1"] = false
	if n := r.Sweep(); n != 2 {
		t.Fatalf("after release, sweep harvested %d, want 2", n)
	}
	for _, id := range []string{"sess/s1/d0/alpha#1", "sess/s1/d1/beta#2"} {
		if _, err := f.Task(id); err == nil {
			t.Fatalf("released session task %s survived", id)
		}
	}
}

// TestReaper_NilKeepKeepsGraceOnlySemantics pins that NewReaper (nil keep)
// still harvests purely on the grace window — the legacy path the existing
// agentfabric tests rely on.
func TestReaper_NilKeepKeepsGraceOnlySemantics(t *testing.T) {
	f := NewFabric()
	seedCompletedTask(t, f, "sess/s1/d0/alpha#1")
	r := NewReaper(f, "sess/", time.Nanosecond)
	time.Sleep(2 * time.Millisecond)
	if n := r.Sweep(); n != 1 {
		t.Fatalf("grace-only sweep harvested %d, want 1", n)
	}
}

// TestReaper_GracePeriodDefault pins the construction default (0 → 30s)
// exposed for startup logging.
func TestReaper_GracePeriodDefault(t *testing.T) {
	if got := NewReaper(NewFabric(), "sess/", 0).GracePeriod(); got != 30*time.Second {
		t.Fatalf("default grace = %s, want 30s", got)
	}
	if got := NewReaper(NewFabric(), "sess/", 5*time.Second).GracePeriod(); got != 5*time.Second {
		t.Fatalf("explicit grace = %s, want 5s", got)
	}
}
