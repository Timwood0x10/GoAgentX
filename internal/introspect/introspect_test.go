package introspect

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/ares_events"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// TestCollectorAssemblesDomains verifies that Collect maps all three source
// domains into one frame and increments the sequence.
func TestCollectorAssemblesDomains(t *testing.T) {
	c := NewCollector(Sources{
		Kernel: func() kernel.SchedulerSnapshot {
			return kernel.SchedulerSnapshot{Scheduled: 7, ReadyTasks: 2}
		},
		Fabric: func() []taskfabric.LeaseEntry {
			return []taskfabric.LeaseEntry{{TaskID: "t-1", State: taskfabric.StateReady}}
		},
		Agents: func() []agentfabric.AgentView {
			return []agentfabric.AgentView{{Identity: "a", State: agentfabric.StateIdle}}
		},
	})

	snap := c.Collect()
	if snap.Seq != 1 {
		t.Fatalf("seq = %d, want 1", snap.Seq)
	}
	if snap.Kernel.Scheduled != 7 || snap.Kernel.ReadyTasks != 2 {
		t.Errorf("kernel domain not mapped: %+v", snap.Kernel)
	}
	if len(snap.Fabric) != 1 || snap.Fabric[0].TaskID != "t-1" {
		t.Errorf("fabric domain not mapped: %+v", snap.Fabric)
	}
	if len(snap.Agents) != 1 || snap.Agents[0].Identity != "a" {
		t.Errorf("agents domain not mapped: %+v", snap.Agents)
	}
}

// TestStoreHoldsLatestOnly locks the O(1) memory contract: Set overwrites and
// Latest always returns the newest frame.
func TestStoreHoldsLatestOnly(t *testing.T) {
	var s Store
	if s.Latest() != nil {
		t.Fatal("Latest before first Set must be nil")
	}
	s.Set(Snapshot{Seq: 1})
	s.Set(Snapshot{Seq: 2})
	got := s.Latest()
	if got == nil || got.Seq != 2 {
		t.Fatalf("latest seq = %+v, want 2", got)
	}
}

// TestHandlerRoutes covers the three route behaviors: UI page, JSON snapshot,
// and 503 before the collector's first tick.
func TestHandlerRoutes(t *testing.T) {
	var store Store
	h := NewHandler(&store)

	t.Run("snapshot_503_before_first_collect", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/introspect/snapshot", nil))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("got %d, want 503", w.Code)
		}
	})

	store.Set(Snapshot{Seq: 9, TS: time.Now()})

	t.Run("snapshot_json", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/introspect/snapshot", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("got %d, want 200", w.Code)
		}
		var snap Snapshot
		if err := json.Unmarshal(w.Body.Bytes(), &snap); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if snap.Seq != 9 {
			t.Errorf("seq = %d, want 9", snap.Seq)
		}
	})

	t.Run("ui_page_served", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/introspect", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("got %d, want 200", w.Code)
		}
		if ct := w.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
			t.Errorf("content-type = %q", ct)
		}
		if len(w.Body.Bytes()) < 500 {
			t.Error("panel html suspiciously small")
		}
	})

	t.Run("unknown_path_404", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/introspect/nope", nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("got %d, want 404", w.Code)
		}
	})
}

// TestConcurrentCollectSetLatest exercises the race surface the serve loop
// will hit: collector writing while HTTP readers poll (go test -race judge).
func TestConcurrentCollectSetLatest(t *testing.T) {
	c := NewCollector(Sources{Fabric: func() []taskfabric.LeaseEntry { return nil }})
	var store Store
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			store.Set(c.Collect())
		}
	}()
	for i := 0; i < 200; i++ {
		_ = store.Latest()
	}
	<-done
	if c.seq.Load() != 200 {
		t.Fatalf("seq = %d, want 200", c.seq.Load())
	}
}

// TestSnapshotJSONContract locks the UI⇄API key alignment (#panel): the JS in
// web/panel.html reads these exact lowercase-camel keys; a struct without json
// tags would silently render an empty panel (caught by smoke review).
func TestSnapshotJSONContract(t *testing.T) {
	var store Store
	store.Set(Snapshot{
		Kernel: kernel.SchedulerSnapshot{
			Scheduled: 1, ReadyTasks: 1,
			Load: kernel.LoadTrackerSnapshot{Agents: []kernel.AgentLoadSnapshot{
				{AgentID: "a", Done: 3, Ok: 2, Load: 0.5},
			}},
		},
		Fabric: []taskfabric.LeaseEntry{{TaskID: "t", State: taskfabric.StateReady}},
		Agents: []agentfabric.AgentView{{Identity: "ag", State: agentfabric.StateIdle}},
	})
	w := httptest.NewRecorder()
	NewHandler(&store).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/introspect/snapshot", nil))
	require := func(cond bool, what string) {
		if !cond {
			t.Errorf("json contract broken: missing %s", what)
		}
	}
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	k, _ := m["kernel"].(map[string]any)
	require(k != nil, "kernel")
	for _, key := range []string{"pollInterval", "preemptInterval", "ttl", "maxConcurrent",
		"eventDriven", "executors", "boundExecutors", "scheduled", "readyTasks",
		"governanceWired", "agentFabricWired", "load"} {
		_, ok := k[key]
		require(ok, "kernel."+key)
	}
	load, _ := k["load"].(map[string]any)
	agents, _ := load["agents"].([]any)
	require(len(agents) == 1, "kernel.load.agents[0]")
	a0, _ := agents[0].(map[string]any)
	for _, key := range []string{"agentID", "done", "ok", "load"} {
		_, ok := a0[key]
		require(ok, "agent."+key)
	}
	fab, _ := m["fabric"].([]any)
	require(len(fab) == 1, "fabric[0]")
	f0, _ := fab[0].(map[string]any)
	for _, key := range []string{"taskID", "state", "owner", "epoch", "expiresAt", "hasCheckpoint", "dependencies"} {
		_, ok := f0[key]
		require(ok, "fabric."+key)
	}
	ags, _ := m["agents"].([]any)
	require(len(ags) == 1, "agents[0]")
	g0, _ := ags[0].(map[string]any)
	for _, key := range []string{"identity", "state", "capabilities", "confidence", "parent"} {
		_, ok := g0[key]
		require(ok, "agentView."+key)
	}
}

// TestMapTimelineEvent locks the feed's voice: terse, leveled, lifecycle-only
// (noise types are dropped). Deaths are danger, completions ok.
func TestMapTimelineEvent(t *testing.T) {
	mk := func(typ ares_events.EventType, payload map[string]any) *ares_events.Event {
		return &ares_events.Event{Type: typ, Payload: payload, Timestamp: time.Now()}
	}

	e, ok := MapTimelineEvent(mk(ares_events.EventAgentStopped,
		map[string]any{"agent_id": "worker_01", "reason": "chaos"}))
	if !ok || e.Level != "danger" || e.Text != "worker_01 died (chaos)" {
		t.Fatalf("death row wrong: %+v ok=%v", e, ok)
	}

	e, ok = MapTimelineEvent(mk(ares_events.EventTaskStarted,
		map[string]any{"task_id": "t-9", "agent_id": "coder"}))
	if !ok || e.Text != "t-9 → coder" || e.Level != "info" {
		t.Fatalf("dispatch row wrong: %+v", e)
	}

	if _, ok := MapTimelineEvent(mk("internal.tick", nil)); ok {
		t.Error("noise events must be dropped")
	}
	if _, ok := MapTimelineEvent(nil); ok {
		t.Error("nil event must be dropped")
	}
}

// TestStoreEventRing bounds the activity ring and returns newest-first.
func TestStoreEventRing(t *testing.T) {
	var s Store
	for i := 0; i < maxTimelineEntries+50; i++ {
		s.PushEvent(TimelineEntry{Text: fmt.Sprint(i)})
	}
	got := s.Events(5)
	if len(got) != 5 {
		t.Fatalf("len = %d, want 5", len(got))
	}
	// newest first: the last pushed had index maxTimelineEntries+49
	if got[0].Text != fmt.Sprint(maxTimelineEntries+49) {
		t.Errorf("newest-first violated: got %q", got[0].Text)
	}
	all := s.Events(0)
	if len(all) != maxTimelineEntries {
		t.Fatalf("ring cap = %d, want %d", len(all), maxTimelineEntries)
	}
}
