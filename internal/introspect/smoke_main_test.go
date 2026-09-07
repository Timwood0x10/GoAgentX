package introspect

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// TestSmokeServePanel renders the real handler with POPULATED data and writes
// both responses to /tmp so a human (or curl) can inspect the served page.
func TestSmokeServePanel(t *testing.T) {
	var store Store
	store.Set(Snapshot{
		TS:  time.Now(),
		Seq: 42,
		Kernel: kernel.SchedulerSnapshot{
			PollInterval: 500 * time.Millisecond, PreemptInterval: time.Second,
			TTL: 5 * time.Minute, MaxConcurrent: 8,
			Executors: 4, BoundExecutors: 1, Scheduled: 128, ReadyTasks: 3,
			EventDriven: true, GovernanceWired: true, AgentFabricWired: true,
			Load: kernel.LoadTrackerSnapshot{Agents: []kernel.AgentLoadSnapshot{
				{AgentID: "coder-1", Done: 40, Ok: 38, Load: 0.75},
				{AgentID: "reviewer-1", Done: 12, Ok: 9, Load: 0.25},
			}},
		},
		Fabric: []taskfabric.LeaseEntry{
			{TaskID: "task-alpha", Capability: "code", State: taskfabric.StateLeased,
				Owner: "coder-1", Epoch: 2, ExpiresAt: time.Now().Add(83 * time.Second),
				Dependencies: []string{"task-root"}, HasCheckpoint: true},
			{TaskID: "task-beta", Capability: "review", State: taskfabric.StateReady,
				Dependencies: []string{"task-alpha"}},
			{TaskID: "task-gamma", Capability: "test", State: taskfabric.StateSuspended,
				Owner: "reviewer-1", Epoch: 1, ExpiresAt: time.Now().Add(25 * time.Second),
				HasCheckpoint: true},
		},
		Agents: []agentfabric.AgentView{
			{Identity: "leader", State: agentfabric.StateIdle, Capabilities: []string{"orchestrate"}, Confidence: 1},
			{Identity: "coder-1", State: agentfabric.StateRunning, Capabilities: []string{"code", "refactor"}, Load: 0.75, Confidence: 0.95, Parent: "leader"},
			{Identity: "reviewer-1", State: agentfabric.StateSuspended, Capabilities: []string{"review"}, Load: 0.25, Confidence: 0.8},
		},
	})
	h := NewHandler(&store)

	ts := httptest.NewServer(h)
	defer ts.Close()

	for _, path := range []string{"/introspect", "/api/v1/introspect/snapshot"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body := make([]byte, 0, 1<<16)
		n, _ := resp.Body.Read(body)
		_ = n
		buf := make([]byte, 0)
		tmp := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(tmp)
			buf = append(buf, tmp[:n]...)
			if err != nil {
				break
			}
		}
		name := map[string]string{"/introspect": "panel.html", "/api/v1/introspect/snapshot": "snapshot.json"}[path]
		out := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(out, buf, 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s -> %d bytes (%d)", path, len(buf), resp.StatusCode)
	}
}
