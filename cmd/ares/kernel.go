package main

import (
	"context"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agentipc"
	"github.com/Timwood0x10/ares/internal/agents/peer"
	"github.com/Timwood0x10/ares/internal/agentsyscall"
	"github.com/Timwood0x10/ares/internal/ares_runtime"
	"github.com/Timwood0x10/ares/internal/aresrecovery"
	"github.com/Timwood0x10/ares/internal/introspect"
	"github.com/Timwood0x10/ares/internal/planprojection"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// kernelHandle carries the assembled kernel from agent construction to the
// serve wiring.
//
// The Kernel pillars (ares-runtime.md §13) are assembled here:
//   - fabric:   Scheduler pillar (taskfabric: Create/Schedule/Acquire/RunQuantum)
//   - agents:   Lifecycle pillar (agentfabric: spawn/suspend/resume/retire/kill)
//   - recovery: Lifecycle recovery surface (aresrecovery: lease-expiry requeue /
//     checkpoint resume / agent restart)
//   - dual/flag: IPC pillar (agentipc: single-track Task Fabric dispatch +
//     execution policy; the legacy leader track was removed — aresos-agentos-plan
//     C1)
type kernelHandle struct {
	dual *agentipc.DualTrackDispatcher
	flag *agentipc.PolicyFlag

	fabric    *taskfabric.Fabric
	agents    *agentfabric.Fabric
	recovery  *aresrecovery.Recovery
	executors map[string]CapabilityExecutor
	// scheduler is the running kernelScheduler. Retained so
	// wireKernelLifecycle can attach the P3 governance provider once the agent
	// fabric exists (the scheduler may start before the lifecycle wiring).
	scheduler *kernelScheduler
	// intro serves the runtime introspection panel (monitoring.md). Wired in
	// createAndServeAgents when the full kernel exists; nil on partial paths.
	intro *introspect.Handler
	// tracker is the shared per-agent load/confidence/priority source for the
	// scheduler and the fabric dispatch path. It is created at startup and
	// retained so agent priorities can be injected into it (B2: OS-thread-style
	// thread priority).
	tracker *loadTracker
	// peerRegistry is the direct peer-to-peer messaging discovery surface
	// (primitive 2). Retained so the registry built at serve time stays
	// reachable for agent messaging / capability discovery instead of being
	// discarded (N4: peer registry return value was dropped).
	peerRegistry *peer.Registry
	// syscalls is the agentsyscall.Kernel backing spawn_agent/create_task/
	// ask_agent. Retained so the collaboration IPC bridge (built later in
	// setupPeerRegistry) can inject ipc.Send into ask_agent (Step Y.2-ACT).
	// Nil on partial paths without syscalls.
	syscalls *agentsyscall.Kernel
	// compileCoord is the C1 projection coordinator. It projects the live
	// MutableDAG into taskfabric PlanSteps and records compile provenance
	// for introspection. Nil when no live DAG is wired.
	compileCoord *planprojection.CompileCoordinator
	// sessionReg is the per-session L2 graph registry (M4-B2). Non-nil only
	// when the DAG execution gate is open; submitPeerTask admits sessions
	// through it. Nil = legacy path, session payloads stay envelope-only.
	sessionReg *agentfabric.SessionRegistry
	// pluginBus is the runtime plugin ecosystem hooked to the scheduler's
	// quantum boundary (runtime_bridge.go). Nil when the scheduler is absent.
	pluginBus *ares_runtime.PluginBus
	// schedulerStop / schedulerDone drive the scheduler drain loop's managed
	// teardown (K2): Stop cancels the loop context, Wait joins the goroutine.
	// Nil on partial paths — the adopt adapter skips those hooks.
	schedulerStop context.CancelFunc
	schedulerDone chan struct{}
	// recoveryStop / recoveryDone do the same for the kernel recovery loop.
	recoveryStop context.CancelFunc
	recoveryDone chan struct{}
	flipped      bool
}
