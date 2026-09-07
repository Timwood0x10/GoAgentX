package main

import (
	"github.com/Timwood0x10/ares/internal/kernel"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// This file is the cmd/ares compatibility layer over the shared
// internal/kernel package (aresos-agentos-plan H1/H2: 合并 SDK 和
// kernel 两条路径 — the scheduler logic lives in one importable package, both
// cmd/ares and sdk drive the same engine). cmd/ares keeps its historical
// names so no caller (kernel wiring, peer mode, tests) changes.

// CapabilityExecutor is the scheduler's executor contract, aliased from the
// shared package so the whole cmd/ares codebase and the kernel loops use the
// identical interface.
type CapabilityExecutor = kernel.CapabilityExecutor

// kernelScheduler aliases the shared Scheduler, preserving cmd/ares's
// historical naming throughout kernel.go / kernel_loop.go / peer_mode.go /
// serve.go and their tests.
type kernelScheduler = kernel.Scheduler

// loadTracker aliases the shared per-agent load/confidence tracker.
type loadTracker = kernel.LoadTracker

// NewKernelScheduler creates the shared scheduler over a fabric. It mirrors
// the historical cmd/ares constructor signature; the implementation lives in
// kernel.New.
func NewKernelScheduler(fabric *taskfabric.Fabric, executors map[string]CapabilityExecutor, tracker *loadTracker) *kernelScheduler {
	return kernel.New(fabric, executors, tracker)
}

// newLoadTracker creates a shared tracker.
func newLoadTracker() *loadTracker {
	return kernel.NewLoadTracker()
}
