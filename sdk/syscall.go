package sdk

import (
	"context"
	"fmt"

	"github.com/Timwood0x10/ares/api/core"
	"github.com/Timwood0x10/ares/api/tools"
	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/agentsyscall"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/kernel"
)

// This file wires the spawn_agent / create_task syscalls (D1) into the SDK
// path. Previously the syscalls were bound only in peer mode (cmd/ares/
// peer_mode.go: BindTools(toolBinder, kernelSyscall)), so an SDK user's agent
// never saw the tools and could not autonomously decompose a task. The SDK is
// now a peer-runtime facade over the SAME kernel the serve path uses, so the
// syscalls operate on the same agent fabric + task fabric + scheduler:
//
//	SDK runtime → agentsyscall.Kernel → sdkFabric (tasks) + agentsFabric (agents)
//	                                      → sched.RegisterExecutor (spawned agents)
//
// spawn_agent / create_task are registered into the runtime's tool registry,
// so every SDK agent's LLM tool list carries them (see resolveTools) and the
// registry executes them (the same ToolExecutor the agentloop engine uses).

// syscallBinder adapts the SDK tool registry to the agentsyscall.ToolBinder
// contract. BindTools only needs BindTool(name, fn); the SDK registry exposes
// Register(tool), so each syscall is wrapped in a thin tool adapter.
type syscallBinder struct {
	reg *tools.Registry
}

var _ agentsyscall.ToolBinder = (*syscallBinder)(nil)

func (b *syscallBinder) BindTool(name string, toolFunc func(ctx context.Context, args map[string]any) (any, error)) {
	if b == nil || b.reg == nil {
		return
	}
	_ = b.reg.Register(&syscallTool{name: name, fn: toolFunc})
}

// syscallTool adapts a syscall handler to the api/tools.Tool interface so the
// SDK registry can carry it. Its schema mirrors the LLM-facing schema from
// agentsyscall.ToolSchemas (matched by name); when no schema exists the tool
// still executes (defensive: the registry only needs Name/Execute).
type syscallTool struct {
	name string
	fn   func(ctx context.Context, args map[string]any) (any, error)
}

var _ tools.Tool = (*syscallTool)(nil)

func (t *syscallTool) Name() string { return t.name }

func (t *syscallTool) Description() string {
	for _, s := range agentsyscall.ToolSchemas() {
		if s.Name == t.name {
			return s.Description
		}
	}
	return t.name
}

func (t *syscallTool) Parameters() map[string]interface{} {
	for _, s := range agentsyscall.ToolSchemas() {
		if s.Name == t.name {
			return s.Parameters
		}
	}
	return map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
}

func (t *syscallTool) Execute(ctx context.Context, params map[string]interface{}) (tools.Result, error) {
	out, err := t.fn(ctx, params)
	if err != nil {
		return tools.Result{Success: false, Data: map[string]any{"error": err.Error()}}, err
	}
	return tools.Result{Success: true, Data: out}, nil
}

func (t *syscallTool) Capabilities() []string { return nil }

// sdkSyscallExecutor adapts a CapabilityExecutor (the sdkAgentExecutor shape)
// to the agentsyscall.Executor contract so a spawned agent is a real
// executable body — the same quantum, different outcome envelope (mirrors
// peerExecutorAdapter in peer mode; code_rules: no second executor
// copy).
type sdkSyscallExecutor struct {
	inner kernel.CapabilityExecutor
}

var _ agentsyscall.Executor = (*sdkSyscallExecutor)(nil)

// ID returns the wrapped executor's agent ID.
func (e *sdkSyscallExecutor) ID() string { return e.inner.ID() }

// Type returns the wrapped executor's agent type.
func (e *sdkSyscallExecutor) Type() models.AgentType { return e.inner.Type() }

func (e *sdkSyscallExecutor) ExecuteStep(ctx context.Context, task *models.Task) (*agentsyscall.StepOutcome, error) {
	out, err := e.inner.ExecuteStep(ctx, task)
	if err != nil {
		return nil, err
	}
	return &agentsyscall.StepOutcome{
		Done:       out.Done,
		Result:     out.Result,
		Checkpoint: out.Checkpoint,
	}, nil
}

// wireSyscalls builds the syscall Kernel over the runtime's fabrics and binds
// spawn_agent / create_task into the tool registry. Called once from
// ensureScheduler (schedOnce) so the same fabric + scheduler that drive
// Submit also back the syscalls. Safe to call multiple times (idempotent:
// tool registry Register overwrites by name; fabric creation is cheap).
func (r *Runtime) wireSyscalls() {
	if r.sdkFabric == nil || r.sched == nil {
		return
	}
	if r.agentsFabric == nil {
		r.agentsFabric = agentfabric.NewFabric()
	}
	// Parity with the serve path (T4): bind the loop lifetime so the
	// create_plan `loop` option works on the SDK path too. The runtime ctx is
	// cancelled in Close, so SDK plan loops cannot outlive the Runtime — the
	// same lifecycle the serve path gets from its own ctx. Without this, the
	// schema advertises a loop parameter that always failed loudly. A nil ctx
	// (never expected: New always wires rtCtx) keeps the fail-loud behavior.
	var opts []agentsyscall.KernelOption
	if r.ctx != nil {
		opts = append(opts, agentsyscall.WithLoopLifetime(r.ctx))
	}
	kernelSyscall := agentsyscall.NewKernel(
		r.agentsFabric,
		r.sdkFabric,
		// factory: a spawned agent executes with the same ReAct engine as a
		// registered sdk agent — one executor instance, reused for both the
		// fabric cognition and the scheduler registration. The executor's
		// scheduler-facing Type is the DECLARED capability (not the generated
		// agent id) so create_task sub-tasks can be matched to the peer.
		func(agentID, capability string) agentsyscall.Executor {
			exec := &sdkAgentExecutor{agent: r.NewAgent(agentID, WithTools()), typ: models.AgentType(capability)}
			return &sdkSyscallExecutor{inner: exec}
		},
		func(agentID string, executor agentsyscall.Executor) {
			if se, ok := executor.(*sdkSyscallExecutor); ok {
				r.sched.RegisterExecutor(agentID, se.inner)
			}
		},
		opts...,
	)
	agentsyscall.BindTools(&syscallBinder{reg: r.toolReg}, kernelSyscall)
	r.syscallTools = syscallLLMTools()
	r.syscallKernel = kernelSyscall
}

// LivePlanLoops returns the plan IDs of the loops currently running on this
// Runtime, sorted. Empty before the first Submit (the syscall kernel is wired
// lazily by ensureScheduler) and after every loop has finished.
//
// T4 parity: the serve path exposes loop observability through its kernel, so
// the SDK must too — otherwise a `loop` plan started via create_plan would be
// unobservable and unstoppable from the embedding program.
func (r *Runtime) LivePlanLoops() []string {
	if r.syscallKernel == nil {
		return nil
	}
	return r.syscallKernel.LivePlanLoops()
}

// StopPlanLoop cancels a live plan loop by plan ID and waits for its driver to
// exit. Unknown or already-finished plans report agentsyscall.ErrPlanLoopNotFound;
// so does a Runtime whose syscall kernel was never wired (no Submit yet).
func (r *Runtime) StopPlanLoop(planID string) error {
	if r.syscallKernel == nil {
		return fmt.Errorf("sdk: stop plan loop %q: %w", planID, agentsyscall.ErrPlanLoopNotFound)
	}
	return r.syscallKernel.StopPlanLoop(planID)
}

// syscallLLMTools converts the syscall schemas to the LLM-facing api/core.Tool
// list so resolveTools can append them to every agent's tool set (the agent
// sees spawn_agent / create_task regardless of its own WithTools list).
func syscallLLMTools() []core.Tool {
	schemas := agentsyscall.ToolSchemas()
	if len(schemas) == 0 {
		return nil
	}
	out := make([]core.Tool, 0, len(schemas))
	for _, s := range schemas {
		out = append(out, core.Tool{
			Type: "function",
			Function: core.FunctionDefinition{
				Name:        s.Name,
				Description: s.Description,
				Parameters:  s.Parameters,
			},
		})
	}
	return out
}

// helper: the factory needs an AgentOption; an empty tool set keeps the
// spawned agent minimal (its LLM tool list still carries the syscall tools
// via resolveTools).
