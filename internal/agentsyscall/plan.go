package agentsyscall

import (
	"context"
	"errors"
	"fmt"
	"sort"

	kctx "github.com/Timwood0x10/ares/internal/kernel/ctx"
	"github.com/Timwood0x10/ares/internal/taskfabric"
)

// CreatePlanTool is the tool name exposed to the LLM for submitting a
// multi-step plan (one batch, dependency-ordered) to the Task Fabric.
const CreatePlanTool = "create_plan"

// defaultMaxPlanLoops caps concurrently live plan loops per Kernel. Each loop
// owns one driver goroutine for the whole serve lifetime, and create_plan is
// LLM-callable, so the count must be bounded by default rather than by
// configuration discipline.
const defaultMaxPlanLoops = 16

// ErrPlanLoopNotFound is returned by StopPlanLoop when no live loop matches
// the plan ID (unknown plan, or a loop that already ended).
var ErrPlanLoopNotFound = errors.New("agentsyscall: plan loop not found")

// PlanStepArgs is the LLM-facing description of one plan step. It mirrors
// taskfabric.PlanStep minus the Kernel-stamped Origin.
type PlanStepArgs struct {
	// ID is the unique step id within the plan.
	ID string `json:"id"`
	// Capability is the required executor capability.
	Capability string `json:"capability"`
	// DependsOn lists prerequisite step IDs within the same plan.
	DependsOn []string `json:"depends_on,omitempty"`
	// Priority drives preemption (higher wins); 0 = normal.
	Priority int `json:"priority,omitempty"`
	// MaxRetries is the TOTAL attempt budget (0 = kernel default).
	MaxRetries int `json:"max_retries,omitempty"`
	// Payload carries opaque step data (task_desc, parameters).
	Payload map[string]any `json:"payload,omitempty"`
}

// CreatePlanArgs is the create_plan tool argument envelope.
type CreatePlanArgs struct {
	// Steps is the dependency-ordered batch to compile. Must be non-empty.
	Steps []PlanStepArgs `json:"steps"`
	// Loop, when set, turns the plan into a bounded DAG-level round loop:
	// after every round reaches terminal state the loop re-compiles the
	// same DAG under round-namespaced task IDs and lets the scheduler run
	// it again, until MaxRounds is exhausted or the exit condition fires.
	// Nil (the default) compiles a single batch, as before.
	Loop *PlanLoopArgs `json:"loop,omitempty"`
	// NOTE: no creator argument — the Kernel stamps every task's Origin from
	// the tool context (kctx.CallerID), identical to CreateTask.
}

// PlanLoopArgs is the LLM-facing round-loop spec for create_plan. Until is
// deliberately a small declarative enum, not an expression language: the
// kernel must be able to evaluate it deterministically without executing
// model-supplied logic.
type PlanLoopArgs struct {
	// MaxRounds is the hard cap on executed rounds (>= 1). Required.
	MaxRounds int `json:"max_rounds"`
	// Until selects the early-exit condition: "all_succeeded" stops the
	// loop before the next round once a round finishes with zero failed
	// tasks; "" runs exactly MaxRounds rounds.
	Until string `json:"until,omitempty"`
}

// untilAllSucceeded is the only declarative exit condition supported for
// LLM-submitted loops (see PlanLoopArgs.Until).
const untilAllSucceeded = "all_succeeded"

// CreatePlanResult reports the batch outcome.
type CreatePlanResult struct {
	// TaskIDs are the created task IDs, in input order. All are READY.
	// With a loop spec these are round-1 IDs ("<plan>#r1#<step>"); later
	// rounds follow the same naming and appear in the fabric as they are
	// compiled.
	TaskIDs []string `json:"task_ids"`
	// Count is len(TaskIDs).
	Count int `json:"count"`
	// State is the shared lifecycle state of the batch ("ready").
	State string `json:"state"`
	// PlanID is set only for looped plans: the plan identity all rounds
	// are namespaced under.
	PlanID string `json:"plan_id,omitempty"`
	// LoopMaxRounds echoes the loop cap, only set for looped plans.
	LoopMaxRounds int `json:"loop_max_rounds,omitempty"`
}

// CreatePlan is the create_plan syscall: it validates and compiles an
// LLM-produced multi-step plan (with dependencies) into one all-or-nothing
// batch of READY tasks. Compared with N× create_task it lets the cognitive
// layer draw the whole DAG at once; Origin is stamped from the tool context
// for every step in the batch (Kernel-enforced provenance).
//
// Args:
//   - ctx: the tool-call context; its caller id becomes every task's Origin.
//   - args: the parsed create_plan arguments.
//
// Returns:
//
//	*CreatePlanResult - the created batch, or nil on failure.
//	error - validation / compilation errors (nothing is created on error).
func (k *Kernel) CreatePlan(ctx context.Context, args CreatePlanArgs) (*CreatePlanResult, error) {
	if k.fabric == nil {
		return nil, errors.New("agentsyscall: task fabric not wired")
	}
	if len(args.Steps) == 0 {
		return nil, errors.New("agentsyscall: plan requires at least one step")
	}
	origin := kctx.CallerID(ctx)
	steps := make([]taskfabric.PlanStep, 0, len(args.Steps))
	for _, s := range args.Steps {
		if s.Capability == "" {
			return nil, fmt.Errorf("agentsyscall: plan step %q: capability is required", s.ID)
		}
		steps = append(steps, taskfabric.PlanStep{
			ID:         s.ID,
			Capability: s.Capability,
			DependsOn:  s.DependsOn,
			Priority:   s.Priority,
			MaxRetries: s.MaxRetries,
			Payload:    s.Payload,
			Origin:     origin,
		})
	}
	// A looped plan is compiled by the PlanLoop itself (round 1 compiles
	// synchronously inside Start, atomically); a plain plan is compiled here.
	if args.Loop != nil {
		return k.startPlanLoop(ctx, origin, steps, args.Loop)
	}
	ids, err := k.fabric.CompilePlan(ctx, steps)
	if err != nil {
		return nil, fmt.Errorf("agentsyscall: compile plan: %w", err)
	}
	log.Info("agentsyscall: created plan batch of tasks → READY", "task_count", len(ids), "origin", origin)
	return &CreatePlanResult{
		TaskIDs: ids,
		Count:   len(ids),
		State:   string(taskfabric.StateReady),
	}, nil
}

// startPlanLoop wires the loop option: it validates the declarative spec,
// builds a taskfabric.PlanLoop over the fabric and starts it on the kernel's
// injected lifetime context. Round 1 is compiled synchronously inside Start —
// a rejected plan creates nothing, identical to the plain path. The started
// loop is registered on the kernel so its terminal error has a reader and the
// concurrency cap is enforceable.
//
// Args:
//   - ctx: the tool-call context. Only its cancellation is consulted (the
//     loop itself outlives the call and therefore runs on k.loopCtx).
//   - origin: kernelctx-stamped provenance for every round's tasks.
//   - steps: the round-invariant base DAG.
//   - spec: the LLM-supplied loop parameters.
func (k *Kernel) startPlanLoop(
	ctx context.Context,
	origin string,
	steps []taskfabric.PlanStep,
	spec *PlanLoopArgs,
) (*CreatePlanResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("agentsyscall: plan loop: %w", err)
	}
	if k.loopCtx == nil {
		return nil, errors.New("agentsyscall: plan loop requires a kernel loop lifetime (WithLoopLifetime)")
	}
	if spec.MaxRounds < 1 {
		return nil, fmt.Errorf("agentsyscall: plan loop: max_rounds must be >= 1, got %d", spec.MaxRounds)
	}
	if spec.Until != "" && spec.Until != untilAllSucceeded {
		return nil, fmt.Errorf("agentsyscall: plan loop: unknown until condition %q (supported: %q)",
			spec.Until, untilAllSucceeded)
	}
	loopSpec := taskfabric.PlanLoopSpec{
		PlanID:    fmt.Sprintf("plan-%s-%d", origin, k.idSeq.Add(1)),
		Steps:     steps,
		MaxRounds: spec.MaxRounds,
	}
	if spec.Until == untilAllSucceeded {
		loopSpec.UntilCondition = func(o taskfabric.RoundOutcome) bool { return o.Succeeded() }
	}
	loop, err := taskfabric.NewPlanLoop(k.fabric, loopSpec)
	if err != nil {
		return nil, fmt.Errorf("agentsyscall: plan loop: %w", err)
	}
	// Reserve the slot before Start: the cap must hold even when several
	// create_plan calls race, and a failed Start releases it again.
	if err := k.reservePlanLoop(loopSpec.PlanID, loop); err != nil {
		return nil, err
	}
	if err := loop.Start(k.loopCtx); err != nil {
		k.releasePlanLoop(loopSpec.PlanID)
		return nil, fmt.Errorf("agentsyscall: plan loop: %w", err)
	}
	k.watchPlanLoop(loopSpec.PlanID, loop)
	log.Info("agentsyscall: started plan loop", "plan_id", loopSpec.PlanID, "max_rounds", spec.MaxRounds, "until", spec.Until, "origin", origin)
	return &CreatePlanResult{
		// Round-1 task IDs, named by the loop's round namespace.
		TaskIDs:       roundOneTaskIDs(loopSpec),
		Count:         len(steps),
		State:         string(taskfabric.StateReady),
		PlanID:        loopSpec.PlanID,
		LoopMaxRounds: spec.MaxRounds,
	}, nil
}

// reservePlanLoop registers a loop under the cap. It fails when the cap is
// already reached, so an LLM cannot turn repeated create_plan calls into
// unbounded background work.
func (k *Kernel) reservePlanLoop(planID string, loop *taskfabric.PlanLoop) error {
	k.loopMu.Lock()
	defer k.loopMu.Unlock()
	if k.planLoops == nil {
		k.planLoops = make(map[string]*taskfabric.PlanLoop)
	}
	limit := k.maxPlanLoops
	if limit <= 0 {
		limit = defaultMaxPlanLoops
	}
	if len(k.planLoops) >= limit {
		return fmt.Errorf("agentsyscall: plan loop: %d loops already live (cap %d)", len(k.planLoops), limit)
	}
	k.planLoops[planID] = loop
	return nil
}

// releasePlanLoop drops a loop from the registry, freeing its slot.
func (k *Kernel) releasePlanLoop(planID string) {
	k.loopMu.Lock()
	defer k.loopMu.Unlock()
	delete(k.planLoops, planID)
}

// watchPlanLoop is the loop's error sink: a managed goroutine bounded by the
// loop's own Done channel that deregisters the finished loop and surfaces a
// fatal loop error. Without it a panicking UntilCondition/Replan would be
// recorded inside the loop and never read by anyone.
func (k *Kernel) watchPlanLoop(planID string, loop *taskfabric.PlanLoop) {
	go func() {
		<-loop.Done()
		k.releasePlanLoop(planID)
		if err := loop.Err(); err != nil {
			log.Warn("agentsyscall: plan loop ended with error", "plan_id", planID, "round", loop.Round(), "error", err)
			return
		}
		log.Info("agentsyscall: plan loop finished", "plan_id", planID, "round", loop.Round())
	}()
}

// LivePlanLoops returns the plan IDs of currently live plan loops, sorted for
// stable output. It is the observability entry point for the loop registry
// (dashboards, tests) and the reason the kernel keeps loop handles at all.
func (k *Kernel) LivePlanLoops() []string {
	k.loopMu.Lock()
	defer k.loopMu.Unlock()
	ids := make([]string, 0, len(k.planLoops))
	for id := range k.planLoops {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// StopPlanLoop cancels a live plan loop by plan ID and waits for its driver to
// exit. Already-finished or unknown plans report ErrPlanLoopNotFound. Tasks of
// an in-flight round are left to the scheduler: the loop owns round
// compilation, never leases.
func (k *Kernel) StopPlanLoop(planID string) error {
	k.loopMu.Lock()
	loop, ok := k.planLoops[planID]
	k.loopMu.Unlock()
	if !ok {
		return fmt.Errorf("agentsyscall: stop plan loop %q: %w", planID, ErrPlanLoopNotFound)
	}
	loop.Stop()
	k.releasePlanLoop(planID)
	return nil
}

// roundOneTaskIDs reproduces the loop's round-1 ID naming so the caller gets
// the actual created task IDs without reaching into taskfabric internals.
func roundOneTaskIDs(spec taskfabric.PlanLoopSpec) []string {
	ids := make([]string, 0, len(spec.Steps))
	for _, s := range spec.Steps {
		ids = append(ids, taskfabric.PlanTaskID(spec.PlanID, 1, s.ID))
	}
	return ids
}

// CreatePlanToolSchema returns the LLM-facing schema for create_plan.
func CreatePlanToolSchema() ToolSchema {
	return ToolSchema{
		Name: CreatePlanTool,
		Description: "Submit a multi-step plan to the Task Fabric in one batch. " +
			"Steps may declare dependencies on other steps in the same plan; the batch is atomic " +
			"(any invalid step rejects the whole plan). Use this instead of repeated create_task calls " +
			"when you can draw the whole dependency DAG up front. " +
			"Optionally pass loop to re-run the whole DAG for up to max_rounds rounds " +
			"(task IDs are suffixed with the round number) until the until condition holds.",
		Parameters: map[string]any{
			paramType: paramTypeObject,
			paramProperties: map[string]any{
				"loop": map[string]any{
					paramType: paramTypeObject,
					paramDescription: "Optional bounded round loop: after every round fully finishes " +
						"(every task COMPLETED or FAILED), the same DAG is re-compiled for the next round " +
						"until max_rounds is reached or the until condition fires.",
					paramProperties: map[string]any{
						"max_rounds": map[string]any{
							paramType:        paramTypeInteger,
							paramDescription: "Hard cap on executed rounds (>= 1). Required when loop is set.",
						},
						"until": map[string]any{
							paramType: paramTypeString,
							paramDescription: "Early exit: 'all_succeeded' stops before the next round " +
								"once a round has zero failed tasks. Empty runs all max_rounds rounds.",
						},
					},
					paramRequired: []string{"max_rounds"},
				},
				"steps": map[string]any{
					paramType: paramTypeArray,
					paramDescription: "The plan steps. Each step needs a unique id, a capability, and optional depends_on ids " +
						"referencing other steps in this plan.",
					paramItems: map[string]any{
						paramType: paramTypeObject,
						paramProperties: map[string]any{
							"id": map[string]any{
								paramType:        paramTypeString,
								paramDescription: "Unique step id within this plan.",
							},
							paramCapability: map[string]any{
								paramType:        paramTypeString,
								paramDescription: "The required capability for this step (e.g. 'coder', 'reviewer').",
							},
							"depends_on": map[string]any{
								paramType:        paramTypeArray,
								paramItems:       map[string]any{paramType: paramTypeString},
								paramDescription: "Step IDs in this plan that must complete before this step runs.",
							},
							"priority": map[string]any{
								paramType:        paramTypeInteger,
								paramDescription: "Scheduling priority (higher wins). 0 = normal.",
							},
							"max_retries": map[string]any{
								paramType:        paramTypeInteger,
								paramDescription: "Total attempt budget. 0 = kernel default (first attempt + one retry).",
							},
							paramPayload: map[string]any{
								paramType:        paramTypeObject,
								paramDescription: "Opaque step data (e.g. task_desc, parameters).",
							},
						},
						paramRequired: []string{"id", paramCapability},
					},
				},
			},
			paramRequired: []string{"steps"},
		},
	}
}
