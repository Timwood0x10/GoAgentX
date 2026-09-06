package ares_bootstrap

import (
	"context"
	"errors"
	"fmt"
	"time"

	evolution "github.com/Timwood0x10/ares/internal/ares_evolution"
	"github.com/Timwood0x10/ares/internal/evolution/deployment"
	"github.com/Timwood0x10/ares/internal/evolution/patch"
)

// defaultEvalEvidenceWindow bounds the staging comparison to recent evidence
// (E1). Both shadow and baseline sides share the same [since, until] so the
// delta cannot be distorted by evidence written between two independent
// queries. One hour covers the judgment window while excluding stale history
// from previous strategies.
const defaultEvalEvidenceWindow = time.Hour

// deploymentStagingRuntime is a shadow runtime used by the DeploymentPipeline.
// It NEVER mutates live state — Apply is a read-only preflight (the patch must
// have a registered executor) and Evaluate returns per-strategy fitness scores
// from the shared EvidenceStore: the shadow score for the staged patch's
// strategy and the baseline score for the currently-active strategy.
//
// Per-patch scoping: Apply records the patch's StrategyID as the candidate
// strategy, and Evaluate queries the aggregator for both the candidate
// (shadow) and the active (baseline) strategy in the same call against the
// same store snapshot. Previously Evaluate used an empty strategy filter
// (global mean), so every patch got the same score regardless of content.
//
// Attribution is required, not inferred: an empty StrategyID is NOT
// backfilled from Source or Target — those are proposer classes and component
// names, disjoint from the evidence store's strategy_id namespace. An
// unattributable patch is reported as unmeasurable (delta 0 → rejected by any
// positive PromotionThreshold) rather than scored against a key that can
// never match.
//
// Cold start: when no evidence exists for a strategy (Window count == 0),
// Evaluate returns coldStartScore for that strategy. There is NO implicit
// default — bootstrap sets it explicitly (0.5) at construction.
//
// Concurrency: currentPatchStrategy is mutable state across calls. Deploy
// holds dp.mu for the entire Deploy lifecycle, so there is no concurrent
// Evaluate during a single Deploy. But MonitorAndRollback also holds dp.mu,
// so the staging runtime's mutable state is guarded by the pipeline's mutex.
// The baseline is resolved live via asm.Current() at Evaluate time (the ASM
// reference is immutable after construction), so it is not mutable runtime
// state. go test -race covers this.
type deploymentStagingRuntime struct {
	reg *patch.Registry
	// agg is the shared fitness scoring backend. Nil means "no evidence
	// backend wired" → Evaluate always returns coldStartScore.
	agg *evolution.RuntimeFitnessAggregator
	// applyCount tracks staging applies for bookkeeping (no longer the
	// primary state — currentPatchStrategy is).
	applyCount     int
	coldStartScore float64
	// currentPatchStrategy is the strategy ID of the currently staged patch,
	// taken verbatim from RuntimePatch.StrategyID. Set by Apply, cleared by
	// Rollback. Empty means "this patch carries no strategy attribution".
	currentPatchStrategy string
	// asm resolves the currently-active (baseline) strategy at Evaluate time so
	// the baseline always reflects the live strategy manager. It must NOT be
	// frozen at construction: the active strategy can change between Deploy
	// calls or while a patch is being measured, and comparing against a stale
	// baseline yields a meaningless delta. When asm is nil or Current() returns
	// nil, no comparable baseline exists and Evaluate reports both sides as
	// cold-start rather than mixing a global mean with a scoped one.
	asm *evolution.ActiveStrategyManager
}

func (r *deploymentStagingRuntime) Apply(_ context.Context, p patch.RuntimePatch) (*patch.RuntimePatch, error) {
	// Preflight only: reject patches no executor can handle (same rejection
	// class as before), but do NOT touch any registry state.
	if !r.reg.CanApply(p.Target) {
		return nil, fmt.Errorf("staging preflight: no executor registered for target %q", p.Target)
	}
	r.applyCount++
	// Record the patch's strategy identifier for per-patch Evaluate scoping.
	// ONLY StrategyID is accepted. Source is the proposer class
	// ("diff.memory", "candidate", "ga") and Target is a component name —
	// both live in namespaces disjoint from the evidence store's
	// strategy_id, so using either as a fallback key produces a lookup that
	// always misses and silently degrades to the cold-start score while
	// looking like a real measurement. An empty StrategyID stays empty.
	r.currentPatchStrategy = p.StrategyID
	return &p, nil
}

// Evaluate returns (shadowScore, baselineScore, err). shadowScore is the
// fitness of the currently-staged patch's strategy (currentPatchStrategy);
// baselineScore is the fitness of the currently-active strategy, resolved
// live at call time via asm.Current() so a mid-run strategy promotion is
// reflected rather than a frozen construction-time ID. Both are sampled from
// the same call — the aggregator's Window queries share the same evidence
// store snapshot, ensuring the delta is not distorted by concurrent evidence
// writes between two independent calls.
//
// Comparability precondition: a delta is only meaningful when BOTH sides are
// strategy-scoped. If either ID is empty, one side would be a global mean and
// the other a per-strategy mean — an asymmetric pair whose difference is
// noise. In that case Evaluate returns coldStartScore for both, yielding
// delta == 0, which a positive PromotionThreshold rejects. That is the honest
// outcome: "not measurable" must not read as "improved".
//
// The aggregator's MinSamplesBeforeJudge is deliberately NOT enforced here:
// partial evidence (any count > 0) still yields the weighted mean — only a
// completely empty store falls back to coldStartScore.
func (r *deploymentStagingRuntime) Evaluate(ctx context.Context) (shadow float64, baseline float64, err error) {
	if r.agg == nil {
		return r.coldStartScore, r.coldStartScore, nil
	}
	// Resolve the live baseline strategy ID at call time, not at
	// construction: the active strategy can change mid-run, and a
	// frozen baseline would measure the wrong side of the delta.
	activeStrategyID := ""
	if r.asm != nil {
		if cur := r.asm.Current(); cur != nil {
			activeStrategyID = cur.ID
		}
	}
	if r.currentPatchStrategy == "" || activeStrategyID == "" {
		return r.coldStartScore, r.coldStartScore, nil
	}
	// E1: take one time anchor and sample both sides with the SAME
	// [since, until] so concurrent evidence writes between two queries cannot
	// skew the delta. Both bounds are non-zero by construction.
	until := time.Now()
	since := until.Add(-defaultEvalEvidenceWindow)
	// Shadow: scoped to the staged patch's strategy.
	shadowRes := r.agg.WindowAt(ctx, r.currentPatchStrategy, since, until)
	if shadowRes.Count == 0 {
		shadow = r.coldStartScore
	} else {
		shadow = shadowRes.Mean
	}
	// Baseline: scoped to the active strategy, same time anchor.
	baselineRes := r.agg.WindowAt(ctx, activeStrategyID, since, until)
	if baselineRes.Count == 0 {
		baseline = r.coldStartScore
	} else {
		baseline = baselineRes.Mean
	}
	return shadow, baseline, nil
}

func (r *deploymentStagingRuntime) Rollback(_ context.Context, _ *patch.RuntimePatch) error {
	// Nothing was applied to any registry during staging, so there is nothing
	// to roll back. Clear the per-patch strategy to prevent cross-patch
	// contamination on the next Apply.
	r.currentPatchStrategy = ""
	if r.applyCount > 0 {
		r.applyCount--
	}
	return nil
}

// deploymentLiveRuntime promotes a patch to the real executor registry, which
// applies it to the actual components: memory patches are written to the live
// comp.Memory; workflow/scheduler/recovery/knowledge patches are written to
// their (currently synthetic) executors. This is the genuine "deploy to
// production" step — it is exactly what the Coordinator did before, now routed
// through the deployment pipeline.
//
// Rollback support: Apply snapshots the target executor before applying the
// patch. Rollback restores that snapshot via patch.Registry.Restore. When the
// target executor does not support Snapshot (returns ErrNoSnapshot), the
// live runtime falls back to saving the old executor reference and Replace-ing
// it back during Rollback.
type deploymentLiveRuntime struct {
	reg *patch.Registry
}

func (r *deploymentLiveRuntime) Apply(ctx context.Context, p patch.RuntimePatch) (*patch.RuntimePatch, error) {
	// Snapshot the target's pre-apply state so Rollback can restore it.
	// The snapshot is stashed in the returned rollback patch's Value field
	// (the patch's Rollback field is for the executor's own inverse; the
	// registry-level snapshot is a separate concern).
	snap, snapErr := r.reg.Snapshot(ctx, p.Target)
	if snapErr != nil && !errors.Is(snapErr, patch.ErrNoSnapshot) {
		// Snapshot failed for a reason other than "not supported" — abort.
		return nil, fmt.Errorf("live apply: snapshot target %q: %w", p.Target, snapErr)
	}

	if err := r.reg.Apply(ctx, p); err != nil {
		return nil, err
	}

	rb := &patch.RuntimePatch{
		Type:   p.Type,
		Target: p.Target,
		Source: p.Source,
	}
	if snap != nil {
		rb.Value = snap
	}
	return rb, nil
}

// Rollback reverts a previously applied live patch by restoring the
// pre-apply snapshot captured during Apply. When the snapshot is nil (the
// target executor returned ErrNoSnapshot), Rollback uses the registry's
// Restore fallback which Replace-s the old executor reference back.
func (r *deploymentLiveRuntime) Rollback(ctx context.Context, rollback *patch.RuntimePatch) error {
	if rollback == nil {
		return errors.New("live rollback: nil rollback patch")
	}
	var snap any
	if rb, ok := rollback.Value.(*patch.ExecutorSnapshot); ok && rb != nil {
		snap = rb
	}
	return r.reg.Restore(ctx, rollback.Target, snap)
}

// deploymentAdapter bridges the deployment.DeploymentPipeline to the
// Coordinator's PatchDeployer interface. Only catastrophic failures surface as
// errors; a normal reject/rollback is reported by the pipeline and treated as
// handled here.
type deploymentAdapter struct {
	dp *deployment.DeploymentPipeline
}

func (a *deploymentAdapter) Enabled() bool {
	return a.dp != nil && a.dp.IsEnabled()
}

func (a *deploymentAdapter) Deploy(ctx context.Context, p patch.RuntimePatch) error {
	// Attribution guard: the pipeline judges a patch by comparing its own
	// strategy's fitness window against the active strategy's. A patch with
	// no StrategyID cannot be placed on either side of that comparison, so
	// there is no evidence on which to promote it. Reject it explicitly here
	// instead of letting it reach Evaluate, where both sides would collapse
	// to the cold-start score and the rejection reason would read like a
	// measured tie ("delta 0.000") rather than an unmeasurable patch.
	//
	// Today no patch producer sets StrategyID: genome/diff patches are keyed
	// by component, while evidence strategy_id comes from the mutation
	// strategy lifecycle. Those namespaces are disjoint, so enabling the
	// deployment pipeline rejects every patch until attribution is wired.
	// That is deliberate — an unjudgeable patch must not be promoted.
	if p.StrategyID == "" {
		return fmt.Errorf(
			"deployment: patch (type %s, target %q, source %q) has no strategy attribution; "+
				"cannot compare against the active strategy, refusing to promote",
			p.Type, p.Target, p.Source)
	}
	rec, err := a.dp.Deploy(ctx, p)
	if err != nil {
		return err
	}
	// A pipeline REJECTION (shadow score below threshold) or ROLLBACK is a
	// normal, non-error return of Deploy — but the Coordinator treats a nil
	// error as "applied successfully" and records PatchResult{Error: nil} in
	// its decision history. Translate the outcome so the operator-facing
	// history reflects reality: only DeploymentPromoted counts as success.
	if rec != nil && rec.Status != deployment.DeploymentPromoted {
		return fmt.Errorf("deployment not applied (status %s): %s", rec.Status, rec.Reason)
	}
	// E2: post-promotion regression watch. MonitorAndRollback waits the
	// configured evaluation window, re-samples live fitness, and rolls the
	// live executor back on regression. A rollback is a normal outcome here,
	// surfaced as an error so the Coordinator does not record success.
	monitored, err := a.dp.MonitorAndRollback(ctx, rec)
	if err != nil {
		return fmt.Errorf("deployment post-promotion monitor: %w", err)
	}
	if monitored != nil && monitored.Status != deployment.DeploymentPromoted {
		return fmt.Errorf("deployment rolled back post-promotion (status %s): %s", monitored.Status, monitored.Reason)
	}
	return nil
}
