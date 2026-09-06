// fitness_aggregator.go provides the JUDGE stage of the evolution control
// plane. RuntimeFitnessAggregator merges evidence from multiple sources
// (workflow, scheduler, recovery, strategy) into a single normalized [0,1]
// fitness value. It is the shared scoring backend for:
//
//   - StrategyLifecycle: decides whether to RecordScore into RollbackPolicy
//     (B1 fix) and whether a candidate is "good enough" to promote.
//   - Deployment staging: replaces the single-source "workflow" check with
//     a multi-dimensional aggregate (B6 fix).
//
// The aggregator is read-only: it never mutates evidence or strategy state.
// Cold-start (insufficient samples) returns ok=false so the caller can
// choose a conservative strategy (e.g. hold in SHADOW, or use
// ColdStartScore as a fallback).
package evolution

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/Timwood0x10/ares/internal/evidence"
)

// FitnessWeights configures how each evidence source contributes to the
// aggregate fitness. Weights should sum to 1.0; if they don't, the
// aggregator normalizes them at query time.
type FitnessWeights struct {
	// Outcome is the weight for task outcome (success/failure) fitness.
	Outcome float64 `json:"outcome"`
	// DimensionEval is the weight for dimension_eval evidence.
	DimensionEval float64 `json:"dimension_eval"`
	// Workflow is the weight for workflow-sourced fitness evidence.
	Workflow float64 `json:"workflow"`
	// Scheduler is the weight for scheduler-sourced fitness evidence.
	Scheduler float64 `json:"scheduler"`
	// Recovery is the weight for recovery-sourced fitness evidence.
	Recovery float64 `json:"recovery"`
	// Collaboration weights cross-agent collaboration receipts (Step Y.2:
	// "should I have asked THAT agent?"). Default 0 — the channel is opt-in,
	// so an operator who has not enabled collab feedback sees an unchanged
	// aggregate even if stray evidence exists.
	Collaboration float64 `json:"collaboration"`
	// ToolCall weights tool-invocation outcomes (Step Y.3: "was calling THAT
	// tool worth it?"). Default 0, same opt-in reasoning as Collaboration.
	ToolCall float64 `json:"tool_call"`
}

// DefaultFitnessWeights returns sensible default weights summing to 1.0.
// Collaboration and ToolCall are deliberately 0: those channels (Step Y.2/Y.3)
// are opt-in, and giving them a non-zero default would silently redistribute
// every existing deployment's fitness mix on upgrade.
func DefaultFitnessWeights() FitnessWeights {
	return FitnessWeights{
		Outcome:       0.40,
		DimensionEval: 0.25,
		Workflow:      0.15,
		Scheduler:     0.15,
		Recovery:      0.05,
	}
}

// AggregatorConfig groups all RuntimeFitnessAggregator settings.
type AggregatorConfig struct {
	// WindowSize is the maximum number of evidence records to consider per
	// source (mirrors recentFitnessSummary's limit).
	WindowSize int `json:"window_size"`
	// MinSamplesBeforeJudge is the minimum total evidence count before the
	// aggregator returns ok=true. Below this, it returns ok=false so callers
	// can apply a conservative cold-start policy (B6 fix).
	MinSamplesBeforeJudge int `json:"min_samples_before_judge"`
	// ColdStartScore is the score returned when no evidence exists. Callers
	// use this when they need a fallback instead of ok=false.
	ColdStartScore float64 `json:"cold_start_score"`
	// Weights controls per-source contribution.
	Weights FitnessWeights `json:"weights"`
}

// TODO(tech-debt): the design doc (ga-runtime-evolution-design-zh.md §4 ②)
// specifies a cost/latency penalty term subtracted from the aggregate
// fitness (penalty(cost, latency)). It is not implemented because task
// events carry no cost or latency data today — see the observer.go
// tech-debt note. Wire it once flight-trace cost/latency reaches the
// EventStore payloads; do not reintroduce a config struct before a real
// data source exists (no dead config fields).

// DefaultAggregatorConfig returns sensible defaults matching the design doc.
func DefaultAggregatorConfig() AggregatorConfig {
	return AggregatorConfig{
		WindowSize:            50,
		MinSamplesBeforeJudge: 10,
		ColdStartScore:        0.5,
		Weights:               DefaultFitnessWeights(),
	}
}

// RuntimeFitnessAggregator computes normalized [0,1] fitness from the shared
// evidence store. It is read-only and safe for concurrent use.
type RuntimeFitnessAggregator struct {
	store evidence.Store
	cfg   AggregatorConfig
	mu    sync.RWMutex
}

// NewRuntimeFitnessAggregator creates an aggregator backed by the given
// evidence store.
func NewRuntimeFitnessAggregator(store evidence.Store, cfg AggregatorConfig) *RuntimeFitnessAggregator {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 50
	}
	if cfg.MinSamplesBeforeJudge <= 0 {
		cfg.MinSamplesBeforeJudge = 10
	}
	if cfg.ColdStartScore <= 0 {
		cfg.ColdStartScore = 0.5
	}
	return &RuntimeFitnessAggregator{store: store, cfg: cfg}
}

// SetStore replaces the evidence store. Used by bootstrap to inject the
// shared evidence store after the aggregator is created with nil (the
// store is not known at NewWiredEvolutionSystem time).
func (a *RuntimeFitnessAggregator) SetStore(store evidence.Store) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.store = store
	a.mu.Unlock()
}

// WindowResult holds the aggregate fitness for a given strategy ID.
type WindowResult struct {
	// Mean is the weighted aggregate fitness in [0,1].
	Mean float64
	// Count is the total number of evidence records used.
	Count int
	// PerSource holds the per-source mean and count.
	PerSource map[string]sourceStat
	// LastAt is the NEWEST evidence timestamp inside the window. Under
	// steady-state churn (window saturated: one record in, one record out)
	// Count stays flat, so "did the window advance" must be judged by this
	// timestamp, not by Count — a count-based check silently stops
	// RecordingScore forever once every source hits WindowSize (the
	// rollback path would die without any error or warning).
	LastAt time.Time
	// Ok reports whether the judging gate passed (see Window's doc).
	Ok bool
}

// sourceStat holds the mean, count, and newest timestamp of one evidence
// source inside the window.
type sourceStat struct {
	Mean   float64
	Count  int
	LastAt time.Time
}

// Window computes the aggregate fitness over recent evidence for the given
// strategy ID. Returns Ok=false when insufficient evidence exists, so callers
// can apply a conservative cold-start policy. Without a time range it queries
// the most recent WindowSize records per source (legacy behavior). Callers
// that compare two strategies (deployment staging Evaluate) MUST use WindowAt
// with a shared [since, until] anchor instead — two independent Window calls
// can straddle concurrent evidence writes and distort the delta (E1).
func (a *RuntimeFitnessAggregator) Window(ctx context.Context, strategyID string) WindowResult {
	return a.WindowAt(ctx, strategyID, time.Time{}, time.Time{})
}

// WindowAt is Window scoped to the [since, until] evidence time range (E1).
// Zero since/until disables that bound (matching MemoryStore/PostgresStore
// semantics). Both bounds SHOULD be non-zero for staging comparisons so the
// shadow and baseline sides read the same snapshot.
//
// The aggregation:
//  1. Queries KindFitness evidence for each configured source.
//  2. Computes the per-source mean (only values in [0,1] are accepted,
//     matching recentFitnessSummary's filter).
//  3. Computes the weighted aggregate across sources.
//
// strategyID scoping AND the judging gate (review fix #4): the "strategy"
// source is scoped by the ID (its records carry a strategy_id payload key
// written by RuntimeObserver), and so are the two Step Y channels
// ("collaboration" / "tool_call" — a receipt is earned by the strategy that
// chose to ask/call). The workflow/scheduler/recovery sources are
// runtime-global — they measure the system that runs the active strategy, not
// a specific candidate — so they intentionally ignore the ID.
//
//   - When strategyID is NON-empty (the rollback-decision path), the
//     "strategy" source must ITSELF hold ≥ MinSamplesBeforeJudge records for
//     the given ID before Ok=true. Global sources contribute to the weighted
//     mean but can never substitute for the active strategy's own evidence
//     (design doc §4⑤ principle 4: rollback decisions must rest on the
//     strategy's own evidence). Without
//     this gate, 10 unrelated global records would license a rollback
//     decision while the strategy's own sample count is 0.
//   - When strategyID is empty (deployment staging), the gate is the total
//     count across sources, matching the pre-existing staging contract.
//
// WindowResult.LastAt carries the newest in-window evidence timestamp:
// callers that feed a score into a sliding policy (the lifecycle watch loop)
// MUST gate on LastAt advancing, never on Count — Count saturates at
// WindowSize per source and stops changing under steady-state churn.
//
// LastAt is deliberately the STRATEGY source's newest timestamp only (when
// the caller scopes by a strategy ID): the global sources churn at their own
// rates, and a global-only advance would re-trigger RecordScore every tick
// while the strategy's own fitness sample set is unchanged — partially
// defeating the decorrelation. Callers needing the overall newest timestamp
// can take the max over PerSource.
func (a *RuntimeFitnessAggregator) WindowAt(ctx context.Context, strategyID string, since, until time.Time) WindowResult {
	// Read cfg and store under the SAME lock: SetStore may run concurrently
	// with Window (bootstrap injects the shared store after construction),
	// and an unlocked store read is a data race.
	a.mu.RLock()
	cfg := a.cfg
	store := a.store
	a.mu.RUnlock()

	if store == nil {
		return WindowResult{Mean: cfg.ColdStartScore, PerSource: map[string]sourceStat{}}
	}

	sources := []struct {
		name       string
		weight     float64
		strategyID string
		// optIn marks a source that is INERT at weight 0: it is skipped
		// entirely rather than merely contributing 0 to the weighted mean.
		// This matters because a counted source also advances totalCount,
		// which is the staging path's judging gate — a channel nobody opted
		// into must not license a staging verdict.
		optIn bool
	}{
		{name: "strategy", weight: cfg.Weights.Outcome, strategyID: strategyID},
		{name: "workflow", weight: cfg.Weights.Workflow},
		{name: "scheduler", weight: cfg.Weights.Scheduler},
		{name: "recovery", weight: cfg.Weights.Recovery},
		// Step Y.2/Y.3 channels. Both are strategy-SCOPED: unlike the
		// runtime-global sources above, a collaboration receipt or a tool
		// outcome is produced BY a specific strategy's decisions ("ask that
		// agent", "call that tool"), so crediting it to another strategy would
		// be a mis-attribution.
		{name: collaborationEvidenceSource, weight: cfg.Weights.Collaboration, strategyID: strategyID, optIn: true},
		{name: toolCallEvidenceSource, weight: cfg.Weights.ToolCall, strategyID: strategyID, optIn: true},
	}

	// Also query dimension_eval evidence.
	dimMean, dimCount, dimLastAt := a.querySourceMeanAt(ctx, store, "dimension_eval", evidence.KindDimensionEval, cfg.WindowSize, "", since, until)

	perSource := make(map[string]sourceStat)
	totalCount := 0
	// strategyCount: samples of the STRATEGY source alone — the judging
	// gate for the rollback path (see the doc comment on Window).
	strategyCount := 0
	strategyLastAt := time.Time{}
	globalLastAt := time.Time{}
	var weightedSum float64
	var weightSum float64

	for _, src := range sources {
		if src.optIn && src.weight <= 0 {
			// Not opted in: skip entirely so the source contributes neither
			// weight nor sample count (see the optIn field comment).
			continue
		}
		m, c, srcLastAt := a.querySourceMeanAt(ctx, store, src.name, evidence.KindFitness, cfg.WindowSize, src.strategyID, since, until)
		if c == 0 {
			continue
		}
		perSource[src.name] = sourceStat{Mean: m, Count: c, LastAt: srcLastAt}
		totalCount += c
		weightedSum += m * src.weight
		weightSum += src.weight
		if srcLastAt.After(globalLastAt) {
			globalLastAt = srcLastAt
		}
		if src.name == "strategy" {
			strategyCount = c
			strategyLastAt = srcLastAt
		}
	}

	if dimCount > 0 {
		perSource["dimension_eval"] = sourceStat{Mean: dimMean, Count: dimCount, LastAt: dimLastAt}
		totalCount += dimCount
		weightedSum += dimMean * cfg.Weights.DimensionEval
		weightSum += cfg.Weights.DimensionEval
		if dimLastAt.After(globalLastAt) {
			globalLastAt = dimLastAt
		}
	}

	result := WindowResult{PerSource: perSource}

	if weightSum == 0 {
		result.Mean = cfg.ColdStartScore
		return result
	}

	mean := weightedSum / weightSum

	// TODO(tech-debt): subtract the cost/latency penalty term here once a
	// real cost/latency data source reaches the EventStore (see the
	// tech-debt note on AggregatorConfig above).

	// Clamp to [0,1].
	if mean < 0 {
		mean = 0
	}
	if mean > 1 {
		mean = 1
	}
	result.Mean = mean
	result.Count = totalCount

	if strategyID != "" {
		// Rollback path: the active strategy's OWN evidence must reach the
		// judge threshold. Global sources weight the mean but never satisfy
		// the gate on the strategy's behalf (review fix #4). The advance
		// signal (LastAt) is likewise scoped to the strategy source.
		result.Ok = strategyCount >= cfg.MinSamplesBeforeJudge
		result.LastAt = strategyLastAt
		return result
	}
	// Staging path: advance signal is the newest timestamp across all
	// sources (the staging Evaluate has no decorrelation consumer; LastAt
	// here is informational).
	result.Ok = totalCount >= cfg.MinSamplesBeforeJudge
	result.LastAt = globalLastAt
	return result
}

// querySourceMeanAt computes the mean fitness value from evidence matching
// the given source and kind within [since, until] (E1). Only values in [0,1]
// are accepted (matching recentFitnessSummary's filter), so callers can rely
// on the [0,1] contract. When strategyID is non-empty, records whose payload
// strategy_id differs are skipped (the strategy source scopes by candidate);
// records without a strategy_id payload key are skipped too, because they
// cannot be attributed. The returned time is the newest in-window record's
// timestamp (zero when no records matched) — the saturation-safe "did the
// window advance" signal. The store is passed in (not read from the receiver)
// so WindowAt can snapshot it under its lock and keep this helper lock-free.
func (a *RuntimeFitnessAggregator) querySourceMeanAt(ctx context.Context, store evidence.Store, source string, kind evidence.EvidenceKind, limit int, strategyID string, since, until time.Time) (float64, int, time.Time) {
	return a.querySourceMeanScopedAt(ctx, store, source, kind, limit, strategyID, "", since, until)
}

// querySourceMeanScopedAt is querySourceMeanAt with an optional tool_step_id
// sub-filter (Y1 C3) and an explicit [since, until] evidence time range (E1).
// When toolStepID is non-empty, only tool_call evidence whose payload
// tool_step_id matches is counted — enabling process-level attribution
// ("this strategy calling the tool THIS way") distinct from the coarse
// per-strategy bucket.
func (a *RuntimeFitnessAggregator) querySourceMeanScopedAt(ctx context.Context, store evidence.Store, source string, kind evidence.EvidenceKind, limit int, strategyID, toolStepID string, since, until time.Time) (float64, int, time.Time) {
	if store == nil {
		return 0, 0, time.Time{}
	}
	evs, err := store.Query(ctx, evidence.Filter{
		Source: source,
		Kind:   kind,
		Since:  since,
		Until:  until,
		Limit:  limit,
	})
	if err != nil {
		return 0, 0, time.Time{}
	}
	var sum float64
	count := 0
	var lastAt time.Time
	for _, ev := range evs {
		if len(ev.Payload) == 0 {
			continue
		}
		var fe struct {
			Value      float64 `json:"value"`
			StrategyID string  `json:"strategy_id"`
			ToolStepID string  `json:"tool_step_id"`
		}
		if err := json.Unmarshal(ev.Payload, &fe); err != nil {
			continue
		}
		if fe.Value < 0 || fe.Value > 1 {
			continue
		}
		if strategyID != "" && fe.StrategyID != strategyID {
			continue
		}
		if toolStepID != "" && fe.ToolStepID != toolStepID {
			continue
		}
		sum += fe.Value
		count++
		if ev.Timestamp.After(lastAt) {
			lastAt = ev.Timestamp
		}
	}
	if count == 0 {
		return 0, 0, time.Time{}
	}
	return sum / float64(count), count, lastAt
}

// WindowToolStep computes the aggregate fitness scoped to a specific tool step
// (strategyID, toolStepID) for the tool_call channel (Y1 C3). The tool_step
// dimension surfaces process-level attribution: two strategies — or two
// argument shapes under the same strategy — calling the same tool no longer
// blend into one undifferentiated signal. It reuses the same cold-start gate
// and source weights as Window; the returned Ok reflects the tool_step-scoped
// sample count only when a non-empty strategyID is supplied.
func (a *RuntimeFitnessAggregator) WindowToolStep(ctx context.Context, strategyID, toolStepID string) WindowResult {
	return a.WindowToolStepAt(ctx, strategyID, toolStepID, time.Time{}, time.Time{})
}

// WindowToolStepAt is WindowToolStep scoped to [since, until] (E1/M6).
func (a *RuntimeFitnessAggregator) WindowToolStepAt(ctx context.Context, strategyID, toolStepID string, since, until time.Time) WindowResult {
	if toolStepID == "" {
		return a.WindowAt(ctx, strategyID, since, until)
	}
	a.mu.RLock()
	cfg := a.cfg
	store := a.store
	a.mu.RUnlock()
	if store == nil {
		return WindowResult{Mean: cfg.ColdStartScore, PerSource: map[string]sourceStat{}}
	}

	m, c, lastAt := a.querySourceMeanScopedAt(ctx, store, toolCallEvidenceSource, evidence.KindFitness, cfg.WindowSize, strategyID, toolStepID, since, until)
	result := WindowResult{PerSource: map[string]sourceStat{}}
	if c == 0 {
		result.Mean = cfg.ColdStartScore
		result.Ok = false
		return result
	}
	result.PerSource[toolStepID] = sourceStat{Mean: m, Count: c, LastAt: lastAt}
	result.Mean = m
	result.Count = c
	result.LastAt = lastAt
	// When no strategy is supplied the projection-level query is not scoped by
	// strategy (the tool-step audit is per-agent/session, and the Projector has
	// no strategy to stamp). Judge on sample count — the staging-path gate —
	// just like Window's non-strategy path, so the audit read is usable rather
	// than permanently cold-start.
	result.Ok = c >= cfg.MinSamplesBeforeJudge
	return result
}
