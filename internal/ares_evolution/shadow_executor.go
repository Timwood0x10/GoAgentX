// shadow_executor.go provides the real-execution shadow A/B path (closure
// plan Step 4 / N-1). When a candidate strategy is submitted for judgment,
// the ShadowExecutor executes the candidate AND the active strategy on
// buffered copies of recent real tasks inside an isolated, side-effect-free
// runner, and writes one shadow-marked fitness evidence record per executed
// arm. The paired scores flow back to the ShadowSampler, which records them
// as the G2 gate's comparisons — this is what makes the verdict
// candidate-specific, the property replay-only evidence can never provide
// for a never-executed candidate.
//
// ISOLATION CONTRACT: the runner is responsible for denying side effects
// (production memory writes, external tool calls) at the interface level.
// This file never touches production state: it clones task descriptions,
// runs the injected ShadowRunner, and writes evidence.
//
// THREAD-SAFETY: all methods are safe for concurrent use. OnTaskFinalized is
// called from scheduler drain goroutines; Feed is called inline in Submit
// (bounded by the sampler's Prime timeout and the per-run execution timeout).
package evolution

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/Timwood0x10/ares/internal/ares_evolution/mutation"
	"github.com/Timwood0x10/ares/internal/core/models"
	"github.com/Timwood0x10/ares/internal/evidence"
)

// shadowEvidenceSource is the DEDICATED evidence source for real-execution
// shadow A/B records. It is deliberately NOT the observer's "strategy"
// source: every live-fitness consumer (the rollback window, deployment
// staging) aggregates source="strategy", and a shadow record — an isolated,
// tool-less execution — measures the strategy under a different standard.
// Mixing the two would corrupt live-fitness verdicts. The G2 comparisons
// flow through the ShadowSampler directly, so nothing reads this source
// back except audits.
const shadowEvidenceSource = "strategy_shadow"

// evidenceKeyShadow marks the payload as a shadow-execution record.
const evidenceKeyShadow = "shadow"

// defaultShadowExecSampleSize is the per-Feed execution pair count when the
// config omits sample_size.
const defaultShadowExecSampleSize = 3

// shadowTaskBuffer keeps the most recent finalized real tasks available for
// shadow sampling. Old entries drop when the buffer is full — evidence must
// come from RECENT tasks or it stops representing current traffic.
const shadowTaskBuffer = 32

// shadowExecTimeout bounds ONE isolated execution pass. Real LLM calls are
// bounded by the client's own timeout too; this is the outer guarantee that
// a Prime batch (which runs inline in Submit) cannot stall the evolution
// heartbeat indefinitely.
const shadowExecTimeout = 15 * time.Second

// ShadowComparisonPair is one paired real-execution A/B result: both arms ran
// the same task copy under the same isolation standard.
type ShadowComparisonPair struct {
	// ActiveScore is the active strategy's isolated-execution outcome
	// (1.0 completed, 0.0 not completed).
	ActiveScore float64
	// ShadowScore is the candidate strategy's isolated-execution outcome.
	ShadowScore float64
}

// ShadowExecutionFeeder produces candidate-specific real-execution A/B
// comparisons (closure plan Step 4 / N-1). The ShadowSampler consumes it
// inside Prime, before the replay-window fallback.
type ShadowExecutionFeeder interface {
	// Feed executes the candidate and active strategies on buffered recent
	// task copies and writes shadow-marked evidence per executed arm. It
	// returns the paired comparisons — empty when no task is buffered or an
	// arm failed to run. Implementations must respect ctx cancellation and
	// bound their own per-execution cost.
	Feed(ctx context.Context, candidate, active *mutation.Strategy) []ShadowComparisonPair
}

// ShadowRunner executes one isolated execution pass for a task copy under a
// specific strategy. The isolation contract (no production memory writes, no
// external tool calls) is enforced by the implementation, not the caller.
type ShadowRunner interface {
	// RunShadow executes the task copy under the strategy. completed reports
	// whether the pass finished the task (a pass that exhausts its quantum
	// budget without finishing reports false — a real outcome, not an error).
	RunShadow(ctx context.Context, task *models.Task, strategy *mutation.Strategy) (completed bool, err error)
}

// ShadowExecutor buffers recent real tasks and executes isolated A/B passes
// on them when a candidate is submitted. It plays both wiring roles without
// importing either side:
//
//   - kernelscheduler's ShadowExecutionHook (structural satisfaction): the
//     serve layer attaches it to the scheduler, so finalized tasks land in
//     the buffer.
//   - evolution.ShadowExecutionFeeder: the serve layer sets it on the
//     ShadowSampler, so each candidate judgment runs the A/B pass.
type ShadowExecutor struct {
	// store is the shared evidence store; shadow records are written to the
	// dedicated shadowEvidenceSource.
	store evidence.Store
	// runner executes one isolated pass per arm.
	runner ShadowRunner
	// sampleSize is how many most-recent buffered tasks each Feed uses.
	sampleSize int
	// mu guards buffer. OnTaskFinalized (drain goroutines) and Feed
	// (Submit) are the two concurrent entry points.
	mu sync.Mutex // guards buffer
	// buffer is the ring of the most recent finalized tasks (oldest first).
	buffer []*models.Task
	// nowFn is the clock seam for deterministic tests.
	nowFn func() time.Time
}

// NewShadowExecutor creates the real-execution shadow A/B executor. A nil
// store or nil runner is rejected loudly: an executor without either would
// silently produce no evidence while looking wired — the exact failure class
// this plan exists to remove.
//
// Args:
//
//	store      - the shared evidence store (must be non-nil).
//	runner     - the isolated execution pass runner (must be non-nil).
//	sampleSize - tasks per Feed; non-positive falls back to the default.
//
// Returns:
//
//	*ShadowExecutor - the configured executor.
//	error           - non-nil when a required dependency is missing.
func NewShadowExecutor(store evidence.Store, runner ShadowRunner, sampleSize int) (*ShadowExecutor, error) {
	if store == nil {
		return nil, errors.New("shadow executor requires an evidence store")
	}
	if runner == nil {
		return nil, errors.New("shadow executor requires a shadow runner")
	}
	if sampleSize <= 0 {
		sampleSize = defaultShadowExecSampleSize
	}
	return &ShadowExecutor{store: store, runner: runner, sampleSize: sampleSize, nowFn: time.Now}, nil
}

// OnTaskFinalized implements kernel.ShadowExecutionHook structurally:
// it buffers the finalized task for later shadow sampling. Non-blocking by
// contract — a full buffer drops the OLDEST entry so the buffer keeps
// tracking recent traffic.
//
// Args:
//
//	task - the scheduler's view of the finalized task; stored as-is and never
//	       mutated (each isolated run gets its own clone).
func (s *ShadowExecutor) OnTaskFinalized(task *models.Task) {
	if s == nil || task == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.buffer) >= shadowTaskBuffer {
		s.buffer = s.buffer[1:]
	}
	s.buffer = append(s.buffer, task)
}

// Buffered reports how many finalized tasks are currently buffered for
// shadow A/B sampling. Observability/test surface.
func (s *ShadowExecutor) Buffered() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buffer)
}

// Feed implements ShadowExecutionFeeder: execute both A/B arms on the most
// recent buffered task copies and write shadow-marked evidence per executed
// arm.
//
// Scoring: an arm that completes the pass scores 1.0, one that exhausts its
// quantum budget scores 0.0 — both are real outcomes. An arm that fails to
// RUN at all (runner error, e.g. LLM transport failure) produces NO score and
// NO evidence: a missing measurement must never be invented into a verdict.
// A pair is only returned when BOTH arms ran on the same task.
//
// Guards: a nil candidate, an empty candidate ID (nothing to attribute the
// evidence to), or candidate == active would all fabricate a verdict, so
// Feed returns nothing in those cases.
func (s *ShadowExecutor) Feed(ctx context.Context, candidate, active *mutation.Strategy) []ShadowComparisonPair {
	if s == nil || s.runner == nil || s.store == nil || candidate == nil || active == nil {
		return nil
	}
	if candidate.ID == "" || candidate.ID == active.ID {
		return nil
	}
	tasks := s.snapshotTasks(s.sampleSize)
	var pairs []ShadowComparisonPair
	for _, t := range tasks {
		activeScore, activeRan, activeErr := s.runSide(ctx, t, active)
		shadowScore, shadowRan, shadowErr := s.runSide(ctx, t, candidate)
		if activeRan {
			s.writeShadowEvidence(ctx, active.ID, string(t.AgentType), activeScore)
		}
		if shadowRan {
			s.writeShadowEvidence(ctx, candidate.ID, string(t.AgentType), shadowScore)
		}
		if activeErr != nil || shadowErr != nil {
			continue
		}
		pairs = append(pairs, ShadowComparisonPair{ActiveScore: activeScore, ShadowScore: shadowScore})
	}
	return pairs
}

// snapshotTasks returns the most recent n buffered tasks in chronological
// order, copied out of the buffer so Feed works outside the lock.
func (s *ShadowExecutor) snapshotTasks(n int) []*models.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n <= 0 || len(s.buffer) == 0 {
		return nil
	}
	if n > len(s.buffer) {
		n = len(s.buffer)
	}
	out := make([]*models.Task, n)
	copy(out, s.buffer[len(s.buffer)-n:])
	return out
}

// runSide executes one arm. ran=false (with err) means the arm failed to run
// at all; ran=true with completed=false is the real "did not finish" outcome.
func (s *ShadowExecutor) runSide(ctx context.Context, t *models.Task, strategy *mutation.Strategy) (score float64, ran bool, err error) {
	execCtx, cancel := context.WithTimeout(ctx, shadowExecTimeout)
	defer cancel()
	completed, err := s.runner.RunShadow(execCtx, s.cloneTask(t), strategy)
	if err != nil {
		return 0, false, err
	}
	if completed {
		return 1, true, nil
	}
	return 0, true, nil
}

// cloneTask copies the finalized task description for one isolated run. The
// runner mutates its copy (yield checkpoints ride in the payload), so each
// arm gets a fresh clone and the buffered original is never touched.
func (s *ShadowExecutor) cloneTask(t *models.Task) *models.Task {
	cp := models.NewTask(t.TaskID+"#shadow", t.AgentType, t.UserProfile)
	cp.UsedExperienceID = t.UsedExperienceID
	cp.StrategyID = t.StrategyID
	if t.Payload != nil {
		cp.Payload = make(map[string]any, len(t.Payload))
		for k, v := range t.Payload {
			cp.Payload[k] = v
		}
	}
	return cp
}

// writeShadowEvidence appends one shadow-marked fitness record. Best-effort
// (mirrors RuntimeObserver.writeEvidence): a shadow record must never break
// the Submit path it runs inside.
func (s *ShadowExecutor) writeShadowEvidence(ctx context.Context, strategyID, taskType string, score float64) {
	payload, err := json.Marshal(map[string]any{
		"value":               score,
		"success":             score > 0,
		evidenceKeyStrategyID: strategyID,
		"task_type":           taskType,
		evidenceKeyShadow:     true,
	})
	if err != nil {
		return
	}
	now := s.nowFn()
	// Full-date format with fractional seconds: the PG store deduplicates on
	// id via ON CONFLICT DO NOTHING, so a coarser timestamp could silently
	// drop records (same reasoning as the observer's evidence IDs).
	_ = s.store.Append(ctx, evidence.Evidence{
		ID:        "shadow_" + strategyID + "_" + now.UTC().Format("20060102150405.000000"),
		Source:    shadowEvidenceSource,
		Kind:      evidence.KindFitness,
		Payload:   payload,
		Timestamp: now,
	})
}
