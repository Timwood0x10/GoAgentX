package taskfabric

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Timwood0x10/ares/internal/ares_events"
)

// This file implements cross-restart task-state rebuild (release-readiness
// T2 / ares-repair-plan GAP): with an event store attached, a restarted
// Fabric can fold the durable task.* event log back into in-memory tasks.
//
// Design contracts:
//
//   - Only must-persist events (isMustPersistEvent: created / checkpointed /
//     completed / failed / expired) are trusted for state rebuild. The other
//     task.* events are observability-only and never consulted here.
//   - Leases are NEVER restored: after a process crash the previous holders
//     are gone, so every non-terminal task folds to READY with no owner.
//     The task then flows through the ordinary READY → acquire → resume-from-
//     checkpoint path; no special execution branch is added.
//   - The fencing epoch is restored to (max epoch seen in the log) + 1, so
//     new leases can never collide with pre-crash tokens and a stale holder
//     presenting an old epoch is rejected by ownerLocked (ErrEpochMismatch).
//   - RestoreFromStore is idempotent: each call resets the task map and
//     re-folds the whole log, so repeated calls on the same store converge to
//     the same state. It is a startup-time operation — the caller must not
//     run it concurrently with scheduler traffic.

// payload keys written by recordLocked and consumed by foldRestoreEvent.
// Declared once so goconst stays quiet and the contract is grep-able.
const (
	restoreKeyTaskID         = "task_id"
	restoreKeyAgentID        = "agent_id"
	restoreKeyOrigin         = "origin"
	restoreKeyState          = "state"
	restoreKeyEpoch          = "epoch"
	restoreKeyCapability     = "capability"
	restoreKeyPriority       = "priority"
	restoreKeyDependencies   = "dependencies"
	restoreKeyDeadline       = "deadline"
	restoreKeyRetryAttempts  = "retry_attempts"
	restoreKeyRetryMax       = "retry_max"
	restoreKeyCreatedAt      = "created_at"
	restoreKeyCheckpointJSON = "checkpoint_json"
	// restoreKeyStrategyID rides on EVERY persisted event (same reasoning as
	// the epoch key): the RuntimeObserver attributes fitness samples by it,
	// and the observability-only task.acquired/completed events are exactly
	// the ones it subscribes to. The value itself lives inside the checkpoint
	// envelope (Task.Checkpoint), which restoreCheckpoint already folds back
	// wholesale — no dedicated Task field, no extra fold branch.
	restoreKeyStrategyID = "strategy_id"
	// restoreKeySessionID rides on EVERY persisted event (same reasoning as
	// StrategyID): the session scope must survive a restart so the L2 graph
	// registry can re-associate resumed tasks with their session. The value
	// also lives inside the checkpoint envelope, but riding it on every event
	// ensures it is visible to event subscribers without decoding checkpoints.
	restoreKeySessionID = "session_id"
)

// RestoreFromStore rebuilds the in-memory task set from the attached event
// store (cross-restart recovery). With no store attached it is a no-op —
// the SDK's default path has no durable log and must not fail startup.
//
// The fold trusts only must-persist events; see the file comment for the full
// contract. Terminal tasks (COMPLETED/FAILED) are restored as terminal and
// never revived; every other task folds to READY (unowned, lease cleared)
// with its checkpoint, priority, dependencies and retry budget intact.
//
// Args:
//   - ctx: bounds the store read.
//
// Returns:
//   - error: a store read failure (the caller should fail startup loudly —
//     silently continuing would drop tasks that the log says exist).
func (f *Fabric) RestoreFromStore(ctx context.Context) error {
	f.mu.Lock()
	store := f.store
	f.mu.Unlock()
	if store == nil {
		return nil
	}

	events, err := store.ReadAll(ctx, ares_events.ReadOptions{})
	if err != nil {
		return fmt.Errorf("taskfabric: restore read failed: %w", err)
	}

	// Two passes: first pass registers every task.created (so fold order of
	// the later events cannot depend on ReadAll's timestamp sort being stable
	// across equal timestamps); second pass folds the remaining must-persist
	// events in log order.
	//
	// The epoch scan is SEPARATE and spans every event: task.acquired is
	// observability-only (never folded into state) but it is the event that
	// carries the fencing token Acquire just granted. Ignoring it would let
	// the rebuilt fabric re-issue tokens that pre-crash holders still hold.
	var created []*ares_events.Event
	var rest []*ares_events.Event
	maxEpoch := uint64(0)
	for _, ev := range events {
		if e := restoreEpoch(ev.Payload); e > maxEpoch {
			maxEpoch = e
		}
		switch ev.Type {
		case ares_events.EventTaskCreated:
			created = append(created, ev)
		case ares_events.EventTaskCheckpointed,
			ares_events.EventTaskCompleted,
			ares_events.EventTaskFailed,
			ares_events.EventTaskExpired:
			rest = append(rest, ev)
		default:
			// Observability-only event: never trusted for state rebuild
			// (its epoch was already harvested above).
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	// Idempotent rebuild: reset, then fold. Any tasks created before this
	// call are replaced by the durable state.
	f.tasks = make(map[string]*Task)
	f.events = nil
	for _, ev := range created {
		if err := f.foldRestoreEvent(ev); err != nil {
			log.Warn("taskfabric: restore skipped unusable task.created event", "error", err)
		}
	}
	for _, ev := range rest {
		if err := f.foldRestoreEvent(ev); err != nil {
			log.Warn("taskfabric: restore skipped unusable event", "event_type", ev.Type, "error", err)
		}
	}
	// Epoch must dominate every token the pre-crash fabric ever handed out.
	// Each persisted event records the fabric-wide epoch at record time, and
	// Acquire (the only epoch bump) always records task.acquired, so max+1 is
	// strictly greater than any pre-crash fencing token. Only ever GROW the
	// epoch: a repeated restore (idempotency contract) must never shrink it,
	// or tokens handed out between two restores would be re-issued.
	if next := maxEpoch + 1; next > f.epoch {
		f.epoch = next
	}
	return nil
}

// foldRestoreEvent applies one durable task.* event to the in-memory task
// set. It must be called with f.mu held. Unusable events (missing fields,
// unknown task for a non-created event, undecodable checkpoint) return an
// error and are skipped by the caller — one corrupt record must not abort the
// whole rebuild, but it is logged so divergence is visible.
func (f *Fabric) foldRestoreEvent(ev *ares_events.Event) error {
	if ev == nil {
		return fmt.Errorf("nil event")
	}
	p := ev.Payload
	id, _ := p[restoreKeyTaskID].(string)
	if id == "" {
		return fmt.Errorf("missing task_id")
	}

	switch ev.Type {
	case ares_events.EventTaskCreated:
		t := &Task{ID: id}
		t.Capability, _ = p[restoreKeyCapability].(string)
		t.Origin, _ = p[restoreKeyOrigin].(string)
		t.Priority = restoreInt(p, restoreKeyPriority)
		t.RetryPolicy.Attempts = restoreInt(p, restoreKeyRetryAttempts)
		t.RetryPolicy.MaxRetries = restoreInt(p, restoreKeyRetryMax)
		// Dependencies may arrive as []string (in-process store) or []any of
		// strings (JSON round-tripped store) — accept both forms.
		switch deps := p[restoreKeyDependencies].(type) {
		case []string:
			t.Dependencies = append([]string(nil), deps...)
		case []any:
			for _, d := range deps {
				if s, ok := d.(string); ok {
					t.Dependencies = append(t.Dependencies, s)
				}
			}
		}
		if s, ok := p[restoreKeyDeadline].(string); ok {
			if ts, err := time.Parse(time.RFC3339, s); err == nil {
				t.Deadline = ts
			}
		}
		if s, ok := p[restoreKeyCreatedAt].(string); ok {
			if ts, err := time.Parse(time.RFC3339, s); err == nil {
				t.CreatedAt = ts
			}
		} else {
			t.CreatedAt = ev.Timestamp
		}
		t.UpdatedAt = ev.Timestamp
		// State folds to READY unless the log already says terminal. Leases
		// are never restored (Owner/Lease stay zero).
		switch state := TaskState(restoreString(p, restoreKeyState)); state {
		case StateCompleted, StateFailed:
			t.State = state
		default:
			t.State = StateReady
		}
		if err := restoreCheckpoint(p, t); err != nil {
			return err
		}
		f.tasks[id] = t
		return nil

	case ares_events.EventTaskCheckpointed,
		ares_events.EventTaskCompleted,
		ares_events.EventTaskFailed,
		ares_events.EventTaskExpired:
		t, ok := f.tasks[id]
		if !ok {
			return fmt.Errorf("unknown task %q (created event missing?)", id)
		}
		t.UpdatedAt = ev.Timestamp
		if n := restoreInt(p, restoreKeyRetryAttempts); n > 0 {
			t.RetryPolicy.Attempts = n
		}
		// A terminal transition may carry the quantum's output as checkpoint
		// (CompleteWithCheckpoint) — fold it before fixing the final state.
		if err := restoreCheckpoint(p, t); err != nil {
			return err
		}
		switch state := TaskState(restoreString(p, restoreKeyState)); state {
		case StateCompleted, StateFailed:
			t.State = state
			t.Owner = ""
			t.Lease = nil
		case StateReady:
			// Fail-with-requeue / expiry requeue: unowned and acquirable.
			t.State = StateReady
			t.Owner = ""
			t.Lease = nil
		default:
			// LEASED/RUNNING/SUSPENDED from a pre-crash life: the holders are
			// gone, so fold to READY unowned.
			t.State = StateReady
			t.Owner = ""
			t.Lease = nil
		}
		return nil

	default:
		return fmt.Errorf("event type %s is not restore-trusted", ev.Type)
	}
}

// restoreCheckpoint decodes the payload's checkpoint_json (written through
// MarshalCheckpoint, so it is always a versioned CheckpointEnvelope) into the
// task's checkpoint. A missing key leaves the task's checkpoint unchanged
// (nil for a fresh restore). JSON round-trip produces the map form that
// DecodeCheckpoint's second branch handles.
func restoreCheckpoint(p map[string]any, t *Task) error {
	raw, ok := p[restoreKeyCheckpointJSON].(string)
	if !ok || raw == "" || raw == "null" {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return fmt.Errorf("decode checkpoint for task %s: %w", t.ID, err)
	}
	t.Checkpoint = decoded
	return nil
}

// restoreString extracts a string payload field ("" when absent/wrong type).
func restoreString(p map[string]any, key string) string {
	s, _ := p[key].(string)
	return s
}

// restoreInt extracts an integer payload field. Event payloads arrive as
// JSON-decoded maps (numbers are float64) but in-process maps carry int.
func restoreInt(p map[string]any, key string) int {
	switch v := p[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	default:
		return 0
	}
}

// restoreEpoch extracts the fabric-wide epoch recorded on the event (0 when
// absent — legacy logs predating the rebuild contract).
func restoreEpoch(p map[string]any) uint64 {
	switch v := p[restoreKeyEpoch].(type) {
	case float64:
		if v < 0 {
			return 0
		}
		return uint64(v) // #nosec G115 — negative values rejected above
	case uint64:
		return v
	case int:
		if v < 0 {
			return 0
		}
		return uint64(v) // #nosec G115 — negative values rejected above
	default:
		return 0
	}
}
