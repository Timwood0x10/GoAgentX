package taskfabric

import (
	"encoding/json"
	"errors"
)

// CurrentCheckpointSchemaVersion is the version of the checkpoint envelope
// schema. Bump when the CheckpointEnvelope fields change; DecodeCheckpoint
// handles migration from prior versions.
//
// v1 → v2 (evolution loop closure E1): StrategyID added as an OPTIONAL field.
// A v1 envelope decodes under v2 code with StrategyID == "" (reads as
// "unattributed" — consumers fall back to the currently active strategy), so
// no migration code is needed in the forward direction. The reverse is NOT
// compatible: v1 code rejects a v2 envelope (ErrCheckpointSchemaVersion), so
// rolling back a deployment requires draining in-flight tasks first.
//
// v2 → v3 (M2 SessionID): SessionID added as an OPTIONAL field. A v2
// envelope decodes under v3 code with SessionID == "" (reads as
// "session-less"), so no migration code is needed in the forward direction.
const CurrentCheckpointSchemaVersion = 3

// CheckpointEnvelope is the durable, versioned checkpoint schema (W3). It
// wraps the submission-time metadata (UserProfile, Payload, UsedExperienceID)
// and the quantum's progress (StepCheckpoint) in a tagged, versioned envelope
// so scheduler, recovery, and executor all decode it through the same path.
//
// The envelope is stored in Task.Checkpoint (any). Before W3, the checkpoint
// was an unversioned fabricTaskMeta struct declared in cmd/ares/scheduler.go —
// a consumer-package type. Moving it here as a versioned, shared schema makes
// the cross-restart protocol stable and lets every consumer decode it through
// DecodeCheckpoint instead of each one re-implementing the same type-switch.
//
// Schema version history:
//   - v0 (pre-W3): unversioned fabricTaskMeta in cmd/ares/scheduler.go.
//   - v1 (W3):     CheckpointEnvelope with SchemaVersion field.
type CheckpointEnvelope struct {
	// SchemaVersion is the envelope's version. DecodeCheckpoint rejects a
	// future version instead of silently misinterpreting it.
	SchemaVersion int `json:"schema_version"`
	// UserProfile is the profile the leader attached to the task. Opaque to
	// the fabric; the executor restores it.
	UserProfile any `json:"user_profile,omitempty"`
	// Payload carries the task's opaque user data (incl. task_desc).
	Payload map[string]any `json:"payload,omitempty"`
	// UsedExperienceID is the experience consumed by this task (bandit
	// feedback linkage), preserved for the outcome recorder.
	UsedExperienceID string `json:"used_experience_id,omitempty"`
	// StepCheckpoint is the quantum's durable progress/output. nil before the
	// first quantum runs. The scheduler wraps every quantum's returned
	// checkpoint back into the envelope, so the submission metadata survives
	// yield→resume cycles (v0.3.0 review Bug 3 fix).
	StepCheckpoint any `json:"step_checkpoint,omitempty"`
	// StrategyID is the evolution strategy active when this task was
	// SUBMITTED. It is stamped once at Create time and never re-read, so
	// every sample the task produces is attributed to the strategy that
	// actually chose its prompt and LLM params — even when a promote happens
	// mid-flight. Empty means "unattributed" (pre-v2 envelope or no strategy
	// deployed): consumers fall back to the currently active strategy.
	//
	// The envelope is the carrier because attribution must stick at TASK
	// granularity (a task spans multiple quanta; re-reading the active
	// strategy per quantum would split one task's samples across strategies).
	StrategyID string `json:"strategy_id,omitempty"`
	// SessionID scopes this task to a conversational session (M2: SessionID
	// 贯通). It is stamped once at Create time and rides the envelope through
	// yield→resume cycles so the executor (plannerCognition) can read it to
	// look up the per-session L2 graph registry. Empty means "session-less"
	// (pre-v3 envelope or a non-session task): consumers treat the task as
	// belonging to no session.
	SessionID string `json:"session_id,omitempty"`
}

// DecodedCheckpoint is the result of DecodeCheckpoint: the envelope's fields
// extracted into a convenient struct for the consumer (scheduler, recovery,
// executor). A nil envelope (no checkpoint) produces a zero-valued result.
type DecodedCheckpoint struct {
	// UserProfile is the restored profile (nil when absent).
	UserProfile any
	// Payload is the task's opaque user data (nil when absent).
	Payload map[string]any
	// UsedExperienceID is the experience ID ("" when absent).
	UsedExperienceID string
	// StepCheckpoint is the quantum's progress (nil when absent — the task
	// has not yet run a quantum, or the checkpoint is a pre-execution
	// envelope).
	StepCheckpoint any
	// StrategyID is the submission-time strategy attribution ("" when absent
	// or when the envelope predates schema v2).
	StrategyID string
	// SessionID is the session scope of this task ("" when absent or when the
	// envelope predates schema v3).
	SessionID string
	// SchemaVersion is the envelope's version (0 when no checkpoint).
	SchemaVersion int
}

// DecodeCheckpoint decodes a Task.Checkpoint value through the single shared
// path (W3: 统一解码). It handles three forms:
//
//   - *CheckpointEnvelope (v1+): the versioned schema. Fields are extracted
//     directly. A future version (> CurrentCheckpointSchemaVersion) returns
//     ErrCheckpointSchemaVersion to prevent silent misinterpretation.
//   - map[string]any: a JSON-round-tripped envelope (e.g. after persistence
//     and reload). The fields are extracted from the map; UserProfile is
//     left as the raw map for the consumer to reify.
//   - any other type (including nil): treated as a raw step checkpoint
//     (pre-envelope or a plain progress marker). The value is placed in
//     StepCheckpoint so the consumer can still surface it.
//
// Returns:
//   - DecodedCheckpoint: the decoded fields.
//   - error: ErrCheckpointSchemaVersion when the envelope's version is from a
//     future schema (the caller must migrate or reject).
func DecodeCheckpoint(cp any) (DecodedCheckpoint, error) {
	if cp == nil {
		return DecodedCheckpoint{}, nil
	}
	// v1+ versioned envelope.
	if env, ok := cp.(*CheckpointEnvelope); ok && env != nil {
		if env.SchemaVersion > CurrentCheckpointSchemaVersion {
			return DecodedCheckpoint{}, ErrCheckpointSchemaVersion
		}
		return DecodedCheckpoint{
			UserProfile:      env.UserProfile,
			Payload:          env.Payload,
			UsedExperienceID: env.UsedExperienceID,
			StepCheckpoint:   env.StepCheckpoint,
			StrategyID:       env.StrategyID,
			SessionID:        env.SessionID,
			SchemaVersion:    env.SchemaVersion,
		}, nil
	}
	// JSON-round-tripped envelope (map[string]any).
	if m, ok := cp.(map[string]any); ok {
		// Check for the schema_version key — if present, treat it as an
		// envelope that survived a JSON round-trip.
		if sv, hasVersion := m["schema_version"]; hasVersion {
			version := 0
			switch v := sv.(type) {
			case float64:
				version = int(v)
			case int:
				version = v
			}
			if version > CurrentCheckpointSchemaVersion {
				return DecodedCheckpoint{}, ErrCheckpointSchemaVersion
			}
			dc := DecodedCheckpoint{
				SchemaVersion: version,
			}
			if up, ok := m["user_profile"]; ok {
				dc.UserProfile = up
			}
			if p, ok := m["payload"].(map[string]any); ok {
				dc.Payload = p
			}
			if eid, ok := m["used_experience_id"].(string); ok {
				dc.UsedExperienceID = eid
			}
			if sc, ok := m["step_checkpoint"]; ok {
				dc.StepCheckpoint = sc
			}
			if sid, ok := m[restoreKeyStrategyID].(string); ok {
				dc.StrategyID = sid
			}
			if sid, ok := m[restoreKeySessionID].(string); ok {
				dc.SessionID = sid
			}
			return dc, nil
		}
		// A plain map without schema_version: treat as a raw step checkpoint.
		return DecodedCheckpoint{StepCheckpoint: m}, nil
	}
	// Any other type: a raw step checkpoint (e.g. a struct, an int, etc.).
	return DecodedCheckpoint{StepCheckpoint: cp}, nil
}

// ErrCheckpointSchemaVersion is returned by DecodeCheckpoint when the
// envelope's schema version is from a future version the current code cannot
// interpret. The caller must migrate the envelope or reject the task.
var ErrCheckpointSchemaVersion = errors.New("taskfabric: checkpoint schema version mismatch")

// EncodeCheckpoint serializes a DecodedCheckpoint back into a
// CheckpointEnvelope for storage. This is the inverse of DecodeCheckpoint;
// the scheduler uses it when re-wrapping a quantum's output (yield/done) so
// the submission metadata survives across yield→resume cycles.
//
// Args:
//   - dc: the decoded checkpoint fields to wrap.
//
// Returns:
//   - *CheckpointEnvelope: the versioned envelope, ready to store in
//     Task.Checkpoint.
func EncodeCheckpoint(dc DecodedCheckpoint) *CheckpointEnvelope {
	return &CheckpointEnvelope{
		SchemaVersion:    CurrentCheckpointSchemaVersion,
		UserProfile:      dc.UserProfile,
		Payload:          dc.Payload,
		UsedExperienceID: dc.UsedExperienceID,
		StepCheckpoint:   dc.StepCheckpoint,
		StrategyID:       dc.StrategyID,
		SessionID:        dc.SessionID,
	}
}

// strategyIDFromCheckpoint extracts the submission-time strategy attribution
// from a checkpoint value through the single shared decode path. A decode
// failure or an unattributed checkpoint returns "" — attribution is strictly
// best-effort: a missing StrategyID degrades to the caller's active-strategy
// fallback and must never break the state machine. DecodeCheckpoint is a pure
// in-memory operation, so this is safe to call under the fabric mutex.
func strategyIDFromCheckpoint(cp any) string {
	dc, err := DecodeCheckpoint(cp)
	if err != nil {
		return ""
	}
	return dc.StrategyID
}

// sessionIDFromCheckpoint extracts the session scope from a checkpoint value
// through the single shared decode path. A decode failure or a session-less
// checkpoint returns "". Best-effort like strategyIDFromCheckpoint: a missing
// SessionID degrades to "" and never breaks the state machine.
func sessionIDFromCheckpoint(cp any) string {
	dc, err := DecodeCheckpoint(cp)
	if err != nil {
		return ""
	}
	return dc.SessionID
}

// MarshalCheckpoint JSON-encodes a checkpoint value. When the value is a
// *CheckpointEnvelope, it marshals directly. When it is any other type, it
// wraps it in an envelope first (StepCheckpoint = the raw value) so the
// serialized form always carries the schema version. This is the single
// serialization path for persistence (W3: 固化协议).
//
// Args:
//   - cp: the checkpoint value (may be nil).
//
// Returns:
//   - []byte: the JSON encoding.
//   - error: json.Marshal error.
func MarshalCheckpoint(cp any) ([]byte, error) {
	if cp == nil {
		return []byte("null"), nil
	}
	if env, ok := cp.(*CheckpointEnvelope); ok {
		return json.Marshal(env)
	}
	// Wrap non-envelope values so the serialized form is always versioned.
	dc, err := DecodeCheckpoint(cp)
	if err != nil {
		return nil, err
	}
	return json.Marshal(EncodeCheckpoint(dc))
}
