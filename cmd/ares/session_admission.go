package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/Timwood0x10/ares/internal/agentfabric"
	"github.com/Timwood0x10/ares/internal/planprojection"
	"github.com/Timwood0x10/ares/internal/taskfabric"
	"github.com/Timwood0x10/ares/internal/workflow/engine"
)

// ensureSessionAdmission admits one L2 session before its first task is
// created (M4-B2): register the session graph, subscribe it to the shared
// incremental compiler, and compile the root task the planner's first
// quantum falls back to.
//
// The caller is submitPeerTask, and only when the request carries a
// session_id AND the gate wired a registry (nil registry = gate off =
// legacy path, session payloads stay envelope-only). Admission is idempotent:
// resubmitting into a live session is a multi-turn continuation, not an
// error — the existing session is reused and no duplicate root is compiled.
//
// Failures are fail-fast (nothing half-created): a session the caller asked
// for but we cannot admit must not silently degrade into an unrunnable
// task. Anything InitSession registered before the failure is released
// again, so a retry starts clean.
func ensureSessionAdmission(ctx context.Context, kernel *kernelHandle, sessionID, prompt string) error {
	if kernel == nil || kernel.sessionReg == nil || sessionID == "" {
		return nil
	}
	if _, err := kernel.sessionReg.GetSession(sessionID); err == nil {
		return nil
	} else if !errors.Is(err, agentfabric.ErrSessionNotFound) {
		return fmt.Errorf("peer mode: look up session %q: %w", sessionID, err)
	}
	if kernel.compileCoord == nil || kernel.fabric == nil {
		return fmt.Errorf("peer mode: cannot admit session %q without compile coordinator and fabric", sessionID)
	}

	// The compile subscription must outlive the submission request: tying it
	// to the request context would kill the projection the moment the HTTP
	// handler returns, while the session lives on.
	liveCtx := context.WithoutCancel(ctx)
	g, err := kernel.sessionReg.InitSession(liveCtx, sessionID, prompt, nil,
		func(subCtx context.Context, dag *engine.MutableDAG) (stop func()) {
			return kernel.compileCoord.SubscribeGraphEvents(subCtx, dag)
		})
	if err != nil {
		// A concurrent admitter may have won the race between our
		// GetSession and InitSession — re-check before failing.
		if errors.Is(err, agentfabric.ErrSessionAlreadyExists) {
			if _, err2 := kernel.sessionReg.GetSession(sessionID); err2 == nil {
				return nil
			}
		}
		return fmt.Errorf("peer mode: init session %q: %w", sessionID, err)
	}

	// Compile the root task the planner's first quantum reads (or falls
	// back to the payload input when still pending). An already-compiled
	// root means a retried admission after a partial failure — adopt it.
	rootStep := g.DAG().StepIndex()[g.Root()]
	if _, err := kernel.fabric.CompileNode(liveCtx, planprojection.ProjectStep(rootStep)); err != nil {
		if !errors.Is(err, taskfabric.ErrTaskExists) {
			releaseSessionQuietly(kernel, sessionID)
			return fmt.Errorf("peer mode: compile session %q root: %w", sessionID, err)
		}
	}
	log.Printf("peer mode: admitted L2 session %q (root %q compiled)", sessionID, g.Root())
	return nil
}

// releaseSessionQuietly drops a half-admitted session during failure
// cleanup. The release itself is best-effort: the admission already failed,
// and a release miss only leaves a normal session behind for the reaper.
func releaseSessionQuietly(kernel *kernelHandle, sessionID string) {
	_ = kernel.sessionReg.ReleaseSession(sessionID)
}

// sessionKeepSet builds the reaper's keep predicate from the session
// registry (P0-1): a task is kept while its owning session is still live.
// The registry is the single authority — an ID that parses as a session
// task but has no live session (released, or never admitted by this
// process) is harvestable once the grace window passes.
func sessionKeepSet(reg *agentfabric.SessionRegistry) func(taskID string) bool {
	return func(taskID string) bool {
		sid, ok := agentfabric.SessionIDFromNode(taskID)
		if !ok {
			return false
		}
		_, err := reg.GetSession(sid)
		return err == nil
	}
}
