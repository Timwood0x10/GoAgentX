package introspect

import (
	"embed"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/Timwood0x10/ares/internal/ares_events"
)

//go:embed web/panel.html
var webFS embed.FS

// maxRawEvents caps the raw event-stream response size (the panel's Execution
// timeline / Events pages request bounded tails; the full durable history
// lives in the event store / archive).
const maxRawEvents = 500

// Handler serves the introspection panel: the embedded UI at /introspect and
// the JSON read API under /api/v1/introspect/*.
//
// SECURITY: this handler performs NO authentication or authorization. The
// /api/v1/introspect/eventstream endpoint returns raw events with their full
// payload (task inputs, checkpoints — see serveEventStream), and the snapshot
// exposes live scheduling/lease/agent state. It is intended for trusted
// operators only. Do NOT bind it to a public address: keep it on
// localhost/an internal network, or place it behind a reverse proxy that
// enforces authentication. Callers wiring this into a mux own that boundary.
type Handler struct {
	store *Store
	// eventStore is the optional raw event source backing the
	// /api/v1/introspect/eventstream endpoint (dashboard.md §9 Event Stream).
	// Nil disables that endpoint (503).
	eventStore ares_events.EventStore
	// systemRuntime is the optional provider for the System Runtime component
	// graph (K5): when set, /api/v1/introspect/snapshot carries a
	// "system_runtime" section with every managed component's state and
	// reason, so a kernel pillar that failed to reach Ready is visible on the
	// read surface. Nil keeps the legacy snapshot shape.
	systemRuntime func() any
}

// NewHandler builds a Handler over the given store (must be non-nil).
func NewHandler(store *Store) *Handler {
	return &Handler{store: store}
}

// WithEventStore attaches the raw event store so the panel can serve the
// original event stream (full payload) for the Execution timeline and Events
// pages. Optional — without it the stream endpoint reports 503 and the
// timeline/events pages fall back to the distilled Store.Events feed.
func (h *Handler) WithEventStore(store ares_events.EventStore) *Handler {
	h.eventStore = store
	return h
}

// WithSystemRuntime attaches the System Runtime snapshot provider (K5). The
// provider must be a read-only, JSON-marshalable value (typically a
// kernel.Snapshot). Optional — without it the snapshot endpoint keeps
// its legacy shape.
func (h *Handler) WithSystemRuntime(provider func() any) *Handler {
	h.systemRuntime = provider
	return h
}

// ServeHTTP routes introspection requests. Register it on the serve mux for
// the /introspect and /api/v1/introspect prefixes.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/introspect", "/introspect/":
		body, err := webFS.ReadFile("web/panel.html")
		if err != nil {
			http.Error(w, "panel asset missing", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(body)
	case "/api/v1/introspect/events":
		limit := 60
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= maxTimelineEntries {
				limit = n
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"events": h.store.Events(limit)})
	case "/api/v1/introspect/eventstream":
		h.serveEventStream(w, r)
	case "/api/v1/introspect/snapshot":
		snap := h.store.Latest()
		if snap == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"collector has not produced a snapshot yet"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// K5: embed the System Runtime component graph alongside the kernel
		// snapshot. The embedded pointer inlines the legacy fields, so the
		// panel keeps parsing the old shape and the new section is additive.
		if h.systemRuntime != nil {
			_ = json.NewEncoder(w).Encode(struct {
				*Snapshot
				SystemRuntime any `json:"system_runtime,omitempty"`
			}{snap, h.systemRuntime()})
			return
		}
		_ = json.NewEncoder(w).Encode(snap)
	default:
		http.NotFound(w, r)
	}
}

// serveEventStream returns the RAW event stream (original Event with full
// payload — dashboard.md §9: "Event ≠ Log"). Supports ?stream_id= for the
// per-task Execution timeline and ?limit= (default 200, cap maxRawEvents).
func (h *Handler) serveEventStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if h.eventStore == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"event stream not configured"}`))
		return
	}
	q := r.URL.Query()
	limit := 200
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= maxRawEvents {
			limit = n
		}
	}
	streamID := q.Get("stream_id")
	ctx := r.Context()

	var events []*ares_events.Event
	var err error
	if streamID != "" {
		events, err = h.eventStore.Read(ctx, streamID, ares_events.ReadOptions{
			Direction: ares_events.ReadDescending,
			Limit:     limit,
		})
	} else {
		events, err = h.eventStore.ReadAll(ctx, ares_events.ReadOptions{
			Direction: ares_events.ReadDescending,
			Limit:     limit,
		})
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	if events == nil {
		events = []*ares_events.Event{}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"events": events})
}
