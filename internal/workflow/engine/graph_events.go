package engine

import (
	"fmt"
	"sync"
	"time"
)

// ChangeType classifies a graph mutation type.
type ChangeType int

const (
	// ChangeAddNode indicates a node was added to the DAG.
	ChangeAddNode ChangeType = iota
	// ChangeRemoveNode indicates a node was removed from the DAG.
	ChangeRemoveNode
	// ChangeAddEdge indicates an edge was added to the DAG.
	ChangeAddEdge
	// ChangeRemoveEdge indicates an edge was removed from the DAG.
	ChangeRemoveEdge
	// ChangeReplaceNode indicates a node was replaced (swap migration).
	ChangeReplaceNode
	// ChangeSetNodeMetadata indicates a node's Metadata map was replaced in
	// place (C4 metadata patch).
	ChangeSetNodeMetadata
	// ChangeReconcile is not published by the DAG: it labels a ChangeResult
	// produced by a full state reconcile (a subscriber compensating for
	// missed events), so "created by reconcile" stays attributable.
	ChangeReconcile
)

// GraphChange describes a single mutation to the DAG.
type GraphChange struct {
	Type      ChangeType
	NodeID    string
	OldNodeID string // populated for ChangeReplaceNode
	FromID    string
	ToID      string
	Step      *Step
	Timestamp time.Time
}

// GraphEvent is emitted when a mutation is applied.
type GraphEvent struct {
	// Seq is the hub-wide monotonic sequence number, assigned under the same
	// mutex that publishes the event. A subscriber that sees Seq skip has
	// missed an event — a skipped AddNode is a node that never becomes a
	// task, so the gap must trigger a full reconcile, never a shrug.
	Seq     uint64
	Change  GraphChange
	Success bool
	Error   error
}

// graphEventBufferSize is the channel buffer size per subscriber.
const graphEventBufferSize = 64

// GraphEventHub provides pub/sub for graph change events.
type GraphEventHub struct {
	mu          sync.RWMutex
	subscribers map[string]chan GraphEvent
	// dropped counts, per subscriber, the published events the subscriber
	// missed because its buffer was full. Guarded by mu, alongside seq: a drop
	// with no counter and no log would be a silent divergence — a lost AddNode
	// is a node that never becomes a task — so every drop is counted loudly.
	dropped map[string]uint64
	nextID  int
	seq     uint64
}

// NewGraphEventHub creates a GraphEventHub.
func NewGraphEventHub() *GraphEventHub {
	return &GraphEventHub{
		subscribers: make(map[string]chan GraphEvent),
		dropped:     make(map[string]uint64),
	}
}

// Subscribe returns a read-only event channel and a subscription ID.
func (h *GraphEventHub) Subscribe() (string, <-chan GraphEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.nextID++
	id := fmt.Sprintf("sub-%d", h.nextID)
	ch := make(chan GraphEvent, graphEventBufferSize)
	h.subscribers[id] = ch

	return id, ch
}

// Unsubscribe removes a subscriber and closes its channel.
func (h *GraphEventHub) Unsubscribe(id string) {
	h.mu.Lock()
	ch, exists := h.subscribers[id]
	if exists {
		delete(h.subscribers, id)
		// Subscription ids are never reused, so a counter left behind here
		// would be one dead map entry per subscribe/unsubscribe cycle for the
		// life of the hub — unbounded on a long-lived graph.
		delete(h.dropped, id)
	}
	h.mu.Unlock()

	if exists {
		close(ch)
	}
}

// Publish sends an event to all subscribers. Delivery is non-blocking: a
// subscriber whose buffer is full misses the event and its drop counter
// increments (see Dropped). A miss is never silent — the counter plus the Seq
// gap on the next delivered event tell the subscriber to reconcile.
func (h *GraphEventHub) Publish(event GraphEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.seq++
	event.Seq = h.seq
	for id, ch := range h.subscribers {
		select {
		case ch <- event:
		default:
			h.dropped[id]++
		}
	}
}

// Dropped returns how many published events the subscriber missed because its
// buffer was full. Monotonic per subscriber; consumers poll it to catch drops
// at the tail of a burst, where no later event arrives to reveal the Seq gap.
func (h *GraphEventHub) Dropped(id string) uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.dropped[id]
}

// SubscriberCount returns the number of active subscribers.
func (h *GraphEventHub) SubscriberCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.subscribers)
}
