// nolint: errcheck // Test code may ignore return values
package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGraphEventHub_NewGraphEventHub verifies that the constructor returns a
// non-nil hub.
func TestGraphEventHub_NewGraphEventHub(t *testing.T) {
	hub := NewGraphEventHub()
	require.NotNil(t, hub, "NewGraphEventHub should return a non-nil hub")
	assert.NotNil(t, hub.subscribers, "subscribers map should be initialized")
	assert.Equal(t, 0, hub.SubscriberCount(), "new hub should have zero subscribers")
}

// TestGraphEventHub_Subscribe_ReturnsValidIDAndChannel verifies that Subscribe
// returns a unique ID and a non-nil receive-only channel.
func TestGraphEventHub_Subscribe_ReturnsValidIDAndChannel(t *testing.T) {
	hub := NewGraphEventHub()

	id1, ch1 := hub.Subscribe()
	id2, ch2 := hub.Subscribe()

	assert.NotEmpty(t, id1, "first subscription ID should not be empty")
	assert.NotEmpty(t, id2, "second subscription ID should not be empty")
	assert.NotEqual(t, id1, id2, "subscription IDs should be unique")
	assert.NotNil(t, ch1, "first channel should not be nil")
	assert.NotNil(t, ch2, "second channel should not be nil")
	assert.Equal(t, 2, hub.SubscriberCount())
}

// TestGraphEventHub_Unsubscribe_ClosesChannel verifies that Unsubscribe
// removes the subscriber and closes its channel.
func TestGraphEventHub_Unsubscribe_ClosesChannel(t *testing.T) {
	hub := NewGraphEventHub()

	id, ch := hub.Subscribe()
	assert.Equal(t, 1, hub.SubscriberCount())

	hub.Unsubscribe(id)
	assert.Equal(t, 0, hub.SubscriberCount(),
		"subscriber count should be zero after unsubscribe")

	// The channel should be closed. Receiving from a closed channel returns
	// the zero value immediately.
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel should be closed")
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for closed channel")
	}
}

// TestGraphEventHub_Unsubscribe_NonExistent verifies that unsubscribing a
// non-existent ID is a no-op and does not panic.
func TestGraphEventHub_Unsubscribe_NonExistent(t *testing.T) {
	hub := NewGraphEventHub()

	// Should not panic.
	hub.Unsubscribe("non-existent-id")
	assert.Equal(t, 0, hub.SubscriberCount())
}

// TestGraphEventHub_Publish_DeliversToAllSubscribers verifies that a published
// event is received by every subscriber.
func TestGraphEventHub_Publish_DeliversToAllSubscribers(t *testing.T) {
	hub := NewGraphEventHub()

	const numSubs = 5
	channels := make([]<-chan GraphEvent, numSubs)
	for i := 0; i < numSubs; i++ {
		_, ch := hub.Subscribe()
		channels[i] = ch
	}

	event := GraphEvent{
		Change: GraphChange{
			Type:   ChangeAddNode,
			NodeID: "node-1",
		},
		Success: true,
	}

	hub.Publish(event)

	for i, ch := range channels {
		select {
		case received := <-ch:
			assert.Equal(t, event.Change.NodeID, received.Change.NodeID,
				"subscriber %d should receive the event", i)
			assert.True(t, received.Success)
		case <-time.After(1 * time.Second):
			t.Fatalf("subscriber %d did not receive event within timeout", i)
		}
	}
}

// TestGraphEventHub_Publish_NoSubscribers verifies that publishing to a hub
// with no subscribers does not panic.
func TestGraphEventHub_Publish_NoSubscribers(t *testing.T) {
	hub := NewGraphEventHub()

	event := GraphEvent{
		Change: GraphChange{
			Type:   ChangeRemoveNode,
			NodeID: "node-1",
		},
		Success: true,
	}

	// Should not panic.
	hub.Publish(event)
}

// TestGraphEventHub_Publish_NonBlocking verifies that Publish is non-blocking:
// events are dropped when a subscriber's buffer is full.
func TestGraphEventHub_Publish_NonBlocking(t *testing.T) {
	hub := NewGraphEventHub()

	// The buffer size is graphEventBufferSize (64). Fill the buffer, then
	// publish one more event -- it should be dropped without blocking.
	_, ch := hub.Subscribe()

	// Fill the buffer.
	for i := 0; i < graphEventBufferSize; i++ {
		hub.Publish(GraphEvent{
			Change:  GraphChange{NodeID: "fill"},
			Success: true,
		})
	}

	// Buffer is now full. The next publish should be non-blocking (event dropped).
	done := make(chan struct{})
	go func() {
		hub.Publish(GraphEvent{
			Change:  GraphChange{NodeID: "overflow"},
			Success: true,
		})
		close(done)
	}()

	select {
	case <-done:
		// Good: Publish returned immediately.
	case <-time.After(1 * time.Second):
		t.Fatal("Publish blocked when subscriber buffer is full")
	}

	// Drain the buffer and verify the overflow event was dropped.
	drained := 0
	for {
		select {
		case <-ch:
			drained++
		default:
			goto done
		}
	}
done:
	assert.Equal(t, graphEventBufferSize, drained,
		"should have drained exactly %d events (overflow was dropped)", graphEventBufferSize)
}

// TestGraphEventHub_SubscriberCount verifies the SubscriberCount method.
func TestGraphEventHub_SubscriberCount(t *testing.T) {
	hub := NewGraphEventHub()

	assert.Equal(t, 0, hub.SubscriberCount())

	id1, _ := hub.Subscribe()
	assert.Equal(t, 1, hub.SubscriberCount())

	id2, _ := hub.Subscribe()
	assert.Equal(t, 2, hub.SubscriberCount())

	hub.Unsubscribe(id1)
	assert.Equal(t, 1, hub.SubscriberCount())

	hub.Unsubscribe(id2)
	assert.Equal(t, 0, hub.SubscriberCount())
}

// TestGraphEventHub_ConcurrentSubscribeUnsubscribePublish verifies that
// concurrent Subscribe, Unsubscribe, and Publish operations are safe.
func TestGraphEventHub_ConcurrentSubscribeUnsubscribePublish(t *testing.T) {
	hub := NewGraphEventHub()

	var wg sync.WaitGroup

	// Concurrent subscribes.
	ids := make([]string, 50)
	chs := make([]<-chan GraphEvent, 50)
	var mu sync.Mutex

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id, ch := hub.Subscribe()
			mu.Lock()
			ids[idx] = id
			chs[idx] = ch
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 50, hub.SubscriberCount())

	// Concurrent publishes.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hub.Publish(GraphEvent{
				Change:  GraphChange{NodeID: "concurrent"},
				Success: true,
			})
		}()
	}

	// Concurrent unsubscribes (every other subscriber).
	for i := 0; i < 50; i += 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mu.Lock()
			id := ids[idx]
			mu.Unlock()
			hub.Unsubscribe(id)
		}(i)
	}

	wg.Wait()

	assert.Equal(t, 25, hub.SubscriberCount(),
		"should have 25 subscribers remaining after unsubscribing half")
}

// TestGraphEventHub_SubscribeReceivesMultipleEvents verifies that a subscriber
// can receive multiple sequential events.
func TestGraphEventHub_SubscribeReceivesMultipleEvents(t *testing.T) {
	hub := NewGraphEventHub()

	_, ch := hub.Subscribe()

	const eventCount = 10
	for i := 0; i < eventCount; i++ {
		hub.Publish(GraphEvent{
			Change:  GraphChange{NodeID: "node-1"},
			Success: i%2 == 0,
		})
	}

	received := 0
	for received < eventCount {
		select {
		case <-ch:
			received++
		case <-time.After(1 * time.Second):
			t.Fatalf("only received %d of %d events", received, eventCount)
		}
	}

	assert.Equal(t, eventCount, received)
}

// TestGraphChange_Type verifies that ChangeType constants are distinct.
func TestGraphChange_Type(t *testing.T) {
	types := []ChangeType{
		ChangeAddNode,
		ChangeRemoveNode,
		ChangeAddEdge,
		ChangeRemoveEdge,
	}

	seen := make(map[ChangeType]bool)
	for _, ct := range types {
		assert.False(t, seen[ct], "ChangeType %d should be unique", ct)
		seen[ct] = true
	}
}

// TestGraphEvent_ErrorField verifies that the Error field can carry an error.
func TestGraphEvent_ErrorField(t *testing.T) {
	event := GraphEvent{
		Change: GraphChange{
			Type:   ChangeAddNode,
			NodeID: "node-1",
		},
		Success: false,
		Error:   ErrCycleDetected,
	}

	assert.False(t, event.Success)
	assert.ErrorIs(t, event.Error, ErrCycleDetected)
}

// TestGraphEventHub_SeqAndDroppedCount pins hub-level gap accounting:
// published events carry hub-wide monotonic sequence numbers, and a
// subscriber that falls behind a burst loses events LOUDLY — the per-subscriber
// drop counter moves by exactly the overflow.
func TestGraphEventHub_SeqAndDroppedCount(t *testing.T) {
	hub := NewGraphEventHub()
	subID, ch := hub.Subscribe()
	defer hub.Unsubscribe(subID)

	// No reader runs during the burst, so every event past the buffer is a
	// deterministic drop — no timing involved.
	const burst = graphEventBufferSize + 20
	for i := 0; i < burst; i++ {
		hub.Publish(GraphEvent{Change: GraphChange{Type: ChangeAddNode, NodeID: "n"}, Success: true})
	}
	assert.Equal(t, uint64(burst-graphEventBufferSize), hub.Dropped(subID),
		"every event past the 64-buffer must be counted, never silently lost")

	var last uint64
	for i := 0; i < graphEventBufferSize; i++ {
		evt := <-ch
		require.Greater(t, evt.Seq, last, "delivered events keep monotonic sequence numbers")
		last = evt.Seq
	}
	assert.Equal(t, uint64(graphEventBufferSize), last, "the first 64 events are delivered in order")
	assert.Equal(t, uint64(0), hub.Dropped("sub-unknown"), "unknown subscriber drops nothing")
}

// TestGraphEventHub_UnsubscribeReclaimsDropCounter pins that the drop
// bookkeeping dies with its subscriber: subscription IDs are never reused, so
// a counter left behind on Unsubscribe would accumulate one dead entry per
// subscribe/unsubscribe cycle for the life of the hub.
func TestGraphEventHub_UnsubscribeReclaimsDropCounter(t *testing.T) {
	hub := NewGraphEventHub()
	subID, _ := hub.Subscribe()
	for i := 0; i < graphEventBufferSize+1; i++ {
		hub.Publish(GraphEvent{Change: GraphChange{Type: ChangeAddNode, NodeID: "n"}, Success: true})
	}
	require.Equal(t, uint64(1), hub.Dropped(subID), "precondition: the subscriber has a live drop counter")

	hub.Unsubscribe(subID)

	assert.Equal(t, uint64(0), hub.Dropped(subID), "the counter is reclaimed with the subscriber")
	assert.Empty(t, hub.dropped, "no bookkeeping outlives its subscriber")
}
