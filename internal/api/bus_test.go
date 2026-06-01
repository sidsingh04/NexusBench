package api_test

// bus_test.go — unit tests for LeaderboardBus (Stage 5.7).
//
// These tests exercise the three behaviors mandated by TASK.md:
//  1. TestLeaderboardBus_Broadcast          — all subscribers receive the event.
//  2. TestLeaderboardBus_SlowSubscriberDropped — full channel does not block Broadcast.
//  3. TestLeaderboardBus_UnsubscribeCleans  — unsubscribed channel receives nothing.

import (
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/api"
	"github.com/nexusbench/nexusbench/internal/contest"
	"github.com/nexusbench/nexusbench/internal/models"
)

// newBusEvent is a test helper that builds a contest.LeaderboardEvent.
func newBusEvent(eventType, contestID string) contest.LeaderboardEvent {
	return contest.LeaderboardEvent{
		Type:      eventType,
		ContestID: contestID,
		Entries: []*models.LeaderboardEntry{
			{Rank: 1, TeamName: "alpha", FinalScore: 92.5},
		},
	}
}

// TestLeaderboardBus_Broadcast verifies that two subscribers both receive
// the broadcasted event with the correct payload.
func TestLeaderboardBus_Broadcast(t *testing.T) {
	t.Parallel()

	bus := api.NewLeaderboardBus()

	_, ch1 := bus.ExportSubscribe()
	_, ch2 := bus.ExportSubscribe()

	event := newBusEvent("update", "contest-abc")
	bus.Broadcast(event)

	// Both subscribers must receive the event within a short deadline.
	deadline := time.After(500 * time.Millisecond)

	for i, ch := range []<-chan api.LeaderboardEvent{ch1, ch2} {
		select {
		case got := <-ch:
			if got.Type != "update" {
				t.Errorf("subscriber %d: Type = %q, want %q", i+1, got.Type, "update")
			}
			if got.ContestID != "contest-abc" {
				t.Errorf("subscriber %d: ContestID = %q, want %q", i+1, got.ContestID, "contest-abc")
			}
			if len(got.Entries) != 1 || got.Entries[0].TeamName != "alpha" {
				t.Errorf("subscriber %d: entries not propagated correctly", i+1)
			}
		case <-deadline:
			t.Errorf("subscriber %d: timed out waiting for event", i+1)
		}
	}
}

// TestLeaderboardBus_SlowSubscriberDropped verifies that a subscriber whose
// channel is full does not block Broadcast — the event is dropped for that
// subscriber while other subscribers are unaffected.
func TestLeaderboardBus_SlowSubscriberDropped(t *testing.T) {
	t.Parallel()

	bus := api.NewLeaderboardBus()

	// slow: never reads — will fill up and start dropping.
	_, _ = bus.ExportSubscribe()

	// fast: will read every event.
	_, fastCh := bus.ExportSubscribe()

	// Flood the bus with enough events to overflow the slow subscriber's buffer.
	for i := 0; i < 10; i++ {
		bus.Broadcast(newBusEvent("update", "c1"))
	}

	// Broadcast must return without hanging (checked implicitly by the test
	// completing before the deadline below).

	// The fast subscriber must have received at least one event.
	deadline := time.After(500 * time.Millisecond)
	received := false
	for !received {
		select {
		case <-fastCh:
			received = true
		case <-deadline:
			t.Error("fast subscriber did not receive any events — Broadcast may have blocked")
			return
		}
	}
}

// TestLeaderboardBus_UnsubscribeCleans verifies that after unsubscribing, a
// subscriber's channel is closed and receives no further events.
func TestLeaderboardBus_UnsubscribeCleans(t *testing.T) {
	t.Parallel()

	bus := api.NewLeaderboardBus()

	id, ch := bus.ExportSubscribe()

	// Unsubscribe before any event is broadcast.
	bus.ExportUnsubscribe(id)

	// The channel must be closed (readable with zero value, ok=false).
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after unsubscribe, but received a real value")
		}
		// ok=false → channel closed. Correct.
	case <-time.After(100 * time.Millisecond):
		t.Error("channel was not closed after unsubscribe")
	}

	// Broadcasting after unsubscribe must not panic or deliver to the closed channel.
	// (If unsubscribe removes the entry from the map, Broadcast will never touch
	// the closed channel again — this is the invariant we're testing.)
	bus.Broadcast(newBusEvent("update", "c2")) // must not panic
}

// TestLeaderboardBus_SubscriberCount verifies the count tracks subscribe/unsubscribe.
func TestLeaderboardBus_SubscriberCount(t *testing.T) {
	t.Parallel()

	bus := api.NewLeaderboardBus()
	if n := bus.SubscriberCount(); n != 0 {
		t.Errorf("initial count = %d, want 0", n)
	}

	id1, _ := bus.ExportSubscribe()
	id2, _ := bus.ExportSubscribe()
	if n := bus.SubscriberCount(); n != 2 {
		t.Errorf("after 2 subscribes count = %d, want 2", n)
	}

	bus.ExportUnsubscribe(id1)
	if n := bus.SubscriberCount(); n != 1 {
		t.Errorf("after 1 unsubscribe count = %d, want 1", n)
	}

	bus.ExportUnsubscribe(id2)
	if n := bus.SubscriberCount(); n != 0 {
		t.Errorf("after 2 unsubscribes count = %d, want 0", n)
	}
}
