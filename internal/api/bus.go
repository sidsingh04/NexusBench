package api

// bus.go implements the leaderboard fan-out broadcaster for SSE.
//
// Design:
//   - LeaderboardBus is the concrete implementation of
//     contest.LeaderboardBroadcaster. It is created once in cmd/server/main.go
//     and passed to both NewContestService (so Close fires "frozen") and
//     NewRouter (so the SSE handler can subscribe to it). The executor also
//     receives it so each completed FinalScore fires "update".
//
//   - Subscriber channels are buffered (size 4). A full channel is dropped
//     silently — a slow SSE client must not block the worker that is
//     broadcasting the result.
//
//   - All exported methods are safe for concurrent use. The internal map is
//     protected by a sync.RWMutex; writes take a full write-lock, reads use
//     a read-lock.
//
//   - LeaderboardEvent is defined here (in the api package) rather than in
//     contest so the JSON tags are co-located with the HTTP layer. The contest
//     package uses its own contest.LeaderboardEvent type; Broadcast converts
//     between them. This is intentional: api types are shaped for wire output;
//     contest types are shaped for domain logic.

import (
	"sync"

	"github.com/google/uuid"
	"github.com/nexusbench/nexusbench/internal/contest"
	"github.com/nexusbench/nexusbench/internal/models"
)

// LeaderboardEvent is the wire type written to SSE subscribers.
// It is distinct from contest.LeaderboardEvent so JSON tags can be
// independently shaped without coupling the domain layer to HTTP output.
type LeaderboardEvent struct {
	// Type is "update" (a score changed) or "frozen" (contest is closed).
	Type string `json:"type"`

	// ContestID identifies which contest this event belongs to.
	ContestID string `json:"contest_id"`

	// Entries is the full ranked leaderboard at the time of the event.
	Entries []*models.LeaderboardEntry `json:"entries"`
}

// subscriberChanSize is the depth of each subscriber's channel.
// Four slots give a slow SSE client four events of buffer before drops begin.
const subscriberChanSize = 4

// LeaderboardBus is a fan-out broadcaster for LeaderboardEvents.
//
// It implements contest.LeaderboardBroadcaster (the Broadcast method satisfies
// the interface signature), so it can be passed directly to
// contest.NewContestService without any adapter.
//
// The zero value is NOT valid. Use NewLeaderboardBus.
type LeaderboardBus struct {
	mu   sync.RWMutex
	subs map[string]chan LeaderboardEvent // key = subscriber UUID
}

// NewLeaderboardBus returns an initialized, ready-to-use LeaderboardBus.
func NewLeaderboardBus() *LeaderboardBus {
	return &LeaderboardBus{
		subs: make(map[string]chan LeaderboardEvent),
	}
}

// Broadcast converts a contest.LeaderboardEvent to the wire LeaderboardEvent
// and delivers it to every subscriber channel.
//
// Delivery is non-blocking per subscriber: if a channel is full, that
// subscriber is skipped silently. The broadcaster is never stalled by a
// slow client.
//
// Broadcast is safe to call from multiple goroutines concurrently (e.g.
// simultaneously from the worker goroutine and the contest auto-close
// goroutine).
func (b *LeaderboardBus) Broadcast(event contest.LeaderboardEvent) {
	wire := LeaderboardEvent{
		Type:      event.Type,
		ContestID: event.ContestID,
		Entries:   event.Entries,
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, ch := range b.subs {
		select {
		case ch <- wire:
		default:
			// Channel full — drop this event for this subscriber.
			// The subscriber will still receive subsequent events.
		}
	}
}

// subscribe registers a new subscriber and returns its UUID and receive channel.
// The channel is closed by unsubscribe; callers must call unsubscribe when
// the client disconnects to avoid goroutine and memory leaks.
func (b *LeaderboardBus) subscribe() (id string, ch <-chan LeaderboardEvent) {
	return b.doSubscribe()
}

// unsubscribe removes the subscriber and closes its channel so the SSE
// handler's select loop terminates cleanly.
func (b *LeaderboardBus) unsubscribe(id string) {
	b.doUnsubscribe(id)
}

// doSubscribe is the shared implementation used by both subscribe (internal)
// and ExportSubscribe (test-only exported wrapper).
func (b *LeaderboardBus) doSubscribe() (id string, ch <-chan LeaderboardEvent) {
	id = uuid.New().String()
	c := make(chan LeaderboardEvent, subscriberChanSize)

	b.mu.Lock()
	b.subs[id] = c
	b.mu.Unlock()

	return id, c
}

// doUnsubscribe is the shared implementation used by both unsubscribe (internal)
// and ExportUnsubscribe (test-only exported wrapper).
func (b *LeaderboardBus) doUnsubscribe(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ch, ok := b.subs[id]; ok {
		delete(b.subs, id)
		close(ch)
	}
}

// SubscriberCount returns the number of active SSE subscribers.
// Exported for observability (tests, metrics, health check).
func (b *LeaderboardBus) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

// ── Test-only exported wrappers ───────────────────────────────────────────────
// These allow bus_test.go (package api_test) to call subscribe/unsubscribe
// without the unexported method restriction. They are thin wrappers with no
// production callers; real code uses subscribe/unsubscribe (lowercase) from
// within the package.

// ExportSubscribe is a test-only exported wrapper for subscribe.
// Do not call from production code.
func (b *LeaderboardBus) ExportSubscribe() (id string, ch <-chan LeaderboardEvent) {
	return b.doSubscribe()
}

// ExportUnsubscribe is a test-only exported wrapper for unsubscribe.
// Do not call from production code.
func (b *LeaderboardBus) ExportUnsubscribe(id string) {
	b.doUnsubscribe(id)
}
