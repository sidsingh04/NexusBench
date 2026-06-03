package botfleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// BotTransport is the interface between a Bot and the contestant's engine.
//
// Each Bot owns its own BotTransport instance. Implementations are not
// required to be goroutine-safe — concurrent access is prevented by the
// one-bot-one-transport ownership model.
//
// Stage 5.8 adds Close() to allow WebSocket connections (which are persistent)
// to be cleanly torn down after a fleet run. REST implementations return nil
// immediately — this is a backward-compatible extension because the Fleet
// calls Close() via Bot.Close() after Run returns, and the existing call site
// in fleet.go is the only caller.
type BotTransport interface {
	// Send transmits o to the engine and returns the Fill response.
	// Returns an error for any transport failure (timeout, connection refused,
	// non-2xx status, malformed response).
	// Send must respect ctx cancellation.
	Send(ctx context.Context, o Order) (Fill, error)

	// Close releases any persistent resources held by this transport.
	// REST implementations return nil immediately (HTTP is connection-pooled
	// by the http.Client and does not require explicit teardown per order).
	// WebSocket implementations close the underlying TCP connection.
	// Safe to call multiple times — subsequent calls after the first are no-ops.
	Close() error
}

// ── RESTTransport ─────────────────────────────────────────────────────────────

// restOrderRequest is the JSON body sent to the contestant's REST endpoint.
type restOrderRequest struct {
	OrderID  string `json:"order_id"`
	Kind     string `json:"kind"`
	Side     string `json:"side,omitempty"`
	Price    int64  `json:"price,omitempty"`
	Quantity int64  `json:"quantity,omitempty"`
}

// restOrderResponse is the JSON body the contestant's engine must return.
type restOrderResponse struct {
	OrderID       string `json:"order_id"`
	Accepted      bool   `json:"accepted"`
	ExecutedPrice int64  `json:"executed_price,omitempty"`
	ExecutedQty   int64  `json:"executed_qty,omitempty"`
}

// RESTTransport implements BotTransport over HTTP/JSON.
//
//	POST   /orders → submit a new Limit or Market order
//	DELETE /orders → submit a cancel
//
// The zero value is NOT valid. Use NewRESTTransport.
type RESTTransport struct {
	baseURL string
	client  *http.Client
}

// NewRESTTransport constructs a RESTTransport targeting the given base URL
// (e.g. "http://localhost:20001"). Pass nil to use a default 5-second client.
func NewRESTTransport(baseURL string, client *http.Client) *RESTTransport {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &RESTTransport{baseURL: baseURL, client: client}
}

// Send serializes o to JSON, POSTs it to the engine, and decodes the fill.
func (t *RESTTransport) Send(ctx context.Context, o Order) (Fill, error) {
	reqBody := restOrderRequest{
		OrderID:  o.ID,
		Kind:     string(o.Kind),
		Side:     string(o.Side),
		Price:    o.Price,
		Quantity: o.Quantity,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return Fill{}, fmt.Errorf("rest: marshal order: %w", err)
	}

	method := http.MethodPost
	if o.Kind == KindCancel {
		method = http.MethodDelete
	}

	req, err := http.NewRequestWithContext(ctx, method, t.baseURL+"/orders", bytes.NewReader(body))
	if err != nil {
		return Fill{}, fmt.Errorf("rest: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		log.Printf("REST client error to %s: %v", req.URL.String(), err)
		return Fill{}, fmt.Errorf("rest: do request: %w", err)
	}
	defer resp.Body.Close()

	rawBody, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return Fill{}, fmt.Errorf("rest: read response: %w", err)
	}

	var respData restOrderResponse
	if err := json.Unmarshal(rawBody, &respData); err != nil {
		log.Printf("REST decode error: status=%d, body=%s, err=%v", resp.StatusCode, string(rawBody), err)
		return Fill{}, fmt.Errorf("rest: decode response (status %d): %w", resp.StatusCode, err)
	}

	return Fill{
		OrderID:       respData.OrderID,
		ExecutedPrice: respData.ExecutedPrice,
		ExecutedQty:   respData.ExecutedQty,
		Accepted:      respData.Accepted,
	}, nil
}

// Close is a no-op for RESTTransport. The underlying http.Client manages its
// connection pool automatically; no explicit teardown is required per transport
// instance.
func (t *RESTTransport) Close() error { return nil }

// ── Bot ───────────────────────────────────────────────────────────────────────

// Bot is a single virtual trading participant. It runs a tight loop:
// generate order → send → record result → repeat until ctx is canceled.
//
// Each Bot owns its OrderGenerator and BotTransport — no sharing, no locking.
// The zero value is NOT valid. Use NewBot.
type Bot struct {
	id        string
	gen       OrderGenerator
	transport BotTransport
}

// NewBot constructs a Bot. gen and transport must not be nil.
func NewBot(id string, gen OrderGenerator, transport BotTransport) (*Bot, error) {
	if gen == nil {
		return nil, fmt.Errorf("botfleet: Bot %q: generator must not be nil", id)
	}
	if transport == nil {
		return nil, fmt.Errorf("botfleet: Bot %q: transport must not be nil", id)
	}
	return &Bot{id: id, gen: gen, transport: transport}, nil
}

// Run sends orders in a tight loop until ctx is canceled, collecting each
// OrderResult. It returns all results accumulated during this run.
//
// Run blocks until ctx.Done() is closed. The caller (Fleet) must provide a
// context with a deadline that matches the desired test duration.
//
// The returned slice may contain results with non-nil Err (transport errors).
// These are included so the stats layer can compute an error rate.
func (b *Bot) Run(ctx context.Context) []OrderResult {
	var results []OrderResult

	for {
		select {
		case <-ctx.Done():
			return results
		default:
		}

		o := b.gen.Next()

		sent := time.Now()
		fill, err := b.transport.Send(ctx, o)
		acked := time.Now()

		r := OrderResult{
			Order:  o,
			Fill:   fill,
			SentAt: sent,
		}

		if err != nil {
			r.Err = err
		} else {
			r.AckedAt = acked
			r.LatencyNs = acked.Sub(sent).Nanoseconds()
		}

		results = append(results, r)
	}
}

// Close tears down the bot's transport. Called by Fleet after Run returns.
// It is safe to call Close concurrently with other goroutines — the transport
// implementation is responsible for its own close-once semantics.
func (b *Bot) Close() error {
	return b.transport.Close()
}

// ── closeOnce ─────────────────────────────────────────────────────────────────

// closeOnce wraps any BotTransport and ensures Close() is idempotent.
// Embedded in WebSocketTransport so WebSocket-specific close logic doesn't
// need to repeat the sync.Once pattern.
type closeOnce struct {
	once sync.Once
	fn   func() error
	err  error
}

func (c *closeOnce) do() error {
	c.once.Do(func() { c.err = c.fn() })
	return c.err
}
