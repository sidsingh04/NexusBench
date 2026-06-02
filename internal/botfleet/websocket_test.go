package botfleet_test

// websocket_test.go — unit tests for WebSocketTransport (Stage 5.8).
//
// All four tests use a minimal httptest server that speaks the WebSocket
// upgrade protocol and the NexusBench JSON order/fill wire format.
// No external libraries are needed — the server uses only net/http and
// net (stdlib), mirroring the implementation in websocket.go.
//
// Tests:
//   TestWebSocketTransport_SendReceive          — happy path: order sent, fill decoded correctly
//   TestWebSocketTransport_ContextCancellation  — canceled ctx causes Send to return an error
//   TestWebSocketTransport_Close_Idempotent     — Close() twice does not panic or return a new error
//   TestRESTTransport_CloseIsNoop               — RESTTransport.Close() always returns nil

import (
	"bufio"
	"context"
	"crypto/sha1" //nolint:gosec // SHA-1 mandated by RFC 6455 §4.1
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/botfleet"
)

// ── WebSocket test server helpers ─────────────────────────────────────────────

const testWsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// serverComputeAccept computes the Sec-WebSocket-Accept value for the given key.
func serverComputeAccept(key string) string {
	//nolint:gosec
	h := sha1.New()
	h.Write([]byte(key + testWsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// wsTestServer starts a TCP listener that accepts one WebSocket connection,
// performs the upgrade handshake, then invokes handler with the live conn.
// Returns the ws:// URL and a cleanup function.
func wsTestServer(t *testing.T, handler func(conn net.Conn, reader *bufio.Reader)) (string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("wsTestServer: listen: %v", err)
	}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed
		}
		defer conn.Close() //nolint:errcheck

		reader := bufio.NewReader(conn)

		// Read the HTTP upgrade request.
		req, err := http.ReadRequest(reader)
		if err != nil {
			return
		}
		key := req.Header.Get("Sec-WebSocket-Key")

		// Write the 101 Switching Protocols response.
		resp := fmt.Sprintf(
			"HTTP/1.1 101 Switching Protocols\r\n"+
				"Upgrade: websocket\r\n"+
				"Connection: Upgrade\r\n"+
				"Sec-WebSocket-Accept: %s\r\n"+
				"\r\n",
			serverComputeAccept(key),
		)
		if _, err := conn.Write([]byte(resp)); err != nil {
			return
		}

		handler(conn, reader)
	}()

	addr := ln.Addr().String()
	wsURL := "ws://" + addr + "/orders"
	return wsURL, func() { _ = ln.Close() }
}

// readClientFrame reads one masked WebSocket frame from the client and
// returns the unmasked payload and opcode.
func readClientFrame(reader *bufio.Reader) (payload []byte, opcode byte, err error) {
	b0, err := reader.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	opcode = b0 & 0x0F

	b1, err := reader.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	masked := (b1 & 0x80) != 0
	plen := int(b1 & 0x7F)

	switch plen {
	case 126:
		var ext [2]byte
		if _, err := bufReadFull(reader, ext[:]); err != nil {
			return nil, 0, err
		}
		plen = int(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := bufReadFull(reader, ext[:]); err != nil {
			return nil, 0, err
		}
		plen = int(binary.BigEndian.Uint64(ext[:])) //nolint:gosec
	}

	var maskKey [4]byte
	if masked {
		if _, err := bufReadFull(reader, maskKey[:]); err != nil {
			return nil, 0, err
		}
	}

	payload = make([]byte, plen)
	if _, err := bufReadFull(reader, payload); err != nil {
		return nil, 0, err
	}

	if masked {
		for i, b := range payload {
			payload[i] = b ^ maskKey[i%4]
		}
	}

	return payload, opcode, nil
}

// writeServerFrame writes an unmasked WebSocket text frame to conn.
// Server-to-client frames are never masked (RFC 6455 §5.1).
func writeServerFrame(conn net.Conn, payload []byte) error {
	plen := len(payload)
	var header []byte
	header = append(header, 0x81) // FIN=1, opcode=text(1)
	if plen <= 125 {
		header = append(header, byte(plen))
	} else if plen <= 65535 {
		header = append(header, 126, byte(plen>>8), byte(plen)) //nolint:gosec
	} else {
		header = append(header, 127)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(plen))
		header = append(header, b[:]...)
	}
	_, err := conn.Write(append(header, payload...))
	return err
}

// bufReadFull reads exactly len(buf) bytes from reader.
func bufReadFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// orderEchoServer starts a ws server that reads one JSON order frame and
// replies with an accepted fill (executed_price=10000, executed_qty=5).
func orderEchoServer(t *testing.T) (wsURL string, cleanup func()) {
	t.Helper()
	return wsTestServer(t, func(conn net.Conn, reader *bufio.Reader) {
		payload, _, err := readClientFrame(reader)
		if err != nil {
			return
		}

		var req struct {
			OrderID string `json:"order_id"`
		}
		if err := json.Unmarshal(payload, &req); err != nil {
			return
		}

		resp, _ := json.Marshal(map[string]any{
			"order_id":       req.OrderID,
			"accepted":       true,
			"executed_price": int64(10000),
			"executed_qty":   int64(5),
		})
		_ = writeServerFrame(conn, resp)
	})
}

// stallServer starts a ws server that completes the upgrade but never
// responds to any frames. Used to test context cancellation.
func stallServer(t *testing.T) (wsURL string, cleanup func()) {
	t.Helper()
	return wsTestServer(t, func(conn net.Conn, reader *bufio.Reader) {
		// Stall until the client closes the connection.
		buf := make([]byte, 1)
		for {
			conn.SetDeadline(time.Now().Add(10 * time.Second)) //nolint:errcheck
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	})
}

// ── Tests ──────────────────────────────────────────────────────────────────────

// TestWebSocketTransport_SendReceive verifies the happy path:
// a limit-buy order is sent over WebSocket and the fill is decoded correctly.
func TestWebSocketTransport_SendReceive(t *testing.T) {
	t.Parallel()

	wsURL, cleanup := orderEchoServer(t)
	defer cleanup()

	transport, err := botfleet.NewWebSocketTransport(wsURL)
	if err != nil {
		t.Fatalf("NewWebSocketTransport: %v", err)
	}
	defer transport.Close() //nolint:errcheck

	order := botfleet.Order{
		ID:       "ws-order-001",
		Kind:     botfleet.KindLimit,
		Side:     botfleet.SideBuy,
		Price:    10000,
		Quantity: 5,
	}

	fill, err := transport.Send(context.Background(), order)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	if fill.OrderID != order.ID {
		t.Errorf("fill.OrderID = %q, want %q", fill.OrderID, order.ID)
	}
	if !fill.Accepted {
		t.Error("fill.Accepted = false, want true")
	}
	if fill.ExecutedPrice != 10000 {
		t.Errorf("fill.ExecutedPrice = %d, want 10000", fill.ExecutedPrice)
	}
	if fill.ExecutedQty != 5 {
		t.Errorf("fill.ExecutedQty = %d, want 5", fill.ExecutedQty)
	}
}

// TestWebSocketTransport_ContextCancellation verifies that a context that
// times out before the server responds causes Send to return a non-nil error.
func TestWebSocketTransport_ContextCancellation(t *testing.T) {
	t.Parallel()

	wsURL, cleanup := stallServer(t)
	defer cleanup()

	transport, err := botfleet.NewWebSocketTransport(wsURL)
	if err != nil {
		t.Fatalf("NewWebSocketTransport: %v", err)
	}
	defer transport.Close() //nolint:errcheck

	// Context expires well before the stall server would ever respond.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	order := botfleet.Order{
		ID:   "ctx-cancel-ws",
		Kind: botfleet.KindLimit,
		Side: botfleet.SideBuy,
	}

	_, err = transport.Send(ctx, order)
	if err == nil {
		t.Error("Send should have returned an error when context was canceled")
	}
}

// TestWebSocketTransport_Close_Idempotent verifies that calling Close() twice
// does not panic and returns a consistent error (same value both times).
func TestWebSocketTransport_Close_Idempotent(t *testing.T) {
	t.Parallel()

	wsURL, cleanup := orderEchoServer(t)
	defer cleanup()

	transport, err := botfleet.NewWebSocketTransport(wsURL)
	if err != nil {
		t.Fatalf("NewWebSocketTransport: %v", err)
	}

	err1 := transport.Close()
	err2 := transport.Close()

	// The second call must return the same error as the first — closeOnce
	// stores the result from the first invocation.
	if err1 != err2 {
		t.Errorf("Close() returned different errors on repeated calls: first=%v second=%v", err1, err2)
	}
}

// TestRESTTransport_CloseIsNoop verifies that RESTTransport.Close() always
// returns nil, and that the transport remains fully usable after Close().
//
// This is the backward-compatibility guarantee: any code path that calls
// transport.Close() after upgrading to the new BotTransport interface must
// not break existing REST-based submissions.
func TestRESTTransport_CloseIsNoop(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		orderID, _ := req["order_id"].(string)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"order_id": orderID,
			"accepted": true,
		})
	}))
	defer srv.Close()

	transport := botfleet.NewRESTTransport(srv.URL, &http.Client{Timeout: time.Second})

	// First Close must return nil.
	if err := transport.Close(); err != nil {
		t.Errorf("RESTTransport.Close() = %v, want nil", err)
	}

	// Transport must still work after Close (HTTP has no connection state per transport).
	order := botfleet.Order{
		ID:   "post-close",
		Kind: botfleet.KindLimit,
		Side: botfleet.SideBuy,
	}
	fill, err := transport.Send(context.Background(), order)
	if err != nil {
		t.Errorf("Send after Close: %v", err)
	}
	if fill.OrderID != order.ID {
		t.Errorf("fill.OrderID = %q, want %q", fill.OrderID, order.ID)
	}

	// Second Close must also return nil (idempotent).
	if err := transport.Close(); err != nil {
		t.Errorf("RESTTransport.Close() second call = %v, want nil", err)
	}
}
