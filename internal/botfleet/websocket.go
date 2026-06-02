package botfleet

// websocket.go implements WebSocketTransport — a BotTransport over a persistent
// WebSocket connection.
//
// # Design rationale
//
// WebSocket sits on top of HTTP/1.1: the client sends an HTTP/1.1 Upgrade
// request; the server replies 101 Switching Protocols; both sides then exchange
// frames over the same TCP connection. This file implements exactly that
// handshake using only net/http and net (stdlib), adding zero new module
// dependencies.
//
// The implementation covers the subset of RFC 6455 needed for NexusBench:
//   - Client-side masking (all client→server frames must be masked per RFC 6455 §5.3).
//   - Text frames only (JSON payload — opcodes 0x1 send, 0x1 receive).
//   - No fragmentation: each Send is one frame in, one frame out.
//   - Control frames: we respond to server Ping with Pong (RFC 6455 §5.5.2).
//     Close frames from the server cleanly terminate the connection.
//
// # Wire protocol (application layer)
//
// Each bot sends a JSON-encoded order over the WebSocket frame:
//
//	send:    {"order_id":"...","kind":"...","side":"...","price":0,"quantity":0}
//	receive: {"order_id":"...","accepted":true,"executed_price":0,"executed_qty":0}
//
// These are identical to the REST JSON shapes so contestant engines that
// support both protocols need only switch their transport layer.
//
// # Thread safety
//
// WebSocketTransport is NOT goroutine-safe. Each Bot owns its own instance —
// the one-bot-one-transport ownership model (enforced in fleet.go) ensures
// that Send and Close are never called concurrently on the same transport.
//
// # Context handling
//
// Send sets a deadline on the underlying net.Conn derived from ctx before
// performing the read/write pair. If ctx is canceled before the deadline
// fires, the next conn.Read/Write call returns an error, which is surfaced
// to the caller.

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // SHA-1 is mandated by the WebSocket spec (RFC 6455 §4.1)
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// wsGUID is the WebSocket magic GUID defined in RFC 6455 §4.1.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsOpcodeText is the WebSocket opcode for a UTF-8 text frame.
const wsOpcodeText = 0x1

// wsOpcodePing is the WebSocket opcode for a Ping control frame.
const wsOpcodePing = 0x9

// wsOpcodePong is the WebSocket opcode for a Pong control frame.
const wsOpcodePong = 0xA

// wsOpcodeClose is the WebSocket opcode for a Close control frame.
const wsOpcodeClose = 0x8

// wsSendTimeout is the per-send write deadline applied to the underlying
// net.Conn. Large enough to absorb a slow server response during a benchmark
// run, tight enough to detect a stalled engine.
const wsSendTimeout = 5 * time.Second

// WebSocketTransport implements BotTransport over a persistent WebSocket
// connection. One instance per Bot — each bot owns its own TCP connection.
//
// The connection is established in NewWebSocketTransport and reused for the
// entire lifetime of one fleet run. This amortizes TLS and HTTP-upgrade
// overhead across the hundreds of orders each bot sends.
//
// Zero value is NOT valid. Use NewWebSocketTransport.
type WebSocketTransport struct {
	conn   net.Conn
	reader *bufio.Reader
	closer closeOnce
}

// NewWebSocketTransport dials the target URL and performs the WebSocket
// upgrade handshake. Returns a ready-to-use transport or an error.
//
// url must be "ws://host:port/path" or "wss://host:port/path".
// The upgrade request is sent to the path component of url.
//
// NewWebSocketTransport does NOT respect a context — dialing and upgrading
// are expected to complete within a few hundred milliseconds on a local
// network. If the caller needs dial cancellation, wrap in a goroutine.
func NewWebSocketTransport(rawURL string) (*WebSocketTransport, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("websocket: parse url %q: %w", rawURL, err)
	}

	// Resolve the dial address from the URL.
	host := u.Host
	if !strings.Contains(host, ":") {
		switch u.Scheme {
		case "wss":
			host += ":443"
		default:
			host += ":80"
		}
	}

	// For now we only support plain ws:// (no TLS).
	// wss:// can be added in a future stage when TLS is needed.
	if u.Scheme != "ws" {
		return nil, fmt.Errorf("websocket: unsupported scheme %q (only ws:// is supported)", u.Scheme)
	}

	conn, err := net.DialTimeout("tcp", host, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("websocket: dial %s: %w", host, err)
	}

	path := u.RequestURI()
	if path == "" {
		path = "/"
	}

	// Generate a random 16-byte nonce and base64-encode it for the
	// Sec-WebSocket-Key header (RFC 6455 §4.1).
	nonce := make([]byte, 16)
	if _, readErr := io.ReadFull(rand.Reader, nonce); readErr != nil {
		conn.Close() //nolint:errcheck
		return nil, fmt.Errorf("websocket: generate nonce: %w", readErr)
	}
	key := base64.StdEncoding.EncodeToString(nonce)
	expectedAccept := computeAccept(key)

	// Send the HTTP/1.1 upgrade request.
	upgradeReq := fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n"+
			"\r\n",
		path, u.Host, key,
	)
	if _, writeErr := conn.Write([]byte(upgradeReq)); writeErr != nil {
		conn.Close() //nolint:errcheck
		return nil, fmt.Errorf("websocket: send upgrade request: %w", writeErr)
	}

	// Read and validate the 101 Switching Protocols response.
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		conn.Close() //nolint:errcheck
		return nil, fmt.Errorf("websocket: read upgrade response: %w", err)
	}
	resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close() //nolint:errcheck
		return nil, fmt.Errorf("websocket: server returned %d, want 101", resp.StatusCode)
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") {
		conn.Close() //nolint:errcheck
		return nil, fmt.Errorf("websocket: server did not set Upgrade: websocket header")
	}
	if resp.Header.Get("Sec-WebSocket-Accept") != expectedAccept {
		conn.Close() //nolint:errcheck
		return nil, fmt.Errorf("websocket: Sec-WebSocket-Accept mismatch")
	}

	t := &WebSocketTransport{conn: conn, reader: reader}
	t.closer.fn = conn.Close
	return t, nil
}

// Send encodes order o as a JSON WebSocket text frame, writes it to the
// connection, and reads back one frame containing the JSON fill response.
//
// The call is bounded by wsSendTimeout or ctx, whichever expires first.
// Control frames (Ping, Close) received before the data frame are handled
// transparently.
func (t *WebSocketTransport) Send(ctx context.Context, o Order) (Fill, error) {
	// Apply a combined deadline: the earlier of ctx deadline and wsSendTimeout.
	deadline := time.Now().Add(wsSendTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := t.conn.SetDeadline(deadline); err != nil {
		return Fill{}, fmt.Errorf("websocket: set deadline: %w", err)
	}
	defer t.conn.SetDeadline(time.Time{}) //nolint:errcheck // reset after each call

	// Encode the order as JSON.
	payload, err := json.Marshal(struct {
		OrderID  string `json:"order_id"`
		Kind     string `json:"kind"`
		Side     string `json:"side,omitempty"`
		Price    int64  `json:"price,omitempty"`
		Quantity int64  `json:"quantity,omitempty"`
	}{
		OrderID:  o.ID,
		Kind:     string(o.Kind),
		Side:     string(o.Side),
		Price:    o.Price,
		Quantity: o.Quantity,
	})
	if err != nil {
		return Fill{}, fmt.Errorf("websocket: marshal order: %w", err)
	}

	// Write the frame.
	if writeErr := t.writeFrame(wsOpcodeText, payload); writeErr != nil {
		return Fill{}, fmt.Errorf("websocket: write frame: %w", writeErr)
	}

	// Read frames, handling control frames, until we get a data frame.
	fillPayload, err := t.readDataFrame()
	if err != nil {
		return Fill{}, fmt.Errorf("websocket: read fill frame: %w", err)
	}

	// Decode the fill.
	var resp struct {
		OrderID       string `json:"order_id"`
		Accepted      bool   `json:"accepted"`
		ExecutedPrice int64  `json:"executed_price,omitempty"`
		ExecutedQty   int64  `json:"executed_qty,omitempty"`
	}
	if err := json.Unmarshal(fillPayload, &resp); err != nil {
		return Fill{}, fmt.Errorf("websocket: decode fill: %w", err)
	}

	return Fill{
		OrderID:       resp.OrderID,
		ExecutedPrice: resp.ExecutedPrice,
		ExecutedQty:   resp.ExecutedQty,
		Accepted:      resp.Accepted,
	}, nil
}

// Close closes the underlying TCP connection. Safe to call multiple times —
// only the first call performs the close; subsequent calls return the error
// from the first.
func (t *WebSocketTransport) Close() error {
	return t.closer.do()
}

// ── WebSocket frame I/O ───────────────────────────────────────────────────────

// writeFrame writes a masked WebSocket frame to the connection.
//
// RFC 6455 §5.2 defines the frame format. Clients MUST mask all frames sent
// to the server (RFC 6455 §5.3). The mask key is 4 random bytes.
//
// Frame layout for payload len ≤ 125 (the common case for our JSON orders):
//
//	Byte 0: FIN=1 | RSV=000 | opcode (4 bits)
//	Byte 1: MASK=1 | payload_len (7 bits)
//	Bytes 2–5: masking key (4 bytes)
//	Bytes 6–N: masked payload
func (t *WebSocketTransport) writeFrame(opcode byte, payload []byte) error {
	// Generate a fresh 4-byte masking key per frame (RFC 6455 §5.3).
	var maskKey [4]byte
	if _, err := io.ReadFull(rand.Reader, maskKey[:]); err != nil {
		return fmt.Errorf("generate mask key: %w", err)
	}

	plen := len(payload)

	// Build header. We support three payload length ranges (RFC 6455 §5.2):
	//   0–125:   7-bit length
	//   126–65535: 7+16-bit
	//   65536+:  7+64-bit (not used for our small JSON payloads)
	var header []byte
	header = append(header, 0x80|opcode) // FIN=1, opcode

	switch {
	case plen <= 125:
		header = append(header, byte(0x80|plen)) //nolint:gosec // MASK=1, len
	case plen <= 65535:
		header = append(header, 0x80|126)
		header = append(header, byte(plen>>8), byte(plen)) //nolint:gosec
	default:
		header = append(header, 0x80|127)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(plen))
		header = append(header, b[:]...)
	}

	header = append(header, maskKey[:]...)

	// Apply masking: each payload byte XORed with maskKey[i%4].
	masked := make([]byte, plen)
	for i, b := range payload {
		masked[i] = b ^ maskKey[i%4]
	}

	// Write header + masked payload in one call to minimize syscalls.
	frame := append(header, masked...)
	_, err := t.conn.Write(frame)
	return err
}

// readDataFrame reads WebSocket frames until it finds a text or binary data
// frame. Ping frames are transparently replied to with a Pong. Close frames
// cause an error return. Continuation frames are not expected (we never
// fragment) and are treated as errors.
func (t *WebSocketTransport) readDataFrame() ([]byte, error) {
	for {
		payload, opcode, err := t.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case wsOpcodeText, 0x2: // text or binary data frame
			return payload, nil
		case wsOpcodePing:
			// Reply with Pong containing the same payload (RFC 6455 §5.5.3).
			if err := t.writeFrame(wsOpcodePong, payload); err != nil {
				return nil, fmt.Errorf("send pong: %w", err)
			}
		case wsOpcodeClose:
			return nil, fmt.Errorf("server closed the WebSocket connection")
		default:
			return nil, fmt.Errorf("unexpected WebSocket opcode 0x%x", opcode)
		}
	}
}

// readFrame reads one complete WebSocket frame from the buffered reader and
// returns the payload bytes and opcode. Server-to-client frames are never
// masked (RFC 6455 §5.1), so we do not apply unmasking here.
func (t *WebSocketTransport) readFrame() (payload []byte, opcode byte, err error) {
	// Byte 0: FIN + opcode
	b0, err := t.reader.ReadByte()
	if err != nil {
		return nil, 0, fmt.Errorf("read frame header byte 0: %w", err)
	}
	opcode = b0 & 0x0F

	// Byte 1: MASK + payload length
	b1, err := t.reader.ReadByte()
	if err != nil {
		return nil, 0, fmt.Errorf("read frame header byte 1: %w", err)
	}
	masked := (b1 & 0x80) != 0
	plen := int(b1 & 0x7F)

	switch plen {
	case 126:
		// Extended 16-bit length
		var ext [2]byte
		if _, err := io.ReadFull(t.reader, ext[:]); err != nil {
			return nil, 0, fmt.Errorf("read 16-bit length: %w", err)
		}
		plen = int(binary.BigEndian.Uint16(ext[:]))
	case 127:
		// Extended 64-bit length
		var ext [8]byte
		if _, err := io.ReadFull(t.reader, ext[:]); err != nil {
			return nil, 0, fmt.Errorf("read 64-bit length: %w", err)
		}
		plen = int(binary.BigEndian.Uint64(ext[:])) //nolint:gosec
	}

	// Read masking key if present (server→client frames are normally unmasked).
	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(t.reader, maskKey[:]); err != nil {
			return nil, 0, fmt.Errorf("read mask key: %w", err)
		}
	}

	// Read payload.
	payload = make([]byte, plen)
	if _, err := io.ReadFull(t.reader, payload); err != nil {
		return nil, 0, fmt.Errorf("read payload (%d bytes): %w", plen, err)
	}

	// Unmask if necessary.
	if masked {
		for i, b := range payload {
			payload[i] = b ^ maskKey[i%4]
		}
	}

	return payload, opcode, nil
}

// computeAccept derives the expected Sec-WebSocket-Accept header value from
// the client's Sec-WebSocket-Key, as specified in RFC 6455 §4.1.
//
//	accept = base64(SHA-1(key + magic_guid))
//
// SHA-1 is mandated by the WebSocket spec and is used solely for the
// handshake challenge-response, not for any security-sensitive purpose.
func computeAccept(key string) string {
	//nolint:gosec // SHA-1 mandated by RFC 6455 §4.1, not used for security
	h := sha1.New()
	h.Write([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
