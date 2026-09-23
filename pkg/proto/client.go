package proto

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrClosed is returned by calls made after the client was closed.
	ErrClosed = errors.New("proto: client is closed")
	// ErrPendingClosed means the connection dropped while a request was
	// waiting for its response.
	ErrPendingClosed = errors.New("proto: connection closed with pending requests")
)

// Response is the decoded reply to a Call.
type Response struct {
	Action string
	Data   []byte
	Err    *ErrorMessage
}

// OK reports whether the server processed the request successfully.
func (r *Response) OK() bool { return r.Err == nil }

// EventHandler receives frames the client did not issue a request for,
// i.e. PUBLISH pushes from the server.
type EventHandler func(frame *Frame)

// Client is the client-side counterpart of ProtocolHandler. A single
// Client multiplexes concurrent requests over one connection, matching
// responses to requests by stream id, and dispatches server pushes to the
// registered EventHandler. It is safe for concurrent use.
type Client struct {
	conn    net.Conn
	onEvent EventHandler

	mu      sync.Mutex
	nextID  uint32
	pending map[uint32]chan *Response

	// streamThreshold is the payload size above which outbound requests
	// are fragmented; fragments of one request are written under the
	// lock, so concurrent large calls cannot interleave on the wire.
	streamThreshold int

	version atomic.Int32 // protocol version agreed in Handshake, 0 if none

	closed   chan struct{}
	closedMu sync.Once
}

// Dial connects to addr and starts the read loop.
func Dial(addr string, onEvent EventHandler) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewClient(conn, onEvent), nil
}

// NewClient wraps an established connection. It takes ownership of conn.
func NewClient(conn net.Conn, onEvent EventHandler) *Client {
	c := &Client{
		conn:            conn,
		onEvent:         onEvent,
		pending:         make(map[uint32]chan *Response),
		streamThreshold: defaultStreamThreshold,
		closed:          make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// HandshakeResult is what the two sides agreed on during Handshake.
type HandshakeResult struct {
	Version  int
	Features []string
	// Token identifies the session server-side. Keep it across
	// reconnects and pass it to HandshakeWith to have the server restore
	// the subscriptions of the dropped connection.
	Token string
	// Resumed reports that the presented token was recognized and the
	// old subscriptions are live on this connection again.
	Resumed bool
}

// Handshake negotiates the protocol version and opens a fresh session.
func (c *Client) Handshake(timeout time.Duration) (*HandshakeResult, error) {
	return c.HandshakeWith("", nil, timeout)
}

// HandshakeWith negotiates the protocol version; on a reconnect, passing
// the Token of the dropped connection also restores its subscriptions, and
// the per-topic Cursors (last seen publish sequence numbers) make the
// server replay the publishes missed while disconnected. On success the
// result is remembered and NegotiatedVersion reports the version; on
// rejection the client closes itself, since frames the server cannot
// interpret are pointless. Handshaking is optional — the server serves
// un-negotiated connections exactly as before.
func (c *Client) HandshakeWith(session string, cursors map[string]uint64, timeout time.Duration) (*HandshakeResult, error) {
	req := &HelloRequest{Versions: []int{ProtocolVersion}, Cursors: cursors}
	if session != "" {
		req.Resume = session
	}
	resp, err := c.roundTrip(HELLO, RESPONSE, "hello", req, timeout)
	if err != nil {
		return nil, err
	}
	if !resp.OK() {
		_ = c.Close()
		return nil, fmt.Errorf("proto: handshake rejected: %s", resp.Err.Error)
	}
	var agreed HelloResponse
	if err := json.Unmarshal(resp.Data, &agreed); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("proto: malformed handshake response: %w", err)
	}
	c.version.Store(int32(agreed.Version))
	return &HandshakeResult{
		Version:  agreed.Version,
		Features: agreed.Features,
		Token:    agreed.Token,
		Resumed:  agreed.Resumed,
	}, nil
}

// NegotiatedVersion reports the protocol version agreed in Handshake, or
// 0 when the connection never negotiated.
func (c *Client) NegotiatedVersion() int { return int(c.version.Load()) }

// Call sends a request and waits for the matching response. A zero timeout
// means wait indefinitely; a timed-out call's late response is discarded.
func (c *Client) Call(action string, data any, timeout time.Duration) (*Response, error) {
	return c.roundTrip(REQUEST, RESPONSE, action, data, timeout)
}

// Subscribe registers interest in a topic and waits for the confirmation.
func (c *Client) Subscribe(topic string, timeout time.Duration) error {
	resp, err := c.roundTrip(SUBSCRIBE, RESPONSE, topic, nil, timeout)
	if err != nil {
		return err
	}
	if !resp.OK() {
		return fmt.Errorf("proto: subscribe %q rejected: %s", topic, resp.Err.Error)
	}
	return nil
}

// Unsubscribe cancels a subscription.
func (c *Client) Unsubscribe(topic string, timeout time.Duration) error {
	resp, err := c.roundTrip(UNSUBSCRIBE, RESPONSE, topic, nil, timeout)
	if err != nil {
		return err
	}
	if !resp.OK() {
		return fmt.Errorf("proto: unsubscribe %q rejected: %s", topic, resp.Err.Error)
	}
	return nil
}

// Ping sends a heartbeat and waits for the pong.
func (c *Client) Ping(timeout time.Duration) error {
	resp, err := c.roundTrip(PING, PONG, "ping", nil, timeout)
	if err != nil {
		return err
	}
	if !resp.OK() {
		return fmt.Errorf("proto: ping rejected: %s", resp.Err.Error)
	}
	return nil
}

// KeepAlive starts a goroutine that pings the server every interval, so an
// otherwise quiet connection survives server-side idle timeouts and a dead
// link is detected within roughly one interval plus one ping timeout. When
// a ping fails the connection is assumed dead: the client closes itself and
// waiters observe it through Done. The returned stop function ends the
// loop; closing the client stops it too. Interval must be positive.
func (c *Client) KeepAlive(interval time.Duration, pingTimeout time.Duration) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.closed:
				return
			case <-done:
				return
			case <-ticker.C:
				if err := c.Ping(pingTimeout); err != nil {
					// unreachable or timed-out server: fail loudly by
					// closing, which wakes every caller of Done
					_ = c.Close()
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// CloseGracefully tells the server goodbye with a CLOSE frame, waits for
// the acknowledgement and shuts the connection down. On any failure it
// falls back to a plain Close. It is safe to call on an already closed
// client.
func (c *Client) CloseGracefully(timeout time.Duration) error {
	select {
	case <-c.closed:
		return nil
	default:
	}
	if _, err := c.roundTrip(CLOSE, RESPONSE, "bye", nil, timeout); err != nil {
		return c.Close()
	}
	return c.Close()
}

// roundTrip registers a pending response channel, writes the frame and
// waits for the matching response. Allocating the id and the channel is one
// critical section so concurrent callers cannot cross wires. Responses and
// pongs are matched back by stream id in the read loop.
func (c *Client) roundTrip(t FrameType, respType FrameType, action string, data any, timeout time.Duration) (*Response, error) {
	select {
	case <-c.closed:
		return nil, ErrClosed
	default:
	}

	c.mu.Lock()
	c.nextID++
	id := c.nextID
	ch := make(chan *Response, 1)
	c.pending[id] = ch

	frame, err := EncodeJSON(t, id, action, data)
	var buffers [][]byte
	if err == nil {
		buffers, err = encodeStreamFrames(t, id, frame.Payload, c.streamThreshold)
	}
	if err != nil {
		c.mu.Unlock()
		c.removePending(id)
		return nil, err
	}
	if len(buffers) == 1 {
		// the common case: writing outside the lock keeps a slow socket
		// from stalling other callers that only want to register
		c.mu.Unlock()
		_, err = c.conn.Write(buffers[0])
	} else {
		// fragments of one message must reach the wire back to back, so
		// concurrent callers cannot interleave their own frames between
		// them; that is worth holding the lock through the writes
		_, err = writeAll(c.conn, buffers)
		c.mu.Unlock()
	}
	if err != nil {
		c.removePending(id)
		return nil, err
	}

	if timeout <= 0 {
		resp, ok := <-ch
		if !ok {
			return nil, ErrPendingClosed
		}
		return resp, nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, ErrPendingClosed
		}
		return resp, nil
	case <-timer.C:
		c.removePending(id)
		return nil, fmt.Errorf("proto: %s %q timed out after %s", t, action, timeout)
	}
}

func (c *Client) removePending(id uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, id)
}

// writeAll writes every buffer in order under one lock hold.
func writeAll(conn net.Conn, buffers [][]byte) (int, error) {
	total := 0
	for _, buf := range buffers {
		n, err := conn.Write(buf)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (c *Client) readLoop() {
	for {
		// fragments are assembled here, mirroring the server's codec, so
		// large responses arrive as one logical frame
		frame, err := DecodeStreamed(c.conn)
		if err != nil {
			c.failPending()
			c.Close()
			return
		}
		if frame.Header.FrameType == RESPONSE || frame.Header.FrameType == PONG {
			c.resolvePending(frame)
			continue
		}
		if c.onEvent != nil {
			c.onEvent(frame)
		}
	}
}

func (c *Client) resolvePending(frame *Frame) {
	c.mu.Lock()
	ch, ok := c.pending[frame.Header.StreamId]
	delete(c.pending, frame.Header.StreamId)
	c.mu.Unlock()
	if !ok {
		return // late response to a timed-out call
	}
	msg, err := DecodeJSONMessage(frame)
	if err != nil {
		ch <- &Response{Err: &ErrorMessage{Error: err.Error()}}
		return
	}
	resp := &Response{Action: msg.Action}
	if len(msg.Data) > 0 {
		var probe ErrorMessage
		if json.Unmarshal(msg.Data, &probe) == nil && probe.Error != "" {
			resp.Err = &probe
		} else {
			resp.Data = msg.Data
		}
	}
	ch <- resp
}

// failPending closes every waiting channel; callers blocked on them observe
// ErrPendingClosed.
func (c *Client) failPending() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
}

// Close shuts the connection down. Pending requests fail with
// ErrPendingClosed; Close is idempotent.
func (c *Client) Close() error {
	var err error
	c.closedMu.Do(func() {
		close(c.closed)
		err = c.conn.Close()
	})
	return err
}

// Done reports when the client has been closed or the connection dropped.
func (c *Client) Done() <-chan struct{} { return c.closed }
