package proto

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
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
		conn:    conn,
		onEvent: onEvent,
		pending: make(map[uint32]chan *Response),
		closed:  make(chan struct{}),
	}
	go c.readLoop()
	return c
}

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
	c.mu.Unlock()

	frame, err := EncodeJSON(t, id, action, data)
	if err == nil {
		var buf []byte
		buf, err = Encode(frame)
		if err == nil {
			_, err = c.conn.Write(buf)
		}
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

func (c *Client) readLoop() {
	for {
		frame, err := Decode(c.conn)
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
