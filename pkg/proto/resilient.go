package proto

import (
	"errors"
	"sync"
	"time"
)

// ErrNotConnected is returned by calls made before Connect.
var ErrNotConnected = errors.New("proto: resilient client not connected (call Connect first)")

// ResilientOptions tunes the reconnection behaviour. The zero value is
// usable; nil options mean the zero value.
type ResilientOptions struct {
	// BackoffStart is the first wait after a failed reconnect attempt.
	BackoffStart time.Duration // default 100ms
	// BackoffMax caps the exponentially growing wait between attempts.
	BackoffMax time.Duration // default 5s
	// HandshakeTimeout bounds Dial + HELLO on every attempt, and the
	// re-subscribes after a session could not be resumed.
	HandshakeTimeout time.Duration // default 5s
}

func (o *ResilientOptions) backoffStart() time.Duration {
	if o.BackoffStart > 0 {
		return o.BackoffStart
	}
	return 100 * time.Millisecond
}

func (o *ResilientOptions) backoffMax() time.Duration {
	if o.BackoffMax > 0 {
		return o.BackoffMax
	}
	return 5 * time.Second
}

func (o *ResilientOptions) handshakeTimeout() time.Duration {
	if o.HandshakeTimeout > 0 {
		return o.HandshakeTimeout
	}
	return 5 * time.Second
}

// ResilientClient wraps Client with automatic reconnection. When the
// connection drops it dials again with exponential backoff and resumes the
// session via the handshake token, so topics subscribed before the drop
// stay subscribed without client action. If the session expired
// server-side (Resumed == false), the recorded subscriptions are re-sent.
//
// Reconnection happens in the background; Call and friends report the
// error of the current connection and it is up to the caller whether to
// retry them. Re-subscribing after a lost session is best-effort: a topic
// that fails is retried on the next reconnect. It is safe for concurrent
// use.
type ResilientClient struct {
	addr    string
	onEvent EventHandler
	opts    *ResilientOptions

	mu         sync.Mutex
	client     *Client
	token      string
	subscribed map[string]struct{}
	cursors    map[string]uint64 // topic -> last seen publish sequence
	closed     bool
	done       chan struct{}
	closeOnce  sync.Once
}

// recordCursor notes the sequence number of a publish as the replay cursor
// for its topic. Called for every publish before the user handler sees it,
// so a panicking or slow handler cannot lose the update.
func (rc *ResilientClient) recordCursor(frame *Frame) {
	if frame.Header.FrameType != PUBLISH {
		return
	}
	msg, err := DecodeJSONMessage(frame)
	if err != nil {
		return
	}
	rc.mu.Lock()
	rc.cursors[msg.Action] = uint64(frame.Header.StreamId)
	rc.mu.Unlock()
}

// snapshotCursors copies the cursor table for the resume handshake.
func (rc *ResilientClient) snapshotCursors() map[string]uint64 {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if len(rc.cursors) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(rc.cursors))
	for topic, seq := range rc.cursors {
		out[topic] = seq
	}
	return out
}

// NewResilientClient creates the client; it does not connect until Connect
// is called. opts may be nil.
func NewResilientClient(addr string, onEvent EventHandler, opts *ResilientOptions) *ResilientClient {
	if opts == nil {
		opts = &ResilientOptions{}
	}
	rc := &ResilientClient{
		addr:       addr,
		opts:       opts,
		subscribed: make(map[string]struct{}),
		cursors:    make(map[string]uint64),
		done:       make(chan struct{}),
	}
	// every publish updates the replay cursor before the user handler
	// runs, so reconnects know exactly where the client left off
	rc.onEvent = func(frame *Frame) {
		rc.recordCursor(frame)
		if onEvent != nil {
			onEvent(frame)
		}
	}
	return rc
}

// Connect dials and handshakes, blocking until the client is ready. It is
// idempotent: later calls on a connected or closed client return nil or
// ErrClosed respectively.
func (rc *ResilientClient) Connect(timeout time.Duration) error {
	rc.mu.Lock()
	switch {
	case rc.closed:
		rc.mu.Unlock()
		return ErrClosed
	case rc.client != nil:
		rc.mu.Unlock()
		return nil
	}
	rc.mu.Unlock()

	c, _, err := rc.dialAndHandshake(timeout)
	if err != nil {
		return err
	}
	rc.mu.Lock()
	rc.client = c
	rc.mu.Unlock()
	go rc.watch()
	return nil
}

// Call sends a request over the current connection, waiting up to timeout
// for the response. ErrNotConnected is returned before Connect; a dropped
// connection surfaces ErrClosed or ErrPendingClosed while the background
// loop reconnects.
func (rc *ResilientClient) Call(action string, data any, timeout time.Duration) (*Response, error) {
	c, err := rc.current()
	if err != nil {
		return nil, err
	}
	return c.Call(action, data, timeout)
}

// Subscribe registers a topic. The subscription is remembered and survives
// reconnections: the server restores it via the session token, or the
// client re-sends it when the session could not be resumed.
func (rc *ResilientClient) Subscribe(topic string, timeout time.Duration) error {
	c, err := rc.current()
	if err != nil {
		return err
	}
	if err := c.Subscribe(topic, timeout); err != nil {
		return err
	}
	rc.mu.Lock()
	rc.subscribed[topic] = struct{}{}
	rc.mu.Unlock()
	return nil
}

// Unsubscribe cancels a topic and forgets it.
func (rc *ResilientClient) Unsubscribe(topic string, timeout time.Duration) error {
	c, err := rc.current()
	if err != nil {
		return err
	}
	if err := c.Unsubscribe(topic, timeout); err != nil {
		return err
	}
	rc.mu.Lock()
	delete(rc.subscribed, topic)
	rc.mu.Unlock()
	return nil
}

// Ping probes the current connection.
func (rc *ResilientClient) Ping(timeout time.Duration) error {
	c, err := rc.current()
	if err != nil {
		return err
	}
	return c.Ping(timeout)
}

// CloseGracefully says goodbye on the current connection, stops the
// reconnect loop and closes everything. It falls back to a plain close on
// any failure.
func (rc *ResilientClient) CloseGracefully(timeout time.Duration) error {
	rc.mu.Lock()
	c := rc.client
	rc.mu.Unlock()
	if c != nil {
		_ = c.CloseGracefully(timeout)
	}
	rc.Close()
	return nil
}

// Close stops the reconnect loop and closes the current connection.
func (rc *ResilientClient) Close() error {
	rc.mu.Lock()
	rc.closed = true
	c := rc.client
	rc.mu.Unlock()
	rc.closeOnce.Do(func() { close(rc.done) })
	if c != nil {
		return c.Close()
	}
	return nil
}

// Done reports when the resilient client was closed; connection drops do
// not close it, they are recovered from.
func (rc *ResilientClient) Done() <-chan struct{} { return rc.done }

// current returns the live client for API calls.
func (rc *ResilientClient) current() (*Client, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	switch {
	case rc.closed:
		return nil, ErrClosed
	case rc.client == nil:
		return nil, ErrNotConnected
	}
	return rc.client, nil
}

// dialAndHandshake opens a connection and resumes the remembered session,
// reporting the replay cursors so missed publishes are sent again.
func (rc *ResilientClient) dialAndHandshake(timeout time.Duration) (*Client, *HandshakeResult, error) {
	c, err := Dial(rc.addr, rc.onEvent)
	if err != nil {
		return nil, nil, err
	}
	rc.mu.Lock()
	token := rc.token
	rc.mu.Unlock()
	res, err := c.HandshakeWith(token, rc.snapshotCursors(), timeout)
	if err != nil {
		_ = c.Close() // do not leak the connection on a rejected handshake
		return nil, nil, err
	}
	return c, res, nil
}

// watch reconnects with backoff for as long as the client lives. Every
// drop is recovered from — Done stays open until Close.
func (rc *ResilientClient) watch() {
	for {
		rc.mu.Lock()
		c := rc.client
		closed := rc.closed
		rc.mu.Unlock()
		if closed || c == nil {
			return
		}
		select {
		case <-rc.done:
			return
		case <-c.Done():
		}
		if !rc.reconnectLoop() {
			return
		}
	}
}

// reconnectLoop keeps dialing until it succeeds (returns true) or the
// client is closed (returns false).
func (rc *ResilientClient) reconnectLoop() bool {
	backoff := rc.opts.backoffStart()
	max := rc.opts.backoffMax()
	for {
		select {
		case <-rc.done:
			return false
		default:
		}
		c, res, err := rc.dialAndHandshake(rc.opts.handshakeTimeout())
		if err == nil {
			rc.swap(c, res)
			return true
		}
		select {
		case <-rc.done:
			return false
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > max {
			backoff = max
		}
	}
}

// swap installs the reconnected client, remembers its session token and,
// when the old session could not be resumed, re-sends the recorded
// subscriptions.
func (rc *ResilientClient) swap(c *Client, res *HandshakeResult) {
	rc.mu.Lock()
	old := rc.client
	rc.client = c
	rc.token = res.Token
	resubscribe := !res.Resumed && !rc.closed
	topics := make([]string, 0, len(rc.subscribed))
	if resubscribe {
		for topic := range rc.subscribed {
			topics = append(topics, topic)
		}
	}
	rc.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	if resubscribe {
		// best-effort: topics that fail stay recorded and are retried on
		// the next reconnect
		for _, topic := range topics {
			_ = c.Subscribe(topic, rc.opts.handshakeTimeout())
		}
	}
}
