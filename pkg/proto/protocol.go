package proto

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// RequestHandler processes a request payload and produces the response
// value. Returning an error makes the server reply with an error response
// instead of aborting the connection.
type RequestHandler func(action string, data []byte) (any, error)

// ErrorMessage is the response body used when a request fails.
type ErrorMessage struct {
	Error string `json:"error"`
}

// ProtocolVersion is the current wire protocol version. It travels in the
// HELLO handshake; a peer speaking only older versions is told so and
// disconnects instead of trading frames neither side can interpret.
const ProtocolVersion = 1

// supportedVersions lists every version this server speaks. A new version
// is appended after the old one stays around, so negotiation can pick the
// best common denominator.
var supportedVersions = []int{ProtocolVersion}

// protocolFeatures names the capabilities behind this version, so a client
// can degrade gracefully instead of probing each one.
var protocolFeatures = []string{"pubsub", "keepalive", "graceful-close"}

// negotiate picks the highest version present in both lists; 0 means the
// sides share none.
func negotiate(clientVersions []int) int {
	best := 0
	for _, v := range clientVersions {
		for _, supported := range supportedVersions {
			if v == supported && v > best {
				best = v
			}
		}
	}
	return best
}

// HelloRequest is the HELLO payload: every version the client can speak,
// plus — on a reconnect — the session token it wants to resume and, per
// topic, the sequence number of the last publish it received so the
// server can replay what was missed.
type HelloRequest struct {
	Versions []int             `json:"versions"`
	Resume   string            `json:"resume,omitempty"`
	Cursors  map[string]uint64 `json:"cursors,omitempty"`
}

// HelloResponse is the handshake reply: the chosen version, what it
// includes, and the session token for resuming after a reconnect.
type HelloResponse struct {
	Version  int      `json:"version"`
	Features []string `json:"features"`
	Token    string   `json:"token,omitempty"`
	Resumed  bool     `json:"resumed,omitempty"`
}

// ProtocolHandler is the server-side semantic layer on top of the frame
// codec: it answers requests, keeps topic subscriptions and publishes
// messages to subscribers. It plugs into the handler pipeline as an inbound
// handler and writes responses back through the pipeline context.
type ProtocolHandler struct {
	handleRequest RequestHandler
	logger        *log.Logger

	mu    sync.RWMutex
	subs  map[string]map[net.Conn]struct{}
	pings atomic.Uint64
	pongs atomic.Uint64
	seq   atomic.Uint32

	// resumable sessions, see handleHello. connTokens is the reverse index
	// so a SUBSCRIBE frame can find the session to record the topic in
	// without scanning every session.
	sessions   map[string]*session
	connTokens map[net.Conn]string

	// topicCache remembers the most recent publishes per topic so a
	// resumed session can be caught up on what it missed while
	// disconnected. Entries exist only while the topic has subscribers.
	topicCache map[string][]cachedPublish

	closed     chan struct{}
	closeOnce  sync.Once
	sessionTTL time.Duration
}

// topicCacheSize bounds how many recent publishes are kept per topic for
// replaying to resumed sessions. Older ones are dropped: delivery stays
// best-effort, a client that was away for too long simply misses them.
const topicCacheSize = 64

// cachedPublish is one replayable publish: the wire-encoded frame (its
// StreamId doubles as the monotonically increasing sequence number) kept
// alongside the sequence for quick cursor comparisons.
type cachedPublish struct {
	seq uint32
	buf []byte
}

// session is the server-side memory of one logical connection. It outlives
// the transport: when a client reconnects and presents the token, its
// topics are moved onto the new connection.
type session struct {
	conn     net.Conn
	topics   map[string]struct{}
	lastSeen time.Time
}

// sessionTTL bounds how long a disconnected session is kept for resuming.
// It is a field on the handler (not a constant) so tests can shrink it.
const defaultSessionTTL = 10 * time.Minute

var _ handler.InboundHandler = (*ProtocolHandler)(nil)

func NewProtocolHandler(handleRequest RequestHandler) *ProtocolHandler {
	if handleRequest == nil {
		handleRequest = func(action string, data []byte) (any, error) {
			return nil, fmt.Errorf("no request handler registered for action %q", action)
		}
	}
	p := &ProtocolHandler{
		handleRequest: handleRequest,
		logger:        log.New(os.Stderr, "[proto]", log.LstdFlags),
		subs:          make(map[string]map[net.Conn]struct{}),
		sessions:      make(map[string]*session),
		connTokens:    make(map[net.Conn]string),
		topicCache:    make(map[string][]cachedPublish),
		closed:        make(chan struct{}),
		sessionTTL:    defaultSessionTTL,
	}
	go p.sessionJanitor()
	return p
}

// Close stops the background session janitor. The handler stays usable for
// connections that are already wired into a pipeline; closing it is only
// needed to release the janitor goroutine, e.g. in tests.
func (p *ProtocolHandler) Close() {
	p.closeOnce.Do(func() { close(p.closed) })
}

// sessionJanitor periodically drops sessions that were never resumed within
// the TTL, so tokens of clients that never came back do not accumulate.
func (p *ProtocolHandler) sessionJanitor() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-p.closed:
			return
		case <-ticker.C:
			p.mu.Lock()
			for token, s := range p.sessions {
				if time.Since(s.lastSeen) > p.sessionTTL {
					delete(p.sessions, token)
					if s.conn != nil {
						delete(p.connTokens, s.conn)
					}
				}
			}
			p.mu.Unlock()
		}
	}
}

// newSessionToken returns a random, unguessable session token.
func newSessionToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing means the system entropy source is broken;
		// an unpredictable token is a hardening feature, not a necessity
		return fmt.Sprintf("s-%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// HandleRead expects message to be a decoded *Frame produced by FrameCodec
// earlier in the pipeline. Any other message type is a protocol error and
// closes the connection.
func (p *ProtocolHandler) HandleRead(ctx handler.InboundContext, message handler.Message) {
	frame, ok := message.(*Frame)
	if !ok {
		ctx.Close(fmt.Errorf("proto: unexpected inbound message %T, want *Frame", message))
		return
	}
	switch frame.Header.FrameType {
	case HELLO:
		p.handleHello(ctx, frame)
	case REQUEST:
		p.handleRequestFrame(ctx, frame)
	case SUBSCRIBE:
		p.handleSubscribe(ctx, frame, true)
	case UNSUBSCRIBE:
		p.handleSubscribe(ctx, frame, false)
	case PING:
		p.pings.Add(1)
		p.reply(ctx, PONG, frame.Header.StreamId, "pong", nil)
	case PONG:
		p.pongs.Add(1)
	case CLOSE:
		// peer asked to finish: acknowledge with a RESPONSE carrying the
		// same stream id, then close from our side. ctx.Write is synchronous,
		// so the ack is on the wire before the connection drops.
		p.reply(ctx, RESPONSE, frame.Header.StreamId, "bye", nil)
		_ = ctx.Conn().Close()
	default:
		ctx.Close(fmt.Errorf("proto: unexpected frame type %s", frame.Header.FrameType))
	}
}

// handleHello answers the version negotiation and hands out session tokens.
// The negotiation itself stays stateless: the server either confirms the
// best common version or replies with an error response — it is then the
// client's job to disconnect, because no further frames could be
// interpreted anyway. A missing or empty versions list is answered the same
// way: the server cannot guess what a client that offers nothing can
// understand.
//
// Sessions are the one piece of per-connection state the server keeps. A
// fresh handshake mints a token; a handshake carrying a known token moves
// the recorded subscriptions onto the new connection, which is also what
// reaps the dead transport from the subscription table — no disconnect
// callback needed. Unknown or expired tokens start a fresh session.
func (p *ProtocolHandler) handleHello(ctx handler.InboundContext, frame *Frame) {
	msg, err := DecodeJSONMessage(frame)
	if err != nil {
		ctx.Close(fmt.Errorf("proto: malformed HELLO payload: %w", err))
		return
	}
	var req HelloRequest
	if len(msg.Data) > 0 {
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			ctx.Close(fmt.Errorf("proto: malformed HELLO payload: %w", err))
			return
		}
	}
	chosen := negotiate(req.Versions)
	if chosen == 0 {
		p.reply(ctx, RESPONSE, frame.Header.StreamId, "hello",
			&ErrorMessage{Error: fmt.Sprintf("no common protocol version (client offers %v, server speaks %v)", req.Versions, supportedVersions)})
		return
	}
	token, resumed, replay := p.attachSession(ctx.Conn(), req.Resume, req.Cursors)
	// replay BEFORE the ack: when the client's Handshake returns it can
	// rely on the catch-up already being on the wire, and replayed frames
	// never race live publishes written after the ack
	p.replayTo(ctx.Conn(), replay)
	p.reply(ctx, RESPONSE, frame.Header.StreamId, "hello", &HelloResponse{
		Version:  chosen,
		Features: protocolFeatures,
		Token:    token,
		Resumed:  resumed,
	})
}

// replayTo catches a just-resumed connection up on the publishes it missed,
// in sequence order. A failing write abandons the replay: the connection
// is presumably going away, and live publishes will reap it properly.
func (p *ProtocolHandler) replayTo(conn net.Conn, replay [][]byte) {
	for _, buf := range replay {
		_ = conn.SetWriteDeadline(time.Now().Add(publishWriteTimeout))
		if _, err := conn.Write(buf); err != nil {
			_ = conn.SetWriteDeadline(time.Time{})
			return
		}
		_ = conn.SetWriteDeadline(time.Time{})
	}
}

// attachSession binds conn to its session: it resumes the session named by
// the token (moving its recorded topics onto conn and dropping the dead
// transport), or mints a fresh session. The returned flag reports whether
// an existing session was resumed; replay carries the wire frames of the
// cached publishes the client has not seen yet, ordered by sequence.
func (p *ProtocolHandler) attachSession(conn net.Conn, token string, cursors map[string]uint64) (string, bool, [][]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if token != "" {
		if s, ok := p.sessions[token]; ok && time.Since(s.lastSeen) <= p.sessionTTL {
			// move the recorded topics onto the new transport: the dead
			// connection leaves every member set, conn takes its place
			for topic := range s.topics {
				members := p.subs[topic]
				if members == nil {
					members = make(map[net.Conn]struct{})
					p.subs[topic] = members
				}
				delete(members, s.conn)
				members[conn] = struct{}{}
			}
			delete(p.connTokens, s.conn)
			s.conn = conn
			s.lastSeen = time.Now()
			p.connTokens[conn] = token
			return token, true, p.collectReplay(s, cursors)
		}
		// unknown, expired or empty: fall through to a fresh session
	}

	token = newSessionToken()
	p.sessions[token] = &session{conn: conn, topics: make(map[string]struct{}), lastSeen: time.Now()}
	p.connTokens[conn] = token
	return token, false, nil
}

// collectReplay picks the cached publishes of the session's topics that
// are newer than the client's cursor, oldest first. Topics without a
// cursor get everything the cache holds; a topic whose cache no longer
// exists (e.g. all subscribers left) simply has nothing to replay.
func (p *ProtocolHandler) collectReplay(s *session, cursors map[string]uint64) [][]byte {
	var replay [][]byte
	for topic := range s.topics {
		cursor := cursors[topic] // missing entry means "nothing seen yet"
		for _, cached := range p.topicCache[topic] {
			if uint64(cached.seq) > cursor {
				replay = append(replay, cached.buf)
			}
		}
	}
	return replay
}

// rememberPublish stores a wire frame in the topic cache, evicting the
// oldest entry past the cache size.
func (p *ProtocolHandler) rememberPublish(topic string, seq uint32, buf []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cache := append(p.topicCache[topic], cachedPublish{seq: seq, buf: buf})
	if len(cache) > topicCacheSize {
		cache = cache[len(cache)-topicCacheSize:]
	}
	p.topicCache[topic] = cache
}

// dropTopicCacheLocked forgets a topic's replay cache once nothing needs
// it anymore: no live subscriber and no session — including disconnected
// ones that may resume and expect the catch-up — records the topic. This
// is what keeps the cache alive exactly while someone could still come
// back for it.
func (p *ProtocolHandler) dropTopicCacheLocked(topic string) {
	if members, ok := p.subs[topic]; ok && len(members) > 0 {
		return
	}
	if p.sessionRecordsTopicLocked(topic) {
		return
	}
	delete(p.topicCache, topic)
}

// sessionRecordsTopicLocked reports whether any session — including a
// disconnected one that may resume and expect the catch-up — still records
// the topic.
func (p *ProtocolHandler) sessionRecordsTopicLocked(topic string) bool {
	for _, s := range p.sessions {
		if _, ok := s.topics[topic]; ok {
			return true
		}
	}
	return false
}

func (p *ProtocolHandler) handleRequestFrame(ctx handler.InboundContext, frame *Frame) {
	msg, err := DecodeJSONMessage(frame)
	if err != nil {
		ctx.Close(fmt.Errorf("proto: malformed request payload: %w", err))
		return
	}
	streamId := frame.Header.StreamId
	resp, err := p.handleRequest(msg.Action, msg.Data)
	// responses carry the JSONMessage envelope too, so the client can read
	// action name and body uniformly whether the call succeeded or failed
	if err != nil {
		errBody, merr := json.Marshal(&ErrorMessage{Error: err.Error()})
		if merr != nil {
			ctx.Close(fmt.Errorf("proto: marshal error response: %w", merr))
			return
		}
		payload, merr := json.Marshal(&JSONMessage{Action: msg.Action, Data: errBody})
		if merr != nil {
			ctx.Close(fmt.Errorf("proto: marshal error envelope: %w", merr))
			return
		}
		ctx.Write(newFrame(RESPONSE, streamId, payload))
		return
	}
	body, err := json.Marshal(resp)
	if err != nil {
		ctx.Close(fmt.Errorf("proto: marshal response: %w", err))
		return
	}
	payload, err := json.Marshal(&JSONMessage{Action: msg.Action, Data: body})
	if err != nil {
		ctx.Close(fmt.Errorf("proto: marshal response envelope: %w", err))
		return
	}
	ctx.Write(newFrame(RESPONSE, streamId, payload))
}

func (p *ProtocolHandler) handleSubscribe(ctx handler.InboundContext, frame *Frame, subscribe bool) {
	topic, err := p.topicOf(frame)
	if err != nil {
		ctx.Close(err)
		return
	}
	p.mu.Lock()
	if subscribe {
		if p.subs[topic] == nil {
			p.subs[topic] = make(map[net.Conn]struct{})
		}
		p.subs[topic][ctx.Conn()] = struct{}{}
	} else if members, ok := p.subs[topic]; ok {
		delete(members, ctx.Conn())
		if len(members) == 0 {
			delete(p.subs, topic)
			p.dropTopicCacheLocked(topic)
		}
	}
	// record the change in the connection's session, so a reconnect with
	// the token can restore exactly these subscriptions
	if token, ok := p.connTokens[ctx.Conn()]; ok {
		if s := p.sessions[token]; s != nil {
			if subscribe {
				s.topics[topic] = struct{}{}
			} else {
				delete(s.topics, topic)
			}
			s.lastSeen = time.Now()
		}
	}
	p.mu.Unlock()

	verb, payload := "subscribed", map[string]string{"topic": topic}
	if !subscribe {
		verb, payload = "unsubscribed", map[string]string{"topic": topic}
	}
	p.reply(ctx, RESPONSE, frame.Header.StreamId, verb, payload)
}

// topicOf extracts the topic name carried in the frame's JSON action field.
func (p *ProtocolHandler) topicOf(frame *Frame) (string, error) {
	msg, err := DecodeJSONMessage(frame)
	if err != nil {
		return "", fmt.Errorf("proto: malformed %s payload: %w", frame.Header.FrameType, err)
	}
	if msg.Action == "" {
		return "", fmt.Errorf("proto: %s frame without topic", frame.Header.FrameType)
	}
	return msg.Action, nil
}

// publishWriteTimeout bounds a single subscriber write inside Publish. A
// subscriber that cannot keep up is dropped instead of stalling the
// publisher forever.
const publishWriteTimeout = 3 * time.Second

// Publish broadcasts payload to every subscriber of topic. Deliveries are
// best-effort: a subscriber whose write fails or times out is dropped from
// the topic. It returns the number of subscribers the message was written
// to.
func (p *ProtocolHandler) Publish(topic string, payload any) (int, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	p.mu.RLock()
	members := make([]net.Conn, 0, len(p.subs[topic]))
	for conn := range p.subs[topic] {
		members = append(members, conn)
	}
	// a disconnected session that still records the topic may resume and
	// ask for the catch-up, so its publishes must be cached even while
	// nobody is connected to receive them
	offline := p.sessionRecordsTopicLocked(topic)
	p.mu.RUnlock()
	if len(members) == 0 && !offline {
		return 0, nil
	}

	frame := newFrame(PUBLISH, p.seq.Add(1), mustJSON(topic, body))
	buf, err := Encode(frame)
	if err != nil {
		return 0, err
	}
	// the publish exists the moment it is encoded: cache it for replaying
	// to sessions that reconnect later, whether this round reaches
	// everyone or not
	p.rememberPublish(topic, frame.Header.StreamId, buf)
	delivered := 0
	for _, conn := range members {
		// transient deadline: cleared afterwards so normal response writes
		// are not affected
		_ = conn.SetWriteDeadline(time.Now().Add(publishWriteTimeout))
		_, err := conn.Write(buf)
		_ = conn.SetWriteDeadline(time.Time{})
		if err != nil {
			p.removeSubscriber(topic, conn)
			continue
		}
		delivered++
	}
	return delivered, nil
}

// mustJSON packs a topic and pre-encoded body into a JSONMessage payload.
func mustJSON(topic string, body []byte) []byte {
	payload, err := json.Marshal(&JSONMessage{Action: topic, Data: body})
	if err != nil {
		// body is already encoded JSON, so marshalling the envelope cannot
		// fail in practice; fall back to the topic alone
		return []byte(`{"action":` + quote(topic) + `}`)
	}
	return payload
}

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (p *ProtocolHandler) removeSubscriber(topic string, conn net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if members, ok := p.subs[topic]; ok {
		delete(members, conn)
		if len(members) == 0 {
			delete(p.subs, topic)
		}
	}
	// The session recording is deliberately NOT touched here: a write
	// failure means the transport died (or stalled), not that the client
	// stopped wanting the subscription. The session keeps the intent, and
	// a resume moves it onto the new transport — cleaning the dead one —
	// by itself. Only an explicit UNSUBSCRIBE removes the recording.
	p.dropTopicCacheLocked(topic)
}

// Subscribers returns the current subscriber count of a topic.
func (p *ProtocolHandler) Subscribers(topic string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.subs[topic])
}

func (p *ProtocolHandler) reply(ctx handler.InboundContext, t FrameType, streamId uint32, action string, data any) {
	frame, err := EncodeJSON(t, streamId, action, data)
	if err != nil {
		ctx.Close(fmt.Errorf("proto: encode %s: %w", t, err))
		return
	}
	ctx.Write(frame)
}

// Stats reports heartbeat counters, for tests and monitoring.
func (p *ProtocolHandler) Stats() (pings, pongs uint64) {
	return p.pings.Load(), p.pongs.Load()
}

// newFrame builds a frame with the JSON envelope payload.
func newFrame(t FrameType, streamId uint32, payload []byte) *Frame {
	return &Frame{
		Header: FrameHeader{
			FrameType: t,
			StreamId:  streamId,
			Length:    int32(len(payload)),
		},
		Payload: payload,
	}
}

// FrameCodec is the pipeline boundary of the frame protocol: inbound it
// turns raw connection bytes into *Frame messages for downstream handlers;
// outbound it encodes *Frame messages back into bytes. It must be added
// before the protocol handler in the pipeline; without it the pipeline has
// nothing to read the socket and the worker loop would spin.
//
// Streaming is transparent here: inbound, fragments (FlagMore) are
// assembled into one logical frame before propagation; outbound, payloads
// above the threshold are split into fragments. The codec is created per
// connection, so an in-flight assembly lives and dies with the connection
// and needs no cleanup hook.
type FrameCodec struct {
	logger *log.Logger

	// streamThreshold is the outbound payload size above which frames are
	// fragmented; inbound assembly is always accepted.
	streamThreshold int
}

func NewFrameCodec() *FrameCodec {
	return &FrameCodec{
		logger:          log.New(os.Stderr, "[codec]", log.LstdFlags),
		streamThreshold: defaultStreamThreshold,
	}
}

// NewFrameCodecWithStreamThreshold builds a codec that fragments outbound
// payloads above the given size. Useful for tests and for deployments that
// prefer many small writes over one large one.
func NewFrameCodecWithStreamThreshold(threshold int) *FrameCodec {
	c := NewFrameCodec()
	c.streamThreshold = threshold
	return c
}

var _ handler.InboundHandler = (*FrameCodec)(nil)
var _ handler.OutboundHandler = (*FrameCodec)(nil)

// HandleRead blocks reading exactly one logical message from the connection
// — assembling FlagMore fragments when present — then propagates it
// downstream. A framing error closes the connection; a clean peer close
// (io.EOF) also closes so the worker loop can finish quietly. A stream
// violation (interleaved fragments, oversize assembly) is a protocol error
// and closes too.
func (c *FrameCodec) HandleRead(ctx handler.InboundContext, _ handler.Message) {
	frame, err := DecodeStreamed(ctx.Conn())
	if err != nil {
		switch {
		case errors.Is(err, os.ErrDeadlineExceeded):
			// the reactor arms a read deadline per idle-timeout period; a
			// timeout means the peer went silent, so reap the connection
			c.logger.Printf("reaping idle connection %s", ctx.Conn().RemoteAddr())
			_ = ctx.Conn().Close()
		case err != io.EOF:
			ctx.Close(fmt.Errorf("proto: decode frame: %w", err))
		default:
			_ = ctx.Conn().Close()
		}
		return
	}
	ctx.HandleRead(frame)
}

// HandleWrite encodes *Frame messages on their way out; any other message
// type is propagated unchanged towards the head of the pipeline. A payload
// above the stream threshold is split into fragments encoded back to back,
// so the wire never carries a frame larger than MaxFrameSize.
func (c *FrameCodec) HandleWrite(ctx handler.OutboundContext, message handler.Message) {
	frame, ok := message.(*Frame)
	if !ok {
		ctx.HandleWrite(message)
		return
	}
	buffers, err := encodeStreamFrames(frame.Header.FrameType, frame.Header.StreamId, frame.Payload, c.streamThreshold)
	if err != nil {
		ctx.Close(fmt.Errorf("proto: encode frame: %w", err))
		return
	}
	if len(buffers) == 1 {
		ctx.HandleWrite(buffers[0])
		return
	}
	joined := make([]byte, 0, sumLens(buffers))
	for _, buf := range buffers {
		joined = append(joined, buf...)
	}
	ctx.HandleWrite(joined)
}

func sumLens(buffers [][]byte) int {
	n := 0
	for _, buf := range buffers {
		n += len(buf)
	}
	return n
}
