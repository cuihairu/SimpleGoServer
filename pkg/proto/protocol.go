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
	"sort"
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
	// Floors reports, per resumed topic, the lowest sequence number still
	// held in the topic's replay cache. A cursor below floor-1 can never be
	// caught up — those frames were evicted — so the client's contiguous
	// accepted prefix starts at the floor. Optional field: peers that do
	// not send it leave cursor alignment to the next subscribe response.
	Floors map[string]uint64 `json:"floors,omitempty"`
}

// ProtocolHandler is the server-side semantic layer on top of the frame
// codec: it answers requests, keeps topic subscriptions and publishes
// messages to subscribers. It plugs into the handler pipeline as an inbound
// handler and writes responses back through the pipeline context.
type ProtocolHandler struct {
	handleRequest RequestHandler
	logger        *log.Logger

	mu        sync.RWMutex
	subs      map[string]map[net.Conn]struct{}
	pings     atomic.Uint64
	pongs     atomic.Uint64
	topicSeqs map[string]*atomic.Uint32

	// publishGates serializes each topic's publish pipeline: sequence
	// commit, replay-cache write, and subscriber fan-out run inside one
	// gate so a subscriber can never observe seq N+1 before seq N. Entries
	// exist only while publishers are active or contending (refcounted —
	// see acquirePublishGateLocked) and are retired when the last one
	// leaves.
	publishGates map[string]*publishGate

	// resumable sessions, see handleHello. connTokens is the reverse index
	// so a SUBSCRIBE frame can find the session to record the topic in
	// without scanning every session.
	sessions   map[string]*session
	connTokens map[net.Conn]string

	// topicCache remembers the most recent publishes per topic so a
	// resumed session can be caught up on what it missed while
	// disconnected. Entries exist while the topic has subscribers or any
	// session — including a disconnected one — still records it. The
	// per-topic entry cap below is not the memory bound: the total byte
	// budget is enforced across all topics (see cacheBudget).
	topicCache map[string][]cachedPublish

	// cacheBytes is the total size of the wire frames held in topicCache,
	// maintained under p.mu; cacheBudget caps it.
	cacheBytes  int
	cacheBudget int

	// requireHello turns on strict handshake mode: a connection's first
	// frame must be HELLO and business frames arriving earlier close it.
	// Whether a connection has handshaked is not tracked separately — a
	// successful handshake is exactly what puts the connection into
	// connTokens, so that map doubles as the handshake marker.
	requireHello atomic.Bool

	closed     chan struct{}
	closeOnce  sync.Once
	sessionTTL time.Duration
}

// topicCacheSize bounds how many recent publishes are kept per topic for
// replaying to resumed sessions. Older ones are dropped: delivery stays
// best-effort, a client that was away for too long simply misses them.
const topicCacheSize = 64

// defaultCacheBudget caps the replay cache across all topics in bytes —
// the per-topic entry cap alone does not bound memory, since topics are
// unbounded and a cached frame can be up to MaxFrameSize. When the budget
// is exceeded, whole topics are evicted largest-first: partial caches
// would break the replay guarantee for exactly the topics being squeezed,
// so they go as a unit. 64 MiB is the order of one topic's worst case
// (64 entries x 1 MiB frames), so one hot topic cannot crowd out the rest.
const defaultCacheBudget = 64 << 20

// cachedPublish is one replayable publish: the wire-encoded frame (its
// StreamId doubles as the monotonically increasing sequence number) kept
// alongside the sequence for quick cursor comparisons.
type cachedPublish struct {
	seq uint32
	buf []byte
}

// session is the server-side memory of one logical connection. It outlives
// the transport: when a client reconnects and presents the token, its
// topics are moved onto the new connection. lastSeen is a Unix-nanosecond
// counter so an inbound frame can refresh it under a read lock (see
// touchSession).
type session struct {
	conn     net.Conn
	topics   map[string]struct{}
	lastSeen atomic.Int64
}

// sessionTTL bounds how long a disconnected session is kept for resuming.
// It is a field on the handler (not a constant) so tests can shrink it.
const defaultSessionTTL = 10 * time.Minute

var _ handler.InboundHandler = (*ProtocolHandler)(nil)
var _ handler.InactiveHandler = (*ProtocolHandler)(nil)

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
		topicSeqs:     make(map[string]*atomic.Uint32),
		publishGates:  make(map[string]*publishGate),
		cacheBudget:   defaultCacheBudget,
		closed:        make(chan struct{}),
		sessionTTL:    defaultSessionTTL,
	}
	go p.sessionJanitor(janitorTickInterval)
	return p
}

// RequireHello toggles strict handshake mode: with it on, a connection's
// first frame must be HELLO and any business frame arriving earlier closes
// the connection. It is safe to call at any time; servers normally set it
// once during setup, before accepting connections.
func (p *ProtocolHandler) RequireHello(v bool) { p.requireHello.Store(v) }

// Close stops the background session janitor. The handler stays usable for
// connections that are already wired into a pipeline; closing it is only
// needed to release the janitor goroutine, e.g. in tests.
func (p *ProtocolHandler) Close() {
	p.closeOnce.Do(func() { close(p.closed) })
}

// sessionJanitor periodically drops sessions that were never resumed within
// the TTL, so tokens of clients that never came back do not accumulate. A
// connected session never expires: every inbound frame refreshes lastSeen
// (see touchSession), so only a client that stopped talking — disconnected
// or silent past the TTL — ages out.
// janitorTickInterval is how often the session janitor sweeps. A variable
// (not a constant) so tests can shrink it instead of waiting a minute.
var janitorTickInterval = time.Minute

// The tick interval is read by the caller and passed in: a goroutine must
// not read the mutable package var itself, or a test that restores it in a
// defer races with a janitor that has not reached its first line yet (the
// CI race detector caught exactly that).
func (p *ProtocolHandler) sessionJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.closed:
			return
		case <-ticker.C:
			p.reapExpiredSessions()
		}
	}
}

// reapExpiredSessions is the janitor's sweep, factored out so tests can
// drive it directly instead of waiting on the production ticker.
func (p *ProtocolHandler) reapExpiredSessions() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for token, s := range p.sessions {
		if time.Since(time.Unix(0, s.lastSeen.Load())) > p.sessionTTL {
			delete(p.sessions, token)
			if s.conn != nil {
				delete(p.connTokens, s.conn)
				// an expired session will never resume, so its transport must
				// leave every member set it still occupies — the same swap a
				// resume performs, done for removal instead of replacement.
				// Without this, a client that subscribed and vanished stays
				// in the subscription table until the next publish fails on
				// the dead write; on a quiet topic that phantom subscriber
				// would also keep the replay cache alive forever.
				for topic := range s.topics {
					if members, ok := p.subs[topic]; ok {
						delete(members, s.conn)
						if len(members) == 0 {
							delete(p.subs, topic)
						}
					}
					p.dropTopicCacheLocked(topic)
				}
			}
		}
	}
}

// touchSession refreshes the connection's session timestamp. Any inbound
// frame — a heartbeat in particular — proves the client is alive, which is
// what keeps a connected session out of the janitor's reach: without this,
// a KeepAlive-only connection's session would expire while it is still
// connected, and its later resume would silently start from scratch. The
// read lock covers the two map lookups; the timestamp itself is atomic.
func (p *ProtocolHandler) touchSession(conn net.Conn) {
	p.mu.RLock()
	token, ok := p.connTokens[conn]
	s := p.sessions[token]
	p.mu.RUnlock()
	if ok && s != nil {
		s.lastSeen.Store(time.Now().UnixNano())
	}
}

// readRandom is swappable so tests can exercise the entropy-failure path.
var readRandom = rand.Read

// newSessionToken returns a random, unguessable session token.
func newSessionToken() string {
	b := make([]byte, 16)
	if _, err := readRandom(b); err != nil {
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
	// any inbound frame proves the client is alive; refresh its session so
	// the janitor only ever reaps clients that truly went quiet
	p.touchSession(ctx.Conn())
	// strict handshake mode: frames that need a negotiated session are
	// refused before one exists. Reading the marker under the same map the
	// handshake itself populates keeps the check exact — no flag that could
	// drift from reality.
	if p.requireHello.Load() && frame.Header.FrameType != HELLO {
		p.mu.RLock()
		_, handshaked := p.connTokens[ctx.Conn()]
		p.mu.RUnlock()
		if !handshaked {
			ctx.Close(fmt.Errorf("proto: strict handshake: first frame must be HELLO, got %s", frame.Header.FrameType))
			return
		}
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
	// a connection handshakes exactly once: a second HELLO means the peer
	// is confused or hostile, and re-running attachSession would silently
	// replace the live session under it. Protocol error, hang up.
	p.mu.RLock()
	_, handshaked := p.connTokens[ctx.Conn()]
	p.mu.RUnlock()
	if handshaked {
		ctx.Close(fmt.Errorf("proto: duplicate HELLO on a handshaked connection"))
		return
	}
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
	res := p.attachSession(ctx.Conn(), req.Resume, req.Cursors)
	// replay BEFORE the ack: when the client's Handshake returns it can
	// rely on the catch-up already being on the wire. The resumed topics'
	// publish gates are still held here, so publishers pinned on them can
	// only fan out after this loop drops them — a live publish can never
	// overtake the replayed frames it should follow.
	p.replayTo(ctx.Conn(), res.replay)
	for _, g := range res.gates {
		g.mu.Unlock()
		p.releasePublishGate(g)
	}
	p.reply(ctx, RESPONSE, frame.Header.StreamId, "hello", &HelloResponse{
		Version:  chosen,
		Features: protocolFeatures,
		Token:    res.token,
		Resumed:  res.resumed,
		Floors:   res.floors,
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
// transport), or mints a fresh session. The result reports whether an
// existing session was resumed; replay carries the wire frames of the
// cached publishes the client has not seen yet, ordered by sequence, and
// floors the per-topic replay-cache floors so a client whose cursor sits
// below an evicted head can jump to where the catch-up starts. On a resume
// the returned gates hold the session's topics' publish pipelines LOCKED:
// the caller must release them (after unlocking) once the replay has been
// written, so publishers fan out only after the history catch-up is on the
// wire — a live publish that raced ahead would push the client's cursor
// past the replayed frames and they would be dropped as stale.
// attachResult is what attachSession hands back to handleHello: the
// session token (fresh or resumed), whether an existing session was
// resumed, the replay frames and cache floors that catch the client up,
// and — on a resume — the topics' publish gates held LOCKED until the
// replay has been written.
type attachResult struct {
	token   string
	resumed bool
	replay  [][]byte
	floors  map[string]uint64
	gates   []*publishGate
}

func (p *ProtocolHandler) attachSession(conn net.Conn, token string, cursors map[string]uint64) attachResult {
	// Phase 1 peeks the session's topics without holding p.mu: the gates
	// must be taken gate.mu-first (the same order publishers use) or a
	// publisher mid-fan-out holding gate.mu and waiting for p.mu would
	// deadlock against this resume.
	if token != "" {
		p.mu.RLock()
		s, ok := p.sessions[token]
		resumable := ok && time.Since(time.Unix(0, s.lastSeen.Load())) <= p.sessionTTL
		var topics []string
		if resumable {
			for topic := range s.topics {
				topics = append(topics, topic)
			}
		}
		p.mu.RUnlock()

		if resumable {
			// sorted order keeps multi-topic resumes deterministic; the
			// member move below re-reads s.topics so topics recorded in
			// between still move (they just ride unpinned — reachable only
			// through a token shared by two concurrent resumes, which the
			// protocol never produces).
			sort.Strings(topics)
			gates := make([]*publishGate, 0, len(topics))
			for _, topic := range topics {
				g := p.acquirePublishGate(topic)
				g.mu.Lock()
				gates = append(gates, g)
			}
			// Phase 2 moves the recorded topics onto the new transport
			// under p.mu while the gates hold publishers off: any publisher
			// waiting on a gate can only snapshot members — including this
			// conn — after the caller has written the replay.
			p.mu.Lock()
			s, ok := p.sessions[token]
			if ok && time.Since(time.Unix(0, s.lastSeen.Load())) <= p.sessionTTL {
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
				s.lastSeen.Store(time.Now().UnixNano())
				p.connTokens[conn] = token
				replay := p.collectReplay(s, cursors)
				floors := p.replayFloorsLocked(s)
				p.mu.Unlock()
				return attachResult{token: token, resumed: true, replay: replay, floors: floors, gates: gates}
			}
			p.mu.Unlock()
			// the session expired between the phases: give the gates back
			// and fall through to a fresh session
			for _, g := range gates {
				g.mu.Unlock()
				p.releasePublishGate(g)
			}
		}
	}

	token = newSessionToken()
	ns := &session{conn: conn, topics: make(map[string]struct{})}
	ns.lastSeen.Store(time.Now().UnixNano())
	p.mu.Lock()
	p.sessions[token] = ns
	p.connTokens[conn] = token
	p.mu.Unlock()
	return attachResult{token: token}
}

// replayFloorsLocked reports the lowest cached sequence per session topic:
// the floor below which the replay cache can no longer help a lagging
// client. The caller holds p.mu — and the topics' publish gates, so no
// concurrent publish can append between this and the replay write. Topics
// with an empty or missing cache get no entry: nothing to replay, and the
// cursor needs no adjustment (a reset counter is adopted by the next
// subscribe response instead).
func (p *ProtocolHandler) replayFloorsLocked(s *session) map[string]uint64 {
	var floors map[string]uint64
	for topic := range s.topics {
		cache := p.topicCache[topic]
		if len(cache) == 0 {
			continue
		}
		if floors == nil {
			floors = make(map[string]uint64)
		}
		floors[topic] = uint64(cache[0].seq)
	}
	return floors
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
// oldest entry past the per-topic size and trimming the total byte
// footprint to the global budget.
func (p *ProtocolHandler) rememberPublish(topic string, seq uint32, buf []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cache := append(p.topicCache[topic], cachedPublish{seq: seq, buf: buf})
	if len(cache) > topicCacheSize {
		p.cacheBytes -= len(cache[0].buf)
		cache = cache[len(cache)-topicCacheSize:]
	}
	p.topicCache[topic] = cache
	p.cacheBytes += len(buf)
	p.enforceCacheBudgetLocked()
}

// enforceCacheBudgetLocked keeps the total replay cache within the byte
// budget by evicting whole topics, largest first. A topic that loses its
// cache simply cannot replay to a session resuming later — the same
// best-effort contract as the per-topic entry cap, applied under memory
// pressure.
func (p *ProtocolHandler) enforceCacheBudgetLocked() {
	for p.cacheBytes > p.cacheBudget {
		victim, size := "", 0
		for topic, cache := range p.topicCache {
			if s := frameBytes(cache); s > size {
				victim, size = topic, s
			}
		}
		if victim == "" {
			// bookkeeping drift would wedge the loop; reset to safe state
			p.cacheBytes = 0
			return
		}
		p.cacheBytes -= size
		delete(p.topicCache, victim)
	}
}

// frameBytes sums the wire size of a topic's cached frames.
func frameBytes(cache []cachedPublish) int {
	total := 0
	for _, c := range cache {
		total += len(c.buf)
	}
	return total
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
	p.cacheBytes -= frameBytes(p.topicCache[topic])
	delete(p.topicCache, topic)
	delete(p.topicSeqs, topic)
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
		// the envelope marshals our own string-plus-raw-JSON types, which
		// json.Marshal cannot fail on — there is deliberately no error path
		errBody, _ := json.Marshal(&ErrorMessage{Error: err.Error()})
		ctx.Write(newFrame(RESPONSE, streamId, envelope{action: msg.Action, raw: errBody}.bytes()))
		return
	}
	body, err := json.Marshal(resp)
	if err != nil {
		// resp comes from the user-supplied handler and can be anything, so
		// this marshal is the one that can genuinely fail
		ctx.Close(fmt.Errorf("proto: marshal response: %w", err))
		return
	}
	ctx.Write(newFrame(RESPONSE, streamId, envelope{action: msg.Action, raw: body}.bytes()))
}

func (p *ProtocolHandler) handleSubscribe(ctx handler.InboundContext, frame *Frame, subscribe bool) {
	topic, err := p.topicOf(frame)
	if err != nil {
		ctx.Close(err)
		return
	}
	if subscribe {
		// The subscribe response's LastSeq is the replay boundary a
		// resilient client baselines on: everything at or below it was
		// never fanned out to this conn, everything above it will be.
		// Holding the topic's publish gate across [member add + sequence
		// read] is what makes that exact — an in-flight publisher either
		// finishes its whole fan-out before the member lands (LastSeq
		// includes it, the conn missed it) or snapshots this conn and
		// delivers after (LastSeq excludes it). Without the gate the member
		// could join between the publisher's member snapshot and its
		// sequence commit, and the conn would silently miss a publish its
		// cursor claims it will see. gate.mu-first keeps the publish
		// pipeline's lock order.
		gate := p.acquirePublishGate(topic)
		defer p.releasePublishGate(gate)
		gate.mu.Lock()
		defer gate.mu.Unlock()
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
			s.lastSeen.Store(time.Now().UnixNano())
		}
	}
	if !subscribe {
		p.dropTopicCacheLocked(topic)
	}
	lastSeq := p.topicSequenceLocked(topic)
	p.mu.Unlock()

	verb, payload := "subscribed", any(&SubscribeResponse{
		Topic:   topic,
		LastSeq: lastSeq,
	})
	if !subscribe {
		verb, payload = "unsubscribed", any(map[string]string{"topic": topic})
	}
	p.reply(ctx, RESPONSE, frame.Header.StreamId, verb, payload)
}

// topicSequenceLocked reports the last publish sequence assigned to topic,
// or zero when the topic was never published to (or its replay state was
// released). A client can use it to distinguish a freshly reset topic
// sequence from one that survived while it was disconnected. The caller
// holds p.mu.
func (p *ProtocolHandler) topicSequenceLocked(topic string) uint64 {
	if seq := p.topicSeqs[topic]; seq != nil {
		// topicSeqs entries are minted in Publish immediately before
		// Add(1), so a live entry is always >= 1: the zero floor can only
		// mean the topic's replay state was released (seq == nil).
		return uint64(seq.Load()) // #nosec G115 -- uint32 is losslessly convertible
	}
	return 0
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

// publishGate serializes one topic's publish pipeline: everything between
// sequence commit and subscriber fan-out runs inside the gate, so a
// subscriber cannot observe seq N+1 before seq N and the replay cache is
// appended in sequence order. The refcount lets a quiet gate be retired
// without racing a blocked publisher back onto a fresh gate: a bare map
// delete would let the waiter mint a second gate and run concurrently
// with the one still draining.
type publishGate struct {
	topic string
	mu    sync.Mutex
	refs  int
}

// acquirePublishGateLocked returns the topic's gate, creating it on first
// use, and pins it with a reference so it survives until this publisher
// releases it. The caller holds p.mu.
func (p *ProtocolHandler) acquirePublishGateLocked(topic string) *publishGate {
	g := p.publishGates[topic]
	if g == nil {
		g = &publishGate{topic: topic}
		p.publishGates[topic] = g
	}
	g.refs++
	return g
}

// acquirePublishGate pins and returns the topic's gate.
func (p *ProtocolHandler) acquirePublishGate(topic string) *publishGate {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acquirePublishGateLocked(topic)
}

// releasePublishGate drops the publisher's reference and retires the gate
// when the last one leaves, so the map cannot accumulate quiet topics.
func (p *ProtocolHandler) releasePublishGate(g *publishGate) {
	p.mu.Lock()
	defer p.mu.Unlock()
	g.refs--
	if g.refs == 0 {
		delete(p.publishGates, g.topic)
	}
}

// Publish broadcasts payload to every subscriber of topic. Deliveries are
// best-effort: a subscriber whose write fails or times out is dropped from
// the topic. It returns the number of subscribers the message was written
// to.
//
// The whole pipeline — sequence commit, cache append, fan-out — runs inside
// the topic's publish gate: without it two concurrent publishers could mint
// seq 1 and 2 and land seq 2 on the wire first. A confirm-mode client's
// cursor would jump to 2, the late seq 1 would be dropped as stale, and the
// reconnect replay (which sends only frames newer than the cursor) could
// never repair the gap.
func (p *ProtocolHandler) Publish(topic string, payload any) (int, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	gate := p.acquirePublishGate(topic)
	defer p.releasePublishGate(gate)
	gate.mu.Lock()
	defer gate.mu.Unlock()

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

	p.mu.Lock()
	topicSeq := p.topicSeqs[topic]
	if topicSeq == nil {
		topicSeq = &atomic.Uint32{}
		p.topicSeqs[topic] = topicSeq
	}
	p.mu.Unlock()
	wire := mustJSON(topic, body)
	// validate the wire size BEFORE committing a sequence number: Encode
	// rejects oversize payloads, and a seq consumed by a rejected frame
	// would leave a permanent hole — confirm-mode clients would wait
	// forever for a message that never existed, and every later publish
	// would widen the gap
	if len(wire) > MaxFrameSize {
		return 0, fmt.Errorf("proto: publish to %q exceeds the frame limit (%d > %d)", topic, len(wire), MaxFrameSize)
	}
	frame := newFrame(PUBLISH, topicSeq.Add(1), wire)
	// Encode cannot fail here: wire is a non-empty JSON envelope sized
	// against MaxFrameSize just above, which excludes Encode's only
	// failure modes (nil frame, empty payload, oversize payload). There
	// is deliberately no error path — same policy as reply() and mustJSON.
	buf, _ := Encode(frame)

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
// The assembly is the shared envelope splice (see envelope.bytes), which
// cannot fail and emits exactly what marshaling the struct emits — there
// is deliberately no error path.
func mustJSON(topic string, body []byte) []byte {
	return envelope{action: topic, raw: body}.bytes()
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

// HandleInactive reclaims a dead connection's live subscription memberships.
// Without it a connection that subscribed and vanished (quiet topic, no
// failing write to reap it) stayed in the member sets forever — worst for a
// subscriber that never handshaked, which no session, janitor or resume
// will ever clean up. The session's subscription intent is deliberately
// kept: a resumable session must be able to restore these topics on the
// next handshake, and dropTopicCacheLocked already spares the replay cache
// while any session still records the topic.
func (p *ProtocolHandler) HandleInactive(ctx handler.InactiveContext, ex handler.Exception) {
	conn := ctx.Conn()
	p.mu.Lock()
	defer p.mu.Unlock()
	for topic, members := range p.subs {
		if _, member := members[conn]; !member {
			continue
		}
		delete(members, conn)
		if len(members) == 0 {
			delete(p.subs, topic)
			p.dropTopicCacheLocked(topic)
		}
	}
}

// Subscribers returns the current subscriber count of a topic.
func (p *ProtocolHandler) Subscribers(topic string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.subs[topic])
}

func (p *ProtocolHandler) reply(ctx handler.InboundContext, t FrameType, streamId uint32, action string, data any) {
	// every call site passes our own plain-JSON types (nil, strings,
	// *HelloResponse), so EncodeJSON cannot fail and there is deliberately
	// no error path
	frame, _ := EncodeJSON(t, streamId, action, data)
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
			// an overflowed int32 goes negative and is rejected by Encode's
			// sign check, so a corrupt length can never reach the wire
			Length: int32(len(payload)), // #nosec G115
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

	// pools supplies the frame and payload buffers this codec recycles. It
	// is per-connection, set at construction and never swapped: a shared
	// global would be reconfigured under connections still decoding.
	pools poolBackend
}

func NewFrameCodec() *FrameCodec {
	return &FrameCodec{
		logger:          log.New(os.Stderr, "[codec]", log.LstdFlags),
		streamThreshold: defaultStreamThreshold,
		pools:           defaultPools,
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
//
// The frame and its payload buffer come from the pools and are released
// right after dispatch returns. That release point is sound because the
// pipeline propagates inbound events as synchronous method calls: by the
// time ctx.HandleRead comes back, the business handler has run, the response
// has been marshaled and encoded, and nothing downstream references the
// payload anymore (DecodeJSONMessage's Data alias is contractually dead by
// then — see pool.go).
func (c *FrameCodec) HandleRead(ctx handler.InboundContext, _ handler.Message) {
	frame, err := decodeStreamed(ctx.Conn(), c.pools)
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
	frame.release(c.pools)
}

// HandleWrite encodes *Frame messages on their way out; any other message
// type is propagated unchanged towards the head of the pipeline. A payload
// above the stream threshold is split into fragments encoded back to back,
// so the wire never carries a frame larger than MaxFrameSize.
//
// Encoded buffers are pooled and released once the downstream write has
// consumed them: the pipeline writes synchronously (HeadHandler's
// conn.Write returns before HandleWrite does), so the bytes are on the wire
// — or the connection is dead — by then. Publish never passes through here;
// it caches its own Encode output for replay and keeps exact-size buffers.
func (c *FrameCodec) HandleWrite(ctx handler.OutboundContext, message handler.Message) {
	frame, ok := message.(*Frame)
	if !ok {
		ctx.HandleWrite(message)
		return
	}
	msg, err := encodeMessage(frame.Header.FrameType, frame.Header.StreamId, frame.Payload, c.streamThreshold, c.pools)
	if err != nil {
		ctx.Close(fmt.Errorf("proto: encode frame: %w", err))
		return
	}
	if msg.buf != nil {
		// the common case: one buffer, no slice bookkeeping at all
		ctx.HandleWrite(msg.buf)
		msg.release(c.pools)
		return
	}
	joined := make([]byte, 0, sumLens(msg.bufs))
	for _, buf := range msg.bufs {
		joined = append(joined, buf...)
	}
	// fragments are dead once joined; recycle before the write even starts
	msg.release(c.pools)
	ctx.HandleWrite(joined)
}

func sumLens(buffers [][]byte) int {
	n := 0
	for _, buf := range buffers {
		n += len(buf)
	}
	return n
}
