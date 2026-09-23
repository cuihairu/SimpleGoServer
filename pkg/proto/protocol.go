package proto

import (
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
}

var _ handler.InboundHandler = (*ProtocolHandler)(nil)

func NewProtocolHandler(handleRequest RequestHandler) *ProtocolHandler {
	if handleRequest == nil {
		handleRequest = func(action string, data []byte) (any, error) {
			return nil, fmt.Errorf("no request handler registered for action %q", action)
		}
	}
	return &ProtocolHandler{
		handleRequest: handleRequest,
		logger:        log.New(os.Stderr, "[proto]", log.LstdFlags),
		subs:          make(map[string]map[net.Conn]struct{}),
	}
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
	p.mu.RUnlock()
	if len(members) == 0 {
		return 0, nil
	}

	frame := newFrame(PUBLISH, p.seq.Add(1), mustJSON(topic, body))
	buf, err := Encode(frame)
	if err != nil {
		return 0, err
	}
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
type FrameCodec struct {
	logger *log.Logger
}

func NewFrameCodec() *FrameCodec {
	return &FrameCodec{logger: log.New(os.Stderr, "[codec]", log.LstdFlags)}
}

var _ handler.InboundHandler = (*FrameCodec)(nil)
var _ handler.OutboundHandler = (*FrameCodec)(nil)

// HandleRead blocks reading exactly one frame from the connection, then
// propagates it downstream. A framing error closes the connection; a clean
// peer close (io.EOF) also closes so the worker loop can finish quietly.
func (c *FrameCodec) HandleRead(ctx handler.InboundContext, _ handler.Message) {
	frame, err := Decode(ctx.Conn())
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
// type is propagated unchanged towards the head of the pipeline.
func (c *FrameCodec) HandleWrite(ctx handler.OutboundContext, message handler.Message) {
	frame, ok := message.(*Frame)
	if !ok {
		ctx.HandleWrite(message)
		return
	}
	buf, err := Encode(frame)
	if err != nil {
		ctx.Close(fmt.Errorf("proto: encode frame: %w", err))
		return
	}
	ctx.HandleWrite(buf)
}
