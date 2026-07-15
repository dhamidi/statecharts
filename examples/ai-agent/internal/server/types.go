// Package server implements the workspace server actors and HTTP handlers.
package server

import (
	"fmt"
	"sync"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/llm"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

const (
	tagUserMessage          = "aiagent.user_message"
	tagToolResult           = "aiagent.tool_result"
	tagLLMReply             = "aiagent.llm_reply"
	tagRegisterConversation = "aiagent.register_conversation"
	tagConversationState    = "aiagent.conversation_state"
	tagCatchupRequest       = "aiagent.catchup_request"
	tagDirectorySync        = "aiagent.directory_sync"
	tagDispatch             = "aiagent.dispatch"
	tagToolOffer            = "aiagent.tool_offer"
	tagCatchupMessage       = "aiagent.catchup_message"
	tagFanoutBroadcast      = "aiagent.fanout_broadcast"
	tagFanoutSubscribe      = "aiagent.fanout_subscribe"
	tagToolClaim            = "aiagent.tool_claim"
	tagToolCall             = "aiagent.tool_call"
	tagConnectionStart      = "aiagent.connection_start"
	tagProviderChunk        = "aiagent.provider_chunk"
	tagDirectoryRequest     = "aiagent.directory_request"
)

func sv(s string) statecharts.Value {
	v, e := statecharts.StringValue(s)
	if e != nil {
		panic(e)
	}
	return v
}
func mv(m map[string]statecharts.Value) statecharts.Value {
	v, e := statecharts.MapValue(m)
	if e != nil {
		panic(e)
	}
	return v
}
func tagged(tag string, v statecharts.Value) statecharts.Value {
	x, e := statecharts.TaggedValue(tag, v)
	if e != nil {
		panic(e)
	}
	return x
}
func fields(v statecharts.Value, tag string) (map[string]statecharts.Value, bool) {
	t, p, ok := v.AsTagged()
	if !ok || t != tag {
		return nil, false
	}
	m, ok := p.AsMap()
	return m, ok
}
func str(m map[string]statecharts.Value, k string) (string, bool) {
	v, ok := m[k]
	if !ok {
		return "", false
	}
	return v.AsString()
}
func integer(m map[string]statecharts.Value, k string) (int, bool) {
	v, ok := m[k]
	if !ok {
		return 0, false
	}
	n, ok := v.AsInt64()
	if !ok {
		return 0, false
	}
	x := int(n)
	return x, int64(x) == n
}
func boolean(m map[string]statecharts.Value, k string) (bool, bool) {
	v, ok := m[k]
	if !ok {
		return false, false
	}
	return v.AsBool()
}
func stringsValue[T ~string](xs []T) statecharts.Value {
	vs := make([]statecharts.Value, len(xs))
	for i, x := range xs {
		vs[i] = sv(string(x))
	}
	return statecharts.ListValue(vs)
}
func decodeStrings[T ~string](v statecharts.Value) ([]T, bool) {
	vs, ok := v.AsList()
	if !ok {
		return nil, false
	}
	out := make([]T, len(vs))
	for i, x := range vs {
		s, ok := x.AsString()
		if !ok {
			return nil, false
		}
		out[i] = T(s)
	}
	return out, true
}

type UserMessageData struct{ Text string }

func encodeUserMessage(x UserMessageData) statecharts.Value {
	return tagged(tagUserMessage, mv(map[string]statecharts.Value{"text": sv(x.Text)}))
}
func decodeUserMessage(v statecharts.Value) (UserMessageData, bool) {
	m, ok := fields(v, tagUserMessage)
	x, o := str(m, "text")
	return UserMessageData{x}, ok && o
}

type ToolResultData struct {
	CallID   protocol.CallID
	Output   string
	ExitCode int
	Error    string
}

func encodeToolResult(x ToolResultData) statecharts.Value {
	return tagged(tagToolResult, mv(map[string]statecharts.Value{"call_id": sv(string(x.CallID)), "output": sv(x.Output), "exit_code": statecharts.Int64Value(int64(x.ExitCode)), "error": sv(x.Error)}))
}
func decodeToolResult(v statecharts.Value) (ToolResultData, bool) {
	m, ok := fields(v, tagToolResult)
	a, aok := str(m, "call_id")
	o, ook := str(m, "output")
	e, eok := integer(m, "exit_code")
	er, erok := str(m, "error")
	return ToolResultData{protocol.CallID(a), o, e, er}, ok && aok && ook && eok && erok
}

type LLMReplyData struct {
	IsToolCall bool
	Text       string
	ToolName   protocol.ToolName
	ToolArgs   protocol.ToolArgs
}

func encodeLLMReply(x LLMReplyData) statecharts.Value {
	j, e := statecharts.ValueFromJSON(map[string]any(x.ToolArgs))
	if e != nil {
		j = mv(nil)
	}
	return tagged(tagLLMReply, mv(map[string]statecharts.Value{"is_tool_call": statecharts.BoolValue(x.IsToolCall), "text": sv(x.Text), "tool_name": sv(string(x.ToolName)), "tool_args": j}))
}
func decodeLLMReply(v statecharts.Value) (LLMReplyData, bool) {
	m, ok := fields(v, tagLLMReply)
	b, bok := boolean(m, "is_tool_call")
	t, tok := str(m, "text")
	n, nok := str(m, "tool_name")
	j, jok := m["tool_args"]
	raw, err := j.JSONValue()
	args, cast := raw.(map[string]any)
	return LLMReplyData{b, t, protocol.ToolName(n), protocol.NewToolArgs(args)}, ok && bok && tok && nok && jok && err == nil && cast
}

type RegisterConversationData struct {
	ID    protocol.ConversationID
	Title string
}

func encodeRegister(x RegisterConversationData) statecharts.Value {
	return tagged(tagRegisterConversation, mv(map[string]statecharts.Value{"id": sv(string(x.ID)), "title": sv(x.Title)}))
}
func decodeRegister(v statecharts.Value) (RegisterConversationData, bool) {
	m, ok := fields(v, tagRegisterConversation)
	id, a := str(m, "id")
	t, b := str(m, "title")
	return RegisterConversationData{protocol.ConversationID(id), t}, ok && a && b
}

type ConversationStateData struct {
	ID    protocol.ConversationID
	State protocol.ConversationState
}

func encodeConversationState(x ConversationStateData) statecharts.Value {
	return tagged(tagConversationState, mv(map[string]statecharts.Value{"id": sv(string(x.ID)), "state": sv(string(x.State))}))
}
func decodeConversationState(v statecharts.Value) (ConversationStateData, bool) {
	m, ok := fields(v, tagConversationState)
	id, a := str(m, "id")
	s, b := str(m, "state")
	return ConversationStateData{protocol.ConversationID(id), protocol.ConversationState(s)}, ok && a && b
}

type CatchupRequestData struct {
	Connection protocol.ConnectionID
	FromSeq    int
}

func encodeCatchupRequest(x CatchupRequestData) statecharts.Value {
	return tagged(tagCatchupRequest, mv(map[string]statecharts.Value{"connection": sv(string(x.Connection)), "from_seq": statecharts.Int64Value(int64(x.FromSeq))}))
}
func decodeCatchupRequest(v statecharts.Value) (CatchupRequestData, bool) {
	m, ok := fields(v, tagCatchupRequest)
	c, a := str(m, "connection")
	n, b := integer(m, "from_seq")
	return CatchupRequestData{protocol.ConnectionID(c), n}, ok && a && b
}
func encodeSummary(x protocol.ConversationSummary) statecharts.Value {
	return tagged(tagDirectorySync, mv(map[string]statecharts.Value{"id": sv(string(x.ID)), "title": sv(x.Title), "state": sv(string(x.State))}))
}
func decodeSummary(v statecharts.Value) (protocol.ConversationSummary, bool) {
	m, ok := fields(v, tagDirectorySync)
	id, a := str(m, "id")
	t, b := str(m, "title")
	s, c := str(m, "state")
	return protocol.ConversationSummary{ID: protocol.ConversationID(id), Title: t, State: protocol.ConversationState(s)}, ok && a && b && c
}
func encodeMessage(x llm.Message) statecharts.Value {
	return mv(map[string]statecharts.Value{"role": sv(string(x.Role)), "text": sv(x.Text)})
}
func decodeMessage(v statecharts.Value) (llm.Message, bool) {
	m, ok := v.AsMap()
	r, a := str(m, "role")
	t, b := str(m, "text")
	return llm.Message{Role: llm.Role(r), Text: t}, ok && a && b
}
func encodeDispatch(x dispatchPayload) statecharts.Value {
	hs := make([]statecharts.Value, len(x.Request.History))
	for i := range x.Request.History {
		hs[i] = encodeMessage(x.Request.History[i])
	}
	ts := make([]statecharts.Value, len(x.Request.Tools))
	for i, t := range x.Request.Tools {
		ts[i] = mv(map[string]statecharts.Value{"name": sv(string(t.Name)), "description": sv(t.Description)})
	}
	return tagged(tagDispatch, mv(map[string]statecharts.Value{"conversation_id": sv(string(x.ConversationID)), "history": statecharts.ListValue(hs), "tools": statecharts.ListValue(ts)}))
}
func decodeDispatch(v statecharts.Value) (dispatchPayload, bool) {
	m, ok := fields(v, tagDispatch)
	id, a := str(m, "conversation_id")
	hv, b := m["history"].AsList()
	tv, c := m["tools"].AsList()
	x := dispatchPayload{ConversationID: protocol.ConversationID(id)}
	for _, v := range hv {
		q, o := decodeMessage(v)
		if !o {
			return x, false
		}
		x.Request.History = append(x.Request.History, q)
	}
	for _, v := range tv {
		z, o := v.AsMap()
		n, nok := str(z, "name")
		d, dok := str(z, "description")
		if !o || !nok || !dok {
			return x, false
		}
		x.Request.Tools = append(x.Request.Tools, llm.ToolDef{Name: n, Description: d})
	}
	return x, ok && a && b && c
}

func encodeFanoutSubscribe(x fanoutSubscribe) statecharts.Value {
	return tagged(tagFanoutSubscribe, mv(map[string]statecharts.Value{"conversation_id": sv(string(x.ConversationID)), "connection": sv(string(x.Connection))}))
}
func decodeFanoutSubscribe(v statecharts.Value) (fanoutSubscribe, bool) {
	m, ok := fields(v, tagFanoutSubscribe)
	a, x := str(m, "conversation_id")
	b, y := str(m, "connection")
	return fanoutSubscribe{protocol.ConversationID(a), protocol.ConnectionID(b)}, ok && x && y
}
func encodeToolClaim(x toolClaim) statecharts.Value {
	return tagged(tagToolClaim, mv(map[string]statecharts.Value{"tool": sv(string(x.Tool)), "owner": sv(string(x.Owner))}))
}
func decodeToolClaim(v statecharts.Value) (toolClaim, bool) {
	m, ok := fields(v, tagToolClaim)
	a, x := str(m, "tool")
	b, y := str(m, "owner")
	return toolClaim{protocol.ToolName(a), protocol.ConnectionID(b)}, ok && x && y
}
func encodeConnectionStart(x connectionStart) statecharts.Value {
	return tagged(tagConnectionStart, mv(map[string]statecharts.Value{"conversation_id": sv(string(x.ConversationID)), "tools": stringsValue(x.Tools), "from_seq": statecharts.Int64Value(int64(x.FromSeq)), "request_id": sv(x.RequestID)}))
}
func decodeConnectionStart(v statecharts.Value) (connectionStart, bool) {
	m, ok := fields(v, tagConnectionStart)
	a, x := str(m, "conversation_id")
	ts, y := decodeStrings[protocol.ToolName](m["tools"])
	n, z := integer(m, "from_seq")
	id, q := str(m, "request_id")
	return connectionStart{ConversationID: protocol.ConversationID(a), Tools: ts, FromSeq: n, RequestID: id}, ok && x && y && z && q
}
func encodeDirectoryRequest(id string) statecharts.Value {
	return tagged(tagDirectoryRequest, mv(map[string]statecharts.Value{"request_id": sv(id)}))
}
func decodeDirectoryRequest(v statecharts.Value) (string, bool) {
	m, ok := fields(v, tagDirectoryRequest)
	id, x := str(m, "request_id")
	return id, ok && x
}
func encodeMessageFrame(x protocol.MessageFrame) statecharts.Value {
	return mv(map[string]statecharts.Value{"role": sv(string(x.Role)), "text": sv(x.Text)})
}
func decodeMessageFrame(v statecharts.Value) (protocol.MessageFrame, bool) {
	m, ok := v.AsMap()
	r, a := str(m, "role")
	t, b := str(m, "text")
	return protocol.MessageFrame{Role: protocol.Role(r), Text: t}, ok && a && b
}
func encodeCatchupMessage(x catchupMessage) statecharts.Value {
	return tagged(tagCatchupMessage, mv(map[string]statecharts.Value{"seq": statecharts.Int64Value(int64(x.Seq)), "frame": encodeMessageFrame(x.Frame)}))
}
func decodeCatchupMessage(v statecharts.Value) (catchupMessage, bool) {
	m, ok := fields(v, tagCatchupMessage)
	n, a := integer(m, "seq")
	f, b := decodeMessageFrame(m["frame"])
	return catchupMessage{n, f}, ok && a && b
}
func encodeFanoutBroadcast(x fanoutBroadcast) statecharts.Value {
	m := map[string]statecharts.Value{"conversation_id": sv(string(x.ConversationID)), "kind": sv(x.Kind), "seq": statecharts.Int64Value(int64(x.Seq))}
	if x.Kind == "message" {
		m["frame"] = encodeMessageFrame(x.Message)
	} else {
		m["frame"] = mv(map[string]statecharts.Value{"kind": sv(x.Delta.Kind), "text": sv(x.Delta.Text)})
	}
	return tagged(tagFanoutBroadcast, mv(m))
}
func decodeFanoutBroadcast(v statecharts.Value) (fanoutBroadcast, bool) {
	m, ok := fields(v, tagFanoutBroadcast)
	id, a := str(m, "conversation_id")
	k, b := str(m, "kind")
	n, c := integer(m, "seq")
	x := fanoutBroadcast{ConversationID: protocol.ConversationID(id), Kind: k, Seq: n}
	if k == "message" {
		f, q := decodeMessageFrame(m["frame"])
		x.Message = f
		return x, ok && a && b && c && q
	}
	z, q := m["frame"].AsMap()
	dk, r := str(z, "kind")
	dt, s := str(z, "text")
	x.Delta = deltaFrame{dk, dt}
	return x, ok && a && b && c && q && r && s
}
func encodeToolCall(x toolCallDelivery) statecharts.Value {
	j, e := statecharts.ValueFromJSON(map[string]any(x.Args))
	if e != nil {
		j = mv(nil)
	}
	return tagged(tagToolCall, mv(map[string]statecharts.Value{"conversation_id": sv(string(x.ConversationID)), "call_id": sv(string(x.CallID)), "name": sv(string(x.Name)), "args": j}))
}
func decodeToolCall(v statecharts.Value) (toolCallDelivery, bool) {
	m, ok := fields(v, tagToolCall)
	a, x := str(m, "conversation_id")
	b, y := str(m, "call_id")
	n, z := str(m, "name")
	raw, e := m["args"].JSONValue()
	args, q := raw.(map[string]any)
	return toolCallDelivery{protocol.ConversationID(a), protocol.CallID(b), protocol.ToolName(n), protocol.NewToolArgs(args)}, ok && x && y && z && e == nil && q
}

func encodeToolOffer(x toolOffer) statecharts.Value {
	j, _ := statecharts.ValueFromJSON(map[string]any(x.Args))
	return tagged(tagToolOffer, mv(map[string]statecharts.Value{"conversation_id": sv(string(x.ConversationID)), "tool": sv(string(x.Tool)), "call_id": sv(string(x.CallID)), "args": j}))
}
func decodeToolOffer(v statecharts.Value) (toolOffer, bool) {
	m, ok := fields(v, tagToolOffer)
	a, x := str(m, "conversation_id")
	b, y := str(m, "tool")
	c, z := str(m, "call_id")
	raw, e := m["args"].JSONValue()
	args, q := raw.(map[string]any)
	return toolOffer{protocol.ConversationID(a), protocol.ToolName(b), protocol.CallID(c), protocol.NewToolArgs(args)}, ok && x && y && z && e == nil && q
}
func encodeProviderChunk(x llm.Chunk) statecharts.Value {
	j, _ := statecharts.ValueFromJSON(x.ToolCall.Args)
	return tagged(tagProviderChunk, mv(map[string]statecharts.Value{"kind": sv(x.Kind), "text": sv(x.TextDelta), "tool_id": sv(x.ToolCall.ID), "tool_name": sv(x.ToolCall.Name), "tool_args": j}))
}
func decodeProviderChunk(v statecharts.Value) (llm.Chunk, bool) {
	m, ok := fields(v, tagProviderChunk)
	k, a := str(m, "kind")
	t, b := str(m, "text")
	id, c := str(m, "tool_id")
	n, d := str(m, "tool_name")
	raw, e := m["tool_args"].JSONValue()
	args, q := raw.(map[string]any)
	return llm.Chunk{Kind: k, TextDelta: t, ToolCall: llm.ToolCall{ID: id, Name: n, Args: args}}, ok && a && b && c && d && e == nil && q
}

type RequestRegistry struct {
	mu          sync.Mutex
	next        uint64
	lists       map[string]chan<- []protocol.ConversationSummary
	watches     map[string]chan<- chan protocol.ConversationSummary
	unwatches   map[string]chan protocol.ConversationSummary
	connections map[string]chan<- chan sseFrame
	directory   map[string][]chan protocol.ConversationSummary
	frames      map[string]chan sseFrame
}

func NewRequestRegistry() *RequestRegistry {
	return &RequestRegistry{
		lists:       map[string]chan<- []protocol.ConversationSummary{},
		watches:     map[string]chan<- chan protocol.ConversationSummary{},
		unwatches:   map[string]chan protocol.ConversationSummary{},
		connections: map[string]chan<- chan sseFrame{},
		directory:   map[string][]chan protocol.ConversationSummary{},
		frames:      map[string]chan sseFrame{},
	}
}

func (r *RequestRegistry) newIDLocked() string {
	r.next++
	return fmt.Sprintf("req-%d", r.next)
}

func (r *RequestRegistry) putList(reply chan<- []protocol.ConversationSummary) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.newIDLocked()
	r.lists[id] = reply
	return id
}

func (r *RequestRegistry) takeList(id string) (chan<- []protocol.ConversationSummary, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.lists[id]
	delete(r.lists, id)
	return v, ok
}

func (r *RequestRegistry) putWatch(reply chan<- chan protocol.ConversationSummary) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.newIDLocked()
	r.watches[id] = reply
	return id
}

func (r *RequestRegistry) takeWatch(id string) (chan<- chan protocol.ConversationSummary, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.watches[id]
	delete(r.watches, id)
	return v, ok
}

func (r *RequestRegistry) putUnwatch(channel chan protocol.ConversationSummary) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.newIDLocked()
	r.unwatches[id] = channel
	return id
}

func (r *RequestRegistry) takeUnwatch(id string) (chan protocol.ConversationSummary, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.unwatches[id]
	delete(r.unwatches, id)
	return v, ok
}

func (r *RequestRegistry) putConnection(reply chan<- chan sseFrame) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.newIDLocked()
	r.connections[id] = reply
	return id
}

func (r *RequestRegistry) takeConnection(id string) (chan<- chan sseFrame, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.connections[id]
	delete(r.connections, id)
	return v, ok
}

func (r *RequestRegistry) addDirectoryWatcher(session string, ch chan protocol.ConversationSummary) {
	r.mu.Lock()
	r.directory[session] = append(r.directory[session], ch)
	r.mu.Unlock()
}

func (r *RequestRegistry) removeDirectoryWatcher(session string, target chan protocol.ConversationSummary) {
	r.mu.Lock()
	watchers := r.directory[session]
	for i, ch := range watchers {
		if ch == target {
			watchers = append(watchers[:i], watchers[i+1:]...)
			break
		}
	}
	if len(watchers) == 0 {
		delete(r.directory, session)
	} else {
		r.directory[session] = watchers
	}
	r.mu.Unlock()
}

func (r *RequestRegistry) directoryWatchers(session string) []chan protocol.ConversationSummary {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]chan protocol.ConversationSummary(nil), r.directory[session]...)
}

func (r *RequestRegistry) openFrames(session string) chan sseFrame {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan sseFrame, 256)
	r.frames[session] = ch
	return ch
}

func (r *RequestRegistry) connectionFrames(session string) (chan sseFrame, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch, ok := r.frames[session]
	return ch, ok
}

func (r *RequestRegistry) closeFrames(session string) {
	r.mu.Lock()
	ch, ok := r.frames[session]
	delete(r.frames, session)
	r.mu.Unlock()
	if ok {
		close(ch)
	}
}

func (r *RequestRegistry) remove(id string) {
	r.mu.Lock()
	delete(r.lists, id)
	delete(r.watches, id)
	delete(r.unwatches, id)
	delete(r.connections, id)
	r.mu.Unlock()
}
