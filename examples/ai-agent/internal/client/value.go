package client

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

const (
	tagSwitch          = "ai-agent.client/switch.v1"
	tagServerFrame     = "ai-agent.client/server-frame.v1"
	tagMessageSeq      = "ai-agent.client/message-with-seq.v1"
	tagDelta           = "ai-agent.client/delta.v1"
	tagToolCall        = "ai-agent.client/tool-call.v1"
	tagExecute         = "ai-agent.client/execute.v1"
	tagSummary         = "ai-agent.client/conversation-summary.v1"
	tagSummaries       = "ai-agent.client/conversation-summaries.v1"
	tagLinkParams      = "ai-agent.client/link-params.v1"
	tagDirectoryParams = "ai-agent.client/directory-params.v1"
	tagUIRequest       = "ai-agent.client/ui-request.v1"
)

func str(s string) statecharts.Value {
	v, err := statecharts.StringValue(s)
	if err != nil {
		panic(err)
	}
	return v
}
func object(m map[string]statecharts.Value) statecharts.Value {
	v, err := statecharts.MapValue(m)
	if err != nil {
		panic(err)
	}
	return v
}
func tagged(tag string, v statecharts.Value) statecharts.Value {
	out, err := statecharts.TaggedValue(tag, v)
	if err != nil {
		panic(err)
	}
	return out
}
func taggedMap(tag string, m map[string]statecharts.Value) statecharts.Value {
	return tagged(tag, object(m))
}
func fields(v statecharts.Value, tag string) (map[string]statecharts.Value, bool) {
	t, p, ok := v.AsTagged()
	if !ok || t != tag {
		return nil, false
	}
	return p.AsMap()
}
func stringField(m map[string]statecharts.Value, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	return v.AsString()
}
func intField(m map[string]statecharts.Value, key string) (int, bool) {
	v, ok := m[key]
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

func switchValue(id protocol.ConversationID) statecharts.Value {
	return taggedMap(tagSwitch, map[string]statecharts.Value{"conversation_id": str(id.String())})
}
func decodeSwitch(v statecharts.Value) (switchRequest, bool) {
	m, ok := fields(v, tagSwitch)
	if !ok {
		return switchRequest{}, false
	}
	s, ok := stringField(m, "conversation_id")
	return switchRequest{protocol.ConversationID(s)}, ok
}
func messageValue(m protocol.MessageFrame) statecharts.Value {
	return object(map[string]statecharts.Value{"role": str(string(m.Role)), "text": str(m.Text)})
}
func decodeMessage(v statecharts.Value) (protocol.MessageFrame, bool) {
	m, ok := v.AsMap()
	if !ok {
		return protocol.MessageFrame{}, false
	}
	r, a := stringField(m, "role")
	t, b := stringField(m, "text")
	return protocol.MessageFrame{Role: protocol.Role(r), Text: t}, a && b
}
func deltaValue(d protocol.DeltaFrame) statecharts.Value {
	return taggedMap(tagDelta, map[string]statecharts.Value{"kind": str(d.Kind), "text": str(d.Text)})
}
func decodeDelta(v statecharts.Value) (protocol.DeltaFrame, bool) {
	m, ok := fields(v, tagDelta)
	if !ok {
		return protocol.DeltaFrame{}, false
	}
	k, a := stringField(m, "kind")
	t, b := stringField(m, "text")
	return protocol.DeltaFrame{Kind: k, Text: t}, a && b
}
func toolCallValue(t protocol.ToolCallFrame) statecharts.Value {
	args, err := statecharts.ValueFromJSON(map[string]any(t.Args))
	if err != nil {
		panic(err)
	}
	return taggedMap(tagToolCall, map[string]statecharts.Value{"conversation_id": str(t.ConversationID.String()), "call_id": str(t.CallID.String()), "name": str(t.Name.String()), "args": args})
}
func decodeToolCall(v statecharts.Value) (protocol.ToolCallFrame, bool) {
	m, ok := fields(v, tagToolCall)
	if !ok {
		return protocol.ToolCallFrame{}, false
	}
	c, a := stringField(m, "conversation_id")
	id, b := stringField(m, "call_id")
	n, d := stringField(m, "name")
	av, e := m["args"]
	if !e {
		return protocol.ToolCallFrame{}, false
	}
	j, err := av.JSONValue()
	if err != nil {
		return protocol.ToolCallFrame{}, false
	}
	args, ok := j.(map[string]any)
	return protocol.ToolCallFrame{ConversationID: protocol.ConversationID(c), CallID: protocol.CallID(id), Name: protocol.ToolName(n), Args: protocol.ToolArgs(args)}, a && b && d && ok
}
func summaryValue(c protocol.ConversationSummary) statecharts.Value {
	return taggedMap(tagSummary, map[string]statecharts.Value{"id": str(c.ID.String()), "title": str(c.Title), "state": str(string(c.State))})
}
func decodeSummary(v statecharts.Value) (protocol.ConversationSummary, bool) {
	m, ok := fields(v, tagSummary)
	if !ok {
		return protocol.ConversationSummary{}, false
	}
	id, a := stringField(m, "id")
	t, b := stringField(m, "title")
	s, c := stringField(m, "state")
	return protocol.ConversationSummary{ID: protocol.ConversationID(id), Title: t, State: protocol.ConversationState(s)}, a && b && c
}
func summariesValue(cs []protocol.ConversationSummary) statecharts.Value {
	vs := make([]statecharts.Value, len(cs))
	for i, c := range cs {
		vs[i] = summaryValue(c)
	}
	return tagged(tagSummaries, statecharts.ListValue(vs))
}
func decodeSummaries(v statecharts.Value) ([]protocol.ConversationSummary, bool) {
	_, p, ok := v.AsTagged()
	tag, _, _ := v.AsTagged()
	if !ok || tag != tagSummaries {
		return nil, false
	}
	vs, ok := p.AsList()
	if !ok {
		return nil, false
	}
	out := make([]protocol.ConversationSummary, len(vs))
	for i, x := range vs {
		var good bool
		out[i], good = decodeSummary(x)
		if !good {
			return nil, false
		}
	}
	return out, true
}

type uiRequests struct {
	next          atomic.Uint64
	mu            sync.Mutex
	snapshots     map[string]chan uiSnapshot
	subscriptions map[string]chan chan string
	unsubscribes  map[string]chan string
}

func newUIRequests() *uiRequests {
	return &uiRequests{snapshots: map[string]chan uiSnapshot{}, subscriptions: map[string]chan chan string{}, unsubscribes: map[string]chan string{}}
}
func (r *uiRequests) newID() string { return fmt.Sprintf("%d", r.next.Add(1)) }
func uiRequestValue(id string, conversation protocol.ConversationID) statecharts.Value {
	return taggedMap(tagUIRequest, map[string]statecharts.Value{"id": str(id), "conversation_id": str(conversation.String())})
}
func decodeUIRequest(v statecharts.Value) (string, protocol.ConversationID, bool) {
	m, ok := fields(v, tagUIRequest)
	if !ok {
		return "", "", false
	}
	id, ok := stringField(m, "id")
	c, _ := stringField(m, "conversation_id")
	return id, protocol.ConversationID(c), ok
}

func serverFrameValue(f serverFrame) statecharts.Value {
	m := map[string]statecharts.Value{"event": str(f.EventName), "id": str(f.ID)}
	if f.Message != nil {
		m["message"] = messageValue(*f.Message)
	}
	if f.Delta != nil {
		m["delta"] = deltaValue(*f.Delta)
	}
	if f.ToolCall != nil {
		m["tool_call"] = toolCallValue(*f.ToolCall)
	}
	return taggedMap(tagServerFrame, m)
}
func decodeServerFrame(v statecharts.Value) (serverFrame, bool) {
	m, ok := fields(v, tagServerFrame)
	if !ok {
		return serverFrame{}, false
	}
	e, a := stringField(m, "event")
	id, b := stringField(m, "id")
	f := serverFrame{EventName: e, ID: id}
	if x, yes := m["message"]; yes {
		z, good := decodeMessage(x)
		if !good {
			return f, false
		}
		f.Message = &z
	}
	if x, yes := m["delta"]; yes {
		z, good := decodeDelta(x)
		if !good {
			return f, false
		}
		f.Delta = &z
	}
	if x, yes := m["tool_call"]; yes {
		z, good := decodeToolCall(x)
		if !good {
			return f, false
		}
		f.ToolCall = &z
	}
	return f, a && b
}
func messageSeqValue(x messageWithSeq) statecharts.Value {
	return taggedMap(tagMessageSeq, map[string]statecharts.Value{"seq": statecharts.Int64Value(int64(x.Seq)), "frame": messageValue(x.Frame)})
}
func decodeMessageSeq(v statecharts.Value) (messageWithSeq, bool) {
	m, ok := fields(v, tagMessageSeq)
	if !ok {
		return messageWithSeq{}, false
	}
	n, a := intField(m, "seq")
	f, b := decodeMessage(m["frame"])
	return messageWithSeq{Seq: n, Frame: f}, a && b
}
func executeValue(x executeRequest) statecharts.Value {
	return taggedMap(tagExecute, map[string]statecharts.Value{"conversation_id": str(x.ConversationID.String()), "call": toolCallValue(x.Call)})
}
func decodeExecute(v statecharts.Value) (executeRequest, bool) {
	m, ok := fields(v, tagExecute)
	if !ok {
		return executeRequest{}, false
	}
	id, a := stringField(m, "conversation_id")
	c, b := decodeToolCall(m["call"])
	return executeRequest{ConversationID: protocol.ConversationID(id), Call: c}, a && b
}
