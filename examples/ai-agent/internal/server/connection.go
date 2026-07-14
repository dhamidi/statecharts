package server

import (
	"strconv"
	"time"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// leaseRenewInterval is how often a connection re-claims each tool it
// advertises, comfortably inside leaseTTL so a live connection's lease
// never lapses on its own.
const leaseRenewInterval = 10 * time.Second

// sseFrame is one line-group ConnectionActor hands to its own HTTP
// handler's SSE writer: ID is the SSE "id:" value (empty = omit, as for a
// delta frame -- see protocol.DeltaFrame's own doc comment), Event is the
// SSE "event:" name, and Data is whatever the HTTP layer JSON-encodes as
// "data:".
type sseFrame struct {
	ID    string
	Event string
	Data  any
}

// connectionStart is "start"'s payload -> a freshly spawned connection
// actor, from the HTTP handler that just Spawned it. Not JSON-wrapped:
// ConnectionActor is non-durable, never logged. Ready is how the HTTP
// handler -- which has no other way to reach into a resident actor's own
// datamodel -- gets the sseFrame channel to read its own SSE writer loop
// from, the same request/reply-over-a-channel idiom UIServerActor's
// get_snapshot uses client-side, safe here for the same reason: this
// actor is non-durable.
type connectionStart struct {
	ConversationID protocol.ConversationID
	Tools          []protocol.ToolName
	FromSeq        int // Last-Event-ID + 1, or 0 for a fresh connection
	Ready          chan<- chan sseFrame
}

// connectionModel is one SSE connection's (non-durable) datamodel.
type connectionModel struct {
	ConversationID protocol.ConversationID
	Tools          []protocol.ToolName
	Frames         chan sseFrame
}

func pushFrame(d *connectionModel, f sseFrame) {
	select {
	case d.Frames <- f:
	default:
		// The HTTP writer isn't keeping up (or is already gone and just
		// hasn't been told yet) -- drop rather than block this actor's own
		// goroutine forever. Acceptable for a demo: a real client reads its
		// own SSE stream continuously, so the buffer (sized generously) is
		// never actually this full in practice.
	}
}

func connectionOwner(ec statecharts.ExecContext) protocol.ConnectionID {
	return protocol.ConnectionID(ec.SessionID())
}

// startConnection subscribes to live fanout traffic for the conversation
// and asks the conversation itself to replay any transcript entries this
// connection doesn't already have -- both by ordinary actor Send, in that
// order (see the package doc comment on why the order matters), then
// claims a lease for every tool this connection advertises and arms its own
// renewal timer.
var startConnection = statecharts.Action(func(d *connectionModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	start, ok := statecharts.Payload[connectionStart](ev)
	if !ok {
		return nil
	}
	d.ConversationID = start.ConversationID
	d.Tools = start.Tools
	owner := connectionOwner(ec)

	ec.Send("subscribe", statecharts.SendOptions{
		Target: "fanout",
		Data:   fanoutSubscribe{ConversationID: d.ConversationID, Connection: owner},
	})
	ec.Send("catchup", statecharts.SendOptions{
		Target: statecharts.Identifier(d.ConversationID),
		Data: &catchupRequestPayload{
			TypeName: "aiagent.catchup_request",
			Value:    CatchupRequestData{Connection: owner, FromSeq: start.FromSeq},
		},
	})
	for _, tool := range d.Tools {
		ec.Send("claim", statecharts.SendOptions{Target: "toolregistry", Data: toolClaim{Tool: tool, Owner: owner}})
	}
	ec.Send("renew_lease", statecharts.SendOptions{Delay: leaseRenewInterval})
	if start.Ready != nil {
		start.Ready <- d.Frames
	}
	return nil
})

var renewLeases = statecharts.Action(func(d *connectionModel, ec statecharts.ExecContext) error {
	owner := connectionOwner(ec)
	for _, tool := range d.Tools {
		ec.Send("claim", statecharts.SendOptions{Target: "toolregistry", Data: toolClaim{Tool: tool, Owner: owner}})
	}
	ec.Send("renew_lease", statecharts.SendOptions{Delay: leaseRenewInterval})
	return nil
})

var forwardFanoutFrame = statecharts.Action(func(d *connectionModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	bc, ok := statecharts.Payload[*fanoutBroadcast](ev)
	if !ok {
		return nil
	}
	switch bc.Kind {
	case "message":
		pushFrame(d, sseFrame{ID: strconv.Itoa(bc.Seq), Event: "message", Data: bc.Frame})
	case "delta":
		pushFrame(d, sseFrame{Event: "delta", Data: bc.Frame})
	}
	return nil
})

var forwardCatchupMessage = statecharts.Action(func(d *connectionModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	m, ok := statecharts.Payload[*catchupMessage](ev)
	if !ok {
		return nil
	}
	pushFrame(d, sseFrame{ID: strconv.Itoa(m.Seq), Event: "message", Data: m.Frame})
	return nil
})

var forwardToolCall = statecharts.Action(func(d *connectionModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	tc, ok := statecharts.Payload[toolCallDelivery](ev)
	if !ok {
		return nil
	}
	pushFrame(d, sseFrame{Event: "tool_call", Data: protocol.ToolCallFrame{
		ConversationID: tc.ConversationID, CallID: tc.CallID, Name: tc.Name, Args: tc.Args,
	}})
	return nil
})

var teardownConnection = statecharts.Action(func(d *connectionModel, ec statecharts.ExecContext) error {
	owner := connectionOwner(ec)
	ec.Send("unsubscribe", statecharts.SendOptions{
		Target: "fanout",
		Data:   fanoutSubscribe{ConversationID: d.ConversationID, Connection: owner},
	})
	for _, tool := range d.Tools {
		ec.Send("release", statecharts.SendOptions{Target: "toolregistry", Data: toolClaim{Tool: tool, Owner: owner}})
	}
	close(d.Frames)
	return nil
})

// ConnectionKind is the chart kind name a per-SSE-request connection actor
// is Registered and Spawned under.
const ConnectionKind statecharts.Identifier = "connection"

// BuildConnectionChart returns the non-durable, per-connection chart: one
// instance per active GET /conversations/{id}/events request. It has no
// per-actor eviction today (see http.go's own note on this known
// limitation) -- a closed connection simply sits in its terminal "closed"
// Final state, which the actor system now frees automatically since it's
// both non-durable and finished (see actors' eviction of finished actors).
func BuildConnectionChart() (*statecharts.Chart, error) {
	return statecharts.Build(
		statecharts.Compound("connection", "streaming",
			statecharts.Children(
				statecharts.Atomic("streaming",
					statecharts.On("start", statecharts.Then(startConnection)),
					statecharts.On("renew_lease", statecharts.Then(renewLeases)),
					statecharts.On("fanout_frame", statecharts.Then(forwardFanoutFrame)),
					statecharts.On("catchup_message", statecharts.Then(forwardCatchupMessage)),
					statecharts.On("tool_call", statecharts.Then(forwardToolCall)),
					statecharts.On("disconnect", statecharts.Target("closed")),
				),
				statecharts.Final("closed", statecharts.OnEntry(teardownConnection)),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &connectionModel{Frames: make(chan sseFrame, 256)} }), statecharts.WithVersion("v1"))
}
