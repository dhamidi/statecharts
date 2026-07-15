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

// connectionStart is "start"'s canonical payload -> a freshly spawned
// connection actor. RequestID resolves an HTTP-owned reply channel in the
// Server's instance-scoped capability registry; the channel itself never
// crosses the actor payload boundary.
type connectionStart struct {
	ConversationID protocol.ConversationID
	Tools          []protocol.ToolName
	FromSeq        int // Last-Event-ID + 1, or 0 for a fresh connection
	RequestID      string
}

// connectionModel is one SSE connection's (non-durable) datamodel.
type connectionModel struct {
	ConversationID protocol.ConversationID
	Tools          []protocol.ToolName
}

func pushFrame(requests *RequestRegistry, session string, f sseFrame) {
	frames, ok := requests.connectionFrames(session)
	if !ok {
		return
	}
	select {
	case frames <- f:
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
func startConnection(requests *RequestRegistry) statecharts.GoAction[connectionModel] {
	return func(d *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		start, ok := decodeConnectionStart(ev.Data)
		if !ok {
			return nil
		}
		d.ConversationID = start.ConversationID
		d.Tools = start.Tools
		owner := connectionOwner(ec)
		frames := requests.openFrames(ec.SessionID())

		ec.Send("subscribe", statecharts.SendOptions{
			Target: "fanout",
			Data:   encodeFanoutSubscribe(fanoutSubscribe{ConversationID: d.ConversationID, Connection: owner}),
		})
		ec.Send("catchup", statecharts.SendOptions{
			Target: statecharts.Identifier(d.ConversationID),
			Data:   encodeCatchupRequest(CatchupRequestData{Connection: owner, FromSeq: start.FromSeq}),
		})
		for _, tool := range d.Tools {
			ec.Send("claim", statecharts.SendOptions{Target: "toolregistry", Data: encodeToolClaim(toolClaim{Tool: tool, Owner: owner})})
		}
		ec.Send("renew_lease", statecharts.SendOptions{Delay: leaseRenewInterval})
		if reply, ok := requests.takeConnection(start.RequestID); ok {
			reply <- frames
		}
		return nil
	}
}

func renewConnectionLeases(d *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	owner := connectionOwner(ec)
	for _, tool := range d.Tools {
		ec.Send("claim", statecharts.SendOptions{Target: "toolregistry", Data: encodeToolClaim(toolClaim{Tool: tool, Owner: owner})})
	}
	ec.Send("renew_lease", statecharts.SendOptions{Delay: leaseRenewInterval})
	return nil
}

func forwardFanoutFrame(requests *RequestRegistry) statecharts.GoAction[connectionModel] {
	return func(_ *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		bc, ok := decodeFanoutBroadcast(ev.Data)
		if !ok {
			return nil
		}
		switch bc.Kind {
		case "message":
			pushFrame(requests, ec.SessionID(), sseFrame{ID: strconv.Itoa(bc.Seq), Event: "message", Data: bc.Message})
		case "delta":
			pushFrame(requests, ec.SessionID(), sseFrame{Event: "delta", Data: bc.Delta})
		}
		return nil
	}
}

func forwardCatchupMessage(requests *RequestRegistry) statecharts.GoAction[connectionModel] {
	return func(_ *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		m, ok := decodeCatchupMessage(ev.Data)
		if !ok {
			return nil
		}
		pushFrame(requests, ec.SessionID(), sseFrame{ID: strconv.Itoa(m.Seq), Event: "message", Data: m.Frame})
		return nil
	}
}

func forwardToolCall(requests *RequestRegistry) statecharts.GoAction[connectionModel] {
	return func(_ *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		tc, ok := decodeToolCall(ev.Data)
		if !ok {
			return nil
		}
		pushFrame(requests, ec.SessionID(), sseFrame{Event: "tool_call", Data: protocol.ToolCallFrame{
			ConversationID: tc.ConversationID, CallID: tc.CallID, Name: tc.Name, Args: tc.Args,
		}})
		return nil
	}
}

func teardownConnection(requests *RequestRegistry) statecharts.GoAction[connectionModel] {
	return func(d *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		owner := connectionOwner(ec)
		ec.Send("unsubscribe", statecharts.SendOptions{
			Target: "fanout",
			Data:   encodeFanoutSubscribe(fanoutSubscribe{ConversationID: d.ConversationID, Connection: owner}),
		})
		for _, tool := range d.Tools {
			ec.Send("release", statecharts.SendOptions{Target: "toolregistry", Data: encodeToolClaim(toolClaim{Tool: tool, Owner: owner})})
		}
		requests.closeFrames(ec.SessionID())
		return nil
	}
}

// ConnectionKind is the chart kind name a per-SSE-request connection actor
// is Registered and Spawned under.
const ConnectionKind statecharts.Identifier = "connection"

// BuildConnectionChart returns the non-durable, per-connection chart: one
// instance per active GET /conversations/{id}/events request. It has no
// per-actor eviction today (see http.go's own note on this known
// limitation) -- a closed connection simply sits in its terminal "closed"
// Final state, which the actor system now frees automatically since it's
// both non-durable and finished (see actors' eviction of finished actors).
func BuildConnectionChart(requests *RequestRegistry) (*statecharts.Chart, error) {
	model := statecharts.NewGoModel(func() *connectionModel { return &connectionModel{} })
	register := func(name string, action statecharts.GoAction[connectionModel]) (statecharts.GoActionRef, error) {
		return model.Action(statecharts.Identifier("ai-agent.server.connection."+name), "v1", action)
	}
	start, err := register("start", startConnection(requests))
	if err != nil {
		return nil, err
	}
	renew, err := register("renew-leases", renewConnectionLeases)
	if err != nil {
		return nil, err
	}
	fanout, err := register("forward-fanout-frame", forwardFanoutFrame(requests))
	if err != nil {
		return nil, err
	}
	catchup, err := register("forward-catchup-message", forwardCatchupMessage(requests))
	if err != nil {
		return nil, err
	}
	toolCall, err := register("forward-tool-call", forwardToolCall(requests))
	if err != nil {
		return nil, err
	}
	teardown, err := register("teardown", teardownConnection(requests))
	if err != nil {
		return nil, err
	}
	return buildCanonicalChart(
		statecharts.Compound("connection", "streaming",
			statecharts.Children(
				statecharts.Atomic("streaming",
					statecharts.On("start", statecharts.Then(start.Do())),
					statecharts.On("renew_lease", statecharts.Then(renew.Do())),
					statecharts.On("fanout_frame", statecharts.Then(fanout.Do())),
					statecharts.On("catchup_message", statecharts.Then(catchup.Do())),
					statecharts.On("tool_call", statecharts.Then(toolCall.Do())),
					statecharts.On("disconnect", statecharts.Target("closed")),
				),
				statecharts.Final("closed", statecharts.OnEntry(teardown.Do())),
			),
		),
		model, statecharts.WithRevisionSalt("v1"))
}
