package server

import (
	"sort"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// fanoutBroadcast is "broadcast"'s payload -> fanout. Kind is "message" for
// a durable, logged transcript entry (Seq is its 1-based index within the
// conversation's own History, matching what replyWithCatchup uses for the
// same entry) or "delta" for an ephemeral, never-logged preview chunk (Seq
// is meaningless for these; ConnectionActor sends them with no SSE id).
type fanoutBroadcast struct {
	ConversationID protocol.ConversationID
	Kind           string // "message" | "delta"
	Seq            int    // Kind == "message" only
	Message        protocol.MessageFrame
	Delta          deltaFrame
}

// fanoutSubscribe is "subscribe"/"unsubscribe"'s payload -> fanout.
type fanoutSubscribe struct {
	ConversationID protocol.ConversationID
	Connection     protocol.ConnectionID
}

// fanoutModel is FanoutActor's (non-durable) datamodel: which connections
// currently want live traffic for which conversation.
type fanoutModel struct {
	Subscribers map[protocol.ConversationID][]protocol.ConnectionID
}

var subscribeConnection = statecharts.Action(func(d *fanoutModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	sub, ok := decodeFanoutSubscribe(ev.Data)
	if !ok {
		return nil
	}
	for _, c := range d.Subscribers[sub.ConversationID] {
		if c == sub.Connection {
			return nil
		}
	}
	d.Subscribers[sub.ConversationID] = append(d.Subscribers[sub.ConversationID], sub.Connection)
	return nil
})

var unsubscribeConnection = statecharts.Action(func(d *fanoutModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	sub, ok := decodeFanoutSubscribe(ev.Data)
	if !ok {
		return nil
	}
	subs := d.Subscribers[sub.ConversationID]
	for i, c := range subs {
		if c == sub.Connection {
			d.Subscribers[sub.ConversationID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	return nil
})

var forwardBroadcast = statecharts.Action(func(d *fanoutModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	bc, ok := decodeFanoutBroadcast(ev.Data)
	if !ok {
		return nil
	}
	subs := append([]protocol.ConnectionID(nil), d.Subscribers[bc.ConversationID]...)
	sort.Slice(subs, func(i, j int) bool { return subs[i] < subs[j] }) // deterministic order, not that it matters for a fanout
	for _, conn := range subs {
		ec.Send("fanout_frame", statecharts.SendOptions{Target: statecharts.Identifier(conn), Data: encodeFanoutBroadcast(bc)})
	}
	return nil
})

// FanoutKind is the chart kind name the singleton "fanout" actor is
// Registered and Spawned under.
const FanoutKind statecharts.Identifier = "fanout"

// BuildFanoutChart returns the non-durable "fanout" singleton: a
// conversation-id-keyed subscriber list, forwarding every broadcast to
// every current subscriber of that conversation, verbatim -- it has no
// idea tools exist, or what "message" vs "delta" even means beyond routing.
func BuildFanoutChart() (*statecharts.Chart, error) {
	return statecharts.Build(
		statecharts.Atomic("fanout",
			statecharts.On("subscribe", statecharts.Then(subscribeConnection)),
			statecharts.On("unsubscribe", statecharts.Then(unsubscribeConnection)),
			statecharts.On("broadcast", statecharts.Then(forwardBroadcast)),
		),
		statecharts.WithNewDatamodel(func() any {
			return &fanoutModel{Subscribers: map[protocol.ConversationID][]protocol.ConnectionID{}}
		}), statecharts.WithVersion("v1"))
}
