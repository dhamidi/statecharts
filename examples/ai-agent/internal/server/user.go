package server

import (
	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// conversationSummary is UserActor's own per-conversation record.
type conversationSummary struct {
	Title string
	State protocol.ConversationState
}

// userModel is UserActor's durable datamodel: the single (demo-only) user's
// whole workspace -- every conversation that has ever been created, and its
// last-known state.
type userModel struct {
	Conversations map[protocol.ConversationID]conversationSummary
}

func notAlreadyKnown(d *userModel, ec statecharts.ExecContext) bool {
	ev, _ := ec.Event()
	payload, ok := statecharts.Payload[*registerConversationPayload](ev)
	if !ok {
		return false
	}
	_, known := d.Conversations[payload.Value.ID]
	return !known
}

var addConversation = statecharts.Action(func(d *userModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	payload, ok := statecharts.Payload[*registerConversationPayload](ev)
	if !ok {
		return nil
	}
	d.Conversations[payload.Value.ID] = conversationSummary{Title: payload.Value.Title, State: protocol.ConversationIdle}
	return nil
})

func isKnownConversation(d *userModel, ec statecharts.ExecContext) bool {
	ev, _ := ec.Event()
	payload, ok := statecharts.Payload[*conversationStatePayload](ev)
	if !ok {
		return false
	}
	_, known := d.Conversations[payload.Value.ID]
	return known
}

var updateConversationState = statecharts.Action(func(d *userModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	payload, ok := statecharts.Payload[*conversationStatePayload](ev)
	if !ok {
		return nil
	}
	summary := d.Conversations[payload.Value.ID]
	summary.State = payload.Value.State
	d.Conversations[payload.Value.ID] = summary
	return nil
})

func syncOne(ec statecharts.ExecContext, id protocol.ConversationID, summary conversationSummary) {
	ec.Send("sync", statecharts.SendOptions{
		Target: "directory",
		Data:   protocol.ConversationSummary{ID: id, Title: summary.Title, State: summary.State},
	})
}

// forwardSync tells DirectoryActor about whichever conversation this
// action's own event concerned, so it stays current for as long as both
// actors are live. Not JSON-wrapped -- "directory" is non-durable, never
// logged; during UserActor's own replay after a restart this Send is
// correctly suppressed like any other real dispatch (see replayGate).
var forwardSync = statecharts.Action(func(d *userModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	var id protocol.ConversationID
	switch payload := ev.Data.(type) {
	case *registerConversationPayload:
		id = payload.Value.ID
	case *conversationStatePayload:
		id = payload.Value.ID
	default:
		return nil
	}
	summary, ok := d.Conversations[id]
	if !ok {
		return nil
	}
	syncOne(ec, id, summary)
	return nil
})

// forwardSyncAll re-sends every known conversation to DirectoryActor, by
// ordinary actor Send, exactly like forwardSync does for one -- used once,
// at startup, to prime DirectoryActor's mirror from UserActor's own
// already-rehydrated state (see cmd/ai-agent's startup wiring), rather than
// DirectoryActor ever reading UserActor's Log directly.
var forwardSyncAll = statecharts.Action(func(d *userModel, ec statecharts.ExecContext) error {
	for id, summary := range d.Conversations {
		syncOne(ec, id, summary)
	}
	return nil
})

// UserKind is the chart kind name the durable, singleton "user" actor is
// Registered and Spawned under.
const UserKind statecharts.Identifier = "user"

// BuildUserChart returns the durable "user" singleton: the one (demo-only)
// user's whole workspace, recording every conversation ever created and its
// last-known state, surviving a restart the same way any other durable
// actor does.
func BuildUserChart() (*statecharts.Chart, error) {
	return statecharts.Build(
		statecharts.Atomic("user",
			statecharts.On("register",
				statecharts.If(statecharts.Cond(notAlreadyKnown)),
				statecharts.Then(addConversation, forwardSync),
			),
			statecharts.On("state_changed",
				statecharts.If(statecharts.Cond(isKnownConversation)),
				statecharts.Then(updateConversationState, forwardSync),
			),
			statecharts.On("bootstrap_directory", statecharts.Then(forwardSyncAll)),
		),
		statecharts.WithNewDatamodel(func() any {
			return &userModel{Conversations: map[protocol.ConversationID]conversationSummary{}}
		}),
	)
}
