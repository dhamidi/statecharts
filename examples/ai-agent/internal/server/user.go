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

func notAlreadyKnown(d *userModel, ec statecharts.ExecContext, _ []statecharts.Value) (bool, error) {
	ev, _ := ec.Event()
	payload, ok := decodeRegister(ev.Data)
	if !ok {
		return false, nil
	}
	_, known := d.Conversations[payload.ID]
	return !known, nil
}

func addConversation(d *userModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	payload, ok := decodeRegister(ev.Data)
	if !ok {
		return nil
	}
	d.Conversations[payload.ID] = conversationSummary{Title: payload.Title, State: protocol.ConversationIdle}
	return nil
}

func isKnownConversation(d *userModel, ec statecharts.ExecContext, _ []statecharts.Value) (bool, error) {
	ev, _ := ec.Event()
	payload, ok := decodeConversationState(ev.Data)
	if !ok {
		return false, nil
	}
	_, known := d.Conversations[payload.ID]
	return known, nil
}

func updateConversationState(d *userModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	payload, ok := decodeConversationState(ev.Data)
	if !ok {
		return nil
	}
	summary := d.Conversations[payload.ID]
	summary.State = payload.State
	d.Conversations[payload.ID] = summary
	return nil
}

func syncOne(ec statecharts.ExecContext, id protocol.ConversationID, summary conversationSummary) {
	ec.Send("sync", statecharts.SendOptions{
		Target: "directory",
		Data:   encodeSummary(protocol.ConversationSummary{ID: id, Title: summary.Title, State: summary.State}),
	})
}

// forwardSync tells DirectoryActor about whichever conversation this
// action's own event concerned, so it stays current for as long as both
// actors are live. Not JSON-wrapped -- "directory" is non-durable, never
// logged; during UserActor's own replay after a restart this Send is
// correctly suppressed like any other real dispatch (see replayGate).
func forwardSync(d *userModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	var id protocol.ConversationID
	if payload, ok := decodeRegister(ev.Data); ok {
		id = payload.ID
	} else if payload, ok := decodeConversationState(ev.Data); ok {
		id = payload.ID
	} else {
		return nil
	}
	summary, ok := d.Conversations[id]
	if !ok {
		return nil
	}
	syncOne(ec, id, summary)
	return nil
}

// forwardSyncAll re-sends every known conversation to DirectoryActor, by
// ordinary actor Send, exactly like forwardSync does for one -- used once,
// at startup, to prime DirectoryActor's mirror from UserActor's own
// already-rehydrated state (see cmd/ai-agent's startup wiring), rather than
// DirectoryActor ever reading UserActor's Log directly.
func forwardSyncAll(d *userModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	for id, summary := range d.Conversations {
		syncOne(ec, id, summary)
	}
	return nil
}

// UserKind is the chart kind name the durable, singleton "user" actor is
// Registered and Spawned under.
const UserKind statecharts.Identifier = "user"

// BuildUserChart returns the durable "user" singleton: the one (demo-only)
// user's whole workspace, recording every conversation ever created and its
// last-known state, surviving a restart the same way any other durable
// actor does.
func BuildUserChart() (*statecharts.Chart, error) {
	model := statecharts.NewGoModel(func() *userModel {
		return &userModel{Conversations: map[protocol.ConversationID]conversationSummary{}}
	})
	condition := func(operation string, fn statecharts.GoCondition[userModel]) (statecharts.GoConditionRef, error) {
		return model.Condition(statecharts.Identifier("ai-agent.server.user."+operation), "v1", fn)
	}
	action := func(operation string, fn statecharts.GoAction[userModel]) (statecharts.GoActionRef, error) {
		return model.Action(statecharts.Identifier("ai-agent.server.user."+operation), "v1", fn)
	}
	notKnown, err := condition("not-already-known", notAlreadyKnown)
	if err != nil {
		return nil, err
	}
	known, err := condition("is-known-conversation", isKnownConversation)
	if err != nil {
		return nil, err
	}
	add, err := action("add-conversation", addConversation)
	if err != nil {
		return nil, err
	}
	update, err := action("update-conversation-state", updateConversationState)
	if err != nil {
		return nil, err
	}
	forward, err := action("forward-sync", forwardSync)
	if err != nil {
		return nil, err
	}
	forwardAll, err := action("forward-sync-all", forwardSyncAll)
	if err != nil {
		return nil, err
	}
	return buildCanonicalChart(
		statecharts.Atomic("user",
			statecharts.On("register",
				statecharts.If(notKnown.If()),
				statecharts.Then(add.Do(), forward.Do()),
			),
			statecharts.On("state_changed",
				statecharts.If(known.If()),
				statecharts.Then(update.Do(), forward.Do()),
			),
			statecharts.On("bootstrap_directory", statecharts.Then(forwardAll.Do())),
		),
		model, statecharts.WithRevisionSalt("user-v1"))
}
