package client

import (
	"context"
	"sort"
	"strings"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// uiModel is UIServerActor's (non-durable) datamodel: whatever the browser
// should see for the currently selected conversation, reset on every
// "conversation_switched". Messages is keyed by each entry's 1-based
// position in the conversation's own History (the same numbering
// broadcastLastMessage and replyWithCatchup use server-side, carried here
// via the SSE "id:" field -- see link.go's messageWithSeq) rather than a
// plain append-ordered slice: message delivery hops through several
// independent asynchronous actor Sends (ConversationActor -> fanout ->
// connection -> link -> ui), each its own goroutine with no ordering
// guarantee relative to the others, so two messages can arrive at ui in
// a different order than they were produced. Keying by seq and
// reconstructing the slice in seq order at snapshot time makes display
// order correct regardless of arrival order.
//
// Subscribers holds one channel per connected browser tab's own /events
// SSE stream (see httpui.go's handleDatastarEvents): every action below
// that changes something the main pane or link banner shows also renders
// the fresh fragment and pushes it to each one -- Datastar's own
// datastar-patch-elements protocol -- so an already-open tab updates
// itself in real time, without polling.
type uiModel struct {
	ConversationID  protocol.ConversationID
	Messages        map[int]protocol.MessageFrame
	MaxSeq          int
	ThinkingDelta   string
	TextDelta       string
	PendingToolCall *protocol.ToolCallFrame
	LinkStatus      string // "" (not yet heard from link) | "idle" | "connecting" | "connected" | "reconnecting"

	// Conversations is the sidebar's own data: the whole workspace's
	// conversation list, kept current by directorylink's single upstream
	// SSE subscription (see directorylink.go) rather than this actor (or
	// any browser tab) ever polling for it.
	Conversations []protocol.ConversationSummary

	Subscribers []chan string
}

func newUIModel() *uiModel {
	// LinkStatus starts "" (not "connecting"): link.go's LinkActor actually
	// starts in "idle" and stays there until the first "switch", emitting
	// its own "link_status" almost immediately after this actor spawns (see
	// link.go's reportIdle) -- so "" only lasts for the brief startup window
	// before the real state arrives, rather than being a guess that could
	// otherwise sit uncorrected forever (see renderLinkBanner, which treats
	// "" the same as "idle").
	return &uiModel{Messages: map[int]protocol.MessageFrame{}}
}

// uiSnapshot is "get_snapshot"'s reply payload: a point-in-time copy of
// uiModel, safe to read from the HTTP handler's own goroutine (see
// runHTTPServer) because it's a plain value, not a live reference into the
// actor's own datamodel.
type uiSnapshot struct {
	ConversationID  protocol.ConversationID
	Messages        []protocol.MessageFrame
	ThinkingDelta   string
	TextDelta       string
	PendingToolCall *protocol.ToolCallFrame
	LinkStatus      string
	Conversations   []protocol.ConversationSummary
}

func snapshotOf(d *uiModel) uiSnapshot {
	messages := make([]protocol.MessageFrame, 0, len(d.Messages))
	for seq := 1; seq <= d.MaxSeq; seq++ {
		if m, ok := d.Messages[seq]; ok {
			messages = append(messages, m)
		}
	}
	return uiSnapshot{
		ConversationID:  d.ConversationID,
		Messages:        messages,
		ThinkingDelta:   d.ThinkingDelta,
		TextDelta:       d.TextDelta,
		PendingToolCall: d.PendingToolCall,
		LinkStatus:      d.LinkStatus,
		Conversations:   d.Conversations,
	}
}

// datastarPatch formats one datastar-patch-elements SSE event -- see
// https://data-star.dev/reference/sse_events. elementHTML must be a single
// top-level element carrying the `id` Datastar morphs it into place by.
//
// elementHTML routinely contains embedded raw newlines -- e.g. any
// shell_command output ends up as message text rendered verbatim into a
// bubble, and shell output almost always ends in "\n" -- so a single
// "data:" line is NOT always enough: per the SSE framing Datastar's own
// client parses (see the vendored static/datastar.js's onmessage, which
// reconstructs a multi-line field by splitting the whole event's data on
// "\n" and grouping lines by their leading keyword), every line of a
// multi-line value must repeat the "elements " keyword, or the client
// silently misparses/truncates everything after the first embedded
// newline -- observed live as a conversation's final assistant reply (the
// turn following a tool call, whose History includes the tool's own,
// newline-terminated output) never reaching the browser, with the compose
// form vanishing along with it, no console error, and no server-side
// symptom at all (the server-side actor state reaches "idle" correctly;
// only this client-to-browser SSE framing was ever broken).
func datastarPatch(elementHTML string) string {
	var b strings.Builder
	b.WriteString("event: datastar-patch-elements\n")
	for _, line := range strings.Split(elementHTML, "\n") {
		b.WriteString("data: elements ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.String()
}

// pushMain re-renders the main pane from d's current state and pushes it
// to every connected browser tab.
func pushMain(d *uiModel) {
	broadcast(d, datastarPatch(renderToString(renderMain(snapshotOf(d)))))
}

// pushSidebar re-renders just the sidebar and pushes it to every connected
// browser tab, over each tab's own already-open /events connection -- see
// applyDirectorySnapshot below and directorylink.go's own doc comment on
// why this is a push, not a poll, and why it costs no extra connection.
func pushSidebar(d *uiModel) {
	broadcast(d, datastarPatch(renderToString(renderSidebar(snapshotOf(d)))))
}

func broadcast(d *uiModel, frame string) {
	for _, ch := range d.Subscribers {
		select {
		case ch <- frame:
		default: // a slow/gone tab never blocks this actor's own goroutine
		}
	}
}

var appendMessage = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	m, ok := statecharts.Payload[messageWithSeq](ev)
	if !ok {
		return nil
	}
	d.Messages[m.Seq] = m.Frame
	if m.Seq > d.MaxSeq {
		d.MaxSeq = m.Seq
	}
	d.ThinkingDelta = ""
	d.TextDelta = ""
	d.PendingToolCall = nil
	pushMain(d)
	return nil
})

var appendDelta = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	delta, ok := statecharts.Payload[protocol.DeltaFrame](ev)
	if !ok {
		return nil
	}
	if delta.Kind == "thinking" {
		d.ThinkingDelta += delta.Text
	} else {
		d.TextDelta += delta.Text
	}
	pushMain(d)
	return nil
})

var appendToolCall = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	tc, ok := statecharts.Payload[protocol.ToolCallFrame](ev)
	if !ok {
		return nil
	}
	d.PendingToolCall = &tc
	pushMain(d)
	return nil
})

var recordLinkStatus = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	status, ok := statecharts.Payload[string](ev)
	if !ok {
		return nil
	}
	d.LinkStatus = status
	broadcast(d, datastarPatch(renderToString(renderLinkBanner(d.LinkStatus))))
	return nil
})

var applyDirectorySnapshot = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	items, ok := statecharts.Payload[[]protocol.ConversationSummary](ev)
	if !ok {
		return nil
	}
	d.Conversations = items
	pushSidebar(d)
	return nil
})

// applyDirectoryUpsert handles one changed conversation at a time (see
// directorylink.go's forwardDirectoryUpsert, fed by the workspace server's
// own diff-based broadcast -- see server/directory.go's broadcastUpsert):
// replacing or inserting just that entry and re-sorting by title, rather
// than waiting for (or requesting) a fresh full list on every single
// change, however many other conversations exist.
var applyDirectoryUpsert = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	cs, ok := statecharts.Payload[protocol.ConversationSummary](ev)
	if !ok {
		return nil
	}
	replaced := false
	for i, existing := range d.Conversations {
		if existing.ID == cs.ID {
			d.Conversations[i] = cs
			replaced = true
			break
		}
	}
	if !replaced {
		d.Conversations = append(d.Conversations, cs)
	}
	sort.Slice(d.Conversations, func(i, j int) bool { return d.Conversations[i].Title < d.Conversations[j].Title })
	pushSidebar(d)
	return nil
})

// resetForSwitch is the actual "switch conversations" state change: cleared
// exactly once per real switch, shared by applySwitch (driven by
// LinkActor's own async "conversation_switched", see link.go's
// handleSwitch) and switchAndReplySnapshot (driven synchronously by
// httpui.go's handleIndex -- see that action's own doc comment for why
// there are two entry points into the same reset). Idempotent: a no-op
// once d is already showing id, so whichever of the two callers gets there
// first "wins" and the other is harmless.
func resetForSwitch(d *uiModel, id protocol.ConversationID) bool {
	if id == "" || d.ConversationID == id {
		return false
	}
	d.ConversationID = id
	d.Messages = map[int]protocol.MessageFrame{}
	d.MaxSeq = 0
	d.ThinkingDelta = ""
	d.TextDelta = ""
	d.PendingToolCall = nil
	pushMain(d)
	pushSidebar(d) // the sidebar's own "active" highlight tracks d.ConversationID
	return true
}

var applySwitch = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	id, ok := statecharts.Payload[protocol.ConversationID](ev)
	if !ok {
		return nil
	}
	resetForSwitch(d, id) // no-op if httpui.go's own switch_and_snapshot already applied this
	return nil
})

var replySnapshot = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	reply, ok := statecharts.Payload[chan<- uiSnapshot](ev)
	if !ok {
		return nil
	}
	reply <- snapshotOf(d)
	return nil
})

// browserSubscribeRequest is "subscribe_browser"'s payload -> ui, from a
// freshly opened /events SSE request (see httpui.go's
// handleDatastarEvents). Reply carries back the channel that request's own
// goroutine reads patch frames from -- the same request/reply-over-a-
// channel idiom get_snapshot uses, safe here because "ui" is non-durable.
type browserSubscribeRequest struct {
	Reply chan<- chan string
}

var subscribeBrowser = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	req, ok := statecharts.Payload[browserSubscribeRequest](ev)
	if !ok {
		return nil
	}
	ch := make(chan string, 32)
	d.Subscribers = append(d.Subscribers, ch)
	req.Reply <- ch
	return nil
})

// browserUnsubscribeRequest is "unsubscribe_browser"'s payload -> ui, sent
// when a browser tab's own /events request ends (tab closed, navigated
// away, reloaded).
type browserUnsubscribeRequest struct {
	Channel chan string
}

var unsubscribeBrowser = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	req, ok := statecharts.Payload[browserUnsubscribeRequest](ev)
	if !ok {
		return nil
	}
	for i, ch := range d.Subscribers {
		if ch == req.Channel {
			d.Subscribers = append(d.Subscribers[:i], d.Subscribers[i+1:]...)
			break
		}
	}
	return nil
})

func getUISnapshot(ctx context.Context, sys *actors.System) (uiSnapshot, error) {
	reply := make(chan uiSnapshot, 1)
	if err := sys.Tell(ctx, "ui", statecharts.Event{
		Name: "get_snapshot", Type: statecharts.EventExternal, Data: (chan<- uiSnapshot)(reply),
	}); err != nil {
		return uiSnapshot{}, err
	}
	select {
	case snap := <-reply:
		return snap, nil
	case <-ctx.Done():
		return uiSnapshot{}, ctx.Err()
	}
}

// switchAndSnapshotRequest is "switch_and_snapshot"'s payload -> ui, from
// httpui.go's handleIndex when a request names a conversation via
// ?conversation=. It exists so handleIndex can ask "ui" -- and only "ui" --
// to both apply the switch (if any) and hand back the resulting snapshot in
// one atomic actor-message round trip, rather than handleIndex reaching
// into LinkActor's own job of announcing "conversation_switched" itself
// (that stays link.go's handleSwitch's alone, sent asynchronously once link
// actually processes the "switch" this same request also Tells it -- see
// applySwitch, which no-ops if switchAndReplySnapshot already got there
// first).
type switchAndSnapshotRequest struct {
	ConversationID protocol.ConversationID
	Reply          chan<- uiSnapshot
}

var switchAndReplySnapshot = statecharts.Action(func(d *uiModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	req, ok := statecharts.Payload[switchAndSnapshotRequest](ev)
	if !ok {
		return nil
	}
	resetForSwitch(d, req.ConversationID)
	req.Reply <- snapshotOf(d)
	return nil
})

// getUISnapshotForSwitch is switch_and_snapshot's own request/reply-over-a-
// channel helper -- the same idiom getUISnapshot above uses for
// get_snapshot -- for httpui.go's handleIndex: it both makes sure "ui" is
// already showing id (applying the same reset applySwitch would, exactly
// once) and returns the fresh snapshot to render, atomically, so the page
// rendered in response to a ?conversation= navigation never shows a stale
// conversation for one request. Separately, and still, the caller must
// Tell "link" to "switch" so it actually redials SSE for id -- this call
// only concerns what "ui" shows.
func getUISnapshotForSwitch(ctx context.Context, sys *actors.System, id protocol.ConversationID) (uiSnapshot, error) {
	reply := make(chan uiSnapshot, 1)
	if err := sys.Tell(ctx, "ui", statecharts.Event{
		Name: "switch_and_snapshot", Type: statecharts.EventExternal,
		Data: switchAndSnapshotRequest{ConversationID: id, Reply: reply},
	}); err != nil {
		return uiSnapshot{}, err
	}
	select {
	case snap := <-reply:
		return snap, nil
	case <-ctx.Done():
		return uiSnapshot{}, ctx.Err()
	}
}

// UIKind is the chart kind name the client's singleton "ui" actor is
// Registered and Spawned under.
const UIKind statecharts.Identifier = "ui"

// BuildUIChart returns the client's "ui" chart: a single state holding the
// local HTTP server as its own long-running Invoke, and the handlers that
// keep uiModel current as LinkActor forwards server traffic -- pushing a
// live Datastar patch to every connected browser tab each time.
func BuildUIChart(sys *actors.System, serverAddr string) (*statecharts.Chart, error) {
	runServer := buildRunHTTPServer(sys, serverAddr)
	return statecharts.Build(
		statecharts.Atomic("ui",
			statecharts.Invoke(runServer),
			statecharts.On("append_message", statecharts.Then(appendMessage)),
			statecharts.On("append_delta", statecharts.Then(appendDelta)),
			statecharts.On("append_tool_call", statecharts.Then(appendToolCall)),
			statecharts.On("link_status", statecharts.Then(recordLinkStatus)),
			statecharts.On("directory_snapshot", statecharts.Then(applyDirectorySnapshot)),
			statecharts.On("directory_upsert", statecharts.Then(applyDirectoryUpsert)),
			statecharts.On("conversation_switched", statecharts.Then(applySwitch)),
			statecharts.On("get_snapshot", statecharts.Then(replySnapshot)),
			statecharts.On("switch_and_snapshot", statecharts.Then(switchAndReplySnapshot)),
			statecharts.On("subscribe_browser", statecharts.Then(subscribeBrowser)),
			statecharts.On("unsubscribe_browser", statecharts.Then(unsubscribeBrowser)),
		),
		statecharts.WithNewDatamodel(func() any { return newUIModel() }), statecharts.WithVersion("v1"))
}
