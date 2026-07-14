package server

import (
	"sort"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// directoryModel is DirectoryActor's (non-durable) datamodel: a live mirror
// of UserActor's own conversation map, safe to query synchronously from an
// HTTP handler (a durable actor like UserActor cannot be -- see
// system.deliver's unconditional write-ahead logging, which fails outright
// on a channel-typed Event.Data). Watchers holds one channel per active
// GET /directory/events request (see http.go's handleDirectoryEvents):
// registered directly with this actor's own datamodel exactly like ui.go's
// browser-subscriber channels, safe here for the same reason (non-durable,
// never logged) -- no separate per-request actor needed. Each watcher's
// channel only ever carries a single changed entry (see broadcastUpsert),
// not the whole map re-serialized -- a workspace with hundreds of
// conversations shouldn't re-transmit hundreds of records because one of
// them changed state. The one-time full list a fresh watcher needs is
// served separately, by "list" (see replyWithList and
// http.go's handleDirectoryEvents priming its stream before ever reading
// from this channel).
type directoryModel struct {
	Items    map[protocol.ConversationID]protocol.ConversationSummary
	Watchers []chan protocol.ConversationSummary
}

func snapshotList(d *directoryModel) []protocol.ConversationSummary {
	items := make([]protocol.ConversationSummary, 0, len(d.Items))
	for _, cs := range d.Items {
		items = append(items, cs)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Title < items[j].Title })
	return items
}

func broadcastUpsert(d *directoryModel, cs protocol.ConversationSummary) {
	for _, ch := range d.Watchers {
		select {
		case ch <- cs:
		default: // a slow/gone watcher never blocks this actor's own goroutine
		}
	}
}

var applySync = statecharts.Action(func(d *directoryModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	payload, ok := statecharts.Payload[*directorySyncPayload](ev)
	if !ok {
		return nil
	}
	cs := payload.Value
	d.Items[cs.ID] = cs
	broadcastUpsert(d, cs)
	return nil
})

var replyWithList = statecharts.Action(func(d *directoryModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	reply, ok := statecharts.Payload[chan<- []protocol.ConversationSummary](ev)
	if !ok {
		return nil
	}
	reply <- snapshotList(d)
	return nil
})

// directoryWatchRequest is "watch"'s payload -> directory, from a freshly
// opened GET /directory/events request. Reply carries back the channel that
// request's own goroutine reads one upserted protocol.ConversationSummary
// from at a time, for as long as the request lasts, until "unwatch".
type directoryWatchRequest struct {
	Reply chan<- chan protocol.ConversationSummary
}

var watchDirectory = statecharts.Action(func(d *directoryModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	req, ok := statecharts.Payload[directoryWatchRequest](ev)
	if !ok {
		return nil
	}
	ch := make(chan protocol.ConversationSummary, 8)
	d.Watchers = append(d.Watchers, ch)
	req.Reply <- ch
	return nil
})

// directoryUnwatchRequest is "unwatch"'s payload -> directory, sent when a
// GET /directory/events request ends.
type directoryUnwatchRequest struct {
	Channel chan protocol.ConversationSummary
}

var unwatchDirectory = statecharts.Action(func(d *directoryModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	req, ok := statecharts.Payload[directoryUnwatchRequest](ev)
	if !ok {
		return nil
	}
	for i, ch := range d.Watchers {
		if ch == req.Channel {
			d.Watchers = append(d.Watchers[:i], d.Watchers[i+1:]...)
			break
		}
	}
	return nil
})

// DirectoryKind is the chart kind name the non-durable, singleton
// "directory" actor is Registered and Spawned under.
const DirectoryKind statecharts.Identifier = "directory"

// BuildDirectoryChart returns the non-durable "directory" singleton: a live
// mirror of UserActor's conversation map, safe for GET /conversations to
// query synchronously via a reply channel (see http.go). It is primed at
// startup by UserActor itself forwarding its whole map via ordinary Sends
// (see user.go's forwardSyncAll and cmd/ai-agent's startup wiring) -- never
// by reading UserActor's Log directly. Every "sync" also broadcasts the
// fresh full list to every GET /directory/events watcher, so a client's own
// sidebar is push-driven, not polled (see http.go's handleDirectoryEvents
// and internal/client's directorylink).
func BuildDirectoryChart() (*statecharts.Chart, error) {
	return statecharts.Build(
		statecharts.Atomic("directory",
			statecharts.On("sync", statecharts.Then(applySync)),
			statecharts.On("list", statecharts.Then(replyWithList)),
			statecharts.On("watch", statecharts.Then(watchDirectory)),
			statecharts.On("unwatch", statecharts.Then(unwatchDirectory)),
		),
		statecharts.WithNewDatamodel(func() any {
			return &directoryModel{Items: map[protocol.ConversationID]protocol.ConversationSummary{}}
		}), statecharts.WithVersion("v1"))
}
