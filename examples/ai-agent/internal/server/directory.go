package server

import (
	"sort"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// directoryModel is DirectoryActor's datamodel: a live mirror
// of UserActor's own conversation map, safe to query synchronously from an
// HTTP handler. RequestRegistry resolves canonical request IDs to one channel
// per active GET /directory/events request (see http.go's
// handleDirectoryEvents), keyed by this actor's session so capabilities never
// enter Event.Data or model snapshots. Each watcher's channel only ever
// carries a single changed entry (see broadcastUpsert),
// not the whole map re-serialized -- a workspace with hundreds of
// conversations shouldn't re-transmit hundreds of records because one of
// them changed state. The one-time full list a fresh watcher needs is
// served separately, by "list" (see replyWithList and
// http.go's handleDirectoryEvents priming its stream before ever reading
// from this channel).
type directoryModel struct {
	Items map[protocol.ConversationID]protocol.ConversationSummary
}

func snapshotList(d *directoryModel) []protocol.ConversationSummary {
	items := make([]protocol.ConversationSummary, 0, len(d.Items))
	for _, cs := range d.Items {
		items = append(items, cs)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Title < items[j].Title })
	return items
}

func broadcastUpsert(requests *RequestRegistry, session string, cs protocol.ConversationSummary) {
	for _, ch := range requests.directoryWatchers(session) {
		select {
		case ch <- cs:
		default: // a slow/gone watcher never blocks this actor's own goroutine
		}
	}
}

func applyDirectorySync(requests *RequestRegistry) statecharts.GoAction[directoryModel] {
	return func(d *directoryModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		payload, ok := decodeSummary(ev.Data)
		if !ok {
			return nil
		}
		cs := payload
		d.Items[cs.ID] = cs
		broadcastUpsert(requests, ec.SessionID(), cs)
		return nil
	}
}

func replyWithList(requests *RequestRegistry) statecharts.GoAction[directoryModel] {
	return func(d *directoryModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		id, ok := decodeDirectoryRequest(ev.Data)
		if !ok {
			return nil
		}
		if reply, ok := requests.takeList(id); ok {
			reply <- snapshotList(d)
		}
		return nil
	}
}
func watchDirectory(requests *RequestRegistry) statecharts.GoAction[directoryModel] {
	return func(d *directoryModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		id, ok := decodeDirectoryRequest(ev.Data)
		if !ok {
			return nil
		}
		reply, ok := requests.takeWatch(id)
		if !ok {
			return nil
		}
		ch := make(chan protocol.ConversationSummary, 8)
		requests.addDirectoryWatcher(ec.SessionID(), ch)
		reply <- ch
		return nil
	}
}
func unwatchDirectory(requests *RequestRegistry) statecharts.GoAction[directoryModel] {
	return func(_ *directoryModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		id, ok := decodeDirectoryRequest(ev.Data)
		if !ok {
			return nil
		}
		target, ok := requests.takeUnwatch(id)
		if !ok {
			return nil
		}
		requests.removeDirectoryWatcher(ec.SessionID(), target)
		return nil
	}
}

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
func BuildDirectoryChart(requests *RequestRegistry) (*statecharts.Chart, error) {
	model := statecharts.NewGoModel(func() *directoryModel {
		return &directoryModel{Items: map[protocol.ConversationID]protocol.ConversationSummary{}}
	})
	applySync, err := model.Action("ai-agent.server.directory.apply-sync", "v1", applyDirectorySync(requests))
	if err != nil {
		return nil, err
	}
	replyList, err := model.Action("ai-agent.server.directory.reply-list", "v1", replyWithList(requests))
	if err != nil {
		return nil, err
	}
	watch, err := model.Action("ai-agent.server.directory.watch", "v1", watchDirectory(requests))
	if err != nil {
		return nil, err
	}
	unwatch, err := model.Action("ai-agent.server.directory.unwatch", "v1", unwatchDirectory(requests))
	if err != nil {
		return nil, err
	}
	return buildCanonicalChart(
		statecharts.Atomic("directory",
			statecharts.On("sync", statecharts.Then(applySync.Do())),
			statecharts.On("list", statecharts.Then(replyList.Do())),
			statecharts.On("watch", statecharts.Then(watch.Do())),
			statecharts.On("unwatch", statecharts.Then(unwatch.Do())),
		),
		model, statecharts.WithRevisionSalt("v1"))
}
