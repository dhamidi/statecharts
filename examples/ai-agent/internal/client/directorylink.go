package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// directoryLinkModel is DirectoryLinkActor's (non-durable) datamodel: just
// enough to reconnect with backoff, mirroring linkModel's own shape.
type directoryLinkModel struct {
	ServerAddr     string
	BackoffAttempt int
}

// dialDirectoryEvents opens (or reopens, on reconnect) the server's single
// GET /directory/events stream and delivers "directory_frame" for a "list"
// event (the whole workspace, used once to (re)seed local state after every
// connect or reconnect) or "directory_upsert" for a "conversation" event
// (one changed entry -- see http.go's handleDirectoryEvents, which sends
// the whole list only once per connection and one entry per change
// thereafter), until the stream ends, errors, or ctx is cancelled. This is
// the ONE connection this whole client process holds for sidebar data, no
// matter how many browser tabs its own local UI server is serving (see
// ui.go's Subscribers fanout) -- opening more browser tabs never opens more
// connections to the workspace server, sidestepping the browser's own
// six-connections-per-origin limit on the *other* side of this client
// entirely, since it isn't the browser holding this connection at all.
func dialDirectoryEvents(ctx context.Context, params any, io statecharts.InvokeIO) (any, error) {
	p, _ := params.(directoryLinkModel)

	u, err := url.Parse(p.ServerAddr)
	if err != nil {
		return nil, err
	}
	u = u.JoinPath("directory", "events")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("client: dial %s: unexpected status %s", u, resp.Status)
	}
	io.Deliver(statecharts.Event{Name: "connected"})

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var event, data string
	flush := func() {
		if event == "" && data == "" {
			return
		}
		switch event {
		case "list":
			var items []protocol.ConversationSummary
			if err := json.Unmarshal([]byte(data), &items); err == nil {
				io.Deliver(statecharts.Event{Name: "directory_frame", Data: items})
			}
		case "conversation":
			var cs protocol.ConversationSummary
			if err := json.Unmarshal([]byte(data), &cs); err == nil {
				io.Deliver(statecharts.Event{Name: "directory_upsert", Data: cs})
			}
		}
		event, data = "", ""
	}
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			flush()
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("client: server closed the directory event stream")
}

func computeDirectoryInvokeParams(ec statecharts.ExecContext) any {
	d, _ := ec.Datamodel().(*directoryLinkModel)
	if d == nil {
		return directoryLinkModel{}
	}
	return *d
}

var forwardDirectorySnapshot = statecharts.Action(func(d *directoryLinkModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	items, ok := statecharts.Payload[[]protocol.ConversationSummary](ev)
	if !ok {
		return nil
	}
	ec.Send("directory_snapshot", statecharts.SendOptions{Target: "ui", Data: items})
	return nil
})

var forwardDirectoryUpsert = statecharts.Action(func(d *directoryLinkModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	cs, ok := statecharts.Payload[protocol.ConversationSummary](ev)
	if !ok {
		return nil
	}
	ec.Send("directory_upsert", statecharts.SendOptions{Target: "ui", Data: cs})
	return nil
})

var resetDirectoryBackoff = statecharts.Action(func(d *directoryLinkModel, ec statecharts.ExecContext) error {
	d.BackoffAttempt = 0
	return nil
})

var scheduleDirectoryReconnect = statecharts.Action(func(d *directoryLinkModel, ec statecharts.ExecContext) error {
	ec.Send("reconnect_timer", statecharts.SendOptions{Delay: backoffDelay(d.BackoffAttempt)})
	d.BackoffAttempt++
	return nil
})

// DirectoryLinkKind is the chart kind name the client's singleton
// "directorylink" actor is Registered and Spawned under.
const DirectoryLinkKind statecharts.Identifier = "directorylink"

// BuildDirectoryLinkChart returns the client's "directorylink" chart: a
// single, always-on SSE connection to the workspace server's whole
// conversation list (see dialDirectoryEvents), reconnecting with the same
// backoff schedule LinkActor uses, forwarding every fresh snapshot to "ui"
// as "directory_snapshot" so the sidebar is push-driven rather than polled.
// Unlike LinkActor there is no per-conversation targeting and nothing to
// switch: this connects the moment the client starts and stays connected
// for its whole lifetime.
func BuildDirectoryLinkChart(serverAddr string) (*statecharts.Chart, error) {
	return statecharts.Build(
		statecharts.Compound("directorylink", "connected",
			statecharts.Children(
				statecharts.Atomic("connected",
					statecharts.Invoke(dialDirectoryEvents, statecharts.WithInvokeParams(computeDirectoryInvokeParams)),
					statecharts.On("connected", statecharts.Then(resetDirectoryBackoff)),
					statecharts.On("directory_frame", statecharts.Then(forwardDirectorySnapshot)),
					statecharts.On("directory_upsert", statecharts.Then(forwardDirectoryUpsert)),
					statecharts.On(string(statecharts.ErrEventCommunication), statecharts.Target("backoff")),
				),
				statecharts.Atomic("backoff",
					statecharts.OnEntry(scheduleDirectoryReconnect),
					statecharts.On("reconnect_timer", statecharts.Target("connected")),
				),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &directoryLinkModel{ServerAddr: serverAddr} }), statecharts.WithVersion("v1"))
}
