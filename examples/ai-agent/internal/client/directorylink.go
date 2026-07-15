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

const directoryLinkInvokeType statecharts.Identifier = "ai-agent.client.directorylink.sse"

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
func dialDirectoryEvents(ctx context.Context, params statecharts.Value, io statecharts.InvokeIO) (statecharts.Value, error) {
	m, _ := fields(params, tagDirectoryParams)
	addr, _ := stringField(m, "server_addr")
	p := directoryLinkModel{ServerAddr: addr}

	u, err := url.Parse(p.ServerAddr)
	if err != nil {
		return statecharts.NullValue(), err
	}
	u = u.JoinPath("directory", "events")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return statecharts.NullValue(), err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return statecharts.NullValue(), err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return statecharts.NullValue(), fmt.Errorf("client: dial %s: unexpected status %s", u, resp.Status)
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
				io.Deliver(statecharts.Event{Name: "directory_frame", Data: summariesValue(items)})
			}
		case "conversation":
			var cs protocol.ConversationSummary
			if err := json.Unmarshal([]byte(data), &cs); err == nil {
				io.Deliver(statecharts.Event{Name: "directory_upsert", Data: summaryValue(cs)})
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
		return statecharts.NullValue(), err
	}
	return statecharts.NullValue(), fmt.Errorf("client: server closed the directory event stream")
}

func computeDirectoryInvokeParams(d *directoryLinkModel, _ statecharts.ExecContext, _ []statecharts.Value) (statecharts.Value, error) {
	return taggedMap(tagDirectoryParams, map[string]statecharts.Value{"server_addr": str(d.ServerAddr)}), nil
}

var forwardDirectorySnapshot = func(d *directoryLinkModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	items, ok := decodeSummaries(ev.Data)
	if !ok {
		return nil
	}
	ec.Send("directory_snapshot", statecharts.SendOptions{Target: "ui", Data: summariesValue(items)})
	return nil
}

var forwardDirectoryUpsert = func(d *directoryLinkModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	cs, ok := decodeSummary(ev.Data)
	if !ok {
		return nil
	}
	ec.Send("directory_upsert", statecharts.SendOptions{Target: "ui", Data: summaryValue(cs)})
	return nil
}

var resetDirectoryBackoff = func(d *directoryLinkModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	d.BackoffAttempt = 0
	return nil
}

var scheduleDirectoryReconnect = func(d *directoryLinkModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ec.Send("reconnect_timer", statecharts.SendOptions{Delay: backoffDelay(d.BackoffAttempt)})
	d.BackoffAttempt++
	return nil
}

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
	model := statecharts.NewGoModel(func() *directoryLinkModel { return &directoryLinkModel{ServerAddr: serverAddr} })
	action := func(name string, fn statecharts.GoAction[directoryLinkModel]) (statecharts.GoActionRef, error) {
		return model.Action(statecharts.Identifier("ai-agent.client.directorylink."+name), "v1", fn)
	}
	reset, err := action("reset-backoff", resetDirectoryBackoff)
	if err != nil {
		return nil, err
	}
	snapshot, err := action("forward-snapshot", forwardDirectorySnapshot)
	if err != nil {
		return nil, err
	}
	upsert, err := action("forward-upsert", forwardDirectoryUpsert)
	if err != nil {
		return nil, err
	}
	reconnect, err := action("schedule-reconnect", scheduleDirectoryReconnect)
	if err != nil {
		return nil, err
	}
	params, err := model.Value("ai-agent.client.directorylink.invoke-params", "v1", computeDirectoryInvokeParams)
	if err != nil {
		return nil, err
	}
	return buildCanonicalChart(
		statecharts.Compound("directorylink", "connected",
			statecharts.Children(
				statecharts.Atomic("connected",
					statecharts.Invoke(string(directoryLinkInvokeType), "directory-events", statecharts.WithInvokeContent(params.Get())),
					statecharts.On("connected", statecharts.Then(reset.Do())),
					statecharts.On("directory_frame", statecharts.Then(snapshot.Do())),
					statecharts.On("directory_upsert", statecharts.Then(upsert.Do())),
					statecharts.On(string(statecharts.ErrEventCommunication), statecharts.Target("backoff")),
				),
				statecharts.Atomic("backoff",
					statecharts.OnEntry(reconnect.Do()),
					statecharts.On("reconnect_timer", statecharts.Target("connected")),
				),
			),
		),
		model)
}
