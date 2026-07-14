// Package client implements the connect-side workspace: LinkActor (the SSE
// link to the server, with reconnect/backoff and conversation switching),
// ToolActor (executes shell_command tool calls this client advertises),
// and UIServerActor (the local browser UI).
package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// linkModel is LinkActor's (non-durable) datamodel.
type linkModel struct {
	ServerAddr     string
	Tools          []protocol.ToolName
	ConversationID protocol.ConversationID
	LastSeq        int
	BackoffAttempt int
}

// switchRequest is "switch"'s payload -> link, from UIServerActor (or
// main, for an initial --conversation).
type switchRequest struct {
	ConversationID protocol.ConversationID
}

// serverFrame is what dialSSE delivers as "server_frame"'s payload -- one
// decoded SSE event, whichever of Message/Delta/ToolCall EventName says it
// is.
type serverFrame struct {
	EventName string
	ID        string
	Message   *protocol.MessageFrame
	Delta     *protocol.DeltaFrame
	ToolCall  *protocol.ToolCallFrame
}

// messageWithSeq is "append_message"'s payload -> ui: a transcript entry
// together with its 1-based position in the conversation's own History
// (parsed from the SSE "id:" field), which ui uses to display messages in
// the right order regardless of the order they actually arrived in -- see
// uiModel's own doc comment on why arrival order isn't reliable.
type messageWithSeq struct {
	Seq   int
	Frame protocol.MessageFrame
}

// linkParams is what WithInvokeParams computes fresh every time "online"
// is (re-)entered, snapshotting whatever dialSSE needs to know at that
// moment (which conversation, and how far it's already caught up).
type linkParams struct {
	ServerAddr     string
	ConversationID protocol.ConversationID
	Tools          []protocol.ToolName
	LastSeq        int
}

func computeInvokeParams(ec statecharts.ExecContext) any {
	d, _ := ec.Datamodel().(*linkModel)
	if d == nil {
		return linkParams{}
	}
	return linkParams{
		ServerAddr:     d.ServerAddr,
		ConversationID: d.ConversationID,
		Tools:          d.Tools,
		LastSeq:        d.LastSeq,
	}
}

// dialSSE is LinkActor's Invoke: it opens (or resumes, via Last-Event-ID)
// the conversation's SSE stream and delivers "connected" once the response
// headers arrive, then "server_frame" for every subsequent event, until the
// stream ends, errors, or ctx is cancelled (the containing "online" state
// was exited -- a fresh "switch", or backoff). A non-nil return here
// becomes error.communication (SCXML 6.4.3) unless ctx was already done,
// in which case the interpreter core suppresses it -- see invoke.go -- so
// this never special-cases a clean cancellation itself.
func dialSSE(ctx context.Context, params any, io statecharts.InvokeIO) (any, error) {
	p, _ := params.(linkParams)
	if p.ConversationID == "" {
		return nil, fmt.Errorf("client: dialSSE: no conversation selected")
	}

	u, err := url.Parse(p.ServerAddr)
	if err != nil {
		return nil, err
	}
	u = u.JoinPath("conversations", p.ConversationID.String(), "events")
	if len(p.Tools) > 0 {
		names := make([]string, len(p.Tools))
		for i, t := range p.Tools {
			names[i] = t.String()
		}
		q := u.Query()
		q.Set("tools", strings.Join(names, ","))
		u.RawQuery = q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if p.LastSeq > 0 {
		req.Header.Set("Last-Event-ID", strconv.Itoa(p.LastSeq))
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
	var id, event, data string
	flush := func() {
		if event == "" && data == "" {
			return
		}
		io.Deliver(statecharts.Event{Name: "server_frame", Data: parseServerFrame(event, id, data)})
		id, event, data = "", "", ""
	}
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			flush()
		case strings.HasPrefix(line, "id: "):
			id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("client: server closed the event stream")
}

func parseServerFrame(event, id, data string) serverFrame {
	f := serverFrame{EventName: event, ID: id}
	switch event {
	case "message":
		var m protocol.MessageFrame
		_ = json.Unmarshal([]byte(data), &m)
		f.Message = &m
	case "delta":
		var dl protocol.DeltaFrame
		_ = json.Unmarshal([]byte(data), &dl)
		f.Delta = &dl
	case "tool_call":
		var t protocol.ToolCallFrame
		_ = json.Unmarshal([]byte(data), &t)
		f.ToolCall = &t
	}
	return f
}

var resetBackoff = statecharts.Action(func(d *linkModel, ec statecharts.ExecContext) error {
	d.BackoffAttempt = 0
	ec.Send("link_status", statecharts.SendOptions{Target: "ui", Data: "connected"})
	return nil
})

var reportReconnecting = statecharts.Action(func(d *linkModel, ec statecharts.ExecContext) error {
	ec.Send("link_status", statecharts.SendOptions{Target: "ui", Data: "reconnecting"})
	return nil
})

var dispatchFrame = statecharts.Action(func(d *linkModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	f, ok := statecharts.Payload[serverFrame](ev)
	if !ok {
		return nil
	}
	seq := 0
	if f.ID != "" {
		if n, err := strconv.Atoi(f.ID); err == nil {
			seq = n
			d.LastSeq = n
		}
	}
	switch f.EventName {
	case "message":
		if f.Message != nil {
			ec.Send("append_message", statecharts.SendOptions{Target: "ui", Data: messageWithSeq{Seq: seq, Frame: *f.Message}})
		}
	case "delta":
		if f.Delta != nil {
			ec.Send("append_delta", statecharts.SendOptions{Target: "ui", Data: *f.Delta})
		}
	case "tool_call":
		if f.ToolCall != nil {
			// The executor lease -- and so this connection -- is shared
			// across every conversation naming this tool, not scoped to
			// whichever one is currently selected in this client's own UI
			// (see protocol.ToolCallFrame's own doc comment): only show it
			// in the transcript if it's actually for the conversation
			// being displayed, but always execute it regardless, using the
			// frame's own ConversationID -- never d.ConversationID -- to
			// post the result back to the right place.
			if f.ToolCall.ConversationID == d.ConversationID {
				ec.Send("append_tool_call", statecharts.SendOptions{Target: "ui", Data: *f.ToolCall})
			}
			ec.Send("execute", statecharts.SendOptions{
				Target: "tool",
				Data:   executeRequest{ConversationID: f.ToolCall.ConversationID, Call: *f.ToolCall},
			})
		}
	}
	return nil
})

// backoffDelay grows 500ms, 1s, 2s, 4s, ... capped at 10s.
func backoffDelay(attempt int) time.Duration {
	d := 500 * time.Millisecond
	for i := 0; i < attempt; i++ {
		d *= 2
		if d >= 10*time.Second {
			return 10 * time.Second
		}
	}
	return d
}

var scheduleReconnect = statecharts.Action(func(d *linkModel, ec statecharts.ExecContext) error {
	ec.Send("reconnect_timer", statecharts.SendOptions{Delay: backoffDelay(d.BackoffAttempt)})
	d.BackoffAttempt++
	return nil
})

var handleSwitch = statecharts.Action(func(d *linkModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	sw, ok := statecharts.Payload[switchRequest](ev)
	if !ok {
		return nil
	}
	d.ConversationID = sw.ConversationID
	d.LastSeq = 0
	d.BackoffAttempt = 0
	ec.Send("conversation_switched", statecharts.SendOptions{Target: "ui", Data: sw.ConversationID})
	return nil
})

// LinkKind is the chart kind name the client's singleton "link" actor is
// Registered and Spawned under.
const LinkKind statecharts.Identifier = "link"

// BuildLinkChart returns the client's "link" chart: idle until the first
// "switch", then online/connecting/connected with automatic
// reconnect-with-backoff, or offline in "backoff" between attempts.
// serverAddr and tools are fixed for this client process's whole lifetime.
func BuildLinkChart(serverAddr string, tools []protocol.ToolName) (*statecharts.Chart, error) {
	return statecharts.Build(
		statecharts.Compound("link", "idle",
			statecharts.Children(
				statecharts.Atomic("idle"),
				statecharts.Compound("online", "connecting",
					statecharts.Children(
						statecharts.Atomic("connecting",
							statecharts.On("connected", statecharts.Target("connected"), statecharts.Then(resetBackoff)),
						),
						statecharts.Atomic("connected",
							statecharts.On("server_frame", statecharts.Then(dispatchFrame)),
						),
					),
					statecharts.Invoke(dialSSE, statecharts.WithInvokeParams(computeInvokeParams)),
					statecharts.On(string(statecharts.ErrEventCommunication), statecharts.Target("backoff")),
				),
				statecharts.Atomic("backoff",
					statecharts.OnEntry(reportReconnecting, scheduleReconnect),
					statecharts.On("reconnect_timer", statecharts.Target("online")),
				),
			),
			statecharts.On("switch", statecharts.Target("online"), statecharts.Then(handleSwitch)),
		),
		statecharts.WithNewDatamodel(func() any { return &linkModel{ServerAddr: serverAddr, Tools: tools} }),
	)
}
