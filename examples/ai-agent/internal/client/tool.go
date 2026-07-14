package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os/exec"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// executeRequest is "execute"'s payload -> tool, from LinkActor: which
// conversation this tool call belongs to (needed to POST the result back
// to the right URL) plus the call itself.
type executeRequest struct {
	ConversationID protocol.ConversationID
	Call           protocol.ToolCallFrame
}

// toolModel is ToolActor's (non-durable) datamodel.
type toolModel struct {
	Current executeRequest
	Queued  []executeRequest
}

var recordCurrent = statecharts.Action(func(d *toolModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	req, ok := statecharts.Payload[executeRequest](ev)
	if !ok {
		return nil
	}
	d.Current = req
	return nil
})

// enqueueCall handles a second "execute" arriving while busy: ToolActor
// runs one call at a time, so this one waits in d.Queued until the current
// one finishes.
var enqueueCall = statecharts.Action(func(d *toolModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	req, ok := statecharts.Payload[executeRequest](ev)
	if !ok {
		return nil
	}
	d.Queued = append(d.Queued, req)
	return nil
})

// dequeueNext re-raises the next queued call (if any) as an ordinary
// "execute", processed immediately against the freshly re-entered "idle"
// state within the same macrostep (SCXML's internal-event processing).
var dequeueNext = statecharts.ActionFunc(func(ec statecharts.ExecContext) error {
	d, _ := ec.Datamodel().(*toolModel)
	if d == nil || len(d.Queued) == 0 {
		return nil
	}
	next := d.Queued[0]
	d.Queued = d.Queued[1:]
	ec.Raise(statecharts.Event{Name: "execute", Data: next})
	return nil
})

func computeExecParams(ec statecharts.ExecContext) any {
	d, _ := ec.Datamodel().(*toolModel)
	if d == nil {
		return executeRequest{}
	}
	return d.Current
}

// buildExecAndPost returns the InvokeFunc that runs a tool call's command
// (the only tool this example implements, "shell_command":
// Args["command"]) and POSTs the result back to serverAddr. Killing or
// disconnecting this client mid-command is exactly what the manual
// recovery test script's tool-executor-handoff scenarios exercise -- see
// the example's README.
func buildExecAndPost(serverAddr string) statecharts.InvokeFunc {
	return func(ctx context.Context, params any, io statecharts.InvokeIO) (any, error) {
		req, _ := params.(executeRequest)

		command, _ := req.Call.Args["command"].(string)
		result := protocol.ToolResultRequest{CallID: req.Call.CallID}

		cmd := exec.CommandContext(ctx, "sh", "-c", command)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		runErr := cmd.Run()
		result.Output = out.String()
		if runErr != nil {
			if exitErr, ok := runErr.(*exec.ExitError); ok {
				result.ExitCode = exitErr.ExitCode()
			} else {
				result.Error = runErr.Error()
			}
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		body, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		u, err := url.Parse(serverAddr)
		if err != nil {
			return nil, err
		}
		u = u.JoinPath("conversations", req.ConversationID.String(), "tool-result")
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("client: POST %s: unexpected status %s", u, resp.Status)
		}
		return nil, nil
	}
}

// ToolKind is the chart kind name the client's singleton "tool" actor is
// Registered and Spawned under.
const ToolKind statecharts.Identifier = "tool"

// BuildToolChart returns the client's "tool" chart: idle/busy, one
// shell_command execution at a time, further calls queued until it's free.
func BuildToolChart(serverAddr string) (*statecharts.Chart, error) {
	execAndPost := buildExecAndPost(serverAddr)
	return statecharts.Build(
		statecharts.Compound("tool", "idle",
			statecharts.Children(
				statecharts.Atomic("idle",
					statecharts.OnEntry(dequeueNext),
					statecharts.On("execute", statecharts.Target("busy"), statecharts.Then(recordCurrent)),
				),
				statecharts.Atomic("busy",
					statecharts.Invoke(execAndPost, statecharts.WithInvokeID("exec"), statecharts.WithInvokeParams(computeExecParams)),
					statecharts.On("execute", statecharts.Then(enqueueCall)),
					statecharts.On("done.invoke.exec", statecharts.Target("idle")),
				),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &toolModel{} }),
	)
}
