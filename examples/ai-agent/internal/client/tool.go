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

const toolInvokeType statecharts.Identifier = "ai-agent.client.tool.execute"

var recordCurrent = func(d *toolModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	req, ok := decodeExecute(ev.Data)
	if !ok {
		return nil
	}
	d.Current = req
	return nil
}

// enqueueCall handles a second "execute" arriving while busy: ToolActor
// runs one call at a time, so this one waits in d.Queued until the current
// one finishes.
var enqueueCall = func(d *toolModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	req, ok := decodeExecute(ev.Data)
	if !ok {
		return nil
	}
	d.Queued = append(d.Queued, req)
	return nil
}

// dequeueNext re-raises the next queued call (if any) as an ordinary
// "execute", processed immediately against the freshly re-entered "idle"
// state within the same macrostep (SCXML's internal-event processing).
var dequeueNext = func(d *toolModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	if len(d.Queued) == 0 {
		return nil
	}
	next := d.Queued[0]
	d.Queued = d.Queued[1:]
	ec.Raise(statecharts.Event{Name: "execute", Data: executeValue(next)})
	return nil
}

func computeExecParams(d *toolModel, _ statecharts.ExecContext, _ []statecharts.Value) (statecharts.Value, error) {
	return executeValue(d.Current), nil
}

// buildExecAndPost returns the invoke handler that runs a tool call's command
// (the only tool this example implements, "shell_command":
// Args["command"]) and POSTs the result back to serverAddr. Killing or
// disconnecting this client mid-command is exactly what the manual
// recovery test script's tool-executor-handoff scenarios exercise -- see
// the example's README.
func buildExecAndPost(serverAddr string) statecharts.InvokeHandlerFactory {
	return func() statecharts.InvokeHandler {
		return statecharts.InvokeHandlerFunc(func(ctx context.Context, request statecharts.InvokeRequest, _ statecharts.InvokeIO) (statecharts.Value, error) {
			req, _ := decodeExecute(request.Data)

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
				return statecharts.NullValue(), ctx.Err()
			}

			body, err := json.Marshal(result)
			if err != nil {
				return statecharts.NullValue(), err
			}
			u, err := url.Parse(serverAddr)
			if err != nil {
				return statecharts.NullValue(), err
			}
			u = u.JoinPath("conversations", req.ConversationID.String(), "tool-result")
			httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
			if err != nil {
				return statecharts.NullValue(), err
			}
			httpReq.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(httpReq)
			if err != nil {
				return statecharts.NullValue(), err
			}
			defer resp.Body.Close()
			if resp.StatusCode >= 300 {
				return statecharts.NullValue(), fmt.Errorf("client: POST %s: unexpected status %s", u, resp.Status)
			}
			return statecharts.NullValue(), nil
		})
	}
}

// ToolKind is the chart kind name the client's singleton "tool" actor is
// Registered and Spawned under.
const ToolKind statecharts.Identifier = "tool"

// BuildToolChart returns the client's "tool" chart: idle/busy, one
// shell_command execution at a time, further calls queued until it's free.
func BuildToolChart(serverAddr string) (*statecharts.Chart, error) {
	model := statecharts.NewGoModel(func() *toolModel { return &toolModel{} })
	record, err := model.Action("ai-agent.client.tool.record-current", "v1", recordCurrent)
	if err != nil {
		return nil, err
	}
	enqueue, err := model.Action("ai-agent.client.tool.enqueue", "v1", enqueueCall)
	if err != nil {
		return nil, err
	}
	dequeue, err := model.Action("ai-agent.client.tool.dequeue", "v1", dequeueNext)
	if err != nil {
		return nil, err
	}
	params, err := model.Value("ai-agent.client.tool.invoke-params", "v1", computeExecParams)
	if err != nil {
		return nil, err
	}
	return buildCanonicalChart(
		statecharts.Compound("tool", "idle",
			statecharts.Children(
				statecharts.Atomic("idle",
					statecharts.OnEntry(dequeue.Do()),
					statecharts.On("execute", statecharts.Target("busy"), statecharts.Then(record.Do())),
				),
				statecharts.Atomic("busy",
					statecharts.Invoke(string(toolInvokeType), "shell-command", statecharts.WithInvokeID("exec"), statecharts.WithInvokeContent(params.Get())),
					statecharts.On("execute", statecharts.Then(enqueue.Do())),
					statecharts.On("done.invoke.exec", statecharts.Target("idle")),
				),
			),
		),
		model)
}
