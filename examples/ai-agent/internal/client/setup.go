package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// buildCanonicalChart follows the same transport path used by definition
// inspection and deployment: build from Go, encode the canonical definition,
// decode it, then recompile it against this chart family's scoped registry.
// Callers therefore exercise the portable definition rather than the
// in-memory builder result.
func buildCanonicalChart(root statecharts.StateDefinition, model statecharts.Datamodel, opts ...statecharts.BuildOption) (*statecharts.Chart, error) {
	chart, err := statecharts.Build(root, model, opts...)
	if err != nil {
		return nil, err
	}
	wire, err := json.Marshal(chart.Definition())
	if err != nil {
		return nil, fmt.Errorf("marshal canonical definition: %w", err)
	}
	var definition statecharts.Definition
	if err := json.Unmarshal(wire, &definition); err != nil {
		return nil, fmt.Errorf("unmarshal canonical definition: %w", err)
	}
	recompiled, err := statecharts.Compile(definition, model)
	if err != nil {
		return nil, fmt.Errorf("recompile canonical definition: %w", err)
	}
	return recompiled, nil
}

// Setup creates the client runtime, system, charts, and singleton actors
// atomically. Runtime network and UI capabilities are instance scoped.
// It registers link/tool/ui, spawns all three, and -- if
// initialConversation is non-empty -- tells link to switch to it right
// away, matching `connect --conversation=<id>`. serverAddr is the
// workspace server's base URL (e.g. "http://127.0.0.1:8080").
func Setup(ctx context.Context, clock statecharts.Clock, serverAddr string, tools []protocol.ToolName, initialConversation protocol.ConversationID) (*actors.System, error) {
	requests := newUIRequests()
	uiRuntime := &uiRuntime{}
	var sys *actors.System
	sys = actors.NewSystem(
		actors.WithClock(clock),
		actors.WithInvokeHandler(linkInvokeType, func() statecharts.InvokeHandler {
			return statecharts.InvokeHandlerFunc(func(ctx context.Context, request statecharts.InvokeRequest, io statecharts.InvokeIO) (statecharts.Value, error) {
				return dialSSE(ctx, request.Data, io)
			})
		}),
		actors.WithInvokeHandler(directoryLinkInvokeType, func() statecharts.InvokeHandler {
			return statecharts.InvokeHandlerFunc(func(ctx context.Context, request statecharts.InvokeRequest, io statecharts.InvokeIO) (statecharts.Value, error) {
				return dialDirectoryEvents(ctx, request.Data, io)
			})
		}),
		actors.WithInvokeHandler(toolInvokeType, buildExecAndPost(serverAddr)),
		actors.WithInvokeHandler(uiInvokeType, func() statecharts.InvokeHandler { return buildRunHTTPServer(sys, serverAddr, requests)() }),
	)
	linkChart, err := BuildLinkChart(serverAddr, tools)
	if err != nil {
		return nil, fmt.Errorf("examples/ai-agent: build link chart: %w", err)
	}
	directoryLinkChart, err := BuildDirectoryLinkChart(serverAddr)
	if err != nil {
		return nil, fmt.Errorf("examples/ai-agent: build directorylink chart: %w", err)
	}
	toolChart, err := BuildToolChart(serverAddr)
	if err != nil {
		return nil, fmt.Errorf("examples/ai-agent: build tool chart: %w", err)
	}
	uiChart, err := BuildUIChart(uiRuntime, requests)
	if err != nil {
		return nil, fmt.Errorf("examples/ai-agent: build ui chart: %w", err)
	}

	for _, c := range []*statecharts.Chart{linkChart, directoryLinkChart, toolChart, uiChart} {
		if err := sys.Register(c); err != nil {
			return nil, fmt.Errorf("examples/ai-agent: register chart %q: %w", c.ID(), err)
		}
	}

	if err := sys.Spawn(ctx, "link", LinkKind); err != nil {
		return nil, fmt.Errorf("examples/ai-agent: spawn link: %w", err)
	}
	if err := sys.Spawn(ctx, "directorylink", DirectoryLinkKind); err != nil {
		return nil, fmt.Errorf("examples/ai-agent: spawn directorylink: %w", err)
	}
	if err := sys.Spawn(ctx, "tool", ToolKind); err != nil {
		return nil, fmt.Errorf("examples/ai-agent: spawn tool: %w", err)
	}
	if err := sys.Spawn(ctx, "ui", UIKind); err != nil {
		return nil, fmt.Errorf("examples/ai-agent: spawn ui: %w", err)
	}

	if initialConversation != "" {
		if err := sys.Tell(ctx, "link", statecharts.Event{
			Name: "switch", Type: statecharts.EventExternal,
			Data: switchValue(initialConversation),
		}); err != nil {
			return nil, fmt.Errorf("examples/ai-agent: initial switch: %w", err)
		}
	}

	return sys, nil
}
