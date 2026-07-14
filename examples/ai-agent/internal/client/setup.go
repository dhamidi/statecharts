package client

import (
	"context"
	"fmt"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// NewSystem builds the client's own actors.System: no WithStorage --
// Durable() is never used client-side
// (link.go, tool.go, ui.go doc comments explain why each actor is what it
// is).
func NewSystem(clock statecharts.Clock) *actors.System {
	return actors.NewSystem(actors.WithClock(clock))
}

// Setup registers link/tool/ui, spawns all three, and -- if
// initialConversation is non-empty -- tells link to switch to it right
// away, matching `connect --conversation=<id>`. serverAddr is the
// workspace server's base URL (e.g. "http://127.0.0.1:8080").
func Setup(ctx context.Context, sys *actors.System, serverAddr string, tools []protocol.ToolName, initialConversation protocol.ConversationID) error {
	linkChart, err := BuildLinkChart(serverAddr, tools)
	if err != nil {
		return fmt.Errorf("examples/ai-agent: build link chart: %w", err)
	}
	directoryLinkChart, err := BuildDirectoryLinkChart(serverAddr)
	if err != nil {
		return fmt.Errorf("examples/ai-agent: build directorylink chart: %w", err)
	}
	toolChart, err := BuildToolChart(serverAddr)
	if err != nil {
		return fmt.Errorf("examples/ai-agent: build tool chart: %w", err)
	}
	uiChart, err := BuildUIChart(sys, serverAddr)
	if err != nil {
		return fmt.Errorf("examples/ai-agent: build ui chart: %w", err)
	}

	for _, c := range []*statecharts.Chart{linkChart, directoryLinkChart, toolChart, uiChart} {
		if err := sys.Register(c); err != nil {
			return fmt.Errorf("examples/ai-agent: register chart %q: %w", c.ID(), err)
		}
	}

	if err := sys.Spawn(ctx, "link", LinkKind); err != nil {
		return fmt.Errorf("examples/ai-agent: spawn link: %w", err)
	}
	if err := sys.Spawn(ctx, "directorylink", DirectoryLinkKind); err != nil {
		return fmt.Errorf("examples/ai-agent: spawn directorylink: %w", err)
	}
	if err := sys.Spawn(ctx, "tool", ToolKind); err != nil {
		return fmt.Errorf("examples/ai-agent: spawn tool: %w", err)
	}
	if err := sys.Spawn(ctx, "ui", UIKind); err != nil {
		return fmt.Errorf("examples/ai-agent: spawn ui: %w", err)
	}

	if initialConversation != "" {
		if err := sys.Tell(ctx, "link", statecharts.Event{
			Name: "switch", Type: statecharts.EventExternal,
			Data: switchRequest{ConversationID: initialConversation},
		}); err != nil {
			return fmt.Errorf("examples/ai-agent: initial switch: %w", err)
		}
	}

	return nil
}
