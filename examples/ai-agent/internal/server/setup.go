package server

import (
	"context"
	"fmt"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/llm"
)

// NewSystem builds the actors.System every server-side actor lives in,
// wiring LLMDispatchProcessor in as its WithFallback (see llmrequest.go's
// own doc comment on why chart actions can't spawn actors directly). log
// and snapshots are typically the same *sqllog.Log value, which implements
// both.
func NewSystem(log statecharts.Log, snapshots statecharts.SnapshotStore, clock statecharts.Clock, logger statecharts.Logger, provider llm.Provider) (*actors.System, *LLMDispatchProcessor) {
	dispatch := NewLLMDispatchProcessor(provider)
	sys := actors.NewSystem(
		actors.WithLog(log),
		actors.WithSnapshotStore(snapshots),
		actors.WithClock(clock),
		actors.WithLogger(logger),
		actors.WithFallback(dispatch),
	)
	dispatch.SetSystem(sys)
	return sys, dispatch
}

// Setup registers every chart kind the server ever spawns, starts the
// fixed singleton actors, and primes DirectoryActor's mirror from
// UserActor's own already-rehydrated state -- all before returning, so the
// caller's HTTP listener never opens onto a workspace that isn't fully
// resident yet.
func Setup(ctx context.Context, sys *actors.System, clock statecharts.Clock) error {
	registerDataTypes()

	charts := []struct {
		kind  statecharts.Identifier
		build func() (*statecharts.Chart, error)
	}{
		{ConversationKind, BuildConversationChart},
		{FanoutKind, BuildFanoutChart},
		{ToolRegistryKind, func() (*statecharts.Chart, error) { return BuildToolRegistryChart(clock) }},
		{UserKind, BuildUserChart},
		{DirectoryKind, BuildDirectoryChart},
		{LLMRequestKind, BuildLLMRequestChart},
		{ConnectionKind, BuildConnectionChart},
	}
	for _, c := range charts {
		chart, err := c.build()
		if err != nil {
			return fmt.Errorf("examples/ai-agent: build chart %q: %w", c.kind, err)
		}
		if err := sys.Register(chart); err != nil {
			return fmt.Errorf("examples/ai-agent: register chart %q: %w", c.kind, err)
		}
	}

	if err := sys.Spawn(ctx, "fanout", FanoutKind); err != nil {
		return fmt.Errorf("examples/ai-agent: spawn fanout: %w", err)
	}
	if err := sys.Spawn(ctx, "toolregistry", ToolRegistryKind); err != nil {
		return fmt.Errorf("examples/ai-agent: spawn toolregistry: %w", err)
	}
	if err := sys.Spawn(ctx, "user", UserKind, actors.Durable()); err != nil {
		return fmt.Errorf("examples/ai-agent: spawn user: %w", err)
	}
	if err := sys.Spawn(ctx, "directory", DirectoryKind); err != nil {
		return fmt.Errorf("examples/ai-agent: spawn directory: %w", err)
	}

	// Prime directory from user's own already-rehydrated conversation map,
	// entirely by ordinary actor Send (see user.go's forwardSyncAll) --
	// never by reading user's Log directly. Tell blocks until user's own
	// action has issued every "sync" Send; those are still each an
	// independent asynchronous delivery hop (routingProcessor.Send's own
	// contract -- see actors/router.go), so this is a practical, not a
	// mathematically proven, barrier. That's an acceptable simplification
	// here: the sidebar is refreshed on load, not push-live, by design
	// (see the example's README), and in-process map delivery settles long
	// before a human can act on it.
	if err := sys.Tell(ctx, "user", statecharts.Event{Name: "bootstrap_directory", Type: statecharts.EventExternal}); err != nil {
		return fmt.Errorf("examples/ai-agent: bootstrap directory: %w", err)
	}

	return nil
}
