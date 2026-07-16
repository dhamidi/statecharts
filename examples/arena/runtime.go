package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

type runtimeOptions struct {
	TickInterval time.Duration
	Bots         int
}

type matchStatus struct {
	ID       statecharts.Identifier `json:"id"`
	Revision statecharts.RevisionID `json:"revision"`
}

type arenaRuntime struct {
	system    *actors.System
	transport *socketTransport
	match     *statecharts.Chart

	matchesMu sync.RWMutex
	matches   map[statecharts.Identifier]statecharts.RevisionID
}

func setupArena(ctx context.Context, options runtimeOptions) (*arenaRuntime, error) {
	if options.TickInterval <= 0 {
		options.TickInterval = 100 * time.Millisecond
	}
	transport := newSocketTransport()
	system := actors.NewSystem(
		actors.WithNodeName("arena"),
		actors.WithIdleTimeout(0),
		actors.WithIOProcessor(socketIOProcessor, transport.factory),
	)
	match, err := buildMatchChart(options.TickInterval)
	if err != nil {
		return nil, err
	}
	connection, err := buildConnectionChart()
	if err != nil {
		return nil, err
	}
	bot, err := buildBotChart()
	if err != nil {
		return nil, err
	}
	for _, chart := range []*statecharts.Chart{match, connection, bot} {
		if err := system.Register(chart); err != nil {
			_ = system.Stop(context.Background())
			return nil, err
		}
	}
	runtime := &arenaRuntime{system: system, transport: transport, match: match, matches: map[statecharts.Identifier]statecharts.RevisionID{}}
	if err := runtime.createMatch(ctx, "match.main"); err != nil {
		_ = system.Stop(context.Background())
		return nil, err
	}
	botColors := []string{"#22d3ee", "#f472b6", "#fbbf24", "#a78bfa"}
	for index := 0; index < options.Bots; index++ {
		name := statecharts.Identifier(fmt.Sprintf("bot.%d", index+1))
		if err := spawnBot(ctx, system, name, "match.main", botColors[index%len(botColors)]); err != nil {
			_ = system.Stop(context.Background())
			return nil, err
		}
	}
	return runtime, nil
}

func newTestSystem(clock statecharts.Clock, transport *socketTransport) (*actors.System, error) {
	system := actors.NewSystem(
		actors.WithClock(clock),
		actors.WithIdleTimeout(0),
		actors.WithIOProcessor(socketIOProcessor, transport.factory),
	)
	match, err := buildMatchChart(100 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	connection, err := buildConnectionChart()
	if err != nil {
		return nil, err
	}
	bot, err := buildBotChart()
	if err != nil {
		return nil, err
	}
	for _, chart := range []*statecharts.Chart{match, connection, bot} {
		if err := system.Register(chart); err != nil {
			_ = system.Stop(context.Background())
			return nil, err
		}
	}
	return system, nil
}

func (runtime *arenaRuntime) createMatch(ctx context.Context, id statecharts.Identifier) error {
	if _, err := statecharts.NewIdentifier(string(id)); err != nil {
		return fmt.Errorf("invalid match ID: %w", err)
	}
	if !strings.HasPrefix(string(id), "match.") {
		return fmt.Errorf("match ID %q must begin with match.", id)
	}
	if runtime.hasMatch(id) {
		return fmt.Errorf("match %q already exists", id)
	}
	if err := spawnMatch(ctx, runtime.system, id); err != nil {
		return err
	}
	revision, ok := runtime.system.ActorRevision(id)
	if !ok {
		return fmt.Errorf("match %q has no revision", id)
	}
	runtime.matchesMu.Lock()
	runtime.matches[id] = revision
	runtime.matchesMu.Unlock()
	return nil
}

func (runtime *arenaRuntime) listMatches() []matchStatus {
	runtime.matchesMu.RLock()
	defer runtime.matchesMu.RUnlock()
	result := make([]matchStatus, 0, len(runtime.matches))
	for id, revision := range runtime.matches {
		result = append(result, matchStatus{ID: id, Revision: revision})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (runtime *arenaRuntime) hasMatch(id statecharts.Identifier) bool {
	runtime.matchesMu.RLock()
	defer runtime.matchesMu.RUnlock()
	_, ok := runtime.matches[id]
	return ok
}

func (runtime *arenaRuntime) stop(ctx context.Context) error {
	return runtime.system.Stop(ctx)
}
