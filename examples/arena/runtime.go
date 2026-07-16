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

type botStatus struct {
	Player         string                 `json:"player"`
	Controller     statecharts.Identifier `json:"controller"`
	Match          statecharts.Identifier `json:"match"`
	Color          string                 `json:"color"`
	PolicyRevision statecharts.RevisionID `json:"policy_revision"`
	Generation     uint64                 `json:"generation"`
}

type arenaRuntime struct {
	system    *actors.System
	transport *socketTransport
	match     *statecharts.Chart
	bot       *statecharts.Chart

	matchesMu  sync.RWMutex
	matches    map[statecharts.Identifier]statecharts.RevisionID
	botAdminMu sync.RWMutex
	bots       map[string]botStatus
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
	runtime := &arenaRuntime{system: system, transport: transport, match: match, bot: bot, matches: map[statecharts.Identifier]statecharts.RevisionID{}, bots: map[string]botStatus{}}
	if err := runtime.createMatch(ctx, "match.main"); err != nil {
		_ = system.Stop(context.Background())
		return nil, err
	}
	botColors := []string{"#22d3ee", "#f472b6", "#fbbf24", "#a78bfa"}
	runtime.botAdminMu.Lock()
	for index := 0; index < options.Bots; index++ {
		player := fmt.Sprintf("bot.%d", index+1)
		if _, err := runtime.startBotLocked(ctx, player, "match.main", botColors[index%len(botColors)], 1); err != nil {
			runtime.botAdminMu.Unlock()
			_ = system.Stop(context.Background())
			return nil, err
		}
	}
	runtime.botAdminMu.Unlock()
	return runtime, nil
}

// startBotLocked starts a controller while the caller holds botAdminMu for
// writing. That makes selecting and pinning the current definition atomic with
// respect to bot publication.
func (runtime *arenaRuntime) startBotLocked(ctx context.Context, player string, match statecharts.Identifier, color string, generation uint64) (botStatus, error) {
	_, expectedRevision, ok := runtime.system.CurrentDefinition(botKind)
	if !ok {
		return botStatus{}, fmt.Errorf("bot definition is not registered")
	}
	controllerName, err := randomIdentifier(fmt.Sprintf("bot-controller.%s.%d", strings.TrimPrefix(player, "bot."), generation))
	if err != nil {
		return botStatus{}, err
	}
	controller := statecharts.Identifier(controllerName)
	if err := runtime.system.Spawn(ctx, controller, botKind); err != nil {
		return botStatus{}, err
	}
	cleanup := func() {
		_ = runtime.system.Tell(context.Background(), controller, statecharts.Event{Name: "bot.stop", Type: statecharts.EventExternal})
	}
	actualRevision, ok := runtime.system.ActorRevision(controller)
	if !ok {
		cleanup()
		return botStatus{}, fmt.Errorf("spawned bot controller %q has no pinned revision", controller)
	}
	if actualRevision != expectedRevision {
		cleanup()
		return botStatus{}, fmt.Errorf("bot controller %q pinned revision %q, expected current revision %q", controller, actualRevision, expectedRevision)
	}
	config, err := taggedStruct(botConfigTag, botConfig{Match: string(match), Player: player, Name: "BOT " + player, Color: color, PolicyRevision: string(actualRevision)})
	if err != nil {
		cleanup()
		return botStatus{}, err
	}
	if err := runtime.system.Tell(ctx, controller, statecharts.Event{Name: "bot.start", Type: statecharts.EventExternal, Data: config}); err != nil {
		cleanup()
		return botStatus{}, err
	}
	status := botStatus{Player: player, Controller: controller, Match: match, Color: color, PolicyRevision: actualRevision, Generation: generation}
	runtime.bots[player] = status
	return status, nil
}

func (runtime *arenaRuntime) listBots() []botStatus {
	runtime.botAdminMu.RLock()
	defer runtime.botAdminMu.RUnlock()
	return runtime.listBotsLocked()
}

func (runtime *arenaRuntime) listBotsLocked() []botStatus {
	result := make([]botStatus, 0, len(runtime.bots))
	for _, status := range runtime.bots {
		result = append(result, status)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Player < result[j].Player })
	return result
}

func (runtime *arenaRuntime) rolloutBot(ctx context.Context, player string) (botStatus, error) {
	runtime.botAdminMu.Lock()
	defer runtime.botAdminMu.Unlock()
	return runtime.rolloutBotLocked(ctx, player)
}

func (runtime *arenaRuntime) rolloutBotLocked(ctx context.Context, player string) (botStatus, error) {
	old, ok := runtime.bots[player]
	if !ok {
		return botStatus{}, fmt.Errorf("unknown bot %q", player)
	}
	next, err := runtime.startBotLocked(ctx, player, old.Match, old.Color, old.Generation+1)
	if err != nil {
		return botStatus{}, err
	}
	if err := runtime.system.Tell(ctx, old.Controller, statecharts.Event{Name: "bot.stop", Type: statecharts.EventExternal}); err != nil {
		return next, fmt.Errorf("replacement %q is active, but stopping obsolete controller %q failed: %w", next.Controller, old.Controller, err)
	}
	return next, nil
}

func (runtime *arenaRuntime) rolloutBots(ctx context.Context) ([]botStatus, error) {
	runtime.botAdminMu.Lock()
	defer runtime.botAdminMu.Unlock()
	players := runtime.listBotsLocked()
	result := make([]botStatus, 0, len(players))
	for _, old := range players {
		next, err := runtime.rolloutBotLocked(ctx, old.Player)
		if err != nil {
			return result, err
		}
		result = append(result, next)
	}
	return result, nil
}

func (runtime *arenaRuntime) publishBotDefinition(ctx context.Context, definition statecharts.Definition) (statecharts.RevisionID, error) {
	runtime.botAdminMu.Lock()
	defer runtime.botAdminMu.Unlock()
	return runtime.system.Publish(ctx, definition)
}

func (runtime *arenaRuntime) botPolicyCandidate(policy botPolicy) (statecharts.Definition, statecharts.RevisionID, error) {
	current, _, ok := runtime.system.CurrentDefinition(botKind)
	if !ok {
		return statecharts.Definition{}, "", fmt.Errorf("bot definition is not registered")
	}
	candidate, err := replaceBotPolicy(current, policy)
	if err != nil {
		return statecharts.Definition{}, "", err
	}
	chart, err := statecharts.Compile(candidate, runtime.bot.Datamodel())
	if err == nil {
		err = chart.Prepare()
	}
	if err != nil {
		return statecharts.Definition{}, "", err
	}
	return candidate, chart.Revision(), nil
}

// publishBotPolicy holds the administration lock across the whole
// read-modify-publish operation so a concurrent full-definition publication
// cannot be overwritten by a policy update based on an older definition.
func (runtime *arenaRuntime) publishBotPolicy(ctx context.Context, policy botPolicy) (statecharts.RevisionID, error) {
	runtime.botAdminMu.Lock()
	defer runtime.botAdminMu.Unlock()
	candidate, _, err := runtime.botPolicyCandidate(policy)
	if err != nil {
		return "", err
	}
	return runtime.system.Publish(ctx, candidate)
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
