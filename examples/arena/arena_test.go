package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	statejson "github.com/dhamidi/statecharts/syntax/json"
)

func TestArenaMechanicsAreDeterministicAndRejectStaleInput(t *testing.T) {
	world := newWorld(11, 9, 42)
	world.Creatures = map[string]creature{
		"red":  {ID: "red", X: 2, Y: 2, Facing: directionRight, Health: 3},
		"blue": {ID: "blue", X: 5, Y: 2, Facing: directionLeft, Health: 3},
	}
	world.Powerups = []powerup{{X: 3, Y: 2, Kind: "charge"}}

	if changed := world.applyInput(playerInput{Player: "red", Sequence: 1, Action: actionRight}, 1); !changed {
		t.Fatal("move was not applied")
	}
	red := world.Creatures["red"]
	if red.X != 3 || red.Y != 2 || red.Power != 1 || red.Score != 5 {
		t.Fatalf("red after powerup = %+v", red)
	}
	if len(world.Powerups) != 0 {
		t.Fatalf("powerup was not consumed: %+v", world.Powerups)
	}
	if changed := world.applyInput(playerInput{Player: "red", Sequence: 1, Action: actionRight}, 1); changed {
		t.Fatal("stale sequence was applied")
	}
	if changed := world.applyInput(playerInput{Player: "red", Sequence: 2, Action: actionShoot}, 1); !changed {
		t.Fatal("shot was not applied")
	}
	if len(world.Projectiles) != 1 || world.Projectiles[0].Damage != 2 {
		t.Fatalf("projectiles = %+v", world.Projectiles)
	}

	world.advance()
	world.advance()
	blue := world.Creatures["blue"]
	if blue.Health != 1 {
		t.Fatalf("blue health = %d, want 1", blue.Health)
	}
	if len(world.Projectiles) != 0 {
		t.Fatalf("projectile survived collision: %+v", world.Projectiles)
	}
}

func TestProjectileHitsAdjacentCreatureOnNextTick(t *testing.T) {
	world := newWorld(11, 9, 42)
	world.Creatures = map[string]creature{
		"red":  {ID: "red", X: 2, Y: 2, Facing: directionRight, Health: 3},
		"blue": {ID: "blue", X: 3, Y: 2, Facing: directionLeft, Health: 3},
	}
	if changed := world.applyInput(playerInput{Player: "red", Sequence: 1, Action: actionShoot}, 1); !changed {
		t.Fatal("shot was not applied")
	}
	world.advance()
	if got := world.Creatures["blue"].Health; got != 2 {
		t.Fatalf("adjacent creature health = %d, want 2", got)
	}
	if len(world.Projectiles) != 0 {
		t.Fatalf("projectile survived adjacent collision: %+v", world.Projectiles)
	}
}

func TestSpawnCellHandlesFullRandomRange(t *testing.T) {
	world := newWorld(19, 13, 1)
	x, y := world.openCell(^uint64(0))
	if x < 1 || x >= world.Width-1 || y < 1 || y >= world.Height-1 {
		t.Fatalf("open cell = (%d,%d), outside playable arena", x, y)
	}
	if world.wallAt(x, y) {
		t.Fatalf("open cell = (%d,%d), which is a wall", x, y)
	}
}

func TestClientProtocolRejectsTrailingJSON(t *testing.T) {
	value, _ := statecharts.StringValue(`{"v":1,"type":"input","seq":1,"action":"right"} garbage`)
	if _, err := parseClientMessage(value); err == nil {
		t.Fatal("client protocol accepted trailing non-JSON data")
	}
}

func TestServerSnapshotEncodesEmptyCollectionsAsArrays(t *testing.T) {
	world := newWorld(7, 7, 1)
	frame, err := encodeServerMessage(serverMessage{Type: "snapshot", Snapshot: world.snapshot("match.test", "revision")})
	if err != nil {
		t.Fatal(err)
	}
	text, ok := frame.AsString()
	if !ok {
		t.Fatal("server frame is not text")
	}
	var message map[string]any
	if err := json.Unmarshal([]byte(text), &message); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := message["snapshot"].(map[string]any)
	if !ok {
		t.Fatalf("snapshot = %#v, want object", message["snapshot"])
	}
	for _, field := range []string{"creatures", "projectiles", "powerups", "walls"} {
		if _, ok := snapshot[field].([]any); !ok {
			t.Errorf("snapshot.%s = %#v, want array", field, snapshot[field])
		}
	}
}

func TestPublishedMovementOnlyAffectsNewMatches(t *testing.T) {
	ctx := context.Background()
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	transport := newSocketTransport()
	matchChart, err := buildMatchChart(100 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	connectionChart, err := buildConnectionChart()
	if err != nil {
		t.Fatal(err)
	}
	system := actors.NewSystem(
		actors.WithClock(clock),
		actors.WithIdleTimeout(0),
		actors.WithIOProcessor(socketIOProcessor, transport.factory),
	)
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	for _, chart := range []*statecharts.Chart{matchChart, connectionChart} {
		if err := system.Register(chart); err != nil {
			t.Fatal(err)
		}
	}

	oldFrames := transport.registerTest("old-output")
	if err := spawnMatch(ctx, system, "match.old"); err != nil {
		t.Fatal(err)
	}
	oldRevision, _ := system.ActorRevision("match.old")
	if err := startTestConnection(ctx, system, "connection.old", "match.old", "old-player", "old-output"); err != nil {
		t.Fatal(err)
	}
	oldInitial := waitForPlayer(t, oldFrames, "old-player", func(creature) bool { return true })

	candidate := matchChart.Definition()
	if !setActionVersion(&candidate.Root, "arena.match.apply-input", "v2") {
		t.Fatal("match definition does not reference apply-input")
	}
	candidate.RevisionSalt = "movement-v2"
	newRevision, err := system.Publish(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if newRevision == oldRevision {
		t.Fatal("publication did not create a new revision")
	}

	newFrames := transport.registerTest("new-output")
	if err := spawnMatch(ctx, system, "match.new"); err != nil {
		t.Fatal(err)
	}
	if err := startTestConnection(ctx, system, "connection.new", "match.new", "new-player", "new-output"); err != nil {
		t.Fatal(err)
	}
	newInitial := waitForPlayer(t, newFrames, "new-player", func(creature) bool { return true })

	tellClientInput(t, system, "connection.old", 1, actionRight)
	oldMoved := waitForPlayer(t, oldFrames, "old-player", func(c creature) bool { return c.LastSequence == 1 })
	tellClientInput(t, system, "connection.new", 1, actionRight)
	newMoved := waitForPlayer(t, newFrames, "new-player", func(c creature) bool { return c.LastSequence == 1 })

	if got := oldMoved.X - oldInitial.X; got != 1 {
		t.Fatalf("old match moved %d cells, want 1", got)
	}
	if got := newMoved.X - newInitial.X; got != 2 {
		t.Fatalf("new match moved %d cells, want 2", got)
	}
	if revision, _ := system.ActorRevision("match.old"); revision != oldRevision {
		t.Fatalf("old match revision changed from %q to %q", oldRevision, revision)
	}
	if revision, _ := system.ActorRevision("match.new"); revision != newRevision {
		t.Fatalf("new match revision = %q, want %q", revision, newRevision)
	}
}

func TestMatchSchedulesOneSuccessorTick(t *testing.T) {
	ctx := context.Background()
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	transport := newSocketTransport()
	system, err := newTestSystem(clock, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	frames := transport.registerTest("tick-output")
	if err := spawnMatch(ctx, system, "match.tick"); err != nil {
		t.Fatal(err)
	}
	if err := startTestConnection(ctx, system, "connection.tick", "match.tick", "timer", "tick-output"); err != nil {
		t.Fatal(err)
	}
	waitForSnapshot(t, frames, func(snapshot arenaSnapshot) bool { return snapshot.Tick == 0 })

	for want := uint64(1); want <= 3; want++ {
		clock.Advance(100 * time.Millisecond)
		got := waitForSnapshot(t, frames, func(snapshot arenaSnapshot) bool { return snapshot.Tick >= want })
		if got.Tick != want {
			t.Fatalf("tick after one interval = %d, want %d (duplicate timer)", got.Tick, want)
		}
	}
}

func TestSocketIOProcessorTargetsOnlyAttachedCapability(t *testing.T) {
	transport := newSocketTransport()
	first := transport.registerTest("first")
	second := transport.registerTest("second")
	binding := transport.factory()
	binding.Attach(discardDispatcher{})
	payload, _ := statecharts.StringValue(`{"v":1,"type":"test"}`)
	if err := binding.Send(context.Background(), statecharts.SendRequest{Type: socketIOProcessor, Target: "second", Data: payload}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first:
		t.Fatal("frame leaked to the wrong capability")
	default:
	}
	select {
	case got := <-second:
		if string(got) != `{"v":1,"type":"test"}` {
			t.Fatalf("frame = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("target capability did not receive frame")
	}
}

func TestBotObservesSnapshotsAndUsesPlayerProtocol(t *testing.T) {
	ctx := context.Background()
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	transport := newSocketTransport()
	system, err := newTestSystem(clock, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	frames := transport.registerTest("observer-output")
	if err := spawnMatch(ctx, system, "match.bot"); err != nil {
		t.Fatal(err)
	}
	if err := startTestConnection(ctx, system, "connection.observer", "match.bot", "observer", "observer-output"); err != nil {
		t.Fatal(err)
	}
	if err := spawnBot(ctx, system, "bot.cyan", "match.bot", "#22d3ee"); err != nil {
		t.Fatal(err)
	}
	initial := waitForPlayer(t, frames, "bot.cyan", func(creature) bool { return true })
	clock.Advance(100 * time.Millisecond)
	bot := waitForPlayer(t, frames, "bot.cyan", func(c creature) bool { return c.LastSequence > 0 })
	if bot.LastSequence != 1 {
		t.Fatalf("bot sequence = %d, want 1", bot.LastSequence)
	}
	if bot.X == initial.X && bot.Y == initial.Y {
		t.Fatalf("bot did not move from (%d,%d)", initial.X, initial.Y)
	}
}

func TestBotMovesTowardPowerupInsteadOfShootingIt(t *testing.T) {
	snapshot := arenaSnapshot{
		Width: 7, Height: 7, Tick: 1,
		Creatures: []creature{{ID: "bot", X: 2, Y: 2, Facing: directionRight, Connected: true}},
		Powerups:  []powerup{{X: 3, Y: 2, Kind: "charge"}},
	}
	self := snapshot.Creatures[0]
	target, creature, ok := nearestBotTarget(snapshot, self, botTargetPowerup)
	if !ok || creature {
		t.Fatalf("powerup target = %+v, creature=%v, found=%v", target, creature, ok)
	}
	if action := botRoute(snapshot, self, target, creature); action != actionRight {
		t.Fatalf("action = %q, want %q toward powerup", action, actionRight)
	}
}

func TestBotRoutesAroundWallBlockingDirectPath(t *testing.T) {
	snapshot := arenaSnapshot{
		Width: 7, Height: 7, Tick: 1,
		Creatures: []creature{{ID: "bot", X: 2, Y: 2, Facing: directionRight, Connected: true}},
		Powerups:  []powerup{{X: 4, Y: 2, Kind: "charge"}},
		Walls:     []tile{{X: 3, Y: 2}},
	}
	self := snapshot.Creatures[0]
	target, creature, ok := nearestBotTarget(snapshot, self, botTargetPowerup)
	if !ok {
		t.Fatal("powerup target not found")
	}
	action := botRoute(snapshot, self, target, creature)
	if action != actionUp && action != actionDown {
		t.Fatalf("action = %q, want a legal detour around wall", action)
	}
}

func TestBotTargetSelectionIsComposable(t *testing.T) {
	snapshot := arenaSnapshot{Width: 9, Height: 9, Tick: 1, Creatures: []creature{
		{ID: "bot", X: 2, Y: 2, Facing: directionRight, Connected: true},
		{ID: "enemy", X: 4, Y: 2, Connected: true},
	}, Powerups: []powerup{{X: 2, Y: 5, Kind: "charge"}}}
	self := snapshot.Creatures[0]
	opponent, creature, ok := nearestBotTarget(snapshot, self, botTargetOpponent)
	if !ok || !creature || opponent != (tile{X: 4, Y: 2}) {
		t.Fatalf("opponent target = %+v, creature=%v, found=%v", opponent, creature, ok)
	}
	item, creature, ok := nearestBotTarget(snapshot, self, botTargetPowerup)
	if !ok || creature || item != (tile{X: 2, Y: 5}) {
		t.Fatalf("powerup target = %+v, creature=%v, found=%v", item, creature, ok)
	}
	nearest, creature, ok := nearestBotTarget(snapshot, self, botTargetNearest)
	if !ok || !creature || nearest != opponent {
		t.Fatalf("nearest target = %+v, creature=%v, found=%v", nearest, creature, ok)
	}
}

func TestBotDefinitionAllowsFreeFormBehaviorWithValidCapabilityArguments(t *testing.T) {
	chart, err := buildBotChart()
	if err != nil {
		t.Fatal(err)
	}
	valid := chart.Definition()
	if err := validateBotDefinition(valid); err != nil {
		t.Fatalf("default definition is invalid: %v", err)
	}

	withoutDecisions := valid.Clone()
	active := &withoutDecisions.Root.Children[0]
	transitions := active.Transitions[:0]
	for _, transition := range active.Transitions {
		if len(transition.Events) != 1 || transition.Events[0] != "match.snapshot" {
			transitions = append(transitions, transition)
		}
	}
	active.Transitions = transitions
	if err := validateBotDefinition(withoutDecisions); err != nil {
		t.Fatalf("definition without snapshot decisions is invalid: %v", err)
	}

	invalid := valid.Clone()
	invalidDirection, _ := statecharts.StringValue("sideways")
	if !setFirstBotActionArguments(&invalid.Root, "arena.bot.move", statecharts.GoLiteral(invalidDirection)) {
		t.Fatal("definition has no move action")
	}
	if err := validateBotDefinition(invalid); err == nil {
		t.Fatal("invalid movement direction was accepted")
	}
}

func TestBotDefinitionRejectsCapabilitiesOutsideTheirEvents(t *testing.T) {
	chart, err := buildBotChart()
	if err != nil {
		t.Fatal(err)
	}
	valid := chart.Definition()

	tests := []struct {
		name   string
		mutate func(*statecharts.StateDefinition)
	}{
		{
			name: "snapshot action on start",
			mutate: func(active *statecharts.StateDefinition) {
				active.Transitions[7].Events = []statecharts.Identifier{"bot.start"}
			},
		},
		{
			name: "snapshot condition on start",
			mutate: func(active *statecharts.StateDefinition) {
				active.Transitions[1].Events = []statecharts.Identifier{"bot.start"}
			},
		},
		{
			name: "start action on stop",
			mutate: func(active *statecharts.StateDefinition) {
				active.Transitions[0].Events = []statecharts.Identifier{"bot.stop"}
			},
		},
		{
			name: "snapshot action on entry",
			mutate: func(active *statecharts.StateDefinition) {
				active.OnEntry = active.Transitions[7].Actions
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := valid.Clone()
			test.mutate(&definition.Root.Children[0])
			if err := validateBotDefinition(definition); err == nil {
				t.Fatal("definition accepted a capability outside its required event")
			}
		})
	}
}

func TestBotChartExposesComposableDecisionVocabulary(t *testing.T) {
	chart, err := buildBotChart()
	if err != nil {
		t.Fatal(err)
	}
	definition := chart.Definition()
	actions := map[statecharts.Identifier]bool{}
	conditions := map[statecharts.Identifier]bool{}
	var visit func(statecharts.StateDefinition)
	visit = func(state statecharts.StateDefinition) {
		for _, transition := range state.Transitions {
			if transition.Condition != nil {
				if name, ok := goReferenceName(*transition.Condition); ok {
					conditions[name] = true
				}
			}
			for _, block := range transition.Actions {
				for _, executable := range block {
					if executable.Call != nil {
						actions[executable.Call.Function.Name] = true
					}
				}
			}
		}
		for _, child := range state.Children {
			visit(child)
		}
	}
	visit(definition.Root)
	for _, name := range []statecharts.Identifier{"arena.bot.move", "arena.bot.move-toward", "arena.bot.shoot", "arena.bot.wander"} {
		if !actions[name] {
			t.Errorf("default bot chart does not demonstrate action %q", name)
		}
	}
	for _, name := range []statecharts.Identifier{"arena.bot.target-exists", "arena.bot.opponent-in-sights", "arena.bot.power-at-least"} {
		if !conditions[name] {
			t.Errorf("default bot chart does not demonstrate condition %q", name)
		}
	}
	if actions["arena.bot.observe"] {
		t.Fatal("default bot chart still hides decision-making behind arena.bot.observe")
	}
}

func TestPublishingBotDefinitionDoesNotRepinExistingActors(t *testing.T) {
	runtime, err := setupArena(context.Background(), runtimeOptions{TickInterval: time.Hour, Bots: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.stop(context.Background()) })
	matchRevision, _ := runtime.system.ActorRevision("match.main")
	before := runtime.listBots()
	candidate := runtime.bot.Definition()
	candidate.RevisionSalt = "published-without-rollout"
	published, err := runtime.publishBotDefinition(context.Background(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	if published == before[0].Revision {
		t.Fatal("publication did not produce a new bot revision")
	}
	if got, _ := runtime.system.ActorRevision("match.main"); got != matchRevision {
		t.Fatalf("match revision changed from %q to %q", matchRevision, got)
	}
	after := runtime.listBots()
	byPlayer := make(map[string]botStatus, len(after))
	for _, status := range after {
		byPlayer[status.Player] = status
	}
	for _, old := range before {
		if got, _ := runtime.system.ActorRevision(old.Controller); got != old.Revision {
			t.Errorf("controller %q revision changed from %q to %q", old.Controller, old.Revision, got)
		}
		if got := byPlayer[old.Player]; got != old {
			t.Errorf("bot status changed on publication: got %+v, want %+v", got, old)
		}
	}
}

func TestBotRolloutPreservesCreatureAndRejectsObsoleteController(t *testing.T) {
	ctx := context.Background()
	runtime, err := setupArena(ctx, runtimeOptions{TickInterval: time.Hour, Bots: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.stop(context.Background()) })
	frames := runtime.transport.registerTest("rollout-output")
	if err := startTestConnection(ctx, runtime.system, "connection.rollout", "match.main", "observer", "rollout-output"); err != nil {
		t.Fatal(err)
	}
	old := runtime.listBots()[0]
	initial := waitForPlayer(t, frames, old.Player, func(creature) bool { return true })
	input, _ := taggedStruct(inputTag, playerInput{Player: old.Player, Lease: string(old.Controller), Sequence: initial.LastSequence + 7, Action: actionShoot})
	if err := runtime.system.Tell(ctx, old.Match, statecharts.Event{Name: "player.input", Type: statecharts.EventExternal, Data: input}); err != nil {
		t.Fatal(err)
	}
	before := waitForPlayer(t, frames, old.Player, func(c creature) bool { return c.LastSequence == initial.LastSequence+7 })

	next, err := runtime.rolloutBot(ctx, old.Player)
	if err != nil {
		t.Fatal(err)
	}
	joined := waitForPlayer(t, frames, old.Player, func(c creature) bool { return c.Controller == string(next.Controller) })
	if joined.X != before.X || joined.Y != before.Y || joined.Health != before.Health || joined.Power != before.Power || joined.Score != before.Score || joined.LastSequence != before.LastSequence {
		t.Fatalf("takeover changed stable creature state: before=%+v after=%+v", before, joined)
	}
	if joined.DefinitionRevision != string(next.Revision) {
		t.Fatalf("creature definition revision = %q, want %q", joined.DefinitionRevision, next.Revision)
	}
	if got, _ := runtime.system.ActorRevision(next.Controller); got != next.Revision {
		t.Fatalf("replacement actual revision = %q, status reports %q", got, next.Revision)
	}
	// A fresh snapshot makes the replacement act. Its first accepted sequence
	// must continue above the match's authoritative sequence.
	snapshot := arenaSnapshot{Match: string(old.Match), Width: 9, Height: 9, Tick: 1, Creatures: []creature{joined}}
	snapshot.Creatures[0].X, snapshot.Creatures[0].Y = 2, 2
	value, _ := taggedStruct(snapshotTag, snapshot)
	if err := runtime.system.Tell(ctx, next.Controller, statecharts.Event{Name: "match.snapshot", Type: statecharts.EventExternal, Data: value}); err != nil {
		t.Fatal(err)
	}
	afterInput := waitForPlayer(t, frames, old.Player, func(c creature) bool { return c.LastSequence > before.LastSequence })
	if afterInput.LastSequence != before.LastSequence+1 {
		t.Fatalf("replacement first sequence = %d, want %d", afterInput.LastSequence, before.LastSequence+1)
	}

	// Stop is ordered before these messages to the old controller. Neither its
	// stale snapshot nor stale lease may act on or disconnect the creature.
	_ = runtime.system.Tell(ctx, old.Controller, statecharts.Event{Name: "match.snapshot", Type: statecharts.EventExternal, Data: value})
	disconnect, _ := taggedStruct(disconnectTag, disconnectRequest{Player: old.Player, Lease: string(old.Controller)})
	if err := runtime.system.Tell(ctx, old.Match, statecharts.Event{Name: "player.disconnect", Type: statecharts.EventExternal, Data: disconnect}); err != nil {
		t.Fatal(err)
	}
	stale, _ := taggedStruct(inputTag, playerInput{Player: old.Player, Lease: string(old.Controller), Sequence: afterInput.LastSequence + 100, Action: actionShoot})
	if err := runtime.system.Tell(ctx, old.Match, statecharts.Event{Name: "player.input", Type: statecharts.EventExternal, Data: stale}); err != nil {
		t.Fatal(err)
	}
	confirm, _ := taggedStruct(inputTag, playerInput{Player: old.Player, Lease: string(next.Controller), Sequence: afterInput.LastSequence + 1, Action: actionShoot})
	if err := runtime.system.Tell(ctx, old.Match, statecharts.Event{Name: "player.input", Type: statecharts.EventExternal, Data: confirm}); err != nil {
		t.Fatal(err)
	}
	final := waitForPlayer(t, frames, old.Player, func(c creature) bool { return c.LastSequence == afterInput.LastSequence+1 })
	if !final.Connected || final.Controller != string(next.Controller) {
		t.Fatalf("obsolete controller altered takeover: %+v", final)
	}
	if got := runtime.listBots()[1]; got.Player == old.Player || got.Generation != 1 {
		t.Fatalf("one-bot rollout changed unselected bot: %+v", got)
	}
}

func TestAllBotRolloutPinsEveryReplacementToOnePublishedRevision(t *testing.T) {
	runtime, err := setupArena(context.Background(), runtimeOptions{TickInterval: time.Hour, Bots: 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.stop(context.Background()) })
	candidate := runtime.bot.Definition()
	candidate.RevisionSalt = "all-bot-rollout"
	want, err := runtime.publishBotDefinition(context.Background(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	bots, err := runtime.rolloutBots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, bot := range bots {
		if bot.Generation != 2 || bot.Revision != want {
			t.Errorf("replacement status = %+v, want generation 2 revision %q", bot, want)
		}
		if got, _ := runtime.system.ActorRevision(bot.Controller); got != want {
			t.Errorf("controller %q pinned %q, want %q", bot.Controller, got, want)
		}
	}
}

func setFirstBotActionArguments(state *statecharts.StateDefinition, name statecharts.Identifier, arguments ...statecharts.Expression) bool {
	for transitionIndex := range state.Transitions {
		for blockIndex := range state.Transitions[transitionIndex].Actions {
			for actionIndex := range state.Transitions[transitionIndex].Actions[blockIndex] {
				action := &state.Transitions[transitionIndex].Actions[blockIndex][actionIndex]
				if action.Call != nil && action.Call.Function.Name == name {
					action.Call.Function.Args = arguments
					return true
				}
			}
		}
	}
	for index := range state.Children {
		if setFirstBotActionArguments(&state.Children[index], name, arguments...) {
			return true
		}
	}
	return false
}

func TestBotDefinitionWorkbenchUsesHTMLCFormAndComposableVocabulary(t *testing.T) {
	runtime, err := setupArena(context.Background(), runtimeOptions{TickInterval: time.Hour, Bots: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.stop(context.Background()) })
	server := httptest.NewServer(arenaHandler(runtime))
	t.Cleanup(server.Close)
	response, err := http.Get(server.URL + "/editor/bots")
	if err != nil {
		t.Fatal(err)
	}
	editor, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("editor response = %d %q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	if text := string(editor); !strings.Contains(text, "<bot-chart-editor") || !strings.Contains(text, "STATES &amp; TRANSITIONS") {
		t.Fatalf("editor is not rendered as the htmlc bot-chart-editor form component")
	} else if strings.Contains(text, `<textarea id="definition"`) || strings.Contains(text, "FULL DEFINITION") {
		t.Fatalf("editor still foregrounds the raw Definition JSON")
	}
	response, err = http.Get(server.URL + "/scripts/index.js")
	if err != nil {
		t.Fatal(err)
	}
	scriptIndex, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(scriptIndex), "bot-chart-editor") {
		t.Fatalf("htmlc script index = %d %q", response.StatusCode, scriptIndex)
	}

	response, err = http.Get(server.URL + "/definitions/bot/vocabulary")
	if err != nil {
		t.Fatal(err)
	}
	var vocabulary struct {
		Actions []struct {
			Name       string           `json:"name"`
			Version    string           `json:"version"`
			Parameters []map[string]any `json:"parameters"`
			Events     []string         `json:"events"`
			Example    json.RawMessage  `json:"example"`
		} `json:"actions"`
		Conditions []struct {
			Name       string           `json:"name"`
			Version    string           `json:"version"`
			Parameters []map[string]any `json:"parameters"`
			Events     []string         `json:"events"`
			Example    json.RawMessage  `json:"example"`
		} `json:"conditions"`
		Events []struct {
			Name string `json:"name"`
		} `json:"events"`
	}
	if err := json.NewDecoder(response.Body).Decode(&vocabulary); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(vocabulary.Actions) < 6 || len(vocabulary.Conditions) < 6 || len(vocabulary.Events) != 3 {
		t.Fatalf("vocabulary = %d %+v", response.StatusCode, vocabulary)
	}
	actionNames := map[string]bool{}
	for _, action := range vocabulary.Actions {
		if action.Name == "" || action.Version == "" || len(action.Events) == 0 || !json.Valid(action.Example) {
			t.Fatalf("incomplete action vocabulary entry: %+v", action)
		}
		actionNames[action.Name] = true
	}
	conditionNames := map[string]bool{}
	for _, condition := range vocabulary.Conditions {
		if condition.Name == "" || condition.Version == "" || len(condition.Events) == 0 || !json.Valid(condition.Example) {
			t.Fatalf("incomplete condition vocabulary entry: %+v", condition)
		}
		conditionNames[condition.Name] = true
	}
	for _, name := range []string{"arena.bot.move", "arena.bot.move-toward", "arena.bot.shoot", "arena.bot.wander"} {
		if !actionNames[name] {
			t.Errorf("action vocabulary omits %q", name)
		}
	}
	for _, name := range []string{"arena.bot.target-exists", "arena.bot.target-within", "arena.bot.opponent-in-sights", "arena.bot.health-below", "arena.bot.power-at-least", "arena.bot.tick-every"} {
		if !conditionNames[name] {
			t.Errorf("condition vocabulary omits %q", name)
		}
	}

	response, err = http.Get(server.URL + "/definitions/bot")
	if err != nil {
		t.Fatal(err)
	}
	definitionData, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.Header.Get("X-Statechart-Revision") == "" {
		t.Fatal("definition response omitted current revision")
	}
	definition, err := statejson.Unmarshal(definitionData)
	if err != nil {
		t.Fatal(err)
	}
	active := &definition.Root.Children[0]
	transitions := active.Transitions[:0]
	for _, transition := range active.Transitions {
		if len(transition.Events) != 1 || transition.Events[0] != "match.snapshot" {
			transitions = append(transitions, transition)
		}
	}
	active.Transitions = transitions
	definition.RevisionSalt = "free-form-no-decisions"
	definitionData, err = statejson.Marshal(definition)
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.Post(server.URL+"/definitions/bot/validate", "application/json", bytes.NewReader(definitionData))
	if err != nil {
		t.Fatal(err)
	}
	message, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("free-form definition validation = %d: %s", response.StatusCode, message)
	}

	request, err := http.NewRequest(http.MethodPut, server.URL+"/definitions/bot", bytes.NewReader(definitionData))
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var published struct {
		Revision statecharts.RevisionID `json:"revision"`
	}
	if err := json.NewDecoder(response.Body).Decode(&published); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || published.Revision == "" {
		t.Fatalf("free-form definition publication = %d %+v", response.StatusCode, published)
	}
	response, err = http.Post(server.URL+"/bots/bot.1/rollout", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var rolledOut botStatus
	if err := json.NewDecoder(response.Body).Decode(&rolledOut); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || rolledOut.Revision != published.Revision || rolledOut.Generation != 2 {
		t.Fatalf("free-form rollout = %d %+v, want revision %q generation 2", response.StatusCode, rolledOut, published.Revision)
	}
}

func TestSpectatorSocketDeliversSnapshotWithoutCreatingCreature(t *testing.T) {
	runtime, err := setupArena(context.Background(), runtimeOptions{TickInterval: time.Hour, Bots: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.stop(context.Background()) })
	server := httptest.NewServer(arenaHandler(runtime))
	t.Cleanup(server.Close)
	connection, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http")+"/ws?match=match.main&spectate=1&player=spectator.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	message := readSocketUntil(t, connection, func(message serverMessage) bool { return message.Type == "snapshot" })
	if snapshotPlayer(message.Snapshot, "spectator.test").ID != "" {
		t.Fatal("spectator created a creature")
	}
	if snapshotPlayer(message.Snapshot, "bot.1").ID == "" {
		t.Fatal("spectator did not receive authoritative bot snapshot")
	}
}

func TestWebSocketReconnectResubscribesToAuthoritativeState(t *testing.T) {
	runtime, err := setupArena(context.Background(), runtimeOptions{TickInterval: time.Hour, Bots: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.stop(context.Background()) })
	server := httptest.NewServer(arenaHandler(runtime))
	t.Cleanup(server.Close)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?match=match.main&player=reconnect"

	first, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	readSocketUntil(t, first, func(message serverMessage) bool {
		return message.Type == "snapshot" && snapshotPlayer(message.Snapshot, "reconnect").ID != ""
	})
	input, _ := json.Marshal(clientMessage{Version: protocolVersion, Type: "input", Sequence: 1, Action: actionRight})
	if err := first.Write(context.Background(), websocket.MessageText, input); err != nil {
		t.Fatal(err)
	}
	moved := readSocketUntil(t, first, func(message serverMessage) bool {
		return message.Type == "snapshot" && snapshotPlayer(message.Snapshot, "reconnect").LastSequence == 1
	})
	first.CloseNow()

	second, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseNow()
	reconnected := readSocketUntil(t, second, func(message serverMessage) bool {
		return message.Type == "snapshot" && snapshotPlayer(message.Snapshot, "reconnect").LastSequence == 1
	})
	if got, want := snapshotPlayer(reconnected.Snapshot, "reconnect").X, snapshotPlayer(moved.Snapshot, "reconnect").X; got != want {
		t.Fatalf("reconnected position = %d, want authoritative %d", got, want)
	}
}

func TestReconnectLeaseSurvivesOldConnectionExpiry(t *testing.T) {
	ctx := context.Background()
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	transport := newSocketTransport()
	system, err := newTestSystem(clock, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	firstFrames := transport.registerTest("lease-first")
	secondFrames := transport.registerTest("lease-second")
	if err := spawnMatch(ctx, system, "match.lease"); err != nil {
		t.Fatal(err)
	}
	if err := startTestConnection(ctx, system, "connection.lease-old", "match.lease", "lease-player", "lease-first"); err != nil {
		t.Fatal(err)
	}
	waitForPlayer(t, firstFrames, "lease-player", func(creature) bool { return true })
	if err := system.Tell(ctx, "connection.lease-old", statecharts.Event{Name: "connection.close", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	if err := startTestConnection(ctx, system, "connection.lease-new", "match.lease", "lease-player", "lease-second"); err != nil {
		t.Fatal(err)
	}
	waitForPlayer(t, secondFrames, "lease-player", func(creature) bool { return true })
	clock.Advance(playerDisconnectGrace + time.Second)
	tellClientInput(t, system, "connection.lease-new", 1, actionRight)
	waitForPlayer(t, secondFrames, "lease-player", func(c creature) bool { return c.LastSequence == 1 })
}

func TestSupersededConnectionCannotControlPlayer(t *testing.T) {
	ctx := context.Background()
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	transport := newSocketTransport()
	system, err := newTestSystem(clock, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	oldFrames := transport.registerTest("authority-old")
	newFrames := transport.registerTest("authority-new")
	if err := spawnMatch(ctx, system, "match.authority"); err != nil {
		t.Fatal(err)
	}
	if err := startTestConnection(ctx, system, "connection.authority-old", "match.authority", "authority-player", "authority-old"); err != nil {
		t.Fatal(err)
	}
	waitForPlayer(t, oldFrames, "authority-player", func(creature) bool { return true })
	if err := startTestConnection(ctx, system, "connection.authority-new", "match.authority", "authority-player", "authority-new"); err != nil {
		t.Fatal(err)
	}
	initial := waitForPlayer(t, newFrames, "authority-player", func(creature) bool { return true })
	tellClientInput(t, system, "connection.authority-old", 99, actionRight)
	tellClientInput(t, system, "connection.authority-new", 1, actionDown)
	current := waitForPlayer(t, newFrames, "authority-player", func(c creature) bool { return c.LastSequence > 0 })
	if current.LastSequence != 1 {
		t.Fatalf("authoritative sequence = %d, old connection was not revoked", current.LastSequence)
	}
	if current.X != initial.X || current.Y != initial.Y+1 {
		t.Fatalf("authoritative position = (%d,%d), want new connection move from (%d,%d)", current.X, current.Y, initial.X, initial.Y)
	}
}

func TestDisconnectedPlayerExpiresAfterGracePeriod(t *testing.T) {
	ctx := context.Background()
	clock := statecharts.NewManualClock(time.Unix(0, 0))
	transport := newSocketTransport()
	system, err := newTestSystem(clock, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	leavingFrames := transport.registerTest("leaving-output")
	observerFrames := transport.registerTest("expiry-observer-output")
	if err := spawnMatch(ctx, system, "match.expiry"); err != nil {
		t.Fatal(err)
	}
	if err := startTestConnection(ctx, system, "connection.leaving", "match.expiry", "leaving-player", "leaving-output"); err != nil {
		t.Fatal(err)
	}
	waitForPlayer(t, leavingFrames, "leaving-player", func(creature) bool { return true })
	if err := startTestConnection(ctx, system, "connection.expiry-observer", "match.expiry", "observer", "expiry-observer-output"); err != nil {
		t.Fatal(err)
	}
	waitForPlayer(t, observerFrames, "leaving-player", func(creature) bool { return true })
	if err := system.Tell(ctx, "connection.leaving", statecharts.Event{Name: "connection.close", Type: statecharts.EventExternal}); err != nil {
		t.Fatal(err)
	}
	waitForPlayer(t, observerFrames, "leaving-player", func(c creature) bool { return !c.Connected })
	clock.Advance(playerDisconnectGrace + time.Second)
	waitForSnapshot(t, observerFrames, func(snapshot arenaSnapshot) bool {
		return snapshotPlayer(snapshot, "leaving-player").ID == ""
	})
}

func TestDefinitionAdministrationValidatesPublishesAndPinsNewMatch(t *testing.T) {
	runtime, err := setupArena(context.Background(), runtimeOptions{TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.stop(context.Background()) })
	server := httptest.NewServer(arenaHandler(runtime))
	t.Cleanup(server.Close)

	response, err := http.Get(server.URL + "/definitions/match")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("export definition: status=%d err=%v", response.StatusCode, err)
	}
	definition, err := statejson.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if !setActionVersion(&definition.Root, "arena.match.apply-input", "v2") {
		t.Fatal("exported match definition has no input behavior")
	}
	definition.RevisionSalt = "admin-test-v2"
	candidate, err := statejson.Marshal(definition)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []struct{ method, path string }{
		{http.MethodPost, "/definitions/match/validate"},
		{http.MethodPut, "/definitions/match"},
	} {
		request, _ := http.NewRequest(operation.method, server.URL+operation.path, bytes.NewReader(candidate))
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s %s status = %d", operation.method, operation.path, response.StatusCode)
		}
	}
	response, err = http.Post(server.URL+"/matches/match.canary", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("create canary status = %d", response.StatusCode)
	}
	current, _ := runtime.system.ActorRevision("match.canary")
	original, _ := runtime.system.ActorRevision("match.main")
	if current == "" || current == original {
		t.Fatalf("canary revision = %q, original = %q", current, original)
	}
}

func setActionVersion(state *statecharts.StateDefinition, name statecharts.Identifier, version string) bool {
	changed := false
	for transitionIndex := range state.Transitions {
		for blockIndex := range state.Transitions[transitionIndex].Actions {
			for actionIndex := range state.Transitions[transitionIndex].Actions[blockIndex] {
				action := &state.Transitions[transitionIndex].Actions[blockIndex][actionIndex]
				if action.Call != nil && action.Call.Function.Name == name {
					action.Call.Function.Version = version
					changed = true
				}
			}
		}
	}
	for index := range state.Children {
		changed = setActionVersion(&state.Children[index], name, version) || changed
	}
	return changed
}

func tellClientInput(t *testing.T, system *actors.System, connection statecharts.Identifier, sequence uint64, action string) {
	t.Helper()
	data, _ := json.Marshal(clientMessage{Version: protocolVersion, Type: "input", Sequence: sequence, Action: action})
	value, _ := statecharts.StringValue(string(data))
	if err := system.Tell(context.Background(), connection, statecharts.Event{Name: "client.message", Type: statecharts.EventExternal, Data: value}); err != nil {
		t.Fatal(err)
	}
}

func waitForPlayer(t *testing.T, frames <-chan []byte, player string, accept func(creature) bool) creature {
	t.Helper()
	snapshot := waitForSnapshot(t, frames, func(snapshot arenaSnapshot) bool {
		creature := snapshotPlayer(snapshot, player)
		return creature.ID != "" && accept(creature)
	})
	return snapshotPlayer(snapshot, player)
}

func waitForSnapshot(t *testing.T, frames <-chan []byte, accept func(arenaSnapshot) bool) arenaSnapshot {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case frame := <-frames:
			var message serverMessage
			if err := json.Unmarshal(frame, &message); err != nil {
				t.Fatalf("decode server frame %q: %v", frame, err)
			}
			if message.Type == "snapshot" && accept(message.Snapshot) {
				return message.Snapshot
			}
		case <-timer.C:
			t.Fatal("timed out waiting for arena snapshot")
		}
	}
}

func readSocketUntil(t *testing.T, connection *websocket.Conn, accept func(serverMessage) bool) serverMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		_, data, err := connection.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var message serverMessage
		if err := json.Unmarshal(data, &message); err != nil {
			t.Fatalf("decode server message: %v", err)
		}
		if accept(message) {
			return message
		}
	}
}

type discardDispatcher struct{}

func (discardDispatcher) Deliver(context.Context, statecharts.Event) error { return nil }
