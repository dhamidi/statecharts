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
	waitForPlayer(t, frames, "bot.cyan", func(creature) bool { return true })
	clock.Advance(100 * time.Millisecond)
	bot := waitForPlayer(t, frames, "bot.cyan", func(c creature) bool { return c.LastSequence > 0 })
	if bot.LastSequence != 1 {
		t.Fatalf("bot sequence = %d, want 1", bot.LastSequence)
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
