package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

const (
	matchKind      statecharts.Identifier = "arena.match"
	connectionKind statecharts.Identifier = "arena.connection"
	botKind        statecharts.Identifier = "arena.bot"

	playerDisconnectGrace = 10 * time.Second
)

type matchModel struct {
	World       world             `json:"world"`
	Match       string            `json:"match"`
	Revision    string            `json:"revision"`
	Subscribers map[string]bool   `json:"subscribers"`
	Leases      map[string]string `json:"leases"`
}

func buildMatchChart(tickInterval time.Duration) (*statecharts.Chart, error) {
	match := statecharts.New(matchKind, func() *matchModel {
		world := newWorld(19, 13, 0x5eed)
		world.Powerups = []powerup{{X: 5, Y: 3, Kind: "charge"}, {X: 13, Y: 9, Kind: "charge"}}
		return &matchModel{World: world, Subscribers: map[string]bool{}, Leases: map[string]string{}}
	}, statecharts.Version("v1"))

	publish := func(data *matchModel, ec statecharts.ExecContext) error {
		snapshot, err := taggedStruct(snapshotTag, data.World.snapshot(data.Match, data.Revision))
		if err != nil {
			return err
		}
		targets := make([]string, 0, len(data.Subscribers))
		for target := range data.Subscribers {
			targets = append(targets, target)
		}
		sort.Strings(targets)
		for _, target := range targets {
			ec.Send("match.snapshot", statecharts.SendOptions{Target: statecharts.Identifier(target), Data: snapshot})
		}
		return nil
	}

	schedule := match.Action("schedule-tick", func(_ *matchModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ec.Send("tick", statecharts.SendOptions{Delay: tickInterval})
		return nil
	})
	configure := match.Action("configure", func(data *matchModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var config matchConfig
		if err := decodeTaggedStruct(event.Data, matchConfigTag, &config); err != nil {
			return err
		}
		data.Match, data.Revision = ec.SessionID(), config.Revision
		return nil
	})
	join := match.Action("join", func(data *matchModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var request joinRequest
		if err := decodeTaggedStruct(event.Data, joinTag, &request); err != nil {
			return err
		}
		if request.Player == "" || request.Name == "" || request.Color == "" || request.Lease == "" {
			return fmt.Errorf("join requires player, name, color, and lease")
		}
		data.Leases[request.Player] = request.Lease
		data.World.addPlayer(request.Player, request.Name, request.Color, request.Bot)
		return publish(data, ec)
	})
	disconnect := match.Action("disconnect", func(data *matchModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var request disconnectRequest
		if err := decodeTaggedStruct(event.Data, disconnectTag, &request); err != nil {
			return err
		}
		if data.Leases[request.Player] != request.Lease {
			return nil
		}
		actor := data.World.Creatures[request.Player]
		actor.Connected = false
		data.World.Creatures[request.Player] = actor
		expiry, err := taggedStruct(disconnectTag, request)
		if err != nil {
			return err
		}
		ec.Send("player.expire", statecharts.SendOptions{Delay: playerDisconnectGrace, Data: expiry})
		return publish(data, ec)
	})
	expire := match.Action("expire-player", func(data *matchModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var request disconnectRequest
		if err := decodeTaggedStruct(event.Data, disconnectTag, &request); err != nil {
			return err
		}
		if data.Leases[request.Player] != request.Lease {
			return nil
		}
		delete(data.Leases, request.Player)
		delete(data.World.Creatures, request.Player)
		projectiles := data.World.Projectiles[:0]
		for _, shot := range data.World.Projectiles {
			if shot.Owner != request.Player {
				projectiles = append(projectiles, shot)
			}
		}
		data.World.Projectiles = projectiles
		return publish(data, ec)
	})
	subscribe := match.Action("subscribe", func(data *matchModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var request subscription
		if err := decodeTaggedStruct(event.Data, subscriptionTag, &request); err != nil {
			return err
		}
		if _, err := statecharts.NewIdentifier(request.Target); err != nil {
			return fmt.Errorf("invalid subscriber: %w", err)
		}
		data.Subscribers[request.Target] = true
		return publish(data, ec)
	})
	unsubscribe := match.Action("unsubscribe", func(data *matchModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		target, ok := event.Data.AsString()
		if !ok {
			return fmt.Errorf("unsubscribe target is not a string")
		}
		delete(data.Subscribers, target)
		return nil
	})
	registerInput := func(version string, movement int) statecharts.GoActionRef {
		return match.ActionVersion("apply-input", version, func(data *matchModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
			event, _ := ec.Event()
			var input playerInput
			if err := decodeTaggedStruct(event.Data, inputTag, &input); err != nil {
				return err
			}
			if data.Leases[input.Player] != input.Lease {
				return nil
			}
			if data.World.applyInput(input, movement) {
				return publish(data, ec)
			}
			return nil
		})
	}
	applyInput := registerInput("v1", 1)
	registerInput("v2", 2)
	tick := match.Action("advance-tick", func(data *matchModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		data.World.advance()
		return publish(data, ec)
	})

	return match.Build(statecharts.Atomic(matchKind,
		statecharts.OnEntry(schedule.Do()),
		statecharts.On("match.configure", statecharts.Then(configure.Call())),
		statecharts.On("player.join", statecharts.Then(join.Call())),
		statecharts.On("player.disconnect", statecharts.Then(disconnect.Call())),
		statecharts.On("player.expire", statecharts.Then(expire.Call())),
		statecharts.On("player.input", statecharts.Then(applyInput.Call())),
		statecharts.On("subscriber.add", statecharts.Then(subscribe.Call())),
		statecharts.On("subscriber.remove", statecharts.Then(unsubscribe.Call())),
		statecharts.On("tick", statecharts.Then(tick.Call(), schedule.Call())),
	))
}

type connectionModel struct {
	Match        string `json:"match"`
	Player       string `json:"player"`
	Output       string `json:"output"`
	LastSequence uint64 `json:"last_sequence"`
}

func buildConnectionChart() (*statecharts.Chart, error) {
	connection := statecharts.New(connectionKind, func() *connectionModel { return &connectionModel{} }, statecharts.Version("v1"))
	start := connection.Action("start", func(data *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var config connectionConfig
		if err := decodeTaggedStruct(event.Data, connectionConfigTag, &config); err != nil {
			return err
		}
		data.Match, data.Player, data.Output = config.Match, config.Player, config.Output
		welcome, err := encodeServerMessage(serverMessage{Type: "welcome", Player: config.Player})
		if err != nil {
			return err
		}
		ec.Send("socket.frame", statecharts.SendOptions{Target: statecharts.Identifier(config.Output), Type: socketIOProcessor, Data: welcome})
		join, err := taggedStruct(joinTag, joinRequest{Player: config.Player, Name: config.Name, Color: config.Color, Lease: ec.SessionID()})
		if err != nil {
			return err
		}
		ec.Send("player.join", statecharts.SendOptions{Target: statecharts.Identifier(config.Match), Data: join})
		subscribe, err := taggedStruct(subscriptionTag, subscription{Target: ec.SessionID()})
		if err != nil {
			return err
		}
		ec.Send("subscriber.add", statecharts.SendOptions{Target: statecharts.Identifier(config.Match), Data: subscribe})
		return nil
	})
	clientInput := connection.Action("route-client-input", func(data *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		message, err := parseClientMessage(event.Data)
		if err != nil {
			return err
		}
		if message.Sequence <= data.LastSequence {
			return nil
		}
		data.LastSequence = message.Sequence
		input, err := taggedStruct(inputTag, playerInput{Player: data.Player, Lease: ec.SessionID(), Sequence: message.Sequence, Action: message.Action})
		if err != nil {
			return err
		}
		ec.Send("player.input", statecharts.SendOptions{Target: statecharts.Identifier(data.Match), Data: input})
		return nil
	})
	emit := connection.Action("emit-snapshot", func(data *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var snapshot arenaSnapshot
		if err := decodeTaggedStruct(event.Data, snapshotTag, &snapshot); err != nil {
			return err
		}
		frame, err := encodeServerMessage(serverMessage{Type: "snapshot", Snapshot: snapshot})
		if err != nil {
			return err
		}
		ec.Send("socket.frame", statecharts.SendOptions{Target: statecharts.Identifier(data.Output), Type: socketIOProcessor, Data: frame})
		return nil
	})
	closeConnection := connection.Action("close", func(data *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		if data.Match == "" {
			return nil
		}
		target, _ := statecharts.StringValue(ec.SessionID())
		ec.Send("subscriber.remove", statecharts.SendOptions{Target: statecharts.Identifier(data.Match), Data: target})
		disconnect, err := taggedStruct(disconnectTag, disconnectRequest{Player: data.Player, Lease: ec.SessionID()})
		if err != nil {
			return err
		}
		ec.Send("player.disconnect", statecharts.SendOptions{Target: statecharts.Identifier(data.Match), Data: disconnect})
		return nil
	})
	return connection.Build(statecharts.Compound(connectionKind, "open", statecharts.Children(
		statecharts.Atomic("open",
			statecharts.On("connection.start", statecharts.Then(start.Call())),
			statecharts.On("client.message", statecharts.Then(clientInput.Call())),
			statecharts.On("match.snapshot", statecharts.Then(emit.Call())),
			statecharts.On("connection.close", statecharts.Target("closed"), statecharts.Then(closeConnection.Call())),
		),
		statecharts.Final("closed"),
	)))
}

type botModel struct {
	Match        string `json:"match"`
	Player       string `json:"player"`
	LastTick     uint64 `json:"last_tick"`
	NextSequence uint64 `json:"next_sequence"`
}

func buildBotChart() (*statecharts.Chart, error) {
	bot := statecharts.New(botKind, func() *botModel { return &botModel{} }, statecharts.Version("v1"))
	start := bot.Action("start", func(data *botModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var config botConfig
		if err := decodeTaggedStruct(event.Data, botConfigTag, &config); err != nil {
			return err
		}
		data.Match, data.Player = config.Match, config.Player
		join, err := taggedStruct(joinTag, joinRequest{Player: config.Player, Name: config.Name, Color: config.Color, Lease: ec.SessionID(), Bot: true})
		if err != nil {
			return err
		}
		ec.Send("player.join", statecharts.SendOptions{Target: statecharts.Identifier(config.Match), Data: join})
		subscribe, err := taggedStruct(subscriptionTag, subscription{Target: ec.SessionID()})
		if err != nil {
			return err
		}
		ec.Send("subscriber.add", statecharts.SendOptions{Target: statecharts.Identifier(config.Match), Data: subscribe})
		return nil
	})
	observe := bot.Action("observe", func(data *botModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var snapshot arenaSnapshot
		if err := decodeTaggedStruct(event.Data, snapshotTag, &snapshot); err != nil {
			return err
		}
		if snapshot.Tick <= data.LastTick {
			return nil
		}
		data.LastTick = snapshot.Tick
		action := chooseBotAction(snapshot, data.Player)
		if action == "" {
			return nil
		}
		data.NextSequence++
		input, err := taggedStruct(inputTag, playerInput{Player: data.Player, Lease: ec.SessionID(), Sequence: data.NextSequence, Action: action})
		if err != nil {
			return err
		}
		ec.Send("player.input", statecharts.SendOptions{Target: statecharts.Identifier(data.Match), Data: input})
		return nil
	})
	return bot.Build(statecharts.Atomic(botKind,
		statecharts.On("bot.start", statecharts.Then(start.Call())),
		statecharts.On("match.snapshot", statecharts.Then(observe.Call())),
	))
}

func chooseBotAction(snapshot arenaSnapshot, player string) string {
	self := snapshotPlayer(snapshot, player)
	if self.ID == "" {
		return ""
	}
	var targetX, targetY int
	found := false
	best := int(^uint(0) >> 1)
	for _, other := range snapshot.Creatures {
		if other.ID == player {
			continue
		}
		distance := abs(other.X-self.X) + abs(other.Y-self.Y)
		if distance < best {
			best, targetX, targetY, found = distance, other.X, other.Y, true
		}
	}
	for _, item := range snapshot.Powerups {
		distance := abs(item.X-self.X) + abs(item.Y-self.Y)
		if distance < best {
			best, targetX, targetY, found = distance, item.X, item.Y, true
		}
	}
	if !found {
		return []string{actionRight, actionDown, actionLeft, actionUp}[snapshot.Tick%4]
	}
	if self.Y == targetY && self.Facing == horizontalDirection(targetX-self.X) {
		return actionShoot
	}
	if self.X == targetX && self.Facing == verticalDirection(targetY-self.Y) {
		return actionShoot
	}
	if abs(targetX-self.X) >= abs(targetY-self.Y) {
		return horizontalDirection(targetX - self.X)
	}
	return verticalDirection(targetY - self.Y)
}

func horizontalDirection(delta int) string {
	if delta < 0 {
		return actionLeft
	}
	return actionRight
}

func verticalDirection(delta int) string {
	if delta < 0 {
		return actionUp
	}
	return actionDown
}

func abs(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func spawnMatch(ctx context.Context, system *actors.System, id statecharts.Identifier) error {
	if err := system.Spawn(ctx, id, matchKind); err != nil {
		return err
	}
	revision, ok := system.ActorRevision(id)
	if !ok {
		return fmt.Errorf("match %q has no pinned revision", id)
	}
	config, err := taggedStruct(matchConfigTag, matchConfig{Revision: string(revision)})
	if err != nil {
		return err
	}
	return system.Tell(ctx, id, statecharts.Event{Name: "match.configure", Type: statecharts.EventExternal, Data: config})
}

func startTestConnection(ctx context.Context, system *actors.System, id, match, player, output statecharts.Identifier) error {
	if err := system.Spawn(ctx, id, connectionKind); err != nil {
		return err
	}
	config, err := taggedStruct(connectionConfigTag, connectionConfig{Match: string(match), Player: string(player), Name: string(player), Color: "#f8fafc", Output: string(output)})
	if err != nil {
		return err
	}
	return system.Tell(ctx, id, statecharts.Event{Name: "connection.start", Type: statecharts.EventExternal, Data: config})
}

func spawnBot(ctx context.Context, system *actors.System, id, match statecharts.Identifier, color string) error {
	if err := system.Spawn(ctx, id, botKind); err != nil {
		return err
	}
	config, err := taggedStruct(botConfigTag, botConfig{Match: string(match), Player: string(id), Name: "BOT " + string(id), Color: color})
	if err != nil {
		return err
	}
	return system.Tell(ctx, id, statecharts.Event{Name: "bot.start", Type: statecharts.EventExternal, Data: config})
}
