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
		return &matchModel{World: newDefaultWorld(), Subscribers: map[string]bool{}, Leases: map[string]string{}}
	}, statecharts.Version("v2"))

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
		data.World.addPlayer(request.Player, request.Name, request.Color, request.Bot, request.Controller, request.DefinitionRevision)
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
	Spectate     bool   `json:"spectate"`
	LastSequence uint64 `json:"last_sequence"`
}

func buildConnectionChart() (*statecharts.Chart, error) {
	connection := statecharts.New(connectionKind, func() *connectionModel { return &connectionModel{} }, statecharts.Version("v2"))
	start := connection.Action("start", func(data *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var config connectionConfig
		if err := decodeTaggedStruct(event.Data, connectionConfigTag, &config); err != nil {
			return err
		}
		data.Match, data.Player, data.Output = config.Match, config.Player, config.Output
		data.Spectate = config.Spectate
		welcome, err := encodeServerMessage(serverMessage{Type: "welcome", Player: config.Player})
		if err != nil {
			return err
		}
		ec.Send("socket.frame", statecharts.SendOptions{Target: statecharts.Identifier(config.Output), Type: socketIOProcessor, Data: welcome})
		if !config.Spectate {
			join, err := taggedStruct(joinTag, joinRequest{Player: config.Player, Name: config.Name, Color: config.Color, Lease: ec.SessionID()})
			if err != nil {
				return err
			}
			ec.Send("player.join", statecharts.SendOptions{Target: statecharts.Identifier(config.Match), Data: join})
		}
		subscribe, err := taggedStruct(subscriptionTag, subscription{Target: ec.SessionID()})
		if err != nil {
			return err
		}
		ec.Send("subscriber.add", statecharts.SendOptions{Target: statecharts.Identifier(config.Match), Data: subscribe})
		return nil
	})
	clientInput := connection.Action("route-client-input", func(data *connectionModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		if data.Spectate {
			return nil
		}
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
		if !data.Spectate {
			disconnect, err := taggedStruct(disconnectTag, disconnectRequest{Player: data.Player, Lease: ec.SessionID()})
			if err != nil {
				return err
			}
			ec.Send("player.disconnect", statecharts.SendOptions{Target: statecharts.Identifier(data.Match), Data: disconnect})
		}
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

type botReferences struct {
	start            statecharts.GoActionRef
	stop             statecharts.GoActionRef
	move             statecharts.GoActionRef
	moveToward       statecharts.GoActionRef
	shoot            statecharts.GoActionRef
	reload           statecharts.GoActionRef
	wander           statecharts.GoActionRef
	targetExists     statecharts.GoConditionRef
	targetWithin     statecharts.GoConditionRef
	opponentInSights statecharts.GoConditionRef
	weaponEmpty      statecharts.GoConditionRef
	healthBelow      statecharts.GoConditionRef
	powerAtLeast     statecharts.GoConditionRef
	tickEvery        statecharts.GoConditionRef
}

func newBotBuilder() (*statecharts.Builder[botModel], botReferences) {
	bot := statecharts.New(botKind, func() *botModel { return &botModel{} }, statecharts.Version("v2"))
	refs := botReferences{}
	refs.start = bot.Action("start", func(data *botModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, _ := ec.Event()
		var config botConfig
		if err := decodeTaggedStruct(event.Data, botConfigTag, &config); err != nil {
			return err
		}
		data.Match, data.Player = config.Match, config.Player
		join, err := taggedStruct(joinTag, joinRequest{Player: config.Player, Name: config.Name, Color: config.Color, Lease: ec.SessionID(), Bot: true, Controller: ec.SessionID(), DefinitionRevision: config.DefinitionRevision})
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
	refs.move = bot.Action("move", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) error {
		if len(args) != 1 {
			return fmt.Errorf("bot move requires one direction argument")
		}
		direction, err := botStringArgument(args, 0, "direction", actionUp, actionDown, actionLeft, actionRight)
		if err != nil {
			return err
		}
		return emitBotAction(data, ec, direction)
	})
	refs.moveToward = bot.Action("move-toward", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) error {
		if len(args) != 1 {
			return fmt.Errorf("bot move-toward requires one target argument")
		}
		targetKind, err := botStringArgument(args, 0, "target", botTargetNearest, botTargetOpponent, botTargetPowerup)
		if err != nil {
			return err
		}
		snapshot, self, err := botSnapshot(data, ec)
		if err != nil {
			return err
		}
		target, creature, ok := nearestBotTarget(snapshot, self, targetKind)
		if !ok {
			return consumeBotTick(data, ec, snapshot, "")
		}
		return consumeBotTick(data, ec, snapshot, botRoute(snapshot, self, target, creature))
	})
	refs.shoot = bot.Action("shoot", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) error {
		if len(args) != 0 {
			return fmt.Errorf("bot shoot takes no arguments")
		}
		return emitBotAction(data, ec, actionShoot)
	})
	refs.reload = bot.Action("reload", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) error {
		if len(args) != 0 {
			return fmt.Errorf("bot reload takes no arguments")
		}
		return emitBotAction(data, ec, actionReload)
	})
	refs.wander = bot.Action("wander", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) error {
		if len(args) != 0 {
			return fmt.Errorf("bot wander takes no arguments")
		}
		snapshot, self, err := botSnapshot(data, ec)
		if err != nil {
			return err
		}
		return consumeBotTick(data, ec, snapshot, botFallbackAction(snapshot, self))
	})
	refs.targetExists = bot.Condition("target-exists", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) (bool, error) {
		if len(args) != 1 {
			return false, fmt.Errorf("bot target-exists requires one target argument")
		}
		targetKind, err := botStringArgument(args, 0, "target", botTargetNearest, botTargetOpponent, botTargetPowerup)
		if err != nil {
			return false, err
		}
		snapshot, self, err := botSnapshot(data, ec)
		if err != nil {
			return false, err
		}
		_, _, ok := nearestBotTarget(snapshot, self, targetKind)
		return ok, nil
	})
	refs.targetWithin = bot.Condition("target-within", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) (bool, error) {
		if len(args) != 2 {
			return false, fmt.Errorf("bot target-within requires target and distance arguments")
		}
		targetKind, err := botStringArgument(args, 0, "target", botTargetNearest, botTargetOpponent, botTargetPowerup)
		if err != nil {
			return false, err
		}
		distance, err := botIntArgument(args, 1, "distance", 0)
		if err != nil {
			return false, err
		}
		snapshot, self, err := botSnapshot(data, ec)
		if err != nil {
			return false, err
		}
		target, _, ok := nearestBotTarget(snapshot, self, targetKind)
		return ok && abs(target.X-self.X)+abs(target.Y-self.Y) <= distance, nil
	})
	refs.opponentInSights = bot.Condition("opponent-in-sights", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) (bool, error) {
		if len(args) != 1 {
			return false, fmt.Errorf("bot opponent-in-sights requires one range argument")
		}
		rangeLimit, err := botIntArgument(args, 0, "range", 1)
		if err != nil {
			return false, err
		}
		snapshot, self, err := botSnapshot(data, ec)
		if err != nil {
			return false, err
		}
		for _, other := range snapshot.Creatures {
			if other.ID == self.ID || !other.Connected {
				continue
			}
			distance := abs(other.X-self.X) + abs(other.Y-self.Y)
			direction, aligned := alignedDirection(self, tile{X: other.X, Y: other.Y})
			if distance <= rangeLimit && aligned && direction == self.Facing && botLineClear(snapshot, self, tile{X: other.X, Y: other.Y}) {
				return true, nil
			}
		}
		return false, nil
	})
	refs.weaponEmpty = bot.Condition("weapon-empty", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) (bool, error) {
		if len(args) != 0 {
			return false, fmt.Errorf("bot weapon-empty takes no arguments")
		}
		_, self, err := botSnapshot(data, ec)
		return err == nil && !self.Loaded, err
	})
	refs.healthBelow = bot.Condition("health-below", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) (bool, error) {
		if len(args) != 1 {
			return false, fmt.Errorf("bot health-below requires one health argument")
		}
		threshold, err := botIntArgument(args, 0, "health", 1)
		if err != nil {
			return false, err
		}
		_, self, err := botSnapshot(data, ec)
		return self.Health < threshold, err
	})
	refs.powerAtLeast = bot.Condition("power-at-least", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) (bool, error) {
		if len(args) != 1 {
			return false, fmt.Errorf("bot power-at-least requires one power argument")
		}
		threshold, err := botIntArgument(args, 0, "power", 0)
		if err != nil {
			return false, err
		}
		_, self, err := botSnapshot(data, ec)
		return self.Power >= threshold, err
	})
	refs.tickEvery = bot.Condition("tick-every", func(data *botModel, ec statecharts.ExecContext, args []statecharts.Value) (bool, error) {
		if len(args) != 1 {
			return false, fmt.Errorf("bot tick-every requires one interval argument")
		}
		interval, err := botIntArgument(args, 0, "interval", 1)
		if err != nil {
			return false, err
		}
		snapshot, _, err := botSnapshot(data, ec)
		return err == nil && snapshot.Tick%uint64(interval) == 0, err
	})
	refs.stop = bot.Action("stop", func(data *botModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
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
	return bot, refs
}

func buildBotChart() (*statecharts.Chart, error) {
	bot, refs := newBotBuilder()
	nearest, _ := statecharts.StringValue(botTargetNearest)
	opponent, _ := statecharts.StringValue(botTargetOpponent)
	powerup, _ := statecharts.StringValue(botTargetPowerup)
	right, _ := statecharts.StringValue(actionRight)
	return bot.Build(statecharts.Compound(botKind, "active", statecharts.Children(
		statecharts.Atomic("active",
			statecharts.On("bot.start", statecharts.Then(refs.start.Call())),
			statecharts.On("match.snapshot", statecharts.If(refs.weaponEmpty.If()), statecharts.Then(refs.reload.Call())),
			statecharts.On("match.snapshot", statecharts.If(refs.opponentInSights.If(statecharts.GoLiteral(statecharts.Int64Value(8)))), statecharts.Then(refs.shoot.Call())),
			statecharts.On("match.snapshot", statecharts.If(refs.tickEvery.If(statecharts.GoLiteral(statecharts.Int64Value(8)))), statecharts.Then(refs.move.Call(statecharts.GoLiteral(right)))),
			statecharts.On("match.snapshot", statecharts.If(refs.powerAtLeast.If(statecharts.GoLiteral(statecharts.Int64Value(1)))), statecharts.Then(refs.moveToward.Call(statecharts.GoLiteral(opponent)))),
			statecharts.On("match.snapshot", statecharts.If(refs.targetExists.If(statecharts.GoLiteral(powerup))), statecharts.Then(refs.moveToward.Call(statecharts.GoLiteral(powerup)))),
			statecharts.On("match.snapshot", statecharts.If(refs.targetExists.If(statecharts.GoLiteral(opponent))), statecharts.Then(refs.moveToward.Call(statecharts.GoLiteral(opponent)))),
			statecharts.On("match.snapshot", statecharts.If(refs.targetExists.If(statecharts.GoLiteral(nearest))), statecharts.Then(refs.wander.Call())),
			statecharts.On("match.snapshot", statecharts.Then(refs.wander.Call())),
			statecharts.On("bot.stop", statecharts.Target("stopped"), statecharts.Then(refs.stop.Call())),
		), statecharts.Final("stopped"))))
}

const (
	botTargetNearest  = "nearest"
	botTargetOpponent = "opponent"
	botTargetPowerup  = "powerup"
)

func botStringArgument(args []statecharts.Value, index int, name string, allowed ...string) (string, error) {
	if index >= len(args) {
		return "", fmt.Errorf("bot %s argument is required", name)
	}
	value, ok := args[index].AsString()
	if !ok {
		return "", fmt.Errorf("bot %s must be a string", name)
	}
	for _, candidate := range allowed {
		if value == candidate {
			return value, nil
		}
	}
	return "", fmt.Errorf("bot %s must be one of %v", name, allowed)
}

func botIntArgument(args []statecharts.Value, index int, name string, minimum int) (int, error) {
	if index >= len(args) {
		return 0, fmt.Errorf("bot %s argument is required", name)
	}
	value, ok := args[index].AsInt64()
	if !ok || value < int64(minimum) || value > int64(^uint(0)>>1) {
		return 0, fmt.Errorf("bot %s must be an integer at least %d", name, minimum)
	}
	return int(value), nil
}

func botSnapshot(data *botModel, ec statecharts.ExecContext) (arenaSnapshot, creature, error) {
	event, ok := ec.Event()
	if !ok || event.Name != "match.snapshot" {
		return arenaSnapshot{}, creature{}, fmt.Errorf("bot decision capability requires match.snapshot")
	}
	var snapshot arenaSnapshot
	if err := decodeTaggedStruct(event.Data, snapshotTag, &snapshot); err != nil {
		return arenaSnapshot{}, creature{}, err
	}
	self := snapshotPlayer(snapshot, data.Player)
	if self.ID == "" {
		return arenaSnapshot{}, creature{}, fmt.Errorf("bot %q is absent from match snapshot", data.Player)
	}
	if self.LastSequence > data.NextSequence {
		data.NextSequence = self.LastSequence
	}
	return snapshot, self, nil
}

func emitBotAction(data *botModel, ec statecharts.ExecContext, action string) error {
	snapshot, _, err := botSnapshot(data, ec)
	if err != nil {
		return err
	}
	return consumeBotTick(data, ec, snapshot, action)
}

func consumeBotTick(data *botModel, ec statecharts.ExecContext, snapshot arenaSnapshot, action string) error {
	if snapshot.Tick <= data.LastTick {
		return nil
	}
	data.LastTick = snapshot.Tick
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
}

func nearestBotTarget(snapshot arenaSnapshot, self creature, kind string) (tile, bool, bool) {
	best := int(^uint(0) >> 1)
	var target tile
	targetIsCreature := false
	found := false
	if kind != botTargetPowerup {
		for _, other := range snapshot.Creatures {
			if other.ID == self.ID || !other.Connected {
				continue
			}
			distance := abs(other.X-self.X) + abs(other.Y-self.Y)
			if distance < best {
				best, target, targetIsCreature, found = distance, tile{X: other.X, Y: other.Y}, true, true
			}
		}
	}
	if kind != botTargetOpponent {
		for _, item := range snapshot.Powerups {
			distance := abs(item.X-self.X) + abs(item.Y-self.Y)
			if distance < best {
				best, target, targetIsCreature, found = distance, tile{X: item.X, Y: item.Y}, false, true
			}
		}
	}
	return target, targetIsCreature, found
}

type botDirection struct {
	action string
	dx     int
	dy     int
}

var botDirections = []botDirection{
	{action: actionUp, dy: -1},
	{action: actionLeft, dx: -1},
	{action: actionDown, dy: 1},
	{action: actionRight, dx: 1},
}

func alignedDirection(self creature, target tile) (string, bool) {
	if self.Y == target.Y && self.X != target.X {
		return horizontalDirection(target.X - self.X), true
	}
	if self.X == target.X && self.Y != target.Y {
		return verticalDirection(target.Y - self.Y), true
	}
	return "", false
}

func botLineClear(snapshot arenaSnapshot, self creature, target tile) bool {
	direction, aligned := alignedDirection(self, target)
	if !aligned {
		return false
	}
	dx, dy := directionVector(direction)
	for x, y := self.X+dx, self.Y+dy; x != target.X || y != target.Y; x, y = x+dx, y+dy {
		if snapshotHasWall(snapshot, x, y) {
			return false
		}
	}
	return true
}

func botRoute(snapshot arenaSnapshot, self creature, target tile, stopAdjacent bool) string {
	type routeNode struct {
		position tile
		first    string
	}
	start := tile{X: self.X, Y: self.Y}
	queue := []routeNode{{position: start}}
	visited := map[tile]bool{start: true}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		distance := abs(current.position.X-target.X) + abs(current.position.Y-target.Y)
		if (!stopAdjacent && distance == 0) || (stopAdjacent && distance == 1) {
			return current.first
		}
		for _, direction := range botDirections {
			next := tile{X: current.position.X + direction.dx, Y: current.position.Y + direction.dy}
			if visited[next] || botCellBlocked(snapshot, next, self.ID) {
				continue
			}
			visited[next] = true
			first := current.first
			if first == "" {
				first = direction.action
			}
			queue = append(queue, routeNode{position: next, first: first})
		}
	}
	return ""
}

func botFallbackAction(snapshot arenaSnapshot, self creature) string {
	start := int(snapshot.Tick % uint64(len(botDirections)))
	for offset := range len(botDirections) {
		direction := botDirections[(start+offset)%len(botDirections)]
		next := tile{X: self.X + direction.dx, Y: self.Y + direction.dy}
		if !botCellBlocked(snapshot, next, self.ID) {
			return direction.action
		}
	}
	return ""
}

func botCellBlocked(snapshot arenaSnapshot, position tile, self string) bool {
	if position.X <= 0 || position.Y <= 0 || position.X >= snapshot.Width-1 || position.Y >= snapshot.Height-1 {
		return true
	}
	if snapshotHasWall(snapshot, position.X, position.Y) {
		return true
	}
	for _, actor := range snapshot.Creatures {
		if actor.ID != self && actor.X == position.X && actor.Y == position.Y {
			return true
		}
	}
	return false
}

func snapshotHasWall(snapshot arenaSnapshot, x, y int) bool {
	for _, wall := range snapshot.Walls {
		if wall.X == x && wall.Y == y {
			return true
		}
	}
	return false
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

func spawnBotPlayer(ctx context.Context, system *actors.System, id, match statecharts.Identifier, player, color string, revision statecharts.RevisionID) error {
	if err := system.Spawn(ctx, id, botKind); err != nil {
		return err
	}
	config, err := taggedStruct(botConfigTag, botConfig{Match: string(match), Player: player, Name: "BOT " + player, Color: color, DefinitionRevision: string(revision)})
	if err != nil {
		return err
	}
	return system.Tell(ctx, id, statecharts.Event{Name: "bot.start", Type: statecharts.EventExternal, Data: config})
}

func spawnBot(ctx context.Context, system *actors.System, id, match statecharts.Identifier, color string) error {
	_, revision, ok := system.CurrentDefinition(botKind)
	if !ok {
		return fmt.Errorf("bot definition is not registered")
	}
	return spawnBotPlayer(ctx, system, id, match, string(id), color, revision)
}
