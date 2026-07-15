package main

import (
	"context"
	"sync"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

const connectionKind statecharts.Identifier = "connection"

type connectionModel struct {
	Status string
}

type connectionStatus struct {
	mu    sync.RWMutex
	value string
}

func (m *connectionStatus) set(status string) { m.mu.Lock(); m.value = status; m.mu.Unlock() }
func (m *connectionStatus) get() string       { m.mu.RLock(); defer m.mu.RUnlock(); return m.value }

type connectionActor struct {
	system      *actors.System
	statusValue *connectionStatus
}

func newConnectionActor(ctx context.Context, changed func(string)) (*connectionActor, error) {
	statusValue := &connectionStatus{value: "connecting"}
	model := statecharts.NewGoModel(func() *connectionModel { return &connectionModel{Status: "connecting"} })
	set := func(name, status string) (statecharts.GoActionRef, error) {
		return model.Action(statecharts.Identifier("counters.connection.enter-"+name), "v1", func(d *connectionModel, _ statecharts.ExecContext, _ []statecharts.Value) error {
			d.Status = status
			statusValue.set(status)
			if changed != nil {
				changed(status)
			}
			return nil
		})
	}
	connecting, err := set("connecting", "connecting")
	if err != nil {
		return nil, err
	}
	connected, err := set("connected", "connected")
	if err != nil {
		return nil, err
	}
	reconnecting, err := set("reconnecting", "reconnecting")
	if err != nil {
		return nil, err
	}
	chart, err := statecharts.Build(
		statecharts.Compound(connectionKind, "connecting", statecharts.Children(
			statecharts.Atomic("connecting", statecharts.OnEntry(connecting.Do()), statecharts.On("success", statecharts.Target("connected")), statecharts.On("failure", statecharts.Target("reconnecting"))),
			statecharts.Atomic("connected", statecharts.OnEntry(connected.Do()), statecharts.On("failure", statecharts.Target("reconnecting"))),
			statecharts.Atomic("reconnecting", statecharts.OnEntry(reconnecting.Do()), statecharts.On("success", statecharts.Target("connected"))),
		)), model, statecharts.WithRevisionSalt("connection-v1"))
	if err != nil {
		return nil, err
	}
	chart, err = canonicalRoundTrip(chart, model)
	if err != nil {
		return nil, err
	}
	sys := actors.NewSystem()
	if err := sys.Register(chart); err != nil {
		_ = sys.Stop(context.Background())
		return nil, err
	}
	if err := sys.Spawn(ctx, "connection", connectionKind); err != nil {
		_ = sys.Stop(context.Background())
		return nil, err
	}
	return &connectionActor{system: sys, statusValue: statusValue}, nil
}

func (a *connectionActor) outcome(ctx context.Context, ok bool) {
	name := statecharts.Identifier("failure")
	if ok {
		name = "success"
	}
	_ = a.system.Tell(ctx, "connection", statecharts.Event{Name: name, Type: statecharts.EventExternal})
}
func (a *connectionActor) status() string                 { return a.statusValue.get() }
func (a *connectionActor) stop(ctx context.Context) error { return a.system.Stop(ctx) }
