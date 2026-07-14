package main

import (
	"context"
	"sync"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

const connectionKind statecharts.Identifier = "connection"

type connectionModel struct {
	mu     sync.RWMutex
	status string
}

func (m *connectionModel) set(status string) { m.mu.Lock(); m.status = status; m.mu.Unlock() }
func (m *connectionModel) get() string       { m.mu.RLock(); defer m.mu.RUnlock(); return m.status }

type connectionActor struct {
	system *actors.System
	model  *connectionModel
}

func newConnectionActor(ctx context.Context, changed func(string)) (*connectionActor, error) {
	m := &connectionModel{status: "connecting"}
	set := func(status string) statecharts.ActionFunc {
		return statecharts.Action(func(d *connectionModel, _ statecharts.ExecContext) error {
			d.set(status)
			if changed != nil {
				changed(status)
			}
			return nil
		})
	}
	chart, err := statecharts.Build(
		statecharts.Compound(connectionKind, "connecting", statecharts.Children(
			statecharts.Atomic("connecting", statecharts.OnEntry(set("connecting")), statecharts.On("success", statecharts.Target("connected")), statecharts.On("failure", statecharts.Target("reconnecting"))),
			statecharts.Atomic("connected", statecharts.OnEntry(set("connected")), statecharts.On("failure", statecharts.Target("reconnecting"))),
			statecharts.Atomic("reconnecting", statecharts.OnEntry(set("reconnecting")), statecharts.On("success", statecharts.Target("connected"))),
		)), statecharts.WithNewDatamodel(func() any { return m }), statecharts.WithVersion("v1"))
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
	return &connectionActor{system: sys, model: m}, nil
}

func (a *connectionActor) outcome(ctx context.Context, ok bool) {
	name := statecharts.Identifier("failure")
	if ok {
		name = "success"
	}
	_ = a.system.Tell(ctx, "connection", statecharts.Event{Name: name, Type: statecharts.EventExternal})
}
func (a *connectionActor) status() string                 { return a.model.get() }
func (a *connectionActor) stop(ctx context.Context) error { return a.system.Stop(ctx) }
