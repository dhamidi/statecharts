package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/dhamidi/statecharts"
)

var colors = []string{"red", "orange", "yellow", "green", "blue", "indigo", "violet"}

var colorValues = map[string]struct{ background, foreground string }{
	"red": {"#dc2626", "#fff"}, "orange": {"#ea580c", "#fff"},
	"yellow": {"#facc15", "#18181b"}, "green": {"#16a34a", "#fff"},
	"blue": {"#2563eb", "#fff"}, "indigo": {"#4f46e5", "#fff"},
	"violet": {"#7c3aed", "#fff"},
}

func selectColors(requested []string, defaultN int) ([]string, error) {
	if len(requested) == 0 {
		if defaultN < 1 || defaultN > len(colors) {
			return nil, fmt.Errorf("number of counters must be 1..%d", len(colors))
		}
		return append([]string(nil), colors[:defaultN]...), nil
	}
	selected := make([]string, 0, len(requested))
	seen := make(map[string]bool, len(requested))
	for _, name := range requested {
		if _, ok := colorValues[name]; !ok {
			return nil, fmt.Errorf("unknown color %q", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("color %q selected more than once", name)
		}
		seen[name] = true
		selected = append(selected, name)
	}
	return selected, nil
}

const counterKind statecharts.Identifier = "counter"

type counterModel struct {
	Value     int
	Processed map[statecharts.Identifier]bool
}

const incrementDataType = "counters.increment.v1"

type incrementData struct {
	WriteID statecharts.Identifier `json:"write_id"`
}
type incrementPayload = statecharts.JSONData[incrementData]

func registerCounterDataTypes() {
	statecharts.RegisterDataType(incrementDataType, func() statecharts.DataUnmarshaler { return &incrementPayload{} })
}

func incrementEvent(writeID statecharts.Identifier) statecharts.Event {
	return statecharts.Event{Name: "increment", Type: statecharts.EventExternal, Data: statecharts.NewJSONData(incrementDataType, incrementData{WriteID: writeID})}
}

type projection struct {
	Name     string `json:"name"`
	Color    string `json:"color"`
	Value    int    `json:"value"`
	Resident bool   `json:"resident"`
}

func buildCounterChart() (*statecharts.Chart, error) {
	publish := statecharts.Action(func(d *counterModel, ec statecharts.ExecContext) error {
		ec.Send("projection", statecharts.SendOptions{Target: "hub", Data: projection{Name: ec.SessionID(), Color: ec.SessionID(), Value: d.Value}})
		return nil
	})
	increment := statecharts.Action(func(d *counterModel, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		var writeID statecharts.Identifier
		switch payload := ev.Data.(type) {
		case incrementPayload:
			writeID = payload.Value.WriteID
		case *incrementPayload:
			writeID = payload.Value.WriteID
		default:
			return fmt.Errorf("counters: increment payload has type %T", ev.Data)
		}
		if d.Processed == nil {
			d.Processed = make(map[statecharts.Identifier]bool)
		}
		if !d.Processed[writeID] {
			d.Processed[writeID] = true
			d.Value++
		}
		return nil
	})
	return statecharts.Build(statecharts.Atomic(counterKind,
		statecharts.On("increment", statecharts.Then(increment, publish)),
		statecharts.On("publish", statecharts.Then(publish))),
		statecharts.WithNewDatamodel(func() any { return &counterModel{Processed: make(map[statecharts.Identifier]bool)} }))
}

type projectionHub struct {
	mu     sync.RWMutex
	values map[string]projection
	subs   map[chan struct{}]struct{}
}

func newProjectionHub() *projectionHub {
	return &projectionHub{values: make(map[string]projection), subs: make(map[chan struct{}]struct{})}
}
func (h *projectionHub) Attach(statecharts.Dispatcher)                        {}
func (h *projectionHub) Cancel(context.Context, statecharts.Identifier) error { return nil }
func (h *projectionHub) Send(ctx context.Context, req statecharts.SendRequest) error {
	if req.Target != "hub" {
		return fmt.Errorf("counters: unsupported target %q", req.Target)
	}
	p, ok := req.Data.(projection)
	if !ok {
		return fmt.Errorf("counters: invalid hub payload %T", req.Data)
	}
	if err := ctx.Err(); err != nil {
		return ctx.Err()
	}
	h.mu.Lock()
	h.values[p.Name] = p
	for ch := range h.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	h.mu.Unlock()
	return nil
}
func (h *projectionHub) snapshot(n int) []projection {
	return h.snapshotColors(colors[:n])
}
func (h *projectionHub) snapshotColors(selected []string) []projection {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]projection, 0, len(selected))
	for _, name := range selected {
		if p, ok := h.values[name]; ok {
			out = append(out, p)
		}
	}
	return out
}
func (h *projectionHub) subscribe() (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() { h.mu.Lock(); delete(h.subs, ch); h.mu.Unlock() }
}
func (h *projectionHub) changed() {
	h.mu.Lock()
	for ch := range h.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	h.mu.Unlock()
}
