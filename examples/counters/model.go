package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
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
	Name       string `json:"name"`
	Color      string `json:"color"`
	Value      int    `json:"value"`
	Resident   bool   `json:"resident"`
	ActorState string `json:"actor_state"`
}

const (
	actorStateResident  = string(actors.ResidencyResident)
	actorStatePagedOut  = string(actors.ResidencyPagedOut)
	actorStateHydrating = string(actors.ResidencyHydrating)
)

func buildCounterChart() (*statecharts.Chart, error) {
	publish := statecharts.Action(func(d *counterModel, ec statecharts.ExecContext) error {
		ec.Send("projection", statecharts.SendOptions{Target: "hub@ui", Data: projection{Name: ec.SessionID(), Color: ec.SessionID(), Value: d.Value}})
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

const hubKind statecharts.Identifier = "projection-hub"
const streamKind statecharts.Identifier = "sse-connection"
const streamIOProcessor statecharts.Identifier = "sse"

type hubModel struct {
	Values      map[string]projection
	Residencies map[string]string
	Subscribers map[statecharts.Identifier][]string
}
type hubSubscription struct {
	Target statecharts.Identifier
	Colors []string
}
type hubQuery struct {
	Colors []string
	Reply  chan []projection
}

func hubSnapshot(d *hubModel, selected []string) []projection {
	out := make([]projection, 0, len(selected))
	for _, name := range selected {
		if p, ok := d.Values[name]; ok {
			p.ActorState = d.Residencies[name]
			if p.ActorState == "" {
				p.ActorState = actorStatePagedOut
			}
			p.Resident = p.ActorState == actorStateResident
			out = append(out, p)
		}
	}
	return out
}

func buildHubChart() (*statecharts.Chart, error) {
	notify := func(d *hubModel, ec statecharts.ExecContext) {
		for target, selected := range d.Subscribers {
			ec.Send("snapshot", statecharts.SendOptions{Target: target, Data: hubSnapshot(d, selected)})
		}
	}
	return statecharts.Build(statecharts.Atomic(hubKind,
		statecharts.On("projection", statecharts.Then(statecharts.Action(func(d *hubModel, ec statecharts.ExecContext) error {
			ev, _ := ec.Event()
			p, ok := ev.Data.(projection)
			if !ok {
				return fmt.Errorf("projection payload %T", ev.Data)
			}
			d.Values[p.Name] = p
			notify(d, ec)
			return nil
		}))),
		statecharts.On("residency", statecharts.Then(statecharts.Action(func(d *hubModel, ec statecharts.ExecContext) error {
			ev, _ := ec.Event()
			change, ok := ev.Data.(actors.ResidencyChange)
			if !ok {
				return fmt.Errorf("residency payload %T", ev.Data)
			}
			d.Residencies[string(change.ActorID)] = string(change.State)
			notify(d, ec)
			return nil
		}))),
		statecharts.On("subscribe", statecharts.Then(statecharts.Action(func(d *hubModel, ec statecharts.ExecContext) error {
			ev, _ := ec.Event()
			sub := ev.Data.(hubSubscription)
			d.Subscribers[sub.Target] = append([]string(nil), sub.Colors...)
			ec.Send("snapshot", statecharts.SendOptions{Target: sub.Target, Data: hubSnapshot(d, sub.Colors)})
			return nil
		}))),
		statecharts.On("unsubscribe", statecharts.Then(statecharts.Action(func(d *hubModel, ec statecharts.ExecContext) error {
			ev, _ := ec.Event()
			delete(d.Subscribers, ev.Data.(statecharts.Identifier))
			return nil
		}))),
		statecharts.On("query", statecharts.Then(statecharts.Action(func(d *hubModel, ec statecharts.ExecContext) error {
			ev, _ := ec.Event()
			q := ev.Data.(hubQuery)
			q.Reply <- hubSnapshot(d, q.Colors)
			return nil
		}))),
	), statecharts.WithNewDatamodel(func() any {
		return &hubModel{Values: map[string]projection{}, Residencies: map[string]string{}, Subscribers: map[statecharts.Identifier][]string{}}
	}))
}

type streamStart struct {
	Mode   string
	Colors []string
	Output statecharts.Identifier
}
type streamModel struct {
	Mode   string
	Colors []string
	Output statecharts.Identifier
	Last   []byte
}

func buildStreamChart() (*statecharts.Chart, error) {
	start := statecharts.Action(func(d *streamModel, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		s := ev.Data.(streamStart)
		d.Mode, d.Colors, d.Output = s.Mode, s.Colors, s.Output
		ec.Send("subscribe", statecharts.SendOptions{Target: "hub", Data: hubSubscription{Target: statecharts.Identifier(ec.SessionID()), Colors: s.Colors}})
		ec.Send("keepalive", statecharts.SendOptions{Delay: 15 * time.Second})
		return nil
	})
	emit := statecharts.Action(func(d *streamModel, ec statecharts.ExecContext) error {
		ev, _ := ec.Event()
		ps := ev.Data.([]projection)
		var frame []byte
		if d.Mode == "browser" {
			frame = []byte(datastarPatch(renderString(renderDashboard("online", ps))))
		} else {
			b, _ := json.Marshal(ps)
			frame = []byte("event: snapshot\ndata: " + string(b) + "\n\n")
		}
		if bytes.Equal(frame, d.Last) {
			return nil
		}
		d.Last = append(d.Last[:0], frame...)
		ec.Send("frame", statecharts.SendOptions{Target: d.Output, Type: streamIOProcessor, Data: frame})
		return nil
	})
	keepalive := statecharts.Action(func(d *streamModel, ec statecharts.ExecContext) error {
		ec.Send("frame", statecharts.SendOptions{Target: d.Output, Type: streamIOProcessor, Data: ": keepalive\n\n"})
		ec.Send("keepalive", statecharts.SendOptions{Delay: 15 * time.Second})
		return nil
	})
	closeStream := statecharts.Action(func(d *streamModel, ec statecharts.ExecContext) error {
		ec.Send("unsubscribe", statecharts.SendOptions{Target: "hub", Data: statecharts.Identifier(ec.SessionID())})
		return nil
	})
	return statecharts.Build(statecharts.Compound(streamKind, "open", statecharts.Children(
		statecharts.Atomic("open", statecharts.On("start", statecharts.Then(start)), statecharts.On("snapshot", statecharts.Then(emit)), statecharts.On("keepalive", statecharts.Then(keepalive)), statecharts.On("close", statecharts.Target("closed"), statecharts.Then(closeStream))),
		statecharts.Final("closed"),
	),
	), statecharts.WithNewDatamodel(func() any { return &streamModel{} }))
}
