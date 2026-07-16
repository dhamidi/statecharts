package main

import (
	stdjson "encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
	statejson "github.com/dhamidi/statecharts/syntax/json"
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

const (
	incrementValueTag    = "counters.increment/v1"
	projectionValueTag   = "counters.projection/v1"
	projectionsValueTag  = "counters.projections/v1"
	residencyValueTag    = "counters.residency/v1"
	revisionValueTag     = "counters.revision/v1"
	subscriptionValueTag = "counters.subscription/v1"
	streamStartValueTag  = "counters.stream-start/v1"
	hubQueryValueTag     = "counters.hub-query/v1"
)

func incrementEvent(writeID statecharts.Identifier) statecharts.Event {
	return statecharts.Event{Name: "increment", Type: statecharts.EventExternal, Data: taggedMap(incrementValueTag, map[string]statecharts.Value{"write_id": stringValue(string(writeID))})}
}

type projection struct {
	Name       string `json:"name"`
	Color      string `json:"color"`
	Value      int    `json:"value"`
	Resident   bool   `json:"resident"`
	ActorState string `json:"actor_state"`
	Revision   string `json:"revision"`
}

func stringValue(s string) statecharts.Value {
	v, err := statecharts.StringValue(s)
	if err != nil {
		panic(err)
	}
	return v
}
func taggedMap(tag string, fields map[string]statecharts.Value) statecharts.Value {
	m, err := statecharts.MapValue(fields)
	if err != nil {
		panic(err)
	}
	v, err := statecharts.TaggedValue(tag, m)
	if err != nil {
		panic(err)
	}
	return v
}
func taggedFields(v statecharts.Value, tag string) (map[string]statecharts.Value, error) {
	got, payload, ok := v.AsTagged()
	if !ok || got != tag {
		return nil, fmt.Errorf("expected tagged value %q", tag)
	}
	fields, ok := payload.AsMap()
	if !ok {
		return nil, fmt.Errorf("%s payload is not a map", tag)
	}
	return fields, nil
}
func requiredString(fields map[string]statecharts.Value, name string) (string, error) {
	s, ok := fields[name].AsString()
	if !ok {
		return "", fmt.Errorf("field %q is not a string", name)
	}
	return s, nil
}

// canonicalRoundTrip exercises the same definition transport path used by an
// inspector or deployment store: JSON contains refs, while behavior remains
// in the appropriately scoped model registry.
func canonicalRoundTrip(chart *statecharts.Chart) (*statecharts.Chart, error) {
	wire, err := statejson.Marshal(chart.Definition())
	if err != nil {
		return nil, fmt.Errorf("marshal canonical definition: %w", err)
	}
	definition, err := statejson.Unmarshal(wire)
	if err != nil {
		return nil, fmt.Errorf("unmarshal canonical definition: %w", err)
	}
	recompiled, err := statecharts.Compile(definition, chart.Datamodel())
	if err != nil {
		return nil, fmt.Errorf("recompile canonical definition: %w", err)
	}
	return recompiled, nil
}
func encodeProjection(p projection) statecharts.Value {
	return taggedMap(projectionValueTag, map[string]statecharts.Value{"name": stringValue(p.Name), "color": stringValue(p.Color), "value": statecharts.Int64Value(int64(p.Value)), "resident": statecharts.BoolValue(p.Resident), "actor_state": stringValue(p.ActorState), "revision": stringValue(p.Revision)})
}
func decodeProjection(v statecharts.Value) (projection, error) {
	f, err := taggedFields(v, projectionValueTag)
	if err != nil {
		return projection{}, err
	}
	name, err := requiredString(f, "name")
	if err != nil {
		return projection{}, err
	}
	color, err := requiredString(f, "color")
	if err != nil {
		return projection{}, err
	}
	n, ok := f["value"].AsInt64()
	if !ok {
		return projection{}, fmt.Errorf("projection value is not an int64")
	}
	value := int(n)
	if int64(value) != n {
		return projection{}, fmt.Errorf("projection value %d does not fit int", n)
	}
	resident, ok := f["resident"].AsBool()
	if !ok {
		return projection{}, fmt.Errorf("projection resident is not a bool")
	}
	actorState, err := requiredString(f, "actor_state")
	if err != nil {
		return projection{}, err
	}
	revision, err := requiredString(f, "revision")
	if err != nil {
		return projection{}, err
	}
	return projection{Name: name, Color: color, Value: value, Resident: resident, ActorState: actorState, Revision: revision}, nil
}
func encodeProjections(ps []projection) statecharts.Value {
	values := make([]statecharts.Value, len(ps))
	for i := range ps {
		values[i] = encodeProjection(ps[i])
	}
	tagged, _ := statecharts.TaggedValue(projectionsValueTag, statecharts.ListValue(values))
	return tagged
}
func decodeProjections(v statecharts.Value) ([]projection, error) {
	tag, payload, ok := v.AsTagged()
	if !ok || tag != projectionsValueTag {
		return nil, fmt.Errorf("expected tagged value %q", projectionsValueTag)
	}
	values, ok := payload.AsList()
	if !ok {
		return nil, fmt.Errorf("projections payload is not a list")
	}
	out := make([]projection, len(values))
	for i := range values {
		p, err := decodeProjection(values[i])
		if err != nil {
			return nil, err
		}
		out[i] = p
	}
	return out, nil
}
func encodeStrings(values []string) statecharts.Value {
	out := make([]statecharts.Value, len(values))
	for i, value := range values {
		out[i] = stringValue(value)
	}
	return statecharts.ListValue(out)
}
func decodeStrings(v statecharts.Value) ([]string, error) {
	values, ok := v.AsList()
	if !ok {
		return nil, fmt.Errorf("expected string list")
	}
	out := make([]string, len(values))
	for i := range values {
		s, ok := values[i].AsString()
		if !ok {
			return nil, fmt.Errorf("list item %d is not a string", i)
		}
		out[i] = s
	}
	return out, nil
}

const (
	actorStateResident  = string(actors.ResidencyResident)
	actorStatePagedOut  = string(actors.ResidencyPagedOut)
	actorStateHydrating = string(actors.ResidencyHydrating)
)

func buildCounterChart() (*statecharts.Chart, error) {
	counter := statecharts.New(counterKind, func() *counterModel {
		return &counterModel{Processed: make(map[statecharts.Identifier]bool)}
	}, statecharts.Version("v1"))
	publish := counter.Action("publish-projection", func(d *counterModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		name := ec.SessionID()
		color, ok := counterColor(name)
		if !ok {
			return fmt.Errorf("counter actor %q has no color prefix", name)
		}
		ec.Send("projection", statecharts.SendOptions{Target: "hub@ui", Data: encodeProjection(projection{Name: name, Color: color, Value: d.Value})})
		return nil
	})
	registerIncrement := func(version string, delta int) statecharts.GoActionRef {
		return counter.ActionVersion("apply-idempotent-increment", version, func(d *counterModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
			ev, _ := ec.Event()
			fields, err := taggedFields(ev.Data, incrementValueTag)
			if err != nil {
				return err
			}
			text, err := requiredString(fields, "write_id")
			if err != nil {
				return err
			}
			writeID := statecharts.Identifier(text)
			if d.Processed == nil {
				d.Processed = make(map[statecharts.Identifier]bool)
			}
			if !d.Processed[writeID] {
				d.Processed[writeID] = true
				d.Value += delta
			}
			return nil
		})
	}
	increment := registerIncrement("v1", 1)
	registerIncrement("v2", 2)
	chart, err := counter.Build(statecharts.Atomic(counterKind,
		statecharts.On("increment", statecharts.Then(increment.Call(), publish.Do())),
		statecharts.On("publish", statecharts.Then(publish.Do()))))
	if err != nil {
		return nil, err
	}
	return canonicalRoundTrip(chart)
}

func counterColor(actorID string) (string, bool) {
	prefix, _, _ := strings.Cut(actorID, ".")
	_, ok := colorValues[prefix]
	return prefix, ok
}

const hubKind statecharts.Identifier = "projection-hub"
const streamKind statecharts.Identifier = "sse-connection"
const streamIOProcessor statecharts.Identifier = "sse"

type hubModel struct {
	Values      map[string]projection
	Residencies map[string]string
	Revisions   map[string]string
	Subscribers map[statecharts.Identifier][]string
}
type hubSubscription struct {
	Target statecharts.Identifier
	Colors []string
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
			p.Revision = d.Revisions[name]
			out = append(out, p)
		}
	}
	return out
}

func buildHubChart(requests *hubRequestRegistry) (*statecharts.Chart, error) {
	notify := func(d *hubModel, ec statecharts.ExecContext) {
		for target, selected := range d.Subscribers {
			ec.Send("snapshot", statecharts.SendOptions{Target: target, Data: encodeProjections(hubSnapshot(d, selected))})
		}
	}
	hub := statecharts.New(hubKind, func() *hubModel {
		return &hubModel{Values: map[string]projection{}, Residencies: map[string]string{}, Revisions: map[string]string{}, Subscribers: map[statecharts.Identifier][]string{}}
	}, statecharts.Version("v1"))
	projectionAction := hub.Action("accept-projection", func(d *hubModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		p, err := decodeProjection(ev.Data)
		if err != nil {
			return err
		}
		d.Values[p.Name] = p
		notify(d, ec)
		return nil
	})
	revisionAction := hub.Action("accept-revision", func(d *hubModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		fields, err := taggedFields(ev.Data, revisionValueTag)
		if err != nil {
			return err
		}
		actorID, err := requiredString(fields, "actor_id")
		if err != nil {
			return err
		}
		revision, err := requiredString(fields, "revision")
		if err != nil {
			return err
		}
		d.Revisions[actorID] = revision
		notify(d, ec)
		return nil
	})
	residencyAction := hub.Action("accept-residency", func(d *hubModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		fields, err := taggedFields(ev.Data, residencyValueTag)
		if err != nil {
			return err
		}
		actorID, err := requiredString(fields, "actor_id")
		if err != nil {
			return err
		}
		state, err := requiredString(fields, "state")
		if err != nil {
			return err
		}
		d.Residencies[actorID] = state
		notify(d, ec)
		return nil
	})
	subscribeAction := hub.Action("subscribe-stream", func(d *hubModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		fields, err := taggedFields(ev.Data, subscriptionValueTag)
		if err != nil {
			return err
		}
		target, err := requiredString(fields, "target")
		if err != nil {
			return err
		}
		selected, err := decodeStrings(fields["colors"])
		if err != nil {
			return err
		}
		sub := hubSubscription{Target: statecharts.Identifier(target), Colors: selected}
		d.Subscribers[sub.Target] = append([]string(nil), sub.Colors...)
		ec.Send("snapshot", statecharts.SendOptions{Target: sub.Target, Data: encodeProjections(hubSnapshot(d, sub.Colors))})
		return nil
	})
	unsubscribeAction := hub.Action("unsubscribe-stream", func(d *hubModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		target, ok := ev.Data.AsString()
		if !ok {
			return fmt.Errorf("unsubscribe target is not a string")
		}
		delete(d.Subscribers, statecharts.Identifier(target))
		return nil
	})
	queryAction := hub.Action("answer-runtime-query", func(d *hubModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		fields, err := taggedFields(ev.Data, hubQueryValueTag)
		if err != nil {
			return err
		}
		id, err := requiredString(fields, "request_id")
		if err != nil {
			return err
		}
		q, ok := requests.take(id)
		if !ok {
			return nil
		}
		q.reply <- hubSnapshot(d, q.colors)
		return nil
	})
	chart, err := hub.Build(statecharts.Atomic(hubKind,
		statecharts.On("projection", statecharts.Then(projectionAction.Do())),
		statecharts.On("residency", statecharts.Then(residencyAction.Do())),
		statecharts.On("revision", statecharts.Then(revisionAction.Do())),
		statecharts.On("subscribe", statecharts.Then(subscribeAction.Do())),
		statecharts.On("unsubscribe", statecharts.Then(unsubscribeAction.Do())),
		statecharts.On("query", statecharts.Then(queryAction.Do())),
	))
	if err != nil {
		return nil, err
	}
	return canonicalRoundTrip(chart)
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
	Last   string
}

func buildStreamChart() (*statecharts.Chart, error) {
	stream := statecharts.New(streamKind, func() *streamModel { return &streamModel{} }, statecharts.Version("v1"))
	start := stream.Action("start-subscription", func(d *streamModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		fields, err := taggedFields(ev.Data, streamStartValueTag)
		if err != nil {
			return err
		}
		mode, err := requiredString(fields, "mode")
		if err != nil {
			return err
		}
		output, err := requiredString(fields, "output")
		if err != nil {
			return err
		}
		selected, err := decodeStrings(fields["colors"])
		if err != nil {
			return err
		}
		s := streamStart{Mode: mode, Colors: selected, Output: statecharts.Identifier(output)}
		d.Mode, d.Colors, d.Output = s.Mode, s.Colors, s.Output
		ec.Send("subscribe", statecharts.SendOptions{Target: "hub", Data: taggedMap(subscriptionValueTag, map[string]statecharts.Value{"target": stringValue(ec.SessionID()), "colors": encodeStrings(s.Colors)})})
		ec.Send("keepalive", statecharts.SendOptions{Delay: 15 * time.Second})
		return nil
	})
	emit := stream.Action("emit-snapshot", func(d *streamModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ev, _ := ec.Event()
		ps, err := decodeProjections(ev.Data)
		if err != nil {
			return err
		}
		var frame string
		if d.Mode == "browser" {
			frame = datastarPatch(renderString(renderDashboard("online", ps)))
		} else {
			b, _ := stdjson.Marshal(ps)
			frame = "event: snapshot\ndata: " + string(b) + "\n\n"
		}
		if frame == d.Last {
			return nil
		}
		d.Last = frame
		ec.Send("frame", statecharts.SendOptions{Target: d.Output, Type: streamIOProcessor, Data: stringValue(frame)})
		return nil
	})
	keepalive := stream.Action("emit-keepalive", func(d *streamModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ec.Send("frame", statecharts.SendOptions{Target: d.Output, Type: streamIOProcessor, Data: stringValue(": keepalive\n\n")})
		ec.Send("keepalive", statecharts.SendOptions{Delay: 15 * time.Second})
		return nil
	})
	closeStream := stream.Action("close-subscription", func(d *streamModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
		ec.Send("unsubscribe", statecharts.SendOptions{Target: "hub", Data: stringValue(ec.SessionID())})
		return nil
	})
	chart, err := stream.Build(statecharts.Compound(streamKind, "open", statecharts.Children(
		statecharts.Atomic("open", statecharts.On("start", statecharts.Then(start.Do())), statecharts.On("snapshot", statecharts.Then(emit.Do())), statecharts.On("keepalive", statecharts.Then(keepalive.Do())), statecharts.On("close", statecharts.Target("closed"), statecharts.Then(closeStream.Do()))),
		statecharts.Final("closed"),
	),
	))
	if err != nil {
		return nil, err
	}
	return canonicalRoundTrip(chart)
}
