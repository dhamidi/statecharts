package statecharts

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

type failingInspectionSession struct {
	*recordingSession
	panicValue any
	err        error
}

func (s *failingInspectionSession) Inspect() (Value, error) {
	if s.panicValue != nil {
		panic(s.panicValue)
	}
	return Value{}, s.err
}

func TestGoInspectionUsesTaggedCanonicalEnvelopeNotSnapshotCodec(t *testing.T) {
	type data struct {
		Count uint64 `json:"count"`
	}
	snapshotCalls := 0
	model := NewGoModel(func() *data { return &data{Count: 9007199254740993} },
		WithGoSnapshotCodec(GoSnapshotCodec[data]{
			Encode: func(*data) ([]byte, error) { snapshotCalls++; return nil, errors.New("snapshot only") },
			Decode: func([]byte) (*data, error) { return &data{}, nil },
		}),
	)
	chart, err := Build(Atomic("live"), model)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.NewInstance(WithSessionID("inspection-session"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Stop(context.Background()) })

	inspection, err := instance.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshotCalls != 0 {
		t.Fatalf("snapshot codec called %d times", snapshotCalls)
	}
	if inspection.SessionID != "inspection-session" || inspection.Revision != chart.Revision() || !inspection.Running {
		t.Fatalf("inspection identity/state = %+v", inspection)
	}
	tag, payload, ok := inspection.Datamodel.AsTagged()
	if !ok || tag != GoInspectionValueTag {
		t.Fatalf("datamodel tag = %q, %v", tag, ok)
	}
	fields, _ := payload.AsMap()
	dataValue, _ := fields["data"].AsMap()
	number, _ := dataValue["count"].AsNumber()
	if number != "9007199254740993" {
		t.Fatalf("count = %q", number)
	}
	if _, ok := fields["variables"].AsMap(); !ok {
		t.Fatal("variables is not a map")
	}
}

func TestGoInspectionCodecPanicIsNonFatal(t *testing.T) {
	model := NewGoModel(func() *int { value := 1; return &value },
		WithGoInspectionCodec(GoInspectionCodec[int]{Encode: func(*int) (Value, error) {
			panic("inspect boom")
		}}),
	)
	chart, err := Build(Atomic("live"), model)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := instance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Stop(context.Background()) })
	if _, err := instance.Inspect(ctx); err == nil || !strings.Contains(err.Error(), "inspection codec panicked") {
		t.Fatalf("Inspect error = %v", err)
	}
	if instance.Err() != nil || len(instance.Configuration()) == 0 {
		t.Fatalf("inspection panic stopped instance: configuration=%v err=%v", instance.Configuration(), instance.Err())
	}
}

func TestInstanceRecoversThirdPartyInspectionPanicAndKeepsFailureCallerLocal(t *testing.T) {
	for _, tc := range []struct {
		name       string
		panicValue any
		err        error
		want       string
	}{
		{name: "panic", panicValue: "third party boom", want: "datamodel inspection panicked: third party boom"},
		{name: "error", err: errors.New("cannot export"), want: "inspect datamodel: cannot export"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			program := &recordingProgram{}
			model := recordingDatamodel{program: program}
			chart, err := Build(Atomic("live", On("still-alive")), model)
			if err != nil {
				t.Fatal(err)
			}
			base := &recordingSession{values: make(map[string]Value)}
			instance, err := newInstanceForSession(chart, &failingInspectionSession{recordingSession: base, panicValue: tc.panicValue, err: tc.err})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := instance.Start(ctx); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = instance.Stop(context.Background()) })
			if _, err := instance.Inspect(ctx); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Inspect error = %v, want %q", err, tc.want)
			}
			if err := instance.Send(ctx, Event{Name: "still-alive"}); err != nil {
				t.Fatalf("Send after inspection failure: %v", err)
			}
			if instance.Err() != nil {
				t.Fatalf("terminal error = %v", instance.Err())
			}
			snapshot, err := instance.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range append(snapshot.InternalQueue, snapshot.ExternalQueue...) {
				if event.Type == EventPlatform {
					t.Fatalf("inspection emitted platform event: %+v", event)
				}
			}
		})
	}
}

func TestGoInspectionCustomCodecAndDeclaredVariablesAreIsolated(t *testing.T) {
	type data struct{ Secret string }
	codecCalls := 0
	model := NewGoModel(func() *data { return &data{Secret: "host"} }, WithGoInspectionCodec(GoInspectionCodec[data]{
		Encode: func(d *data) (Value, error) { codecCalls++; return StringValue("custom:" + d.Secret) },
	}))
	literal, err := StringValue("original")
	if err != nil {
		t.Fatal(err)
	}
	expression := GoLiteral(literal)
	chart, err := Build(Atomic("live"), model, WithData(DataDefinition{ID: "declared", Expr: &expression}))
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.NewInstance()
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Stop(context.Background()) })
	first, err := instance.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, payload, _ := first.Datamodel.AsTagged()
	fields, _ := payload.AsMap()
	if got, _ := fields["data"].AsString(); got != "custom:host" || codecCalls != 1 {
		t.Fatalf("custom data/calls = %q/%d", got, codecCalls)
	}
	variables, _ := fields["variables"].AsMap()
	if got, _ := variables["declared"].AsString(); got != "original" {
		t.Fatalf("declared = %q", got)
	}
	variables["declared"], _ = StringValue("mutated")
	second, err := instance.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, payload, _ = second.Datamodel.AsTagged()
	fields, _ = payload.AsMap()
	variables, _ = fields["variables"].AsMap()
	if got, _ := variables["declared"].AsString(); got != "original" {
		t.Fatalf("live declared mutated to %q", got)
	}
}

func TestInstanceInspectionObservesCompletedMacrostepAndOwnsPendingData(t *testing.T) {
	type data struct {
		Count int `json:"count"`
	}
	model := NewGoModel(func() *data { return &data{} })
	increment, err := model.Action("increment", "v1", func(data *data, _ ExecContext, _ []Value) error {
		data.Count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	label, err := StringValue("original")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := MapValue(map[string]Value{"label": label})
	if err != nil {
		t.Fatal(err)
	}
	chart, err := Build(
		Compound("root", "idle", Children(
			Atomic("idle", On("advance", Target("waiting"), Then(
				increment.Do(),
				Send("wake", SendID("wake"), SendDelay(time.Hour), SendContent(GoLiteral(payload))),
			))),
			Atomic("waiting"),
		)),
		model,
	)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.NewInstance(WithClock(NewManualClock(time.Unix(100, 0))))
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Stop(context.Background()) })
	if err := instance.Send(t.Context(), Event{Name: "advance"}); err != nil {
		t.Fatal(err)
	}

	first, err := instance.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := first.Configuration, []Identifier{"waiting"}; !slices.Equal(got, want) {
		t.Fatalf("configuration = %v, want %v", got, want)
	}
	_, modelPayload, ok := first.Datamodel.AsTagged()
	if !ok {
		t.Fatal("Go datamodel is not tagged")
	}
	modelFields, _ := modelPayload.AsMap()
	dataFields, _ := modelFields["data"].AsMap()
	if count, _ := dataFields["count"].AsNumber(); count != "1" {
		t.Fatalf("count = %q, want 1", count)
	}
	if len(first.PendingSends) != 1 || first.PendingSends[0].SendID != "wake" {
		t.Fatalf("pending sends = %+v", first.PendingSends)
	}
	pendingData, _ := first.PendingSends[0].Event.Data.AsMap()
	pendingData["label"], _ = StringValue("changed")
	first.Configuration[0] = "idle"
	first.HistoryValue["invented"] = []Identifier{"waiting"}

	second, err := instance.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := second.Configuration, []Identifier{"waiting"}; !slices.Equal(got, want) {
		t.Fatalf("live configuration changed to %v, want %v", got, want)
	}
	if _, ok := second.HistoryValue["invented"]; ok {
		t.Fatal("mutating inspection HistoryValue changed the live instance")
	}
	pendingData, _ = second.PendingSends[0].Event.Data.AsMap()
	if label, _ := pendingData["label"].AsString(); label != "original" {
		t.Fatalf("live pending data changed to %q", label)
	}
}

func TestGoCompileReportsNilInspectionCodec(t *testing.T) {
	model := NewGoModel(func() *int { return new(int) }, WithGoInspectionCodec(GoInspectionCodec[int]{}))
	_, err := Build(Atomic("live"), model)
	if err == nil || !strings.Contains(err.Error(), "inspection codec") {
		t.Fatalf("Build error = %v", err)
	}
}
