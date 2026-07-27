package inspector

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

func TestRedactInspectionPinsEveryCategoryAndOwnsValues(t *testing.T) {
	var got []RedactionContext
	replacement, _ := statecharts.StringValue("REDACTED")
	s := New(WithRedactor(RedactorFuncs{
		Value: func(_ context.Context, rc RedactionContext, _ statecharts.Value) (statecharts.Value, error) {
			got = append(got, rc)
			return replacement, nil
		},
		Text: func(_ context.Context, rc RedactionContext, _ string) (string, error) {
			got = append(got, rc)
			return "REDACTED", nil
		},
	}))
	secret, _ := statecharts.StringValue("secret")
	in := statecharts.InstanceInspection{
		Datamodel: secret, InternalQueue: []statecharts.Event{{Data: secret}}, ExternalQueue: []statecharts.Event{{Data: secret}},
		PendingSends: []statecharts.PendingSend{{Event: statecharts.Event{Data: secret}}}, ActiveInvokes: []statecharts.ActiveInvoke{{Source: "secret-source"}},
	}
	out, err := s.redactInspection(context.Background(), "sys", "actor", in)
	if err != nil {
		t.Fatal(err)
	}
	want := []RedactionCategory{CategoryDatamodel, CategoryEvent, CategoryEvent, CategoryPendingSend, CategoryInvocation}
	if len(got) != len(want) {
		t.Fatalf("contexts = %#v", got)
	}
	for i := range want {
		if got[i].System != "sys" || got[i].ActorID != "actor" || got[i].Category != want[i] {
			t.Errorf("context[%d] = %#v", i, got[i])
		}
	}
	if text, _ := out.Datamodel.AsString(); text != "REDACTED" {
		t.Fatalf("datamodel = %#v", out.Datamodel)
	}
	if text, _ := in.Datamodel.AsString(); text != "secret" {
		t.Fatal("input was mutated")
	}
}

func TestRedactorErrorAndPanicAreNormalized(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func(context.Context, RedactionContext, statecharts.Value) (statecharts.Value, error)
	}{
		{"error", func(context.Context, RedactionContext, statecharts.Value) (statecharts.Value, error) {
			return statecharts.Value{}, errors.New("HOST SECRET")
		}},
		{"panic", func(context.Context, RedactionContext, statecharts.Value) (statecharts.Value, error) {
			panic("PANIC SECRET")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New(WithRedactor(RedactorFuncs{Value: tc.fn}))
			_, err := s.redactValue(context.Background(), "sys", "actor", CategoryEvent, statecharts.Value{})
			if !errors.Is(err, ErrRedaction) || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("error leaked implementation detail: %v", err)
			}
		})
	}
}

func TestInspectActorRedactionFailureDoesNotReturnFabricatedLiveState(t *testing.T) {
	system := testSystem(t, "inspection-redaction", "actor")
	service := New(
		WithAuthorizer(AllowAll()),
		WithRedactor(RedactorFuncs{Value: func(context.Context, RedactionContext, statecharts.Value) (statecharts.Value, error) {
			return statecharts.Value{}, errors.New("host redaction failure")
		}}),
	)
	t.Cleanup(service.Close)
	if err := service.RegisterSystem("system", system); err != nil {
		t.Fatal(err)
	}

	_, live, err := service.InspectActor(t.Context(), "system", "actor")
	if !errors.Is(err, ErrRedaction) || live != nil {
		t.Fatalf("InspectActor live = %#v, error = %v", live, err)
	}
}

func TestObservationRedactsOuterMicrostepAndDiagnostic(t *testing.T) {
	replacement, _ := statecharts.StringValue("safe")
	s := New(WithRedactor(RedactorFuncs{
		Value: func(context.Context, RedactionContext, statecharts.Value) (statecharts.Value, error) {
			return replacement, nil
		},
		Text: func(context.Context, RedactionContext, string) (string, error) { return "safe diagnostic", nil },
	}))
	secret, _ := statecharts.StringValue("secret")
	o := actors.Observation{Actor: &actors.ActorInfo{ID: "actor"}, Macrostep: &statecharts.MacrostepTrace{
		Trigger: &statecharts.Event{Data: secret}, TerminalError: "secret diagnostic",
		Microsteps: []statecharts.MicrostepTrace{{Trigger: &statecharts.Event{Data: secret}}},
	}}
	out, err := s.redactObservation(context.Background(), "sys", o)
	if err != nil || out.Macrostep.TerminalError != "safe diagnostic" {
		t.Fatalf("observation = %#v, %v", out, err)
	}
	if v, _ := out.Macrostep.Trigger.Data.AsString(); v != "safe" {
		t.Fatal("outer trigger not redacted")
	}
	if v, _ := out.Macrostep.Microsteps[0].Trigger.Data.AsString(); v != "safe" {
		t.Fatal("microstep trigger not redacted")
	}
}

func TestStreamRedactionFailureCreatesGapAndRingStoresOnlyRedactedData(t *testing.T) {
	chart, err := statecharts.Build(
		statecharts.Atomic("stream-redaction", statecharts.On("message", statecharts.Target("stream-redaction"))),
		statecharts.NewGoModel(func() *struct{} { return &struct{}{} }),
	)
	if err != nil {
		t.Fatal(err)
	}
	system := actors.NewSystem()
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	if err := system.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := system.Spawn(t.Context(), "actor", chart.ID()); err != nil {
		t.Fatal(err)
	}
	safe, err := statecharts.StringValue("safe")
	if err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	diagnostics := make(chan error, 1)
	service := New(
		WithAuthorizer(AllowAll()),
		WithRedactor(RedactorFuncs{Value: func(_ context.Context, rc RedactionContext, _ statecharts.Value) (statecharts.Value, error) {
			if rc.Category == CategoryEvent && fail.Load() {
				return statecharts.Value{}, errors.New("host failure containing SECRET")
			}
			return safe, nil
		}}),
		WithDiagnosticSink(func(_ context.Context, err error) { diagnostics <- err }),
	)
	t.Cleanup(service.Close)
	if err := service.RegisterSystem("redacted", system); err != nil {
		t.Fatal(err)
	}
	sub, err := service.Subscribe(t.Context(), "redacted", 8)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	secret, err := statecharts.StringValue("SECRET")
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if err := system.Tell(t.Context(), "actor", statecharts.Event{Name: "message", Type: statecharts.EventExternal, Data: secret}); err != nil {
		t.Fatal(err)
	}
	diagnostic := <-diagnostics
	if !errors.Is(diagnostic, ErrRedaction) || strings.Contains(diagnostic.Error(), "SECRET") {
		t.Fatalf("diagnostic leaked redaction details: %v", diagnostic)
	}
	select {
	case gap := <-sub.C:
		if gap.Kind != StreamGap || gap.Dropped != 1 {
			t.Fatalf("redaction gap = %#v", gap)
		}
	default:
		t.Fatal("redaction failure was not immediately visible to subscriber")
	}
	page, err := service.Recent(t.Context(), "redacted", 0, 10)
	if err != nil || len(page.Records) != 1 || page.Records[0].Kind != StreamGap || page.Records[0].Dropped != 1 {
		t.Fatalf("ring after redaction failure = %#v, %v", page, err)
	}
	fail.Store(false)
	if err := system.Tell(t.Context(), "actor", statecharts.Event{Name: "message", Type: statecharts.EventExternal, Data: secret}); err != nil {
		t.Fatal(err)
	}
	record := <-sub.C
	if record.Kind != StreamObservation || record.Observation == nil || record.Observation.Macrostep == nil {
		t.Fatalf("stream record = %#v", record)
	}
	trace := record.Observation.Macrostep
	if outer, _ := trace.Trigger.Data.AsString(); outer != "safe" {
		t.Fatalf("outer trigger = %#v", trace.Trigger.Data)
	}
	if len(trace.Microsteps) != 1 || trace.Microsteps[0].Trigger == nil {
		t.Fatalf("microsteps = %#v", trace.Microsteps)
	}
	if micro, _ := trace.Microsteps[0].Trigger.Data.AsString(); micro != "safe" {
		t.Fatalf("microstep trigger = %#v", trace.Microsteps[0].Trigger.Data)
	}
	page, err = service.Recent(t.Context(), "redacted", 0, 10)
	if err != nil || len(page.Records) != 2 {
		t.Fatalf("ring = %#v, %v", page, err)
	}
	for _, ringRecord := range page.Records {
		if strings.Contains(ringRecord.Reason, "SECRET") {
			t.Fatalf("ring leaked secret: %#v", ringRecord)
		}
		if ringRecord.Observation != nil && ringRecord.Observation.Macrostep != nil {
			if value, _ := ringRecord.Observation.Macrostep.Trigger.Data.AsString(); value == "SECRET" {
				t.Fatalf("ring retained unredacted trigger: %#v", ringRecord)
			}
		}
	}
}
