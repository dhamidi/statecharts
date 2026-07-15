package statecharts

import (
	"context"
	"testing"
)

func TestSendClonesCanonicalValueAtEvaluation(t *testing.T) {
	// Build a Value through package-private fields so the test can mutate the
	// source after <send> evaluation. Public constructors already isolate
	// their inputs; this specifically verifies the interpreter boundary.
	source := map[string]Value{"count": Int64Value(1)}
	payload := Value{kind: ValueKindMap, object: source}
	mutateAfterSend := Action(func(_ *struct{}, ec ExecContext) error {
		ec.Send("sent", SendOptions{Data: payload})
		source["count"] = Int64Value(9)
		return nil
	})
	var got Value
	record := Action(func(_ *struct{}, ec ExecContext) error {
		ev, _ := ec.Event()
		got = ev.Data
		return nil
	})
	chart, err := Build(Atomic("ready", On("go", Then(mutateAfterSend)), On("sent", Then(record))))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	in := New(chart, &struct{}{})
	if err := in.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer in.Stop(context.Background())
	if err := in.Send(context.Background(), Event{Name: "go"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	object, ok := got.AsMap()
	if !ok || !object["count"].Equal(Int64Value(1)) {
		t.Fatalf("sent payload = %#v, want isolated count=1", got)
	}
}
