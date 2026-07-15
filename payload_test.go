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
	var data *struct{}
	model := NewGoModel(func() *struct{} {
		data = &struct{}{}
		return data
	})
	mutateAfterSend, err := model.Action("payload.mutate-source-after-send", "v1", func(_ *struct{}, _ ExecContext, _ []Value) error {
		source["count"] = Int64Value(9)
		return nil
	})
	if err != nil {
		t.Fatalf("register send action: %v", err)
	}
	var got Value
	record, err := model.Action("payload.record-received", "v1", func(_ *struct{}, ec ExecContext, _ []Value) error {
		ev, _ := ec.Event()
		got = ev.Data
		return nil
	})
	if err != nil {
		t.Fatalf("register record action: %v", err)
	}
	chart, err := Build(Atomic("ready",
		On("go", Then(Send("sent", SendContent(GoLiteral(payload))), mutateAfterSend.Do())),
		On("sent", Then(record.Do())),
	), model)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	in, err := chart.NewInstance()
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	if data == nil {
		t.Fatal("NewInstance did not create datamodel")
	}
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
