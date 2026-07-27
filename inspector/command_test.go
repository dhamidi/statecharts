package inspector

import (
	"context"
	"errors"
	"reflect"
	"testing"

	statecharts "github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"
)

func commandCaptureSystem(t *testing.T, received chan<- statecharts.Event) *actors.System {
	t.Helper()
	builder := statecharts.New("command-capture", func() *struct{} { return &struct{}{} })
	record := builder.Action("record", func(_ *struct{}, ec statecharts.ExecContext, _ []statecharts.Value) error {
		event, ok := ec.Event()
		if !ok {
			t.Error("command action had no current event")
			return nil
		}
		received <- event
		return nil
	})
	chart, err := builder.Build(statecharts.Atomic("active", statecharts.On("message", statecharts.Then(record.Do()))))
	if err != nil {
		t.Fatal(err)
	}
	system := actors.NewSystem()
	t.Cleanup(func() { _ = system.Stop(context.Background()) })
	if err := system.Register(chart); err != nil {
		t.Fatal(err)
	}
	if err := system.Spawn(t.Context(), "target", chart.ID()); err != nil {
		t.Fatal(err)
	}
	return system
}

func TestSendEventDeliversOneExternalEventWithNoForgedMetadata(t *testing.T) {
	received := make(chan statecharts.Event, 2)
	system := commandCaptureSystem(t, received)
	var audit []AuditRecord
	service := New(WithAuthorizer(AllowAll()), WithAuditSink(func(_ context.Context, record AuditRecord) error {
		audit = append(audit, record)
		return nil
	}))
	t.Cleanup(service.Close)
	if err := service.RegisterSystem("commands", system); err != nil {
		t.Fatal(err)
	}
	source := map[string]statecharts.Value{"count": statecharts.Int64Value(7)}
	payload, err := statecharts.MapValue(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SendEvent(t.Context(), "commands", "target", "message", payload); err != nil {
		t.Fatal(err)
	}
	source["count"] = statecharts.Int64Value(99)
	event := <-received
	if event.Name != "message" || event.Type != statecharts.EventExternal || !event.Data.Equal(payload) {
		t.Fatalf("delivered event = %#v", event)
	}
	if event.SendID != "" || event.Origin != "" || event.OriginType != "" || event.InvokeID != "" || event.DeliveryID != "" {
		t.Fatalf("inspector forged event metadata: %#v", event)
	}
	select {
	case duplicate := <-received:
		t.Fatalf("duplicate event = %#v", duplicate)
	default:
	}
	if len(audit) != 1 || !audit[0].Authorized || audit[0].Outcome != "success" || audit[0].Error != "" {
		t.Fatalf("audit = %#v", audit)
	}
}

func TestDeniedSendDeliversNothing(t *testing.T) {
	received := make(chan statecharts.Event, 1)
	system := commandCaptureSystem(t, received)
	service := New(WithAuthorizer(AuthorizerFunc(func(context.Context, AuthorizationRequest) error {
		return errors.New("denied with secret detail")
	})))
	t.Cleanup(service.Close)
	if err := service.RegisterSystem("commands", system); err != nil {
		t.Fatal(err)
	}
	if err := service.SendEvent(t.Context(), "commands", "target", "message", statecharts.NullValue()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("SendEvent = %v", err)
	}
	select {
	case event := <-received:
		t.Fatalf("denied event delivered: %#v", event)
	default:
	}
}

func TestSendEventAuditDeniedAndUnknownActor(t *testing.T) {
	system := testSystem(t, "commands", "known")
	var records []AuditRecord
	allowed := false
	s := New(
		WithAuthorizer(AuthorizerFunc(func(_ context.Context, request AuthorizationRequest) error {
			if request.Operation != OperationSendEvent || request.System != "system" || request.ActorID == "" {
				t.Fatalf("authorization request = %#v", request)
			}
			if !allowed {
				return errors.New("denied detail")
			}
			return nil
		})),
		WithAuditSink(func(_ context.Context, record AuditRecord) error {
			records = append(records, record)
			return errors.New("ignored audit failure")
		}),
	)
	if err := s.RegisterSystem("system", system); err != nil {
		t.Fatal(err)
	}
	secret, _ := statecharts.StringValue("payload must not be audited")
	if err := s.SendEvent(context.Background(), "system", "known", "message", secret); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("denied send = %v", err)
	}
	allowed = true
	if err := s.SendEvent(context.Background(), "system", "missing", "message", secret); !errors.Is(err, actors.ErrUnknownActor) {
		t.Fatalf("unknown send = %v", err)
	}
	if len(records) != 2 || records[0].Authorized || records[0].Outcome != "denied" || !records[1].Authorized || records[1].Outcome != "failure" {
		t.Fatalf("audit records = %#v", records)
	}
	for _, record := range records {
		if record.System != "system" || record.EventName != "message" {
			t.Errorf("audit target = %#v", record)
		}
	}
	if _, ok := reflect.TypeOf(AuditRecord{}).FieldByName("Payload"); ok {
		t.Fatal("AuditRecord must not expose payload")
	}
}

func TestAuditPanicDoesNotChangeSuccessfulSend(t *testing.T) {
	system := testSystem(t, "audit-panic", "known")
	s := New(WithAuthorizer(AllowAll()), WithAuditSink(func(context.Context, AuditRecord) error { panic("ignored") }))
	if err := s.RegisterSystem("system", system); err != nil {
		t.Fatal(err)
	}
	if err := s.SendEvent(context.Background(), "system", "known", "message", statecharts.Value{}); err != nil {
		t.Fatalf("successful send changed by audit panic: %v", err)
	}
}
