package statecharts

import (
	"errors"
	"testing"
)

type doorOpened struct{ Count int }

type codedError struct{ Message string }

func (e codedError) Error() string { return e.Message }

func (e codedError) MarshalData() (string, []byte, error) {
	return "test.coded_error", []byte("coded:" + e.Message), nil
}

func (e *codedError) UnmarshalData(payload []byte) error {
	e.Message = string(payload[len("coded:"):])
	return nil
}

func TestEventPayload(t *testing.T) {
	ev := Event{Name: "door.opened", Type: EventExternal, Data: doorOpened{Count: 3}}

	got, ok := Payload[doorOpened](ev)
	if !ok {
		t.Fatalf("Payload[doorOpened] ok = false, want true")
	}
	if got.Count != 3 {
		t.Fatalf("Payload count = %d, want 3", got.Count)
	}

	if _, ok := Payload[string](ev); ok {
		t.Fatalf("Payload[string] ok = true, want false for mismatched type")
	}
}

func TestEventTypeString(t *testing.T) {
	cases := map[EventType]string{
		EventExternal: "external",
		EventInternal: "internal",
		EventPlatform: "platform",
	}
	for typ, want := range cases {
		if got := typ.String(); got != want {
			t.Errorf("EventType(%d).String() = %q, want %q", typ, got, want)
		}
	}
}

func TestEventCodecRoundTripsPlatformErrorData(t *testing.T) {
	want := errors.New("target became unavailable")
	encoded, err := EncodeEvent(Event{
		Name: ErrEventCommunication, Type: EventPlatform, SendID: "request-7", Data: want,
	})
	if err != nil {
		t.Fatalf("EncodeEvent: %v", err)
	}
	got, err := DecodeEvent(encoded)
	if err != nil {
		t.Fatalf("DecodeEvent: %v", err)
	}
	gotErr, ok := got.Data.(error)
	if !ok {
		t.Fatalf("decoded Data type = %T, want error", got.Data)
	}
	if gotErr.Error() != want.Error() {
		t.Fatalf("decoded error = %q, want %q", gotErr, want)
	}
}

func TestEventCodecPrefersApplicationCodecForErrorType(t *testing.T) {
	RegisterDataType("test.coded_error", func() DataUnmarshaler { return &codedError{} })
	want := codedError{Message: "target became unavailable"}
	encoded, err := EncodeEvent(Event{Data: want})
	if err != nil {
		t.Fatalf("EncodeEvent: %v", err)
	}
	if encoded.DataType != "test.coded_error" {
		t.Fatalf("encoded DataType = %q, want application codec %q", encoded.DataType, "test.coded_error")
	}
	got, err := DecodeEvent(encoded)
	if err != nil {
		t.Fatalf("DecodeEvent: %v", err)
	}
	gotErr, ok := got.Data.(*codedError)
	if !ok || gotErr.Message != want.Message {
		t.Fatalf("decoded Data = %#v, want *codedError with message %q", got.Data, want.Message)
	}
}
