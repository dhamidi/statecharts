package statecharts

import (
	"errors"
	"testing"
)

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

func TestEventCodecUsesCanonicalValueBytes(t *testing.T) {
	payload, err := TaggedValue("door.opened/v1", ListValue([]Value{
		Int64Value(3),
		mustStringValue(t, "front"),
	}))
	if err != nil {
		t.Fatalf("TaggedValue: %v", err)
	}
	wantBytes, err := payload.MarshalBinary()
	if err != nil {
		t.Fatalf("Value.MarshalBinary: %v", err)
	}

	encoded, err := EncodeEvent(Event{Name: "door.opened", Type: EventExternal, Data: payload})
	if err != nil {
		t.Fatalf("EncodeEvent: %v", err)
	}
	if string(encoded.Data) != string(wantBytes) {
		t.Fatalf("encoded Data = %q, want canonical Value bytes %q", encoded.Data, wantBytes)
	}

	got, err := DecodeEvent(encoded)
	if err != nil {
		t.Fatalf("DecodeEvent: %v", err)
	}
	if !got.Data.Equal(payload) {
		t.Fatalf("decoded Data = %#v, want %#v", got.Data, payload)
	}
}

func TestEventCodecRejectsNonCanonicalValueBytes(t *testing.T) {
	_, err := DecodeEvent(EncodedEvent{Data: []byte(`{"version":99,"kind":"null"}`)})
	if err == nil {
		t.Fatal("DecodeEvent accepted a non-current Value wire version")
	}
}

func TestPlatformErrorValueHasStableClassificationAndMessage(t *testing.T) {
	value := PlatformErrorValue(ErrEventCommunication, errors.New("target became unavailable"))
	classification, message, ok := PlatformErrorDetails(value)
	if !ok {
		t.Fatalf("PlatformErrorDetails(%#v) ok = false", value)
	}
	if classification != ErrEventCommunication || message != "target became unavailable" {
		t.Fatalf("platform error = (%q, %q), want (%q, %q)", classification, message, ErrEventCommunication, "target became unavailable")
	}
	tag, _, ok := value.AsTagged()
	if !ok || tag != PlatformErrorValueTag {
		t.Fatalf("platform error tag = %q, %v, want %q, true", tag, ok, PlatformErrorValueTag)
	}
}
