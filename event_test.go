package statecharts

import "testing"

type doorOpened struct{ Count int }

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
