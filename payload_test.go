package statecharts

import "testing"

func TestClonePayloadPreservesTypesCyclesAndCapabilities(t *testing.T) {
	type payload struct {
		Values map[string][]int
		Next   *payload
		Done   chan struct{}
	}
	done := make(chan struct{})
	original := &payload{Values: map[string][]int{"numbers": {1, 2}}, Done: done}
	original.Next = original

	gotAny, err := clonePayload(original)
	if err != nil {
		t.Fatalf("clonePayload: %v", err)
	}
	got := gotAny.(*payload)
	original.Values["numbers"][0] = 9
	if got == original || got.Next != got || got.Values["numbers"][0] != 1 {
		t.Fatalf("clone = %#v, want isolated concrete cyclic payload", got)
	}
	if got.Done != done {
		t.Fatal("clone did not preserve opaque channel capability")
	}
}
