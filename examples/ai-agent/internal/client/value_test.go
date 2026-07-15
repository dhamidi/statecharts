package client

import (
	"testing"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

func TestClientPayloadDecodesCanonicalExponentInteger(t *testing.T) {
	fields := map[string]statecharts.Value{"seq": statecharts.Int64Value(10)}
	if got, ok := intField(fields, "seq"); !ok || got != 10 {
		t.Fatalf("intField canonical 1e1 = (%d, %t), want (10, true)", got, ok)
	}

	want := messageWithSeq{Seq: 10, Frame: protocol.MessageFrame{Role: protocol.RoleAssistant, Text: "tenth"}}
	if got, ok := decodeMessageSeq(messageSeqValue(want)); !ok || got != want {
		t.Fatalf("decoded message seq 10 = %#v, %t; want %#v, true", got, ok, want)
	}
}
