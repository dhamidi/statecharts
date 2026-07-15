package server

import (
	"testing"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/llm"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

func TestCanonicalPayloadsDecodeWithoutTypeRegistration(t *testing.T) {
	userMessage := UserMessageData{Text: "hello"}
	encoded, err := statecharts.EncodeEvent(statecharts.Event{Name: "user_message", Data: encodeUserMessage(userMessage)})
	if err != nil {
		t.Fatalf("EncodeEvent user message: %v", err)
	}
	event, err := statecharts.DecodeEvent(encoded)
	if err != nil {
		t.Fatalf("DecodeEvent user message: %v", err)
	}
	if got, ok := decodeUserMessage(event.Data); !ok || got != userMessage {
		t.Fatalf("decoded user message = %#v, %t; want %#v, true", got, ok, userMessage)
	}

	dispatch := dispatchPayload{
		ConversationID: "conversation-1",
		Request: llm.GenerateRequest{
			History: []llm.Message{{Role: llm.RoleUser, Text: "question"}},
			Tools:   []llm.ToolDef{{Name: "shell_command", Description: "run a command"}},
		},
	}
	encoded, err = statecharts.EncodeEvent(statecharts.Event{Name: "generate", Data: encodeDispatch(dispatch)})
	if err != nil {
		t.Fatalf("EncodeEvent dispatch: %v", err)
	}
	event, err = statecharts.DecodeEvent(encoded)
	if err != nil {
		t.Fatalf("DecodeEvent dispatch: %v", err)
	}
	got, ok := decodeDispatch(event.Data)
	if !ok || got.ConversationID != dispatch.ConversationID || len(got.Request.History) != 1 || got.Request.History[0] != dispatch.Request.History[0] || len(got.Request.Tools) != 1 || got.Request.Tools[0] != dispatch.Request.Tools[0] {
		t.Fatalf("decoded dispatch = %#v, %t; want %#v, true", got, ok, dispatch)
	}

	toolOffer := toolOffer{
		ConversationID: "conversation-1",
		Tool:           "shell_command",
		CallID:         "call-1",
		Args:           protocol.NewToolArgs(map[string]any{"command": "printf hello"}),
	}
	if got, ok := decodeToolOffer(encodeToolOffer(toolOffer)); !ok || got.ConversationID != toolOffer.ConversationID || got.Tool != toolOffer.Tool || got.CallID != toolOffer.CallID || got.Args["command"] != "printf hello" {
		t.Fatalf("decoded tool offer = %#v, %t; want %#v, true", got, ok, toolOffer)
	}

	catchup := catchupMessage{Seq: 10, Frame: protocol.MessageFrame{Role: protocol.RoleAssistant, Text: "tenth"}}
	if got, ok := decodeCatchupMessage(encodeCatchupMessage(catchup)); !ok || got != catchup {
		t.Fatalf("decoded catchup seq 10 = %#v, %t; want %#v, true", got, ok, catchup)
	}
}
