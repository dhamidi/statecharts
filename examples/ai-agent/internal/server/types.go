// Package server implements the workspace server: the durable
// ConversationActor and UserActor, the non-durable FanoutActor,
// ToolRegistryActor, DirectoryActor, ConnectionActor and per-turn
// llmrequest actors, LLMDispatchProcessor, and the HTTP/SSE handlers that
// tie them together.
package server

import (
	"encoding/json"
	"fmt"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// Every Event.Data that crosses a durable boundary must be registered: both
// inbound events logged by a durable target and outbound intents emitted by
// a durable sender. The latter remains true when the recipient itself is an
// ephemeral actor, because the sender's outbox must be able to retry the
// original typed request after a crash.

// UserMessageData is "user_message"'s payload -> conversation.
type UserMessageData struct {
	Text string
}

type userMessagePayload = statecharts.JSONData[UserMessageData]

// ToolResultData is "tool_result"'s payload -> conversation.
type ToolResultData struct {
	CallID   protocol.CallID
	Output   string
	ExitCode int
	Error    string
}

type toolResultPayload = statecharts.JSONData[ToolResultData]

// LLMReplyData is "llm_reply"'s payload -> conversation, covering both
// shapes a turn can resolve to: plain text, or a decided tool call.
type LLMReplyData struct {
	IsToolCall bool
	Text       string
	ToolName   protocol.ToolName
	ToolArgs   protocol.ToolArgs
}

type llmReplyPayload = statecharts.JSONData[LLMReplyData]

// RegisterConversationData is "register"'s payload -> user.
type RegisterConversationData struct {
	ID    protocol.ConversationID
	Title string
}

type registerConversationPayload = statecharts.JSONData[RegisterConversationData]

// ConversationStateData is "state_changed"'s payload -> user.
type ConversationStateData struct {
	ID    protocol.ConversationID
	State protocol.ConversationState
}

type conversationStatePayload = statecharts.JSONData[ConversationStateData]

// CatchupRequestData is "catchup"'s payload -> conversation: a connection
// asking to be sent every transcript entry it doesn't already have, by
// ordinary actor Send -- never by reading the conversation's Log directly.
// ConversationActor answers from its own already-rehydrated History, the
// same in-memory state it would use to serve a live turn.
type CatchupRequestData struct {
	Connection protocol.ConnectionID
	FromSeq    int // 0 = from the beginning
}

type catchupRequestPayload = statecharts.JSONData[CatchupRequestData]

type directorySyncPayload = statecharts.JSONData[protocol.ConversationSummary]

func marshalActorData(typeName string, value any) (string, []byte, error) {
	payload, err := json.Marshal(value)
	return typeName, payload, err
}

func (p *dispatchPayload) MarshalData() (string, []byte, error) {
	return marshalActorData("aiagent.dispatch", p)
}

func (p *dispatchPayload) UnmarshalData(payload []byte) error { return json.Unmarshal(payload, p) }

func (p *toolOffer) MarshalData() (string, []byte, error) {
	return marshalActorData("aiagent.tool_offer", p)
}

func (p *toolOffer) UnmarshalData(payload []byte) error { return json.Unmarshal(payload, p) }

func (p *catchupMessage) MarshalData() (string, []byte, error) {
	return marshalActorData("aiagent.catchup_message", p)
}

func (p *catchupMessage) UnmarshalData(payload []byte) error { return json.Unmarshal(payload, p) }

type fanoutBroadcastWire struct {
	ConversationID protocol.ConversationID
	Kind           string
	Seq            int
	Frame          json.RawMessage
}

func (p *fanoutBroadcast) MarshalData() (string, []byte, error) {
	frame, err := json.Marshal(p.Frame)
	if err != nil {
		return "", nil, err
	}
	return marshalActorData("aiagent.fanout_broadcast", fanoutBroadcastWire{
		ConversationID: p.ConversationID,
		Kind:           p.Kind,
		Seq:            p.Seq,
		Frame:          frame,
	})
}

func (p *fanoutBroadcast) UnmarshalData(payload []byte) error {
	var wire fanoutBroadcastWire
	if err := json.Unmarshal(payload, &wire); err != nil {
		return err
	}
	p.ConversationID, p.Kind, p.Seq = wire.ConversationID, wire.Kind, wire.Seq
	switch wire.Kind {
	case "message":
		var frame protocol.MessageFrame
		if err := json.Unmarshal(wire.Frame, &frame); err != nil {
			return err
		}
		p.Frame = frame
	case "delta":
		var frame deltaFrame
		if err := json.Unmarshal(wire.Frame, &frame); err != nil {
			return err
		}
		p.Frame = frame
	default:
		return fmt.Errorf("unknown fanout frame kind %q", wire.Kind)
	}
	return nil
}

// registerDataTypes must run once at startup, before any durable actor can
// receive traffic or emit an outbound intent.
func registerDataTypes() {
	statecharts.RegisterDataType("aiagent.user_message", func() statecharts.DataUnmarshaler {
		return &userMessagePayload{TypeName: "aiagent.user_message"}
	})
	statecharts.RegisterDataType("aiagent.tool_result", func() statecharts.DataUnmarshaler {
		return &toolResultPayload{TypeName: "aiagent.tool_result"}
	})
	statecharts.RegisterDataType("aiagent.llm_reply", func() statecharts.DataUnmarshaler {
		return &llmReplyPayload{TypeName: "aiagent.llm_reply"}
	})
	statecharts.RegisterDataType("aiagent.register_conversation", func() statecharts.DataUnmarshaler {
		return &registerConversationPayload{TypeName: "aiagent.register_conversation"}
	})
	statecharts.RegisterDataType("aiagent.conversation_state", func() statecharts.DataUnmarshaler {
		return &conversationStatePayload{TypeName: "aiagent.conversation_state"}
	})
	statecharts.RegisterDataType("aiagent.catchup_request", func() statecharts.DataUnmarshaler {
		return &catchupRequestPayload{TypeName: "aiagent.catchup_request"}
	})
	statecharts.RegisterDataType("aiagent.directory_sync", func() statecharts.DataUnmarshaler {
		return &directorySyncPayload{TypeName: "aiagent.directory_sync"}
	})
	statecharts.RegisterDataType("aiagent.dispatch", func() statecharts.DataUnmarshaler { return &dispatchPayload{} })
	statecharts.RegisterDataType("aiagent.tool_offer", func() statecharts.DataUnmarshaler { return &toolOffer{} })
	statecharts.RegisterDataType("aiagent.catchup_message", func() statecharts.DataUnmarshaler { return &catchupMessage{} })
	statecharts.RegisterDataType("aiagent.fanout_broadcast", func() statecharts.DataUnmarshaler { return &fanoutBroadcast{} })
}
