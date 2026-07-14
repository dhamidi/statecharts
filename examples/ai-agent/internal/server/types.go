// Package server implements the workspace server: the durable
// ConversationActor and UserActor, the non-durable FanoutActor,
// ToolRegistryActor, DirectoryActor, ConnectionActor and per-turn
// llmrequest actors, LLMDispatchProcessor, and the HTTP/SSE handlers that
// tie them together.
package server

import (
	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// Every Event.Data that can reach a *durable* target (a "conversation" or
// "user" actor) must be RegisterDataType-registered, since a durable
// actor's Log.Append (system.deliver) requires DataMarshaler. Non-durable
// targets (fanout, toolregistry, directory, llmrequest, connections) and
// the LLMDispatchProcessor fallback hop are never logged, so their payloads
// are plain Go structs with no such requirement.

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

// registerDataTypes must run once at startup, before any durable actor can
// receive traffic (see the six call sites' own comments below, matching
// issue #4's acceptance criteria: every Event.Data reaching a durable actor
// is registered).
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
}
