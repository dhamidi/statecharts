// Package protocol defines the wire shapes server and client exchange over
// HTTP/SSE. internal/server and internal/client never import each other --
// they only share these struct definitions -- so the fact that they still
// talk exclusively over HTTP/SSE holds even when compiled into the same
// binary and run in the same process (embedded mode).
package protocol

// Role identifies who produced a MessageFrame.
type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// MessageFrame is an SSE "message" event's data: one finished, durable
// transcript entry. Its 1-based position in the conversation's own History
// -- the same number ConversationActor and ConnectionActor both derive
// purely from actor state, never a raw Log read -- travels via the SSE
// "id:" field, not a struct field, so a resuming client's Last-Event-ID
// header works with no extra parsing.
type MessageFrame struct {
	Role Role   `json:"role"`
	Text string `json:"text"`
}

// DeltaFrame is an SSE "delta" event's data: an in-progress, never-logged
// preview chunk.
type DeltaFrame struct {
	Kind string `json:"kind"` // "thinking" | "text"
	Text string `json:"text"`
}

// ToolCallFrame is an SSE "tool_call" event's data -- sent only to the
// connection currently holding the executor lease for Name. ConversationID
// travels with it because the lease -- and so this connection -- is shared
// across every conversation that names this tool, not scoped to whichever
// one this connection's own SSE subscription happens to currently be
// displaying: a client executing shell_command can be asked to run a
// command for a conversation other than the one it's actively viewing, and
// must post the result back to the right one.
type ToolCallFrame struct {
	ConversationID ConversationID `json:"conversation_id"`
	CallID         CallID         `json:"call_id"`
	Name           ToolName       `json:"name"`
	Args           ToolArgs       `json:"args"`
}

// SendMessageRequest is POST /conversations/{id}/messages's body.
type SendMessageRequest struct {
	Text string `json:"text"`
}

// ToolResultRequest is POST /conversations/{id}/tool-result's body.
type ToolResultRequest struct {
	CallID   CallID `json:"call_id"`
	Output   string `json:"output"`
	ExitCode int    `json:"exit_code"`
	Error    string `json:"error,omitempty"`
}

// CreateConversationRequest is POST /conversations's body.
type CreateConversationRequest struct {
	Title string `json:"title"`
}

// CreateConversationResponse is POST /conversations's response.
type CreateConversationResponse struct {
	ID ConversationID `json:"id"`
}

// ConversationState is the small, closed set of states a conversation
// reports for its sidebar badge.
type ConversationState string

const (
	ConversationIdle         ConversationState = "idle"
	ConversationThinking     ConversationState = "thinking"
	ConversationAwaitingTool ConversationState = "awaiting_tool"
)

// ConversationSummary is one entry in GET /conversations's response array.
type ConversationSummary struct {
	ID    ConversationID    `json:"id"`
	Title string            `json:"title"`
	State ConversationState `json:"state"`
}
