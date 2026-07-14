// Package llm defines the Provider interface every LLM backend in this
// example implements, plus EchoProvider, a fast, offline, deterministic
// stand-in that needs no credentials or network access.
package llm

import "context"

// Role identifies who produced a Message.
type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// Message is one entry in a conversation's history, as sent to a Provider.
type Message struct {
	Role Role
	Text string
}

// ToolDef describes one tool a Provider may decide to call.
type ToolDef struct {
	Name        string
	Description string
}

// ToolCall is a Provider's decision to invoke a tool.
type ToolCall struct {
	ID   string
	Name string
	Args map[string]any
}

// Chunk is one piece of a streamed reply.
type Chunk struct {
	Kind      string // "thinking" | "text" | "tool_call"
	TextDelta string
	ToolCall  ToolCall
}

// GenerateRequest is one turn's worth of context handed to a Provider.
type GenerateRequest struct {
	History []Message
	Tools   []ToolDef
}

// Provider streams a reply to req, calling emit for each Chunk in order as
// it becomes available, and returns once the reply is complete or ctx is
// cancelled. A non-nil error means the reply did not complete successfully;
// emit may still have been called for a partial reply before the error.
// Both EchoProvider and the real google.golang.org/genai-backed provider
// (internal/llmgenai) implement this identically, so consumer code (the
// llmrequest actor, see internal/server) never knows which one it's
// talking to.
type Provider interface {
	Generate(ctx context.Context, req GenerateRequest, emit func(Chunk)) error
}
