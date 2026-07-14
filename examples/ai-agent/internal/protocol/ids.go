package protocol

import "fmt"

// ConversationID identifies one conversation: its actor name server-side,
// and the value carried in URLs, JSON bodies, and SSE routing on both
// sides of the wire.
type ConversationID string

// NewConversationID validates and constructs a ConversationID.
func NewConversationID(s string) (ConversationID, error) {
	if s == "" {
		return "", fmt.Errorf("protocol: empty conversation id")
	}
	return ConversationID(s), nil
}

func (id ConversationID) String() string { return string(id) }

// ToolName identifies a tool by name (e.g. "shell_command") -- the unit
// ToolRegistryActor leases executorship of.
type ToolName string

// NewToolName validates and constructs a ToolName.
func NewToolName(s string) (ToolName, error) {
	if s == "" {
		return "", fmt.Errorf("protocol: empty tool name")
	}
	return ToolName(s), nil
}

func (n ToolName) String() string { return string(n) }

// CallID identifies one specific tool call within a conversation.
type CallID string

// NewCallID validates and constructs a CallID.
func NewCallID(s string) (CallID, error) {
	if s == "" {
		return "", fmt.Errorf("protocol: empty call id")
	}
	return CallID(s), nil
}

func (id CallID) String() string { return string(id) }

// ConnectionID identifies one live client connection (one SSE stream):
// server-side ConnectionActor's own actor name, and the value a
// ToolRegistryActor lease's Owner holds.
type ConnectionID string

// NewConnectionID validates and constructs a ConnectionID.
func NewConnectionID(s string) (ConnectionID, error) {
	if s == "" {
		return "", fmt.Errorf("protocol: empty connection id")
	}
	return ConnectionID(s), nil
}

func (id ConnectionID) String() string { return string(id) }

// ToolArgs is a tool call's arguments: a structured key/value object (what
// a real LLM's function-calling API actually hands back, and what a tool
// implementation actually wants to read fields off of), never an opaque
// JSON-encoded string passed around and re-parsed at every hop. It
// round-trips through JSON as a plain object, since map[string]any already
// marshals that way.
type ToolArgs map[string]any

// NewToolArgs constructs a ToolArgs from v, treating a nil map as empty
// (a tool that takes no arguments) rather than a meaningful nil.
func NewToolArgs(v map[string]any) ToolArgs {
	if v == nil {
		return ToolArgs{}
	}
	return ToolArgs(v)
}
