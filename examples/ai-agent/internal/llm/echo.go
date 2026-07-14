package llm

import (
	"context"
	"strings"
	"time"
)

// wordDelay is how far apart EchoProvider streams successive words -- long
// enough to be visibly incremental in a browser, short enough not to make
// manual testing tedious.
const wordDelay = 30 * time.Millisecond

// EchoProvider is a fast, offline, deterministic Provider: no credentials,
// no network access, real streaming (a genuine goroutine and real delays,
// not an instant callback loop) so it exercises the same timing-sensitive
// consumer code (deltas arriving over time, a tool call arriving mid-turn)
// a real backend would.
//
// Two conventions drive its behavior, both keyed off the *last* message in
// the request's History:
//
//   - If it's a tool result (Role: RoleTool), EchoProvider treats this as
//     the model's follow-up after a tool call: it streams the tool's own
//     output, upper-cased, as its final text reply.
//   - Otherwise (a fresh user message): a message starting with "!" is the
//     tool-call convention -- EchoProvider emits one "thinking" chunk, then
//     one "tool_call" chunk naming the "shell_command" tool with the text
//     after "!" as its command. Any other message streams its own
//     upper-cased text back, word by word.
//
// This convention is EchoProvider-only -- it has no meaning to
// internal/llmgenai's real provider, which lets an actual model decide
// when to call a tool.
type EchoProvider struct{}

// Generate implements Provider.
func (EchoProvider) Generate(ctx context.Context, req GenerateRequest, emit func(Chunk)) error {
	if len(req.History) == 0 {
		return nil
	}
	last := req.History[len(req.History)-1]

	if last.Role == RoleTool {
		return streamWords(ctx, strings.ToUpper(last.Text), emit)
	}

	if cmd, ok := strings.CutPrefix(last.Text, "!"); ok {
		select {
		case <-time.After(wordDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
		emit(Chunk{Kind: "thinking", TextDelta: "Let me run that for you."})

		select {
		case <-time.After(wordDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
		emit(Chunk{Kind: "tool_call", ToolCall: ToolCall{
			ID:   "call", // the caller (llmrequest) overwrites/derives the real, deterministic call id
			Name: "shell_command",
			Args: map[string]any{"command": cmd},
		}})
		return nil
	}

	return streamWords(ctx, strings.ToUpper(last.Text), emit)
}

// streamWords emits text word by word, wordDelay apart, honoring ctx
// cancellation between words.
func streamWords(ctx context.Context, text string, emit func(Chunk)) error {
	words := strings.Fields(text)
	for i, w := range words {
		select {
		case <-time.After(wordDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
		delta := w
		if i > 0 {
			delta = " " + w
		}
		emit(Chunk{Kind: "text", TextDelta: delta})
	}
	return nil
}
