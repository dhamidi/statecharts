package llm

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"
)

// wordDelay is the base delay EchoProvider streams successive words, and
// waits before a "thinking" or "tool_call" chunk, apart -- long enough to
// be visibly incremental in a browser, short enough not to make manual
// testing tedious. Every wait in this file is jittered around this base
// (see jitter) rather than fired on a perfectly uniform beat, since a real
// model's own streaming timing never is either.
const wordDelay = 30 * time.Millisecond

// stepDelim separates a scripted message's steps -- see EchoProvider's own
// doc comment. Deliberately two characters and unlikely to appear inside a
// real shell command under test, unlike a single ";" (a common, legitimate
// way to sequence commands within one shell invocation, e.g.
// "!echo a; echo b" as a single tool call).
const stepDelim = ";;"

// thinkingPhrases are cycled through (by step index) rather than repeated
// verbatim, so a multi-step script doesn't read as visibly canned.
var thinkingPhrases = []string{
	"Let me run that for you.",
	"One moment, checking.",
	"Let me try that next.",
	"Working on it.",
}

// EchoProvider is a fast, offline, deterministic Provider: no credentials,
// no network access, real streaming (a genuine goroutine and real,
// jittered delays, not an instant callback loop) so it exercises the same
// timing-variable consumer code (deltas arriving unevenly, a tool call
// landing mid-turn at an unpredictable moment, several tool calls in a
// row) a real backend would.
//
// EchoProvider treats the most recent RoleUser message in History as a
// script: stepDelim-separated steps, run one per Generate call (one call =
// one turn = one tool_call or one final text reply, exactly like a real
// model) rather than all at once, resuming from wherever the script left
// off. How far along the script is is never stored anywhere explicit --
// it's recomputed each call from History itself, by counting how many
// RoleTool entries (one per already-completed tool-call step) have landed
// since that user message, which is exactly ADR 0006's replay premise
// applied to a fake provider standing in for a stateful one.
//
//   - A step starting with "!" is a tool-call step: after a jittered pause,
//     EchoProvider emits a "thinking" chunk, then (after another pause)
//     decides to call "shell_command" with the text after "!" as its
//     command.
//   - A step not starting with "!" is a text step: EchoProvider streams it
//     back, upper-cased, word by word, as a final reply -- which, like a
//     real model's own final reply, ends the turn and returns the
//     conversation to idle. Because of that, only a script's LAST step can
//     usefully be a text step: earlier ones would just end the script
//     before any later step ever ran. A script that's nothing but "!"
//     steps (no trailing text step) falls back to streaming the final
//     tool's own output, upper-cased -- the original single-tool-call
//     convention, still available as the simplest case ("!echo hi" is a
//     one-step script).
//   - A plain message with no "!" anywhere is a one-step, all-text script:
//     it streams its own upper-cased text back immediately, same as
//     always.
//
// Example: "!echo one;; !sleep 1 && echo two;; done with both" runs
// shell_command twice in sequence (each with its own jittered "thinking"
// pause and tool_call), then replies "DONE WITH BOTH" once both results
// are in.
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

	steps, stepIndex := scriptSteps(req.History)
	if stepIndex >= len(steps) {
		// Every step already ran (or the script was never explicitly
		// terminated by a text step) -- fall back to echoing the most
		// recent tool result, exactly the original single-tool-call
		// convention's own final reply.
		last := req.History[len(req.History)-1]
		return streamWords(ctx, strings.ToUpper(last.Text), emit)
	}

	step := steps[stepIndex]
	if cmd, ok := strings.CutPrefix(step, "!"); ok {
		if err := sleep(ctx, jitter(wordDelay)); err != nil {
			return err
		}
		emit(Chunk{Kind: "thinking", TextDelta: thinkingPhrases[stepIndex%len(thinkingPhrases)]})

		if err := sleep(ctx, jitter(wordDelay)); err != nil {
			return err
		}
		emit(Chunk{Kind: "tool_call", ToolCall: ToolCall{
			ID:   "call", // the caller (llmrequest) overwrites/derives the real, deterministic call id
			Name: "shell_command",
			Args: map[string]any{"command": strings.TrimSpace(cmd)},
		}})
		return nil
	}

	if stepIndex > 0 {
		// This text step wraps up a script that already ran at least one
		// tool call -- a brief "thinking" pause first sells the "model is
		// reasoning about the tool results" illusion, matching a
		// tool-call step's own pacing instead of jumping straight to text.
		if err := sleep(ctx, jitter(wordDelay)); err != nil {
			return err
		}
		emit(Chunk{Kind: "thinking", TextDelta: thinkingPhrases[stepIndex%len(thinkingPhrases)]})
	}
	return streamWords(ctx, strings.ToUpper(step), emit)
}

// scriptSteps splits the most recent RoleUser message in history into
// stepDelim-separated steps and reports which one to run this turn: the
// count of RoleTool entries that have landed since that user message, each
// one corresponding to exactly one already-completed tool-call step (see
// EchoProvider's own doc comment for why recomputing this from History,
// rather than storing it anywhere, is deliberate). An empty history, or
// one with no user message at all, yields no steps.
func scriptSteps(history []Message) (steps []string, stepIndex int) {
	userIdx := -1
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == RoleUser {
			userIdx = i
			break
		}
	}
	if userIdx == -1 {
		return nil, 0
	}
	for _, s := range strings.Split(history[userIdx].Text, stepDelim) {
		steps = append(steps, strings.TrimSpace(s))
	}
	for _, m := range history[userIdx+1:] {
		if m.Role == RoleTool {
			stepIndex++
		}
	}
	return steps, stepIndex
}

// jitter returns d randomized by +/-40%, so consecutive waits don't fire on
// a perfectly uniform beat -- a real model's own streaming/thinking timing
// never does either.
func jitter(d time.Duration) time.Duration {
	const spread = 0.4
	factor := 1 - spread + rand.Float64()*2*spread
	return time.Duration(float64(d) * factor)
}

// sleep waits for d, honoring ctx cancellation, mirroring the wait pattern
// streamWords already used inline -- factored out since tool-call and
// thinking-preamble steps now need the identical wait in more than one
// place.
func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// streamWords emits text word by word, jitter(wordDelay) apart, honoring
// ctx cancellation between words.
func streamWords(ctx context.Context, text string, emit func(Chunk)) error {
	words := strings.Fields(text)
	for i, w := range words {
		if err := sleep(ctx, jitter(wordDelay)); err != nil {
			return err
		}
		delta := w
		if i > 0 {
			delta = " " + w
		}
		emit(Chunk{Kind: "text", TextDelta: delta})
	}
	return nil
}
