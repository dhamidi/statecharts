package server

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/llm"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// dispatchPayload is "generate"'s payload from ConversationActor to
// LLMDispatchProcessor (via the WithFallback hop, never logged -- see
// router.go's routingProcessor.Send) and, as "start", from
// LLMDispatchProcessor to the llmrequest actor it just spawned. Not
// JSON-wrapped: neither hop ever reaches system.deliver's write-ahead log.
type dispatchPayload struct {
	ConversationID protocol.ConversationID
	Request        llm.GenerateRequest
}

// llmRequestModel is one per-turn llmrequest actor's (non-durable)
// datamodel: it accumulates whatever the Provider streamed until the turn
// concludes, then sends exactly one llm_reply to the owning conversation.
type llmRequestModel struct {
	ConversationID protocol.ConversationID
	Thinking       strings.Builder
	Text           strings.Builder
	IsToolCall     bool
	ToolName       protocol.ToolName
	ToolArgs       protocol.ToolArgs
}

var recordRequestStart = statecharts.Action(func(d *llmRequestModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	payload, ok := statecharts.Payload[dispatchPayload](ev)
	if !ok {
		return nil
	}
	d.ConversationID = payload.ConversationID
	return nil
})

var applyProviderChunk = statecharts.Action(func(d *llmRequestModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	c, ok := statecharts.Payload[llm.Chunk](ev)
	if !ok {
		return nil
	}
	switch c.Kind {
	case "thinking":
		d.Thinking.WriteString(c.TextDelta)
		broadcastDelta(ec, d.ConversationID, "thinking", c.TextDelta)
	case "text":
		d.Text.WriteString(c.TextDelta)
		broadcastDelta(ec, d.ConversationID, "text", c.TextDelta)
	case "tool_call":
		d.IsToolCall = true
		d.ToolName = protocol.ToolName(c.ToolCall.Name)
		d.ToolArgs = protocol.NewToolArgs(c.ToolCall.Args)
	}
	return nil
})

func broadcastDelta(ec statecharts.ExecContext, conversationID protocol.ConversationID, kind, text string) {
	ec.Send("broadcast", statecharts.SendOptions{
		Target: "fanout",
		Data: fanoutBroadcast{
			ConversationID: conversationID,
			Kind:           "delta",
			Frame:          deltaFrame{Kind: kind, Text: text},
		},
	})
}

// deltaFrame mirrors protocol.DeltaFrame, as its own type at this internal
// actor-to-actor hop (the HTTP layer converts at the real wire boundary --
// see connection.go).
type deltaFrame struct {
	Kind string
	Text string
}

var sendFinalReply = statecharts.Action(func(d *llmRequestModel, ec statecharts.ExecContext) error {
	ec.Send("llm_reply", statecharts.SendOptions{
		Target: statecharts.Identifier(d.ConversationID),
		Data: &llmReplyPayload{
			TypeName: "aiagent.llm_reply",
			Value: LLMReplyData{
				IsToolCall: d.IsToolCall,
				Text:       d.Text.String(),
				ToolName:   d.ToolName,
				ToolArgs:   d.ToolArgs,
			},
		},
	})
	return nil
})

var sendErrorReply = statecharts.Action(func(d *llmRequestModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	msg := "unknown error"
	if err, ok := statecharts.Payload[error](ev); ok && err != nil {
		msg = err.Error()
	}
	ec.Send("llm_reply", statecharts.SendOptions{
		Target: statecharts.Identifier(d.ConversationID),
		Data: &llmReplyPayload{
			TypeName: "aiagent.llm_reply",
			Value:    LLMReplyData{Text: fmt.Sprintf("[error: %s]", msg)},
		},
	})
	return nil
})

// LLMRequestKind is the chart kind name a per-turn llmrequest actor is
// Registered and (by LLMDispatchProcessor) Spawned under.
const LLMRequestKind statecharts.Identifier = "llmrequest"

// BuildLLMRequestChart returns the non-durable, per-turn "llmrequest"
// chart: it reaches its own top-level final state the moment the turn
// concludes, so the actor system frees it immediately (see actors'
// automatic eviction of actors in a final state) rather than it lingering
// resident forever.
func BuildLLMRequestChart() (*statecharts.Chart, error) {
	return statecharts.Build(
		statecharts.Compound("llmrequest", "active",
			statecharts.Children(
				statecharts.Atomic("active",
					statecharts.On("start", statecharts.Then(recordRequestStart)),
					statecharts.On("provider_chunk", statecharts.Then(applyProviderChunk)),
					statecharts.On("provider_done", statecharts.Target("done"), statecharts.Then(sendFinalReply)),
					statecharts.On("provider_error", statecharts.Target("done"), statecharts.Then(sendErrorReply)),
				),
				statecharts.Final("done"),
			),
		),
		statecharts.WithNewDatamodel(func() any { return &llmRequestModel{} }),
	)
}

// LLMDispatchProcessor is the statecharts.IOProcessor installed via
// actors.WithFallback: the only way a chart action (which only ever gets
// an ExecContext, never a *actors.System) can start a new actor and drive a
// real streaming provider call from a goroutine. Its Send is reached only
// when ConversationActor addresses a not-yet-spawned llmrequest actor's own
// name with SendOptions{Type: "llm"} -- routingProcessor's own routing
// table has no entry for that name yet, so it falls through here.
type LLMDispatchProcessor struct {
	provider llm.Provider

	// sys is filled in by SetSystem, once, before any traffic flows --
	// actors.WithFallback needs a complete IOProcessor before NewSystem can
	// even return the *System this processor needs to Spawn/Tell against,
	// the same chicken-and-egg actors.Bridge already solves via its own
	// NewBridge(nil)+SetTarget.
	sys *actors.System

	mu      sync.Mutex
	cancels map[statecharts.Identifier]context.CancelFunc
}

// NewLLMDispatchProcessor returns an LLMDispatchProcessor driving provider.
// Call SetSystem before starting the System it will be installed on via
// actors.WithFallback.
func NewLLMDispatchProcessor(provider llm.Provider) *LLMDispatchProcessor {
	return &LLMDispatchProcessor{provider: provider, cancels: map[statecharts.Identifier]context.CancelFunc{}}
}

// SetSystem supplies the *actors.System this processor spawns per-turn
// llmrequest actors on and Tells results back through. Must be called
// exactly once, before Register/Spawn/Tell are ever called on sys.
func (p *LLMDispatchProcessor) SetSystem(sys *actors.System) { p.sys = sys }

// Attach implements statecharts.IOProcessor. LLMDispatchProcessor represents
// "the LLM", not any one actor's own session -- its results reach their
// destination via sys.Tell (safe from any goroutine), not by delivering
// back through a Dispatcher of its own, so there is nothing to capture here.
func (p *LLMDispatchProcessor) Attach(statecharts.Dispatcher) {}

// Send implements statecharts.IOProcessor.
func (p *LLMDispatchProcessor) Send(ctx context.Context, req statecharts.SendRequest) error {
	if req.Type != "llm" {
		return fmt.Errorf("examples/ai-agent: LLMDispatchProcessor: unsupported send (target %q, type %q)", req.Target, req.Type)
	}
	payload, ok := req.Data.(dispatchPayload)
	if !ok {
		return fmt.Errorf("examples/ai-agent: LLMDispatchProcessor: unexpected data type %T", req.Data)
	}

	if err := p.sys.Spawn(ctx, req.Target, LLMRequestKind); err != nil {
		return fmt.Errorf("examples/ai-agent: LLMDispatchProcessor: spawn %q: %w", req.Target, err)
	}
	if err := p.sys.Tell(ctx, req.Target, statecharts.Event{
		Name: "start", Type: statecharts.EventExternal, Data: payload,
	}); err != nil {
		return fmt.Errorf("examples/ai-agent: LLMDispatchProcessor: start %q: %w", req.Target, err)
	}

	reqCtx, cancel := context.WithCancel(context.Background())
	p.mu.Lock()
	p.cancels[req.SendID] = cancel
	p.mu.Unlock()

	target := req.Target
	sys := p.sys
	provider := p.provider
	go func() {
		defer func() {
			p.mu.Lock()
			delete(p.cancels, req.SendID)
			p.mu.Unlock()
		}()

		err := provider.Generate(reqCtx, payload.Request, func(c llm.Chunk) {
			_ = sys.Tell(context.Background(), target, statecharts.Event{
				Name: "provider_chunk", Type: statecharts.EventExternal, Data: c,
			})
		})
		if err != nil {
			_ = sys.Tell(context.Background(), target, statecharts.Event{
				Name: "provider_error", Type: statecharts.EventExternal, Data: err,
			})
			return
		}
		_ = sys.Tell(context.Background(), target, statecharts.Event{
			Name: "provider_done", Type: statecharts.EventExternal,
		})
	}()

	return nil
}

// Cancel implements statecharts.IOProcessor, best-effort cancelling the
// goroutine driving sendID's provider call, if it's still running.
func (p *LLMDispatchProcessor) Cancel(ctx context.Context, sendID statecharts.Identifier) error {
	p.mu.Lock()
	cancel, ok := p.cancels[sendID]
	p.mu.Unlock()
	if ok {
		cancel()
	}
	return nil
}
