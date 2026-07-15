package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/dhamidi/statecharts"
	"github.com/dhamidi/statecharts/actors"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/llm"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// dispatchPayload is "generate"'s durable outbound payload from
// ConversationActor to LLMDispatchProcessor (registered as the explicit
// "llm" processor) and, as "start", from
// LLMDispatchProcessor to the llmrequest actor it just spawned. The first
// hop is encoded in ConversationActor's durable outbox; the second target is
// ephemeral and receives the same decoded concrete type.
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

func recordRequestStart(d *llmRequestModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	payload, ok := decodeDispatch(ev.Data)
	if !ok {
		return nil
	}
	d.ConversationID = payload.ConversationID
	return nil
}

func applyProviderChunk(d *llmRequestModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	c, ok := decodeProviderChunk(ev.Data)
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
}

func broadcastDelta(ec statecharts.ExecContext, conversationID protocol.ConversationID, kind, text string) {
	ec.Send("broadcast", statecharts.SendOptions{
		Target: "fanout",
		Data: encodeFanoutBroadcast(fanoutBroadcast{
			ConversationID: conversationID,
			Kind:           "delta",
			Delta:          deltaFrame{Kind: kind, Text: text},
		}),
	})
}

// deltaFrame mirrors protocol.DeltaFrame, as its own type at this internal
// actor-to-actor hop (the HTTP layer converts at the real wire boundary --
// see connection.go).
type deltaFrame struct {
	Kind string
	Text string
}

func sendFinalReply(d *llmRequestModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ec.Send("llm_reply", statecharts.SendOptions{
		Target: statecharts.Identifier(d.ConversationID),
		Data: encodeLLMReply(LLMReplyData{
			IsToolCall: d.IsToolCall,
			Text:       d.Text.String(),
			ToolName:   d.ToolName,
			ToolArgs:   d.ToolArgs,
		}),
	})
	return nil
}

func sendErrorReply(d *llmRequestModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	msg := "unknown error"
	if _, detail, ok := statecharts.PlatformErrorDetails(ev.Data); ok {
		msg = detail
	}
	ec.Send("llm_reply", statecharts.SendOptions{
		Target: statecharts.Identifier(d.ConversationID),
		Data:   encodeLLMReply(LLMReplyData{Text: fmt.Sprintf("[error: %s]", msg)}),
	})
	return nil
}

// LLMRequestKind is the chart kind name a per-turn llmrequest actor is
// Registered and (by LLMDispatchProcessor) Spawned under.
const LLMRequestKind statecharts.Identifier = "llmrequest"

// BuildLLMRequestChart returns the non-durable, per-turn "llmrequest"
// chart: it reaches its own top-level final state the moment the turn
// concludes, so the actor system frees it immediately (see actors'
// automatic eviction of actors in a final state) rather than it lingering
// resident forever.
func BuildLLMRequestChart() (*statecharts.Chart, error) {
	model := statecharts.NewGoModel(func() *llmRequestModel { return &llmRequestModel{} })
	action := func(operation string, fn statecharts.GoAction[llmRequestModel]) (statecharts.GoActionRef, error) {
		return model.Action(statecharts.Identifier("ai-agent.server.llmrequest."+operation), "v1", fn)
	}
	record, err := action("record-request-start", recordRequestStart)
	if err != nil {
		return nil, err
	}
	apply, err := action("apply-provider-chunk", applyProviderChunk)
	if err != nil {
		return nil, err
	}
	finalReply, err := action("send-final-reply", sendFinalReply)
	if err != nil {
		return nil, err
	}
	errorReply, err := action("send-error-reply", sendErrorReply)
	if err != nil {
		return nil, err
	}
	return buildCanonicalChart(
		statecharts.Compound("llmrequest", "active",
			statecharts.Children(
				statecharts.Atomic("active",
					statecharts.On("start", statecharts.Then(record.Do())),
					statecharts.On("provider_chunk", statecharts.Then(apply.Do())),
					statecharts.On("provider_done", statecharts.Target("done"), statecharts.Then(finalReply.Do())),
					statecharts.On("provider_error", statecharts.Target("done"), statecharts.Then(errorReply.Do())),
				),
				statecharts.Final("done"),
			),
		),
		model, statecharts.WithRevisionSalt("llmrequest-v1"))
}

// LLMDispatchProcessor is the statecharts.IOProcessor installed via
// actors.WithIOProcessor: the way a chart action (which only ever gets
// an ExecContext, never a *actors.System) can start a new actor and drive a
// real streaming provider call from a goroutine. Its Send is reached only
// when ConversationActor addresses a not-yet-spawned llmrequest actor's own
// name with SendOptions{Type: "llm"} -- routingProcessor's own routing
// table has no entry for that name yet, so it falls through here.
type LLMDispatchProcessor struct {
	provider llm.Provider

	// sys is filled in by SetSystem, once, before any traffic flows --
	// actors.WithIOProcessor needs a complete IOProcessor before NewSystem can
	// even return the *System this processor needs to Spawn/Tell against,
	// the same chicken-and-egg actors.Bridge already solves via its own
	// NewBridge(nil)+SetTarget.
	sys *actors.System
}

// NewLLMDispatchProcessor returns an LLMDispatchProcessor driving provider.
// Call SetSystem before starting the System it will be installed on via
// actors.WithIOProcessor.
func NewLLMDispatchProcessor(provider llm.Provider) *LLMDispatchProcessor {
	return &LLMDispatchProcessor{provider: provider}
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
	payload, ok := decodeDispatch(req.Data)
	if !ok {
		return fmt.Errorf("examples/ai-agent: LLMDispatchProcessor: invalid dispatch payload")
	}

	if err := p.sys.Spawn(ctx, req.Target, LLMRequestKind); err != nil {
		return fmt.Errorf("examples/ai-agent: LLMDispatchProcessor: spawn %q: %w", req.Target, err)
	}
	if err := p.sys.Tell(ctx, req.Target, statecharts.Event{
		Name: "start", Type: statecharts.EventExternal, Data: encodeDispatch(payload),
	}); err != nil {
		return fmt.Errorf("examples/ai-agent: LLMDispatchProcessor: start %q: %w", req.Target, err)
	}

	target := req.Target
	sys := p.sys
	provider := p.provider
	go func() {
		err := provider.Generate(context.Background(), payload.Request, func(c llm.Chunk) {
			_ = sys.Tell(context.Background(), target, statecharts.Event{
				Name: "provider_chunk", Type: statecharts.EventExternal, Data: encodeProviderChunk(c),
			})
		})
		if err != nil {
			_ = sys.Tell(context.Background(), target, statecharts.Event{
				Name: "provider_error", Type: statecharts.EventExternal, Data: statecharts.PlatformErrorValue(statecharts.ErrEventExecution, err),
			})
			return
		}
		_ = sys.Tell(context.Background(), target, statecharts.Event{
			Name: "provider_done", Type: statecharts.EventExternal,
		})
	}()

	return nil
}
