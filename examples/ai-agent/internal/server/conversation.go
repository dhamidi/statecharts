package server

import (
	"fmt"
	"time"

	"github.com/dhamidi/statecharts"

	"github.com/dhamidi/statecharts/examples/ai-agent/internal/llm"
	"github.com/dhamidi/statecharts/examples/ai-agent/internal/protocol"
)

// conversationModel is ConversationActor's durable datamodel. Every
// identifier it derives itself (reqName, the tool-call id) comes from
// TurnSeq, never uuid/rand/time.Now -- ADR 0006's replay premise requires
// everything but the two logged entry kinds to be pure, deterministic
// recomputation. History is also the sole source of truth a "catchup"
// request is answered from (see replyWithCatchup) -- never the Log
// directly.
type conversationModel struct {
	History []llm.Message
	TurnSeq int

	PendingRequest  statecharts.Identifier // the per-turn llmrequest actor's name
	PendingToolName protocol.ToolName
	PendingArgs     protocol.ToolArgs
	PendingCallID   protocol.CallID
	PendingRetries  int // count of tool_offer_retry ticks since this tool call was decided; see toolOfferMaxRetries
}

// toolOfferRetryInterval is how often awaiting_tool re-offers a pending tool
// call to toolregistry while nobody has claimed the lease (or the offer was
// delivered but its owner never answered). toolOfferMaxRetries bounds how
// long a conversation waits before giving up: retryOrFailToolOffer counts
// ticks rather than reading a clock, since a durable actor's own actions
// must stay pure and replay-deterministic (ADR 0006) -- counting the number
// of times this purely-internal, self-scheduled event has fired is exactly
// as deterministic on replay as the delayed Send that produces it already
// is.
const (
	toolOfferRetryInterval = 10 * time.Second
	toolOfferMaxRetries    = 6 // 6 * 10s = 60s grace period
)

// staticToolDefs is the fixed set of tools ConversationActor tells every
// Provider it can offer. Whether a client is actually around to execute one
// is ToolRegistryActor's concern (a lease), not the LLM-facing tool list.
var staticToolDefs = []llm.ToolDef{
	{Name: "shell_command", Description: "Execute a shell command on the user's machine and return its output."},
}

// catchupMessage is "catchup_message"'s payload -> a specific connection:
// one reconstructed transcript entry, numbered the same way a live
// broadcastLastMessage numbers it (its 1-based index within History), so a
// resuming client's Last-Event-ID lines up whether the entry arrived via
// catch-up or live.
type catchupMessage struct {
	Seq   int
	Frame protocol.MessageFrame
}

func mapRole(r llm.Role) protocol.Role {
	switch r {
	case llm.RoleAssistant:
		return protocol.RoleAssistant
	case llm.RoleTool:
		return protocol.RoleTool
	default:
		return protocol.RoleUser
	}
}

func reportState(state protocol.ConversationState) statecharts.ActionFunc {
	return func(ec statecharts.ExecContext) error {
		convID, err := protocol.NewConversationID(ec.SessionID())
		if err != nil {
			return err
		}
		ec.Send("state_changed", statecharts.SendOptions{
			Target: "user",
			Data: &conversationStatePayload{
				TypeName: "aiagent.conversation_state",
				Value:    ConversationStateData{ID: convID, State: state},
			},
		})
		return nil
	}
}

// broadcastLastMessage sends the just-appended History entry (assumed to
// be the last thing the preceding action in the same Then(...) list added)
// to fanout as a durable "message" frame, numbered by its position in
// History -- the same numbering replyWithCatchup uses for the same entry,
// so live and catch-up delivery of the same entry always agree.
var broadcastLastMessage = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	if len(d.History) == 0 {
		return nil
	}
	convID, err := protocol.NewConversationID(ec.SessionID())
	if err != nil {
		return err
	}
	last := d.History[len(d.History)-1]
	ec.Send("broadcast", statecharts.SendOptions{
		Target: "fanout",
		Data: fanoutBroadcast{
			ConversationID: convID,
			Kind:           "message",
			Seq:            len(d.History),
			Frame:          protocol.MessageFrame{Role: mapRole(last.Role), Text: last.Text},
		},
	})
	return nil
})

var appendUserMessage = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	payload, _ := statecharts.Payload[*userMessagePayload](ev)
	if payload != nil {
		d.History = append(d.History, llm.Message{Role: llm.RoleUser, Text: payload.Value.Text})
	}
	return nil
})

var startRequest = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	d.TurnSeq++
	reqName := statecharts.Identifier(fmt.Sprintf("%s-%d", ec.SessionID(), d.TurnSeq))
	d.PendingRequest = reqName

	convID, err := protocol.NewConversationID(ec.SessionID())
	if err != nil {
		return err
	}

	history := make([]llm.Message, len(d.History))
	copy(history, d.History)

	ec.Send("generate", statecharts.SendOptions{
		Target: reqName,
		Type:   "llm",
		Data: dispatchPayload{
			ConversationID: convID,
			Request:        llm.GenerateRequest{History: history, Tools: staticToolDefs},
		},
	})
	return nil
})

func matchesPendingRequest(d *conversationModel, ec statecharts.ExecContext) bool {
	ev, _ := ec.Event()
	return d.PendingRequest != "" && ev.Origin == d.PendingRequest
}

func isToolCallReply(d *conversationModel, ec statecharts.ExecContext) bool {
	ev, _ := ec.Event()
	payload, ok := statecharts.Payload[*llmReplyPayload](ev)
	return ok && payload.Value.IsToolCall
}

func isTextReply(d *conversationModel, ec statecharts.ExecContext) bool {
	ev, _ := ec.Event()
	payload, ok := statecharts.Payload[*llmReplyPayload](ev)
	return ok && !payload.Value.IsToolCall
}

var recordPendingToolCall = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	payload, _ := statecharts.Payload[*llmReplyPayload](ev)
	if payload == nil {
		return nil
	}
	d.PendingToolName = payload.Value.ToolName
	d.PendingArgs = payload.Value.ToolArgs
	d.PendingCallID = protocol.CallID(d.PendingRequest) // one llmrequest per turn: reusing its name as the call id is unambiguous
	d.PendingRetries = 0
	return nil
})

var appendAssistantMessage = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	payload, _ := statecharts.Payload[*llmReplyPayload](ev)
	if payload != nil {
		d.History = append(d.History, llm.Message{Role: llm.RoleAssistant, Text: payload.Value.Text})
	}
	return nil
})

var clearPendingRequest = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	d.PendingRequest = ""
	return nil
})

var offerToolCall = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	if d.PendingToolName == "" {
		return nil
	}
	convID, err := protocol.NewConversationID(ec.SessionID())
	if err != nil {
		return err
	}
	ec.Send("offer", statecharts.SendOptions{
		Target: "toolregistry",
		Data: toolOffer{
			ConversationID: convID,
			Tool:           d.PendingToolName,
			CallID:         d.PendingCallID,
			Args:           d.PendingArgs,
		},
	})
	return nil
})

var scheduleRetryTimer = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	ec.Send("tool_offer_retry", statecharts.SendOptions{Delay: toolOfferRetryInterval})
	return nil
})

func retriesExhausted(d *conversationModel, ec statecharts.ExecContext) bool {
	return d.PendingRetries >= toolOfferMaxRetries
}

// failPendingToolCall gives up on a pending tool call after
// toolOfferMaxRetries: no executor ever claimed (or renewed) the lease for
// long enough to actually run it. It synthesizes a "tool_result" targeting
// this same conversation, carrying an error -- exactly the same event, same
// payload type, and same target a real POST .../tool-result produces (see
// http.go's handleToolResult) -- so it is handled by the ordinary
// "tool_result" transition below with no special-casing: the failure is
// recorded in History and the LLM gets a turn to react to it, same as any
// other tool error.
var failPendingToolCall = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	ec.Send("tool_result", statecharts.SendOptions{
		Target: statecharts.Identifier(ec.SessionID()),
		Data: &toolResultPayload{
			TypeName: "aiagent.tool_result",
			Value: ToolResultData{
				CallID: d.PendingCallID,
				Error: fmt.Sprintf("no executor claimed %q within %s; giving up",
					d.PendingToolName, toolOfferRetryInterval*time.Duration(toolOfferMaxRetries)),
			},
		},
	})
	return nil
})

// retryOfferAndReschedule re-offers a still-pending tool call to
// toolregistry (identical to offerToolCall) and arms the next retry tick --
// both folded into one action, since tool_offer_retry is now a targetless
// transition (see below) rather than a re-entry into awaiting_tool, so
// nothing else re-arms the timer.
var retryOfferAndReschedule = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	d.PendingRetries++
	if err := offerToolCall(ec); err != nil {
		return err
	}
	ec.Send("tool_offer_retry", statecharts.SendOptions{Delay: toolOfferRetryInterval})
	return nil
})

func matchesPendingCallID(d *conversationModel, ec statecharts.ExecContext) bool {
	ev, _ := ec.Event()
	payload, ok := statecharts.Payload[*toolResultPayload](ev)
	return ok && d.PendingCallID != "" && payload.Value.CallID == d.PendingCallID
}

var appendToolResultMessage = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	payload, _ := statecharts.Payload[*toolResultPayload](ev)
	if payload == nil {
		return nil
	}
	text := payload.Value.Output
	if payload.Value.Error != "" {
		text = fmt.Sprintf("error: %s", payload.Value.Error)
	}
	d.History = append(d.History, llm.Message{Role: llm.RoleTool, Text: text})
	return nil
})

var clearPendingToolCall = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	d.PendingToolName = ""
	d.PendingArgs = nil
	d.PendingCallID = ""
	d.PendingRetries = 0
	return nil
})

// replyWithCatchup answers a "catchup" request entirely from this actor's
// own already-rehydrated History -- ordinary actor communication, the same
// in-memory state a live turn already uses, never a direct Log read. It is
// attached to the outer "conversation" state as a targetless transition, so
// it runs (and answers correctly) no matter which child state is current.
var replyWithCatchup = statecharts.Action(func(d *conversationModel, ec statecharts.ExecContext) error {
	ev, _ := ec.Event()
	payload, ok := statecharts.Payload[*catchupRequestPayload](ev)
	if !ok {
		return nil
	}
	from := payload.Value.FromSeq
	if from < 0 || from > len(d.History) {
		from = 0
	}
	for i := from; i < len(d.History); i++ {
		m := d.History[i]
		ec.Send("catchup_message", statecharts.SendOptions{
			Target: statecharts.Identifier(payload.Value.Connection),
			Data:   catchupMessage{Seq: i + 1, Frame: protocol.MessageFrame{Role: mapRole(m.Role), Text: m.Text}},
		})
	}
	return nil
})

// ConversationKind is the chart kind name conversations are Registered and
// Spawned under.
const ConversationKind statecharts.Identifier = "conversation"

// BuildConversationChart returns the durable "conversation" chart: idle,
// waiting on a user message; thinking, with an in-flight llmrequest;
// awaiting_tool, offering a decided tool call to whichever client currently
// holds its executor lease, retrying every 10s until an answer comes back --
// or, after toolOfferMaxRetries with nobody ever claiming the lease long
// enough to actually run it, synthesizing a failure and giving the LLM a
// turn to react to it rather than waiting forever.
func BuildConversationChart() (*statecharts.Chart, error) {
	return statecharts.Build(
		statecharts.Compound("conversation", "idle",
			statecharts.Children(
				statecharts.Atomic("idle",
					statecharts.OnEntry(reportState(protocol.ConversationIdle)),
					statecharts.On("user_message",
						statecharts.Target("thinking"),
						statecharts.Then(appendUserMessage, broadcastLastMessage),
					),
				),
				statecharts.Atomic("thinking",
					statecharts.OnEntry(reportState(protocol.ConversationThinking), startRequest),
					statecharts.On("llm_reply",
						statecharts.Target("awaiting_tool"),
						statecharts.If(statecharts.Cond(func(d *conversationModel, ec statecharts.ExecContext) bool {
							return matchesPendingRequest(d, ec) && isToolCallReply(d, ec)
						})),
						statecharts.Then(recordPendingToolCall),
					),
					statecharts.On("llm_reply",
						statecharts.Target("idle"),
						statecharts.If(statecharts.Cond(func(d *conversationModel, ec statecharts.ExecContext) bool {
							return matchesPendingRequest(d, ec) && isTextReply(d, ec)
						})),
						statecharts.Then(appendAssistantMessage, broadcastLastMessage, clearPendingRequest),
					),
				),
				statecharts.Atomic("awaiting_tool",
					statecharts.OnEntry(reportState(protocol.ConversationAwaitingTool), offerToolCall, scheduleRetryTimer),
					statecharts.On("tool_offer_retry",
						statecharts.If(statecharts.Cond(retriesExhausted)),
						statecharts.Then(failPendingToolCall),
					),
					statecharts.On("tool_offer_retry", statecharts.Then(retryOfferAndReschedule)),
					statecharts.On("tool_result",
						statecharts.Target("thinking"),
						statecharts.If(statecharts.Cond(matchesPendingCallID)),
						statecharts.Then(appendToolResultMessage, broadcastLastMessage, clearPendingToolCall),
					),
				),
			),
			statecharts.On("catchup", statecharts.Then(replyWithCatchup)),
		),
		statecharts.WithNewDatamodel(func() any { return &conversationModel{} }),
	)
}
