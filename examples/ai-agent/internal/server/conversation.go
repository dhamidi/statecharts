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

// toolOfferRetrySendID is the fixed, deterministic SendID every delayed
// "tool_offer_retry" self-send is armed under (see scheduleRetryTimer and
// retryOfferAndReschedule). Only one such timer is ever meant to be
// outstanding at a time per conversation instance, so it doesn't need to
// vary per tool-call cycle -- a fixed ID lets cancelRetryTimer reliably
// reach whichever one is currently pending when awaiting_tool is left,
// regardless of which action last (re)armed it. Without this, the
// interpreter would auto-generate a fresh, uncancelable "send.N" id (see
// interpreter.go's doSend) for every arm, and a stale timer from a
// finished tool-call cycle could fire against a later cycle's
// awaiting_tool -- see cancelRetryTimer.
const toolOfferRetrySendID statecharts.Identifier = "tool_offer_retry"

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

func reportState(state protocol.ConversationState, ec statecharts.ExecContext) error {
	convID, err := protocol.NewConversationID(ec.SessionID())
	if err != nil {
		return err
	}
	ec.Send("state_changed", statecharts.SendOptions{
		Target: "user",
		Data:   encodeConversationState(ConversationStateData{ID: convID, State: state}),
	})
	return nil
}

func reportIdle(_ *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	return reportState(protocol.ConversationIdle, ec)
}

func reportThinking(_ *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	return reportState(protocol.ConversationThinking, ec)
}

func reportAwaitingTool(_ *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	return reportState(protocol.ConversationAwaitingTool, ec)
}

// broadcastLastMessage sends the just-appended History entry (assumed to
// be the last thing the preceding action in the same Then(...) list added)
// to fanout as a durable "message" frame, numbered by its position in
// History -- the same numbering replyWithCatchup uses for the same entry,
// so live and catch-up delivery of the same entry always agree.
func broadcastLastMessage(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
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
		Data: encodeFanoutBroadcast(fanoutBroadcast{
			ConversationID: convID,
			Kind:           "message",
			Seq:            len(d.History),
			Message:        protocol.MessageFrame{Role: mapRole(last.Role), Text: last.Text},
		}),
	})
	return nil
}

func appendUserMessage(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	payload, ok := decodeUserMessage(ev.Data)
	if ok {
		d.History = append(d.History, llm.Message{Role: llm.RoleUser, Text: payload.Text})
	}
	return nil
}

func startRequest(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
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
		Data: encodeDispatch(dispatchPayload{
			ConversationID: convID,
			Request:        llm.GenerateRequest{History: history, Tools: staticToolDefs},
		}),
	})
	return nil
}

func matchesPendingRequest(d *conversationModel, ec statecharts.ExecContext) bool {
	ev, _ := ec.Event()
	return d.PendingRequest != "" && ev.Origin == d.PendingRequest
}

func isToolCallReply(d *conversationModel, ec statecharts.ExecContext) bool {
	ev, _ := ec.Event()
	payload, ok := decodeLLMReply(ev.Data)
	return ok && payload.IsToolCall
}

func isTextReply(d *conversationModel, ec statecharts.ExecContext) bool {
	ev, _ := ec.Event()
	payload, ok := decodeLLMReply(ev.Data)
	return ok && !payload.IsToolCall
}

func isPendingToolCallReply(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) (bool, error) {
	return matchesPendingRequest(d, ec) && isToolCallReply(d, ec), nil
}

func isPendingTextReply(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) (bool, error) {
	return matchesPendingRequest(d, ec) && isTextReply(d, ec), nil
}

func recordPendingToolCall(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	payload, ok := decodeLLMReply(ev.Data)
	if !ok {
		return nil
	}
	d.PendingToolName = payload.ToolName
	d.PendingArgs = payload.ToolArgs
	d.PendingCallID = protocol.CallID(d.PendingRequest) // one llmrequest per turn: reusing its name as the call id is unambiguous
	d.PendingRetries = 0
	return nil
}

func appendAssistantMessage(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	payload, ok := decodeLLMReply(ev.Data)
	if ok {
		d.History = append(d.History, llm.Message{Role: llm.RoleAssistant, Text: payload.Text})
	}
	return nil
}

func clearPendingRequest(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	d.PendingRequest = ""
	return nil
}

func offerToolCall(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	if d.PendingToolName == "" {
		return nil
	}
	convID, err := protocol.NewConversationID(ec.SessionID())
	if err != nil {
		return err
	}
	ec.Send("offer", statecharts.SendOptions{
		Target: "toolregistry",
		Data: encodeToolOffer(toolOffer{
			ConversationID: convID,
			Tool:           d.PendingToolName,
			CallID:         d.PendingCallID,
			Args:           d.PendingArgs,
		}),
	})
	return nil
}

func scheduleRetryTimer(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ec.Send("tool_offer_retry", statecharts.SendOptions{Delay: toolOfferRetryInterval, SendID: toolOfferRetrySendID})
	return nil
}

// cancelRetryTimer best-effort cancels the delayed "tool_offer_retry" send
// currently armed under toolOfferRetrySendID, if any. It belongs in every
// Then(...) list on a transition that leaves awaiting_tool, so a timer
// armed by cycle A's scheduleRetryTimer/retryOfferAndReschedule can never
// survive to fire against a later cycle B's awaiting_tool (where it would
// otherwise increment cycle B's PendingRetries, or -- if it coincided with
// retriesExhausted -- synthesize a bogus failure against cycle B's real,
// possibly still in-flight, PendingCallID). ec.Cancel is a no-op for an
// unknown or already-fired SendID (see interpreter.go's doCancel), so
// including this unconditionally is always safe, including the first time
// awaiting_tool is ever left (nothing to cancel yet).
func cancelRetryTimer(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ec.Cancel(toolOfferRetrySendID)
	return nil
}

func retriesExhausted(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) (bool, error) {
	return d.PendingRetries >= toolOfferMaxRetries, nil
}

// failPendingToolCall gives up on a pending tool call after
// toolOfferMaxRetries: no executor ever claimed (or renewed) the lease for
// long enough to actually run it. It synthesizes a "tool_result" targeting
// this same conversation, carrying an error -- exactly the same event, same
// payload type, and same target a real POST .../tool-result produces (see
// http.go's handleToolResult) -- so it is handled by the ordinary
// "tool_result" transition below with no special-casing: the failure is
// recorded in History and the LLM gets a turn to react to it, same as any
// other tool error. It runs from the "tool_offer_retry" tick that just
// fired (whose own pending record is already gone from ip.pending by the
// time an action can observe it -- see interpreter.go's fireTimer), so
// there is no armed timer to cancel here; the actual exit from
// awaiting_tool happens once the synthetic "tool_result" it sends is
// processed below, where cancelRetryTimer already runs.
func failPendingToolCall(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ec.Send("tool_result", statecharts.SendOptions{
		Target: statecharts.Identifier(ec.SessionID()),
		Data: encodeToolResult(ToolResultData{
			CallID: d.PendingCallID,
			Error:  fmt.Sprintf("no executor claimed %q within %s; giving up", d.PendingToolName, toolOfferRetryInterval*time.Duration(toolOfferMaxRetries)),
		}),
	})
	return nil
}

// retryOfferAndReschedule re-offers a still-pending tool call to
// toolregistry (identical to offerToolCall) and arms the next retry tick --
// both folded into one action, since tool_offer_retry is now a targetless
// transition (see below) rather than a re-entry into awaiting_tool, so
// nothing else re-arms the timer.
//
// Known follow-up: this counts every tick as a retry, even one where
// toolregistry's lease for PendingToolName is currently held and the
// in-flight call simply hasn't answered yet (see toolregistry.go's
// toolLease.DeliveredCallID, which distinguishes "already handed to a
// still-current owner, maybe still running" from "nobody has the lease" --
// but only inside toolregistry's own datamodel). A genuinely slow-but-alive
// tool call can therefore still hit toolOfferMaxRetries and be killed by
// failPendingToolCall's synthetic timeout, which is meant for "nobody ever
// claimed the lease" rather than "someone's running it and it's slow".
// Fixing that cleanly needs toolregistry to answer a query about whether
// PendingCallID was actually delivered/claimed, which is a new
// cross-actor request/reply this actor doesn't have today -- left as a
// follow-up rather than folded in here.
func retryOfferAndReschedule(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	d.PendingRetries++
	if err := offerToolCall(d, ec, nil); err != nil {
		return err
	}
	ec.Send("tool_offer_retry", statecharts.SendOptions{Delay: toolOfferRetryInterval, SendID: toolOfferRetrySendID})
	return nil
}

func matchesPendingCallID(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) (bool, error) {
	ev, _ := ec.Event()
	payload, ok := decodeToolResult(ev.Data)
	return ok && d.PendingCallID != "" && payload.CallID == d.PendingCallID, nil
}

func appendToolResultMessage(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	payload, ok := decodeToolResult(ev.Data)
	if !ok {
		return nil
	}
	text := payload.Output
	if payload.Error != "" {
		text = fmt.Sprintf("error: %s", payload.Error)
	}
	d.History = append(d.History, llm.Message{Role: llm.RoleTool, Text: text})
	return nil
}

func clearPendingToolCall(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	d.PendingToolName = ""
	d.PendingArgs = nil
	d.PendingCallID = ""
	d.PendingRetries = 0
	return nil
}

// replyWithCatchup answers a "catchup" request entirely from this actor's
// own already-rehydrated History -- ordinary actor communication, the same
// in-memory state a live turn already uses, never a direct Log read. It is
// attached to the outer "conversation" state as a targetless transition, so
// it runs (and answers correctly) no matter which child state is current.
func replyWithCatchup(d *conversationModel, ec statecharts.ExecContext, _ []statecharts.Value) error {
	ev, _ := ec.Event()
	payload, ok := decodeCatchupRequest(ev.Data)
	if !ok {
		return nil
	}
	from := payload.FromSeq
	if from < 0 || from > len(d.History) {
		from = 0
	}
	for i := from; i < len(d.History); i++ {
		m := d.History[i]
		ec.Send("catchup_message", statecharts.SendOptions{
			Target: statecharts.Identifier(payload.Connection),
			Data:   encodeCatchupMessage(catchupMessage{Seq: i + 1, Frame: protocol.MessageFrame{Role: mapRole(m.Role), Text: m.Text}}),
		})
	}
	return nil
}

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
	model := statecharts.NewGoModel(func() *conversationModel { return &conversationModel{} })
	actions := map[string]func(*conversationModel, statecharts.ExecContext, []statecharts.Value) error{
		"report-idle":                reportIdle,
		"report-thinking":            reportThinking,
		"report-awaiting-tool":       reportAwaitingTool,
		"broadcast-last-message":     broadcastLastMessage,
		"append-user-message":        appendUserMessage,
		"start-request":              startRequest,
		"record-pending-tool-call":   recordPendingToolCall,
		"append-assistant-message":   appendAssistantMessage,
		"clear-pending-request":      clearPendingRequest,
		"offer-tool-call":            offerToolCall,
		"schedule-retry-timer":       scheduleRetryTimer,
		"cancel-retry-timer":         cancelRetryTimer,
		"fail-pending-tool-call":     failPendingToolCall,
		"retry-offer-and-reschedule": retryOfferAndReschedule,
		"append-tool-result-message": appendToolResultMessage,
		"clear-pending-tool-call":    clearPendingToolCall,
		"reply-with-catchup":         replyWithCatchup,
	}
	actionRefs := make(map[string]statecharts.GoActionRef, len(actions))
	for operation, fn := range actions {
		ref, err := model.Action(statecharts.Identifier("ai-agent.server.conversation."+operation), "v1", fn)
		if err != nil {
			return nil, err
		}
		actionRefs[operation] = ref
	}
	conditions := map[string]func(*conversationModel, statecharts.ExecContext, []statecharts.Value) (bool, error){
		"is-pending-tool-call-reply": isPendingToolCallReply,
		"is-pending-text-reply":      isPendingTextReply,
		"retries-exhausted":          retriesExhausted,
		"matches-pending-call-id":    matchesPendingCallID,
	}
	conditionRefs := make(map[string]statecharts.GoConditionRef, len(conditions))
	for operation, fn := range conditions {
		ref, err := model.Condition(statecharts.Identifier("ai-agent.server.conversation."+operation), "v1", fn)
		if err != nil {
			return nil, err
		}
		conditionRefs[operation] = ref
	}

	return buildCanonicalChart(
		statecharts.Compound("conversation", "idle",
			statecharts.Children(
				statecharts.Atomic("idle",
					statecharts.OnEntry(actionRefs["report-idle"].Do()),
					statecharts.On("user_message",
						statecharts.Target("thinking"),
						statecharts.Then(actionRefs["append-user-message"].Do(), actionRefs["broadcast-last-message"].Do()),
					),
				),
				statecharts.Atomic("thinking",
					statecharts.OnEntry(actionRefs["report-thinking"].Do(), actionRefs["start-request"].Do()),
					statecharts.On("llm_reply",
						statecharts.Target("awaiting_tool"),
						statecharts.If(conditionRefs["is-pending-tool-call-reply"].If()),
						statecharts.Then(actionRefs["record-pending-tool-call"].Do()),
					),
					statecharts.On("llm_reply",
						statecharts.Target("idle"),
						statecharts.If(conditionRefs["is-pending-text-reply"].If()),
						statecharts.Then(actionRefs["append-assistant-message"].Do(), actionRefs["broadcast-last-message"].Do(), actionRefs["clear-pending-request"].Do()),
					),
				),
				statecharts.Atomic("awaiting_tool",
					statecharts.OnEntry(actionRefs["report-awaiting-tool"].Do(), actionRefs["offer-tool-call"].Do(), actionRefs["schedule-retry-timer"].Do()),
					statecharts.On("tool_offer_retry",
						statecharts.If(conditionRefs["retries-exhausted"].If()),
						statecharts.Then(actionRefs["fail-pending-tool-call"].Do()),
					),
					statecharts.On("tool_offer_retry", statecharts.Then(actionRefs["retry-offer-and-reschedule"].Do())),
					statecharts.On("tool_result",
						statecharts.Target("thinking"),
						statecharts.If(conditionRefs["matches-pending-call-id"].If()),
						statecharts.Then(actionRefs["cancel-retry-timer"].Do(), actionRefs["append-tool-result-message"].Do(), actionRefs["broadcast-last-message"].Do(), actionRefs["clear-pending-tool-call"].Do()),
					),
				),
			),
			statecharts.On("catchup", statecharts.Then(actionRefs["reply-with-catchup"].Do())),
		),
		model, statecharts.WithRevisionSalt("conversation-v1"))
}
