package statecharts

import (
	"fmt"
	"strings"
)

// DeliveryID is an opaque, stable identity for one external delivery.
// Applications must not interpret it; durable transports use it for inbox
// deduplication and as an idempotency key.
type DeliveryID string

// EventType classifies where an Event originated, per SCXML 5.10.1.
type EventType uint8

const (
	// EventExternal events arrived via the blocking external queue (an
	// application Send, or a message from another session/processor).
	EventExternal EventType = iota
	// EventInternal events were raised via <raise> or sent to "#_internal".
	EventInternal
	// EventPlatform events are placed by the interpreter itself, e.g.
	// error.execution, error.communication, done.state.<id>.
	EventPlatform
)

// String implements fmt.Stringer.
func (t EventType) String() string {
	switch t {
	case EventExternal:
		return "external"
	case EventInternal:
		return "internal"
	case EventPlatform:
		return "platform"
	default:
		return fmt.Sprintf("EventType(%d)", uint8(t))
	}
}

// Event is a message flowing through a chart's internal or external queue.
// It mirrors the mandatory fields of an SCXML event (5.10.1). Data is the
// canonical payload representation shared by datamodel, actor, process, and
// durability boundaries. Its zero value is canonical null.
type Event struct {
	Name       Identifier
	Type       EventType
	Data       Value
	SendID     Identifier
	Origin     Identifier
	OriginType Identifier
	InvokeID   Identifier
	DeliveryID DeliveryID
}

func cloneEvent(event Event) Event {
	event.Data = event.Data.Clone()
	return event
}

func cloneEvents(events []Event) []Event {
	clones := make([]Event, len(events))
	for i := range events {
		clones[i] = cloneEvent(events[i])
	}
	return clones
}

// PlatformErrorValueTag is the stable tag used for interpreter and platform
// errors that cross event, session, actor, or durability boundaries.
const PlatformErrorValueTag = "statecharts.error/v1"

// PlatformErrorValue converts err into canonical data with a stable
// classification and message. Invalid UTF-8 is replaced deterministically so
// an error can never make a durable event unencodable.
func PlatformErrorValue(classification Identifier, err error) Value {
	message := ""
	if err != nil {
		message = strings.ToValidUTF8(err.Error(), "\uFFFD")
	}
	classificationValue, _ := StringValue(strings.ToValidUTF8(string(classification), "\uFFFD"))
	messageValue, _ := StringValue(message)
	payload, _ := MapValue(map[string]Value{
		"classification": classificationValue,
		"message":        messageValue,
	})
	value, _ := TaggedValue(PlatformErrorValueTag, payload)
	return value
}

// PlatformErrorDetails extracts a canonical platform error.
func PlatformErrorDetails(value Value) (classification Identifier, message string, ok bool) {
	tag, payload, ok := value.AsTagged()
	if !ok || tag != PlatformErrorValueTag {
		return "", "", false
	}
	fields, ok := payload.AsMap()
	if !ok {
		return "", "", false
	}
	classificationText, classificationOK := fields["classification"].AsString()
	message, messageOK := fields["message"].AsString()
	if !classificationOK || !messageOK {
		return "", "", false
	}
	return Identifier(classificationText), message, true
}
