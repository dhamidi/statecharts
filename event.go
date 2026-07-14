package statecharts

import "fmt"

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
// It mirrors the mandatory fields of an SCXML event (5.10.1). Data carries
// the payload as a plain Go value -- there is no expression language, so the
// datamodel touchpoints the spec defines (cond, assign, send params, etc.)
// become ordinary Go closures operating on the user's own datamodel value,
// with Data available through ExecContext during their evaluation.
type Event struct {
	Name       Identifier
	Type       EventType
	Data       any
	SendID     Identifier
	Origin     Identifier
	OriginType Identifier
	InvokeID   Identifier
	DeliveryID DeliveryID
}

// Payload type-asserts e.Data to T, the way callers are expected to recover
// a typed payload from a heterogeneous event queue.
func Payload[T any](e Event) (T, bool) {
	v, ok := e.Data.(T)
	return v, ok
}
