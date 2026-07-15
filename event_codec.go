package statecharts

import "fmt"

// EncodedEvent is the flat, JSON/SQL-safe encoding of an Event, used by
// Snapshot's JSON envelope and durable stores. Data is always the canonical
// Value marshal representation, including for null.
type EncodedEvent struct {
	Name       Identifier `json:"name"`
	Type       EventType  `json:"type"`
	SendID     Identifier `json:"send_id,omitempty"`
	Origin     Identifier `json:"origin,omitempty"`
	OriginType Identifier `json:"origin_type,omitempty"`
	InvokeID   Identifier `json:"invoke_id,omitempty"`
	DeliveryID DeliveryID `json:"delivery_id,omitempty"`
	Data       []byte     `json:"data"`
}

// EncodeEvent flattens ev using Value's canonical, versioned bytes.
func EncodeEvent(ev Event) (EncodedEvent, error) {
	payload, err := ev.Data.MarshalBinary()
	if err != nil {
		return EncodedEvent{}, fmt.Errorf("statecharts: encode Event.Data: %w", err)
	}
	return EncodedEvent{
		Name: ev.Name, Type: ev.Type, SendID: ev.SendID, Origin: ev.Origin,
		OriginType: ev.OriginType, InvokeID: ev.InvokeID,
		DeliveryID: ev.DeliveryID, Data: payload,
	}, nil
}

// DecodeEvent reconstructs an Event from canonical Value bytes.
func DecodeEvent(enc EncodedEvent) (Event, error) {
	var data Value
	if err := data.UnmarshalBinary(enc.Data); err != nil {
		return Event{}, fmt.Errorf("statecharts: decode Event.Data: %w", err)
	}
	return Event{
		Name: enc.Name, Type: enc.Type, Data: data, SendID: enc.SendID,
		Origin: enc.Origin, OriginType: enc.OriginType, InvokeID: enc.InvokeID,
		DeliveryID: enc.DeliveryID,
	}, nil
}
