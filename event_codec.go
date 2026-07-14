package statecharts

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

const errorDataType = "statecharts.error"

// DataMarshaler lets a concrete Event.Data type control its own persisted
// encoding. There is no expression language or generic datamodel to
// introspect -- Data is an arbitrary Go value -- so persistence needs an
// explicit contract rather than reflection.
type DataMarshaler interface {
	MarshalData() (typeName string, payload []byte, err error)
}

// DataUnmarshaler is the decode half of DataMarshaler.
type DataUnmarshaler interface {
	UnmarshalData(payload []byte) error
}

var (
	dataRegistryMu sync.RWMutex
	dataRegistry   = map[string]func() DataUnmarshaler{}
)

// RegisterDataType associates typeName (as produced by some value's
// MarshalData) with factory, so DecodeEvent can reconstruct a value of that
// type. Call this at program startup for every concrete Data type that will
// ever be persisted (in a Snapshot or a Log).
func RegisterDataType(typeName string, factory func() DataUnmarshaler) {
	dataRegistryMu.Lock()
	defer dataRegistryMu.Unlock()
	dataRegistry[typeName] = factory
}

// EncodedEvent is the flat, JSON/SQL-safe encoding of an Event, used by
// Snapshot's JSON envelope and by the sqllog backend.
type EncodedEvent struct {
	Name        Identifier `json:"name"`
	Type        EventType  `json:"type"`
	SendID      Identifier `json:"send_id,omitempty"`
	Origin      Identifier `json:"origin,omitempty"`
	OriginType  Identifier `json:"origin_type,omitempty"`
	InvokeID    Identifier `json:"invoke_id,omitempty"`
	DataType    string     `json:"data_type,omitempty"`
	DataPayload []byte     `json:"data_payload,omitempty"`
}

// EncodeEvent flattens ev, delegating a non-nil ev.Data to its
// DataMarshaler implementation. A non-nil Data that does not implement
// DataMarshaler is an error: there is no reflection-based fallback, since
// silently dropping payload data would break replay fidelity.
func EncodeEvent(ev Event) (EncodedEvent, error) {
	enc := EncodedEvent{
		Name: ev.Name, Type: ev.Type,
		SendID: ev.SendID, Origin: ev.Origin, OriginType: ev.OriginType, InvokeID: ev.InvokeID,
	}
	if ev.Data == nil {
		return enc, nil
	}
	if m, ok := ev.Data.(DataMarshaler); ok {
		typeName, payload, err := m.MarshalData()
		if err != nil {
			return EncodedEvent{}, fmt.Errorf("statecharts: MarshalData: %w", err)
		}
		enc.DataType = typeName
		enc.DataPayload = payload
		return enc, nil
	}
	if err, ok := ev.Data.(error); ok {
		enc.DataType = errorDataType
		enc.DataPayload = []byte(err.Error())
		return enc, nil
	}
	return EncodedEvent{}, fmt.Errorf("statecharts: Event.Data of type %T does not implement DataMarshaler", ev.Data)
}

// DecodeEvent reconstructs an Event from its flat encoding, looking up
// enc.DataType in the registry populated by RegisterDataType.
func DecodeEvent(enc EncodedEvent) (Event, error) {
	ev := Event{
		Name: enc.Name, Type: enc.Type,
		SendID: enc.SendID, Origin: enc.Origin, OriginType: enc.OriginType, InvokeID: enc.InvokeID,
	}
	if enc.DataType == "" {
		return ev, nil
	}
	if enc.DataType == errorDataType {
		ev.Data = errors.New(string(enc.DataPayload))
		return ev, nil
	}
	dataRegistryMu.RLock()
	factory, ok := dataRegistry[enc.DataType]
	dataRegistryMu.RUnlock()
	if !ok {
		return Event{}, fmt.Errorf("statecharts: no registered data type %q (call RegisterDataType at startup)", enc.DataType)
	}
	v := factory()
	if err := v.UnmarshalData(enc.DataPayload); err != nil {
		return Event{}, fmt.Errorf("statecharts: UnmarshalData(%q): %w", enc.DataType, err)
	}
	ev.Data = v
	return ev, nil
}

// JSONData is a convenience DataMarshaler/DataUnmarshaler implementation
// backed by encoding/json, for callers who don't want to hand-write a
// custom encoding. TypeName must match between the value produced by
// NewJSONData and the corresponding RegisterDataType call.
type JSONData[T any] struct {
	TypeName string
	Value    T
}

// NewJSONData constructs a JSONData wrapper for value, tagged with typeName.
func NewJSONData[T any](typeName string, value T) JSONData[T] {
	return JSONData[T]{TypeName: typeName, Value: value}
}

// MarshalData implements DataMarshaler.
func (d JSONData[T]) MarshalData() (string, []byte, error) {
	b, err := json.Marshal(d.Value)
	return d.TypeName, b, err
}

// UnmarshalData implements DataUnmarshaler.
func (d *JSONData[T]) UnmarshalData(payload []byte) error {
	return json.Unmarshal(payload, &d.Value)
}
