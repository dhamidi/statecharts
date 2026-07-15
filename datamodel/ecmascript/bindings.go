package ecmascript

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/dhamidi/statecharts"
)

func (s *session) eventWire() (string, error) {
	context, err := s.execContext()
	if err != nil {
		return "", err
	}
	event, ok := context.Event()
	if !ok {
		return "", nil
	}
	fields := map[string]statecharts.Value{"data": event.Data}
	for _, field := range []struct{ key, text string }{
		{"name", string(event.Name)}, {"type", event.Type.String()},
		{"sendid", string(event.SendID)}, {"origin", string(event.Origin)},
		{"origintype", string(event.OriginType)}, {"invokeid", string(event.InvokeID)},
	} {
		fields[field.key], err = statecharts.StringValue(field.text)
		if err != nil {
			return "", fmt.Errorf("ecmascript: event field %s: %w", field.key, err)
		}
	}
	value, err := mapValue(fields)
	if err != nil {
		return "", err
	}
	return marshalValue(value)
}

func (s *session) ioProcessorsWire() (string, error) {
	context, err := s.execContext()
	if err != nil {
		return "", err
	}
	processors := context.IOProcessors()
	values := make([]statecharts.Value, len(processors))
	for i, processor := range processors {
		typ, err := statecharts.StringValue(string(processor.Type))
		if err != nil {
			return "", fmt.Errorf("ecmascript: I/O processor %d type: %w", i, err)
		}
		location, err := statecharts.StringValue(processor.Location.String())
		if err != nil {
			return "", fmt.Errorf("ecmascript: I/O processor %d location: %w", i, err)
		}
		values[i], err = mapValue(map[string]statecharts.Value{
			"type":     typ,
			"location": location,
		})
		if err != nil {
			return "", err
		}
	}
	return marshalValue(statecharts.ListValue(values))
}

func (s *session) platformWire() (string, error) {
	context, err := s.execContext()
	if err != nil {
		return "", err
	}
	values := context.PlatformVariables()
	converted := make(map[string]statecharts.Value, len(values))
	for key, value := range values {
		converted[key], err = canonicalValue(value)
		if err != nil {
			return "", fmt.Errorf("ecmascript: platform value %q: %w", key, err)
		}
	}
	result, err := statecharts.MapValue(converted)
	if err != nil {
		return "", err
	}
	return marshalValue(result)
}

func canonicalValue(input any) (statecharts.Value, error) {
	if value, ok := input.(statecharts.Value); ok {
		return value.Clone(), nil
	}
	wire, err := json.Marshal(input)
	if err != nil {
		return statecharts.Value{}, fmt.Errorf("cannot encode %T: %w", input, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return statecharts.Value{}, fmt.Errorf("cannot decode %T: %w", input, err)
	}
	return statecharts.ValueFromJSON(decoded)
}

func mapValue(values map[string]statecharts.Value) (statecharts.Value, error) {
	return statecharts.MapValue(values)
}

func marshalValue(value statecharts.Value) (string, error) {
	wire, err := value.MarshalBinary()
	if err != nil {
		return "", err
	}
	return string(wire), nil
}
