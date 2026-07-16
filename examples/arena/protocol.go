package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/dhamidi/statecharts"
)

const protocolVersion = 1

const (
	matchConfigTag      = "arena.match-config/v1"
	joinTag             = "arena.join/v1"
	disconnectTag       = "arena.disconnect/v1"
	subscriptionTag     = "arena.subscription/v1"
	inputTag            = "arena.input/v1"
	snapshotTag         = "arena.snapshot/v1"
	connectionConfigTag = "arena.connection-config/v1"
	botConfigTag        = "arena.bot-config/v1"
)

type matchConfig struct {
	Revision string `json:"revision"`
}

type joinRequest struct {
	Player string `json:"player"`
	Name   string `json:"name"`
	Color  string `json:"color"`
	Lease  string `json:"lease"`
	Bot    bool   `json:"bot"`
}

type disconnectRequest struct {
	Player string `json:"player"`
	Lease  string `json:"lease"`
}

type subscription struct {
	Target string `json:"target"`
}

type connectionConfig struct {
	Match  string `json:"match"`
	Player string `json:"player"`
	Name   string `json:"name"`
	Color  string `json:"color"`
	Output string `json:"output"`
}

type botConfig struct {
	Match  string `json:"match"`
	Player string `json:"player"`
	Name   string `json:"name"`
	Color  string `json:"color"`
}

type clientMessage struct {
	Version  int    `json:"v"`
	Type     string `json:"type"`
	Sequence uint64 `json:"seq"`
	Action   string `json:"action"`
}

type serverMessage struct {
	Version  int           `json:"v"`
	Type     string        `json:"type"`
	Player   string        `json:"player,omitempty"`
	Snapshot arenaSnapshot `json:"snapshot,omitempty"`
	Message  string        `json:"message,omitempty"`
}

func taggedStruct(tag string, value any) (statecharts.Value, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return statecharts.Value{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var shape any
	if err := decoder.Decode(&shape); err != nil {
		return statecharts.Value{}, err
	}
	payload, err := statecharts.ValueFromJSON(shape)
	if err != nil {
		return statecharts.Value{}, err
	}
	return statecharts.TaggedValue(tag, payload)
}

func decodeTaggedStruct(value statecharts.Value, tag string, destination any) error {
	actual, payload, ok := value.AsTagged()
	if !ok || actual != tag {
		return fmt.Errorf("expected tagged value %q", tag)
	}
	shape, err := ordinaryJSON(payload)
	if err != nil {
		return err
	}
	data, err := json.Marshal(shape)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode %s: %w", tag, err)
	}
	return nil
}

func ordinaryJSON(value statecharts.Value) (any, error) {
	switch value.Kind() {
	case statecharts.ValueKindNull:
		return nil, nil
	case statecharts.ValueKindBool:
		result, _ := value.AsBool()
		return result, nil
	case statecharts.ValueKindString:
		result, _ := value.AsString()
		return result, nil
	case statecharts.ValueKindNumber:
		if result, ok := value.AsInt64(); ok {
			return result, nil
		}
		result, _ := value.AsNumber()
		return json.Number(result), nil
	case statecharts.ValueKindList:
		values, _ := value.AsList()
		result := make([]any, len(values))
		for index, item := range values {
			converted, err := ordinaryJSON(item)
			if err != nil {
				return nil, err
			}
			result[index] = converted
		}
		return result, nil
	case statecharts.ValueKindMap:
		values, _ := value.AsMap()
		result := make(map[string]any, len(values))
		for key, item := range values {
			converted, err := ordinaryJSON(item)
			if err != nil {
				return nil, err
			}
			result[key] = converted
		}
		return result, nil
	default:
		return nil, fmt.Errorf("nested %s cannot be converted to protocol JSON", value.Kind())
	}
}

func encodeServerMessage(message serverMessage) (statecharts.Value, error) {
	message.Version = protocolVersion
	data, err := json.Marshal(message)
	if err != nil {
		return statecharts.Value{}, err
	}
	return statecharts.StringValue(string(data))
}

func parseClientMessage(value statecharts.Value) (clientMessage, error) {
	text, ok := value.AsString()
	if !ok {
		return clientMessage{}, fmt.Errorf("client frame is not text")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(text))
	decoder.DisallowUnknownFields()
	var message clientMessage
	if err := decoder.Decode(&message); err != nil {
		return clientMessage{}, fmt.Errorf("decode client frame: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return clientMessage{}, fmt.Errorf("client frame contains trailing data")
	}
	if message.Version != protocolVersion || message.Type != "input" || message.Sequence == 0 {
		return clientMessage{}, fmt.Errorf("invalid client envelope")
	}
	switch message.Action {
	case actionUp, actionDown, actionLeft, actionRight, actionShoot:
		return message, nil
	default:
		return clientMessage{}, fmt.Errorf("unknown action %q", message.Action)
	}
}
