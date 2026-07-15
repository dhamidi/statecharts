package statecharts

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestValueScalarCanonicalEncodingRoundTrip(t *testing.T) {
	number, err := NumberValue("-123.4500e+2")
	if err != nil {
		t.Fatalf("NumberValue: %v", err)
	}
	values := []Value{
		{},
		NullValue(),
		BoolValue(false),
		BoolValue(true),
		StringValue(""),
		StringValue("hello, 世界"),
		number,
		Int64Value(math.MinInt64),
		Uint64Value(math.MaxUint64),
		ListValue(nil),
		MapValue(nil),
	}

	for _, want := range values {
		binary, err := want.MarshalBinary()
		if err != nil {
			t.Fatalf("%v MarshalBinary: %v", want.Kind(), err)
		}
		text, err := want.MarshalText()
		if err != nil {
			t.Fatalf("%v MarshalText: %v", want.Kind(), err)
		}
		if !bytes.Equal(binary, text) {
			t.Fatalf("%v binary encoding %q differs from text encoding %q", want.Kind(), binary, text)
		}

		var fromBinary Value
		if err := fromBinary.UnmarshalBinary(binary); err != nil {
			t.Fatalf("%v UnmarshalBinary: %v", want.Kind(), err)
		}
		if !fromBinary.Equal(want) {
			t.Fatalf("binary round trip = %#v, want %#v", fromBinary, want)
		}

		var fromText Value
		if err := fromText.UnmarshalText(text); err != nil {
			t.Fatalf("%v UnmarshalText: %v", want.Kind(), err)
		}
		if !fromText.Equal(want) {
			t.Fatalf("text round trip = %#v, want %#v", fromText, want)
		}
	}
}

func TestValueNestedAndTaggedRoundTrip(t *testing.T) {
	count := Int64Value(3)
	payload := MapValue(map[string]Value{
		"enabled": BoolValue(true),
		"items": ListValue([]Value{
			StringValue("first"),
			count,
			NullValue(),
		}),
	})
	want, err := TaggedValue("example.order/v1", payload)
	if err != nil {
		t.Fatalf("TaggedValue: %v", err)
	}

	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var got Value
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}

	tag, taggedPayload, ok := got.AsTagged()
	if !ok || tag != "example.order/v1" || !taggedPayload.Equal(payload) {
		t.Fatalf("AsTagged = (%q, %#v, %t), want tag and original payload", tag, taggedPayload, ok)
	}
}

func TestValueNumbersAreExactAndRejectUnsupportedForms(t *testing.T) {
	cases := map[string]string{
		"0":                      "0",
		"-0":                     "0",
		"1.0":                    "1",
		"10":                     "1e1",
		"1e1":                    "1e1",
		"-123.4500e+2":           "-12345",
		"9223372036854775807":    "9223372036854775807",
		"18446744073709551615":   "18446744073709551615",
		"18446744073709551616":   "18446744073709551616",
		"1e99999999999999999999": "1e99999999999999999999",
	}
	for input, want := range cases {
		value, err := NumberValue(input)
		if err != nil {
			t.Errorf("NumberValue(%q): %v", input, err)
			continue
		}
		got, ok := value.AsNumber()
		if !ok || got != want {
			t.Errorf("NumberValue(%q).AsNumber() = (%q, %t), want (%q, true)", input, got, ok, want)
		}
	}

	for _, input := range []string{"", "+1", "01", "-01", ".1", "1.", "1e", "NaN", "Inf", "-Inf"} {
		if _, err := NumberValue(input); err == nil {
			t.Errorf("NumberValue(%q) succeeded, want error", input)
		}
	}
	for _, input := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := Float64Value(input); err == nil {
			t.Errorf("Float64Value(%v) succeeded, want error", input)
		}
	}

	fromJSON, err := ValueFromJSON(json.Number("18446744073709551616"))
	if err != nil {
		t.Fatalf("ValueFromJSON exact number: %v", err)
	}
	if got, _ := fromJSON.AsNumber(); got != "18446744073709551616" {
		t.Fatalf("ValueFromJSON number = %q, want exact integer", got)
	}
}

func TestValueMapEncodingIsDeterministic(t *testing.T) {
	left := MapValue(map[string]Value{
		"z": Int64Value(1),
		"a": StringValue("first"),
		"m": BoolValue(true),
	})
	right := MapValue(map[string]Value{
		"m": BoolValue(true),
		"z": Int64Value(1),
		"a": StringValue("first"),
	})

	leftBytes, err := left.MarshalBinary()
	if err != nil {
		t.Fatalf("left MarshalBinary: %v", err)
	}
	rightBytes, err := right.MarshalBinary()
	if err != nil {
		t.Fatalf("right MarshalBinary: %v", err)
	}
	if !bytes.Equal(leftBytes, rightBytes) {
		t.Fatalf("canonical revision material differs by insertion order:\nleft:  %s\nright: %s", leftBytes, rightBytes)
	}
}

func TestValueConstructorsCloneAndAccessorsDoNotLeakAliases(t *testing.T) {
	nestedMap := map[string]Value{"name": StringValue("before")}
	mapValue := MapValue(nestedMap)
	items := []Value{mapValue}
	original := ListValue(items)

	nestedMap["name"] = StringValue("after")
	items[0] = NullValue()
	gotItems, ok := original.AsList()
	if !ok || len(gotItems) != 1 {
		t.Fatalf("AsList = (%#v, %t), want one item", gotItems, ok)
	}
	gotMap, ok := gotItems[0].AsMap()
	if !ok {
		t.Fatalf("nested AsMap ok = false")
	}
	if got, _ := gotMap["name"].AsString(); got != "before" {
		t.Fatalf("nested name = %q, want constructor-isolated value", got)
	}

	clone := original.Clone()
	clone.list[0].object["name"] = StringValue("clone mutation")
	originalItems, _ := original.AsList()
	originalMap, _ := originalItems[0].AsMap()
	if got, _ := originalMap["name"].AsString(); got != "before" {
		t.Fatalf("original name after clone mutation = %q, want %q", got, "before")
	}

	gotMap["name"] = StringValue("accessor mutation")
	again, _ := original.AsList()
	againMap, _ := again[0].AsMap()
	if got, _ := againMap["name"].AsString(); got != "before" {
		t.Fatalf("original name after accessor mutation = %q, want %q", got, "before")
	}
}

func TestValueWireRejectsMalformedUnionsAndUnknownKinds(t *testing.T) {
	tests := []struct {
		name string
		wire string
		want string
	}{
		{"unknown version", `{"version":2,"kind":"null"}`, "unsupported Value wire version 2"},
		{"unknown kind", `{"version":1,"kind":"future"}`, `unknown Value wire kind "future"`},
		{"missing bool", `{"version":1,"kind":"bool"}`, "bool wire value requires bool"},
		{"wrong union field", `{"version":1,"kind":"bool","string":"wrong"}`, "bool wire value requires bool"},
		{"empty tag", `{"version":1,"kind":"tagged","tag":"","payload":{"version":1,"kind":"null"}}`, "non-empty tag"},
		{"missing tagged payload", `{"version":1,"kind":"tagged","tag":"example"}`, "requires payload"},
		{"unknown field", `{"version":1,"kind":"null","extra":true}`, "unknown field"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var value Value
			err := value.UnmarshalText([]byte(tt.wire))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("UnmarshalText error = %v, want containing %q", err, tt.want)
			}
		})
	}

	text := "wrong"
	_, err := ValueFromWire(ValueWire{
		Version: ValueWireVersion,
		Kind:    ValueKindBool,
		String:  &text,
	})
	if err == nil || !strings.Contains(err.Error(), "bool wire value requires bool") {
		t.Fatalf("ValueFromWire malformed union error = %v", err)
	}

	unchanged := StringValue("keep me")
	if err := unchanged.UnmarshalText([]byte(`{"version":1,"kind":"future"}`)); err == nil {
		t.Fatal("unknown wire kind succeeded, want error")
	}
	if got, ok := unchanged.AsString(); !ok || got != "keep me" {
		t.Fatalf("failed decode mutated receiver to (%q, %t)", got, ok)
	}

	if _, err := TaggedValue("", NullValue()); err == nil {
		t.Fatal("TaggedValue with empty tag succeeded, want error")
	}
}

func TestValueJSONShapedConversionsAreIsolated(t *testing.T) {
	input := map[string]any{
		"count": json.Number("9007199254740993"),
		"items": []any{"one", true, nil},
	}
	value, err := ValueFromJSON(input)
	if err != nil {
		t.Fatalf("ValueFromJSON: %v", err)
	}
	input["count"] = json.Number("1")
	input["items"].([]any)[0] = "changed"

	output, err := value.JSONValue()
	if err != nil {
		t.Fatalf("JSONValue: %v", err)
	}
	object := output.(map[string]any)
	if got := object["count"].(json.Number).String(); got != "9007199254740993" {
		t.Fatalf("count = %q, want exact original integer", got)
	}
	if got := object["items"].([]any)[0]; got != "one" {
		t.Fatalf("first item = %v, want isolated original", got)
	}

	tagged, err := TaggedValue("example", NullValue())
	if err != nil {
		t.Fatalf("TaggedValue: %v", err)
	}
	if _, err := tagged.JSONValue(); err == nil {
		t.Fatal("tagged JSONValue succeeded, want explicit non-JSON error")
	}
	if _, err := ValueFromJSON(make(chan int)); err == nil {
		t.Fatal("ValueFromJSON unsupported capability succeeded, want error")
	}
}

func TestValueZeroValueIsNullAndAccessorsAreKindChecked(t *testing.T) {
	var value Value
	if value.Kind() != ValueKindNull || !value.Equal(NullValue()) {
		t.Fatalf("zero Value kind = %v, want null", value.Kind())
	}
	if _, ok := value.AsBool(); ok {
		t.Fatal("null AsBool ok = true")
	}
	if _, ok := BoolValue(true).AsString(); ok {
		t.Fatal("bool AsString ok = true")
	}
}
