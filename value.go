package statecharts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"unicode/utf8"
)

// ValueKind identifies one of the closed set of canonical Value variants.
type ValueKind string

const (
	// ValueKindNull is the null variant. It is also the kind of a zero Value.
	ValueKindNull ValueKind = "null"
	// ValueKindBool is the boolean variant.
	ValueKindBool ValueKind = "bool"
	// ValueKindString is the UTF-8 string variant.
	ValueKindString ValueKind = "string"
	// ValueKindNumber is the arbitrary-precision decimal number variant.
	ValueKindNumber ValueKind = "number"
	// ValueKindList is the ordered list variant.
	ValueKindList ValueKind = "list"
	// ValueKindMap is the string-keyed map variant.
	ValueKindMap ValueKind = "map"
	// ValueKindTagged is an application-tagged canonical payload.
	ValueKindTagged ValueKind = "tagged"
)

// Value is syntax-neutral data that can cross datamodel, actor, process, and
// durability boundaries. Its variants are null, boolean, UTF-8 string, exact
// decimal number, ordered list, string-keyed map, and a tagged application
// value whose payload is another Value.
//
// Value is immutable through its public API: constructors copy collection
// inputs, collection accessors return copies, and Clone makes a deep copy.
// The zero value is the canonical null value.
//
// Numbers are finite decimal values, not float64 values. NumberValue accepts
// RFC 8259 JSON number syntax and stores a canonical coefficient/exponent
// representation without imposing an integer size or silently rounding.
type Value struct {
	kind   ValueKind
	bool   bool
	text   string
	list   []Value
	object map[string]Value
	tagged *taggedValue
}

type taggedValue struct {
	tag     string
	payload Value
}

// NullValue returns the canonical null Value. It is equal to a zero Value.
func NullValue() Value { return Value{} }

// BoolValue returns a canonical boolean Value.
func BoolValue(value bool) Value {
	return Value{kind: ValueKindBool, bool: value}
}

// StringValue returns a canonical string Value. A string must contain valid
// UTF-8 when encoded; MarshalBinary, MarshalText, MarshalJSON, and Wire report
// an error for invalid UTF-8 rather than changing it.
func StringValue(value string) Value {
	return Value{kind: ValueKindString, text: value}
}

// NumberValue parses an RFC 8259 JSON number without converting it through a
// floating-point type. Mathematically equal spellings have the same canonical
// representation: insignificant zeroes and a negative sign on zero are
// removed, and a base-10 exponent is used when necessary.
func NumberValue(value string) (Value, error) {
	canonical, err := canonicalNumber(value)
	if err != nil {
		return Value{}, err
	}
	return Value{kind: ValueKindNumber, text: canonical}, nil
}

// Int64Value returns an exact canonical number for value.
func Int64Value(value int64) Value {
	result, err := NumberValue(strconv.FormatInt(value, 10))
	if err != nil {
		panic(err) // strconv.FormatInt always produces valid number syntax.
	}
	return result
}

// Uint64Value returns an exact canonical number for value.
func Uint64Value(value uint64) Value {
	result, err := NumberValue(strconv.FormatUint(value, 10))
	if err != nil {
		panic(err) // strconv.FormatUint always produces valid number syntax.
	}
	return result
}

// Float64Value returns a canonical number for the exact decimal spelling Go
// uses to round-trip value. NaN and infinities are rejected because they are
// not canonical data numbers.
func Float64Value(value float64) (Value, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return Value{}, fmt.Errorf("statecharts: non-finite float64 %v cannot be a Value number", value)
	}
	return NumberValue(strconv.FormatFloat(value, 'g', -1, 64))
}

// ListValue returns an ordered list and deeply copies values.
func ListValue(values []Value) Value {
	list := make([]Value, len(values))
	for i := range values {
		list[i] = values[i].Clone()
	}
	return Value{kind: ValueKindList, list: list}
}

// MapValue returns a string-keyed map and deeply copies values. Map order is
// not semantically significant; canonical encodings sort keys.
func MapValue(values map[string]Value) Value {
	object := make(map[string]Value, len(values))
	for key, value := range values {
		object[key] = value.Clone()
	}
	return Value{kind: ValueKindMap, object: object}
}

// TaggedValue returns an application value identified by tag. A tag is a
// stable application-defined type identifier; it must be non-empty UTF-8.
// The canonical payload is deeply copied and needs no concrete-type registry
// to decode.
func TaggedValue(tag string, payload Value) (Value, error) {
	if tag == "" {
		return Value{}, fmt.Errorf("statecharts: tagged Value requires a non-empty tag")
	}
	if !utf8.ValidString(tag) {
		return Value{}, fmt.Errorf("statecharts: tagged Value tag is not valid UTF-8")
	}
	return Value{
		kind: ValueKindTagged,
		tagged: &taggedValue{
			tag:     tag,
			payload: payload.Clone(),
		},
	}, nil
}

// Kind returns the Value variant. A zero Value has ValueKindNull.
func (v Value) Kind() ValueKind {
	if v.kind == "" {
		return ValueKindNull
	}
	return v.kind
}

// AsBool returns the boolean and true when v is a boolean Value.
func (v Value) AsBool() (bool, bool) {
	if v.Kind() != ValueKindBool {
		return false, false
	}
	return v.bool, true
}

// AsString returns the string and true when v is a string Value.
func (v Value) AsString() (string, bool) {
	if v.Kind() != ValueKindString {
		return "", false
	}
	return v.text, true
}

// AsNumber returns the exact canonical decimal spelling and true when v is a
// number Value. The spelling is either "0", a signed coefficient, or a
// signed coefficient followed by "e" and a signed base-10 exponent.
func (v Value) AsNumber() (string, bool) {
	if v.Kind() != ValueKindNumber {
		return "", false
	}
	return v.text, true
}

// AsList returns a deep copy of the list and true when v is a list Value.
func (v Value) AsList() ([]Value, bool) {
	if v.Kind() != ValueKindList {
		return nil, false
	}
	result := make([]Value, len(v.list))
	for i := range v.list {
		result[i] = v.list[i].Clone()
	}
	return result, true
}

// AsMap returns a deep copy of the map and true when v is a map Value.
func (v Value) AsMap() (map[string]Value, bool) {
	if v.Kind() != ValueKindMap {
		return nil, false
	}
	result := make(map[string]Value, len(v.object))
	for key, value := range v.object {
		result[key] = value.Clone()
	}
	return result, true
}

// AsTagged returns the tag, a deep copy of the payload, and true when v is a
// tagged application Value.
func (v Value) AsTagged() (tag string, payload Value, ok bool) {
	if v.Kind() != ValueKindTagged || v.tagged == nil {
		return "", Value{}, false
	}
	return v.tagged.tag, v.tagged.payload.Clone(), true
}

// Clone returns a deeply independent Value.
func (v Value) Clone() Value {
	switch v.Kind() {
	case ValueKindNull:
		return NullValue()
	case ValueKindBool:
		return BoolValue(v.bool)
	case ValueKindString:
		return StringValue(v.text)
	case ValueKindNumber:
		return Value{kind: ValueKindNumber, text: v.text}
	case ValueKindList:
		return ListValue(v.list)
	case ValueKindMap:
		return MapValue(v.object)
	case ValueKindTagged:
		if v.tagged == nil {
			return Value{kind: ValueKindTagged}
		}
		return Value{
			kind: ValueKindTagged,
			tagged: &taggedValue{
				tag:     v.tagged.tag,
				payload: v.tagged.payload.Clone(),
			},
		}
	default:
		return Value{kind: v.kind}
	}
}

// Equal reports whether v and other contain the same canonical data.
func (v Value) Equal(other Value) bool {
	if v.Kind() != other.Kind() {
		return false
	}
	switch v.Kind() {
	case ValueKindNull:
		return true
	case ValueKindBool:
		return v.bool == other.bool
	case ValueKindString, ValueKindNumber:
		return v.text == other.text
	case ValueKindList:
		if len(v.list) != len(other.list) {
			return false
		}
		for i := range v.list {
			if !v.list[i].Equal(other.list[i]) {
				return false
			}
		}
		return true
	case ValueKindMap:
		if len(v.object) != len(other.object) {
			return false
		}
		for key, value := range v.object {
			otherValue, ok := other.object[key]
			if !ok || !value.Equal(otherValue) {
				return false
			}
		}
		return true
	case ValueKindTagged:
		if v.tagged == nil || other.tagged == nil {
			return v.tagged == nil && other.tagged == nil
		}
		return v.tagged.tag == other.tagged.tag && v.tagged.payload.Equal(other.tagged.payload)
	default:
		return false
	}
}

// ValueWireVersion is the current canonical Value wire-format version.
const ValueWireVersion = 1

// ValueWire is the documented wire union for Value. Exactly one variant
// field is present according to Kind; null has no variant field, while tagged
// has both Tag and Payload. Version must be ValueWireVersion.
//
// The JSON field names are the canonical text and binary representation used
// by Value's marshal methods. JSON object keys in Map are sorted by
// encoding/json, so the resulting bytes are deterministic and suitable as
// revision material. The binary representation deliberately uses the same
// versioned UTF-8 bytes as the text representation. Unmarshal into Value when
// decoding bytes; ValueFromWire is the validation gate for a ValueWire built
// or decoded separately.
type ValueWire struct {
	Version int                   `json:"version"`
	Kind    ValueKind             `json:"kind"`
	Bool    *bool                 `json:"bool,omitempty"`
	String  *string               `json:"string,omitempty"`
	Number  *string               `json:"number,omitempty"`
	List    *[]ValueWire          `json:"list,omitempty"`
	Map     *map[string]ValueWire `json:"map,omitempty"`
	Tag     *string               `json:"tag,omitempty"`
	Payload *ValueWire            `json:"payload,omitempty"`
}

// Wire returns an independent, validated wire representation of v.
func (v Value) Wire() (ValueWire, error) {
	wire := ValueWire{Version: ValueWireVersion, Kind: v.Kind()}
	switch v.Kind() {
	case ValueKindNull:
		return wire, nil
	case ValueKindBool:
		value := v.bool
		wire.Bool = &value
	case ValueKindString:
		if !utf8.ValidString(v.text) {
			return ValueWire{}, fmt.Errorf("statecharts: Value string is not valid UTF-8")
		}
		value := v.text
		wire.String = &value
	case ValueKindNumber:
		canonical, err := canonicalNumber(v.text)
		if err != nil || canonical != v.text {
			return ValueWire{}, fmt.Errorf("statecharts: Value contains non-canonical number %q", v.text)
		}
		value := v.text
		wire.Number = &value
	case ValueKindList:
		list := make([]ValueWire, len(v.list))
		for i, value := range v.list {
			child, err := value.Wire()
			if err != nil {
				return ValueWire{}, fmt.Errorf("statecharts: Value list item %d: %w", i, err)
			}
			list[i] = child
		}
		wire.List = &list
	case ValueKindMap:
		object := make(map[string]ValueWire, len(v.object))
		for key, value := range v.object {
			if !utf8.ValidString(key) {
				return ValueWire{}, fmt.Errorf("statecharts: Value map key is not valid UTF-8")
			}
			child, err := value.Wire()
			if err != nil {
				return ValueWire{}, fmt.Errorf("statecharts: Value map key %q: %w", key, err)
			}
			object[key] = child
		}
		wire.Map = &object
	case ValueKindTagged:
		if v.tagged == nil || v.tagged.tag == "" {
			return ValueWire{}, fmt.Errorf("statecharts: tagged Value requires a non-empty tag and payload")
		}
		if !utf8.ValidString(v.tagged.tag) {
			return ValueWire{}, fmt.Errorf("statecharts: tagged Value tag is not valid UTF-8")
		}
		payload, err := v.tagged.payload.Wire()
		if err != nil {
			return ValueWire{}, fmt.Errorf("statecharts: tagged Value payload: %w", err)
		}
		tag := v.tagged.tag
		wire.Tag = &tag
		wire.Payload = &payload
	default:
		return ValueWire{}, fmt.Errorf("statecharts: unknown Value kind %q", v.kind)
	}
	return wire, nil
}

// ValueFromWire validates wire and returns an immutable Value. Unknown
// versions, unknown kinds, malformed union combinations, invalid numbers,
// invalid UTF-8, and malformed tagged values are rejected.
func ValueFromWire(wire ValueWire) (Value, error) {
	if wire.Version != ValueWireVersion {
		return Value{}, fmt.Errorf("statecharts: unsupported Value wire version %d", wire.Version)
	}
	switch wire.Kind {
	case ValueKindNull:
		if wireFieldCount(wire) != 0 {
			return Value{}, fmt.Errorf("statecharts: null wire value must not contain a union field")
		}
		return NullValue(), nil
	case ValueKindBool:
		if wire.Bool == nil || wireFieldCount(wire) != 1 {
			return Value{}, fmt.Errorf("statecharts: bool wire value requires bool and no other union field")
		}
		return BoolValue(*wire.Bool), nil
	case ValueKindString:
		if wire.String == nil || wireFieldCount(wire) != 1 {
			return Value{}, fmt.Errorf("statecharts: string wire value requires string and no other union field")
		}
		if !utf8.ValidString(*wire.String) {
			return Value{}, fmt.Errorf("statecharts: string wire value is not valid UTF-8")
		}
		return StringValue(*wire.String), nil
	case ValueKindNumber:
		if wire.Number == nil || wireFieldCount(wire) != 1 {
			return Value{}, fmt.Errorf("statecharts: number wire value requires number and no other union field")
		}
		return NumberValue(*wire.Number)
	case ValueKindList:
		if wire.List == nil || wireFieldCount(wire) != 1 {
			return Value{}, fmt.Errorf("statecharts: list wire value requires list and no other union field")
		}
		values := make([]Value, len(*wire.List))
		for i, child := range *wire.List {
			value, err := ValueFromWire(child)
			if err != nil {
				return Value{}, fmt.Errorf("statecharts: list wire item %d: %w", i, err)
			}
			values[i] = value
		}
		return Value{kind: ValueKindList, list: values}, nil
	case ValueKindMap:
		if wire.Map == nil || wireFieldCount(wire) != 1 {
			return Value{}, fmt.Errorf("statecharts: map wire value requires map and no other union field")
		}
		values := make(map[string]Value, len(*wire.Map))
		for key, child := range *wire.Map {
			if !utf8.ValidString(key) {
				return Value{}, fmt.Errorf("statecharts: map wire key is not valid UTF-8")
			}
			value, err := ValueFromWire(child)
			if err != nil {
				return Value{}, fmt.Errorf("statecharts: map wire key %q: %w", key, err)
			}
			values[key] = value
		}
		return Value{kind: ValueKindMap, object: values}, nil
	case ValueKindTagged:
		if wire.Tag == nil {
			return Value{}, fmt.Errorf("statecharts: tagged wire value requires tag")
		}
		if wire.Payload == nil {
			return Value{}, fmt.Errorf("statecharts: tagged wire value requires payload")
		}
		if wireFieldCount(wire) != 2 {
			return Value{}, fmt.Errorf("statecharts: tagged wire value requires only tag and payload")
		}
		if *wire.Tag == "" {
			return Value{}, fmt.Errorf("statecharts: tagged wire value requires a non-empty tag")
		}
		if !utf8.ValidString(*wire.Tag) {
			return Value{}, fmt.Errorf("statecharts: tagged wire tag is not valid UTF-8")
		}
		payload, err := ValueFromWire(*wire.Payload)
		if err != nil {
			return Value{}, fmt.Errorf("statecharts: tagged wire payload: %w", err)
		}
		return Value{
			kind: ValueKindTagged,
			tagged: &taggedValue{
				tag:     *wire.Tag,
				payload: payload,
			},
		}, nil
	default:
		return Value{}, fmt.Errorf("statecharts: unknown Value wire kind %q", wire.Kind)
	}
}

func wireFieldCount(wire ValueWire) int {
	count := 0
	if wire.Bool != nil {
		count++
	}
	if wire.String != nil {
		count++
	}
	if wire.Number != nil {
		count++
	}
	if wire.List != nil {
		count++
	}
	if wire.Map != nil {
		count++
	}
	if wire.Tag != nil {
		count++
	}
	if wire.Payload != nil {
		count++
	}
	return count
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("statecharts: unexpected data after Value wire")
		}
		return fmt.Errorf("statecharts: decode Value wire: %w", err)
	}
	return nil
}

// MarshalBinary implements encoding.BinaryMarshaler using the deterministic,
// versioned ValueWire representation.
func (v Value) MarshalBinary() ([]byte, error) {
	return v.marshalWire()
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler using the
// deterministic, versioned ValueWire representation.
func (v *Value) UnmarshalBinary(data []byte) error {
	return v.unmarshalWire(data)
}

// MarshalText implements encoding.TextMarshaler using the deterministic,
// versioned ValueWire representation.
func (v Value) MarshalText() ([]byte, error) {
	return v.marshalWire()
}

// UnmarshalText implements encoding.TextUnmarshaler using the deterministic,
// versioned ValueWire representation.
func (v *Value) UnmarshalText(data []byte) error {
	return v.unmarshalWire(data)
}

// MarshalJSON implements json.Marshaler using the deterministic, versioned
// ValueWire representation. This is the canonical Value wire JSON, not the
// untagged JSON-shaped form returned by JSONValue.
func (v Value) MarshalJSON() ([]byte, error) {
	return v.marshalWire()
}

// UnmarshalJSON implements json.Unmarshaler using the deterministic,
// versioned ValueWire representation.
func (v *Value) UnmarshalJSON(data []byte) error {
	return v.unmarshalWire(data)
}

func (v Value) marshalWire() ([]byte, error) {
	wire, err := v.Wire()
	if err != nil {
		return nil, err
	}
	return json.Marshal(wire)
}

func (v *Value) unmarshalWire(data []byte) error {
	if v == nil {
		return fmt.Errorf("statecharts: cannot unmarshal Value into nil receiver")
	}
	if !utf8.Valid(data) {
		return fmt.Errorf("statecharts: Value wire is not valid UTF-8")
	}
	var wire ValueWire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return fmt.Errorf("statecharts: decode Value wire: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	decoded, err := ValueFromWire(wire)
	if err != nil {
		return err
	}
	*v = decoded
	return nil
}

// ValueFromJSON converts a JSON-shaped Go value to a canonical Value without
// reflection. It accepts nil, bool, string, json.Number, finite float types,
// integer types, []any, and map[string]any. Use json.Decoder.UseNumber before
// this conversion when decoding JSON whose integer precision must be kept.
func ValueFromJSON(input any) (Value, error) {
	switch value := input.(type) {
	case nil:
		return NullValue(), nil
	case bool:
		return BoolValue(value), nil
	case string:
		if !utf8.ValidString(value) {
			return Value{}, fmt.Errorf("statecharts: JSON-shaped string is not valid UTF-8")
		}
		return StringValue(value), nil
	case json.Number:
		return NumberValue(value.String())
	case float64:
		return Float64Value(value)
	case float32:
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return Value{}, fmt.Errorf("statecharts: non-finite float32 %v cannot be a Value number", value)
		}
		return NumberValue(strconv.FormatFloat(float64(value), 'g', -1, 32))
	case int:
		return Int64Value(int64(value)), nil
	case int8:
		return Int64Value(int64(value)), nil
	case int16:
		return Int64Value(int64(value)), nil
	case int32:
		return Int64Value(int64(value)), nil
	case int64:
		return Int64Value(value), nil
	case uint:
		return Uint64Value(uint64(value)), nil
	case uint8:
		return Uint64Value(uint64(value)), nil
	case uint16:
		return Uint64Value(uint64(value)), nil
	case uint32:
		return Uint64Value(uint64(value)), nil
	case uint64:
		return Uint64Value(value), nil
	case []any:
		items := make([]Value, len(value))
		for i, item := range value {
			converted, err := ValueFromJSON(item)
			if err != nil {
				return Value{}, fmt.Errorf("statecharts: JSON-shaped list item %d: %w", i, err)
			}
			items[i] = converted
		}
		return Value{kind: ValueKindList, list: items}, nil
	case map[string]any:
		items := make(map[string]Value, len(value))
		for key, item := range value {
			if !utf8.ValidString(key) {
				return Value{}, fmt.Errorf("statecharts: JSON-shaped map key is not valid UTF-8")
			}
			converted, err := ValueFromJSON(item)
			if err != nil {
				return Value{}, fmt.Errorf("statecharts: JSON-shaped map key %q: %w", key, err)
			}
			items[key] = converted
		}
		return Value{kind: ValueKindMap, object: items}, nil
	default:
		return Value{}, fmt.Errorf("statecharts: cannot convert JSON-shaped value of type %T to Value", input)
	}
}

// JSONValue converts v to an independent JSON-shaped Go value. Numbers are
// returned as json.Number to retain precision. Tagged values are rejected
// because an untagged JSON value cannot preserve their application tag.
func (v Value) JSONValue() (any, error) {
	switch v.Kind() {
	case ValueKindNull:
		return nil, nil
	case ValueKindBool:
		return v.bool, nil
	case ValueKindString:
		if !utf8.ValidString(v.text) {
			return nil, fmt.Errorf("statecharts: Value string is not valid UTF-8")
		}
		return v.text, nil
	case ValueKindNumber:
		canonical, err := canonicalNumber(v.text)
		if err != nil || canonical != v.text {
			return nil, fmt.Errorf("statecharts: Value contains non-canonical number %q", v.text)
		}
		return json.Number(v.text), nil
	case ValueKindList:
		items := make([]any, len(v.list))
		for i, item := range v.list {
			converted, err := item.JSONValue()
			if err != nil {
				return nil, fmt.Errorf("statecharts: Value list item %d: %w", i, err)
			}
			items[i] = converted
		}
		return items, nil
	case ValueKindMap:
		items := make(map[string]any, len(v.object))
		for key, item := range v.object {
			if !utf8.ValidString(key) {
				return nil, fmt.Errorf("statecharts: Value map key is not valid UTF-8")
			}
			converted, err := item.JSONValue()
			if err != nil {
				return nil, fmt.Errorf("statecharts: Value map key %q: %w", key, err)
			}
			items[key] = converted
		}
		return items, nil
	case ValueKindTagged:
		if v.tagged == nil {
			return nil, fmt.Errorf("statecharts: malformed tagged Value")
		}
		return nil, fmt.Errorf("statecharts: tagged Value %q is not JSON-shaped", v.tagged.tag)
	default:
		return nil, fmt.Errorf("statecharts: unknown Value kind %q", v.kind)
	}
}

func canonicalNumber(input string) (string, error) {
	if input == "" {
		return "", invalidNumber(input, "empty number")
	}

	position := 0
	negative := false
	if input[position] == '-' {
		negative = true
		position++
		if position == len(input) {
			return "", invalidNumber(input, "missing integer")
		}
	}

	integerStart := position
	switch {
	case input[position] == '0':
		position++
		if position < len(input) && isDecimalDigit(input[position]) {
			return "", invalidNumber(input, "leading zero")
		}
	case input[position] >= '1' && input[position] <= '9':
		for position < len(input) && isDecimalDigit(input[position]) {
			position++
		}
	default:
		return "", invalidNumber(input, "invalid integer")
	}
	integer := input[integerStart:position]

	fraction := ""
	if position < len(input) && input[position] == '.' {
		position++
		fractionStart := position
		for position < len(input) && isDecimalDigit(input[position]) {
			position++
		}
		if position == fractionStart {
			return "", invalidNumber(input, "missing fraction digits")
		}
		fraction = input[fractionStart:position]
	}

	exponent := new(big.Int)
	if position < len(input) && (input[position] == 'e' || input[position] == 'E') {
		position++
		exponentNegative := false
		if position < len(input) && (input[position] == '+' || input[position] == '-') {
			exponentNegative = input[position] == '-'
			position++
		}
		exponentStart := position
		for position < len(input) && isDecimalDigit(input[position]) {
			position++
		}
		if position == exponentStart {
			return "", invalidNumber(input, "missing exponent digits")
		}
		exponent.SetString(input[exponentStart:position], 10)
		if exponentNegative {
			exponent.Neg(exponent)
		}
	}
	if position != len(input) {
		return "", invalidNumber(input, fmt.Sprintf("unexpected character %q", input[position]))
	}

	digits := integer + fraction
	firstNonzero := 0
	for firstNonzero < len(digits) && digits[firstNonzero] == '0' {
		firstNonzero++
	}
	if firstNonzero == len(digits) {
		return "0", nil
	}
	digits = digits[firstNonzero:]

	exponent.Sub(exponent, new(big.Int).SetUint64(uint64(len(fraction))))
	trailingZeroes := 0
	for len(digits)-trailingZeroes > 0 && digits[len(digits)-1-trailingZeroes] == '0' {
		trailingZeroes++
	}
	if trailingZeroes != 0 {
		digits = digits[:len(digits)-trailingZeroes]
		exponent.Add(exponent, new(big.Int).SetUint64(uint64(trailingZeroes)))
	}

	if negative {
		digits = "-" + digits
	}
	if exponent.Sign() == 0 {
		return digits, nil
	}
	return digits + "e" + exponent.String(), nil
}

func isDecimalDigit(value byte) bool {
	return value >= '0' && value <= '9'
}

func invalidNumber(input, reason string) error {
	return fmt.Errorf("statecharts: invalid number %q: %s", input, reason)
}
