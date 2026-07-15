package statecharts

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// legacyDatamodelProgram/session keep the callback builder operational while
// #12-#15 replace it with the default Go datamodel. They are deliberately
// unexported and are not a second extension API.
type legacyDatamodelProgram struct {
	factory func() any
}

func (*legacyDatamodelProgram) Fingerprint() []byte { return []byte("legacy-go-json/v1") }

func (p *legacyDatamodelProgram) NewSession(SessionOptions) (DatamodelSession, error) {
	if p == nil || p.factory == nil {
		return nil, fmt.Errorf("statecharts: legacy chart has no datamodel factory")
	}
	return newLegacyDatamodelSession(p.factory()), nil
}

type legacyDatamodelSession struct {
	value any
}

func newLegacyDatamodelSession(value any) *legacyDatamodelSession {
	return &legacyDatamodelSession{value: value}
}

func (s *legacyDatamodelSession) legacyValue() any { return s.value }

func (*legacyDatamodelSession) EvaluateBoolean(ExecContext, CompiledExpression) (bool, error) {
	return false, fmt.Errorf("statecharts: legacy datamodel does not compile expressions")
}

func (*legacyDatamodelSession) EvaluateValue(ExecContext, CompiledExpression) (Value, error) {
	return Value{}, fmt.Errorf("statecharts: legacy datamodel does not compile expressions")
}

func (*legacyDatamodelSession) Assign(ExecContext, CompiledExpression, Value) error {
	return fmt.Errorf("statecharts: legacy datamodel does not compile expressions")
}

func (*legacyDatamodelSession) Execute(ExecContext, CompiledExpression) error {
	return fmt.Errorf("statecharts: legacy datamodel does not compile expressions")
}

func (*legacyDatamodelSession) ForEach(ExecContext, CompiledExpression, IterationBindings, func() error) error {
	return fmt.Errorf("statecharts: legacy datamodel does not compile expressions")
}

func (s *legacyDatamodelSession) EncodeSnapshot() ([]byte, error) {
	return json.Marshal(s.value)
}

func (s *legacyDatamodelSession) DecodeSnapshot(data []byte) error {
	if s.value == nil && len(data) == 0 {
		return nil
	}
	decoded, err := decodeLegacyDatamodel(data, freshLegacyDatamodel(s.value))
	if err != nil {
		return err
	}
	s.value = commitLegacyDatamodel(s.value, decoded)
	return nil
}

func (*legacyDatamodelSession) Close() error { return nil }

func decodeLegacyDatamodel(data []byte, prototype any) (any, error) {
	if prototype == nil {
		var value any
		err := json.Unmarshal(data, &value)
		return value, err
	}
	typeOfValue := reflect.TypeOf(prototype)
	var value any
	if typeOfValue.Kind() == reflect.Pointer {
		value = reflect.New(typeOfValue.Elem()).Interface()
	} else {
		value = reflect.New(typeOfValue).Interface()
	}
	if err := json.Unmarshal(data, value); err != nil {
		return nil, err
	}
	if typeOfValue.Kind() != reflect.Pointer {
		value = reflect.ValueOf(value).Elem().Interface()
	}
	return value, nil
}

func freshLegacyDatamodel(datamodel any) any {
	if datamodel == nil {
		return nil
	}
	typeOfValue := reflect.TypeOf(datamodel)
	if typeOfValue.Kind() == reflect.Pointer {
		return reflect.New(typeOfValue.Elem()).Interface()
	}
	return reflect.New(typeOfValue).Elem().Interface()
}

func commitLegacyDatamodel(datamodel, decoded any) any {
	if datamodel == nil || decoded == nil {
		return decoded
	}
	destination, source := reflect.ValueOf(datamodel), reflect.ValueOf(decoded)
	switch destination.Kind() {
	case reflect.Pointer:
		if !destination.IsNil() {
			if source.Type() == destination.Type() && !source.IsNil() {
				destination.Elem().Set(source.Elem())
				return datamodel
			}
			if source.Type().AssignableTo(destination.Elem().Type()) {
				destination.Elem().Set(source)
				return datamodel
			}
		}
	case reflect.Map:
		if !destination.IsNil() && source.Type() == destination.Type() {
			destination.Clear()
			for _, key := range source.MapKeys() {
				destination.SetMapIndex(key, source.MapIndex(key))
			}
			return datamodel
		}
	case reflect.Slice:
		if !destination.IsNil() && source.Type() == destination.Type() && destination.Len() == source.Len() {
			reflect.Copy(destination, source)
			return datamodel
		}
	}
	return decoded
}
