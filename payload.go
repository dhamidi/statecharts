package statecharts

import (
	"reflect"
)

// clonePayload takes the SCXML data-model snapshot required when <send> is
// evaluated. Capability values are deliberately retained: they are opaque to
// the Go data model and copying them would either be meaningless or invalid.
func clonePayload(data any) (any, error) {
	if data == nil {
		return nil, nil
	}
	v, err := cloneValue(reflect.ValueOf(data), make(map[cloneVisit]reflect.Value))
	if err != nil {
		return nil, err
	}
	return v.Interface(), nil
}

type cloneVisit struct {
	typ reflect.Type
	ptr unsafePointer
}

// unsafePointer is only an address-sized identity; no unsafe operations are
// performed. uintptr keeps capability values themselves out of the key.
type unsafePointer uintptr

func cloneValue(v reflect.Value, seen map[cloneVisit]reflect.Value) (reflect.Value, error) {
	if !v.IsValid() {
		return v, nil
	}
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		c, err := cloneValue(v.Elem(), seen)
		if err != nil {
			return reflect.Value{}, err
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(c)
		return out, nil
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		key := cloneVisit{v.Type(), unsafePointer(v.Pointer())}
		if c, ok := seen[key]; ok {
			return c, nil
		}
		out := reflect.New(v.Type().Elem())
		seen[key] = out
		c, err := cloneValue(v.Elem(), seen)
		if err != nil {
			return reflect.Value{}, err
		}
		out.Elem().Set(c)
		return out, nil
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		key := cloneVisit{v.Type(), unsafePointer(v.Pointer())}
		if c, ok := seen[key]; ok {
			return c, nil
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		seen[key] = out
		iter := v.MapRange()
		for iter.Next() {
			k, err := cloneValue(iter.Key(), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			val, err := cloneValue(iter.Value(), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			out.SetMapIndex(k, val)
		}
		return out, nil
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type()), nil
		}
		key := cloneVisit{v.Type(), unsafePointer(v.Pointer())}
		if c, ok := seen[key]; ok {
			return c, nil
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Cap())
		seen[key] = out
		for i := 0; i < v.Len(); i++ {
			c, err := cloneValue(v.Index(i), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			out.Index(i).Set(c)
		}
		return out, nil
	case reflect.Array:
		out := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			c, err := cloneValue(v.Index(i), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			out.Index(i).Set(c)
		}
		return out, nil
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		// Preserve inaccessible implementation state (time.Time and similar
		// value types), then recursively replace every exported field.
		out.Set(v)
		for i := 0; i < v.NumField(); i++ {
			if !v.Field(i).CanInterface() {
				continue
			}
			c, err := cloneValue(v.Field(i), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			out.Field(i).Set(c)
		}
		return out, nil
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v, nil
	default:
		return v, nil
	}
}
