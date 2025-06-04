package reflectx

import (
	"reflect"
	"sync"
)

// Mapper is a general purpose mapper of names to struct fields.  A Mapper
// behaves like most marshallers in the standard library, obeying a field tag
// for name mapping but also providing a basic transform function.
type Mapper struct {
	cache      map[reflect.Type]*StructMap
	tagName    string
	tagMapFunc func(string) string
	mapFunc    func(string) string
	mutex      sync.Mutex
}

// TypeMap returns a mapping of field strings to int slices representing
// the traversal down the struct to reach the field.
func (m *Mapper) TypeMap(t reflect.Type) *StructMap {
	m.mutex.Lock()
	mapping, ok := m.cache[t]
	if !ok {
		mapping = getMapping(t, m.tagName, m.mapFunc, m.tagMapFunc)
		m.cache[t] = mapping
	}
	m.mutex.Unlock()
	return mapping
}

// FieldMap returns the mapper's mapping of field names to reflect values.  Panics
// if v's Kind is not Struct, or v is not Indirectable to a struct kind.
func (m *Mapper) FieldMap(v reflect.Value) map[string]reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	r := map[string]reflect.Value{}
	tm := m.TypeMap(v.Type())
	for tagName, fi := range tm.Names {
		r[tagName] = FieldByIndexes(v, fi.Index)
	}
	return r
}

// FieldByName returns a field by its mapped name as a reflect.Value.
// Panics if v's Kind is not Struct or v is not Indirectable to a struct Kind.
// Returns zero Value if the name is not found.
func (m *Mapper) FieldByName(v reflect.Value, name string) reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	tm := m.TypeMap(v.Type())
	fi, ok := tm.Names[name]
	if !ok {
		return v
	}
	return FieldByIndexes(v, fi.Index)
}

// FieldsByName returns a slice of values corresponding to the slice of names
// for the value.  Panics if v's Kind is not Struct or v is not Indirectable
// to a struct Kind.  Returns zero Value for each name not found.
func (m *Mapper) FieldsByName(v reflect.Value, names []string) []reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	tm := m.TypeMap(v.Type())
	vals := make([]reflect.Value, 0, len(names))
	for _, name := range names {
		fi, ok := tm.Names[name]
		if !ok {
			vals = append(vals, *new(reflect.Value))
		} else {
			vals = append(vals, FieldByIndexes(v, fi.Index))
		}
	}
	return vals
}

// TraversalsByName returns a slice of int slices which represent the struct
// traversals for each mapped name.  Panics if t is not a struct or Indirectable
// to a struct.  Returns empty int slice for each name not found.
func (m *Mapper) TraversalsByName(t reflect.Type, names []string) [][]int {
	r := make([][]int, 0, len(names))
	m.TraversalsByNameFunc(t, names, func(_ int, i []int) error {
		if i == nil {
			r = append(r, []int{})
		} else {
			r = append(r, i)
		}

		return nil
	})
	return r
}

// TraversalsByNameFunc traverses the mapped names and calls fn with the index of
// each name and the struct traversal represented by that name. Panics if t is not
// a struct or Indirectable to a struct. Returns the first error returned by fn or nil.
func (m *Mapper) TraversalsByNameFunc(t reflect.Type, names []string, fn func(int, []int) error) error {
	t = Deref(t)
	mustBe(t, reflect.Struct)
	tm := m.TypeMap(t)
	for i, name := range names {
		fi, ok := tm.Names[name]
		if !ok {
			if err := fn(i, nil); err != nil {
				return err
			}
		} else {
			if err := fn(i, fi.Index); err != nil {
				return err
			}
		}
	}
	return nil
}
