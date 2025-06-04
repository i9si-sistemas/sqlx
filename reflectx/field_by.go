package reflectx

import "reflect"

// FieldByIndexes returns a value for the field given by the struct traversal
// for the given value.
func FieldByIndexes(v reflect.Value, indexes []int) reflect.Value {
	for _, i := range indexes {
		v = reflect.Indirect(v).Field(i)
		// if this is a pointer and it's nil, allocate a new value and set it
		if v.Kind() == reflect.Pointer && v.IsNil() {
			alloc := reflect.New(Deref(v.Type()))
			v.Set(alloc)
		}
		if v.Kind() == reflect.Map && v.IsNil() {
			v.Set(reflect.MakeMap(v.Type()))
		}
	}
	return v
}

// FieldByIndexesReadOnly returns a value for a particular struct traversal,
// but is not concerned with allocating nil pointers because the value is
// going to be used for reading and not setting.
func FieldByIndexesReadOnly(v reflect.Value, indexes []int) reflect.Value {
	for _, i := range indexes {
		v = reflect.Indirect(v).Field(i)
	}
	return v
}
