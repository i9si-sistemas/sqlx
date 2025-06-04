package reflectx

import "reflect"

// A FieldInfo is metadata for a struct field.
type FieldInfo struct {
	Index    []int
	Path     string
	Field    reflect.StructField
	Zero     reflect.Value
	Name     string
	Options  map[string]string
	Embedded bool
	Children []*FieldInfo
	Parent   *FieldInfo
}
