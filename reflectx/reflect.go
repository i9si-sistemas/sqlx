// Package reflectx implements extensions to the standard reflect lib suitable
// for implementing marshalling and unmarshalling packages.  The main Mapper type
// allows for Go-compatible named attribute access, including accessing embedded
// struct attributes and the ability to use  functions and struct tags to
// customize field names.
package reflectx

import (
	"reflect"
)


// NewMapper returns a new mapper using the tagName as its struct field tag.
// If tagName is the empty string, it is ignored.
func NewMapper(tagName string) *Mapper {
	return &Mapper{
		cache:   make(map[reflect.Type]*StructMap),
		tagName: tagName,
	}
}

// NewMapperTagFunc returns a new mapper which contains a mapper for field names
// AND a mapper for tag values.  This is useful for tags like json which can
// have values like "name,omitempty".
func NewMapperTagFunc(tagName string, mapFunc, tagMapFunc func(string) string) *Mapper {
	return &Mapper{
		cache:      make(map[reflect.Type]*StructMap),
		tagName:    tagName,
		mapFunc:    mapFunc,
		tagMapFunc: tagMapFunc,
	}
}

// NewMapperFunc returns a new mapper which optionally obeys a field tag and
// a struct field name mapper func given by f.  Tags will take precedence, but
// for any other field, the mapped name will be f(field.Name)
func NewMapperFunc(tagName string, f func(string) string) *Mapper {
	return &Mapper{
		cache:   make(map[reflect.Type]*StructMap),
		tagName: tagName,
		mapFunc: f,
	}
}


