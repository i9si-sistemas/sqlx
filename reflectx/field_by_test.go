package reflectx

import (
	"reflect"
	"testing"

	"github.com/i9si-sistemas/assert"
)

func TestFieldByIndexes(t *testing.T) {
	type C struct {
		C0 bool
		C1 string
		C2 int
		C3 map[string]int
	}
	type B struct {
		B1 C
		B2 *C
	}
	type A struct {
		A1 B
		A2 *B
	}
	testCases := []struct {
		value         any
		indexes       []int
		expectedValue any
		readOnly      bool
	}{
		{
			value: A{
				A1: B{B1: C{C0: true}},
			},
			indexes:       []int{0, 0, 0},
			expectedValue: true,
			readOnly:      true,
		},
		{
			value: A{
				A2: &B{B2: &C{C1: "answer"}},
			},
			indexes:       []int{1, 1, 1},
			expectedValue: "answer",
			readOnly:      true,
		},
		{
			value:         &A{},
			indexes:       []int{1, 1, 3},
			expectedValue: map[string]int{},
		},
	}

	for _, tc := range testCases {
		checkResults := func(v reflect.Value) {
			if tc.expectedValue == nil {
				assert.True(t, v.IsNil())
			} else {
				assert.Equal(t, tc.expectedValue, v.Interface())
			}
		}

		checkResults(FieldByIndexes(reflect.ValueOf(tc.value), tc.indexes))
		if tc.readOnly {
			checkResults(FieldByIndexesReadOnly(reflect.ValueOf(tc.value), tc.indexes))
		}
	}
}
