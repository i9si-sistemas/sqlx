package reflectx

import (
	"reflect"
	"testing"
)

// TestMapperMethodsByName tests Mapper methods FieldByName and TraversalsByName
func TestMapperMethodsByName(t *testing.T) {
	type C struct {
		C0 string
		C1 int
	}
	type B struct {
		B0 *C     `db:"B0"`
		B1 C      `db:"B1"`
		B2 string `db:"B2"`
	}
	type A struct {
		A0 *B `db:"A0"`
		B  `db:"A1"`
		A2 int
	}

	val := &A{
		A0: &B{
			B0: &C{C0: "0", C1: 1},
			B1: C{C0: "2", C1: 3},
			B2: "4",
		},
		B: B{
			B0: nil,
			B1: C{C0: "5", C1: 6},
			B2: "7",
		},
		A2: 8,
	}

	testCases := []struct {
		Name            string
		ExpectInvalid   bool
		ExpectedValue   any
		ExpectedIndexes []int
	}{
		{
			Name:            "A0.B0.C0",
			ExpectedValue:   "0",
			ExpectedIndexes: []int{0, 0, 0},
		},
		{
			Name:            "A0.B0.C1",
			ExpectedValue:   1,
			ExpectedIndexes: []int{0, 0, 1},
		},
		{
			Name:            "A0.B1.C0",
			ExpectedValue:   "2",
			ExpectedIndexes: []int{0, 1, 0},
		},
		{
			Name:            "A0.B1.C1",
			ExpectedValue:   3,
			ExpectedIndexes: []int{0, 1, 1},
		},
		{
			Name:            "A0.B2",
			ExpectedValue:   "4",
			ExpectedIndexes: []int{0, 2},
		},
		{
			Name:            "A1.B0.C0",
			ExpectedValue:   "",
			ExpectedIndexes: []int{1, 0, 0},
		},
		{
			Name:            "A1.B0.C1",
			ExpectedValue:   0,
			ExpectedIndexes: []int{1, 0, 1},
		},
		{
			Name:            "A1.B1.C0",
			ExpectedValue:   "5",
			ExpectedIndexes: []int{1, 1, 0},
		},
		{
			Name:            "A1.B1.C1",
			ExpectedValue:   6,
			ExpectedIndexes: []int{1, 1, 1},
		},
		{
			Name:            "A1.B2",
			ExpectedValue:   "7",
			ExpectedIndexes: []int{1, 2},
		},
		{
			Name:            "A2",
			ExpectedValue:   8,
			ExpectedIndexes: []int{2},
		},
		{
			Name:            "XYZ",
			ExpectInvalid:   true,
			ExpectedIndexes: []int{},
		},
		{
			Name:            "a3",
			ExpectInvalid:   true,
			ExpectedIndexes: []int{},
		},
	}

	// build the names array from the test cases
	names := make([]string, len(testCases))
	for i, tc := range testCases {
		names[i] = tc.Name
	}
	m := NewMapperFunc("db", func(n string) string { return n })
	v := reflect.ValueOf(val)
	values := m.FieldsByName(v, names)
	if len(values) != len(testCases) {
		t.Errorf("expected %d values, got %d", len(testCases), len(values))
		t.FailNow()
	}
	indexes := m.TraversalsByName(v.Type(), names)
	if len(indexes) != len(testCases) {
		t.Errorf("expected %d traversals, got %d", len(testCases), len(indexes))
		t.FailNow()
	}
	for i, val := range values {
		tc := testCases[i]
		traversal := indexes[i]
		if !reflect.DeepEqual(tc.ExpectedIndexes, traversal) {
			t.Errorf("expected %v, got %v", tc.ExpectedIndexes, traversal)
			t.FailNow()
		}
		val = reflect.Indirect(val)
		if tc.ExpectInvalid {
			if val.IsValid() {
				t.Errorf("%d: expected zero value, got %v", i, val)
			}
			continue
		}
		if !val.IsValid() {
			t.Errorf("%d: expected valid value, got %v", i, val)
			continue
		}
		actualValue := reflect.Indirect(val).Interface()
		if !reflect.DeepEqual(tc.ExpectedValue, actualValue) {
			t.Errorf("%d: expected %v, got %v", i, tc.ExpectedValue, actualValue)
		}
	}
}
