package reflectx

import (
	"reflect"
	"strings"
	"testing"
)

func TestMapping(t *testing.T) {
	type Person struct {
		ID           int
		Name         string
		WearsGlasses bool `db:"wears_glasses"`
	}

	m := NewMapperFunc("db", strings.ToLower)
	p := Person{1, "Jason", true}
	mapping := m.TypeMap(reflect.TypeOf(p))

	for _, key := range []string{"id", "name", "wears_glasses"} {
		if fi := mapping.GetByPath(key); fi == nil {
			t.Errorf("Expecting to find key %s in mapping but did not.", key)
		}
	}

	type SportsPerson struct {
		Weight int
		Age    int
		Person
	}
	s := SportsPerson{Weight: 100, Age: 30, Person: p}
	mapping = m.TypeMap(reflect.TypeOf(s))
	for _, key := range []string{"id", "name", "wears_glasses", "weight", "age"} {
		if fi := mapping.GetByPath(key); fi == nil {
			t.Errorf("Expecting to find key %s in mapping but did not.", key)
		}
	}

	type RugbyPlayer struct {
		Position   int
		IsIntense  bool `db:"is_intense"`
		IsAllBlack bool `db:"-"`
		SportsPerson
	}
	r := RugbyPlayer{12, true, false, s}
	mapping = m.TypeMap(reflect.TypeOf(r))
	for _, key := range []string{"id", "name", "wears_glasses", "weight", "age", "position", "is_intense"} {
		if fi := mapping.GetByPath(key); fi == nil {
			t.Errorf("Expecting to find key %s in mapping but did not.", key)
		}
	}

	if fi := mapping.GetByPath("isallblack"); fi != nil {
		t.Errorf("Expecting to ignore `IsAllBlack` field")
	}
}

func TestGetByTraversal(t *testing.T) {
	type C struct {
		C0 int
		C1 int
	}
	type B struct {
		B0 string
		B1 *C
	}
	type A struct {
		A0 int
		A1 B
	}

	testCases := []struct {
		Index        []int
		ExpectedName string
		ExpectNil    bool
	}{
		{
			Index:        []int{0},
			ExpectedName: "A0",
		},
		{
			Index:        []int{1, 0},
			ExpectedName: "B0",
		},
		{
			Index:        []int{1, 1, 1},
			ExpectedName: "C1",
		},
		{
			Index:     []int{3, 4, 5},
			ExpectNil: true,
		},
		{
			Index:     []int{},
			ExpectNil: true,
		},
		{
			Index:     nil,
			ExpectNil: true,
		},
	}

	m := NewMapperFunc("db", func(n string) string { return n })
	tm := m.TypeMap(reflect.TypeOf(A{}))

	for i, tc := range testCases {
		fi := tm.GetByTraversal(tc.Index)
		if tc.ExpectNil {
			if fi != nil {
				t.Errorf("%d: expected nil, got %v", i, fi)
			}
			continue
		}

		if fi == nil {
			t.Errorf("%d: expected %s, got nil", i, tc.ExpectedName)
			continue
		}

		if fi.Name != tc.ExpectedName {
			t.Errorf("%d: expected %s, got %s", i, tc.ExpectedName, fi.Name)
		}
	}
}
