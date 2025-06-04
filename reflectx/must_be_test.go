package reflectx

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/i9si-sistemas/assert"
)

type E1 struct {
	A int
}

func TestMustBe(t *testing.T) {
	typ := reflect.TypeOf(E1{})
	mustBe(typ, reflect.Struct)

	defer func() {
		if r := recover(); r != nil {
			valueErr, ok := r.(*reflect.ValueError)
			if !ok {
				t.Fatal("expected panic with *reflect.ValueError")
			}
			assert.Equal(t, valueErr.Method, "github.com/i9si-sistemas/sqlx/reflectx.TestMustBe", fmt.Sprintf("unexpected Method: %s", valueErr.Method))
			assert.Equal(t, valueErr.Kind, reflect.String, fmt.Sprintf("unexpected Kind: %s", valueErr.Kind))
		} else {
			t.Fatal("expected panic")
		}
	}()
	typ = reflect.TypeOf("string")
	mustBe(typ, reflect.Struct)
	t.Fatal("got here, didn't expect to")
}
