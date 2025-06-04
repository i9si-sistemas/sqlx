package reflectx

import "reflect"

type Kinder interface {
	Kind() reflect.Kind
}
