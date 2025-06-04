package reflectx

import "reflect"

// mustBe checks a value against a kind, panicing with a reflect.ValueError
// if the kind isn't that which is required.
func mustBe(v Kinder, expected reflect.Kind) {
	if k := v.Kind(); k != expected {
		panic(&reflect.ValueError{Method: methodName(), Kind: k})
	}
}
