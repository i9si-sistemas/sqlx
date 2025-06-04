package types

import "testing"

func TestBitBool(t *testing.T) {
	// Test true value
	var b BitBool = true

	v, err := b.Value()
	if err != nil {
		t.Errorf("Cannot return error")
	}
	err = (&b).Scan(v)
	if err != nil {
		t.Errorf("Was not expecting an error")
	}
	if !b {
		t.Errorf("Was expecting the bool we sent in (true), got %v", b)
	}

	// Test false value
	b = false

	v, err = b.Value()
	if err != nil {
		t.Errorf("Cannot return error")
	}
	err = (&b).Scan(v)
	if err != nil {
		t.Errorf("Was not expecting an error")
	}
	if b {
		t.Errorf("Was expecting the bool we sent in (false), got %v", b)
	}
}
