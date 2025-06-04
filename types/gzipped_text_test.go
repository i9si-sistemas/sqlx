package types

import "testing"

func TestGzipText(t *testing.T) {
	g := NewGzippedText([]byte("Hello, world")...)
	v, err := g.Value()
	if err != nil {
		t.Errorf("Was not expecting an error")
	}
	err = g.Scan(v)
	if err != nil {
		t.Errorf("Was not expecting an error")
	}
	if string(g.Bytes()) != "Hello, world" {
		t.Errorf("Was expecting the string we sent in (Hello World), got %s", string(g.Bytes()))
	}
}
