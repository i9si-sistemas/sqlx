package types

import (
	"testing"

	"github.com/i9si-sistemas/assert"
)

func TestNullJSONText(t *testing.T) {
	j := NullJSONText{}
	err := j.Scan(`{"foo": 1, "bar": 2}`)
	assert.NoError(t, err)
	v, err := j.Value()
	assert.NoError(t, err)
	err = (&j).Scan(v)
	assert.NoError(t, err)
	m := map[string]any{}
	j.Unmarshal(&m)

	assert.False(t, m["foo"].(float64) != 1 || m["bar"].(float64) != 2)

	j = NullJSONText{}
	err = j.Scan(nil)
	assert.NoError(t, err)
	assert.False(t, j.Valid)
}
