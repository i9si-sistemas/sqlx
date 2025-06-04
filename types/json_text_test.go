package types

import (
	"testing"

	"github.com/i9si-sistemas/assert"
)

func TestJSONText(t *testing.T) {
	j := NewJSONText([]byte(`{"foo": 1, "bar": 2}`)...)
	v, err := j.Value()
	assert.NoError(t, err)
	err = (&j).Scan(v)
	assert.NoError(t, err)
	m := map[string]any{}
	j.Unmarshal(&m)
	assert.False(t, m["foo"].(float64) != 1 || m["bar"].(float64) != 2)

	j = JSONText(`{"foo": 1, invalid, false}`)
	v, err = j.Value()
	assert.Error(t, err)
	assert.Equal(t, string(v.([]byte)), EmptyJSON)

	j = NewJSONText()
	assert.Equal(t, j, EmptyJSON)
	v, err = j.Value()
	assert.NoError(t, err)

	err = (&j).Scan(v)
	assert.NoError(t, err)

	j = JSONText(nil)
	v, err = j.Value()
	assert.NoError(t, err)

	err = (&j).Scan(v)
	assert.NoError(t, err)
}
