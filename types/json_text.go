package types

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

// EmptyJSON is a JSON without key-value pairs.
var EmptyJSON = JSONText("{}")

// JSONText is a json.RawMessage, which is a []byte underneath.
// Value() validates the json format in the source, and returns an error if
// the json is not valid.  Scan does no validation.  JSONText additionally
// implements `Unmarshal`, which unmarshals the json within to an any
type JSONText json.RawMessage

// NewJSONText returns a new JSONText with the given data.
func NewJSONText(data ...byte) JSONText {
	if !json.Valid(data) {
		return EmptyJSON
	}
	return JSONText(data)
}

// MarshalJSON returns the *j as the JSON encoding of j.
func (j JSONText) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return EmptyJSON, nil
	}
	return j, nil
}

// UnmarshalJSON sets *j to a copy of data
func (j *JSONText) UnmarshalJSON(data []byte) error {
	if j == nil {
		return errors.New("JSONText: UnmarshalJSON on nil pointer")
	}
	*j = append((*j)[0:0], data...)
	return nil
}

// Value returns j as a value.  This does a validating unmarshal into another
// RawMessage.  If j is invalid json, it returns an error.
func (j JSONText) Value() (driver.Value, error) {
	var m json.RawMessage
	var err = j.Unmarshal(&m)
	if err != nil {
		return []byte(EmptyJSON), err
	}
	return []byte(j), nil
}

// Scan stores the src in *j.  No validation is done.
func (j *JSONText) Scan(src any) error {
	var source []byte
	switch t := src.(type) {
	case string:
		source = []byte(t)
	case []byte:
		if len(t) == 0 {
			source = EmptyJSON
		} else {
			source = t
		}
	case nil:
		*j = EmptyJSON
	default:
		//lint:ignore ST1005 changing this could break consumers of this package
		return errors.New("Incompatible type for JSONText")
	}
	*j = append((*j)[0:0], source...)
	return nil
}

// Unmarshal unmarshal's the json in j to v, as in json.Unmarshal.
func (j *JSONText) Unmarshal(v any) error {
	if len(*j) == 0 {
		*j = EmptyJSON
	}
	return json.Unmarshal([]byte(*j), v)
}

// String supports pretty printing for JSONText types.
func (j JSONText) String() string {
	return string(j)
}
