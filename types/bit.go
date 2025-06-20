package types

import (
	"database/sql/driver"
	"errors"
)

// BitBool is an implementation of a bool for the MySQL type BIT(1).
// This type allows you to avoid wasting an entire byte for MySQL's boolean type TINYINT.
type BitBool bool

// Value implements the driver.Valuer interface,
// and turns the BitBool into a bitfield (BIT(1)) for MySQL storage.
func (b BitBool) Value() (driver.Value, error) {
	if b {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

// ErrBadBitBoolSource is returned when BitBool.Scan the type assertion fails
var ErrBadBitBoolSource = errors.New("bad []byte type assertion")

// Scan implements the sql.Scanner interface,
// and turns the bitfield incoming from MySQL into a BitBool
func (b *BitBool) Scan(src any) error {
	v, ok := src.([]byte)
	if !ok {
		return ErrBadBitBoolSource
	}
	*b = v[0] == 1
	return nil
}
