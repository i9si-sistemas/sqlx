package sqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/i9si-sistemas/sqlx/reflectx"
)

// Rows is a wrapper around sql.Rows which caches costly reflect operations
// during a looped StructScan
type Rows struct {
	*sql.Rows
	unsafe bool
	Mapper *reflectx.Mapper
	// these fields cache memory use for a rows during iteration w/ structScan
	started bool
	fields  [][]int
	values  []any
}

// SliceScan using this Rows.
func (r *Rows) SliceScan() ([]any, error) {
	return SliceScan(r)
}

// MapScan using this Rows.
func (r *Rows) MapScan(dest map[string]any) error {
	return MapScan(r, dest)
}

// ErrMustPassAPointerToStructScan is returned by StructScan when a non-pointer
var ErrMustPassAPointerToStructScan = errors.New("must pass a pointer, not a value, to StructScan destination")

// StructScan is like sql.Rows.Scan, but scans a single Row into a single Struct.
// Use this and iterate over Rows manually when the memory load of Select() might be
// prohibitive.  *Rows.StructScan caches the reflect work of matching up column
// positions to fields to avoid that overhead per scan, which means it is not safe
// to run StructScan on the same Rows instance with different struct types.
func (r *Rows) StructScan(dest any) error {
	v := reflect.ValueOf(dest)

	if v.Kind() != reflect.Pointer {
		return ErrMustPassAPointerToStructScan
	}

	v = v.Elem()

	if !r.started {
		columns, err := r.Columns()
		if err != nil {
			return err
		}
		m := r.Mapper

		r.fields = m.TraversalsByName(v.Type(), columns)
		if f, err := missingFields(r.fields); err != nil && !r.unsafe {
			return fmt.Errorf("missing destination name %s in %T", columns[f], dest)
		}
		r.values = make([]any, len(columns))
		r.started = true
	}

	if err := fieldsByTraversal(v, r.fields, r.values, true); err != nil {
		return err
	}

	if err := r.Scan(r.values...); err != nil {
		return err
	}
	return r.Err()
}
