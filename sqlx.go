package sqlx

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/i9si-sistemas/sqlx/reflectx"
)

// isScannable takes the reflect.Type and the actual dest value and returns
// whether or not it's Scannable.  Something is scannable if:
//   - it is not a struct
//   - it implements sql.Scanner
//   - it has no exported fields
func isScannable(t reflect.Type) bool {
	if reflect.PointerTo(t).Implements(_scannerInterface) {
		return true
	}
	if t.Kind() != reflect.Struct {
		return true
	}

	// it's not important that we use the right mapper for this particular object,
	// we're only concerned on how many exported fields this struct has
	return len(mapper().TypeMap(t).Index) == 0
}

// ColScanner is an interface used by MapScan and SliceScan
type ColScanner interface {
	Columns() ([]string, error)
	Scan(dest ...any) error
	Err() error
}

// Queryer is an interface used by Get and Select
type Queryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
	Queryx(query string, args ...any) (*Rows, error)
	QueryRowx(query string, args ...any) *Row
}

// Execer is an interface used by MustExec and LoadFile
type Execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// Binder is an interface for something which can bind queries (Tx, DB)
type Binder interface {
	DriverName() string
	Rebind(string) string
	BindNamed(string, any) (string, []any, error)
}

// Preparer is an interface used by Preparex.
type Preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

// isUnsafe determine if any of our extensions are unsafe
func isUnsafe(i any) bool {
	switch v := i.(type) {
	case Row:
		return v.unsafe
	case *Row:
		return v.unsafe
	case Rows:
		return v.unsafe
	case *Rows:
		return v.unsafe
	case NamedStmt:
		return v.Stmt.unsafe
	case *NamedStmt:
		return v.Stmt.unsafe
	case Stmt:
		return v.unsafe
	case *Stmt:
		return v.unsafe
	case qStmt:
		return v.unsafe
	case *qStmt:
		return v.unsafe
	case DB:
		return v.unsafe
	case *DB:
		return v.unsafe
	case Tx:
		return v.unsafe
	case *Tx:
		return v.unsafe
	case sql.Rows, *sql.Rows:
		return false
	default:
		return false
	}
}

var _scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

//lint:ignore U1000 ignoring this for now
var _valuerInterface = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

// Preparex prepares a statement.
func Preparex(p Preparer, query string) (*Stmt, error) {
	s, err := p.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{Stmt: s, unsafe: isUnsafe(p), Mapper: mapperFor(p)}, err
}

// Select executes a query using the provided Queryer, and StructScans each row
// into dest, which must be a slice.  If the slice elements are scannable, then
// the result set must have only one column.  Otherwise, StructScan is used.
// The *sql.Rows are closed automatically.
// Any placeholder parameters are replaced with supplied args.
func Select(q Queryer, dest any, query string, args ...any) error {
	rows, err := q.Queryx(query, args...)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return scanAll(rows, dest, false)
}

// Get does a QueryRow using the provided Queryer, and scans the resulting row
// to dest.  If dest is scannable, the result must only have one column.  Otherwise,
// StructScan is used.  Get will return sql.ErrNoRows like row.Scan would.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func Get(q Queryer, dest any, query string, args ...any) error {
	r := q.QueryRowx(query, args...)
	return r.scanAny(dest, false)
}

// LoadFile exec's every statement in a file (as a single call to Exec).
// LoadFile may return a nil *sql.Result if errors are encountered locating or
// reading the file at path.  LoadFile reads the entire file into memory, so it
// is not suitable for loading large data dumps, but can be useful for initializing
// schemas or loading indexes.
func LoadFile(e Execer, path string) (*sql.Result, error) {
	realpath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	contents, err := os.ReadFile(realpath)
	if err != nil {
		return nil, err
	}
	res, err := e.Exec(string(contents))
	return &res, err
}

// MustExec execs the query using e and panics if there was an error.
// Any placeholder parameters are replaced with supplied args.
func MustExec(e Execer, query string, args ...any) sql.Result {
	res, err := e.Exec(query, args...)
	if err != nil {
		panic(err)
	}
	return res
}

// SliceScan a row, returning a []any with values similar to MapScan.
// This function is primarily intended for use where the number of columns
// is not known.  Because you can pass an []any directly to Scan,
// it's recommended that you do that as it will not have to allocate new
// slices per row.
func SliceScan(r ColScanner) ([]any, error) {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return []any{}, err
	}

	values := make([]any, len(columns))
	for i := range values {
		values[i] = new(any)
	}

	if err := r.Scan(values...); err != nil {
		return values, err
	}

	for i := range columns {
		values[i] = *(values[i].(*any))
	}

	return values, r.Err()
}

// MapScan scans a single Row into the dest map[string]any.
// Use this to get results for SQL that might not be under your control
// (for instance, if you're building an interface for an SQL server that
// executes SQL from input).  Please do not use this as a primary interface!
// This will modify the map sent to it in place, so reuse the same map with
// care.  Columns which occur more than once in the result will overwrite
// each other!
func MapScan(r ColScanner, dest map[string]any) error {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return err
	}

	values := make([]any, len(columns))
	for i := range values {
		values[i] = new(any)
	}

	err = r.Scan(values...)
	if err != nil {
		return err
	}

	for i, column := range columns {
		dest[column] = *(values[i].(*any))
	}

	return r.Err()
}

type rowsi interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(...any) error
}

// structOnlyError returns an error appropriate for type when a non-scannable
// struct is expected but something else is given
func structOnlyError(t reflect.Type) error {
	isStruct := t.Kind() == reflect.Struct
	isScanner := reflect.PointerTo(t).Implements(_scannerInterface)
	if !isStruct {
		return fmt.Errorf("expected %s but got %s", reflect.Struct, t.Kind())
	}
	if isScanner {
		return fmt.Errorf("structscan expects a struct dest but the provided struct type %s implements scanner", t.Name())
	}
	return fmt.Errorf("expected a struct, but struct %s has no exported fields", t.Name())
}

// scanAll scans all rows into a destination, which must be a slice of any
// type.  It resets the slice length to zero before appending each element to
// the slice.  If the destination slice type is a Struct, then StructScan will
// be used on each row.  If the destination is some other kind of base type,
// then each row must only have one column which can scan into that type.  This
// allows you to do something like:
//
//	rows, _ := db.Query("select id from people;")
//	var ids []int
//	scanAll(rows, &ids, false)
//
// and ids will be a list of the id results.  I realize that this is a desirable
// interface to expose to users, but for now it will only be exposed via changes
// to `Get` and `Select`.  The reason that this has been implemented like this is
// this is the only way to not duplicate reflect work in the new API while
// maintaining backwards compatibility.
func scanAll(rows rowsi, dest any, structOnly bool) error {
	var v, vp reflect.Value

	value := reflect.ValueOf(dest)

	// json.Unmarshal returns errors for these
	if value.Kind() != reflect.Pointer {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}
	direct := reflect.Indirect(value)

	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return err
	}
	direct.SetLen(0)

	isPtr := slice.Elem().Kind() == reflect.Pointer
	base := reflectx.Deref(slice.Elem())
	scannable := isScannable(base)

	if structOnly && scannable {
		return structOnlyError(base)
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// if it's a base type make sure it only has 1 column;  if not return an error
	if scannable && len(columns) > 1 {
		return fmt.Errorf("non-struct dest type %s with >1 columns (%d)", base.Kind(), len(columns))
	}

	if !scannable {
		var values []any
		var m *reflectx.Mapper

		switch rows := rows.(type) {
		case *Rows:
			m = rows.Mapper
		default:
			m = mapper()
		}

		fields := m.TraversalsByName(base, columns)
		// if we are not unsafe and are missing fields, return an error
		if f, err := missingFields(fields); err != nil && !isUnsafe(rows) {
			return fmt.Errorf("missing destination name %s in %T", columns[f], dest)
		}
		values = make([]any, len(columns))

		for rows.Next() {
			// create a new struct type (which returns PtrTo) and indirect it
			vp = reflect.New(base)
			v = reflect.Indirect(vp)

			err = fieldsByTraversal(v, fields, values, true)
			if err != nil {
				return err
			}

			// scan into the struct field pointers and append to our results
			err = rows.Scan(values...)
			if err != nil {
				return err
			}

			if isPtr {
				direct.Set(reflect.Append(direct, vp))
			} else {
				direct.Set(reflect.Append(direct, v))
			}
		}
	} else {
		for rows.Next() {
			vp = reflect.New(base)
			err = rows.Scan(vp.Interface())
			if err != nil {
				return err
			}
			// append
			if isPtr {
				direct.Set(reflect.Append(direct, vp))
			} else {
				direct.Set(reflect.Append(direct, reflect.Indirect(vp)))
			}
		}
	}

	return rows.Err()
}

// StructScan all rows from an sql.Rows or an sqlx.Rows into the dest slice.
// StructScan will scan in the entire rows result, so if you do not want to
// allocate structs for the entire result, use Queryx and see sqlx.Rows.StructScan.
// If rows is sqlx.Rows, it will use its mapper, otherwise it will use the default.
func StructScan(rows rowsi, dest any) error {
	return scanAll(rows, dest, true)
}

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = reflectx.Deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}

// fieldsByName fills a values interface with fields from the passed value based
// on the traversals in int.  If ptrs is true, return addresses instead of values.
// We write this instead of using FieldsByName to save allocations and map lookups
// when iterating over many rows.  Empty traversals will get an interface pointer.
// Because of the necessity of requesting ptrs or values, it's considered a bit too
// specialized for inclusion in reflectx itself.
func fieldsByTraversal(v reflect.Value, traversals [][]int, values []any, ptrs bool) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			values[i] = new(any)
			continue
		}
		f := reflectx.FieldByIndexes(v, traversal)
		if ptrs {
			values[i] = f.Addr().Interface()
		} else {
			values[i] = f.Interface()
		}
	}
	return nil
}

func missingFields(transversals [][]int) (field int, err error) {
	for i, t := range transversals {
		if len(t) == 0 {
			return i, errors.New("missing field")
		}
	}
	return 0, nil
}
