package sqlx

// Named Query Support
//
//  * BindMap - bind query bindvars to map/struct args
//	* NamedExec, NamedQuery - named query w/ struct or map
//  * NamedStmt - a pre-compiled named query which is a prepared statement
//
// Internal Interfaces:
//
//  * compileNamedQuery - rebind a named query, returning a query and list of names
//  * bindArgs, bindMapArgs, bindAnyArgs - given a list of names, return an arglist
//
import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"unicode"

	"github.com/i9si-sistemas/sqlx/binder"
	"github.com/i9si-sistemas/sqlx/reflectx"
)

// NamedStmt is a prepared statement that executes named queries.  Prepare it
// how you would execute a NamedQuery, but pass in a struct or map when executing.
type NamedStmt struct {
	Params      []string
	QueryString string
	Stmt        *Stmt
}

// Close closes the named statement.
func (n *NamedStmt) Close() error {
	return n.Stmt.Close()
}

// Exec executes a named statement using the struct passed.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Exec(arg any) (sql.Result, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return *new(sql.Result), err
	}
	return n.Stmt.Exec(args...)
}

// Query executes a named statement using the struct argument, returning rows.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Query(arg any) (*sql.Rows, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return nil, err
	}
	return n.Stmt.Query(args...)
}

// QueryRow executes a named statement against the database.  Because sqlx cannot
// create a *sql.Row with an error condition pre-set for binding errors, sqlx
// returns a *sqlx.Row instead.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRow(arg any) *Row {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return &Row{err: err}
	}
	return n.Stmt.QueryRowx(args...)
}

// MustExec execs a NamedStmt, panicing on error
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) MustExec(arg any) sql.Result {
	res, err := n.Exec(arg)
	if err != nil {
		panic(err)
	}
	return res
}

// Queryx using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Queryx(arg any) (*Rows, error) {
	r, err := n.Query(arg)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, Mapper: n.Stmt.Mapper, unsafe: isUnsafe(n)}, err
}

// QueryRowx this NamedStmt.  Because of limitations with QueryRow, this is
// an alias for QueryRow.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRowx(arg any) *Row {
	return n.QueryRow(arg)
}

// Select using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Select(dest any, arg any) error {
	rows, err := n.Queryx(arg)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return scanAll(rows, dest, false)
}

// Get using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Get(dest any, arg any) error {
	r := n.QueryRowx(arg)
	return r.scanAny(dest, false)
}

// Unsafe creates an unsafe version of the NamedStmt
func (n *NamedStmt) Unsafe() *NamedStmt {
	r := &NamedStmt{Params: n.Params, Stmt: n.Stmt, QueryString: n.QueryString}
	r.Stmt.unsafe = true
	return r
}

// A union interface of preparer and binder, required to be able to prepare
// named statements (as the bindtype must be determined).
type namedPreparer interface {
	Preparer
	Binder
}

func prepareNamed(p namedPreparer, query string) (*NamedStmt, error) {
	bindType := binder.Default.Type(p.DriverName())
	q, args, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return nil, err
	}
	stmt, err := Preparex(p, q)
	if err != nil {
		return nil, err
	}
	return &NamedStmt{
		QueryString: q,
		Params:      args,
		Stmt:        stmt,
	}, nil
}

// convertMapStringInterface attempts to convert v to map[string]any.
// Unlike v.(map[string]any), this function works on named types that
// are convertible to map[string]any as well.
func convertMapStringInterface(v any) (map[string]any, bool) {
	var m map[string]any
	mtype := reflect.TypeOf(m)
	t := reflect.TypeOf(v)
	if !t.ConvertibleTo(mtype) {
		return nil, false
	}
	return reflect.ValueOf(v).Convert(mtype).Interface().(map[string]any), true

}

func bindAnyArgs(names []string, arg any, m *reflectx.Mapper) ([]any, error) {
	if maparg, ok := convertMapStringInterface(arg); ok {
		return bindMapArgs(names, maparg)
	}
	return bindArgs(names, arg, m)
}

// private interface to generate a list of interfaces from a given struct
// type, given a list of names to pull out of the struct.  Used by public
// BindStruct interface.
func bindArgs(names []string, arg any, m *reflectx.Mapper) ([]any, error) {
	arglist := make([]any, 0, len(names))

	// grab the indirected value of arg
	var v reflect.Value
	for v = reflect.ValueOf(arg); v.Kind() == reflect.Pointer; {
		v = v.Elem()
	}

	err := m.TraversalsByNameFunc(v.Type(), names, func(i int, t []int) error {
		if len(t) == 0 {
			return fmt.Errorf("could not find name %s in %#v", names[i], arg)
		}

		val := reflectx.FieldByIndexesReadOnly(v, t)
		arglist = append(arglist, val.Interface())

		return nil
	})

	return arglist, err
}

// like bindArgs, but for maps.
func bindMapArgs(names []string, arg map[string]any) ([]any, error) {
	arglist := make([]any, 0, len(names))

	for _, name := range names {
		val, ok := arg[name]
		if !ok {
			return arglist, fmt.Errorf("could not find name %s in %#v", name, arg)
		}
		arglist = append(arglist, val)
	}
	return arglist, nil
}

// bindStruct binds a named parameter query with fields from a struct argument.
// The rules for binding field names to parameter names follow the same
// conventions as for StructScan, including obeying the `db` struct tags.
func bindStruct(bindType int, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []any{}, err
	}

	arglist, err := bindAnyArgs(names, arg, m)
	if err != nil {
		return "", []any{}, err
	}

	return bound, arglist, nil
}

var valuesReg = regexp.MustCompile(`\)\s*(?i)VALUES\s*\(`)

func findMatchingClosingBracketIndex(s string) int {
	count := 0
	for i, ch := range s {
		if ch == '(' {
			count++
		}
		if ch == ')' {
			count--
			if count == 0 {
				return i
			}
		}
	}
	return 0
}

func fixBound(bound string, loop int) string {
	loc := valuesReg.FindStringIndex(bound)
	// defensive guard when "VALUES (...)" not found
	if len(loc) < 2 {
		return bound
	}

	openingBracketIndex := loc[1] - 1
	index := findMatchingClosingBracketIndex(bound[openingBracketIndex:])
	// defensive guard. must have closing bracket
	if index == 0 {
		return bound
	}
	closingBracketIndex := openingBracketIndex + index + 1

	var buffer bytes.Buffer

	buffer.WriteString(bound[0:closingBracketIndex])
	for range loop-1 {
		buffer.WriteString(",")
		buffer.WriteString(bound[openingBracketIndex:closingBracketIndex])
	}
	buffer.WriteString(bound[closingBracketIndex:])
	return buffer.String()
}

// bindArray binds a named parameter query with fields from an array or slice of
// structs argument.
func bindArray(bindType int, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	// do the initial binding with QUESTION;  if bindType is not question,
	// we can rebind it at the end.
	bound, names, err := compileNamedQuery([]byte(query), binder.QUESTION)
	if err != nil {
		return "", []any{}, err
	}
	arrayValue := reflect.ValueOf(arg)
	arrayLen := arrayValue.Len()
	if arrayLen == 0 {
		return "", []any{}, fmt.Errorf("length of array is 0: %#v", arg)
	}
	var arglist = make([]any, 0, len(names)*arrayLen)
	for i := range arrayLen {
		elemArglist, err := bindAnyArgs(names, arrayValue.Index(i).Interface(), m)
		if err != nil {
			return "", []any{}, err
		}
		arglist = append(arglist, elemArglist...)
	}
	if arrayLen > 1 {
		bound = fixBound(bound, arrayLen)
	}
	// adjust binding type if we weren't on question
	if bindType != binder.QUESTION {
		bound = binder.Default.Rebind(bindType, bound)
	}
	return bound, arglist, nil
}

// bindMap binds a named parameter query with a map of arguments.
func bindMap(bindType int, query string, args map[string]any) (string, []any, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []any{}, err
	}

	arglist, err := bindMapArgs(names, args)
	return bound, arglist, err
}

// -- Compilation of Named Queries

// Allow digits and letters in bind params;  additionally runes are
// checked against underscores, meaning that bind params can have be
// alphanumeric with underscores.  Mind the difference between unicode
// digits and numbers, where '5' is a digit but '五' is not.
var allowedBindRunes = []*unicode.RangeTable{unicode.Letter, unicode.Digit}

// compile a NamedQuery into an unbound query (using the '?' bindvar) and
// a list of names.
func compileNamedQuery(qs []byte, bindType int) (query string, names []string, err error) {
	names = make([]string, 0, 10)
	rebound := make([]byte, 0, len(qs))

	inName := false
	last := len(qs) - 1
	currentVar := 1
	name := make([]byte, 0, 10)

	for i, b := range qs {
		// a ':' while we're in a name is an error
		if b == ':' {
			// if this is the second ':' in a '::' escape sequence, append a ':'
			if inName && i > 0 && qs[i-1] == ':' {
				rebound = append(rebound, ':')
				inName = false
				continue
			} else if inName {
				err = errors.New("unexpected `:` while reading named param at " + strconv.Itoa(i))
				return query, names, err
			}
			inName = true
			name = []byte{}
		} else if inName && i > 0 && b == '=' && len(name) == 0 {
			rebound = append(rebound, ':', '=')
			inName = false
			continue
			// if we're in a name, and this is an allowed character, continue
		} else if inName && (unicode.IsOneOf(allowedBindRunes, rune(b)) || b == '_' || b == '.') && i != last {
			// append the byte to the name if we are in a name and not on the last byte
			name = append(name, b)
			// if we're in a name and it's not an allowed character, the name is done
		} else if inName {
			inName = false
			// if this is the final byte of the string and it is part of the name, then
			// make sure to add it to the name
			if i == last && unicode.IsOneOf(allowedBindRunes, rune(b)) {
				name = append(name, b)
			}
			// add the string representation to the names list
			names = append(names, string(name))
			// add a proper bindvar for the bindType
			switch bindType {
			// oracle only supports named type bind vars even for positional
			case binder.NAMED:
				rebound = append(rebound, ':')
				rebound = append(rebound, name...)
			case binder.QUESTION, binder.UNKNOWN:
				rebound = append(rebound, '?')
			case binder.DOLLAR:
				rebound = append(rebound, '$')
				for _, b := range strconv.Itoa(currentVar) {
					rebound = append(rebound, byte(b))
				}
				currentVar++
			case binder.AT:
				rebound = append(rebound, '@', 'p')
				for _, b := range strconv.Itoa(currentVar) {
					rebound = append(rebound, byte(b))
				}
				currentVar++
			}
			// add this byte to string unless it was not part of the name
			if i != last {
				rebound = append(rebound, b)
			} else if !unicode.IsOneOf(allowedBindRunes, rune(b)) {
				rebound = append(rebound, b)
			}
		} else {
			// this is a normal byte and should just go onto the rebound query
			rebound = append(rebound, b)
		}
	}

	return string(rebound), names, err
}

// BindNamed binds a struct or a map to a query with named parameters.
// DEPRECATED: use sqlx.Named` instead of this, it may be removed in future.
func BindNamed(bindType int, query string, arg any) (string, []any, error) {
	return bindNamedMapper(bindType, query, arg, mapper())
}

// Named takes a query using named parameters and an argument and
// returns a new query with a list of args that can be executed by
// a database.  The return value uses the `?` bindvar.
func Named(query string, arg any) (string, []any, error) {
	return bindNamedMapper(binder.QUESTION, query, arg, mapper())
}

func bindNamedMapper(bindType int, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	t := reflect.TypeOf(arg)
	k := t.Kind()
	switch {
	case k == reflect.Map && t.Key().Kind() == reflect.String:
		m, ok := convertMapStringInterface(arg)
		if !ok {
			return "", nil, fmt.Errorf("sqlx.bindNamedMapper: unsupported map type: %T", arg)
		}
		return bindMap(bindType, query, m)
	case k == reflect.Array || k == reflect.Slice:
		return bindArray(bindType, query, arg, m)
	default:
		return bindStruct(bindType, query, arg, m)
	}
}

// Ext is a union interface which can bind, query, and exec, used by
// NamedQuery and NamedExec.
type Ext interface {
	Binder
	Queryer
	Execer
}

// NamedQuery binds a named query and then runs Query on the result using the
// provided Ext (sqlx.Tx, sqlx.Db).  It works with both structs and with
// map[string]any types.
func NamedQuery(e Ext, query string, arg any) (*Rows, error) {
	q, args, err := bindNamedMapper(binder.Default.Type(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.Queryx(q, args...)
}

// NamedExec uses BindStruct to get a query executable by the driver and
// then runs Exec on the result.  Returns an error from the binding
// or the query execution itself.
func NamedExec(e Ext, query string, arg any) (sql.Result, error) {
	q, args, err := bindNamedMapper(binder.Default.Type(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.Exec(q, args...)
}
