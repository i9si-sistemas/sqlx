package mapper

import (
	"reflect"
	"strings"
	"sync"

	"github.com/i9si-sistemas/sqlx/reflectx"
)

var (
	// Name is used to map column names to struct field names.  By default,
	// it uses strings.ToLower to lowercase struct field names.  It can be set
	// to whatever you want, but it is encouraged to be set before sqlx is used
	// as name-to-field mappings are cached after first use on a type.
	Name = strings.ToLower
	origin = reflect.ValueOf(Name)
)

var (
	mpr *reflectx.Mapper
	// mprMu protects mpr.
	mprMu sync.Mutex
)

// New returns a valid mapper using the configured Name func.
func New() *reflectx.Mapper {
	mprMu.Lock()
	defer mprMu.Unlock()

	if mpr == nil {
		mpr = reflectx.NewMapperFunc("db", Name)
	} else if origin != reflect.ValueOf(Name) {
		// if Name has changed, create a new mapper
		mpr = reflectx.NewMapperFunc("db", Name)
		origin = reflect.ValueOf(Name)
	}
	return mpr
}
