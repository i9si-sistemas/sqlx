package sqlx

import (
	mpr "github.com/i9si-sistemas/sqlx/mapper"
	"github.com/i9si-sistemas/sqlx/reflectx"
)

func mapper() *reflectx.Mapper {
	return mpr.New()
}

func mapperFor(i any) *reflectx.Mapper {
	switch i := i.(type) {
	case DB:
		return i.Mapper
	case *DB:
		return i.Mapper
	case Tx:
		return i.Mapper
	case *Tx:
		return i.Mapper
	default:
		return mapper()
	}
}
