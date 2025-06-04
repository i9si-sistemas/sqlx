package sqlx

import "database/sql"

// Open is the same as sql.Open, but returns an *sqlx.DB instead.
func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return NewDb(db, driverName), nil
}

// MustOpen is the same as sql.Open, but returns an *sqlx.DB instead and panics on error.
func MustOpen(driverName, dataSourceName string) *DB {
	db, err := Open(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}
