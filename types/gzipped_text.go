package types

import (
	"bytes"
	"compress/gzip"
	"database/sql/driver"
	"errors"
	"io"
)

// GzippedText is a []byte which transparently gzips data being submitted to
// a database and ungzips data being Scanned from a database.
type GzippedText struct {
	content []byte
}

func NewGzippedText(content ...byte) *GzippedText {
	return &GzippedText{content: content}
}

// Bytes returns the gzipped text bytes
func (g *GzippedText) Bytes() []byte {
	return g.content
}

// Value implements the driver.Valuer interface, gzipping the raw value of
// this GzippedText.
func (g GzippedText) Value() (driver.Value, error) {
	b := make([]byte, 0, len(g.Bytes()))
	buf := bytes.NewBuffer(b)
	w := gzip.NewWriter(buf)
	w.Write(g.Bytes())
	w.Close()
	return buf.Bytes(), nil

}

// ErrIncopatibleTypeForGzipText is returned when the type passed to GzippedText.Scan is incompatible
var ErrIncopatibleTypeForGzipText = errors.New("incompatible type for GzippedText")

// Scan implements the sql.Scanner interface, ungzipping the value coming off
// the wire and storing the raw result in the GzippedText.
func (g *GzippedText) Scan(src any) error {
	var source []byte
	switch src := src.(type) {
	case string:
		source = []byte(src)
	case []byte:
		source = src
	default:
		return ErrIncopatibleTypeForGzipText
	}
	reader, err := gzip.NewReader(bytes.NewReader(source))
	if err != nil {
		return err
	}
	defer reader.Close()
	b, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	g.content = b
	return nil
}
