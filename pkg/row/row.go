// Package row defines the row-oriented data model used by the row-by-row
// (Volcano-style) reference execution path.
package row

import (
	"fmt"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
)

// Row is a single record: a slice of nullable datums.
type Row struct {
	Vals []types.Datum
}

// NewRow allocates a row with n columns.
func NewRow(n int) *Row {
	return &Row{Vals: make([]types.Datum, n)}
}

// Schema returns the column types of the row.
func (r *Row) Schema() []types.T {
	s := make([]types.T, len(r.Vals))
	for i, d := range r.Vals {
		s[i] = d.T
	}
	return s
}

// Clone returns a deep copy of the row. Datums are value types, so copying the
// slice is sufficient.
func (r *Row) Clone() *Row {
	vals := make([]types.Datum, len(r.Vals))
	copy(vals, r.Vals)
	return &Row{Vals: vals}
}

func (r *Row) String() string {
	s := "["
	for i, d := range r.Vals {
		if i > 0 {
			s += ", "
		}
		s += d.String()
	}
	return s + "]"
}

// CheckSchema panics if rows do not all have the same width.
func CheckSchema(rows []*Row) error {
	var w int
	for i, r := range rows {
		if i == 0 {
			w = len(r.Vals)
			continue
		}
		if len(r.Vals) != w {
			return fmt.Errorf("row width mismatch: %d vs %d", w, len(r.Vals))
		}
	}
	return nil
}
