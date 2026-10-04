// Package vector implements the columnar batch data model used by the
// vectorized execution path. A Batch holds N rows laid out by column.
package vector

import (
	"fmt"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
)

// Column is a single typed column of a Batch. Null[i] reports whether row i is
// NULL; the typed slice still contains a (zero) value in that case.
type Column struct {
	Type types.T
	Null []bool
	I64  []int64
	F64  []float64
	Str  []string
	Bool []bool
}

// Batch is a columnar block of N rows.
type Batch struct {
	Cols []*Column
	N    int
}

// NewColumn allocates a column with capacity for cap rows.
func NewColumn(t types.T, cap int) *Column {
	c := &Column{Type: t}
	switch t {
	case types.Int64:
		c.I64 = make([]int64, 0, cap)
	case types.Float64:
		c.F64 = make([]float64, 0, cap)
	case types.String:
		c.Str = make([]string, 0, cap)
	case types.Bool:
		c.Bool = make([]bool, 0, cap)
	default:
		panic(fmt.Sprintf("vector: unknown column type %s", t))
	}
	c.Null = make([]bool, 0, cap)
	return c
}

// NewBatch allocates a batch with the given schema and capacity.
func NewBatch(schema []types.T, cap int) *Batch {
	b := &Batch{Cols: make([]*Column, len(schema))}
	for i, t := range schema {
		b.Cols[i] = NewColumn(t, cap)
	}
	return b
}

// Append appends one datum to a column.
func (c *Column) Append(d types.Datum) {
	if d.T != c.Type {
		panic(fmt.Sprintf("vector: append %s to %s column", d.T, c.Type))
	}
	c.Null = append(c.Null, d.Null)
	switch c.Type {
	case types.Int64:
		c.I64 = append(c.I64, d.I)
	case types.Float64:
		c.F64 = append(c.F64, d.F)
	case types.String:
		c.Str = append(c.Str, d.S)
	case types.Bool:
		c.Bool = append(c.Bool, d.B)
	}
}

// Get returns the datum at row i.
func (c *Column) Get(i int) types.Datum {
	d := types.Datum{T: c.Type, Null: c.Null[i]}
	if c.Null[i] {
		return d
	}
	switch c.Type {
	case types.Int64:
		d.I = c.I64[i]
	case types.Float64:
		d.F = c.F64[i]
	case types.String:
		d.S = c.Str[i]
	case types.Bool:
		d.B = c.Bool[i]
	}
	return d
}

// Set overwrites row i with datum d (used by in-place vectorized kernels).
func (c *Column) Set(i int, d types.Datum) {
	if d.T != c.Type {
		panic(fmt.Sprintf("vector: set %s into %s column", d.T, c.Type))
	}
	c.Null[i] = d.Null
	switch c.Type {
	case types.Int64:
		c.I64[i] = d.I
	case types.Float64:
		c.F64[i] = d.F
	case types.String:
		c.Str[i] = d.S
	case types.Bool:
		c.Bool[i] = d.B
	}
}

// AppendRow appends a full row to the batch.
func (b *Batch) AppendRow(row []types.Datum) {
	if len(row) != len(b.Cols) {
		panic(fmt.Sprintf("vector: append row with %d values to batch with %d columns", len(row), len(b.Cols)))
	}
	for i, d := range row {
		b.Cols[i].Append(d)
	}
	b.N++
}

// Row returns the i-th row as datums. The returned slice is freshly
// allocated; callers that need to retain rows should copy them.
func (b *Batch) Row(i int) []types.Datum {
	row := make([]types.Datum, len(b.Cols))
	for j, c := range b.Cols {
		row[j] = c.Get(i)
	}
	return row
}

// Schema returns the column types.
func (b *Batch) Schema() []types.T {
	s := make([]types.T, len(b.Cols))
	for i, c := range b.Cols {
		s[i] = c.Type
	}
	return s
}

// Compact returns a new batch containing only the rows selected by sel. This
// is the vectorized filter primitive: rows not selected are physically removed
// so they cannot participate in any later operator.
func (b *Batch) Compact(sel []int) *Batch {
	out := NewBatch(b.Schema(), len(sel))
	for ci, c := range b.Cols {
		nc := out.Cols[ci]
		for _, ri := range sel {
			nc.Append(c.Get(ri))
		}
	}
	out.N = len(sel)
	return out
}

// Project returns a new batch containing only the given column indices.
func (b *Batch) Project(cols []int) *Batch {
	schema := make([]types.T, len(cols))
	for i, ci := range cols {
		schema[i] = b.Cols[ci].Type
	}
	out := NewBatch(schema, b.N)
	for i, ci := range cols {
		c := b.Cols[ci]
		nc := out.Cols[i]
		for ri := 0; ri < b.N; ri++ {
			nc.Append(c.Get(ri))
		}
	}
	out.N = b.N
	return out
}

// AppendBatch appends all rows of src to b.
func (b *Batch) AppendBatch(src *Batch) {
	if len(src.Cols) != len(b.Cols) {
		panic("vector: append batch with different column count")
	}
	for ri := 0; ri < src.N; ri++ {
		for ci, c := range src.Cols {
			b.Cols[ci].Append(c.Get(ri))
		}
	}
	b.N += src.N
}
