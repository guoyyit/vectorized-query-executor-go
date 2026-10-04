package executor

// Schema describes column names and types.
type Schema struct {
	Names []string
	Types []Type
}

// ColIndex resolves a column name to its index, or -1.
func (s *Schema) ColIndex(name string) int {
	for i, n := range s.Names {
		if n == name {
			return i
		}
	}
	return -1
}

// Table is an in-process input relation.
type Table struct {
	Schema *Schema
	Rows   []Row
}

// NewTable builds a table; rows are not copied.
func NewTable(s *Schema, rows []Row) *Table { return &Table{Schema: s, Rows: rows} }

// Validate checks that every non-NULL value matches the schema column type
// and every row has the right width. NULLs are type-agnostic.
func (t *Table) Validate() error {
	if t.Schema == nil {
		return ErrSchema
	}
	width := len(t.Schema.Types)
	for ri, r := range t.Rows {
		if len(r) != width {
			return ErrSchema
		}
		for ci, v := range r {
			if !v.Null && v.Typ != t.Schema.Types[ci] {
				return &ValidationError{Row: ri, Col: ci, Want: t.Schema.Types[ci], Got: v.Typ}
			}
		}
	}
	return nil
}

// ValidationError describes a schema/value mismatch in input data.
type ValidationError struct {
	Row, Col  int
	Want, Got Type
}

func (e *ValidationError) Error() string {
	return "executor: type mismatch: row " + itoa(e.Row) + " col " + itoa(e.Col) +
		": want " + e.Want.String() + ", got " + e.Got.String()
}

// Unwrap exposes ErrTypeMismatch for errors.Is.
func (e *ValidationError) Unwrap() error { return ErrTypeMismatch }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// PartitionRange returns the [start,end) row range of partition p out of n
// contiguous, deterministic partitions over total rows. Sizes differ by at
// most one row; earlier partitions get the remainder.
func PartitionRange(total, p, n int) (start, end int) {
	if n <= 0 {
		n = 1
	}
	base := total / n
	rem := total % n
	start = p*base + min(p, rem)
	end = start + base
	if p < rem {
		end++
	}
	return start, end
}
