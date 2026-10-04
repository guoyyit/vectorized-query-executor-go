package executor

// Vector is a columnar batch of values of a single type with a null bitmap.
// Only the slice matching Typ is populated.
type Vector struct {
	Typ   Type
	Nulls []bool
	I64s  []int64
	F64s  []float64
	Strs  []string
	Bs    []bool
}

// NewVector allocates a vector with room for n rows.
func NewVector(t Type, n int) *Vector {
	v := &Vector{Typ: t, Nulls: make([]bool, n)}
	switch t {
	case TypeInt64:
		v.I64s = make([]int64, n)
	case TypeFloat64:
		v.F64s = make([]float64, n)
	case TypeString:
		v.Strs = make([]string, n)
	case TypeBool:
		v.Bs = make([]bool, n)
	}
	return v
}

// ValueAt materializes row i as a Value.
func (v *Vector) ValueAt(i int) Value {
	if v.Nulls[i] {
		return NullOf(v.Typ)
	}
	switch v.Typ {
	case TypeInt64:
		return IntVal(v.I64s[i])
	case TypeFloat64:
		return FloatVal(v.F64s[i])
	case TypeString:
		return StringVal(v.Strs[i])
	case TypeBool:
		return BoolVal(v.Bs[i])
	}
	return Value{}
}

// SetValue stores val at row i.
func (v *Vector) SetValue(i int, val Value) {
	if val.Null {
		v.Nulls[i] = true
		return
	}
	v.Nulls[i] = false
	switch v.Typ {
	case TypeInt64:
		v.I64s[i] = val.I64
	case TypeFloat64:
		v.F64s[i] = val.F64
	case TypeString:
		v.Strs[i] = val.Str
	case TypeBool:
		v.Bs[i] = val.B
	}
}

// Batch is a set of column vectors plus an optional selection vector.
// Sel==nil means all N rows are selected. Operators must treat unselected
// rows as absent.
type Batch struct {
	Cols []*Vector
	Sel  []int
	N    int
}

// NumSelected returns the number of effective rows.
func (b *Batch) NumSelected() int {
	if b.Sel == nil {
		return b.N
	}
	return len(b.Sel)
}
