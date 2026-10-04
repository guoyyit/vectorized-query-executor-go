package executor

import "fmt"

// Row is one record.
type Row []Value

// Expr is a scalar expression evaluated against a row (row-at-a-time path)
// or against a batch (vectorized path). Both evaluations must agree.
type Expr interface {
	// Eval evaluates the expression for a single row.
	Eval(row Row) (Value, error)
	// evalVec evaluates the expression for the selected rows of b and
	// returns a freshly allocated vector (length b.N). Rows not selected
	// in b are left NULL and must never be observed downstream.
	evalVec(b *Batch) (*Vector, error)
	String() string
}

// Literal is a constant value.
type Literal struct{ Val Value }

func (l Literal) Eval(Row) (Value, error) { return l.Val, nil }
func (l Literal) String() string          { return fmt.Sprintf("lit(%v)", l.Val) }

// Column references the row column at Idx.
type Column struct{ Idx int }

func (c Column) Eval(row Row) (Value, error) {
	if c.Idx < 0 || c.Idx >= len(row) {
		return Value{}, ErrSchema
	}
	return row[c.Idx], nil
}
func (c Column) String() string { return fmt.Sprintf("col(%d)", c.Idx) }

// UnaryOp enumerates unary operators.
type UnaryOp int

const (
	OpNeg UnaryOp = iota // arithmetic negation
	OpNot                // boolean NOT (three-valued)
)

// Unary applies Op to X.
type Unary struct {
	Op UnaryOp
	X  Expr
}

func (u Unary) Eval(row Row) (Value, error) {
	v, err := u.X.Eval(row)
	if err != nil {
		return Value{}, err
	}
	switch u.Op {
	case OpNeg:
		return Neg(v)
	case OpNot:
		if v.Typ != TypeBool {
			return Value{}, ErrTypeMismatch
		}
		if v.Null {
			return NullOf(TypeBool), nil
		}
		return BoolVal(!v.B), nil
	}
	return Value{}, ErrTypeMismatch
}
func (u Unary) String() string { return fmt.Sprintf("unary(%d,%s)", u.Op, u.X) }

// BinOp enumerates binary operators.
type BinOp int

const (
	OpAdd BinOp = iota
	OpSub
	OpMul
	OpDiv
	OpEq
	OpNE
	OpLT
	OpLE
	OpGT
	OpGE
	OpAnd // three-valued AND
	OpOr  // three-valued OR
)

// Binary applies Op to L and R.
type Binary struct {
	Op BinOp
	L  Expr
	R  Expr
}

func (b Binary) Eval(row Row) (Value, error) {
	l, err := b.L.Eval(row)
	if err != nil {
		return Value{}, err
	}
	r, err := b.R.Eval(row)
	if err != nil {
		return Value{}, err
	}
	return ApplyBinOp(b.Op, l, r)
}

// ApplyBinOp applies op to already-evaluated operands. It is shared by the
// row and vectorized paths so semantics cannot diverge.
func ApplyBinOp(op BinOp, l, r Value) (Value, error) {
	switch op {
	case OpAdd:
		return Add(l, r)
	case OpSub:
		return Sub(l, r)
	case OpMul:
		return Mul(l, r)
	case OpDiv:
		return Div(l, r)
	case OpAnd:
		return logicAnd(l, r)
	case OpOr:
		return logicOr(l, r)
	default:
		return compareOp(op, l, r)
	}
}

func boolOperand(v Value) error {
	if v.Typ != TypeBool {
		return ErrTypeMismatch
	}
	return nil
}

// logicAnd implements SQL three-valued AND.
func logicAnd(l, r Value) (Value, error) {
	if err := boolOperand(l); err != nil {
		return Value{}, err
	}
	if err := boolOperand(r); err != nil {
		return Value{}, err
	}
	if (!l.Null && !l.B) || (!r.Null && !r.B) {
		return BoolVal(false), nil
	}
	if l.Null || r.Null {
		return NullOf(TypeBool), nil
	}
	return BoolVal(l.B && r.B), nil
}

// logicOr implements SQL three-valued OR.
func logicOr(l, r Value) (Value, error) {
	if err := boolOperand(l); err != nil {
		return Value{}, err
	}
	if err := boolOperand(r); err != nil {
		return Value{}, err
	}
	if (!l.Null && l.B) || (!r.Null && r.B) {
		return BoolVal(true), nil
	}
	if l.Null || r.Null {
		return NullOf(TypeBool), nil
	}
	return BoolVal(l.B || r.B), nil
}

// compareOp evaluates a comparison; NULL in either operand yields NULL.
func compareOp(op BinOp, l, r Value) (Value, error) {
	if l.Null || r.Null {
		return NullOf(TypeBool), nil
	}
	c, err := Compare(l, r)
	if err != nil {
		return Value{}, err
	}
	var res bool
	switch op {
	case OpEq:
		res = c == 0
	case OpNE:
		res = c != 0
	case OpLT:
		res = c < 0
	case OpLE:
		res = c <= 0
	case OpGT:
		res = c > 0
	case OpGE:
		res = c >= 0
	default:
		return Value{}, ErrTypeMismatch
	}
	return BoolVal(res), nil
}
func (b Binary) String() string {
	return fmt.Sprintf("bin(%d,%s,%s)", b.Op, b.L, b.R)
}

// IsNull tests whether X evaluates to NULL (or non-NULL when Negated).
type IsNull struct {
	X       Expr
	Negated bool
}

func (n IsNull) Eval(row Row) (Value, error) {
	v, err := n.X.Eval(row)
	if err != nil {
		return Value{}, err
	}
	return BoolVal(v.Null != n.Negated), nil
}
func (n IsNull) String() string { return fmt.Sprintf("isnull(%s,%v)", n.X, n.Negated) }
