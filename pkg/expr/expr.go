// Package expr defines scalar expressions over both rows and columnar batches.
// The same expression tree is interpreted row-by-row (reference path) and
// evaluated with columnar kernels (vectorized path); NULL propagation and
// three-valued logic are implemented once here so both paths agree.
package expr

import (
	"fmt"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/row"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vector"
)

// Op is a binary or unary operator.
type Op uint8

const (
	OpEq Op = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAdd
	OpSub
	OpMul
	OpDiv
	OpAnd
	OpOr
	OpNot
)

func (o Op) String() string {
	switch o {
	case OpEq:
		return "="
	case OpNe:
		return "<>"
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpNot:
		return "NOT"
	default:
		return "?"
	}
}

// Expr is a scalar expression.
type Expr interface {
	// Eval evaluates the expression on a single row.
	Eval(r *row.Row) (types.Datum, error)
	// EvalBatch evaluates the expression on a whole batch, returning a
	// column with one entry per input row (NULL bitmap included).
	EvalBatch(b *vector.Batch) (*vector.Column, error)
	// InferType validates the expression against an input schema and returns
	// its result type.
	InferType(schema []types.T) (types.T, error)
}

// ---- Column reference ----

// ColRef refers to the column at Index in the current input.
type ColRef struct {
	Index int
}

func Col(index int) *ColRef { return &ColRef{Index: index} }

func (e *ColRef) Eval(r *row.Row) (types.Datum, error) {
	if e.Index < 0 || e.Index >= len(r.Vals) {
		return types.Datum{}, fmt.Errorf("%w: column index %d out of range [0,%d)", types.ErrTypeMismatch, e.Index, len(r.Vals))
	}
	return r.Vals[e.Index], nil
}

func (e *ColRef) EvalBatch(b *vector.Batch) (*vector.Column, error) {
	if e.Index < 0 || e.Index >= len(b.Cols) {
		return nil, fmt.Errorf("column index %d out of range [0,%d)", e.Index, len(b.Cols))
	}
	return b.Cols[e.Index], nil
}

func (e *ColRef) InferType(schema []types.T) (types.T, error) {
	if e.Index < 0 || e.Index >= len(schema) {
		return 0, fmt.Errorf("column index %d out of range [0,%d)", e.Index, len(schema))
	}
	return schema[e.Index], nil
}

// ---- Literal ----

// Literal is a constant value.
type Literal struct {
	Value types.Datum
}

func Lit(v types.Datum) *Literal { return &Literal{Value: v} }
func IntLit(v int64) *Literal    { return &Literal{Value: types.NewInt64(v)} }
func FloatLit(v float64) *Literal { return &Literal{Value: types.NewFloat64(v)} }
func StrLit(v string) *Literal   { return &Literal{Value: types.NewString(v)} }
func BoolLit(v bool) *Literal    { return &Literal{Value: types.NewBool(v)} }

func (e *Literal) Eval(r *row.Row) (types.Datum, error) { return e.Value, nil }

func (e *Literal) EvalBatch(b *vector.Batch) (*vector.Column, error) {
	c := vector.NewColumn(e.Value.T, b.N)
	for i := 0; i < b.N; i++ {
		c.Append(e.Value)
	}
	return c, nil
}

func (e *Literal) InferType(schema []types.T) (types.T, error) { return e.Value.T, nil }

// ---- Binary expression ----

// Binary applies Op to Left and Right.
type Binary struct {
	Op          Op
	Left, Right Expr
}

func Bin(op Op, left, right Expr) *Binary {
	return &Binary{Op: op, Left: left, Right: right}
}

func Eq(l, r Expr) *Binary  { return Bin(OpEq, l, r) }
func Ne(l, r Expr) *Binary  { return Bin(OpNe, l, r) }
func Lt(l, r Expr) *Binary  { return Bin(OpLt, l, r) }
func Le(l, r Expr) *Binary  { return Bin(OpLe, l, r) }
func Gt(l, r Expr) *Binary  { return Bin(OpGt, l, r) }
func Ge(l, r Expr) *Binary  { return Bin(OpGe, l, r) }
func Add(l, r Expr) *Binary { return Bin(OpAdd, l, r) }
func Sub(l, r Expr) *Binary { return Bin(OpSub, l, r) }
func Mul(l, r Expr) *Binary { return Bin(OpMul, l, r) }
func Div(l, r Expr) *Binary { return Bin(OpDiv, l, r) }
func And(l, r Expr) *Binary { return Bin(OpAnd, l, r) }
func Or(l, r Expr) *Binary  { return Bin(OpOr, l, r) }

func isCmp(op Op) bool {
	switch op {
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		return true
	}
	return false
}

func isArith(op Op) bool {
	switch op {
	case OpAdd, OpSub, OpMul, OpDiv:
		return true
	}
	return false
}

func (e *Binary) InferType(schema []types.T) (types.T, error) {
	lt, err := e.Left.InferType(schema)
	if err != nil {
		return 0, err
	}
	rt, err := e.Right.InferType(schema)
	if err != nil {
		return 0, err
	}
	switch {
	case isCmp(e.Op):
		if lt != rt {
			return 0, fmt.Errorf("%w: compare %s with %s", types.ErrTypeMismatch, lt, rt)
		}
		return types.Bool, nil
	case isArith(e.Op):
		if lt != rt {
			return 0, fmt.Errorf("%w: arithmetic %s with %s", types.ErrTypeMismatch, lt, rt)
		}
		return lt, nil
	case e.Op == OpAnd || e.Op == OpOr:
		if lt != types.Bool || rt != types.Bool {
			return 0, fmt.Errorf("%w: %s requires bool operands, got %s and %s", types.ErrTypeMismatch, e.Op, lt, rt)
		}
		return types.Bool, nil
	default:
		return 0, fmt.Errorf("%w: unknown binary op %s", types.ErrTypeMismatch, e.Op)
	}
}

func (e *Binary) Eval(r *row.Row) (types.Datum, error) {
	l, err := e.Left.Eval(r)
	if err != nil {
		return types.Datum{}, err
	}
	ri, err := e.Right.Eval(r)
	if err != nil {
		return types.Datum{}, err
	}
	switch {
	case isCmp(e.Op):
		if l.Null || ri.Null {
			return types.NewNull(types.Bool), nil
		}
		cmp, err := types.Compare(l, ri)
		if err != nil {
			return types.Datum{}, err
		}
		switch e.Op {
		case OpEq:
			return types.NewBool(cmp == 0), nil
		case OpNe:
			return types.NewBool(cmp != 0), nil
		case OpLt:
			return types.NewBool(cmp < 0), nil
		case OpLe:
			return types.NewBool(cmp <= 0), nil
		case OpGt:
			return types.NewBool(cmp > 0), nil
		case OpGe:
			return types.NewBool(cmp >= 0), nil
		}
	case isArith(e.Op):
		switch e.Op {
		case OpAdd:
			return types.Add(l, ri)
		case OpSub:
			return types.Sub(l, ri)
		case OpMul:
			return types.Mul(l, ri)
		case OpDiv:
			return types.Div(l, ri)
		}
	case e.Op == OpAnd || e.Op == OpOr:
		return evalLogic(e.Op, l, ri), nil
	}
	return types.Datum{}, fmt.Errorf("%w: unimplemented binary op %s", types.ErrTypeMismatch, e.Op)
}

// evalLogic implements SQL three-valued logic for AND/OR on scalar datums.
func evalLogic(op Op, l, r types.Datum) types.Datum {
	switch op {
	case OpAnd:
		if (l.Null && r.Null) || (l.Null && r.B) || (r.Null && l.B) {
			return types.NewNull(types.Bool)
		}
		if (!l.Null && !l.B) || (!r.Null && !r.B) {
			return types.NewBool(false)
		}
		return types.NewBool(true)
	case OpOr:
		if (!l.Null && l.B) || (!r.Null && r.B) {
			return types.NewBool(true)
		}
		if (l.Null && r.Null) || (l.Null && !r.B) || (r.Null && !l.B) {
			return types.NewNull(types.Bool)
		}
		return types.NewBool(false)
	}
	return types.NewNull(types.Bool)
}

func (e *Binary) EvalBatch(b *vector.Batch) (*vector.Column, error) {
	lc, err := e.Left.EvalBatch(b)
	if err != nil {
		return nil, err
	}
	rc, err := e.Right.EvalBatch(b)
	if err != nil {
		return nil, err
	}
	if lc.Type != rc.Type {
		return nil, fmt.Errorf("%w: %s between %s and %s", types.ErrTypeMismatch, e.Op, lc.Type, rc.Type)
	}
	switch {
	case isCmp(e.Op):
		return cmpKernel(e.Op, lc, rc)
	case isArith(e.Op):
		return arithKernel(e.Op, lc, rc)
	case e.Op == OpAnd || e.Op == OpOr:
		return logicKernel(e.Op, lc, rc)
	default:
		return nil, fmt.Errorf("%w: unimplemented binary op %s", types.ErrTypeMismatch, e.Op)
	}
}

// cmpKernel evaluates a comparison over two columns, producing a nullable bool
// column. NULL in either operand yields NULL result.
func cmpKernel(op Op, lc, rc *vector.Column) (*vector.Column, error) {
	n := len(lc.Null)
	out := vector.NewColumn(types.Bool, n)
	lt, rt := lc.Type, rc.Type
	if lt != rt {
		return nil, fmt.Errorf("%w: compare %s with %s", types.ErrTypeMismatch, lt, rt)
	}
	for i := 0; i < n; i++ {
		if lc.Null[i] || rc.Null[i] {
			out.Null = append(out.Null, true)
			out.Bool = append(out.Bool, false)
			continue
		}
		var cmp int
		switch lt {
		case types.Int64:
			switch {
			case lc.I64[i] < rc.I64[i]:
				cmp = -1
			case lc.I64[i] > rc.I64[i]:
				cmp = 1
			}
		case types.Float64:
			switch {
			case lc.F64[i] < rc.F64[i]:
				cmp = -1
			case lc.F64[i] > rc.F64[i]:
				cmp = 1
			}
		case types.String:
			switch {
			case lc.Str[i] < rc.Str[i]:
				cmp = -1
			case lc.Str[i] > rc.Str[i]:
				cmp = 1
			}
		case types.Bool:
			li, ri := 0, 0
			if lc.Bool[i] {
				li = 1
			}
			if rc.Bool[i] {
				ri = 1
			}
			switch {
			case li < ri:
				cmp = -1
			case li > ri:
				cmp = 1
			}
		default:
			return nil, fmt.Errorf("%w: compare unsupported for %s", types.ErrTypeMismatch, lt)
		}
		var res bool
		switch op {
		case OpEq:
			res = cmp == 0
		case OpNe:
			res = cmp != 0
		case OpLt:
			res = cmp < 0
		case OpLe:
			res = cmp <= 0
		case OpGt:
			res = cmp > 0
		case OpGe:
			res = cmp >= 0
		}
		out.Null = append(out.Null, false)
		out.Bool = append(out.Bool, res)
	}
	return out, nil
}

// arithKernel evaluates arithmetic over two columns, producing a nullable
// result column of the operand type.
func arithKernel(op Op, lc, rc *vector.Column) (*vector.Column, error) {
	n := len(lc.Null)
	if lc.Type != rc.Type {
		return nil, fmt.Errorf("%w: arithmetic %s with %s", types.ErrTypeMismatch, lc.Type, rc.Type)
	}
	t := lc.Type
	out := vector.NewColumn(t, n)
	for i := 0; i < n; i++ {
		if lc.Null[i] || rc.Null[i] {
			out.Null = append(out.Null, true)
			switch t {
			case types.Int64:
				out.I64 = append(out.I64, 0)
			case types.Float64:
				out.F64 = append(out.F64, 0)
			case types.String:
				out.Str = append(out.Str, "")
			}
			continue
		}
		out.Null = append(out.Null, false)
		switch t {
		case types.Int64:
			a, b := lc.I64[i], rc.I64[i]
			var d types.Datum
			var err error
			switch op {
			case OpAdd:
				d, err = types.Add(types.NewInt64(a), types.NewInt64(b))
			case OpSub:
				d, err = types.Sub(types.NewInt64(a), types.NewInt64(b))
			case OpMul:
				d, err = types.Mul(types.NewInt64(a), types.NewInt64(b))
			case OpDiv:
				d, err = types.Div(types.NewInt64(a), types.NewInt64(b))
			}
			if err != nil {
				return nil, err
			}
			out.I64 = append(out.I64, d.I)
		case types.Float64:
			a, b := lc.F64[i], rc.F64[i]
			var v float64
			switch op {
			case OpAdd:
				v = a + b
			case OpSub:
				v = a - b
			case OpMul:
				v = a * b
			case OpDiv:
				if b == 0 {
					return nil, fmt.Errorf("%w: %g / %g", types.ErrDivByZero, a, b)
				}
				v = a / b
			}
			out.F64 = append(out.F64, v)
		case types.String:
			if op != OpAdd {
				return nil, fmt.Errorf("%w: %s unsupported for string", types.ErrTypeMismatch, op)
			}
			out.Str = append(out.Str, lc.Str[i]+rc.Str[i])
		default:
			return nil, fmt.Errorf("%w: arithmetic unsupported for %s", types.ErrTypeMismatch, t)
		}
	}
	return out, nil
}

// logicKernel evaluates AND/OR over two nullable bool columns using
// three-valued logic.
func logicKernel(op Op, lc, rc *vector.Column) (*vector.Column, error) {
	n := len(lc.Null)
	if lc.Type != types.Bool || rc.Type != types.Bool {
		return nil, fmt.Errorf("%w: %s requires bool operands", types.ErrTypeMismatch, op)
	}
	out := vector.NewColumn(types.Bool, n)
	for i := 0; i < n; i++ {
		l := types.Datum{T: types.Bool, Null: lc.Null[i], B: lc.Bool[i]}
		r := types.Datum{T: types.Bool, Null: rc.Null[i], B: rc.Bool[i]}
		res := evalLogic(op, l, r)
		out.Null = append(out.Null, res.Null)
		out.Bool = append(out.Bool, res.B)
	}
	return out, nil
}

// ---- Unary expression ----

// Unary currently implements NOT.
type Unary struct {
	Op      Op
	Operand Expr
}

func Not(e Expr) *Unary { return &Unary{Op: OpNot, Operand: e} }

func (e *Unary) InferType(schema []types.T) (types.T, error) {
	t, err := e.Operand.InferType(schema)
	if err != nil {
		return 0, err
	}
	if e.Op != OpNot || t != types.Bool {
		return 0, fmt.Errorf("%w: NOT requires bool operand, got %s", types.ErrTypeMismatch, t)
	}
	return types.Bool, nil
}

func (e *Unary) Eval(r *row.Row) (types.Datum, error) {
	v, err := e.Operand.Eval(r)
	if err != nil {
		return types.Datum{}, err
	}
	if v.Null {
		return types.NewNull(types.Bool), nil
	}
	return types.NewBool(!v.B), nil
}

func (e *Unary) EvalBatch(b *vector.Batch) (*vector.Column, error) {
	c, err := e.Operand.EvalBatch(b)
	if err != nil {
		return nil, err
	}
	if c.Type != types.Bool {
		return nil, fmt.Errorf("%w: NOT requires bool column", types.ErrTypeMismatch)
	}
	out := vector.NewColumn(types.Bool, len(c.Null))
	for i := 0; i < len(c.Null); i++ {
		out.Null = append(out.Null, c.Null[i])
		if c.Null[i] {
			out.Bool = append(out.Bool, false)
		} else {
			out.Bool = append(out.Bool, !c.Bool[i])
		}
	}
	return out, nil
}
