package executor

// Vectorized evaluation. Each evalVec fills a fresh vector of length b.N for
// the selected rows of b; unselected rows are NULL and must never be
// observed by downstream operators. Scalar semantics are shared with the row
// path through ApplyBinOp/Neg/Compare so the two paths cannot diverge.

func eachSelected(b *Batch, f func(i int)) {
	if b.Sel == nil {
		for i := 0; i < b.N; i++ {
			f(i)
		}
		return
	}
	for _, i := range b.Sel {
		f(i)
	}
}

func (l Literal) evalVec(b *Batch) (*Vector, error) {
	out := NewVector(l.Val.Typ, b.N)
	eachSelected(b, func(i int) { out.SetValue(i, l.Val) })
	return out, nil
}

func (c Column) evalVec(b *Batch) (*Vector, error) {
	if c.Idx < 0 || c.Idx >= len(b.Cols) {
		return nil, ErrSchema
	}
	return b.Cols[c.Idx], nil
}

func (u Unary) evalVec(b *Batch) (*Vector, error) {
	xv, err := u.X.evalVec(b)
	if err != nil {
		return nil, err
	}
	switch u.Op {
	case OpNeg:
		if xv.Typ != TypeInt64 && xv.Typ != TypeFloat64 {
			return nil, ErrTypeMismatch
		}
		out := NewVector(xv.Typ, b.N)
		var evalErr error
		eachSelected(b, func(i int) {
			if evalErr != nil {
				return
			}
			var v Value
			v, evalErr = Neg(xv.ValueAt(i))
			if evalErr == nil {
				out.SetValue(i, v)
			}
		})
		return out, evalErr
	case OpNot:
		if xv.Typ != TypeBool {
			return nil, ErrTypeMismatch
		}
		out := NewVector(TypeBool, b.N)
		eachSelected(b, func(i int) {
			if xv.Nulls[i] {
				out.Nulls[i] = true
				return
			}
			out.Bs[i] = !xv.Bs[i]
		})
		return out, nil
	}
	return nil, ErrTypeMismatch
}

// vecArithType validates operand types for arithmetic and returns the output
// type (float64 if either side is float).
func vecArithType(l, r Type) (Type, error) {
	if !isNumeric(l) || !isNumeric(r) {
		return TypeInt64, ErrTypeMismatch
	}
	if l == TypeFloat64 || r == TypeFloat64 {
		return TypeFloat64, nil
	}
	return TypeInt64, nil
}

func (bin Binary) evalVec(b *Batch) (*Vector, error) {
	lv, err := bin.L.evalVec(b)
	if err != nil {
		return nil, err
	}
	rv, err := bin.R.evalVec(b)
	if err != nil {
		return nil, err
	}
	var outTyp Type
	switch bin.Op {
	case OpAdd, OpSub, OpMul, OpDiv:
		if outTyp, err = vecArithType(lv.Typ, rv.Typ); err != nil {
			return nil, err
		}
	case OpEq, OpNE, OpLT, OpLE, OpGT, OpGE:
		num := isNumeric(lv.Typ) && isNumeric(rv.Typ)
		if !num && lv.Typ != rv.Typ {
			return nil, ErrTypeMismatch
		}
		outTyp = TypeBool
	case OpAnd, OpOr:
		if lv.Typ != TypeBool || rv.Typ != TypeBool {
			return nil, ErrTypeMismatch
		}
		outTyp = TypeBool
	default:
		return nil, ErrTypeMismatch
	}
	out := NewVector(outTyp, b.N)
	var evalErr error
	eachSelected(b, func(i int) {
		if evalErr != nil {
			return
		}
		var v Value
		v, evalErr = ApplyBinOp(bin.Op, lv.ValueAt(i), rv.ValueAt(i))
		if evalErr == nil {
			out.SetValue(i, v)
		}
	})
	if evalErr != nil {
		return nil, evalErr
	}
	return out, nil
}

func (n IsNull) evalVec(b *Batch) (*Vector, error) {
	xv, err := n.X.evalVec(b)
	if err != nil {
		return nil, err
	}
	out := NewVector(TypeBool, b.N)
	eachSelected(b, func(i int) {
		out.Bs[i] = xv.Nulls[i] != n.Negated
	})
	return out, nil
}
