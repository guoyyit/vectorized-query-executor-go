// Package agg defines the aggregate functions and per-group accumulation
// state shared by both execution paths. Keeping the accumulation logic here
// guarantees that the row-by-row and vectorized paths produce identical
// aggregate results (NULL handling, empty groups, overflow behavior).
package agg

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/expr"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/row"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vector"
)

// Func identifies an aggregate function.
type Func uint8

const (
	CountStar Func = iota // COUNT(*)
	Count                 // COUNT(expr)
	Sum                   // SUM(expr)
	Avg                   // AVG(expr)
	Min                   // MIN(expr)
	Max                   // MAX(expr)
)

func (f Func) String() string {
	switch f {
	case CountStar:
		return "COUNT(*)"
	case Count:
		return "COUNT"
	case Sum:
		return "SUM"
	case Avg:
		return "AVG"
	case Min:
		return "MIN"
	case Max:
		return "MAX"
	default:
		return "?"
	}
}

// Def is an aggregate over an input expression. Expr is nil for CountStar.
type Def struct {
	Func Func
	Expr expr.Expr
}

// ResultType returns the type of the aggregate result given the input schema.
func (d Def) ResultType(schema []types.T) (types.T, error) {
	switch d.Func {
	case CountStar, Count:
		return types.Int64, nil
	case Avg:
		return types.Float64, nil
	case Sum, Min, Max:
		if d.Expr == nil {
			return 0, fmt.Errorf("aggregate %s requires an expression", d.Func)
		}
		return d.Expr.InferType(schema)
	default:
		return 0, fmt.Errorf("unknown aggregate function %d", d.Func)
	}
}

// State accumulates aggregate values for a single group.
type State struct {
	CountStar int64
	Count     int64 // non-null inputs
	SumI      int64
	SumF      float64
	Min       types.Datum
	Max       types.Datum
	hasMin    bool
	hasMax    bool
}

// AddRow accumulates one row (row-by-row path).
func (s *State) AddRow(r *row.Row, defs []Def) error {
	s.CountStar++
	for i, d := range defs {
		switch d.Func {
		case CountStar:
			// already counted
		case Count:
			v, err := d.Expr.Eval(r)
			if err != nil {
				return err
			}
			if !v.Null {
				s.Count++
			}
		case Sum, Avg, Min, Max:
			v, err := d.Expr.Eval(r)
			if err != nil {
				return err
			}
			if v.Null {
				continue
			}
			if err := s.accum(i, d, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// AddDatum accumulates one already-evaluated datum for aggregate i. Used by
// the vectorized path after columnar expression evaluation. The caller is
// responsible for incrementing CountStar once per row.
func (s *State) AddDatum(i int, d Def, v types.Datum) error {
	switch d.Func {
	case CountStar:
		return nil
	case Count:
		if !v.Null {
			s.Count++
		}
		return nil
	case Sum, Avg, Min, Max:
		if v.Null {
			return nil
		}
		return s.accum(i, d, v)
	}
	return nil
}

func (s *State) accum(i int, d Def, v types.Datum) error {
	switch d.Func {
	case Sum, Avg:
		switch v.T {
		case types.Int64:
			if d.Func == Avg {
				// Accumulate AVG in float64 to avoid int64 sum overflow; the
				// result is float64 anyway.
				s.SumF += float64(v.I)
			} else {
				if (v.I > 0 && s.SumI > math.MaxInt64-v.I) || (v.I < 0 && s.SumI < math.MinInt64-v.I) {
					return fmt.Errorf("%w: SUM overflow", types.ErrOverflow)
				}
				s.SumI += v.I
			}
		case types.Float64:
			s.SumF += v.F
		default:
			return fmt.Errorf("%w: SUM/AVG unsupported for %s", types.ErrTypeMismatch, v.T)
		}
		s.Count++
	case Min:
		if !s.hasMin {
			s.Min = v
			s.hasMin = true
		} else {
			cmp, err := types.Compare(v, s.Min)
			if err != nil {
				return err
			}
			if cmp < 0 {
				s.Min = v
			}
		}
	case Max:
		if !s.hasMax {
			s.Max = v
			s.hasMax = true
		} else {
			cmp, err := types.Compare(v, s.Max)
			if err != nil {
				return err
			}
			if cmp > 0 {
				s.Max = v
			}
		}
	}
	return nil
}

// Result returns the aggregate result datums for the group.
func (s *State) Result(defs []Def, inputSchema []types.T) []types.Datum {
	out := make([]types.Datum, len(defs))
	for i, d := range defs {
		switch d.Func {
		case CountStar:
			out[i] = types.NewInt64(s.CountStar)
		case Count:
			out[i] = types.NewInt64(s.Count)
		case Sum:
			if s.Count == 0 {
				t, _ := d.ResultType(inputSchema)
				out[i] = types.NewNull(t)
			} else {
				t, _ := d.ResultType(inputSchema)
				if t == types.Int64 {
					out[i] = types.NewInt64(s.SumI)
				} else {
					out[i] = types.NewFloat64(s.SumF)
				}
			}
		case Avg:
			if s.Count == 0 {
				out[i] = types.NewNull(types.Float64)
			} else {
				out[i] = types.NewFloat64(s.SumF / float64(s.Count))
			}
		case Min:
			if s.Count == 0 {
				t, _ := d.ResultType(inputSchema)
				out[i] = types.NewNull(t)
			} else {
				out[i] = s.Min
			}
		case Max:
			if s.Count == 0 {
				t, _ := d.ResultType(inputSchema)
				out[i] = types.NewNull(t)
			} else {
				out[i] = s.Max
			}
		}
	}
	return out
}

// ResultTypes returns the result schema for the given aggregate definitions.
func ResultTypes(defs []Def, inputSchema []types.T) ([]types.T, error) {
	ts := make([]types.T, len(defs))
	for i, d := range defs {
		t, err := d.ResultType(inputSchema)
		if err != nil {
			return nil, err
		}
		ts[i] = t
	}
	return ts, nil
}

// ---- Group key encoding ----

const (
	tNullInt    = 0
	tNullFloat  = 1
	tNullString = 2
	tNullBool   = 3
	tInt64      = 4
	tFloat64    = 5
	tString     = 6
	tBool       = 7
)

// EncodeKey encodes group key datums into a comparable string. NULLs of the
// same type encode identically and group together.
func EncodeKey(keys []types.Datum) string {
	var b strings.Builder
	var buf [8]byte
	for _, d := range keys {
		if d.Null {
			switch d.T {
			case types.Int64:
				b.WriteByte(tNullInt)
			case types.Float64:
				b.WriteByte(tNullFloat)
			case types.String:
				b.WriteByte(tNullString)
			case types.Bool:
				b.WriteByte(tNullBool)
			}
			continue
		}
		switch d.T {
		case types.Int64:
			b.WriteByte(tInt64)
			binary.BigEndian.PutUint64(buf[:], uint64(d.I))
			b.Write(buf[:])
		case types.Float64:
			b.WriteByte(tFloat64)
			binary.BigEndian.PutUint64(buf[:], math.Float64bits(d.F))
			b.Write(buf[:])
		case types.String:
			b.WriteByte(tString)
			var lb [4]byte
			binary.BigEndian.PutUint32(lb[:], uint32(len(d.S)))
			b.Write(lb[:])
			b.WriteString(d.S)
		case types.Bool:
			b.WriteByte(tBool)
			if d.B {
				b.WriteByte(1)
			} else {
				b.WriteByte(0)
			}
		}
	}
	return b.String()
}

// DecodeKey decodes a key back into datums (used when emitting group rows).
func DecodeKey(s string, schema []types.T) ([]types.Datum, error) {
	out := make([]types.Datum, 0, len(schema))
	i := 0
	for i < len(s) {
		tag := s[i]
		i++
		switch tag {
		case tNullInt:
			out = append(out, types.NewNull(types.Int64))
		case tNullFloat:
			out = append(out, types.NewNull(types.Float64))
		case tNullString:
			out = append(out, types.NewNull(types.String))
		case tNullBool:
			out = append(out, types.NewNull(types.Bool))
		case tInt64:
			if i+8 > len(s) {
				return nil, fmt.Errorf("agg: truncated int64 key")
			}
			out = append(out, types.NewInt64(int64(binary.BigEndian.Uint64([]byte(s[i:i+8])))))
			i += 8
		case tFloat64:
			if i+8 > len(s) {
				return nil, fmt.Errorf("agg: truncated float64 key")
			}
			out = append(out, types.NewFloat64(math.Float64frombits(binary.BigEndian.Uint64([]byte(s[i:i+8])))))
			i += 8
		case tString:
			if i+4 > len(s) {
				return nil, fmt.Errorf("agg: truncated string key length")
			}
			l := binary.BigEndian.Uint32([]byte(s[i : i+4]))
			i += 4
			if i+int(l) > len(s) {
				return nil, fmt.Errorf("agg: truncated string key")
			}
			out = append(out, types.NewString(s[i:i+int(l)]))
			i += int(l)
		case tBool:
			if i >= len(s) {
				return nil, fmt.Errorf("agg: truncated bool key")
			}
			out = append(out, types.NewBool(s[i] != 0))
			i++
		default:
			return nil, fmt.Errorf("agg: unknown key tag %d", tag)
		}
	}
	if len(out) != len(schema) {
		return nil, fmt.Errorf("agg: key has %d fields, schema has %d", len(out), len(schema))
	}
	return out, nil
}

// EvalBatchKeys evaluates key expressions over a batch and returns one
// encoded key per row.
func EvalBatchKeys(exprs []expr.Expr, b *vector.Batch) ([]string, error) {
	cols := make([]*vector.Column, len(exprs))
	for i, e := range exprs {
		c, err := e.EvalBatch(b)
		if err != nil {
			return nil, err
		}
		cols[i] = c
	}
	keys := make([]string, b.N)
	var buf [8]byte
	var lb [4]byte
	var sb strings.Builder
	for ri := 0; ri < b.N; ri++ {
		sb.Reset()
		for _, c := range cols {
			d := c.Get(ri)
			if d.Null {
				switch d.T {
				case types.Int64:
					sb.WriteByte(tNullInt)
				case types.Float64:
					sb.WriteByte(tNullFloat)
				case types.String:
					sb.WriteByte(tNullString)
				case types.Bool:
					sb.WriteByte(tNullBool)
				}
				continue
			}
			switch d.T {
			case types.Int64:
				sb.WriteByte(tInt64)
				binary.BigEndian.PutUint64(buf[:], uint64(d.I))
				sb.Write(buf[:])
			case types.Float64:
				sb.WriteByte(tFloat64)
				binary.BigEndian.PutUint64(buf[:], math.Float64bits(d.F))
				sb.Write(buf[:])
			case types.String:
				sb.WriteByte(tString)
				binary.BigEndian.PutUint32(lb[:], uint32(len(d.S)))
				sb.Write(lb[:])
				sb.WriteString(d.S)
			case types.Bool:
				sb.WriteByte(tBool)
				if d.B {
					sb.WriteByte(1)
				} else {
					sb.WriteByte(0)
				}
			default:
				return nil, fmt.Errorf("%w: unsupported group key type %s", types.ErrTypeMismatch, d.T)
			}
		}
		keys[ri] = sb.String()
	}
	return keys, nil
}
