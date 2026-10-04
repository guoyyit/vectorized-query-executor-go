package expr

import (
	"math"
	"testing"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/row"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
	"github.com/guoyyit/vectorized-query-executor-go/pkg/vector"
)

func testBatch() *vector.Batch {
	schema := []types.T{types.Int64, types.Float64, types.String, types.Bool}
	b := vector.NewBatch(schema, 5)
	b.AppendRow([]types.Datum{types.NewInt64(1), types.NewFloat64(1.5), types.NewString("a"), types.NewBool(true)})
	b.AppendRow([]types.Datum{types.NewInt64(2), types.NewFloat64(2.5), types.NewString("b"), types.NewBool(false)})
	b.AppendRow([]types.Datum{types.NewNull(types.Int64), types.NewFloat64(3.5), types.NewString("c"), types.NewBool(true)})
	b.AppendRow([]types.Datum{types.NewInt64(4), types.NewNull(types.Float64), types.NewString("d"), types.NewBool(false)})
	b.AppendRow([]types.Datum{types.NewInt64(5), types.NewFloat64(5.5), types.NewNull(types.String), types.NewNull(types.Bool)})
	return b
}

// TestExprRowVsBatch: evaluate expressions row-by-row and batch-at-a-time,
// verify identical results.
func TestExprRowVsBatch(t *testing.T) {
	b := testBatch()
	exprs := []Expr{
		Col(0),
		Col(1),
		Col(2),
		Col(3),
		IntLit(42),
		FloatLit(3.14),
		StrLit("hello"),
		BoolLit(true),
		Eq(Col(0), IntLit(2)),
		Ne(Col(0), IntLit(2)),
		Lt(Col(0), IntLit(3)),
		Le(Col(0), IntLit(3)),
		Gt(Col(1), FloatLit(2.0)),
		Ge(Col(1), FloatLit(2.0)),
		Add(Col(0), IntLit(10)),
		Sub(Col(0), IntLit(10)),
		Mul(Col(0), IntLit(2)),
		Div(Col(0), IntLit(2)),
		And(Col(3), BoolLit(true)),
		Or(Col(3), BoolLit(false)),
		Not(Col(3)),
		Eq(Col(2), StrLit("c")),
		Add(Col(1), FloatLit(1.0)),
	}
	for _, e := range exprs {
		// Batch evaluation.
		col, err := e.EvalBatch(b)
		if err != nil {
			t.Fatalf("EvalBatch(%T): %v", e, err)
		}
		// Row-by-row evaluation.
		for ri := 0; ri < b.N; ri++ {
			r := &row.Row{Vals: b.Row(ri)}
			d, err := e.Eval(r)
			if err != nil {
				t.Fatalf("Eval(%T) row %d: %v", e, ri, err)
			}
			bd := col.Get(ri)
			if !datumsEqual(d, bd) {
				t.Errorf("expr %T row %d: row=%v batch=%v", e, ri, d, bd)
			}
		}
	}
}

func datumsEqual(a, b types.Datum) bool {
	if a.T != b.T {
		return false
	}
	if a.Null || b.Null {
		return a.Null && b.Null
	}
	switch a.T {
	case types.Float64:
		return a.F == b.F
	default:
		eq, _ := types.Equal(a, b)
		return eq
	}
}

// TestExprNullPropagation: NULL propagates through arithmetic and comparison.
func TestExprNullPropagation(t *testing.T) {
	b := testBatch()
	// Col(0) is NULL in row 2.
	e := Add(Col(0), IntLit(1))
	col, err := e.EvalBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if !col.Null[2] {
		t.Error("expected NULL in row 2 (NULL + 1)")
	}
	// Col(1) is NULL in row 3.
	e = Add(Col(1), FloatLit(1.0))
	col, err = e.EvalBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if !col.Null[3] {
		t.Error("expected NULL in row 3 (NULL + 1.0)")
	}
	// Comparison with NULL yields NULL.
	e = Eq(Col(0), IntLit(1))
	col, err = e.EvalBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if !col.Null[2] {
		t.Error("expected NULL result for NULL = 1")
	}
}

// TestExprLogicThreeValued: AND/OR three-valued logic.
func TestExprLogicThreeValued(t *testing.T) {
	b := testBatch()
	// Row 4: Col(3) is NULL. AND(NULL, true) = NULL.
	var e Expr = And(Col(3), BoolLit(true))
	col, err := e.EvalBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if !col.Null[4] {
		t.Error("expected NULL for NULL AND true")
	}
	// Row 4: OR(NULL, false) = NULL.
	e = Or(Col(3), BoolLit(false))
	col, err = e.EvalBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if !col.Null[4] {
		t.Error("expected NULL for NULL OR false")
	}
	// Row 4: NOT(NULL) = NULL.
	e = Not(Col(3))
	col, err = e.EvalBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if !col.Null[4] {
		t.Error("expected NULL for NOT NULL")
	}
}

// TestExprTypeMismatch: type mismatches are reported.
func TestExprTypeMismatch(t *testing.T) {
	b := testBatch()
	// Comparing int64 with string.
	e := Eq(Col(0), Col(2))
	_, err := e.EvalBatch(b)
	if err == nil {
		t.Fatal("expected type mismatch error, got nil")
	}
}

// TestExprOverflow: integer overflow is reported.
func TestExprOverflow(t *testing.T) {
	schema := []types.T{types.Int64}
	b := vector.NewBatch(schema, 1)
	b.AppendRow([]types.Datum{types.NewInt64(math.MaxInt64)})
	e := Add(Col(0), IntLit(1))
	_, err := e.EvalBatch(b)
	if err == nil {
		t.Fatal("expected overflow error, got nil")
	}
}
