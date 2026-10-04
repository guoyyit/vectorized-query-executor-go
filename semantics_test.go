package executor

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestNullPropagation(t *testing.T) {
	cases := []struct {
		name string
		expr Expr
		want Value
	}{
		{"null+1", Binary{Op: OpAdd, L: Literal{NullOf(TypeInt64)}, R: Literal{IntVal(1)}}, NullOf(TypeInt64)},
		{"null+1.0", Binary{Op: OpAdd, L: Literal{NullOf(TypeInt64)}, R: Literal{FloatVal(1)}}, NullOf(TypeFloat64)},
		{"null=null", Binary{Op: OpEq, L: Literal{NullOf(TypeInt64)}, R: Literal{NullOf(TypeInt64)}}, NullOf(TypeBool)},
		{"null<1", Binary{Op: OpLT, L: Literal{NullOf(TypeInt64)}, R: Literal{IntVal(1)}}, NullOf(TypeBool)},
		{"true and null", Binary{Op: OpAnd, L: Literal{BoolVal(true)}, R: Literal{NullOf(TypeBool)}}, NullOf(TypeBool)},
		{"false and null", Binary{Op: OpAnd, L: Literal{BoolVal(false)}, R: Literal{NullOf(TypeBool)}}, BoolVal(false)},
		{"true or null", Binary{Op: OpOr, L: Literal{BoolVal(true)}, R: Literal{NullOf(TypeBool)}}, BoolVal(true)},
		{"null or null", Binary{Op: OpOr, L: Literal{NullOf(TypeBool)}, R: Literal{NullOf(TypeBool)}}, NullOf(TypeBool)},
		{"not null", Unary{Op: OpNot, X: Literal{NullOf(TypeBool)}}, NullOf(TypeBool)},
		{"-null", Unary{Op: OpNeg, X: Literal{NullOf(TypeInt64)}}, NullOf(TypeInt64)},
		{"isnull(null)", IsNull{X: Literal{NullOf(TypeString)}}, BoolVal(true)},
		{"isnotnull(null)", IsNull{X: Literal{NullOf(TypeString)}, Negated: true}, BoolVal(false)},
	}
	row := Row{}
	for _, tc := range cases {
		got, err := tc.expr.Eval(row)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !EqualValue(got, tc.want) {
			t.Fatalf("%s: got %v want %v", tc.name, got, tc.want)
		}
		// vectorized path must agree
		b := &Batch{Cols: nil, N: 3}
		v, err := tc.expr.evalVec(b)
		if err != nil {
			t.Fatalf("%s vec: %v", tc.name, err)
		}
		for i := 0; i < 3; i++ {
			if !EqualValue(v.ValueAt(i), tc.want) {
				t.Fatalf("%s vec row %d: got %v want %v", tc.name, i, v.ValueAt(i), tc.want)
			}
		}
	}
}

func TestFailureReasons(t *testing.T) {
	cases := []struct {
		name string
		expr Expr
		want error
	}{
		{"int+string", Binary{Op: OpAdd, L: Literal{IntVal(1)}, R: Literal{StringVal("x")}}, ErrTypeMismatch},
		{"string<int", Binary{Op: OpLT, L: Literal{StringVal("x")}, R: Literal{IntVal(1)}}, ErrTypeMismatch},
		{"bool+bool", Binary{Op: OpAdd, L: Literal{BoolVal(true)}, R: Literal{BoolVal(false)}}, ErrTypeMismatch},
		{"not int", Unary{Op: OpNot, X: Literal{IntVal(1)}}, ErrTypeMismatch},
		{"overflow add", Binary{Op: OpAdd, L: Literal{IntVal(math.MaxInt64)}, R: Literal{IntVal(1)}}, ErrOverflow},
		{"overflow sub", Binary{Op: OpSub, L: Literal{IntVal(math.MinInt64)}, R: Literal{IntVal(1)}}, ErrOverflow},
		{"overflow mul", Binary{Op: OpMul, L: Literal{IntVal(math.MaxInt64)}, R: Literal{IntVal(2)}}, ErrOverflow},
		{"overflow neg", Unary{Op: OpNeg, X: Literal{IntVal(math.MinInt64)}}, ErrOverflow},
		{"div zero int", Binary{Op: OpDiv, L: Literal{IntVal(4)}, R: Literal{IntVal(0)}}, ErrDivByZero},
		{"div zero float", Binary{Op: OpDiv, L: Literal{FloatVal(4)}, R: Literal{FloatVal(0)}}, ErrDivByZero},
		{"div overflow", Binary{Op: OpDiv, L: Literal{IntVal(math.MinInt64)}, R: Literal{IntVal(-1)}}, ErrOverflow},
	}
	row := Row{}
	for _, tc := range cases {
		_, rowErr := tc.expr.Eval(row)
		if !errors.Is(rowErr, tc.want) {
			t.Fatalf("%s row: got %v want %v", tc.name, rowErr, tc.want)
		}
		b := &Batch{N: 2}
		_, vecErr := tc.expr.evalVec(b)
		if !errors.Is(vecErr, tc.want) {
			t.Fatalf("%s vec: got %v want %v", tc.name, vecErr, tc.want)
		}
	}
}

func TestBothPathsFailIdentically(t *testing.T) {
	// table with a string column used in arithmetic -> both paths must fail
	// with ErrTypeMismatch.
	tbl := NewTable(testSchema(), []Row{
		{IntVal(1), StringVal("a"), FloatVal(1), BoolVal(true)},
		{IntVal(2), StringVal("b"), FloatVal(2), BoolVal(false)},
	})
	plan := &Plan{Proj: []Expr{Binary{Op: OpAdd, L: Column{1}, R: Literal{IntVal(1)}}}}
	_, err := CrossCheck(context.Background(), tbl, plan, Options{BatchSize: 1, Partitions: 2, Concurrency: 2})
	if !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("expected ErrTypeMismatch, got %v", err)
	}
	// division by zero encountered mid-stream on both paths
	tbl2 := NewTable(testSchema(), []Row{
		{IntVal(4), StringVal("a"), FloatVal(1), BoolVal(true)},
		{IntVal(0), StringVal("b"), FloatVal(2), BoolVal(false)},
	})
	plan2 := &Plan{Proj: []Expr{Binary{Op: OpDiv, L: Literal{IntVal(8)}, R: Column{0}}}}
	_, err = CrossCheck(context.Background(), tbl2, plan2, Options{BatchSize: 1})
	if !errors.Is(err, ErrDivByZero) {
		t.Fatalf("expected ErrDivByZero, got %v", err)
	}
}

func TestSortStability(t *testing.T) {
	// duplicate keys: original relative order must be preserved
	tbl := NewTable(testSchema(), []Row{
		{IntVal(2), StringVal("first"), FloatVal(0), BoolVal(true)},
		{IntVal(1), StringVal("a"), FloatVal(0), BoolVal(true)},
		{IntVal(2), StringVal("second"), FloatVal(0), BoolVal(true)},
		{IntVal(2), StringVal("third"), FloatVal(0), BoolVal(true)},
		{IntVal(1), StringVal("b"), FloatVal(0), BoolVal(true)},
	})
	plan := &Plan{SortKeys: []SortKey{{Col: 0}}}
	res, err := CrossCheck(context.Background(), tbl, plan, Options{BatchSize: 2, Partitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{"a", "b", "first", "second", "third"}
	for i, w := range wantNames {
		if res[i][1].Str != w {
			t.Fatalf("row %d: got %v want %v (unstable sort)", i, res[i][1].Str, w)
		}
	}
}

func TestNullSortOrder(t *testing.T) {
	tbl := NewTable(testSchema(), []Row{
		{IntVal(1), NullOf(TypeString), FloatVal(0), BoolVal(true)},
		{IntVal(2), StringVal("x"), FloatVal(0), BoolVal(true)},
		{IntVal(3), NullOf(TypeString), FloatVal(0), BoolVal(true)},
	})
	// ascending: NULLs first
	res, err := CrossCheck(context.Background(), tbl, &Plan{SortKeys: []SortKey{{Col: 1}}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !res[0][1].Null || !res[1][1].Null || res[2][1].Null {
		t.Fatalf("ascending NULL placement wrong: %v", res)
	}
	// descending: NULLs last
	res, err = CrossCheck(context.Background(), tbl, &Plan{SortKeys: []SortKey{{Col: 1, Desc: true}}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res[0][1].Null || !res[1][1].Null || !res[2][1].Null {
		t.Fatalf("descending NULL placement wrong: %v", res)
	}
}

func TestAggregateNullSemantics(t *testing.T) {
	// group 1 has all-NULL scores; group 2 mixes NULL and values
	tbl := NewTable(testSchema(), []Row{
		{IntVal(1), StringVal("g1"), NullOf(TypeFloat64), BoolVal(true)},
		{IntVal(2), StringVal("g1"), NullOf(TypeFloat64), BoolVal(true)},
		{IntVal(3), StringVal("g2"), NullOf(TypeFloat64), BoolVal(true)},
		{IntVal(4), StringVal("g2"), FloatVal(10), BoolVal(true)},
	})
	plan := &Plan{
		GroupBy: []int{1},
		Aggs: []AggSpec{
			{Func: AggCount, Col: -1},
			{Func: AggCount, Col: 2},
			{Func: AggSum, Col: 2},
			{Func: AggAvg, Col: 2},
			{Func: AggMin, Col: 2},
		},
	}
	res, err := CrossCheck(context.Background(), tbl, plan, Options{BatchSize: 1, Partitions: 4, Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("groups: %v", res)
	}
	g1, g2 := res[0], res[1]
	// g1: count(*)=2, count(score)=0, sum=NULL, avg=NULL, min=NULL
	if g1[1].I64 != 2 || g1[2].I64 != 0 || !g1[3].Null || !g1[4].Null || !g1[5].Null {
		t.Fatalf("g1 wrong: %v", g1)
	}
	// g2: count(*)=2, count(score)=1, sum=10, avg=10, min=10
	if g2[1].I64 != 2 || g2[2].I64 != 1 || g2[3].F64 != 10 || g2[4].F64 != 10 || g2[5].F64 != 10 {
		t.Fatalf("g2 wrong: %v", g2)
	}
}

func TestSumOverflowFails(t *testing.T) {
	tbl := NewTable(testSchema(), []Row{
		{IntVal(math.MaxInt64), StringVal("a"), FloatVal(0), BoolVal(true)},
		{IntVal(1), StringVal("a"), FloatVal(0), BoolVal(true)},
	})
	plan := &Plan{Aggs: []AggSpec{{Func: AggSum, Col: 0}}}
	_, err := CrossCheck(context.Background(), tbl, plan, Options{BatchSize: 1, Partitions: 2})
	if !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected ErrOverflow, got %v", err)
	}
}
