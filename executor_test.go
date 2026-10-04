package executor

import (
	"context"
	"fmt"
	"testing"
)

// testSchema: (id int, name string, score float, active bool)
func testSchema() *Schema {
	return &Schema{
		Names: []string{"id", "name", "score", "active"},
		Types: []Type{TypeInt64, TypeString, TypeFloat64, TypeBool},
	}
}

// makeTestTable builds n rows with deterministic, collision-prone values.
func makeTestTable(n int) *Table {
	rows := make([]Row, n)
	for i := 0; i < n; i++ {
		score := Value{Typ: TypeFloat64, F64: float64(i%17) * 1.5}
		if i%11 == 0 {
			score = NullOf(TypeFloat64)
		}
		name := Value{Typ: TypeString, Str: fmt.Sprintf("name-%d", i%7)}
		if i%13 == 0 {
			name = NullOf(TypeString)
		}
		rows[i] = Row{
			IntVal(int64(i % 23)),
			name,
			score,
			BoolVal(i%2 == 0),
		}
	}
	return NewTable(testSchema(), rows)
}

func optMatrix() []Options {
	var out []Options
	for _, bs := range []int{1, 2, 3, 7, 64, 1024} {
		for _, parts := range []int{1, 2, 3, 8} {
			for _, conc := range []int{1, 2, 4} {
				out = append(out, Options{BatchSize: bs, Partitions: parts, Concurrency: conc})
			}
		}
	}
	return out
}

func TestFilterProjectCrossCheck(t *testing.T) {
	tbl := makeTestTable(500)
	// WHERE id >= 5 AND (score IS NULL OR score > 3.0) SELECT id+1, name, active
	plan := &Plan{
		Filter: Binary{Op: OpAnd,
			L: Binary{Op: OpGE, L: Column{0}, R: Literal{IntVal(5)}},
			R: Binary{Op: OpOr,
				L: IsNull{X: Column{2}},
				R: Binary{Op: OpGT, L: Column{2}, R: Literal{FloatVal(3.0)}},
			},
		},
		Proj: []Expr{
			Binary{Op: OpAdd, L: Column{0}, R: Literal{IntVal(1)}},
			Column{1},
			Column{3},
		},
	}
	want, err := ExecuteRow(context.Background(), tbl, plan, Options{})
	if err != nil {
		t.Fatalf("row path: %v", err)
	}
	if len(want) == 0 {
		t.Fatal("expected some rows")
	}
	for _, o := range optMatrix() {
		got, err := ExecuteVec(context.Background(), tbl, plan, o)
		if err != nil {
			t.Fatalf("vec path %+v: %v", o, err)
		}
		if !EqualRows(want, got) {
			t.Fatalf("mismatch with %+v: %v", o, DiffRows(want, got))
		}
	}
}

func TestSortCrossCheck(t *testing.T) {
	tbl := makeTestTable(500)
	plan := &Plan{
		SortKeys: []SortKey{{Col: 0}, {Col: 2, Desc: true}, {Col: 1}},
	}
	want, err := ExecuteRow(context.Background(), tbl, plan, Options{})
	if err != nil {
		t.Fatalf("row path: %v", err)
	}
	// verify sorted order
	less := makeLess(plan.SortKeys, new(error))
	for i := 1; i < len(want); i++ {
		if less(want[i], want[i-1]) {
			t.Fatalf("output not sorted at %d: %v < %v", i, want[i], want[i-1])
		}
	}
	for _, o := range optMatrix() {
		got, err := ExecuteVec(context.Background(), tbl, plan, o)
		if err != nil {
			t.Fatalf("vec path %+v: %v", o, err)
		}
		if !EqualRows(want, got) {
			t.Fatalf("mismatch with %+v: %v", o, DiffRows(want, got))
		}
	}
}

func TestAggregateCrossCheck(t *testing.T) {
	tbl := makeTestTable(500)
	plan := &Plan{
		GroupBy: []int{0},
		Aggs: []AggSpec{
			{Func: AggCount, Col: -1},
			{Func: AggCount, Col: 2},
			{Func: AggSum, Col: 2},
			{Func: AggMin, Col: 2},
			{Func: AggMax, Col: 2},
			{Func: AggAvg, Col: 2},
		},
	}
	want, err := ExecuteRow(context.Background(), tbl, plan, Options{})
	if err != nil {
		t.Fatalf("row path: %v", err)
	}
	if len(want) != 23 {
		t.Fatalf("expected 23 groups, got %d", len(want))
	}
	for _, o := range optMatrix() {
		got, err := ExecuteVec(context.Background(), tbl, plan, o)
		if err != nil {
			t.Fatalf("vec path %+v: %v", o, err)
		}
		if !EqualRows(want, got) {
			t.Fatalf("mismatch with %+v: %v", o, DiffRows(want, got))
		}
	}
}

func TestFullPipelineCrossCheck(t *testing.T) {
	tbl := makeTestTable(1000)
	// WHERE active SELECT id, score GROUP BY id AGG count(*), avg(score)
	// ORDER BY 2 DESC, 1
	plan := &Plan{
		Filter:  Column{3},
		Proj:    []Expr{Column{0}, Column{2}},
		GroupBy: []int{0},
		Aggs: []AggSpec{
			{Func: AggCount, Col: -1},
			{Func: AggAvg, Col: 1},
		},
		SortKeys: []SortKey{{Col: 1, Desc: true}, {Col: 0}},
	}
	for _, o := range optMatrix() {
		res, err := CrossCheck(context.Background(), tbl, plan, o)
		if err != nil {
			t.Fatalf("crosscheck %+v: %v", o, err)
		}
		if len(res) == 0 {
			t.Fatalf("empty result with %+v", o)
		}
	}
}

func TestEmptyTable(t *testing.T) {
	tbl := makeTestTable(0)
	// plain plan: no rows
	plan := &Plan{Filter: Column{3}}
	for _, o := range optMatrix() {
		res, err := CrossCheck(context.Background(), tbl, plan, o)
		if err != nil {
			t.Fatalf("%+v: %v", o, err)
		}
		if len(res) != 0 {
			t.Fatalf("expected 0 rows, got %d", len(res))
		}
	}
	// aggregate without GROUP BY over empty input: one row
	aggPlan := &Plan{Aggs: []AggSpec{{Func: AggCount, Col: -1}, {Func: AggSum, Col: 0}}}
	res, err := CrossCheck(context.Background(), tbl, aggPlan, Options{BatchSize: 4, Partitions: 3, Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0][0].I64 != 0 || !res[0][1].Null {
		t.Fatalf("unexpected empty-input aggregate: %v", res)
	}
	// aggregate with GROUP BY over empty input: zero rows
	grpPlan := &Plan{GroupBy: []int{0}, Aggs: []AggSpec{{Func: AggCount, Col: -1}}}
	res, err = CrossCheck(context.Background(), tbl, grpPlan, Options{BatchSize: 4, Partitions: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 0 {
		t.Fatalf("expected 0 groups, got %d", len(res))
	}
}

func TestAllFilteredOut(t *testing.T) {
	tbl := makeTestTable(300)
	// predicate matches nothing: every batch is fully skipped
	plan := &Plan{
		Filter: Binary{Op: OpLT, L: Column{0}, R: Literal{IntVal(-1)}},
		Proj:   []Expr{Column{0}},
	}
	for _, o := range optMatrix() {
		res, err := CrossCheck(context.Background(), tbl, plan, o)
		if err != nil {
			t.Fatalf("%+v: %v", o, err)
		}
		if len(res) != 0 {
			t.Fatalf("expected 0 rows with %+v, got %d", o, len(res))
		}
	}
}
