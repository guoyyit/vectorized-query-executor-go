package executor

import (
	"context"
	"math/rand"
	"testing"
)

// randomTable builds a table with random values, NULLs and duplicates.
func randomTable(rng *rand.Rand, n int) *Table {
	rows := make([]Row, n)
	for i := range rows {
		var id Value
		switch rng.Intn(8) {
		case 0:
			id = NullOf(TypeInt64)
		default:
			id = IntVal(int64(rng.Intn(40) - 20))
		}
		var name Value
		switch rng.Intn(6) {
		case 0:
			name = NullOf(TypeString)
		default:
			name = StringVal(string(rune('a' + rng.Intn(6))))
		}
		var score Value
		switch rng.Intn(5) {
		case 0:
			score = NullOf(TypeFloat64)
		default:
			score = FloatVal(float64(rng.Intn(200)-100) / 4)
		}
		rows[i] = Row{id, name, score, BoolVal(rng.Intn(2) == 0)}
	}
	return NewTable(testSchema(), rows)
}

// randomExpr builds a random boolean expression over the table columns.
func randomExpr(rng *rand.Rand, depth int) Expr {
	if depth == 0 || rng.Intn(3) == 0 {
		col := rng.Intn(4)
		switch col {
		case 0:
			return Binary{Op: OpGE, L: Column{0}, R: Literal{IntVal(int64(rng.Intn(30) - 15))}}
		case 1:
			return Binary{Op: OpEq, L: Column{1}, R: Literal{StringVal(string(rune('a' + rng.Intn(6))))}}
		case 2:
			return Binary{Op: OpLT, L: Column{2}, R: Literal{FloatVal(float64(rng.Intn(40) - 20))}}
		default:
			return Column{3}
		}
	}
	switch rng.Intn(3) {
	case 0:
		return Binary{Op: OpAnd, L: randomExpr(rng, depth-1), R: randomExpr(rng, depth-1)}
	case 1:
		return Binary{Op: OpOr, L: randomExpr(rng, depth-1), R: randomExpr(rng, depth-1)}
	default:
		return Unary{Op: OpNot, X: randomExpr(rng, depth-1)}
	}
}

func TestRandomizedCrossCheck(t *testing.T) {
	for seed := int64(0); seed < 30; seed++ {
		rng := rand.New(rand.NewSource(seed))
		tbl := randomTable(rng, 1+rng.Intn(800))
		plan := &Plan{Filter: randomExpr(rng, 3)}
		if rng.Intn(2) == 0 {
			plan.Proj = []Expr{
				Binary{Op: OpMul, L: Column{0}, R: Literal{IntVal(3)}},
				Column{1},
				Column{2},
			}
		}
		switch rng.Intn(3) {
		case 0:
			plan.SortKeys = []SortKey{{Col: rng.Intn(3)}, {Col: rng.Intn(3), Desc: rng.Intn(2) == 0}}
		case 1:
			plan.GroupBy = []int{rng.Intn(3)}
			plan.Aggs = []AggSpec{
				{Func: AggCount, Col: -1},
				{Func: AggSum, Col: 2},
				{Func: AggAvg, Col: 2},
				{Func: AggMin, Col: 0},
				{Func: AggMax, Col: 0},
			}
			if rng.Intn(2) == 0 {
				plan.SortKeys = []SortKey{{Col: 0}}
			}
		}
		opts := []Options{
			{BatchSize: 1 + rng.Intn(200), Partitions: 1 + rng.Intn(8), Concurrency: 1 + rng.Intn(6)},
			{BatchSize: 1 + rng.Intn(2000), Partitions: 1 + rng.Intn(8), Concurrency: 1 + rng.Intn(6)},
		}
		for _, o := range opts {
			if _, err := CrossCheck(context.Background(), tbl, plan, o); err != nil {
				t.Fatalf("seed=%d opts=%+v: %v", seed, o, err)
			}
		}
	}
}
