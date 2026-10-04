package agg

import (
	"fmt"
	"math"

	"github.com/guoyyit/vectorized-query-executor-go/pkg/types"
)

// Merge combines another group's state into this one. The merge order matters
// for floating-point sums (float addition is not associative): callers must
// merge partial states in a deterministic order (e.g. partition order) so the
// result matches the row-by-row path.
func (s *State) Merge(other *State, defs []Def, inputSch []types.T) error {
	s.CountStar += other.CountStar
	s.Count += other.Count
	// Sum and Avg share the SumI/SumF fields; merge them once, not per def.
	sumMerged := false
	for _, d := range defs {
		switch d.Func {
		case Sum, Avg:
			if sumMerged {
				continue
			}
			sumMerged = true
			t, err := d.ResultType(inputSch)
			if err != nil {
				return err
			}
			switch t {
			case types.Int64:
				if d.Func == Avg {
					s.SumF += float64(other.SumI)
				} else {
					if (other.SumI > 0 && s.SumI > math.MaxInt64-other.SumI) ||
						(other.SumI < 0 && s.SumI < math.MinInt64-other.SumI) {
						return fmt.Errorf("%w: SUM overflow during merge", types.ErrOverflow)
					}
					s.SumI += other.SumI
				}
			case types.Float64:
				s.SumF += other.SumF
			}
		case Min:
			if other.hasMin {
				if !s.hasMin {
					s.Min = other.Min
					s.hasMin = true
				} else {
					cmp, err := types.Compare(other.Min, s.Min)
					if err != nil {
						return err
					}
					if cmp < 0 {
						s.Min = other.Min
					}
				}
			}
		case Max:
			if other.hasMax {
				if !s.hasMax {
					s.Max = other.Max
					s.hasMax = true
				} else {
					cmp, err := types.Compare(other.Max, s.Max)
					if err != nil {
						return err
					}
					if cmp > 0 {
						s.Max = other.Max
					}
				}
			}
		}
	}
	return nil
}
