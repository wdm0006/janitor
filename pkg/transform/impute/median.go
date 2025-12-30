package impute

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
	"sort"
)

type Median struct{ Column string }

func (t *Median) Name() string { return "impute_median" }

func (t *Median) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
	col, ok := f.ColumnByName(t.Column)
	if !ok {
		return f, nil
	}
	switch c := col.(type) {
	case *j.FloatColumn:
		vals := make([]float64, 0, c.Len())
		for i := 0; i < c.Len(); i++ {
			if !c.IsNull(i) {
				v, _ := c.Get(i)
				vals = append(vals, v)
			}
		}
		if len(vals) == 0 {
			return f, nil
		}
		sort.Float64s(vals)
		var med float64
		mid := len(vals) / 2
		if len(vals)%2 == 0 {
			med = (vals[mid-1] + vals[mid]) / 2
		} else {
			med = vals[mid]
		}
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				c.Set(i, med)
			}
		}
	case *j.IntColumn:
		vals := make([]int64, 0, c.Len())
		for i := 0; i < c.Len(); i++ {
			if !c.IsNull(i) {
				v, _ := c.Get(i)
				vals = append(vals, v)
			}
		}
		if len(vals) == 0 {
			return f, nil
		}
		sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
		mid := len(vals) / 2
		var med int64
		if len(vals)%2 == 0 {
			med = (vals[mid-1] + vals[mid]) / 2
		} else {
			med = vals[mid]
		}
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				c.Set(i, med)
			}
		}
	}
	return f, nil
}
