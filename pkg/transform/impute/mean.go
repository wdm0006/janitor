package impute

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
)

type Mean struct{ Column string }

func (t *Mean) Name() string { return "impute_mean" }

func (t *Mean) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
	col, ok := f.ColumnByName(t.Column)
	if !ok {
		return f, nil
	}
	switch c := col.(type) {
	case *j.FloatColumn:
		var sum float64
		var n int
		for i := 0; i < c.Len(); i++ {
			if !c.IsNull(i) {
				v, _ := c.Get(i)
				sum += v
				n++
			}
		}
		if n == 0 {
			return f, nil
		}
		mean := sum / float64(n)
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				c.Set(i, mean)
			}
		}
	case *j.IntColumn:
		var sum int64
		var n int
		for i := 0; i < c.Len(); i++ {
			if !c.IsNull(i) {
				v, _ := c.Get(i)
				sum += v
				n++
			}
		}
		if n == 0 {
			return f, nil
		}
		mean := float64(sum) / float64(n)
		// round to nearest
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				c.Set(i, int64(mean+0.5))
			}
		}
	}
	return f, nil
}
