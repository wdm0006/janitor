package impute

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
)

type Mode struct{ Column string }

func (t *Mode) Name() string { return "impute_mode" }

func (t *Mode) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
	col, ok := f.ColumnByName(t.Column)
	if !ok {
		return f, nil
	}
	switch c := col.(type) {
	case *j.StringColumn:
		counts := map[string]int{}
		var best string
		var bestc int
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				continue
			}
			v, _ := c.Get(i)
			counts[v]++
			if counts[v] > bestc {
				bestc = counts[v]
				best = v
			}
		}
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				c.Set(i, best)
			}
		}
	case *j.IntColumn:
		counts := map[int64]int{}
		var best int64
		var bestc int
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				continue
			}
			v, _ := c.Get(i)
			counts[v]++
			if counts[v] > bestc {
				bestc = counts[v]
				best = v
			}
		}
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				c.Set(i, best)
			}
		}
	}
	return f, nil
}
