package outliers

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
)

type Cap struct {
	Column string
	Min    *float64
	Max    *float64
}

func (t *Cap) Name() string { return "cap_range" }

func (t *Cap) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
	col, ok := f.ColumnByName(t.Column)
	if !ok {
		return f, nil
	}
	switch c := col.(type) {
	case *j.FloatColumn:
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				continue
			}
			v, _ := c.Get(i)
			if t.Min != nil && v < *t.Min {
				v = *t.Min
			}
			if t.Max != nil && v > *t.Max {
				v = *t.Max
			}
			c.Set(i, v)
		}
	case *j.IntColumn:
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				continue
			}
			v, _ := c.Get(i)
			if t.Min != nil && float64(v) < *t.Min {
				v = int64(*t.Min)
			}
			if t.Max != nil && float64(v) > *t.Max {
				v = int64(*t.Max)
			}
			c.Set(i, v)
		}
	}
	return f, nil
}
