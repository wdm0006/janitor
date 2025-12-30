package validate

import (
	"context"
	"fmt"
	j "github.com/wdm0006/janitor/pkg/janitor"
)

type Range struct {
	Column string
	Min    *float64
	Max    *float64
}

func (t *Range) Name() string { return "validate_range" }

func (t *Range) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
	col, ok := f.ColumnByName(t.Column)
	if !ok {
		return f, nil
	}
	var bad int
	switch c := col.(type) {
	case *j.FloatColumn:
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				continue
			}
			v, _ := c.Get(i)
			if t.Min != nil && v < *t.Min {
				bad++
			}
			if t.Max != nil && v > *t.Max {
				bad++
			}
		}
	case *j.IntColumn:
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				continue
			}
			v, _ := c.Get(i)
			if t.Min != nil && float64(v) < *t.Min {
				bad++
			}
			if t.Max != nil && float64(v) > *t.Max {
				bad++
			}
		}
	}
	if bad > 0 {
		return f, fmt.Errorf("validate_range: column %s has %d out-of-range values", t.Column, bad)
	}
	return f, nil
}
