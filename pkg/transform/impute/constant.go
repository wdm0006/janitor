package impute

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
)

type Constant struct {
	Column string
	// use any; will be coerced per column kind
	Value any
}

func (t *Constant) Name() string { return "impute_constant" }

func (t *Constant) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
	col, ok := f.ColumnByName(t.Column)
	if !ok {
		return f, nil
	}
	switch c := col.(type) {
	case *j.FloatColumn:
		var vv float64
		switch v := t.Value.(type) {
		case int:
			vv = float64(v)
		case int64:
			vv = float64(v)
		case float64:
			vv = v
		}
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				c.Set(i, vv)
			}
		}
	case *j.IntColumn:
		var vv int64
		switch v := t.Value.(type) {
		case int:
			vv = int64(v)
		case int64:
			vv = v
		case float64:
			vv = int64(v)
		}
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				c.Set(i, vv)
			}
		}
	case *j.StringColumn:
		vv, _ := t.Value.(string)
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				c.Set(i, vv)
			}
		}
	case *j.BoolColumn:
		vv, _ := t.Value.(bool)
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				c.Set(i, vv)
			}
		}
	}
	return f, nil
}
