package standardize

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
)

type MapValues struct {
	Column string
	Map    map[string]string
}

func (t *MapValues) Name() string { return "map_values" }

func (t *MapValues) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
	col, ok := f.ColumnByName(t.Column)
	if !ok {
		return f, nil
	}
	if c, ok := col.(*j.StringColumn); ok {
		for i := 0; i < c.Len(); i++ {
			if c.IsNull(i) {
				continue
			}
			v, _ := c.Get(i)
			if nv, ok := t.Map[v]; ok {
				c.Set(i, nv)
			}
		}
	}
	return f, nil
}
