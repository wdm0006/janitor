package standardize

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
	"strings"
)

type Lower struct{ Column string }

func (t *Lower) Name() string { return "lower" }

func (t *Lower) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
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
			c.Set(i, strings.ToLower(v))
		}
	}
	return f, nil
}
