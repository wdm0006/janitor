package standardize

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
	"strings"
)

type Trim struct{ Column string }

func (t *Trim) Name() string { return "trim" }

func (t *Trim) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
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
			c.Set(i, strings.TrimSpace(v))
		}
	}
	return f, nil
}
