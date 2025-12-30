package standardize

import (
	"context"
	j "github.com/wdm0006/janitor/pkg/janitor"
	"regexp"
)

type RegexReplace struct {
	Column  string
	Pattern string
	Replace string
	re      *regexp.Regexp
}

func (t *RegexReplace) Name() string { return "regex_replace" }

func (t *RegexReplace) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
	if t.re == nil {
		re, err := regexp.Compile(t.Pattern)
		if err != nil {
			return f, err
		}
		t.re = re
	}
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
			c.Set(i, t.re.ReplaceAllString(v, t.Replace))
		}
	}
	return f, nil
}
