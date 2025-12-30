package validate

import (
	"context"
	"fmt"
	j "github.com/wdm0006/janitor/pkg/janitor"
)

type InSet struct {
	Column string
	Values map[string]struct{}
}

func NewInSet(col string, vals []string) *InSet {
	m := make(map[string]struct{}, len(vals))
	for _, v := range vals {
		m[v] = struct{}{}
	}
	return &InSet{Column: col, Values: m}
}

func (t *InSet) Name() string { return "validate_in" }

func (t *InSet) Apply(ctx context.Context, f *j.Frame) (*j.Frame, error) {
	col, ok := f.ColumnByName(t.Column)
	if !ok {
		return f, nil
	}
	sc, ok := col.(*j.StringColumn)
	if !ok {
		return f, nil
	}
	var bad int
	for i := 0; i < sc.Len(); i++ {
		if sc.IsNull(i) {
			continue
		}
		v, _ := sc.Get(i)
		if _, ok := t.Values[v]; !ok {
			bad++
		}
	}
	if bad > 0 {
		return f, fmt.Errorf("validate_in: column %s has %d values outside allowed set", t.Column, bad)
	}
	return f, nil
}
