package golearn

// Package golearn provides adapters to convert between janitor's Frame and
// github.com/sjwhitworth/golearn/base DenseInstances.

import (
	"github.com/sjwhitworth/golearn/base"
	j "github.com/wdm0006/janitor/pkg/janitor"
)

// ToDenseInstances converts a Frame into golearn DenseInstances.
func ToDenseInstances(f *j.Frame) (*base.DenseInstances, error) {
	attrs := make([]base.Attribute, len(f.Schema().Columns))
	for i, cs := range f.Schema().Columns {
		switch cs.Type {
		case j.KindFloat, j.KindInt:
			attrs[i] = base.NewFloatAttribute(cs.Name)
		default:
			ca := new(base.CategoricalAttribute)
			ca.SetName(cs.Name)
			attrs[i] = ca
		}
	}
    inst := base.NewDenseInstances()
    specs := make([]base.AttributeSpec, len(attrs))
    for i, a := range attrs {
        specs[i] = inst.AddAttribute(a)
    }
    if err := inst.Extend(f.Rows()); err != nil {
        return nil, err
    }

	for r := 0; r < f.Rows(); r++ {
		for c, cs := range f.Schema().Columns {
			col, _ := f.ColumnByName(cs.Name)
			switch cs.Type {
			case j.KindFloat:
				if v, ok := col.(*j.FloatColumn).Get(r); ok {
					inst.Set(specs[c], r, base.PackFloatToBytes(v))
				}
			case j.KindInt:
				if v, ok := col.(*j.IntColumn).Get(r); ok {
					inst.Set(specs[c], r, base.PackFloatToBytes(float64(v)))
				}
			default:
				if v, ok := col.(*j.StringColumn).Get(r); ok {
					inst.Set(specs[c], r, base.Attribute.GetSysValFromString(attrs[c], v))
				}
			}
		}
	}
	// Heuristic: last column as class if categorical
    if len(attrs) > 0 {
        if err := inst.AddClassAttribute(attrs[len(attrs)-1]); err != nil {
            return nil, err
        }
    }
	return inst, nil
}

// FromDenseInstances converts golearn DenseInstances into a Frame.
func FromDenseInstances(inst *base.DenseInstances) (*j.Frame, error) {
	attrs := inst.AllAttributes()
	schema := j.Schema{Columns: make([]j.ColumnSchema, len(attrs))}
	specs := make([]base.AttributeSpec, len(attrs))
	for i, a := range attrs {
		k := j.KindString
		if a.GetType() == 1 { // float in golearn
			k = j.KindFloat
		}
		schema.Columns[i] = j.ColumnSchema{Name: a.GetName(), Type: k, Nullable: true}
		spec, _ := inst.GetAttribute(a)
		specs[i] = spec
	}
	f := j.NewFrame(schema)
    _, nrows := inst.Size()
    for r := 0; r < nrows; r++ {
        f.AppendNullRow()
        row := r
        for c, cs := range schema.Columns {
			switch cs.Type {
			case j.KindFloat:
				v := base.UnpackBytesToFloat(inst.Get(specs[c], r))
				_ = f.SetCell(row, cs.Name, v)
			default:
				v := base.Attribute.GetStringFromSysVal(specs[c].GetAttribute(), inst.Get(specs[c], r))
				_ = f.SetCell(row, cs.Name, v)
			}
		}
	}
	return f, nil
}
