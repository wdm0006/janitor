package jsonlio

import (
    "bufio"
    "encoding/json"

    j "github.com/wdm0006/janitor/pkg/janitor"
    iox "github.com/wdm0006/janitor/pkg/io/ioutils"
)

func WriteAll(path string, f *j.Frame) error {
    out, err := iox.CreateMaybeCompressed(path)
    if err != nil {
        return err
    }
    defer func() { _ = out.Close() }()
	w := bufio.NewWriter(out)
	enc := json.NewEncoder(w)
	for r := 0; r < f.Rows(); r++ {
		m := map[string]any{}
		for _, cs := range f.Schema().Columns {
			col, _ := f.ColumnByName(cs.Name)
			switch cs.Type {
			case j.KindFloat:
				if v, ok := col.(*j.FloatColumn).Get(r); ok {
					m[cs.Name] = v
				}
			case j.KindInt:
				if v, ok := col.(*j.IntColumn).Get(r); ok {
					m[cs.Name] = v
				}
			case j.KindBool:
				if v, ok := col.(*j.BoolColumn).Get(r); ok {
					m[cs.Name] = v
				}
			case j.KindString:
				if v, ok := col.(*j.StringColumn).Get(r); ok {
					m[cs.Name] = v
				}
			case j.KindTime:
				if v, ok := col.(*j.TimeColumn).Get(r); ok {
					m[cs.Name] = v
				}
			}
		}
		if err := enc.Encode(m); err != nil {
			return err
		}
	}
	return w.Flush()
}
