package csvio

import (
	"encoding/csv"
	"os"
	"strconv"

	j "github.com/wdm0006/janitor/pkg/janitor"
)

type WriterOptions struct {
	Delimiter rune // default ','
}

// WriteAll writes a Frame to a CSV file with headers.
func WriteAll(path string, f *j.Frame, opt WriterOptions) error {
    out, err := os.Create(path)
    if err != nil {
        return err
    }
    defer func() { _ = out.Close() }()
	w := csv.NewWriter(out)
	if opt.Delimiter != 0 {
		w.Comma = opt.Delimiter
	}

	// header
	hdr := make([]string, len(f.Schema().Columns))
	for i, cs := range f.Schema().Columns {
		hdr[i] = cs.Name
	}
	if err := w.Write(hdr); err != nil {
		return err
	}

	// rows
	for r := 0; r < f.Rows(); r++ {
		row := make([]string, len(hdr))
		for c, cs := range f.Schema().Columns {
			col, _ := f.ColumnByName(cs.Name)
			switch cs.Type {
			case j.KindFloat:
				v, ok := col.(*j.FloatColumn).Get(r)
				if ok {
					row[c] = strconv.FormatFloat(v, 'g', -1, 64)
				}
			case j.KindInt:
				v, ok := col.(*j.IntColumn).Get(r)
				if ok {
					row[c] = strconv.FormatInt(v, 10)
				}
			case j.KindBool:
				v, ok := col.(*j.BoolColumn).Get(r)
				if ok {
					if v {
						row[c] = "true"
					} else {
						row[c] = "false"
					}
				}
			case j.KindString:
				v, ok := col.(*j.StringColumn).Get(r)
				if ok {
					row[c] = v
				}
			case j.KindTime:
				v, ok := col.(*j.TimeColumn).Get(r)
				if ok {
					row[c] = v.Format("2006-01-02T15:04:05Z07:00")
				}
			}
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
