package csvio

import (
	"encoding/csv"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	j "github.com/wdm0006/janitor/pkg/janitor"
)

type ReaderOptions struct {
	HasHeader  bool
	Delimiter  rune // default ','
	SampleRows int  // for inference; default 100
}

type Reader struct {
	r   *csv.Reader
	opt ReaderOptions
	buf [][]string
}

// Open opens a CSV file and returns a Reader.
func Open(path string, opt ReaderOptions) (*Reader, *os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	rr := csv.NewReader(f)
	if opt.Delimiter != 0 {
		rr.Comma = opt.Delimiter
	}
	rr.ReuseRecord = true
	return &Reader{r: rr, opt: opt}, f, nil
}

// InferSchema reads header (if present) and samples rows to determine column kinds.
func (r *Reader) InferSchema() (j.Schema, []string, error) {
	var names []string
	// Peek first record to get column count and optionally header
	rec, err := r.r.Read()
	if err != nil {
		return j.Schema{}, nil, err
	}
	if r.opt.HasHeader {
		names = make([]string, len(rec))
		copy(names, rec)
		rec, err = r.r.Read()
		if err != nil {
			return j.Schema{}, nil, err
		}
	} else {
		names = make([]string, len(rec))
		for i := range names {
			names[i] = "col_" + strconv.Itoa(i)
		}
	}

	sample := [][]string{rec}
	max := r.opt.SampleRows
	if max <= 0 {
		max = 100
	}
	for i := 1; i < max; i++ {
		rr, err := r.r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return j.Schema{}, nil, err
		}
		sample = append(sample, rr)
	}

	kinds := inferKinds(sample)
	schema := j.Schema{Columns: make([]j.ColumnSchema, len(names))}
	for i := range names {
		schema.Columns[i] = j.ColumnSchema{Name: names[i], Type: kinds[i], Nullable: true}
	}
	// retain sampled rows for subsequent ReadAll
	r.buf = append(r.buf, sample...)
	return schema, names, nil
}

// ReadAll loads the rest of the CSV into a Frame.
func (r *Reader) ReadAll(schema j.Schema) (*j.Frame, error) {
	f := j.NewFrame(schema)
	// drain buffered records from inference (if any)
	for len(r.buf) > 0 {
		rec := r.buf[0]
		r.buf = r.buf[1:]
		f.AppendNullRow()
		row := f.Rows() - 1
		for i, cs := range schema.Columns {
			val := strings.TrimSpace(rec[i])
			if val == "" {
				continue
			}
			switch cs.Type {
			case j.KindFloat:
				if x, err := strconv.ParseFloat(val, 64); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			case j.KindInt:
				if x, err := strconv.ParseInt(val, 10, 64); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			case j.KindBool:
				if x, err := strconv.ParseBool(strings.ToLower(val)); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			default:
				_ = f.SetCell(row, cs.Name, val)
			}
		}
	}
	for {
		rec, err := r.r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		// append a null row then set non-empty values
		f.AppendNullRow()
		row := f.Rows() - 1
		for i, cs := range schema.Columns {
			val := strings.TrimSpace(rec[i])
			if val == "" {
				continue
			}
			switch cs.Type {
			case j.KindFloat:
				if x, err := strconv.ParseFloat(val, 64); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			case j.KindInt:
				if x, err := strconv.ParseInt(val, 10, 64); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			case j.KindBool:
				if x, err := strconv.ParseBool(strings.ToLower(val)); err == nil {
					_ = f.SetCell(row, cs.Name, x)
				}
			default:
				_ = f.SetCell(row, cs.Name, val)
			}
		}
	}
	return f, nil
}

func inferKinds(rows [][]string) []j.Kind {
	if len(rows) == 0 {
		return nil
	}
	ncol := len(rows[0])
	kinds := make([]j.Kind, ncol)
	// numeric regex similar to old code
	numre := regexp.MustCompile(`^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$`)
	for c := 0; c < ncol; c++ {
		num, integer, str := 0, 0, 0
		for _, row := range rows {
			if c >= len(row) {
				continue
			}
			v := strings.TrimSpace(row[c])
			if v == "" {
				continue
			}
			if numre.MatchString(v) {
				num++
				if !strings.ContainsAny(v, ".eE") {
					integer++
				}
			} else {
				// try bool
				lv := strings.ToLower(v)
				if lv == "true" || lv == "false" {
					continue
				}
				str++
			}
		}
		// prefer float over int to be permissive
		if num > str {
			if integer == num {
				kinds[c] = j.KindInt
			} else {
				kinds[c] = j.KindFloat
			}
		} else {
			kinds[c] = j.KindString
		}
	}
	return kinds
}
