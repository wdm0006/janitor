package jsonlio

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	j "github.com/wdm0006/janitor/pkg/janitor"
)

type ReaderOptions struct {
	SampleRows int
}

type Reader struct {
	r    *bufio.Reader
	opt  ReaderOptions
	buf  []map[string]any
	keys []string
}

func Open(path string, opt ReaderOptions) (*Reader, *os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	rd := bufio.NewReader(f)
	return &Reader{r: rd, opt: opt}, f, nil
}

func (r *Reader) InferSchema() (j.Schema, error) {
	max := r.opt.SampleRows
	if max <= 0 {
		max = 100
	}
	dec := json.NewDecoder(r.r)
	var sample []map[string]any
	keysSet := map[string]struct{}{}
	for len(sample) < max {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			if err == io.EOF {
				break
			}
			return j.Schema{}, err
		}
		sample = append(sample, m)
		for k := range m {
			keysSet[k] = struct{}{}
		}
	}
	r.buf = append(r.buf, sample...)
	r.keys = make([]string, 0, len(keysSet))
	for k := range keysSet {
		r.keys = append(r.keys, k)
	}
	kinds := inferKinds(sample, r.keys)
	schema := j.Schema{Columns: make([]j.ColumnSchema, len(r.keys))}
	for i, k := range r.keys {
		schema.Columns[i] = j.ColumnSchema{Name: k, Type: kinds[i], Nullable: true}
	}
	return schema, nil
}

func (r *Reader) ReadAll(schema j.Schema) (*j.Frame, error) {
	f := j.NewFrame(schema)
	// drain buffer
	for len(r.buf) > 0 {
		m := r.buf[0]
		r.buf = r.buf[1:]
		f.AppendNullRow()
		row := f.Rows() - 1
		r.setRowFromMap(f, row, m)
	}
	// continue decoding
	dec := json.NewDecoder(r.r)
	for {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		f.AppendNullRow()
		row := f.Rows() - 1
		r.setRowFromMap(f, row, m)
	}
	return f, nil
}

func (r *Reader) setRowFromMap(f *j.Frame, row int, m map[string]any) {
	for _, cs := range f.Schema().Columns {
		if v, ok := m[cs.Name]; ok {
			switch cs.Type {
			case j.KindFloat:
				switch t := v.(type) {
				case float64:
					_ = f.SetCell(row, cs.Name, t)
				case string:
					if s := strings.TrimSpace(t); s != "" {
						if x, err := strconv.ParseFloat(s, 64); err == nil {
							_ = f.SetCell(row, cs.Name, x)
						}
					}
				}
			case j.KindInt:
				switch t := v.(type) {
				case float64:
					_ = f.SetCell(row, cs.Name, int64(t))
				case string:
					if s := strings.TrimSpace(t); s != "" {
						if x, err := strconv.ParseInt(s, 10, 64); err == nil {
							_ = f.SetCell(row, cs.Name, x)
						}
					}
				}
			case j.KindBool:
				switch t := v.(type) {
				case bool:
					_ = f.SetCell(row, cs.Name, t)
				case string:
					lv := strings.ToLower(strings.TrimSpace(t))
					if x, err := strconv.ParseBool(lv); err == nil {
						_ = f.SetCell(row, cs.Name, x)
					}
				}
			default:
				switch t := v.(type) {
				case string:
					_ = f.SetCell(row, cs.Name, t)
				default:
					// fallback to JSON encoding
					b, _ := json.Marshal(t)
					_ = f.SetCell(row, cs.Name, string(b))
				}
			}
		}
	}
}

func inferKinds(sample []map[string]any, keys []string) []j.Kind {
	kinds := make([]j.Kind, len(keys))
	numre := regexp.MustCompile(`^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$`)
	for i, k := range keys {
		nNum, nInt, nBool, nStr := 0, 0, 0, 0
		for _, m := range sample {
			v, ok := m[k]
			if !ok || v == nil {
				continue
			}
			switch t := v.(type) {
			case float64:
				nNum++
				if float64(int64(t)) == t {
					nInt++
				}
			case bool:
				nBool++
			case string:
				s := strings.TrimSpace(t)
				if s == "" {
					continue
				}
				if numre.MatchString(s) {
					nNum++
					if !strings.ContainsAny(s, ".eE") {
						nInt++
					}
				} else {
					nStr++
				}
			default:
				nStr++
			}
		}
		switch {
		case nBool > nNum && nBool >= nStr:
			kinds[i] = j.KindBool
		case nNum > nStr:
			if nInt == nNum {
				kinds[i] = j.KindInt
			} else {
				kinds[i] = j.KindFloat
			}
		default:
			kinds[i] = j.KindString
		}
	}
	return kinds
}
