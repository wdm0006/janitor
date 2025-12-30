package jsonlio

import (
	"bufio"
	"encoding/json"
	"io"
	"os"

	j "github.com/wdm0006/janitor/pkg/janitor"
)

type StreamReader struct {
	dec       *json.Decoder
	schema    j.Schema
	chunkSize int
}

func NewStreamReader(path string, chunkSize int) (*StreamReader, *os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	r := bufio.NewReader(f)
	dec := json.NewDecoder(r)
	// infer schema from first chunk
	sample := make([]map[string]any, 0, 100)
	keysSet := map[string]struct{}{}
	for len(sample) < 100 {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			if err == io.EOF {
				break
			}
            _ = f.Close()
			return nil, nil, err
		}
		sample = append(sample, m)
		for k := range m {
			keysSet[k] = struct{}{}
		}
	}
	keys := make([]string, 0, len(keysSet))
	for k := range keysSet {
		keys = append(keys, k)
	}
	kinds := inferKinds(sample, keys)
	schema := j.Schema{Columns: make([]j.ColumnSchema, len(keys))}
	for i, k := range keys {
		schema.Columns[i] = j.ColumnSchema{Name: k, Type: kinds[i], Nullable: true}
	}
	// create a new decoder over the same file by seeking back to start
    if _, err := f.Seek(0, io.SeekStart); err != nil {
        _ = f.Close()
        return nil, nil, err
    }
	dec = json.NewDecoder(bufio.NewReader(f))
	return &StreamReader{dec: dec, schema: schema, chunkSize: chunkSize}, f, nil
}

func (s *StreamReader) Next() (*j.Frame, error) {
	if s.chunkSize <= 0 {
		s.chunkSize = 1024
	}
	f := j.NewFrame(s.schema)
	for f.Rows() < s.chunkSize {
		var m map[string]any
		if err := s.dec.Decode(&m); err != nil {
			if err == io.EOF {
				if f.Rows() == 0 {
					return nil, io.EOF
				}
				return f, nil
			}
			return nil, err
		}
		f.AppendNullRow()
		row := f.Rows() - 1
		// reuse setter from reader.go
		(&Reader{}).setRowFromMap(f, row, m)
	}
	return f, nil
}

func (s *StreamReader) Schema() j.Schema { return s.schema }

type StreamWriter struct {
	enc  *json.Encoder
	w    *bufio.Writer
	file *os.File
}

func NewStreamWriter(path string) (*StreamWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(f)
	return &StreamWriter{enc: json.NewEncoder(w), w: w, file: f}, nil
}

func (s *StreamWriter) Write(f *j.Frame) error {
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
		if err := s.enc.Encode(m); err != nil {
			return err
		}
	}
	return s.w.Flush()
}

func (s *StreamWriter) Close() error {
	if err := s.w.Flush(); err != nil {
		_ = s.file.Close()
		return err
	}
	return s.file.Close()
}
