package csvio

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"

	j "github.com/wdm0006/janitor/pkg/janitor"
)

// StreamReader reads CSV into Frame chunks of up to ChunkSize rows.
type StreamReader struct {
	r         *Reader
	schema    j.Schema
	chunkSize int
}

// NewStreamReader opens the file, infers schema (respecting options), and returns a StreamReader.
func NewStreamReader(path string, opt ReaderOptions, chunkSize int) (*StreamReader, *os.File, error) {
	rr, f, err := Open(path, opt)
	if err != nil {
		return nil, nil, err
	}
	schema, _, err := rr.InferSchema()
    if err != nil {
        _ = f.Close()
        return nil, nil, err
    }
	return &StreamReader{r: rr, schema: schema, chunkSize: chunkSize}, f, nil
}

// Next returns the next chunk frame or io.EOF when complete.
func (s *StreamReader) Next() (*j.Frame, error) {
	if s.chunkSize <= 0 {
		s.chunkSize = 1024
	}
	f := j.NewFrame(s.schema)
	// drain buffered lines first
	for len(s.r.buf) > 0 && f.Rows() < s.chunkSize {
		rec := s.r.buf[0]
		s.r.buf = s.r.buf[1:]
		appendCSVRecord(f, s.schema, rec)
	}
	for f.Rows() < s.chunkSize {
		rec, err := s.r.r.Read()
		if err == io.EOF {
			if f.Rows() == 0 {
				return nil, io.EOF
			}
			return f, nil
		}
		if err != nil {
			return nil, err
		}
		appendCSVRecord(f, s.schema, rec)
	}
	return f, nil
}

func (s *StreamReader) Schema() j.Schema { return s.schema }

func appendCSVRecord(f *j.Frame, schema j.Schema, rec []string) {
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

// StreamWriter appends frames to a CSV file with a header (written once).
type StreamWriter struct {
	w           *csv.Writer
	file        *os.File
	wroteHeader bool
	schema      j.Schema
}

func NewStreamWriter(path string, schema j.Schema, opt WriterOptions) (*StreamWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(f)
	if opt.Delimiter != 0 {
		w.Comma = opt.Delimiter
	}
	return &StreamWriter{w: w, file: f, schema: schema}, nil
}

func (s *StreamWriter) Write(fr *j.Frame) error {
	if !s.wroteHeader {
		hdr := make([]string, len(s.schema.Columns))
		for i, cs := range s.schema.Columns {
			hdr[i] = cs.Name
		}
		if err := s.w.Write(hdr); err != nil {
			return err
		}
		s.wroteHeader = true
	}
	// reuse existing writer conversion by formatting values here
	for r := 0; r < fr.Rows(); r++ {
		row := make([]string, len(s.schema.Columns))
		for c, cs := range s.schema.Columns {
			col, _ := fr.ColumnByName(cs.Name)
			switch cs.Type {
			case j.KindFloat:
				if v, ok := col.(*j.FloatColumn).Get(r); ok {
					row[c] = strconv.FormatFloat(v, 'g', -1, 64)
				}
			case j.KindInt:
				if v, ok := col.(*j.IntColumn).Get(r); ok {
					row[c] = strconv.FormatInt(v, 10)
				}
			case j.KindBool:
				if v, ok := col.(*j.BoolColumn).Get(r); ok {
					if v {
						row[c] = "true"
					} else {
						row[c] = "false"
					}
				}
			case j.KindString:
				if v, ok := col.(*j.StringColumn).Get(r); ok {
					row[c] = v
				}
			case j.KindTime:
				if v, ok := col.(*j.TimeColumn).Get(r); ok {
					row[c] = v.Format("2006-01-02T15:04:05Z07:00")
				}
			}
		}
		if err := s.w.Write(row); err != nil {
			return err
		}
	}
	s.w.Flush()
	return s.w.Error()
}

func (s *StreamWriter) Close() error {
	s.w.Flush()
	if err := s.w.Error(); err != nil {
		_ = s.file.Close()
		return err
	}
	return s.file.Close()
}
