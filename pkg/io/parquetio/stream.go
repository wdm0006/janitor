package parquetio

import (
    "fmt"
    "os"

    parquet "github.com/segmentio/parquet-go"

    j "github.com/wdm0006/janitor/pkg/janitor"
)

// StreamReader reads Parquet rows in chunks as Frames.
type StreamReader struct {
    file      *os.File
    reader    *parquet.GenericReader[map[string]any]
    schema    j.Schema
    chunkSize int
    buf       []map[string]any
}

func NewStreamReader(path string, chunkSize int, sampleRows int) (*StreamReader, error) {
    // infer schema using OpenReader
    rd, err := OpenReader(path, sampleRows)
    if err != nil { return nil, err }
    schema := rd.Schema()
    // reuse underlying file; reopen reader for streaming
    f := rd.file
    if err := rd.reader.Close(); err != nil { _ = f.Close(); return nil, err }
    if _, err := f.Seek(0, 0); err != nil { _ = f.Close(); return nil, err }
    gr := parquet.NewGenericReader[map[string]any](f)
    if chunkSize <= 0 { chunkSize = 8192 }
    return &StreamReader{file: f, reader: gr, schema: schema, chunkSize: chunkSize, buf: make([]map[string]any, chunkSize)}, nil
}

func (s *StreamReader) Close() error {
    _ = s.reader.Close()
    return s.file.Close()
}

func (s *StreamReader) Schema() j.Schema { return s.schema }

func (s *StreamReader) Next() (*j.Frame, error) {
    n, err := s.reader.Read(s.buf)
    if n == 0 && err != nil {
        return nil, err
    }
    f := j.NewFrame(s.schema)
    for i := 0; i < n; i++ {
        f.AppendNullRow()
        setRow(f, f.Rows()-1, s.buf[i])
    }
    return f, nil
}

// StreamWriter writes Frames to a Parquet file incrementally.
type StreamWriter struct {
    file   *os.File
    writer *parquet.GenericWriter[map[string]any]
}

func NewStreamWriter(path string) (*StreamWriter, error) {
    f, err := os.Create(path)
    if err != nil { return nil, err }
    w := parquet.NewGenericWriter[map[string]any](f)
    return &StreamWriter{file: f, writer: w}, nil
}

func (s *StreamWriter) Write(fr *j.Frame) error {
    // convert each row to map[string]any
    for r := 0; r < fr.Rows(); r++ {
        rec := make(map[string]any, len(fr.Schema().Columns))
        for _, cs := range fr.Schema().Columns {
            col, _ := fr.ColumnByName(cs.Name)
            switch cs.Type {
            case j.KindFloat:
                if v, ok := col.(*j.FloatColumn).Get(r); ok { rec[cs.Name] = v }
            case j.KindInt:
                if v, ok := col.(*j.IntColumn).Get(r); ok { rec[cs.Name] = v }
            case j.KindBool:
                if v, ok := col.(*j.BoolColumn).Get(r); ok { rec[cs.Name] = v }
            case j.KindString:
                if v, ok := col.(*j.StringColumn).Get(r); ok { rec[cs.Name] = v }
            case j.KindTime:
                if v, ok := col.(*j.TimeColumn).Get(r); ok { rec[cs.Name] = v.Format("2006-01-02T15:04:05Z07:00") }
            }
        }
        if _, err := s.writer.Write([]map[string]any{rec}); err != nil { return fmt.Errorf("parquet stream write: %w", err) }
    }
    return nil
}

func (s *StreamWriter) Close() error {
    if err := s.writer.Close(); err != nil { _ = s.file.Close(); return err }
    return s.file.Close()
}
