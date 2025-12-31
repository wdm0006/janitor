package parquetio

import (
    "fmt"
    "os"
    "strconv"
    "strings"

    parquet "github.com/segmentio/parquet-go"

    j "github.com/wdm0006/janitor/pkg/janitor"
)

type Reader struct {
    file   *os.File
    reader *parquet.GenericReader[map[string]any]
    schema j.Schema
}

func OpenReader(path string, sampleRows int) (*Reader, error) {
    f, err := os.Open(path)
    if err != nil { return nil, err }
    r := parquet.NewGenericReader[map[string]any](f)
    // infer schema from first N rows
    if sampleRows <= 0 { sampleRows = 100 }
    rows := make([]map[string]any, sampleRows)
    n, err := r.Read(rows)
    if err != nil && err.Error() != "EOF" && !strings.Contains(err.Error(), "EOF") {
        _ = r.Close(); _ = f.Close(); return nil, err
    }
    rows = rows[:n]
    schema := inferSchema(rows)
    // push the read rows back by creating a new reader (segmentio readers can't unread), so reopen
    if err := r.Close(); err != nil { _ = f.Close(); return nil, err }
    if _, err := f.Seek(0, 0); err != nil { _ = f.Close(); return nil, err }
    r2 := parquet.NewGenericReader[map[string]any](f)
    return &Reader{file: f, reader: r2, schema: schema}, nil
}

func (r *Reader) Close() error {
    _ = r.reader.Close()
    return r.file.Close()
}

func (r *Reader) Schema() j.Schema { return r.schema }

func (r *Reader) ReadAll() (*j.Frame, error) {
    f := j.NewFrame(r.schema)
    buf := make([]map[string]any, 1024)
    for {
        n, err := r.reader.Read(buf)
        if n > 0 {
            for i := 0; i < n; i++ {
                f.AppendNullRow()
                row := f.Rows() - 1
                setRow(f, row, buf[i])
            }
        }
        if err != nil {
            if err.Error() == "EOF" || strings.Contains(err.Error(), "EOF") { break }
            return nil, err
        }
        if n == 0 { break }
    }
    return f, nil
}

func inferSchema(rows []map[string]any) j.Schema {
    keysSet := map[string]struct{}{}
    for _, m := range rows {
        for k := range m { keysSet[k] = struct{}{} }
    }
    keys := make([]string, 0, len(keysSet))
    for k := range keysSet { keys = append(keys, k) }
    kinds := make([]j.Kind, len(keys))
    for i, k := range keys {
        nNum, nInt, nBool, nStr := 0, 0, 0, 0
        for _, m := range rows {
            v, ok := m[k]
            if !ok || v == nil { continue }
            switch t := v.(type) {
            case float64:
                nNum++; if float64(int64(t)) == t { nInt++ }
            case int, int64:
                nNum++; nInt++
            case bool:
                nBool++
            case string:
                s := strings.TrimSpace(t)
                if s == "" { continue }
                if x, err := strconv.ParseFloat(s, 64); err == nil { nNum++; if float64(int64(x)) == x { nInt++ } } else { nStr++ }
            default:
                nStr++
            }
        }
        switch {
        case nBool > nNum && nBool >= nStr:
            kinds[i] = j.KindBool
        case nNum > nStr:
            if nInt == nNum { kinds[i] = j.KindInt } else { kinds[i] = j.KindFloat }
        default:
            kinds[i] = j.KindString
        }
    }
    schema := j.Schema{Columns: make([]j.ColumnSchema, len(keys))}
    for i, k := range keys { schema.Columns[i] = j.ColumnSchema{Name: k, Type: kinds[i], Nullable: true} }
    return schema
}

func setRow(f *j.Frame, row int, m map[string]any) {
    for _, cs := range f.Schema().Columns {
        if v, ok := m[cs.Name]; ok {
            switch cs.Type {
            case j.KindFloat:
                switch t := v.(type) {
                case float64:
                    _ = f.SetCell(row, cs.Name, t)
                case int:
                    _ = f.SetCell(row, cs.Name, float64(t))
                case int64:
                    _ = f.SetCell(row, cs.Name, float64(t))
                case string:
                    if s := strings.TrimSpace(t); s != "" { if x, err := strconv.ParseFloat(s, 64); err == nil { _ = f.SetCell(row, cs.Name, x) } }
                }
            case j.KindInt:
                switch t := v.(type) {
                case int64:
                    _ = f.SetCell(row, cs.Name, t)
                case int:
                    _ = f.SetCell(row, cs.Name, int64(t))
                case float64:
                    _ = f.SetCell(row, cs.Name, int64(t))
                case string:
                    if s := strings.TrimSpace(t); s != "" { if x, err := strconv.ParseInt(s, 10, 64); err == nil { _ = f.SetCell(row, cs.Name, x) } }
                }
            case j.KindBool:
                switch t := v.(type) {
                case bool:
                    _ = f.SetCell(row, cs.Name, t)
                case string:
                    lv := strings.ToLower(strings.TrimSpace(t))
                    if x, err := strconv.ParseBool(lv); err == nil { _ = f.SetCell(row, cs.Name, x) }
                }
            default:
                switch t := v.(type) {
                case string:
                    _ = f.SetCell(row, cs.Name, t)
                default:
                    _ = f.SetCell(row, cs.Name, fmt.Sprintf("%v", t))
                }
            }
        }
    }
}

