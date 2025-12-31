package parquetio

import (
    "encoding/json"
    "fmt"

    j "github.com/wdm0006/janitor/pkg/janitor"
    pw "github.com/xitongsys/parquet-go/writer"
    local "github.com/xitongsys/parquet-go-source/local"
)

func parquetSchemaJSON(s j.Schema) string {
    // Build a minimal JSON schema for parquet-go JSONWriter
    type field struct { Tag string `json:"Tag"` }
    type schema struct {
        Tag    string  `json:"Tag"`
        Fields []field `json:"Fields"`
    }
    sc := schema{Tag: "name=schema, repetitiontype=REQUIRED"}
    for _, cs := range s.Columns {
        tag := "name=" + cs.Name + ", repetitiontype=OPTIONAL, type="
        switch cs.Type {
        case j.KindFloat:
            tag += "DOUBLE"
        case j.KindInt:
            tag += "INT64"
        case j.KindBool:
            tag += "BOOLEAN"
        case j.KindString, j.KindTime:
            tag += "BYTE_ARRAY, convertedtype=UTF8"
        default:
            tag += "BYTE_ARRAY, convertedtype=UTF8"
        }
        sc.Fields = append(sc.Fields, field{Tag: tag})
    }
    b, _ := json.Marshal(sc)
    return string(b)
}

// WriteAll writes a Frame to a Parquet file using parquet-go JSONWriter.
func WriteAll(path string, f *j.Frame) error {
    fw, err := local.NewLocalFileWriter(path)
    if err != nil { return err }
    schema := parquetSchemaJSON(f.Schema())
    writer, err := pw.NewJSONWriter(schema, fw, 4)
    if err != nil { _ = fw.Close(); return fmt.Errorf("parquet writer init: %w", err) }
    defer func() { _ = writer.WriteStop(); _ = fw.Close() }()
    // write rows
    for r := 0; r < f.Rows(); r++ {
        rec := make(map[string]any, len(f.Schema().Columns))
        for _, cs := range f.Schema().Columns {
            col, _ := f.ColumnByName(cs.Name)
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
        if err := writer.Write(rec); err != nil { return fmt.Errorf("parquet write row: %w", err) }
    }
    return nil
}
