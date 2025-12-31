package profile

import (
    "fmt"
    j "github.com/wdm0006/janitor/pkg/janitor"
    "math"
    "sort"
)

type NumStats struct {
    Count int
    Nulls int
    Min   float64
    Max   float64
    Sum   float64
}

type BoolStats struct {
    Count int
    Nulls int
    True  int
    False int
}

type StringStats struct {
    Count    int
    Nulls    int
    TopK     int
    Freqs    map[string]int
}

type ColumnProfile struct {
    Name string
    Kind j.Kind
    Num  *NumStats
    Bool *BoolStats
    Str  *StringStats
}

type Collector struct {
    cols   []ColumnProfile
    index  map[string]int
    topK   int
}

func NewCollector(schema j.Schema, topK int) *Collector {
    c := &Collector{index: make(map[string]int), topK: topK}
    c.cols = make([]ColumnProfile, len(schema.Columns))
    for i, cs := range schema.Columns {
        cp := ColumnProfile{Name: cs.Name, Kind: cs.Type}
        switch cs.Type {
        case j.KindFloat, j.KindInt:
            cp.Num = &NumStats{Min: math.Inf(1), Max: math.Inf(-1)}
        case j.KindBool:
            cp.Bool = &BoolStats{}
        case j.KindString, j.KindTime:
            cp.Str = &StringStats{TopK: topK, Freqs: make(map[string]int)}
        }
        c.cols[i] = cp
        c.index[cs.Name] = i
    }
    return c
}

func (c *Collector) ConsumeFrame(f *j.Frame) {
    for _, cs := range f.Schema().Columns {
        idx := c.index[cs.Name]
        cp := &c.cols[idx]
        switch cs.Type {
        case j.KindFloat:
            col := fMustFloat(f, cs.Name)
            for i := 0; i < col.Len(); i++ {
                if col.IsNull(i) { cp.Num.Nulls++; continue }
                v, _ := col.Get(i)
                cp.Num.Count++
                if v < cp.Num.Min { cp.Num.Min = v }
                if v > cp.Num.Max { cp.Num.Max = v }
                cp.Num.Sum += v
            }
        case j.KindInt:
            col := fMustInt(f, cs.Name)
            for i := 0; i < col.Len(); i++ {
                if col.IsNull(i) { cp.Num.Nulls++; continue }
                v, _ := col.Get(i)
                cp.Num.Count++
                fv := float64(v)
                if fv < cp.Num.Min { cp.Num.Min = fv }
                if fv > cp.Num.Max { cp.Num.Max = fv }
                cp.Num.Sum += fv
            }
        case j.KindBool:
            col := fMustBool(f, cs.Name)
            for i := 0; i < col.Len(); i++ {
                if col.IsNull(i) { cp.Bool.Nulls++; continue }
                v, _ := col.Get(i)
                cp.Bool.Count++
                if v { cp.Bool.True++ } else { cp.Bool.False++ }
            }
        case j.KindString:
            col := fMustString(f, cs.Name)
            for i := 0; i < col.Len(); i++ {
                if col.IsNull(i) { cp.Str.Nulls++; continue }
                v, _ := col.Get(i)
                cp.Str.Count++
                if c.topK > 0 {
                    cp.Str.Freqs[v]++
                }
            }
        case j.KindTime:
            col := fMustTime(f, cs.Name)
            for i := 0; i < col.Len(); i++ {
                if col.IsNull(i) { cp.Str.Nulls++; continue }
                // represent as string for frequency
                v, _ := col.Get(i)
                cp.Str.Count++
                if c.topK > 0 { cp.Str.Freqs[v.String()]++ }
            }
        }
    }
}

func fMustFloat(f *j.Frame, name string) *j.FloatColumn  { col,_ := f.ColumnByName(name); return col.(*j.FloatColumn) }
func fMustInt(f *j.Frame, name string) *j.IntColumn       { col,_ := f.ColumnByName(name); return col.(*j.IntColumn) }
func fMustBool(f *j.Frame, name string) *j.BoolColumn     { col,_ := f.ColumnByName(name); return col.(*j.BoolColumn) }
func fMustString(f *j.Frame, name string) *j.StringColumn { col,_ := f.ColumnByName(name); return col.(*j.StringColumn) }
func fMustTime(f *j.Frame, name string) *j.TimeColumn     { col,_ := f.ColumnByName(name); return col.(*j.TimeColumn) }

func (c *Collector) ReportText() string {
    var b stringsBuilder
    b.WriteString("Profile Summary\n")
    for _, cp := range c.cols {
        b.WriteString(fmt.Sprintf("- %s (%v): ", cp.Name, cp.Kind))
        switch cp.Kind {
        case j.KindFloat, j.KindInt:
            mean := 0.0
            if cp.Num.Count > 0 { mean = cp.Num.Sum / float64(cp.Num.Count) }
            b.WriteString(fmt.Sprintf("count=%d nulls=%d min=%.6g max=%.6g mean=%.6g\n", cp.Num.Count, cp.Num.Nulls, cp.Num.Min, cp.Num.Max, mean))
        case j.KindBool:
            b.WriteString(fmt.Sprintf("count=%d nulls=%d true=%d false=%d\n", cp.Bool.Count, cp.Bool.Nulls, cp.Bool.True, cp.Bool.False))
        default:
            b.WriteString(fmt.Sprintf("count=%d nulls=%d\n", cp.Str.Count, cp.Str.Nulls))
            if cp.Str != nil && len(cp.Str.Freqs) > 0 {
                type kv struct{ k string; v int }
                arr := make([]kv, 0, len(cp.Str.Freqs))
                for k, v := range cp.Str.Freqs { arr = append(arr, kv{k, v}) }
                sort.Slice(arr, func(i, j int) bool { return arr[i].v > arr[j].v })
                n := c.topK
                if n <= 0 || n > len(arr) { n = len(arr) }
                for i := 0; i < n; i++ {
                    b.WriteString(fmt.Sprintf("  • %q: %d\n", arr[i].k, arr[i].v))
                }
            }
        }
    }
    return b.String()
}

type JSONProfile struct {
    Columns []JSONColumn `json:"columns"`
}
type JSONColumn struct {
    Name string   `json:"name"`
    Kind string   `json:"kind"`
    Num  *NumStats `json:"num,omitempty"`
    Bool *BoolStats `json:"bool,omitempty"`
    Str  *struct {
        Count int            `json:"count"`
        Nulls int            `json:"nulls"`
        Top   map[string]int `json:"top,omitempty"`
    } `json:"str,omitempty"`
}

func (c *Collector) ReportJSON() JSONProfile {
    out := JSONProfile{Columns: make([]JSONColumn, 0, len(c.cols))}
    for _, cp := range c.cols {
        jc := JSONColumn{Name: cp.Name, Kind: kindString(cp.Kind)}
        switch cp.Kind {
        case j.KindFloat, j.KindInt:
            jc.Num = cp.Num
        case j.KindBool:
            jc.Bool = cp.Bool
        default:
            if cp.Str != nil {
                var top map[string]int
                if len(cp.Str.Freqs) > 0 {
                    top = cp.Str.Freqs
                }
                jc.Str = &struct{ Count int `json:"count"`; Nulls int `json:"nulls"`; Top map[string]int `json:"top,omitempty"`}{Count: cp.Str.Count, Nulls: cp.Str.Nulls, Top: top}
            }
        }
        out.Columns = append(out.Columns, jc)
    }
    return out
}

func kindString(k j.Kind) string {
    switch k {
    case j.KindBool:
        return "bool"
    case j.KindInt:
        return "int"
    case j.KindFloat:
        return "float"
    case j.KindString:
        return "string"
    case j.KindTime:
        return "time"
    default:
        return "invalid"
    }
}

type stringsBuilder struct{ buf []byte }
func (s *stringsBuilder) WriteString(x string) { s.buf = append(s.buf, x...) }
func (s *stringsBuilder) String() string       { return string(s.buf) }
