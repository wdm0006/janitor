package janitor

import (
	"fmt"
	"time"
)

// Schema describes the logical shape of a dataset.
type Schema struct {
	Columns []ColumnSchema
}

type ColumnSchema struct {
	Name     string
	Type     Kind
	Nullable bool
}

// Kind enumerates supported logical types.
type Kind int

const (
	KindInvalid Kind = iota
	KindBool
	KindInt
	KindFloat
	KindString
	KindTime
)

// Column is a typed, nullable column abstraction.
type Column interface {
	Name() string
	Kind() Kind
	Len() int
	IsNull(i int) bool
	SetNull(i int)
}

type BoolColumn struct {
	name  string
	data  []bool
	nulls []bool
}

func NewBoolColumn(name string, n int) *BoolColumn {
	return &BoolColumn{name: name, data: make([]bool, n), nulls: make([]bool, n)}
}
func (c *BoolColumn) Name() string           { return c.name }
func (c *BoolColumn) Kind() Kind             { return KindBool }
func (c *BoolColumn) Len() int               { return len(c.data) }
func (c *BoolColumn) IsNull(i int) bool      { return c.nulls[i] }
func (c *BoolColumn) SetNull(i int)          { c.nulls[i] = true }
func (c *BoolColumn) Get(i int) (bool, bool) { return c.data[i], !c.nulls[i] }
func (c *BoolColumn) Set(i int, v bool)      { c.data[i] = v; c.nulls[i] = false }
func (c *BoolColumn) AppendNull()            { c.data = append(c.data, false); c.nulls = append(c.nulls, true) }
func (c *BoolColumn) Append(v bool)          { c.data = append(c.data, v); c.nulls = append(c.nulls, false) }

type IntColumn struct {
	name  string
	data  []int64
	nulls []bool
}

func NewIntColumn(name string, n int) *IntColumn {
	return &IntColumn{name: name, data: make([]int64, n), nulls: make([]bool, n)}
}
func (c *IntColumn) Name() string            { return c.name }
func (c *IntColumn) Kind() Kind              { return KindInt }
func (c *IntColumn) Len() int                { return len(c.data) }
func (c *IntColumn) IsNull(i int) bool       { return c.nulls[i] }
func (c *IntColumn) SetNull(i int)           { c.nulls[i] = true }
func (c *IntColumn) Get(i int) (int64, bool) { return c.data[i], !c.nulls[i] }
func (c *IntColumn) Set(i int, v int64)      { c.data[i] = v; c.nulls[i] = false }
func (c *IntColumn) AppendNull()             { c.data = append(c.data, 0); c.nulls = append(c.nulls, true) }
func (c *IntColumn) Append(v int64)          { c.data = append(c.data, v); c.nulls = append(c.nulls, false) }

type FloatColumn struct {
	name  string
	data  []float64
	nulls []bool
}

func NewFloatColumn(name string, n int) *FloatColumn {
	return &FloatColumn{name: name, data: make([]float64, n), nulls: make([]bool, n)}
}
func (c *FloatColumn) Name() string              { return c.name }
func (c *FloatColumn) Kind() Kind                { return KindFloat }
func (c *FloatColumn) Len() int                  { return len(c.data) }
func (c *FloatColumn) IsNull(i int) bool         { return c.nulls[i] }
func (c *FloatColumn) SetNull(i int)             { c.nulls[i] = true }
func (c *FloatColumn) Get(i int) (float64, bool) { return c.data[i], !c.nulls[i] }
func (c *FloatColumn) Set(i int, v float64)      { c.data[i] = v; c.nulls[i] = false }
func (c *FloatColumn) AppendNull()               { c.data = append(c.data, 0); c.nulls = append(c.nulls, true) }
func (c *FloatColumn) Append(v float64)          { c.data = append(c.data, v); c.nulls = append(c.nulls, false) }

type StringColumn struct {
	name  string
	data  []string
	nulls []bool
}

func NewStringColumn(name string, n int) *StringColumn {
	return &StringColumn{name: name, data: make([]string, n), nulls: make([]bool, n)}
}
func (c *StringColumn) Name() string             { return c.name }
func (c *StringColumn) Kind() Kind               { return KindString }
func (c *StringColumn) Len() int                 { return len(c.data) }
func (c *StringColumn) IsNull(i int) bool        { return c.nulls[i] }
func (c *StringColumn) SetNull(i int)            { c.nulls[i] = true }
func (c *StringColumn) Get(i int) (string, bool) { return c.data[i], !c.nulls[i] }
func (c *StringColumn) Set(i int, v string)      { c.data[i] = v; c.nulls[i] = false }
func (c *StringColumn) AppendNull()              { c.data = append(c.data, ""); c.nulls = append(c.nulls, true) }
func (c *StringColumn) Append(v string)          { c.data = append(c.data, v); c.nulls = append(c.nulls, false) }

type TimeColumn struct {
	name  string
	data  []time.Time
	nulls []bool
}

func NewTimeColumn(name string, n int) *TimeColumn {
	return &TimeColumn{name: name, data: make([]time.Time, n), nulls: make([]bool, n)}
}
func (c *TimeColumn) Name() string                { return c.name }
func (c *TimeColumn) Kind() Kind                  { return KindTime }
func (c *TimeColumn) Len() int                    { return len(c.data) }
func (c *TimeColumn) IsNull(i int) bool           { return c.nulls[i] }
func (c *TimeColumn) SetNull(i int)               { c.nulls[i] = true }
func (c *TimeColumn) Get(i int) (time.Time, bool) { return c.data[i], !c.nulls[i] }
func (c *TimeColumn) Set(i int, v time.Time)      { c.data[i] = v; c.nulls[i] = false }
func (c *TimeColumn) AppendNull() {
	c.data = append(c.data, time.Time{})
	c.nulls = append(c.nulls, true)
}
func (c *TimeColumn) Append(v time.Time) {
	c.data = append(c.data, v)
	c.nulls = append(c.nulls, false)
}

// Frame is a columnar container for tabular data.
type Frame struct {
	schema Schema
	cols   []Column
	index  map[string]int // name -> col index
	nrows  int
}

func NewFrame(s Schema) *Frame {
	f := &Frame{schema: s, cols: make([]Column, len(s.Columns)), index: make(map[string]int)}
	for i, cs := range s.Columns {
		switch cs.Type {
		case KindBool:
			f.cols[i] = NewBoolColumn(cs.Name, 0)
		case KindInt:
			f.cols[i] = NewIntColumn(cs.Name, 0)
		case KindFloat:
			f.cols[i] = NewFloatColumn(cs.Name, 0)
		case KindString:
			f.cols[i] = NewStringColumn(cs.Name, 0)
		case KindTime:
			f.cols[i] = NewTimeColumn(cs.Name, 0)
		default:
			panic("invalid column kind")
		}
		f.index[cs.Name] = i
	}
	return f
}

func (f *Frame) Schema() Schema { return f.schema }
func (f *Frame) Rows() int      { return f.nrows }
func (f *Frame) Cols() int      { return len(f.cols) }

func (f *Frame) ColumnByName(name string) (Column, bool) {
	i, ok := f.index[name]
	if !ok {
		return nil, false
	}
	return f.cols[i], true
}

// AppendNullRow appends a row with all-null values.
func (f *Frame) AppendNullRow() {
	for _, c := range f.cols {
		switch col := c.(type) {
		case *BoolColumn:
			col.AppendNull()
		case *IntColumn:
			col.AppendNull()
		case *FloatColumn:
			col.AppendNull()
		case *StringColumn:
			col.AppendNull()
		case *TimeColumn:
			col.AppendNull()
		default:
			panic("unknown column type")
		}
	}
	f.nrows++
}

// SetCell sets a single cell value by name (row must exist).
func (f *Frame) SetCell(row int, name string, v any) error {
	i, ok := f.index[name]
	if !ok {
		return fmt.Errorf("unknown column: %s", name)
	}
	c := f.cols[i]
	switch col := c.(type) {
	case *BoolColumn:
		if v == nil {
			col.SetNull(row)
			return nil
		}
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("column %s expects bool", name)
		}
		col.Set(row, b)
	case *IntColumn:
		if v == nil {
			col.SetNull(row)
			return nil
		}
		switch t := v.(type) {
		case int:
			col.Set(row, int64(t))
		case int64:
			col.Set(row, t)
		case float64:
			col.Set(row, int64(t))
		default:
			return fmt.Errorf("column %s expects int/int64", name)
		}
	case *FloatColumn:
		if v == nil {
			col.SetNull(row)
			return nil
		}
		switch t := v.(type) {
		case float32:
			col.Set(row, float64(t))
		case float64:
			col.Set(row, t)
		case int:
			col.Set(row, float64(t))
		case int64:
			col.Set(row, float64(t))
		default:
			return fmt.Errorf("column %s expects float64", name)
		}
	case *StringColumn:
		if v == nil {
			col.SetNull(row)
			return nil
		}
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("column %s expects string", name)
		}
		col.Set(row, s)
	case *TimeColumn:
		if v == nil {
			col.SetNull(row)
			return nil
		}
		t, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("column %s expects time.Time", name)
		}
		col.Set(row, t)
	default:
		return fmt.Errorf("unknown column kind")
	}
	return nil
}
